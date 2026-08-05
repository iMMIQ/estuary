use std::{
    collections::HashMap,
    convert::Infallible,
    future::pending,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Json, Router,
    body::Body,
    extract::State,
    http::{HeaderMap, StatusCode, header::CONTENT_TYPE},
    response::Response,
    routing::post,
};
use bytes::Bytes;
use estuary::{
    Gateway, Settings,
    config::{NodeConfig, RetryConfig},
};
use futures_util::stream;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{sleep, timeout},
};

const IO_TIMEOUT: Duration = Duration::from_secs(3);

struct TestServer {
    base_url: String,
    task: JoinHandle<()>,
}

impl TestServer {
    async fn spawn(router: Router) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("test server failed");
        });
        Self {
            base_url: format!("http://{address}"),
            task,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn test_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("test client")
}

fn node(
    id: &str,
    upstream: &TestServer,
    models: impl IntoIterator<Item = (&'static str, &'static str)>,
) -> NodeConfig {
    NodeConfig {
        id: id.to_owned(),
        base_url: format!("{}/v1", upstream.base_url),
        models: models
            .into_iter()
            .map(|(public, upstream)| (public.to_owned(), upstream.to_owned()))
            .collect(),
        ..NodeConfig::default()
    }
}

async fn spawn_gateway(nodes: Vec<NodeConfig>) -> TestServer {
    let settings = gateway_settings(nodes);
    let gateway = Gateway::build(settings).expect("build gateway");
    TestServer::spawn(gateway.public_router()).await
}

fn gateway_settings(nodes: Vec<NodeConfig>) -> Settings {
    let mut settings = Settings {
        nodes,
        retry: RetryConfig {
            max_attempts: 1,
            ..RetryConfig::default()
        },
        ..Settings::default()
    };
    settings.health.route_while_starting = true;
    settings
}

#[tokio::test]
async fn models_are_openai_shaped_sorted_and_deduplicated() {
    let upstream = TestServer::spawn(Router::new()).await;
    let gateway = spawn_gateway(vec![
        node(
            "node-b",
            &upstream,
            [("public-b", "internal-b"), ("shared", "shared-b")],
        ),
        node(
            "node-a",
            &upstream,
            [("public-a", "internal-a"), ("shared", "shared-a")],
        ),
    ])
    .await;

    let response = test_client()
        .get(gateway.url("/v1/models"))
        .send()
        .await
        .expect("models response");
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = response.json().await.expect("models JSON");
    assert_eq!(
        body,
        json!({
            "object": "list",
            "data": [
                {"id": "public-a", "object": "model", "created": 0, "owned_by": "estuary"},
                {"id": "public-b", "object": "model", "created": 0, "owned_by": "estuary"},
                {"id": "shared", "object": "model", "created": 0, "owned_by": "estuary"}
            ]
        })
    );

    let response = test_client()
        .get(gateway.url("/v1/models/public-a"))
        .send()
        .await
        .expect("model response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.json::<Value>().await.expect("model JSON"),
        json!({
            "id": "public-a",
            "object": "model",
            "created": 0,
            "owned_by": "estuary"
        })
    );
}

struct CapturedRequest {
    headers: HeaderMap,
    body: Bytes,
}

async fn capture_request(
    State(sender): State<mpsc::UnboundedSender<CapturedRequest>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    sender
        .send(CapturedRequest { headers, body })
        .expect("capture request");
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"id":"chatcmpl_1","object":"chat.completion","model":"internal-model","choices":[],"vendor_response":{"kept":true}}"#,
        ))
        .expect("upstream response")
}

#[tokio::test]
async fn maps_model_preserves_unknown_fields_and_replaces_credentials() {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let upstream = TestServer::spawn(
        Router::new()
            .route("/v1/chat/completions", post(capture_request))
            .with_state(sender),
    )
    .await;
    let mut upstream_node = node("only-node", &upstream, [("public-model", "internal-model")]);
    upstream_node.headers = HashMap::from([(
        "authorization".to_owned(),
        "Bearer upstream-secret".to_owned(),
    )]);
    let gateway = spawn_gateway(vec![upstream_node]).await;
    let request = json!({
        "model": "public-model",
        "messages": [{"role": "user", "content": "hello", "vendor_part": 17}],
        "vendor_extension": {"nested": [true, null, {"answer": 42}]}
    });

    let response = test_client()
        .post(gateway.url("/v1/chat/completions"))
        .header(CONTENT_TYPE, "application/json")
        .header("authorization", "Bearer client-secret")
        .header("openai-organization", "client-org")
        .header("openai-project", "client-project")
        .header("x-api-key", "client-provider-key")
        .header("openai-beta", "responses=v1")
        .json(&request)
        .send()
        .await
        .expect("gateway response");
    assert_eq!(response.status(), StatusCode::OK);
    let response_body: Value = response.json().await.expect("response JSON");
    assert_eq!(response_body["vendor_response"]["kept"], true);

    let captured = timeout(IO_TIMEOUT, receiver.recv())
        .await
        .expect("upstream request timeout")
        .expect("upstream request missing");
    assert_eq!(
        captured
            .headers
            .get("authorization")
            .and_then(|value| value.to_str().ok()),
        Some("Bearer upstream-secret")
    );
    assert!(!captured.headers.contains_key("openai-organization"));
    assert!(!captured.headers.contains_key("openai-project"));
    assert!(!captured.headers.contains_key("x-api-key"));
    assert_eq!(
        captured
            .headers
            .get("openai-beta")
            .and_then(|value| value.to_str().ok()),
        Some("responses=v1")
    );
    assert!(captured.headers.contains_key("x-gateway-request-id"));

    let upstream_body: Value = serde_json::from_slice(&captured.body).expect("upstream JSON");
    assert_eq!(upstream_body["model"], "internal-model");
    assert_eq!(upstream_body["messages"][0]["vendor_part"], 17);
    assert_eq!(
        upstream_body["vendor_extension"],
        request["vendor_extension"]
    );
}

static CHAT_SSE: &[u8] = b": upstream comment\r\n\r\ndata: { \"id\": \"chatcmpl_1\", \"object\": \"chat.completion.chunk\", \"vendor\": [1,2], \"choices\": [{\"index\":0,\"delta\":{\"content\":\"hello\"},\"finish_reason\":null}] }\r\n\r\ndata: [DONE]\r\n\r\n";

async fn chat_sse() -> Response {
    let chunks = vec![
        Bytes::from_static(&CHAT_SSE[..7]),
        Bytes::from_static(&CHAT_SSE[7..71]),
        Bytes::from_static(&CHAT_SSE[71..CHAT_SSE.len() - 3]),
        Bytes::from_static(&CHAT_SSE[CHAT_SSE.len() - 3..]),
    ];
    let body = Body::from_stream(stream::iter(chunks.into_iter().map(Ok::<_, Infallible>)));
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/event-stream; charset=utf-8")
        .header("cache-control", "no-cache")
        .header("x-request-id", "upstream-request-1")
        .body(body)
        .expect("SSE response")
}

#[tokio::test]
async fn chat_sse_bytes_are_forwarded_unchanged() {
    let upstream =
        TestServer::spawn(Router::new().route("/v1/chat/completions", post(chat_sse))).await;
    let gateway = spawn_gateway(vec![node(
        "sse-node",
        &upstream,
        [("chat-model", "chat-model")],
    )])
    .await;

    let response = test_client()
        .post(gateway.url("/v1/chat/completions"))
        .json(&json!({
            "model": "chat-model",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": true
        }))
        .send()
        .await
        .expect("SSE response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("text/event-stream; charset=utf-8")
    );
    assert_eq!(
        response
            .headers()
            .get("x-upstream-request-id")
            .and_then(|value| value.to_str().ok()),
        Some("upstream-request-1")
    );
    assert_eq!(
        response
            .headers()
            .get("x-accel-buffering")
            .and_then(|value| value.to_str().ok()),
        Some("no")
    );
    assert_eq!(response.bytes().await.expect("SSE body").as_ref(), CHAT_SSE);
}

static RESPONSES_SSE: &[u8] = b"event: response.created\r\ndata: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_1\",\"status\":\"in_progress\",\"vendor\":true}}\r\n\r\nevent: response.output_text.delta\r\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":1,\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"delta\":\"hello\"}\r\n\r\nevent: response.completed\r\ndata: {\"type\":\"response.completed\",\"sequence_number\":2,\"response\":{\"id\":\"resp_1\",\"status\":\"completed\"}}\r\n\r\n";

async fn responses_sse() -> Response {
    let chunks = vec![
        Bytes::from_static(&RESPONSES_SSE[..3]),
        Bytes::from_static(&RESPONSES_SSE[3..29]),
        Bytes::from_static(&RESPONSES_SSE[29..157]),
        Bytes::from_static(&RESPONSES_SSE[157..RESPONSES_SSE.len() - 1]),
        Bytes::from_static(&RESPONSES_SSE[RESPONSES_SSE.len() - 1..]),
    ];
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/event-stream; charset=utf-8")
        .body(Body::from_stream(stream::iter(
            chunks.into_iter().map(Ok::<_, Infallible>),
        )))
        .expect("Responses SSE response")
}

#[tokio::test]
async fn responses_named_sse_bytes_are_forwarded_unchanged_without_done() {
    let upstream =
        TestServer::spawn(Router::new().route("/v1/responses", post(responses_sse))).await;
    let gateway = spawn_gateway(vec![node(
        "responses-node",
        &upstream,
        [("responses-model", "responses-model")],
    )])
    .await;

    let response = test_client()
        .post(gateway.url("/v1/responses"))
        .json(&json!({
            "model": "responses-model",
            "input": "hello",
            "stream": true
        }))
        .send()
        .await
        .expect("Responses SSE response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("text/event-stream; charset=utf-8")
    );
    let body = response.bytes().await.expect("Responses SSE body");
    assert_eq!(body.as_ref(), RESPONSES_SSE);
    assert!(
        !body
            .windows(b"[DONE]".len())
            .any(|window| window == b"[DONE]")
    );
}

#[tokio::test]
async fn oversized_request_returns_openai_413_with_request_id() {
    let upstream = TestServer::spawn(Router::new()).await;
    let mut settings = gateway_settings(vec![node(
        "unused-node",
        &upstream,
        [("chat-model", "chat-model")],
    )]);
    settings.server.max_request_body_bytes = 128;
    let gateway = Gateway::build(settings).expect("build limited gateway");
    let gateway = TestServer::spawn(gateway.public_router()).await;

    let response = test_client()
        .post(gateway.url("/v1/chat/completions"))
        .json(&json!({
            "model": "chat-model",
            "messages": [{"role": "user", "content": "x".repeat(512)}]
        }))
        .send()
        .await
        .expect("oversized response");
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("application/json"))
    );
    let request_id = response
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .expect("413 x-request-id");
    assert!(!request_id.is_empty());

    let body: Value = response.json().await.expect("413 OpenAI error JSON");
    assert_eq!(body["error"]["type"], "invalid_request_error");
    assert_eq!(body["error"]["code"], "request_too_large");
    assert_eq!(body["error"]["param"], Value::Null);
    assert!(
        body["error"]["message"]
            .as_str()
            .is_some_and(|message| !message.is_empty())
    );
}

#[tokio::test]
async fn stateful_responses_parameters_return_openai_400() {
    let upstream = TestServer::spawn(Router::new()).await;
    let gateway = spawn_gateway(vec![node(
        "unused-node",
        &upstream,
        [("responses-model", "responses-model")],
    )])
    .await;

    for request in [
        json!({"model": "responses-model", "input": "hello", "background": true}),
        json!({
            "model": "responses-model",
            "input": "hello",
            "previous_response_id": "resp_previous"
        }),
    ] {
        let response = test_client()
            .post(gateway.url("/v1/responses"))
            .json(&request)
            .send()
            .await
            .expect("stateful Responses rejection");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(response.headers().contains_key("x-request-id"));
        let body: Value = response.json().await.expect("OpenAI error JSON");
        assert_eq!(body["error"]["type"], "invalid_request_error");
        assert_eq!(body["error"]["code"], "unsupported_feature");
        assert_eq!(body["error"]["param"], Value::Null);
    }
}

static OPENAI_ERROR: &[u8] = br#"{ "error": { "message": "slow down", "type": "rate_limit_error", "param": null, "code": "rate_limit_exceeded", "vendor": true } }"#;

async fn valid_openai_error() -> Response {
    Response::builder()
        .status(StatusCode::TOO_MANY_REQUESTS)
        .header(CONTENT_TYPE, "application/json")
        .header("retry-after", "7")
        .header("x-request-id", "upstream-error-request")
        .body(Body::from(OPENAI_ERROR))
        .expect("error response")
}

#[tokio::test]
async fn valid_upstream_openai_error_is_forwarded() {
    let upstream =
        TestServer::spawn(Router::new().route("/v1/chat/completions", post(valid_openai_error)))
            .await;
    let gateway = spawn_gateway(vec![node(
        "error-node",
        &upstream,
        [("chat-model", "chat-model")],
    )])
    .await;

    let response = test_client()
        .post(gateway.url("/v1/chat/completions"))
        .json(&json!({
            "model": "chat-model",
            "messages": [{"role": "user", "content": "hello"}]
        }))
        .send()
        .await
        .expect("error response");
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        response
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok()),
        Some("7")
    );
    assert_eq!(
        response.bytes().await.expect("error body").as_ref(),
        OPENAI_ERROR
    );
}

async fn fixed_length_success() -> Response {
    const BODY: &str = r#"{"id":"chatcmpl_fixed","object":"chat.completion","model":"internal-model","choices":[]}"#;
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .header("content-length", BODY.len())
        .body(Body::from(BODY))
        .expect("fixed-length response")
}

#[tokio::test]
async fn complete_content_length_body_is_not_counted_as_cancelled() {
    let upstream =
        TestServer::spawn(Router::new().route("/v1/chat/completions", post(fixed_length_success)))
            .await;
    let gateway = Gateway::build(gateway_settings(vec![node(
        "fixed-length-node",
        &upstream,
        [("public-model", "internal-model")],
    )]))
    .expect("build gateway");
    let public = TestServer::spawn(gateway.public_router()).await;
    let admin = TestServer::spawn(gateway.admin_router()).await;

    let response = test_client()
        .post(public.url("/v1/chat/completions"))
        .json(&json!({
            "model": "public-model",
            "messages": [{"role": "user", "content": "hello"}]
        }))
        .send()
        .await
        .expect("fixed-length gateway response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.json::<Value>().await.expect("fixed-length JSON")["id"],
        "chatcmpl_fixed"
    );

    let metrics = test_client()
        .get(admin.url("/metrics"))
        .send()
        .await
        .expect("metrics response")
        .text()
        .await
        .expect("metrics body");
    assert!(!metrics.contains("estuary_stream_cancellations_total{node=\"fixed-length-node\"}"));
    assert!(metrics.contains("estuary_node_active{node=\"fixed-length-node\"} 0"));
}

#[derive(Clone)]
struct FirstStreamState {
    requests: Arc<AtomicUsize>,
}

fn successful_followup() -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"id":"chatcmpl_followup","object":"chat.completion","model":"internal-model","choices":[]}"#,
        ))
        .expect("follow-up response")
}

async fn first_error_body_hangs(State(state): State<FirstStreamState>) -> Response {
    if state.requests.fetch_add(1, Ordering::SeqCst) > 0 {
        return successful_followup();
    }

    let body = Body::from_stream(async_stream::stream! {
        yield Ok::<Bytes, Infallible>(Bytes::from_static(b"{\"error\":"));
        pending::<()>().await;
    });
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .header(CONTENT_TYPE, "application/json")
        .body(body)
        .expect("hanging error response")
}

#[tokio::test]
async fn hanging_error_body_times_out_and_releases_node_permit() {
    let state = FirstStreamState {
        requests: Arc::new(AtomicUsize::new(0)),
    };
    let upstream = TestServer::spawn(
        Router::new()
            .route("/v1/chat/completions", post(first_error_body_hangs))
            .with_state(state.clone()),
    )
    .await;
    let mut upstream_node = node(
        "error-body-node",
        &upstream,
        [("public-model", "internal-model")],
    );
    upstream_node.max_concurrency = 1;
    let mut settings = gateway_settings(vec![upstream_node]);
    settings.server.stream_idle_timeout_ms = 75;
    settings.server.upstream_body_timeout_ms = 500;
    let gateway = Gateway::build(settings).expect("build gateway");
    let gateway = TestServer::spawn(gateway.public_router()).await;
    let client = test_client();
    let payload = json!({
        "model": "public-model",
        "messages": [{"role": "user", "content": "hello"}]
    });

    let first = timeout(
        IO_TIMEOUT,
        client
            .post(gateway.url("/v1/chat/completions"))
            .json(&payload)
            .send(),
    )
    .await
    .expect("hanging error body did not time out")
    .expect("gateway error response");
    assert_eq!(first.status(), StatusCode::BAD_GATEWAY);
    let error: Value = first.json().await.expect("gateway error JSON");
    assert_eq!(error["error"]["code"], "invalid_upstream_response");

    let second = timeout(
        IO_TIMEOUT,
        client
            .post(gateway.url("/v1/chat/completions"))
            .json(&payload)
            .send(),
    )
    .await
    .expect("node permit was not released after error body timeout")
    .expect("follow-up response");
    assert_eq!(second.status(), StatusCode::OK);
    assert_eq!(
        second.json::<Value>().await.expect("follow-up JSON")["id"],
        "chatcmpl_followup"
    );
    assert_eq!(state.requests.load(Ordering::SeqCst), 2);
}

async fn first_body_floods(State(state): State<FirstStreamState>) -> Response {
    if state.requests.fetch_add(1, Ordering::SeqCst) > 0 {
        return successful_followup();
    }

    let body = Body::from_stream(async_stream::stream! {
        let chunk = Bytes::from(vec![b'x'; 1024 * 1024]);
        loop {
            yield Ok::<Bytes, Infallible>(chunk.clone());
            tokio::task::yield_now().await;
        }
    });
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(body)
        .expect("continuous response")
}

#[tokio::test]
async fn unpolled_downstream_body_stalls_then_releases_node_permit() {
    let state = FirstStreamState {
        requests: Arc::new(AtomicUsize::new(0)),
    };
    let upstream = TestServer::spawn(
        Router::new()
            .route("/v1/chat/completions", post(first_body_floods))
            .with_state(state.clone()),
    )
    .await;
    let mut upstream_node = node(
        "downstream-stall-node",
        &upstream,
        [("public-model", "internal-model")],
    );
    upstream_node.max_concurrency = 1;
    let mut settings = gateway_settings(vec![upstream_node]);
    settings.server.stream_idle_timeout_ms = 1_000;
    settings.server.upstream_body_timeout_ms = 10_000;
    settings.server.downstream_stall_timeout_ms = 75;
    let gateway = Gateway::build(settings).expect("build gateway");
    let gateway = TestServer::spawn(gateway.public_router()).await;

    let payload = json!({
        "model": "public-model",
        "messages": [{"role": "user", "content": "hello"}],
        "stream": true
    })
    .to_string();
    let authority = gateway
        .base_url
        .strip_prefix("http://")
        .expect("gateway HTTP URL");
    let mut unread_client = TcpStream::connect(authority)
        .await
        .expect("connect unread downstream");
    let request = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: {authority}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
        payload.len(),
    );
    unread_client
        .write_all(request.as_bytes())
        .await
        .expect("send unread downstream request");

    timeout(IO_TIMEOUT, async {
        while state.requests.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first upstream request was not observed");

    let followup_payload = json!({
        "model": "public-model",
        "messages": [{"role": "user", "content": "follow up"}]
    });
    let second = timeout(
        IO_TIMEOUT,
        test_client()
            .post(gateway.url("/v1/chat/completions"))
            .json(&followup_payload)
            .send(),
    )
    .await
    .expect("node permit was not released after downstream stall")
    .expect("follow-up response");
    assert_eq!(second.status(), StatusCode::OK);
    assert_eq!(
        second.json::<Value>().await.expect("follow-up JSON")["id"],
        "chatcmpl_followup"
    );
    assert_eq!(state.requests.load(Ordering::SeqCst), 2);

    let mut raw_response = Vec::new();
    let read_result = timeout(IO_TIMEOUT, unread_client.read_to_end(&mut raw_response))
        .await
        .expect("stalled downstream did not terminate after reading resumed");
    assert!(
        read_result.is_err() || !raw_response.ends_with(b"0\r\n\r\n"),
        "stalled downstream ended as a complete chunked response"
    );
}

async fn first_body_runs_past_deadline(State(state): State<FirstStreamState>) -> Response {
    if state.requests.fetch_add(1, Ordering::SeqCst) > 0 {
        return successful_followup();
    }

    let body = Body::from_stream(async_stream::stream! {
        loop {
            yield Ok::<Bytes, Infallible>(Bytes::from_static(b"data: tick\n\n"));
            sleep(Duration::from_millis(10)).await;
        }
    });
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/event-stream")
        .body(body)
        .expect("long-running response")
}

#[tokio::test]
async fn whole_body_deadline_is_observable_and_releases_node_permit() {
    let state = FirstStreamState {
        requests: Arc::new(AtomicUsize::new(0)),
    };
    let upstream = TestServer::spawn(
        Router::new()
            .route("/v1/chat/completions", post(first_body_runs_past_deadline))
            .with_state(state.clone()),
    )
    .await;
    let mut upstream_node = node(
        "body-deadline-node",
        &upstream,
        [("public-model", "internal-model")],
    );
    upstream_node.max_concurrency = 1;
    let mut settings = gateway_settings(vec![upstream_node]);
    settings.server.stream_idle_timeout_ms = 200;
    settings.server.upstream_body_timeout_ms = 250;
    settings.server.downstream_stall_timeout_ms = 1_000;
    let gateway = Gateway::build(settings).expect("build gateway");
    let gateway = TestServer::spawn(gateway.public_router()).await;
    let client = test_client();
    let payload = json!({
        "model": "public-model",
        "messages": [{"role": "user", "content": "hello"}],
        "stream": true
    });

    let mut first = client
        .post(gateway.url("/v1/chat/completions"))
        .json(&payload)
        .send()
        .await
        .expect("streaming response");
    assert_eq!(first.status(), StatusCode::OK);
    let mut received = 0usize;
    let body_result = timeout(IO_TIMEOUT, async {
        loop {
            match first.chunk().await {
                Ok(Some(chunk)) => received += chunk.len(),
                Ok(None) => return Ok(()),
                Err(error) => return Err(error),
            }
        }
    })
    .await
    .expect("whole body deadline did not terminate the response");
    assert!(body_result.is_err(), "body deadline ended as a clean EOF");
    assert!(received > 0, "upstream produced no bytes before deadline");
    drop(first);

    let second = timeout(
        IO_TIMEOUT,
        client
            .post(gateway.url("/v1/chat/completions"))
            .json(&payload)
            .send(),
    )
    .await
    .expect("node permit was not released after whole body deadline")
    .expect("follow-up response");
    assert_eq!(second.status(), StatusCode::OK);
    assert_eq!(
        second.json::<Value>().await.expect("follow-up JSON")["id"],
        "chatcmpl_followup"
    );
    assert_eq!(state.requests.load(Ordering::SeqCst), 2);
}

#[derive(Clone)]
struct HangingState {
    requests: Arc<AtomicUsize>,
    dropped: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

struct DropSignal(Option<oneshot::Sender<()>>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

async fn first_response_hangs(State(state): State<HangingState>) -> Response {
    let request = state.requests.fetch_add(1, Ordering::SeqCst);
    if request > 0 {
        return Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"id":"chatcmpl_2","object":"chat.completion","model":"internal-model","choices":[]}"#,
            ))
            .expect("second response");
    }

    let signal = state.dropped.lock().expect("drop signal lock").take();
    let body = Body::from_stream(async_stream::stream! {
        let _signal = DropSignal(signal);
        yield Ok::<Bytes, Infallible>(Bytes::from_static(
            b"data: {\"id\":\"chatcmpl_hanging\",\"object\":\"chat.completion.chunk\",\"choices\":[]}\n\n",
        ));
        pending::<()>().await;
    });
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/event-stream")
        .body(body)
        .expect("hanging response")
}

#[tokio::test]
async fn dropping_downstream_body_releases_node_permit() {
    let (dropped_sender, dropped_receiver) = oneshot::channel();
    let state = HangingState {
        requests: Arc::new(AtomicUsize::new(0)),
        dropped: Arc::new(Mutex::new(Some(dropped_sender))),
    };
    let upstream = TestServer::spawn(
        Router::new()
            .route("/v1/chat/completions", post(first_response_hangs))
            .with_state(state.clone()),
    )
    .await;
    let mut upstream_node = node(
        "single-slot-node",
        &upstream,
        [("public-model", "internal-model")],
    );
    upstream_node.max_concurrency = 1;
    let gateway = spawn_gateway(vec![upstream_node]).await;
    let client = test_client();
    let payload = json!({
        "model": "public-model",
        "messages": [{"role": "user", "content": "hello"}],
        "stream": true
    });

    let mut first = client
        .post(gateway.url("/v1/chat/completions"))
        .json(&payload)
        .send()
        .await
        .expect("first response");
    let first_chunk = timeout(IO_TIMEOUT, first.chunk())
        .await
        .expect("first chunk timeout")
        .expect("first chunk error")
        .expect("first chunk missing");
    assert!(first_chunk.starts_with(b"data:"));
    drop(first);

    timeout(IO_TIMEOUT, dropped_receiver)
        .await
        .expect("upstream body was not cancelled")
        .expect("drop signal sender disappeared");

    let second = timeout(
        IO_TIMEOUT,
        client
            .post(gateway.url("/v1/chat/completions"))
            .json(&payload)
            .send(),
    )
    .await
    .expect("second request remained queued")
    .expect("second response");
    assert_eq!(second.status(), StatusCode::OK);
    let body: Value = timeout(IO_TIMEOUT, second.json())
        .await
        .expect("second body timeout")
        .expect("second body JSON");
    assert_eq!(body["id"], "chatcmpl_2");
    assert_eq!(state.requests.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn admin_drain_and_resume_change_readiness() {
    let upstream = TestServer::spawn(Router::new()).await;
    let gateway = Gateway::build(gateway_settings(vec![node(
        "drainable",
        &upstream,
        [("model", "model")],
    )]))
    .expect("build gateway");
    let admin = TestServer::spawn(gateway.admin_router()).await;
    let client = test_client();

    let drained = client
        .put(admin.url("/admin/nodes/drainable/drain"))
        .send()
        .await
        .expect("drain response");
    assert_eq!(drained.status(), StatusCode::OK);
    let drained: Value = drained.json().await.expect("drain JSON");
    assert_eq!(drained["lifecycle"], "draining");
    assert_eq!(drained["drained"], true);

    let readiness = client
        .get(admin.url("/health/ready"))
        .send()
        .await
        .expect("readiness response");
    assert_eq!(readiness.status(), StatusCode::SERVICE_UNAVAILABLE);

    let resumed = client
        .delete(admin.url("/admin/nodes/drainable/drain"))
        .send()
        .await
        .expect("resume response");
    assert_eq!(resumed.status(), StatusCode::OK);
    let resumed: Value = resumed.json().await.expect("resume JSON");
    assert_eq!(resumed["lifecycle"], "serving");
    assert_eq!(resumed["routable"], true);
}

#[tokio::test]
async fn non_streaming_body_failure_retries_before_downstream_commit() {
    let broken_requests = Arc::new(AtomicUsize::new(0));
    let broken_counter = Arc::clone(&broken_requests);
    let broken = TestServer::spawn(Router::new().route(
        "/v1/chat/completions",
        post(move || {
            let broken_counter = Arc::clone(&broken_counter);
            async move {
                broken_counter.fetch_add(1, Ordering::SeqCst);
                let chunks = async_stream::stream! {
                    yield Ok::<_, io::Error>(Bytes::from_static(b"{\"partial\":"));
                    sleep(Duration::from_millis(20)).await;
                    yield Err(io::Error::other("truncated upstream body"));
                };
                Response::builder()
                    .status(StatusCode::OK)
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from_stream(chunks))
                    .unwrap()
            }
        }),
    ))
    .await;
    let fallback_requests = Arc::new(AtomicUsize::new(0));
    let fallback_counter = Arc::clone(&fallback_requests);
    let fallback = TestServer::spawn(Router::new().route(
        "/v1/chat/completions",
        post(move || {
            let fallback_counter = Arc::clone(&fallback_counter);
            async move {
                fallback_counter.fetch_add(1, Ordering::SeqCst);
                Json(json!({
                    "id": "chatcmpl_fallback",
                    "object": "chat.completion",
                    "choices": []
                }))
            }
        }),
    ))
    .await;
    let mut settings = gateway_settings(vec![
        node("broken", &broken, [("model", "model")]),
        node("fallback", &fallback, [("model", "model")]),
    ]);
    settings.retry.max_attempts = 2;
    let gateway = TestServer::spawn(
        Gateway::build(settings)
            .expect("build gateway")
            .public_router(),
    )
    .await;

    let response = test_client()
        .post(gateway.url("/v1/chat/completions"))
        .json(&json!({
            "model": "model",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": false
        }))
        .send()
        .await
        .expect("gateway response");
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = response.json().await.expect("complete fallback JSON");
    assert_eq!(body["id"], "chatcmpl_fallback");
    assert_eq!(broken_requests.load(Ordering::SeqCst), 1);
    assert_eq!(fallback_requests.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn streaming_body_failure_never_switches_nodes_after_headers() {
    let broken = TestServer::spawn(Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            let chunks = async_stream::stream! {
                yield Ok::<_, io::Error>(Bytes::from_static(b"data: {\"partial\":true}\n\n"));
                sleep(Duration::from_millis(20)).await;
                yield Err(io::Error::other("stream failed"));
            };
            Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "text/event-stream")
                .body(Body::from_stream(chunks))
                .unwrap()
        }),
    ))
    .await;
    let fallback_requests = Arc::new(AtomicUsize::new(0));
    let fallback_counter = Arc::clone(&fallback_requests);
    let fallback = TestServer::spawn(Router::new().route(
        "/v1/chat/completions",
        post(move || {
            let fallback_counter = Arc::clone(&fallback_counter);
            async move {
                fallback_counter.fetch_add(1, Ordering::SeqCst);
                "data: [DONE]\n\n"
            }
        }),
    ))
    .await;
    let mut settings = gateway_settings(vec![
        node("broken", &broken, [("model", "model")]),
        node("fallback", &fallback, [("model", "model")]),
    ]);
    settings.retry.max_attempts = 2;
    let gateway = TestServer::spawn(
        Gateway::build(settings)
            .expect("build gateway")
            .public_router(),
    )
    .await;

    let response = test_client()
        .post(gateway.url("/v1/chat/completions"))
        .json(&json!({
            "model": "model",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": true
        }))
        .send()
        .await
        .expect("streaming response headers");
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.bytes().await.is_err());
    assert_eq!(fallback_requests.load(Ordering::SeqCst), 0);
}
