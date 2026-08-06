use std::{
    convert::Infallible,
    future::pending,
    io,
    net::SocketAddr,
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
    config::{AnthropicProtocol, NodeConfig, ProviderKind, RetryConfig},
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

struct RunningGateway {
    base_url: String,
    task: JoinHandle<anyhow::Result<()>>,
}

impl RunningGateway {
    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }
}

impl Drop for RunningGateway {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn spawn_vllm_gateway(mut config: NodeConfig) -> RunningGateway {
    config.provider.kind = ProviderKind::Vllm;
    config.provider.monitor_interval_ms = 25;
    config.provider.request_timeout_ms = 500;
    config.provider.telemetry_stale_ms = 500;
    let public = unused_address().await;
    let admin = unused_address().await;
    let mut settings = gateway_settings(vec![config]);
    settings.server.listen = public.to_string();
    settings.server.admin_listen = admin.to_string();
    settings.server.withdrawal_delay_ms = 1;
    let task = tokio::spawn(async move { Gateway::build(settings)?.run().await });
    wait_for_success(&test_client(), &format!("http://{admin}/health/ready")).await;
    RunningGateway {
        base_url: format!("http://{public}"),
        task,
    }
}

async fn unused_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind unused address");
    listener.local_addr().expect("unused address")
}

async fn wait_for_success(client: &reqwest::Client, url: &str) {
    timeout(IO_TIMEOUT, async {
        loop {
            if client
                .get(url)
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                return;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("gateway did not become ready");
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
    upstream_node.api_key = Some("upstream-secret".to_owned());
    let gateway = spawn_gateway(vec![upstream_node]).await;
    let request = json!({
        "model": "public-model",
        "system": [
            {"type": "text", "text": "x-anthropic-billing-header: cc_version=2.1.220.8a5; cc_entrypoint=sdk-cli;"},
            {"type": "text", "text": "Keep this system instruction", "cache_control": {"type": "ephemeral"}}
        ],
        "messages": [
            {"role": "system", "content": "x-anthropic-billing-header: cc_version=2.1.219.7f1; cc_entrypoint=cli;"},
            {"role": "user", "content": "hello", "vendor_part": 17}
        ],
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
    assert!(!captured.headers.contains_key("anthropic-version"));
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
    assert_eq!(upstream_body["messages"].as_array().unwrap().len(), 1);
    assert_eq!(upstream_body["messages"][0]["vendor_part"], 17);
    assert_eq!(upstream_body["system"].as_array().unwrap().len(), 1);
    assert_eq!(
        upstream_body["system"][0]["text"],
        "Keep this system instruction"
    );
    assert_eq!(
        upstream_body["vendor_extension"],
        request["vendor_extension"]
    );
}

async fn anthropic_non_stream_response(
    State(sender): State<mpsc::UnboundedSender<CapturedRequest>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    sender
        .send(CapturedRequest { headers, body })
        .expect("capture Anthropic request");
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"id":"chatcmpl_anthropic","model":"internal-model","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":21,"completion_tokens":2}}"#,
        ))
        .expect("upstream Anthropic response")
}

#[tokio::test]
async fn anthropic_messages_maps_request_response_and_claude_code_system() {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let upstream = TestServer::spawn(
        Router::new()
            .route("/v1/chat/completions", post(anthropic_non_stream_response))
            .with_state(sender),
    )
    .await;
    let mut upstream_node = node("anthropic-node", &upstream, [("claude", "internal-model")]);
    upstream_node.api_key = Some("upstream-secret".to_owned());
    let gateway = spawn_gateway(vec![upstream_node]).await;

    let response = test_client()
        .post(gateway.url("/v1/messages?beta=true"))
        .header("x-api-key", "client-key")
        .header("anthropic-version", "2023-06-01")
        .json(&json!({
            "model": "claude",
            "max_tokens": 128,
            "system": [
                {"type": "text", "text": "x-anthropic-billing-header: cc_version=2.1.220.8a5; cc_entrypoint=sdk-cli;"},
                {"type": "text", "text": "Use tools carefully", "cache_control": {"type": "ephemeral"}}
            ],
            "messages": [{"role": "user", "content": "hello"}],
            "tools": [{"name": "Read", "input_schema": {"type": "object", "properties": {}}}]
        }))
        .send()
        .await
        .expect("Anthropic gateway response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/json")
    );
    let body: Value = response.json().await.expect("Anthropic response JSON");
    assert_eq!(body["type"], "message");
    assert_eq!(body["id"], "msg_chatcmpl_anthropic");
    assert_eq!(body["model"], "claude");
    assert_eq!(body["content"][0], json!({"type": "text", "text": "hello"}));
    assert_eq!(body["stop_reason"], "end_turn");
    assert_eq!(body["usage"]["input_tokens"], 21);

    let captured = timeout(IO_TIMEOUT, receiver.recv())
        .await
        .expect("upstream Anthropic request timeout")
        .expect("upstream Anthropic request missing");
    assert_eq!(
        captured
            .headers
            .get("authorization")
            .and_then(|value| value.to_str().ok()),
        Some("Bearer upstream-secret")
    );
    assert!(!captured.headers.contains_key("x-api-key"));
    let upstream_body: Value = serde_json::from_slice(&captured.body).expect("upstream JSON");
    assert_eq!(upstream_body["model"], "internal-model");
    assert_eq!(
        upstream_body["messages"][0],
        json!({"role": "system", "content": "Use tools carefully"})
    );
    assert_eq!(upstream_body["messages"][1]["role"], "user");
    assert_eq!(upstream_body["tools"][0]["function"]["name"], "Read");
    assert_eq!(upstream_body["max_completion_tokens"], 128);
    assert!(upstream_body.get("max_tokens").is_none());
    assert_eq!(upstream_body["stream"], false);
}

async fn responses_anthropic_response(
    State(sender): State<mpsc::UnboundedSender<CapturedRequest>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    sender
        .send(CapturedRequest { headers, body })
        .expect("capture Responses request");
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"id":"resp_1","object":"response","status":"completed","output":[{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"output_text","text":"hello"}]},{"id":"fc_1","type":"function_call","call_id":"call_1","name":"Read","arguments":"{\"path\":\"a\"}"}],"usage":{"input_tokens":100,"output_tokens":8,"input_tokens_details":{"cached_tokens":40}}}"#,
        ))
        .expect("Responses response")
}

#[tokio::test]
async fn anthropic_messages_use_configured_responses_adapter() {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let upstream = TestServer::spawn(
        Router::new()
            .route("/v1/responses", post(responses_anthropic_response))
            .with_state(sender),
    )
    .await;
    let mut upstream_node = node("responses-node", &upstream, [("claude", "gpt-internal")]);
    upstream_node.provider.anthropic_protocol = AnthropicProtocol::Responses;
    let gateway = spawn_gateway(vec![upstream_node]).await;

    let response = test_client()
        .post(gateway.url("/v1/messages"))
        .header("anthropic-version", "2023-06-01")
        .header("anthropic-beta", "prompt-caching-2024-07-31")
        .json(&json!({
            "model":"claude", "max_tokens":512,
            "system":"be concise",
            "messages":[{"role":"user","content":"hello"}],
            "tools":[{"name":"Read","description":"read a file","input_schema":{"type":"object","properties":{"path":{"type":"string"}}}}]
        }))
        .send()
        .await
        .expect("Responses-backed Anthropic response");
    assert_eq!(response.status(), StatusCode::OK);
    let response: Value = response.json().await.expect("Anthropic response JSON");
    assert_eq!(response["model"], "claude");
    assert_eq!(
        response["content"][0],
        json!({"type":"text","text":"hello"})
    );
    assert_eq!(response["content"][1]["type"], "tool_use");
    assert_eq!(response["stop_reason"], "tool_use");
    assert_eq!(response["usage"]["input_tokens"], 60);
    assert_eq!(response["usage"]["cache_read_input_tokens"], 40);

    let captured = timeout(IO_TIMEOUT, receiver.recv())
        .await
        .expect("Responses capture timeout")
        .expect("Responses request missing");
    assert!(!captured.headers.contains_key("anthropic-version"));
    assert!(!captured.headers.contains_key("anthropic-beta"));
    let request: Value = serde_json::from_slice(&captured.body).expect("Responses JSON");
    assert_eq!(request["model"], "gpt-internal");
    assert_eq!(request["store"], false);
    assert_eq!(request["max_output_tokens"], 512);
    assert_eq!(request["instructions"], "be concise");
    assert_eq!(request["tools"][0]["type"], "function");
}

static ANTHROPIC_UPSTREAM_SSE: &[u8] = concat!(
    "data: {\"id\":\"chatcmpl_stream\",\"model\":\"internal-model\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"hi\"},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"chatcmpl_stream\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"Read\",\"arguments\":\"{\\\"path\\\":\"}}]},\"finish_reason\":null}]}\n\n",
    "data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"\\\"a\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\n",
    "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":30,\"completion_tokens\":5}}\n\n",
    "data: [DONE]\n\n"
)
.as_bytes();

async fn anthropic_upstream_sse() -> Response {
    let split = ANTHROPIC_UPSTREAM_SSE.len() / 4;
    let chunks = vec![
        Bytes::copy_from_slice(&ANTHROPIC_UPSTREAM_SSE[..split]),
        Bytes::copy_from_slice(&ANTHROPIC_UPSTREAM_SSE[split..split * 2]),
        Bytes::copy_from_slice(&ANTHROPIC_UPSTREAM_SSE[split * 2..split * 3]),
        Bytes::copy_from_slice(&ANTHROPIC_UPSTREAM_SSE[split * 3..]),
    ];
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/event-stream")
        .body(Body::from_stream(stream::iter(
            chunks.into_iter().map(Ok::<_, Infallible>),
        )))
        .expect("upstream Anthropic SSE")
}

#[tokio::test]
async fn anthropic_stream_converts_text_tools_and_event_order() {
    let upstream = TestServer::spawn(
        Router::new().route("/v1/chat/completions", post(anthropic_upstream_sse)),
    )
    .await;
    let gateway = spawn_gateway(vec![node(
        "anthropic-stream-node",
        &upstream,
        [("claude", "internal-model")],
    )])
    .await;
    let response = test_client()
        .post(gateway.url("/v1/messages"))
        .json(&json!({
            "model": "claude", "max_tokens": 128, "stream": true,
            "messages": [{"role": "user", "content": "hello"}]
        }))
        .send()
        .await
        .expect("Anthropic SSE response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("text/event-stream; charset=utf-8")
    );
    let stream = response.text().await.expect("Anthropic SSE body");
    let starts = stream.find("event: message_start").unwrap();
    let text = stream.find(r#""text":"hi""#).unwrap();
    let tool = stream.find(r#""type":"tool_use""#).unwrap();
    let stop = stream.find("event: message_stop").unwrap();
    assert!(starts < text && text < tool && tool < stop);
    assert!(stream.contains(r#""partial_json":"{\"path\":"#));
    assert!(stream.contains(r#""stop_reason":"tool_use""#));
    assert!(stream.ends_with("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"));
}

#[tokio::test]
async fn anthropic_validation_errors_use_anthropic_envelope() {
    let upstream = TestServer::spawn(Router::new()).await;
    let gateway = spawn_gateway(vec![node(
        "unused-anthropic-node",
        &upstream,
        [("claude", "claude")],
    )])
    .await;
    let response = test_client()
        .post(gateway.url("/v1/messages"))
        .json(&json!({
            "model": "claude",
            "messages": [{"role": "user", "content": "hello"}]
        }))
        .send()
        .await
        .expect("Anthropic validation response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(response.headers().contains_key("request-id"));
    assert_eq!(
        response.headers().get("request-id"),
        response.headers().get("x-request-id")
    );
    let body: Value = response.json().await.expect("Anthropic error JSON");
    assert_eq!(body["type"], "error");
    assert_eq!(body["error"]["type"], "invalid_request_error");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("max_tokens")
    );
}

#[tokio::test]
async fn oversized_anthropic_request_uses_anthropic_envelope() {
    let upstream = TestServer::spawn(Router::new()).await;
    let mut settings = gateway_settings(vec![node(
        "unused-anthropic-node",
        &upstream,
        [("claude", "claude")],
    )]);
    settings.server.max_request_body_bytes = 128;
    let gateway = Gateway::build(settings).expect("build limited Anthropic gateway");
    let gateway = TestServer::spawn(gateway.public_router()).await;
    let response = test_client()
        .post(gateway.url("/v1/messages"))
        .json(&json!({
            "model": "claude", "max_tokens": 16,
            "messages": [{"role": "user", "content": "x".repeat(512)}]
        }))
        .send()
        .await
        .expect("oversized Anthropic response");
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let body: Value = response.json().await.expect("Anthropic 413 JSON");
    assert_eq!(body["type"], "error");
    assert_eq!(body["error"]["type"], "invalid_request_error");
}

#[derive(Clone)]
struct NativeAnthropicCapture {
    messages: mpsc::UnboundedSender<Value>,
    counts: mpsc::UnboundedSender<Value>,
}

async fn vllm_version() -> Json<Value> {
    Json(json!({"version": "0.25.0"}))
}

async fn vllm_metrics() -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/plain; version=0.0.4")
        .body(Body::from(concat!(
            "# TYPE vllm:num_requests_running gauge\n",
            "vllm:num_requests_running 0\n",
            "# TYPE vllm:num_requests_waiting gauge\n",
            "vllm:num_requests_waiting 0\n",
            "# TYPE vllm:kv_cache_usage_perc gauge\n",
            "vllm:kv_cache_usage_perc 0\n"
        )))
        .expect("vLLM metrics response")
}

async fn native_anthropic_messages(
    State(state): State<NativeAnthropicCapture>,
    Json(body): Json<Value>,
) -> Json<Value> {
    state.messages.send(body).expect("capture native Messages");
    Json(json!({
        "id": "msg_native",
        "type": "message",
        "role": "assistant",
        "model": "internal-model",
        "content": [
            {"type": "thinking", "thinking": "reason", "signature": "native-signature"},
            {"type": "text", "text": "native response"}
        ],
        "stop_reason": "end_turn",
        "stop_sequence": null,
        "usage": {"input_tokens": 9, "output_tokens": 4},
        "future_native_field": true
    }))
}

async fn native_anthropic_count(
    State(state): State<NativeAnthropicCapture>,
    Json(body): Json<Value>,
) -> Json<Value> {
    state.counts.send(body).expect("capture native count");
    Json(json!({"input_tokens": 73, "context_management": {"original_input_tokens": 81}}))
}

async fn slow_native_anthropic_stream() -> Response {
    let body = async_stream::stream! {
        yield Ok::<_, Infallible>(Bytes::from_static(
            b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_slow\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"internal-model\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\n",
        ));
        sleep(Duration::from_millis(10_200)).await;
        yield Ok::<_, Infallible>(Bytes::from_static(
            b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
        ));
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/event-stream")
        .body(Body::from_stream(body))
        .expect("slow native Anthropic SSE")
}

#[tokio::test]
async fn native_anthropic_stream_injects_keepalive_ping() {
    let upstream =
        TestServer::spawn(Router::new().route("/v1/messages", post(slow_native_anthropic_stream)))
            .await;
    let mut native_node = node(
        "native-stream-node",
        &upstream,
        [("claude", "internal-model")],
    );
    native_node.provider.anthropic_protocol = AnthropicProtocol::Native;
    let gateway = spawn_gateway(vec![native_node]).await;
    let response = test_client()
        .post(gateway.url("/v1/messages"))
        .json(&json!({
            "model": "claude", "max_tokens": 16, "stream": true,
            "messages": [{"role": "user", "content": "hello"}]
        }))
        .send()
        .await
        .expect("native Anthropic SSE response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = timeout(Duration::from_secs(15), response.text())
        .await
        .expect("native Anthropic keepalive timeout")
        .expect("native Anthropic SSE body");
    let start = body
        .find("event: message_start")
        .unwrap_or_else(|| panic!("missing message_start in {body:?}"));
    let ping = body
        .find("event: ping")
        .unwrap_or_else(|| panic!("missing ping in {body:?}"));
    let stop = body
        .find("event: message_stop")
        .unwrap_or_else(|| panic!("missing message_stop in {body:?}"));
    assert!(start < ping && ping < stop);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn vllm_native_anthropic_supports_hello_messages_and_count_tokens() {
    let (message_sender, mut message_receiver) = mpsc::unbounded_channel();
    let (count_sender, mut count_receiver) = mpsc::unbounded_channel();
    let capture = NativeAnthropicCapture {
        messages: message_sender,
        counts: count_sender,
    };
    let upstream = TestServer::spawn(
        Router::new()
            .route("/version", axum::routing::get(vllm_version))
            .route("/metrics", axum::routing::get(vllm_metrics))
            .route(
                "/v1/models",
                axum::routing::get(|| async { Json(json!({"object": "list", "data": []})) }),
            )
            .route("/v1/messages", post(native_anthropic_messages))
            .route("/v1/messages/count_tokens", post(native_anthropic_count))
            .with_state(capture),
    )
    .await;
    let public = unused_address().await;
    let admin = unused_address().await;
    let mut vllm_node = node(
        "native-vllm",
        &upstream,
        [("claude-public", "internal-model")],
    );
    vllm_node.provider.kind = ProviderKind::Vllm;
    vllm_node.provider.monitor_interval_ms = 50;
    vllm_node.provider.request_timeout_ms = 500;
    vllm_node.provider.telemetry_stale_ms = 500;
    let mut settings = gateway_settings(vec![vllm_node]);
    settings.server.listen = public.to_string();
    settings.server.admin_listen = admin.to_string();
    settings.server.withdrawal_delay_ms = 1;
    let gateway = tokio::spawn(async move { Gateway::build(settings).unwrap().run().await });
    let client = test_client();
    wait_for_success(&client, &format!("http://{admin}/health/ready")).await;

    let hello = client
        .head(format!("http://{public}/api/hello"))
        .send()
        .await
        .expect("Claude Code hello response");
    assert_eq!(hello.status(), StatusCode::NO_CONTENT);

    let response = client
        .post(format!("http://{public}/v1/messages?beta=true"))
        .header("anthropic-version", "2023-06-01")
        .header("anthropic-beta", "future-claude-code-beta")
        .json(&json!({
            "model": "claude-public", "max_tokens": 4096, "stream": false,
            "system": [
                {"type": "text", "text": "x-anthropic-billing-header: cc_version=2.1.220.8a5; cc_entrypoint=sdk-cli;"},
                {"type": "text", "text": "system"}
            ],
            "messages": [
                {"role": "system", "content": "inline native system"},
                {"role": "user", "content": "hello"}
            ],
            "thinking": {"type": "enabled", "budget_tokens": 2048, "display": "omitted"},
            "context_management": {
                "edits": [{"type": "clear_thinking_20251015", "keep": "all"}]
            },
            "output_config": {"effort": "high"},
            "future_claude_code_field": {"preserved": true}
        }))
        .send()
        .await
        .expect("native Messages response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-estuary-thinking-budget").unwrap(),
        "approximated-by-max-tokens"
    );
    let response: Value = response.json().await.expect("native Messages JSON");
    assert_eq!(response["model"], "claude-public");
    assert_eq!(response["content"][0]["thinking"], "reason");
    assert_eq!(response["content"][0]["signature"], "native-signature");
    assert_eq!(response["future_native_field"], true);
    let captured = timeout(IO_TIMEOUT, message_receiver.recv())
        .await
        .expect("native Messages capture timeout")
        .expect("native Messages missing");
    assert_eq!(captured["model"], "internal-model");
    assert_eq!(captured["thinking"]["type"], "enabled");
    assert!(captured["thinking"].get("display").is_none());
    assert_eq!(captured["chat_template_kwargs"]["enable_thinking"], true);
    assert!(captured.get("context_management").is_none());
    assert_eq!(captured["future_claude_code_field"]["preserved"], true);
    assert_eq!(captured["system"].as_array().unwrap().len(), 1);
    assert_eq!(captured["messages"][0]["role"], "system");

    let count = client
        .post(format!(
            "http://{public}/v1/messages/count_tokens?beta=true"
        ))
        .json(&json!({
            "model": "claude-public",
            "system": "system",
            "messages": [{"role": "user", "content": [
                {"type": "tool_reference", "tool_name": "deferred_tool"},
                {"type": "text", "text": "count"}
            ]}]
        }))
        .send()
        .await
        .expect("native token count response");
    assert_eq!(count.status(), StatusCode::OK);
    let count: Value = count.json().await.expect("native count JSON");
    assert_eq!(count["input_tokens"], 73);
    assert_eq!(count["context_management"]["original_input_tokens"], 81);
    let captured = timeout(IO_TIMEOUT, count_receiver.recv())
        .await
        .expect("native count capture timeout")
        .expect("native count missing");
    assert_eq!(captured["model"], "internal-model");
    assert_eq!(
        captured["messages"][0]["content"][0]["type"],
        "tool_reference"
    );

    let file = client
        .get(format!("http://{public}/v1/files/file_capture/content"))
        .send()
        .await
        .expect("Claude Code file error response");
    assert_eq!(file.status(), StatusCode::BAD_REQUEST);
    let file: Value = file.json().await.expect("Anthropic file error JSON");
    assert_eq!(file["type"], "error");
    assert_eq!(file["error"]["type"], "invalid_request_error");
    assert!(
        file["error"]["message"]
            .as_str()
            .unwrap()
            .contains("file service")
    );

    gateway.abort();
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

#[derive(Clone)]
struct CodexCapture {
    sender: mpsc::UnboundedSender<Value>,
}

async fn codex_namespace_response(
    State(capture): State<CodexCapture>,
    Json(body): Json<Value>,
) -> Json<Value> {
    capture.sender.send(body).expect("capture Codex request");
    Json(json!({
        "id": "resp_codex_1",
        "object": "response",
        "status": "completed",
        "model": "gpt-oss-internal",
        "output": [{
            "id": "fc_1",
            "type": "function_call",
            "status": "completed",
            "call_id": "call_2",
            "name": "multi_agent_v1__spawn_agent",
            "arguments": "{}"
        }]
    }))
}

fn codex_namespace_request(stream: bool) -> Value {
    json!({
        "model": "gpt-oss-public",
        "stream": stream,
        "store": false,
        "client_metadata": {
            "x-codex-installation-id": "00000000-0000-0000-0000-000000000000"
        },
        "tools": [{
            "type": "function",
            "name": "exec_command",
            "description": "Run a command.",
            "parameters": {"type": "object"}
        }, {
            "type": "namespace",
            "name": "multi_agent_v1",
            "description": "Manage workers.",
            "tools": [{
                "type": "function",
                "name": "spawn_agent",
                "description": "Start a worker.",
                "parameters": {"type": "object"}
            }]
        }],
        "input": [{
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "delegate"}]
        }, {
            "type": "function_call",
            "id": "fc_previous",
            "call_id": "call_previous",
            "namespace": "multi_agent_v1",
            "name": "spawn_agent",
            "arguments": "{}"
        }, {
            "type": "function_call_output",
            "call_id": "call_previous",
            "output": "done"
        }]
    })
}

fn codex_vllm_node(upstream: &TestServer) -> NodeConfig {
    node(
        "codex-vllm",
        upstream,
        [("gpt-oss-public", "gpt-oss-internal")],
    )
}

#[tokio::test]
async fn codex_namespace_tools_round_trip_through_vllm_responses() {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let upstream = TestServer::spawn(
        Router::new()
            .route("/version", axum::routing::get(vllm_version))
            .route("/metrics", axum::routing::get(vllm_metrics))
            .route(
                "/v1/models",
                axum::routing::get(|| async {
                    Json(json!({"object": "list", "data": [{"id": "gpt-oss-internal"}]}))
                }),
            )
            .route("/v1/responses", post(codex_namespace_response))
            .with_state(CodexCapture { sender }),
    )
    .await;
    let gateway = spawn_vllm_gateway(codex_vllm_node(&upstream)).await;

    let response = test_client()
        .post(gateway.url("/v1/responses"))
        .header("user-agent", "codex_exec/0.146.0")
        .json(&codex_namespace_request(false))
        .send()
        .await
        .expect("Codex response");
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get("content-length").is_none());
    let response: Value = response.json().await.expect("Codex response JSON");
    assert_eq!(response["output"][0]["namespace"], "multi_agent_v1");
    assert_eq!(response["output"][0]["name"], "spawn_agent");

    let upstream_body = receiver.recv().await.expect("upstream Codex request");
    assert_eq!(upstream_body["model"], "gpt-oss-internal");
    assert_eq!(upstream_body["tools"][0]["name"], "exec_command");
    assert_eq!(
        upstream_body["tools"][1]["name"],
        "multi_agent_v1__spawn_agent"
    );
    assert_eq!(
        upstream_body["input"][1]["name"],
        "multi_agent_v1__spawn_agent"
    );
    assert!(upstream_body["input"][1].get("namespace").is_none());
}

async fn codex_namespace_sse(Json(_body): Json<Value>) -> Response {
    static SSE: &[u8] = concat!(
        "event: response.output_item.added\r\n",
        "data: {\"type\":\"response.output_item.added\",\"sequence_number\":1,\"output_index\":0,\"item\":{\"id\":\"fc_1\",\"type\":\"function_call\",\"name\":\"multi_agent_v1__spawn_agent\",\"call_id\":\"call_1\",\"arguments\":\"\"}}\r\n\r\n",
        "event: response.output_item.done\r\n",
        "data: {\"type\":\"response.output_item.done\",\"sequence_number\":2,\"output_index\":0,\"item\":{\"id\":\"fc_1\",\"type\":\"function_call\",\"name\":\"multi_agent_v1__spawn_agent\",\"call_id\":\"call_1\",\"arguments\":\"{}\"}}\r\n\r\n",
        "event: response.completed\r\n",
        "data: {\"type\":\"response.completed\",\"sequence_number\":3,\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"output\":[{\"id\":\"fc_1\",\"type\":\"function_call\",\"name\":\"multi_agent_v1__spawn_agent\",\"call_id\":\"call_1\",\"arguments\":\"{}\"}]}}\r\n\r\n",
        "data: [DONE]\r\n\r\n"
    )
    .as_bytes();
    let chunks = SSE
        .chunks(17)
        .map(Bytes::copy_from_slice)
        .map(Ok::<_, Infallible>);
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/event-stream")
        .body(Body::from_stream(stream::iter(chunks)))
        .expect("Codex SSE response")
}

#[tokio::test]
async fn codex_namespace_sse_is_restored_across_upstream_chunks() {
    let upstream = TestServer::spawn(
        Router::new()
            .route("/version", axum::routing::get(vllm_version))
            .route("/metrics", axum::routing::get(vllm_metrics))
            .route(
                "/v1/models",
                axum::routing::get(|| async {
                    Json(json!({"object": "list", "data": [{"id": "gpt-oss-internal"}]}))
                }),
            )
            .route("/v1/responses", post(codex_namespace_sse)),
    )
    .await;
    let gateway = spawn_vllm_gateway(codex_vllm_node(&upstream)).await;
    let response = test_client()
        .post(gateway.url("/v1/responses"))
        .header("user-agent", "codex_exec/0.146.0")
        .json(&codex_namespace_request(true))
        .send()
        .await
        .expect("Codex SSE response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("x-accel-buffering")
            .and_then(|value| value.to_str().ok()),
        Some("no")
    );
    let body = response.text().await.expect("Codex SSE body");
    assert!(!body.contains("multi_agent_v1__spawn_agent"));
    assert_eq!(body.matches("\"namespace\":\"multi_agent_v1\"").count(), 3);
    assert_eq!(body.matches("\"name\":\"spawn_agent\"").count(), 3);
    assert!(body.contains("data: [DONE]"));
}

#[tokio::test]
async fn codex_vllm_incompatible_tools_return_actionable_400() {
    let upstream = TestServer::spawn(
        Router::new()
            .route("/version", axum::routing::get(vllm_version))
            .route("/metrics", axum::routing::get(vllm_metrics))
            .route(
                "/v1/models",
                axum::routing::get(|| async {
                    Json(json!({"object": "list", "data": [{"id": "gpt-oss-internal"}]}))
                }),
            ),
    )
    .await;
    let gateway = spawn_vllm_gateway(codex_vllm_node(&upstream)).await;
    for (request, expected) in [
        (
            json!({
                "model": "gpt-oss-public",
                "stream": true,
                "input": "search",
                "tools": [{"type": "web_search"}]
            }),
            "web_search = \"disabled\"",
        ),
        (
            json!({
                "model": "gpt-oss-public",
                "stream": true,
                "input": [{
                    "type": "additional_tools",
                    "role": "developer",
                    "tools": []
                }]
            }),
            "use_responses_lite: false",
        ),
    ] {
        let response = test_client()
            .post(gateway.url("/v1/responses"))
            .header("user-agent", "codex_exec/0.146.0")
            .json(&request)
            .send()
            .await
            .expect("Codex compatibility error");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body: Value = response.json().await.expect("OpenAI error JSON");
        assert_eq!(body["error"]["type"], "invalid_request_error");
        assert_eq!(body["error"]["code"], "invalid_request");
        assert!(
            body["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains(expected)),
            "{body}"
        );
    }
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
