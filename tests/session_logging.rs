use axum::{
    Json, Router,
    body::Body,
    http::{StatusCode, header},
    response::IntoResponse,
    routing::post,
};
use estuary::{
    Gateway, Settings,
    config::{NodeConfig, SessionLogConfig},
};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct Server {
    url: String,
    task: tokio::task::JoinHandle<()>,
}
impl Server {
    async fn spawn(router: Router) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self { url, task }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
struct Database(PathBuf);
impl Database {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("estuary-logs-{}.sqlite", uuid::Uuid::now_v7())))
    }
}
impl Drop for Database {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
        }
    }
}
fn client() -> reqwest::Client {
    reqwest::Client::builder().no_proxy().build().unwrap()
}
fn settings(path: &Database, nodes: Vec<NodeConfig>) -> Settings {
    let mut settings = Settings {
        nodes,
        session_log: SessionLogConfig {
            database: Some(path.0.clone()),
            ..SessionLogConfig::default()
        },
        ..Settings::default()
    };
    settings.health.route_while_starting = true;
    settings.retry.max_attempts = 2;
    settings
}
fn node(id: &str, url: &str) -> NodeConfig {
    NodeConfig {
        id: id.to_owned(),
        base_url: format!("{url}/v1"),
        models: std::collections::HashMap::from([("m".to_owned(), "upstream-m".to_owned())]),
        ..NodeConfig::default()
    }
}
async fn logged_gateway(
    settings: Settings,
) -> (Server, Server, Arc<estuary::session_log::LogSink>) {
    let gateway = Gateway::build(settings).unwrap();
    let logs = gateway.session_logs();
    assert!(logs.flush(Duration::from_secs(3)).await);
    (
        Server::spawn(gateway.public_router()).await,
        Server::spawn(gateway.admin_router()).await,
        logs,
    )
}

#[tokio::test]
async fn roundtrip_deduplicates_history_without_merging_requests_or_protocols() {
    let upstream=Server::spawn(Router::new().route("/v1/chat/completions",post(|| async {Json(json!({"id":"c","choices":[{"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":100,"completion_tokens":3,"prompt_tokens_details":{"cached_tokens":80}}}))}))).await;
    let db = Database::new();
    let (public, admin, logs) = logged_gateway(settings(&db, vec![node("n", &upstream.url)])).await;
    let mut messages = vec![json!({"role":"system","content":"stable instructions ".repeat(512)})];
    for i in 0..20 {
        messages.push(json!({"role":"user","content":format!("turn {i}")}));
        let response = client()
            .post(format!("{}/v1/chat/completions", public.url))
            .header("x-request-id", "reused")
            .header("x-estuary-session-id", "agent-1")
            .json(&json!({"model":"m","messages":messages}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let _: Value = response.json().await.unwrap();
    }
    assert!(logs.flush(Duration::from_secs(3)).await);
    let page = logs
        .list(Some("agent-1".to_owned()), None, 0, 100)
        .await
        .unwrap();
    assert_eq!(page.requests.len(), 20);
    let request = &page.requests[0];
    assert_eq!(request.external_request_id, "reused");
    assert_eq!(request.outcome, "success");
    assert_eq!(request.usage["cache_read_tokens"], 80);
    let detail = logs.detail(request.id.clone()).await.unwrap().unwrap();
    assert_eq!(detail.request.attempts.len(), 1);
    assert_eq!(detail.request.attempts[0].adapter, "passthrough");
    let input = detail
        .payloads
        .iter()
        .find(|p| p.stage == "client_input")
        .unwrap();
    assert_eq!(input.content["messages"], Value::Array(messages));
    let mapped = detail
        .payloads
        .iter()
        .find(|p| p.stage == "upstream_input")
        .unwrap();
    assert_eq!(mapped.content["model"], "upstream-m");
    let connection = rusqlite::Connection::open(&db.0).unwrap();
    let stored: u64 = connection
        .query_row("SELECT sum(stored_bytes) FROM content_blobs", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert!(stored < 20_000, "repeated prompts expanded to {stored}");
    let sequences: u64 = connection
        .query_row("SELECT count(*) FROM sequence_nodes", [], |r| r.get(0))
        .unwrap();
    assert!(
        sequences < 100,
        "histories did not share sequence prefixes: {sequences}"
    );
    let first = logs.list(None, None, 0, 7).await.unwrap();
    let next = logs.list(None, first.next_cursor, 0, 100).await.unwrap();
    assert_eq!(first.requests.len() + next.requests.len(), 20);
    let sessions = client()
        .get(format!("{}/admin/api/logs/sessions?since=0", admin.url))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(sessions["sessions"][0]["requests"], 20);
}

#[tokio::test]
async fn retry_keeps_failed_attempt_and_final_success() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&calls);
    let bad = Server::spawn(Router::new().route(
        "/v1/chat/completions",
        post(move || {
            let count = Arc::clone(&count);
            async move {
                count.fetch_add(1, Ordering::Relaxed);
                StatusCode::SERVICE_UNAVAILABLE
            }
        }),
    ))
    .await;
    let good = Server::spawn(Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            Json(json!({"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":0},"metrics":{"queue_time_ms":2.5}}))
        }),
    ))
    .await;
    let db = Database::new();
    let (public, _, logs) = logged_gateway(settings(
        &db,
        vec![node("a-bad", &bad.url), node("z-good", &good.url)],
    ))
    .await;
    let response = client()
        .post(format!("{}/v1/chat/completions", public.url))
        .json(&json!({"model":"m","messages":[]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let _ = response.bytes().await.unwrap();
    assert!(logs.flush(Duration::from_secs(3)).await);
    let page = logs.list(None, None, 0, 10).await.unwrap();
    let detail = logs
        .detail(page.requests[0].id.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(detail.request.outcome, "success");
    assert_eq!(detail.request.error_phase, None);
    assert_eq!(detail.request.error_class, None);
    assert_eq!(detail.request.attempts.len(), 2);
    assert_eq!(detail.request.attempts[0].http_status, Some(503));
    assert_eq!(
        detail.request.attempts[0].retry_reason.as_deref(),
        Some("503")
    );
    assert_eq!(detail.request.attempts[1].outcome, "success");
    assert!(
        !detail.request.attempts[0]
            .timings_us
            .contains_key("engine_queue_time")
    );
    assert_eq!(
        detail.request.attempts[1].timings_us["engine_queue_time"],
        2_500
    );
}

#[tokio::test]
async fn streaming_end_and_usage_are_observed_without_chunk_rows() {
    let sse = "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2}}\n\ndata: [DONE]\n\n";
    let upstream = Server::spawn(Router::new().route(
        "/v1/chat/completions",
        post(move || async move {
            (
                [(header::CONTENT_TYPE, "text/event-stream")],
                Body::from(sse),
            )
                .into_response()
        }),
    ))
    .await;
    let db = Database::new();
    let (public, _, logs) = logged_gateway(settings(&db, vec![node("n", &upstream.url)])).await;
    let response = client()
        .post(format!("{}/v1/chat/completions", public.url))
        .json(&json!({"model":"m","stream":true,"messages":[]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.text().await.unwrap(), sse);
    assert!(logs.flush(Duration::from_secs(3)).await);
    let page = logs.list(None, None, 0, 10).await.unwrap();
    let detail = logs
        .detail(page.requests[0].id.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(detail.request.outcome, "success");
    assert_eq!(detail.request.usage["output_tokens"], 2);
    assert!(detail.request.timings_us.contains_key("first_output"));
    assert_eq!(detail.request.attempts[0].outcome, "success");
    let payload = detail
        .payloads
        .iter()
        .find(|p| p.stage == "client_output")
        .unwrap();
    assert_eq!(payload.state, "summary");
    assert_eq!(payload.content["blocks"][0]["content"], "hi");
}

#[tokio::test]
async fn missing_stream_terminal_is_logged_even_after_http_200() {
    let upstream = Server::spawn(Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            (
                [(header::CONTENT_TYPE, "text/event-stream")],
                "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n",
            )
        }),
    ))
    .await;
    let db = Database::new();
    let (public, _, logs) = logged_gateway(settings(&db, vec![node("n", &upstream.url)])).await;
    let response = client()
        .post(format!("{}/v1/chat/completions", public.url))
        .json(&json!({"model":"m","stream":true,"messages":[]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let _ = response.bytes().await.unwrap();
    assert!(logs.flush(Duration::from_secs(3)).await);
    let page = logs.list(None, None, 0, 10).await.unwrap();
    assert_eq!(page.requests[0].http_status, Some(200));
    assert_eq!(
        page.requests[0].error_class.as_deref(),
        Some("missing_terminal_marker")
    );
}

#[tokio::test]
async fn disabled_content_still_records_usage_and_unavailable_database_does_not_break_proxy() {
    let upstream = Server::spawn(Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            Json(json!({"choices":[],"usage":{"prompt_tokens":3,"completion_tokens":1}}))
        }),
    ))
    .await;
    let db = Database::new();
    let mut config = settings(&db, vec![node("n", &upstream.url)]);
    config.session_log.capture_content = false;
    let (public, _, logs) = logged_gateway(config).await;
    let _ = client()
        .post(format!("{}/v1/chat/completions", public.url))
        .json(&json!({"model":"m","messages":[]}))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert!(logs.flush(Duration::from_secs(3)).await);
    let page = logs.list(None, None, 0, 10).await.unwrap();
    let detail = logs
        .detail(page.requests[0].id.clone())
        .await
        .unwrap()
        .unwrap();
    assert!(detail.payloads.is_empty());
    assert_eq!(detail.request.capture_state, "metadata_only");
    assert_eq!(detail.request.usage["input_tokens"], 3);
    let mut config = settings(&db, vec![node("n", &upstream.url)]);
    config.session_log.database = Some(std::env::temp_dir());
    let gateway = Gateway::build(config).unwrap();
    let logs = gateway.session_logs();
    let public = Server::spawn(gateway.public_router()).await;
    let response = client()
        .post(format!("{}/v1/chat/completions", public.url))
        .json(&json!({"model":"m","messages":[]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let _ = response.bytes().await.unwrap();
    assert!(!logs.flush(Duration::from_secs(3)).await);
    assert!(logs.status().write_errors > 0);
}

fn generic_protocol_upstream() -> Router {
    async fn chat(Json(body): Json<Value>) -> axum::response::Response {
        assert_eq!(body["model"], "upstream-m");
        assert!(body["messages"].is_array());
        let usage = json!({"prompt_tokens":12,"completion_tokens":3});
        if body["stream"] == true {
            ([(header::CONTENT_TYPE,"text/event-stream")],format!("data: {}\n\ndata: {}\n\ndata: [DONE]\n\n", json!({"choices":[{"index":0,"delta":{"content":"hello"},"finish_reason":"stop"}]}),json!({"choices":[],"usage":usage}))).into_response()
        } else {
            Json(json!({"id":"upstream","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],"usage":usage})).into_response()
        }
    }
    async fn responses(Json(body): Json<Value>) -> axum::response::Response {
        assert_eq!(body["model"], "upstream-m");
        assert_eq!(body["input"], "hello");
        let response = json!({"id":"upstream", "object":"response", "status":"completed", "model":"upstream-m", "output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"hello"}]}], "usage":{"input_tokens":12,"output_tokens":3,"input_tokens_details":{"cached_tokens":4,"cache_write_tokens":2}}, "metrics":{"time_to_first_token_ms":12.5,"generation_time_ms":30,"queue_time_ms":0,"mean_itl_ms":1.25}});
        if body["stream"] == true {
            (
                [(header::CONTENT_TYPE, "text/event-stream")],
                format!(
                    "event: response.completed\ndata: {}\n\n",
                    json!({"type":"response.completed", "response":response})
                ),
            )
                .into_response()
        } else {
            Json(response).into_response()
        }
    }
    Router::new()
        .route("/v1/chat/completions", post(chat))
        .route("/v1/responses", post(responses))
}

#[tokio::test]
async fn records_client_protocols_and_generic_upstream_paths() {
    use estuary::config::AnthropicProtocol;
    let upstream = Server::spawn(generic_protocol_upstream()).await;
    let db = Database::new();
    let mut config = node("openai", &upstream.url);
    config.provider.anthropic_protocol = AnthropicProtocol::Chat;
    let (public, _, logs) = logged_gateway(settings(&db, vec![config])).await;
    for endpoint in ["messages", "responses"] {
        for streaming in [false, true] {
            let body = if endpoint == "messages" {
                json!({"model":"m","max_tokens":128,"stream":streaming,"messages":[{"role":"user","content":"hello"}]})
            } else {
                json!({"model":"m","stream":streaming,"input":"hello"})
            };
            let response = client()
                .post(format!("{}/v1/{endpoint}", public.url))
                .header(
                    "user-agent",
                    if endpoint == "messages" {
                        "codex_cli_rs/1.0"
                    } else {
                        "claude-code/1.0"
                    },
                )
                .json(&body)
                .send()
                .await
                .unwrap();
            let status = response.status();
            let text = response.text().await.unwrap();
            assert_eq!(status, StatusCode::OK, "{text}");
            if streaming {
                assert!(
                    text.contains(if endpoint == "messages" {
                        "message_stop"
                    } else {
                        "response.completed"
                    }),
                    "{text}"
                );
            } else {
                let value: Value = serde_json::from_str(&text).unwrap();
                assert_eq!(
                    value["type"].as_str() == Some("message"),
                    endpoint == "messages"
                );
            }
        }
    }
    assert!(logs.flush(Duration::from_secs(3)).await);
    let page = logs.list(None, None, 0, 10).await.unwrap();
    assert_eq!(page.requests.len(), 4);
    for request in page.requests {
        let detail = logs.detail(request.id).await.unwrap().unwrap();
        assert_eq!(detail.request.outcome, "success");
        assert_eq!(
            detail.request.attempts[0].adapter,
            if detail.request.protocol == "openai_responses" {
                "passthrough"
            } else {
                "chat_to_anthropic"
            }
        );
        assert_eq!(
            detail.request.attempts[0].endpoint,
            if detail.request.protocol == "openai_responses" {
                "responses"
            } else {
                "chat/completions"
            }
        );
        assert_eq!(detail.request.usage["input_tokens"], 12);
        assert_eq!(detail.request.usage["output_tokens"], 3);
        let payload = detail
            .payloads
            .iter()
            .find(|p| p.stage == "client_output")
            .unwrap();
        if detail.request.protocol == "openai_responses" {
            assert_eq!(payload.content["object"], "response");
            assert!(payload.content.get("content").is_none());
        } else if !detail.request.streaming {
            assert_eq!(payload.content["type"], "message");
        }
    }
}

#[tokio::test]
async fn vllm_cache_creation_is_logged_with_and_without_content_capture() {
    let upstream = Server::spawn(Router::new().route("/v1/chat/completions", post(|| async {
        Json(json!({"choices":[{"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":100,"completion_tokens":4,"prompt_tokens_details":{"cached_tokens":40,"created_cache_tokens":32}}}))
    }))).await;
    for capture_content in [true, false] {
        let db = Database::new();
        let mut config = settings(&db, vec![node("n", &upstream.url)]);
        config.session_log.capture_content = capture_content;
        let (public, _, logs) = logged_gateway(config).await;
        let response = client()
            .post(format!("{}/v1/chat/completions", public.url))
            .json(&json!({"model":"m","messages":[{"role":"user","content":"hello"}]}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        response.bytes().await.unwrap();
        assert!(logs.flush(Duration::from_secs(3)).await);
        let page = logs.list(None, None, 0, 10).await.unwrap();
        let usage = &page.requests[0].usage;
        assert_eq!(usage["input_tokens"], 100);
        assert_eq!(usage["cache_read_tokens"], 40);
        assert_eq!(usage["cache_write_tokens"], 32);
        assert_eq!(usage["output_tokens"], 4);
    }
}

#[tokio::test]
async fn responses_engine_timings_and_cache_writes_survive_disabled_or_partial_capture() {
    let upstream = Server::spawn(generic_protocol_upstream()).await;
    for (capture_content, limit) in [(true, 2048), (false, 2048), (true, 64)] {
        let db = Database::new();
        let mut config = settings(&db, vec![node("n", &upstream.url)]);
        config.session_log.capture_content = capture_content;
        config.session_log.max_payload_bytes = limit;
        let (public, _, logs) = logged_gateway(config).await;
        for streaming in [false, true] {
            let response = client()
                .post(format!("{}/v1/responses", public.url))
                .json(&json!({"model":"m","stream":streaming,"input":"hello"}))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let text = response.text().await.unwrap();
            // Observation must preserve the client-visible protocol and metrics.
            let value: Value = if streaming {
                serde_json::from_str(
                    text.lines()
                        .find_map(|line| line.strip_prefix("data: "))
                        .unwrap(),
                )
                .unwrap()
            } else {
                serde_json::from_str(&text).unwrap()
            };
            let response = if streaming {
                &value["response"]
            } else {
                &value
            };
            assert_eq!(response["metrics"]["time_to_first_token_ms"], 12.5);
            assert_eq!(
                response["usage"]["input_tokens_details"]["cache_write_tokens"],
                2
            );
        }
        assert!(logs.flush(Duration::from_secs(3)).await);
        let page = logs.list(None, None, 0, 10).await.unwrap();
        assert_eq!(page.requests.len(), 2);
        for request in page.requests {
            let detail = logs.detail(request.id).await.unwrap().unwrap();
            assert_eq!(detail.request.outcome, "success");
            assert_eq!(detail.request.usage["input_tokens"], 12);
            assert_eq!(detail.request.usage["cache_read_tokens"], 4);
            assert_eq!(detail.request.usage["cache_write_tokens"], 2);
            let timings = &detail.request.attempts[0].timings_us;
            assert!(timings.contains_key("total"));
            assert_eq!(timings["engine_time_to_first_token"], 12_500);
            assert_eq!(timings["engine_generation_time"], 30_000);
            assert_eq!(timings["engine_queue_time"], 0);
            assert_eq!(timings["engine_mean_itl"], 1_250);
            if !capture_content {
                assert!(detail.payloads.is_empty());
            } else if limit == 64 {
                assert_eq!(detail.request.capture_state, "partial");
            } else {
                let payload = detail
                    .payloads
                    .iter()
                    .find(|p| p.stage == "client_output")
                    .unwrap();
                assert_eq!(
                    payload.content["usage"]["input_tokens_details"]["cache_write_tokens"],
                    2
                );
            }
        }
    }
}

#[tokio::test]
async fn streaming_request_errors_keep_the_requested_mode() {
    let upstream = Server::spawn(Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            (
                StatusCode::BAD_REQUEST,
                Json(json!({"error":{"message":"invalid request"}})),
            )
        }),
    ))
    .await;
    let db = Database::new();
    let (public, _, logs) = logged_gateway(settings(&db, vec![node("n", &upstream.url)])).await;
    for streaming in [false, true] {
        let response = client()
            .post(format!("{}/v1/chat/completions", public.url))
            .json(&json!({"model":"m","stream":streaming,"messages":[{"role":"user","content":"hello"}]}))
            .send().await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        response.bytes().await.unwrap();
        assert!(logs.flush(Duration::from_secs(3)).await);
        let page = logs.list(None, None, 0, 10).await.unwrap();
        let request = &page.requests[0];
        assert_eq!(request.streaming, streaming);
        assert_eq!(request.outcome, "error");
        assert_eq!(request.http_status, Some(400));
        assert_eq!(request.error_phase.as_deref(), Some("upstream"));
        assert_eq!(request.error_class.as_deref(), Some("upstream_status"));
    }
}

#[tokio::test]
async fn logs_require_admin_auth_and_partial_capture_does_not_change_inference() {
    let upstream = Server::spawn(Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            Json(json!({"choices":[],"usage":{"prompt_tokens":7,"completion_tokens":0}}))
        }),
    ))
    .await;
    let db = Database::new();
    let mut config = settings(&db, vec![node("n", &upstream.url)]);
    config.server.admin_token = Some("admin-test-token".to_owned());
    config.session_log.max_payload_bytes = 64;
    config.session_log.max_content_bytes = 128;
    let (public, admin, logs) = logged_gateway(config).await;
    let response=client().post(format!("{}/v1/chat/completions",public.url)).json(&json!({"model":"m","messages":[{"role":"user","content":"large context".repeat(100)}]})).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.json::<Value>().await.unwrap()["usage"]["prompt_tokens"],
        7
    );
    assert!(logs.flush(Duration::from_secs(3)).await);
    for path in ["logs/status", "logs/requests", "logs/sessions"] {
        let url = format!("{}/admin/api/{path}", admin.url);
        assert_eq!(
            client().get(&url).send().await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            client()
                .get(&url)
                .bearer_auth("admin-test-token")
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }
    let request = logs
        .list(None, None, 0, 10)
        .await
        .unwrap()
        .requests
        .pop()
        .unwrap();
    assert_eq!(request.capture_state, "partial");
    assert_eq!(request.usage["input_tokens"], 7);
    let detail = logs.detail(request.id.clone()).await.unwrap().unwrap();
    assert!(detail.payloads.iter().any(|p| p.state == "partial"));
    let url = format!("{}/admin/api/logs/requests/{}", admin.url, request.id);
    assert_eq!(
        client().get(url).send().await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let url = format!("{}/admin/api/logs/requests?cursor=wrong:value", admin.url);
    assert_eq!(
        client()
            .get(url)
            .bearer_auth("admin-test-token")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    let metrics = client()
        .get(format!("{}/metrics", admin.url))
        .bearer_auth("admin-test-token")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains("estuary_session_log_committed_total 1\n"));
    assert!(metrics.ends_with("# EOF\n"));
    assert_eq!(metrics.matches("# EOF").count(), 1);
}

#[tokio::test]
async fn client_disconnect_finalizes_both_request_and_attempt() {
    use futures_util::StreamExt;
    let upstream=Server::spawn(Router::new().route("/v1/chat/completions",post(||async{
        let body=async_stream::stream! {
            yield Ok::<_,std::io::Error>(bytes::Bytes::from_static(b"data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n"));
            std::future::pending::<()>().await;
        };
        ([(header::CONTENT_TYPE,"text/event-stream")],Body::from_stream(body))
    }))).await;
    let db = Database::new();
    let (public, _, logs) = logged_gateway(settings(&db, vec![node("n", &upstream.url)])).await;
    let response = client()
        .post(format!("{}/v1/chat/completions", public.url))
        .json(&json!({"model":"m","stream":true,"messages":[]}))
        .send()
        .await
        .unwrap();
    let mut body = response.bytes_stream();
    assert!(body.next().await.unwrap().is_ok());
    drop(body);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            assert!(logs.flush(Duration::from_secs(1)).await);
            let page = logs.list(None, None, 0, 10).await.unwrap();
            if page
                .requests
                .first()
                .is_some_and(|r| r.ended_at_ms.is_some())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let request = logs
        .list(None, None, 0, 10)
        .await
        .unwrap()
        .requests
        .pop()
        .unwrap();
    let detail = logs.detail(request.id).await.unwrap().unwrap();
    assert_eq!(detail.request.outcome, "cancelled");
    assert_eq!(detail.request.delivery, "body_dropped");
    assert_eq!(detail.request.attempts[0].outcome, "cancelled");
}

#[tokio::test]
async fn partial_stream_capture_preserves_late_usage_and_unknown_streams_remain_unknown() {
    let streamed = format!(
        "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
        json!({"choices":[{"delta":{"content":"long answer".repeat(100)}}]}),
        json!({"usage":{"prompt_tokens":10,"completion_tokens":2}})
    );
    let expected = streamed.clone();
    let upstream = Server::spawn(
        Router::new()
            .route(
                "/v1/chat/completions",
                post(move || {
                    let text = streamed.clone();
                    async move { ([(header::CONTENT_TYPE, "text/event-stream")], text) }
                }),
            )
            .route(
                "/v1/embeddings",
                post(|| async {
                    (
                        [(header::CONTENT_TYPE, "text/event-stream")],
                        "data: {\"vendor\":\"opaque\"}\n\n",
                    )
                }),
            )
            .route("/v1/completions", post(|| async { StatusCode::NO_CONTENT })),
    )
    .await;
    let db = Database::new();
    let mut config = settings(&db, vec![node("n", &upstream.url)]);
    config.session_log.max_payload_bytes = 64;
    let (public, _, logs) = logged_gateway(config).await;
    for endpoint in ["chat/completions", "embeddings", "completions"] {
        let response = client()
            .post(format!("{}/v1/{endpoint}", public.url))
            .json(&json!({"model":"m","stream":true,"messages":[]}))
            .send()
            .await
            .unwrap();
        let status = response.status();
        let text = response.text().await.unwrap();
        if endpoint == "chat/completions" {
            assert_eq!(text, expected);
        }
        if endpoint == "completions" {
            assert_eq!(status, StatusCode::NO_CONTENT);
        }
    }
    assert!(logs.flush(Duration::from_secs(3)).await);
    let rows = logs.list(None, None, 0, 10).await.unwrap();
    assert_eq!(rows.requests.len(), 3);
    for row in rows.requests {
        assert_eq!(row.delivery, "body_consumed");
        match row.endpoint.as_str() {
            "/v1/chat/completions" => {
                assert_eq!(row.outcome, "success");
                assert_eq!(row.capture_state, "partial");
                assert_eq!(row.usage["input_tokens"], 10);
                assert_eq!(row.usage["output_tokens"], 2);
            }
            "/v1/embeddings" => {
                assert_eq!(row.outcome, "unknown");
                assert_eq!(
                    row.error_class.as_deref(),
                    Some("unrecognized_stream_completion")
                );
            }
            "/v1/completions" => assert_eq!(row.outcome, "success"),
            endpoint => panic!("unexpected endpoint {endpoint}"),
        }
    }
}
