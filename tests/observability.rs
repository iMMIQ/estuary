use std::{collections::HashMap, sync::Arc, time::Duration};

use axum::{
    Json, Router,
    body::Body,
    http::{StatusCode, header},
    response::IntoResponse,
    routing::post,
};
use estuary::{Gateway, Settings, config::NodeConfig};
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::Notify, task::JoinHandle};

struct Server {
    url: String,
    task: JoinHandle<()>,
}
impl Server {
    async fn spawn(router: Router) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
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
fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap()
}
async fn gateway(upstream: &Server) -> (Server, Server) {
    let mut settings = Settings {
        nodes: vec![NodeConfig {
            id: "node".into(),
            base_url: format!("{}/v1", upstream.url),
            models: HashMap::from([("public".into(), "upstream".into())]),
            ..NodeConfig::default()
        }],
        ..Settings::default()
    };
    settings.health.route_while_starting = true;
    let gateway = Gateway::build(settings).unwrap();
    (
        Server::spawn(gateway.public_router()).await,
        Server::spawn(gateway.admin_router()).await,
    )
}
async fn snapshot(admin: &Server) -> Value {
    client()
        .get(format!("{}/admin/api/observability", admin.url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}
async fn send_request(public: &Server, streaming: bool) -> reqwest::Response {
    client().post(format!("{}/v1/chat/completions", public.url))
        .json(&json!({"model":"public","stream":streaming,"messages":[{"role":"user","content":"hi"}]})).send().await.unwrap()
}

#[tokio::test]
async fn buffered_usage_model_filters_and_metrics_work_with_logging_disabled() {
    let upstream = Server::spawn(Router::new().route("/v1/chat/completions", post(|| async {
        Json(json!({"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}],
            "usage":{"prompt_tokens":100,"completion_tokens":10,"prompt_tokens_details":{"cached_tokens":75}},
            "metrics":{"mean_itl_ms":20}}))
    }))).await;
    let (public, admin) = gateway(&upstream).await;
    let response = send_request(&public, false).await;
    assert_eq!(response.status(), StatusCode::OK);
    response.bytes().await.unwrap();
    let all = snapshot(&admin).await;
    assert_eq!(all["fleet"]["total"]["success"], 1);
    assert_eq!(all["fleet"]["total"]["output_tokens"], 10);
    assert_eq!(all["fleet"]["cache_hit_ratio"], 0.75);
    assert_eq!(all["fleet"]["latency"]["ttft"]["samples"], 0);
    assert_eq!(all["fleet"]["latency"]["duration"]["samples"], 1);
    assert_eq!(all["fleet"]["window"]["tpot_reported"], 1);
    let filtered: Value = client()
        .get(format!(
            "{}/admin/api/observability?scope=model&id=public&window=60",
            admin.url
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(filtered["selected"]["total"]["success"], 1);
    assert_eq!(filtered["window_seconds"], 60);
    let invalid = client()
        .get(format!("{}/admin/api/observability?window=42", admin.url))
        .send()
        .await
        .unwrap();
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    let metrics = client()
        .get(format!("{}/metrics", admin.url))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains("estuary_gateway_success_total{scope=\"fleet\",name=\"\"} 1"));
    assert_eq!(metrics.matches("# EOF").count(), 1);
}

#[tokio::test]
async fn stream_errors_and_unrecognized_eof_are_not_successful_http_200s() {
    for (ending, outcome) in [
        ("data: [DONE]\n\n", "success"),
        ("data: {\"error\":{\"message\":\"failed\"}}\n\n", "failed"),
        ("", "unknown"),
    ] {
        let body =
            format!("data: {{\"choices\":[{{\"delta\":{{\"content\":\"hello\"}}}}]}}\n\n{ending}");
        let upstream = Server::spawn(Router::new().route(
            "/v1/chat/completions",
            post(move || {
                let body = body.clone();
                async move { ([(header::CONTENT_TYPE, "text/event-stream")], body) }
            }),
        ))
        .await;
        let (public, admin) = gateway(&upstream).await;
        let response = send_request(&public, true).await;
        assert_eq!(response.status(), StatusCode::OK);
        response.bytes().await.unwrap();
        let all = snapshot(&admin).await;
        assert_eq!(all["fleet"]["total"][outcome], 1, "{all}");
        assert_eq!(all["fleet"]["latency"]["ttft"]["samples"], 1);
        assert_eq!(all["fleet"]["usage_coverage"], 0.0);
    }
}

#[tokio::test]
async fn active_stream_is_finalized_at_body_end_and_client_cancellation() {
    let release = Arc::new(Notify::new());
    let upstream = Server::spawn(Router::new().route("/v1/chat/completions", post({
        let release = Arc::clone(&release);
        move || { let release = Arc::clone(&release); async move {
            let body = Body::from_stream(async_stream::stream! {
                yield Ok::<_, std::io::Error>("data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n");
                release.notified().await;
                yield Ok("data: [DONE]\n\n");
            });
            ([(header::CONTENT_TYPE, "text/event-stream")], body).into_response()
        }}
    }))).await;
    let (public, admin) = gateway(&upstream).await;
    let mut response = send_request(&public, true).await;
    response.chunk().await.unwrap().unwrap();
    let active = snapshot(&admin).await;
    assert_eq!(active["fleet"]["total"]["started"], 1);
    assert_eq!(active["fleet"]["total"]["success"], 0);
    assert_eq!(active["fleet"]["pressure"]["active"], 1);
    drop(response);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let data = snapshot(&admin).await;
            if data["fleet"]["total"]["cancelled"] == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    release.notify_waiters();
}

#[tokio::test]
async fn delayed_stream_preserves_bytes_and_reports_estimated_generation_speed() {
    const FIRST: &str = "data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n";
    const LAST: &str = "data: {\"choices\":[{\"delta\":{\"content\":\"last\"}}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":3}}\n\ndata: [DONE]\n\n";
    let upstream = Server::spawn(Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            let body = Body::from_stream(async_stream::stream! {
                yield Ok::<_, std::io::Error>(FIRST);
                tokio::time::sleep(Duration::from_millis(50)).await;
                yield Ok(LAST);
            });
            ([(header::CONTENT_TYPE, "text/event-stream")], body)
        }),
    ))
    .await;
    let (public, admin) = gateway(&upstream).await;
    let body = send_request(&public, true).await.text().await.unwrap();
    assert_eq!(body, format!("{FIRST}{LAST}"));
    let all = snapshot(&admin).await;
    let scope = &all["fleet"];
    assert_eq!(scope["total"]["success"], 1);
    assert_eq!(scope["total"]["output_tokens"], 3);
    assert_eq!(scope["usage_coverage"], 1.0);
    assert_eq!(scope["window"]["tpot_estimated"], 1);
    assert_eq!(scope["window"]["tpot_reported"], 0);
    assert!(scope["latency"]["duration"]["mean"].as_f64().unwrap() >= 0.05);
    assert!(scope["latency"]["tpot"]["mean"].as_f64().unwrap() >= 0.025);
    assert!(scope["latency"]["generation_rate"]["p50"].as_f64().unwrap() > 0.0);
    assert_eq!(scope["pressure"]["active"], 0);
}

#[tokio::test]
async fn observability_requires_the_existing_admin_authentication() {
    let mut settings = Settings::default();
    settings.server.admin_token = Some("test-admin".into());
    let admin = Server::spawn(Gateway::build(settings).unwrap().admin_router()).await;
    let url = format!("{}/admin/api/observability", admin.url);
    assert_eq!(
        client().get(&url).send().await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client()
            .get(&url)
            .bearer_auth("test-admin")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
}
