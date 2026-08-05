use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{Json, Router, extract::State, routing::post};
use estuary::{Gateway, Settings, config::NodeConfig};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::Notify, task::JoinHandle, time::timeout};

struct UpstreamState {
    calls: AtomicUsize,
    release_first: Notify,
}

struct TestServer {
    address: SocketAddr,
    task: JoinHandle<()>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn upstream_chat(State(state): State<Arc<UpstreamState>>) -> Json<Value> {
    if state.calls.fetch_add(1, Ordering::AcqRel) == 0 {
        state.release_first.notified().await;
    }
    Json(json!({
        "id": "chatcmpl-test",
        "object": "chat.completion",
        "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}}]
    }))
}

async fn spawn_upstream(state: Arc<UpstreamState>) -> TestServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = Router::new()
        .route(
            "/v1/models",
            axum::routing::get(|| async { Json(json!({"object": "list", "data": []})) }),
        )
        .route("/v1/chat/completions", post(upstream_chat))
        .with_state(state);
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    TestServer { address, task }
}

async fn unused_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

async fn wait_for_url(client: &reqwest::Client, url: &str) {
    timeout(Duration::from_secs(3), async {
        loop {
            if client.get(url).send().await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn process_drain_finishes_an_existing_queue_before_exit() {
    let upstream_state = Arc::new(UpstreamState {
        calls: AtomicUsize::new(0),
        release_first: Notify::new(),
    });
    let upstream = spawn_upstream(Arc::clone(&upstream_state)).await;
    let public = unused_address().await;
    let admin = unused_address().await;

    let mut settings = Settings::default();
    settings.server.listen = public.to_string();
    settings.server.admin_listen = admin.to_string();
    settings.server.withdrawal_delay_ms = 20;
    settings.server.shutdown_grace_ms = 3_000;
    settings.health.route_while_starting = true;
    settings.nodes = vec![NodeConfig {
        id: "only".to_owned(),
        base_url: format!("http://{}/v1", upstream.address),
        models: HashMap::from([("model".to_owned(), "model".to_owned())]),
        max_concurrency: 1,
        ..NodeConfig::default()
    }];
    let gateway = tokio::spawn(async move { Gateway::build(settings).unwrap().run().await });
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    wait_for_url(&client, &format!("http://{admin}/health/live")).await;

    let body = json!({
        "model": "model",
        "messages": [{"role": "user", "content": "test"}]
    });
    let first = {
        let client = client.clone();
        let body = body.clone();
        tokio::spawn(async move {
            client
                .post(format!("http://{public}/v1/chat/completions"))
                .json(&body)
                .send()
                .await
                .unwrap()
        })
    };
    timeout(Duration::from_secs(2), async {
        while upstream_state.calls.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let second = {
        let client = client.clone();
        tokio::spawn(async move {
            client
                .post(format!("http://{public}/v1/chat/completions"))
                .json(&body)
                .send()
                .await
                .unwrap()
        })
    };
    timeout(Duration::from_secs(2), async {
        loop {
            let status = client
                .get(format!("http://{admin}/admin/api/status"))
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap();
            if status["queue"]["requests"] == 1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    let drain = client
        .put(format!("http://{admin}/admin/api/process/drain"))
        .send()
        .await
        .unwrap();
    assert_eq!(drain.status(), StatusCode::ACCEPTED);
    upstream_state.release_first.notify_one();

    assert_eq!(first.await.unwrap().status(), StatusCode::OK);
    assert_eq!(second.await.unwrap().status(), StatusCode::OK);
    timeout(Duration::from_secs(4), gateway)
        .await
        .expect("gateway did not finish its drain")
        .unwrap()
        .unwrap();
}
