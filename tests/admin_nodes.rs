use std::{collections::HashMap, time::Duration};

use axum::{Json, Router, routing::get};
use estuary::{Gateway, Settings, config::NodeConfig};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::{net::TcpListener, task::JoinHandle, time::timeout};

const IO_TIMEOUT: Duration = Duration::from_secs(3);

struct TestServer {
    base_url: String,
    task: JoinHandle<()>,
}

impl TestServer {
    async fn spawn(router: Router) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
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

fn client() -> reqwest::Client {
    reqwest::Client::builder().no_proxy().build().unwrap()
}

fn node(base_url: &str) -> NodeConfig {
    NodeConfig {
        id: "dynamic-a".to_owned(),
        base_url: format!("{base_url}/v1"),
        models: HashMap::from([("public-chat".to_owned(), "upstream-chat".to_owned())]),
        ..NodeConfig::default()
    }
}

#[tokio::test]
async fn creates_updates_and_deletes_a_live_node() {
    let upstream = TestServer::spawn(Router::new().route(
        "/v1/models",
        get(|| async { Json(json!({"object": "list", "data": []})) }),
    ))
    .await;
    let mut settings = Settings::default();
    settings.health.timeout_ms = 500;
    let gateway = Gateway::build(settings).unwrap();
    let admin = TestServer::spawn(gateway.admin_router()).await;
    let public = TestServer::spawn(gateway.public_router()).await;
    let client = client();

    let initial = client
        .get(admin.url("/admin/api/nodes"))
        .send()
        .await
        .unwrap();
    assert_eq!(initial.status(), StatusCode::OK);
    assert!(
        initial.json::<Value>().await.unwrap()["nodes"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let initial_status = client
        .get(admin.url("/admin/api/status"))
        .send()
        .await
        .unwrap();
    assert_eq!(initial_status.status(), StatusCode::OK);
    let initial_status = initial_status.json::<Value>().await.unwrap();
    assert_eq!(initial_status["status"], "not_ready");
    assert_eq!(initial_status["fleet"]["total_nodes"], 0);
    assert_eq!(initial_status["queue"]["requests"], 0);

    let created = timeout(
        IO_TIMEOUT,
        client
            .post(admin.url("/admin/api/nodes"))
            .json(&node(&upstream.base_url))
            .send(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    let created = created.json::<Value>().await.unwrap();
    assert_eq!(created["revision"], 1);
    assert_eq!(created["runtime"]["health"], "healthy");
    assert_eq!(created["admission"]["state"], "accepting");
    assert_eq!(created["admission"]["accepting_assignments"], true);

    let ready_status = client
        .get(admin.url("/admin/api/status"))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(ready_status["status"], "ready");
    assert_eq!(ready_status["fleet"]["routable_nodes"], 1);
    assert_eq!(ready_status["fleet"]["accepting_nodes"], 1);

    let models = client.get(public.url("/v1/models")).send().await.unwrap();
    assert_eq!(models.status(), StatusCode::OK);
    assert_eq!(
        models.json::<Value>().await.unwrap()["data"][0]["id"],
        "public-chat"
    );

    let mut changed = node(&upstream.base_url);
    changed.weight = 1.5;
    let stale = client
        .put(admin.url("/admin/api/nodes/dynamic-a"))
        .json(&json!({"revision": 9, "config": changed}))
        .send()
        .await
        .unwrap();
    assert_eq!(stale.status(), StatusCode::CONFLICT);

    let updated = client
        .put(admin.url("/admin/api/nodes/dynamic-a"))
        .json(&json!({"revision": 1, "config": changed}))
        .send()
        .await
        .unwrap();
    assert_eq!(updated.status(), StatusCode::OK);
    let updated = updated.json::<Value>().await.unwrap();
    assert_eq!(updated["revision"], 2);
    assert_eq!(updated["config"]["weight"], 1.5);

    let deleted = client
        .delete(admin.url("/admin/api/nodes/dynamic-a?revision=2"))
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), StatusCode::OK);
    assert_eq!(
        client
            .get(admin.url("/health/ready"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn preflight_checks_a_node_without_persisting_it() {
    let upstream = TestServer::spawn(Router::new().route(
        "/v1/models",
        get(|| async { Json(json!({"object": "list", "data": []})) }),
    ))
    .await;
    let mut settings = Settings::default();
    settings.health.timeout_ms = 500;
    let gateway = Gateway::build(settings).unwrap();
    let admin = TestServer::spawn(gateway.admin_router()).await;

    let response = client()
        .post(admin.url("/admin/api/nodes/preflight"))
        .json(&node(&upstream.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = response.json::<Value>().await.unwrap();
    assert_eq!(response["ok"], true);
    assert_eq!(response["runtime"]["health"], "healthy");
    assert_eq!(response["checks"]["provider"], "passed");

    let nodes = client()
        .get(admin.url("/admin/api/nodes"))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert!(nodes["nodes"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn process_drain_disables_readiness_without_draining_upstream_nodes() {
    let mut settings = Settings::default();
    settings.health.route_while_starting = true;
    settings.nodes = vec![node("http://127.0.0.1:1")];
    let gateway = Gateway::build(settings).unwrap();
    let admin = TestServer::spawn(gateway.admin_router()).await;
    let client = client();

    assert_eq!(
        client
            .get(admin.url("/health/ready"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let response = client
        .put(admin.url("/admin/api/process/drain"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let payload = response.json::<Value>().await.unwrap();
    assert_eq!(payload["process"]["state"], "quiescing");
    assert_eq!(
        client
            .get(admin.url("/health/ready"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );

    let nodes = client
        .get(admin.url("/admin/nodes"))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(nodes["nodes"][0]["lifecycle"], "serving");
}

#[tokio::test]
async fn serves_the_embedded_admin_application_with_security_headers() {
    let gateway = Gateway::build(Settings::default()).unwrap();
    let admin = TestServer::spawn(gateway.admin_router()).await;
    let response = client().get(admin.url("/admin/")).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let content_security_policy = response
        .headers()
        .get("content-security-policy")
        .and_then(|value| value.to_str().ok())
        .unwrap();
    assert!(content_security_policy.contains("style-src 'self' 'unsafe-inline'"));
    assert!(!content_security_policy.contains("script-src 'unsafe-inline'"));
    assert_eq!(
        response
            .headers()
            .get("x-frame-options")
            .and_then(|value| value.to_str().ok()),
        Some("DENY")
    );
    assert!(
        response
            .text()
            .await
            .unwrap()
            .contains("Estuary Control Plane")
    );
}
