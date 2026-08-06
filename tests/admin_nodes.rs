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

fn assert_empty_gateway_status(status: &Value) {
    assert_eq!(status["status"], "not_ready");
    assert_eq!(status["fleet"]["total_nodes"], 0);
    assert_eq!(status["queue"]["requests"], 0);
    assert_eq!(status["response_buffer"]["used_bytes"], 0);
    assert_eq!(
        status["response_buffer"]["max_bytes"],
        Settings::default().server.max_buffered_response_bytes
    );
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
    assert_empty_gateway_status(&initial_status);

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
    assert_eq!(created["exact_kv_bytes"], 0);

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
async fn redacts_preserves_and_clears_a_database_api_key() {
    let upstream = TestServer::spawn(Router::new().route(
        "/v1/models",
        get(|| async { Json(json!({"object": "list", "data": []})) }),
    ))
    .await;
    let mut settings = Settings::default();
    settings.health.timeout_ms = 500;
    let admin = TestServer::spawn(Gateway::build(settings).unwrap().admin_router()).await;
    let client = client();

    let mut config = node(&upstream.base_url);
    config.api_key = Some("stored-secret".to_owned());
    let created = client
        .post(admin.url("/admin/api/nodes"))
        .json(&config)
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(created["config"]["api_key"], Value::Null);
    assert_eq!(created["credentials"]["api_key_configured"], true);
    assert_eq!(created["credentials"]["api_key_source"], "database");

    config.api_key = None;
    config.weight = 1.5;
    let preserved = client
        .put(admin.url("/admin/api/nodes/dynamic-a"))
        .json(&json!({"revision": 1, "config": config}))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(preserved["revision"], 2);
    assert_eq!(preserved["config"]["api_key"], Value::Null);
    assert_eq!(preserved["credentials"]["api_key_configured"], true);

    let cleared = client
        .put(admin.url("/admin/api/nodes/dynamic-a"))
        .json(&json!({
            "revision": 2,
            "config": config,
            "clear_api_key": true,
        }))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(cleared["revision"], 3);
    assert_eq!(cleared["credentials"]["api_key_configured"], false);
}

#[tokio::test]
async fn redacts_preserves_and_explicitly_clears_sensitive_headers() {
    let upstream = TestServer::spawn(Router::new().route(
        "/v1/models",
        get(|| async { Json(json!({"object": "list", "data": []})) }),
    ))
    .await;
    let admin =
        TestServer::spawn(Gateway::build(Settings::default()).unwrap().admin_router()).await;
    let client = client();

    let mut config = node(&upstream.base_url);
    config
        .headers
        .insert("x-api-key".to_owned(), "header-secret".to_owned());
    let created = client
        .post(admin.url("/admin/api/nodes"))
        .json(&config)
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(created["config"]["headers"], json!({}));
    assert_eq!(created["credentials"]["header_names"], json!(["x-api-key"]));

    config.headers.clear();
    config.weight = 1.5;
    let preserved = client
        .put(admin.url("/admin/api/nodes/dynamic-a"))
        .json(&json!({"revision": 1, "config": config}))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(
        preserved["credentials"]["header_names"],
        json!(["x-api-key"])
    );

    let cleared = client
        .put(admin.url("/admin/api/nodes/dynamic-a"))
        .json(&json!({
            "revision": 2,
            "config": config,
            "clear_headers": true,
        }))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(cleared["credentials"]["header_names"], json!([]));
}

#[tokio::test]
async fn admin_token_protects_management_routes_but_not_health_checks() {
    let mut settings = Settings::default();
    settings.server.admin_token = Some("admin-secret".to_owned());
    let admin = TestServer::spawn(Gateway::build(settings).unwrap().admin_router()).await;
    let client = client();

    assert_eq!(
        client
            .get(admin.url("/health/live"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let unauthorized = client
        .get(admin.url("/admin/api/status"))
        .send()
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    assert!(unauthorized.headers().contains_key("www-authenticate"));
    assert_eq!(
        client
            .get(admin.url("/admin/api/status"))
            .bearer_auth("admin-secret")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        client
            .get(admin.url("/admin/"))
            .basic_auth("estuary", Some("admin-secret"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
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
