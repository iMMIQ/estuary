use std::{collections::HashMap, time::Duration};

use axum::{
    Json, Router,
    routing::{get, post},
};
use estuary::{
    Gateway, Settings,
    config::{NodeConfig, ProviderKind},
};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::{net::TcpListener, task::JoinHandle, time::timeout};

const IO_TIMEOUT: Duration = Duration::from_secs(3);

async fn live_update_upstreams(
    release: std::sync::Arc<tokio::sync::Semaphore>,
    started: tokio::sync::mpsc::UnboundedSender<(axum::http::HeaderMap, Value)>,
) -> (TestServer, TestServer) {
    let blocked = std::sync::Arc::clone(&release);
    let first = TestServer::spawn(
        Router::new()
            .route("/v1/models", get(|| async { StatusCode::OK }))
            .route("/v1/chat/completions", post(move |headers: axum::http::HeaderMap, Json(body): Json<Value>| {
                let started = started.clone();
                let blocked = std::sync::Arc::clone(&blocked);
                async move {
                    started.send((headers, body.clone())).unwrap();
                    let _permit = blocked.acquire().await.unwrap();
                    Json(json!({"id":"old", "object":"chat.completion", "model": body["model"],
                        "choices":[{"index":0,"message":{"role":"assistant","content":"old response"},"finish_reason":"stop"}]}))
                }
            })),
    ).await;
    let second = TestServer::spawn(
        Router::new()
            .route("/ready", get(|headers: axum::http::HeaderMap| async move {
                assert_eq!(headers["authorization"], "Bearer new-secret");
                assert_eq!(headers["x-tenant"], "new-tenant");
                StatusCode::OK
            }))
            .route("/engine/version", get(|| async { Json(json!({"version":"0.25.0"})) }))
            .route("/engine/metrics", get(|| async { "vllm:num_requests_running 0\nvllm:num_requests_waiting 0\n" }))
            .route("/v1/chat/completions", post(|headers: axum::http::HeaderMap, Json(body): Json<Value>| async move {
                assert_eq!(headers["authorization"], "Bearer new-secret");
                assert_eq!(headers["x-tenant"], "new-tenant");
                assert_eq!(body["model"], "new-model");
                Json(json!({"id":"new", "object":"chat.completion", "model":"new-model",
                    "choices":[{"index":0,"message":{"role":"assistant","content":"new response"},"finish_reason":"stop"}]}))
            })),
    ).await;
    (first, second)
}

fn live_update_provider() -> estuary::config::ProviderConfig {
    estuary::config::ProviderConfig {
        kind: ProviderKind::Vllm,
        version_path: "/engine/version".to_owned(),
        metrics_path: "/engine/metrics".to_owned(),
        tokenize_path: "/engine/tokenize".to_owned(),
        monitor_interval_ms: 500,
        request_timeout_ms: 500,
        telemetry_stale_ms: 2_000,
        waiting_threshold: 3,
        tokenize_cache_entries: 32,
        kv_events: Some(estuary::config::VllmKvEventsConfig::default()),
    }
}

#[tokio::test]
async fn connection_settings_change_live_while_old_response_uses_its_snapshot() {
    let release = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
    let (started, mut received) = tokio::sync::mpsc::unbounded_channel();
    let (first, second) = live_update_upstreams(std::sync::Arc::clone(&release), started).await;
    let gateway = Gateway::build(Settings::default()).unwrap();
    let admin = TestServer::spawn(gateway.admin_router()).await;
    let public = TestServer::spawn(gateway.public_router()).await;
    let client = client();
    let mut config = node(&first.base_url);
    config.api_key = Some("old-secret".to_owned());
    config
        .headers
        .insert("x-tenant".to_owned(), "old-tenant".to_owned());
    client
        .post(admin.url("/admin/api/nodes"))
        .json(&config)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let old_request = {
        let client = client.clone();
        let url = public.url("/v1/chat/completions");
        tokio::spawn(async move {
            client
                .post(url)
                .json(
                    &json!({"model":"public-chat","messages":[{"role":"user","content":"hello"}]}),
                )
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap()
                .json::<Value>()
                .await
                .unwrap()
        })
    };
    let (headers, body) = timeout(IO_TIMEOUT, received.recv()).await.unwrap().unwrap();
    assert_eq!(headers["authorization"], "Bearer old-secret");
    assert_eq!(headers["x-tenant"], "old-tenant");
    assert_eq!(body["model"], "upstream-chat");

    config.base_url = format!("{}/v1", second.base_url);
    config
        .models
        .insert("public-chat".to_owned(), "new-model".to_owned());
    config.api_key = Some("new-secret".to_owned());
    config
        .headers
        .insert("x-tenant".to_owned(), "new-tenant".to_owned());
    config.health_path = "/ready".to_owned();
    config.max_concurrency = 2;
    config.weight = 1.5;
    config.provider = live_update_provider();
    let updated = timeout(
        IO_TIMEOUT,
        client
            .put(admin.url("/admin/api/nodes/dynamic-a?timeout_ms=0"))
            .json(&json!({"revision":1,"config":config}))
            .send(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        updated.status(),
        StatusCode::OK,
        "{}",
        updated.text().await.unwrap()
    );
    let updated = updated.json::<Value>().await.unwrap();
    assert_eq!(updated["runtime"]["active"], 1);
    assert_eq!(updated["runtime"]["available"], 1);
    assert_eq!(updated["runtime"]["lifecycle"], "serving");
    assert!(!old_request.is_finished());
    let new_response = timeout(
        IO_TIMEOUT,
        client
            .post(public.url("/v1/chat/completions"))
            .json(&json!({"model":"public-chat","messages":[{"role":"user","content":"hello"}]}))
            .send(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(new_response.status(), StatusCode::OK);
    assert_eq!(
        new_response.json::<Value>().await.unwrap()["choices"][0]["message"]["content"],
        "new response"
    );
    release.add_permits(1);
    let old_response = timeout(IO_TIMEOUT, old_request).await.unwrap().unwrap();
    assert_eq!(
        old_response["choices"][0]["message"]["content"],
        "old response"
    );
}

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
    assert_eq!(status["version"], estuary::VERSION);
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
async fn vllm_operational_metrics_are_exposed_as_structured_admin_data() {
    let upstream = TestServer::spawn(
        Router::new()
            .route(
                "/v1/models",
                get(|| async { Json(json!({"object": "list", "data": []})) }),
            )
            .route(
                "/version",
                get(|| async { Json(json!({"version": "0.25.0"})) }),
            )
            .route(
                "/metrics",
                get(|| async {
                    concat!(
                        "vllm:num_requests_running 2\n",
                        "vllm:num_requests_waiting 1\n",
                        "vllm:kv_cache_usage_perc 0.62\n",
                        "vllm:prompt_tokens_total 1200\n",
                        "vllm:generation_tokens_total 340\n",
                        "vllm:request_success_total{finished_reason=\"stop\"} 8\n",
                        "vllm:prefix_cache_queries_total 1000\n",
                        "vllm:prefix_cache_hits_total 740\n",
                        "vllm:num_preemptions_total 2\n",
                    )
                }),
            ),
    )
    .await;
    let mut config = node(&upstream.base_url);
    config.provider.kind = ProviderKind::Vllm;
    let mut settings = Settings::default();
    settings.health.timeout_ms = 500;
    let admin = TestServer::spawn(Gateway::build(settings).unwrap().admin_router()).await;

    let created = client()
        .post(admin.url("/admin/api/nodes"))
        .json(&config)
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    let created = created.json::<Value>().await.unwrap();
    assert_eq!(created["runtime"]["provider_version"], "0.25.0");
    assert_eq!(created["runtime"]["upstream_running"], 2);
    assert_eq!(created["runtime"]["upstream_waiting"], 1);
    assert_eq!(created["runtime"]["kv_cache_usage"], 0.62);
    // A recent-window ratio needs two samples; lifetime totals alone are not a rate.
    assert!(created["runtime"]["prefix_cache_hit_rate"].is_null());
    assert_eq!(created["runtime"]["prefix_cache_queries_total"], 1000.0);
    assert_eq!(created["runtime"]["prefix_cache_hits_total"], 740.0);
    assert_eq!(created["runtime"]["preemptions_total"], 2.0);
    assert!(created["runtime"]["prompt_tokens_per_second"].is_null());
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
    assert_eq!(initial.json::<Value>().await.unwrap()["nodes"], json!([]));
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
    assert_eq!(nodes["nodes"], json!([]));
}

#[tokio::test]
async fn preflight_preserves_credentials_unless_explicitly_cleared() {
    let upstream = TestServer::spawn(Router::new().route(
        "/v1/models",
        get(|headers: axum::http::HeaderMap| async move {
            if headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                == Some("Bearer stored-secret")
                && headers
                    .get("x-tenant")
                    .and_then(|value| value.to_str().ok())
                    == Some("tenant-secret")
            {
                StatusCode::OK
            } else {
                StatusCode::UNAUTHORIZED
            }
        }),
    ))
    .await;
    let admin =
        TestServer::spawn(Gateway::build(Settings::default()).unwrap().admin_router()).await;
    let client = client();
    let mut config = node(&upstream.base_url);
    config.api_key = Some("stored-secret".to_owned());
    config
        .headers
        .insert("x-tenant".to_owned(), "tenant-secret".to_owned());
    let created = client
        .post(admin.url("/admin/api/nodes"))
        .json(&config)
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);

    let mut redacted = config.clone();
    redacted.api_key = None;
    redacted.headers.clear();
    for (query, candidate, expected) in [
        ("", &redacted, StatusCode::OK),
        (
            "?clear_api_key=true",
            &redacted,
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            "?clear_headers=true",
            &redacted,
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            "?clear_api_key=true&clear_headers=true",
            &config,
            StatusCode::OK,
        ),
    ] {
        let response = client
            .post(admin.url(&format!("/admin/api/nodes/preflight{query}")))
            .json(candidate)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), expected, "preflight query {query:?}");
    }
    let stored = client
        .get(admin.url("/admin/api/nodes/dynamic-a"))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(stored["revision"], 1);
    assert_eq!(stored["credentials"]["api_key_configured"], true);
    assert_eq!(stored["credentials"]["header_names"], json!(["x-tenant"]));
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

#[tokio::test]
async fn validates_and_manages_ip_connection_limits() {
    let gateway = Gateway::build(Settings::default()).unwrap();
    let admin = TestServer::spawn(gateway.admin_router()).await;
    let client = client();

    assert_eq!(
        client
            .put(admin.url("/admin/api/ip-limits/not-an-ip"))
            .json(&json!({"limit": 1}))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        client
            .put(admin.url("/admin/api/ip-limits/192.0.2.10"))
            .json(&json!({"limit": 2}))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let status = client
        .get(admin.url("/admin/api/status"))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(
        status["connections"]["ip_limits"],
        json!([{"ip": "192.0.2.10", "limit": 2}])
    );
    assert_eq!(
        client
            .delete(admin.url("/admin/api/ip-limits/192.0.2.10"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
}
