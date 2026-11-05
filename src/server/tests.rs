use super::assets::*;
use super::middleware::*;
use super::transport::*;
use super::*;
use axum::http::{
    HeaderMap,
    header::{ACCEPT_ENCODING, CACHE_CONTROL, CONTENT_ENCODING, CONTENT_TYPE, VARY},
};
use axum::serve::Listener;

fn gateway_with_busy_node(max_concurrency: usize) -> (Gateway, crate::config::NodeConfig) {
    let config = crate::config::NodeConfig {
        id: "live".to_owned(),
        // Routing-only updates must succeed even without a reachable upstream.
        base_url: "http://127.0.0.1:1/v1".to_owned(),
        models: HashMap::from([("public".to_owned(), "old-model".to_owned())]),
        max_concurrency,
        ..crate::config::NodeConfig::default()
    };
    let settings = Settings {
        nodes: vec![config.clone()],
        ..Settings::default()
    };
    let gateway = Gateway::build(settings).unwrap();
    gateway
        .state
        .scheduler
        .node("live")
        .unwrap()
        .record_probe_success(&gateway.state.settings.health);
    (gateway, config)
}

async fn apply_node_update(
    state: &Arc<AppState>,
    config: crate::config::NodeConfig,
    revision: u64,
) -> axum::response::Response {
    admin::update_node(
        State(Arc::clone(state)),
        axum::extract::Path(config.id.clone()),
        axum::extract::Query(admin::MutationQuery {
            timeout_ms: Some(0),
            revision: None,
        }),
        Json(admin::UpdateNodeRequest {
            revision,
            config,
            clear_api_key: false,
            clear_headers: false,
        }),
    )
    .await
}

#[tokio::test]
async fn routing_updates_preserve_busy_runtime_without_upstream_probes() {
    let (gateway, mut config) = gateway_with_busy_node(1);
    let state = &gateway.state;
    let previous = state.scheduler.node("live").unwrap();
    let lease = previous
        .try_acquire(Arc::new(tokio::sync::Notify::new()))
        .unwrap();
    lease.record_success(Duration::from_millis(123));
    config
        .models
        .insert("public".to_owned(), "new-model".to_owned());
    config.model_capabilities.insert(
        "public".to_owned(),
        crate::config::ModelCapabilityConfig { multimodal: false },
    );
    config.max_concurrency = 2;
    config.weight = 2.5;
    let response = apply_node_update(state, config.clone(), 1).await;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(payload["runtime"]["active"], 1);
    assert_eq!(payload["runtime"]["available"], 1);
    assert_eq!(payload["runtime"]["max_concurrency"], 2);
    assert_eq!(payload["runtime"]["weight"], 2.5);
    assert_eq!(payload["runtime"]["latency_ewma_ms"], 123.0);
    let current = state.scheduler.node("live").unwrap();
    assert!(previous.is_retired());
    assert_eq!(
        previous.upstream_model(Some("public")),
        Some(Some("old-model".to_owned()))
    );
    assert_eq!(
        current.upstream_model(Some("public")),
        Some(Some("new-model".to_owned()))
    );
    assert!(!current.supports_multimodal("public"));
    assert!(current.is_routable());
    let second = current
        .try_acquire(Arc::new(tokio::sync::Notify::new()))
        .unwrap();
    assert!(
        current
            .try_acquire(Arc::new(tokio::sync::Notify::new()))
            .is_none()
    );

    // A failed or stale write must not start draining or alter live capacity.
    config.max_concurrency = 0;
    assert_eq!(
        apply_node_update(state, config.clone(), 2).await.status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    config.max_concurrency = 1;
    assert_eq!(
        apply_node_update(state, config.clone(), 1).await.status(),
        StatusCode::CONFLICT
    );
    assert_eq!(current.snapshot().available, 0);
    assert!(current.is_routable());
    assert_eq!(
        apply_node_update(state, config.clone(), 2).await.status(),
        StatusCode::OK
    );
    let current = state.scheduler.node("live").unwrap();
    assert_eq!(current.active(), 2);
    assert_eq!(current.snapshot().available, 0);
    drop(second);
    assert!(
        current
            .try_acquire(Arc::new(tokio::sync::Notify::new()))
            .is_none()
    );
    drop(lease);
    assert_eq!(current.active(), 0);
    assert_eq!(current.snapshot().available, 1);
}

#[tokio::test]
async fn reconciliation_updates_busy_nodes_and_deletion_waits_for_all_generations() {
    let (gateway, mut config) = gateway_with_busy_node(2);
    let state = &gateway.state;
    let previous = state.scheduler.node("live").unwrap();
    let lease = previous
        .try_acquire(Arc::new(tokio::sync::Notify::new()))
        .unwrap();
    for revision in 1..=2 {
        config.max_concurrency = 1;
        config.weight += 1.0;
        state
            .store
            .update_async("live", revision, &config)
            .await
            .unwrap()
            .unwrap();
        reconcile::reconcile_control_plane(state).await.unwrap();
        let current = state.scheduler.node("live").unwrap();
        assert!(current.is_routable());
        assert_eq!(current.active(), 1);
        assert_eq!(current.snapshot().available, 0);
        assert_eq!(
            state.runtime_revisions.read().get("live"),
            Some(&(revision + 1))
        );
    }
    let deletion = || {
        admin::delete_node(
            State(Arc::clone(state)),
            axum::extract::Path("live".to_owned()),
            axum::extract::Query(admin::MutationQuery {
                timeout_ms: Some(0),
                revision: Some(3),
            }),
        )
    };
    assert_eq!(deletion().await.status(), StatusCode::CONFLICT);
    assert_eq!(state.scheduler.node("live").unwrap().active(), 1);
    drop(lease);
    assert_eq!(deletion().await.status(), StatusCode::OK);
    assert!(state.scheduler.node("live").is_none());
}

#[tokio::test]
async fn rejected_shared_update_keeps_previous_node_serving() {
    let (gateway, mut config) = gateway_with_busy_node(1);
    let state = &gateway.state;
    let previous = state.scheduler.node("live").unwrap();
    let lease = previous
        .try_acquire(Arc::new(tokio::sync::Notify::new()))
        .unwrap();
    config.health_path = "/new-health".to_owned();
    state
        .store
        .update_async("live", 1, &config)
        .await
        .unwrap()
        .unwrap();
    assert!(reconcile::reconcile_control_plane(state).await.is_err());
    assert!(Arc::ptr_eq(
        &previous,
        &state.scheduler.node("live").unwrap()
    ));
    assert!(previous.is_routable());
    assert_eq!(previous.active(), 1);
    assert_eq!(state.runtime_revisions.read().get("live"), Some(&1));
    drop(lease);
}

#[test]
fn gzip_negotiation_respects_explicit_refusal_and_quality() {
    for (value, expected) in [
        ("gzip, br", true),
        ("GZIP; q=0.5", true),
        ("identity", false),
        ("gzip;q=0, *;q=1", false),
        ("*;q=0.2", true),
        ("gzip;q=invalid", false),
        ("gzip;q=2", false),
    ] {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT_ENCODING, value.parse().unwrap());
        assert_eq!(accepts_gzip(&headers), expected, "{value}");
    }
    assert!(!accepts_gzip(&HeaderMap::new()));
}

#[tokio::test]
async fn admin_assets_serve_precompressed_bytes_with_matching_content_type_and_cache_policy() {
    let path = AdminAssets::iter()
        .find(|path| path.ends_with(".css"))
        .unwrap();
    let plain = embedded_admin_response(&path, true, false);
    let compressed = embedded_admin_response(&path, true, true);
    assert_eq!(compressed.headers()[CONTENT_ENCODING], "gzip");
    assert_eq!(compressed.headers()[VARY], "Accept-Encoding");
    assert_eq!(
        compressed.headers()[CONTENT_TYPE],
        plain.headers()[CONTENT_TYPE]
    );
    assert_eq!(
        compressed.headers()[CACHE_CONTROL],
        "public, max-age=31536000, immutable"
    );
    assert!(!plain.headers().contains_key(CONTENT_ENCODING));
    let compressed = axum::body::to_bytes(compressed.into_body(), usize::MAX)
        .await
        .unwrap();
    let plain = axum::body::to_bytes(plain.into_body(), usize::MAX)
        .await
        .unwrap();
    assert!(compressed.starts_with(&[0x1f, 0x8b]));
    assert!(compressed.len() < plain.len());
    assert_eq!(
        embedded_admin_response("index.html", false, true).headers()[CACHE_CONTROL],
        "no-store"
    );
}

#[test]
fn metric_paths_have_bounded_cardinality() {
    assert_eq!(metric_endpoint("/v1/chat/completions"), "chat_completions");
    assert_eq!(metric_endpoint("/v1/messages"), "anthropic_messages");
    assert_eq!(metric_endpoint("/v1/unknown/user-value"), "other");
}

#[tokio::test]
async fn public_listener_waits_before_accepting_above_connection_limit() {
    let inner = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = inner.local_addr().unwrap();
    let metrics = Metrics::new();
    let mut listener = BoundedTcpListener::new(
        inner,
        1,
        Arc::clone(&metrics),
        Arc::new(ConnectionTracker::default()),
        true,
        CancellationToken::new(),
    );

    let first_client = tokio::net::TcpStream::connect(address).await.unwrap();
    let (first_server, _) = listener.accept().await;
    assert_eq!(metrics.public_connections(), 1);
    let second_client = tokio::net::TcpStream::connect(address).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );

    drop(first_server);
    let (second_server, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
        .await
        .unwrap();
    assert_eq!(metrics.public_connections(), 1);
    drop((first_client, second_client, second_server));
    assert_eq!(metrics.public_connections(), 0);
}

#[test]
fn connection_tracker_limits_and_ranks_ips() {
    let tracker = ConnectionTracker::default();
    let first = "192.0.2.1".parse().unwrap();
    let second = "192.0.2.2".parse().unwrap();
    tracker.set_limit(first, 1);
    assert!(tracker.open(first));
    assert!(!tracker.open(first));
    assert!(tracker.open(second));
    assert!(tracker.open(second));
    assert_eq!(tracker.snapshot().0, vec![(second, 2), (first, 1)]);
    tracker.close(first);
    assert!(tracker.open(first));
}
#[tokio::test]
async fn drain_preserves_an_accepted_connection_until_its_first_request() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let public_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let admin_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let public = public_listener.local_addr().unwrap();
    let admin = admin_listener.local_addr().unwrap();
    drop((public_listener, admin_listener));
    let mut settings = Settings::default();
    settings.server.listen = public.to_string();
    settings.server.admin_listen = admin.to_string();
    settings.server.withdrawal_delay_ms = 1;
    settings.server.shutdown_grace_ms = 3_000;
    let built = Gateway::build(settings).unwrap();
    let state = Arc::clone(&built.state);
    let gateway = tokio::spawn(async move { built.run().await });
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if client
                .get(format!("http://{admin}/health/live"))
                .send()
                .await
                .is_ok()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut connection = tokio::net::TcpStream::connect(public).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.metrics.public_connections() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    client
        .put(format!("http://{admin}/admin/api/process/drain"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.process.snapshot().state != crate::lifecycle::ProcessState::Draining {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    connection
        .write_all(b"GET /v1/models HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = String::new();
    tokio::time::timeout(
        Duration::from_secs(2),
        connection.read_to_string(&mut response),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    tokio::time::timeout(Duration::from_secs(3), gateway)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
