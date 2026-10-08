use super::assets::*;
use super::middleware::*;
use super::transport::*;
use super::*;
use axum::http::{
    HeaderMap,
    header::{ACCEPT_ENCODING, CACHE_CONTROL, CONTENT_ENCODING, CONTENT_TYPE, VARY},
};
use axum::serve::Listener;

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
