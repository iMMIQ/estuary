use super::kv_events::*;
use super::monitor::*;
use super::tokenization::*;
use crate::kv_cache::{BlockHash, CacheMutation};
use rmpv::Value;
use std::{
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
};
use zeromq::{Socket, SocketSend, ZmqMessage};

use axum::{
    Json, Router,
    body::Body,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use rmpv::encode::write_value;
use serde_json::json;
use tokio::{net::TcpListener, task::JoinHandle};
use zeromq::PubSocket;

use super::*;
use crate::config::{HealthConfig, NodeConfig, PrefixConfig, ProviderConfig, VllmKvEventsConfig};

async fn management_server(version: &'static str) -> (String, JoinHandle<()>) {
    let router = Router::new()
        .route(
            "/version",
            get(move || async move { json!({"version": version}).to_string() }),
        )
        .route(
            "/metrics",
            get(|| async {
                "# TYPE vllm:num_requests_running gauge\n\
                 vllm:num_requests_running 2\n\
                 # TYPE vllm:num_requests_waiting gauge\n\
                 vllm:num_requests_waiting 3\n\
                 # TYPE vllm:kv_cache_usage_perc gauge\n\
                 vllm:kv_cache_usage_perc 0.5\n"
            }),
        )
        .route(
            "/tokenize",
            post(|| async { axum::Json(json!({"tokens": [1, 2, 3]})) }),
        );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (format!("http://{address}"), handle)
}

async fn counted_tokenize(
    State((calls, success, delay)): State<(Arc<AtomicUsize>, bool, Duration)>,
) -> Response {
    calls.fetch_add(1, Ordering::Relaxed);
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
    if success {
        Json(json!({"tokens": [1, 2, 3]})).into_response()
    } else {
        StatusCode::INTERNAL_SERVER_ERROR.into_response()
    }
}

async fn tokenize_server(
    success: bool,
    delay: Duration,
) -> (String, Arc<AtomicUsize>, JoinHandle<()>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let router = Router::new()
        .route("/tokenize", post(counted_tokenize))
        .with_state((Arc::clone(&calls), success, delay));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (format!("http://{address}"), calls, handle)
}

#[test]
fn tokenization_lru_access_log_remains_bounded_on_hot_hits() {
    let mut cache = TokenizationCache::new(4);
    let key = [7; 32];
    cache.insert(key, vec![1, 2, 3]);
    for _ in 0..10_000 {
        assert_eq!(cache.get(&key), Some(vec![1, 2, 3]));
    }
    assert_eq!(cache.values.len(), 1);
    assert_eq!(cache.values.cap().get(), 4);
}

#[test]
fn tokenization_lru_enforces_its_byte_budget() {
    let mut cache = TokenizationCache::with_max_bytes(10, 300);
    cache.insert([1; 32], vec![1; 10]);
    cache.insert([2; 32], vec![2; 10]);
    assert!(cache.used_bytes <= cache.max_bytes);
    assert_eq!(cache.values.len(), 1);
    assert!(cache.get(&[1; 32]).is_none());
    assert!(cache.get(&[2; 32]).is_some());
}

#[test]
fn provider_task_restart_delay_is_exponential_and_bounded() {
    assert_eq!(task_restart_delay(1), Duration::from_secs(1));
    assert_eq!(task_restart_delay(3), Duration::from_secs(4));
    assert_eq!(task_restart_delay(100), TASK_MAX_BACKOFF);
}

#[tokio::test]
async fn management_response_is_rejected_while_streaming_past_limit() {
    let router = Router::new().route(
        "/large",
        get(|| async {
            Body::from_stream(futures_util::stream::iter([
                Ok::<_, std::io::Error>(Bytes::from(vec![0; MAX_MANAGEMENT_BODY_BYTES])),
                Ok(Bytes::from_static(b"x")),
            ]))
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let response = Client::new()
        .get(format!("http://{address}/large"))
        .send()
        .await
        .unwrap();
    let error = read_bounded_response(response, "test body")
        .await
        .expect_err("response must be bounded");
    assert!(error.to_string().contains("too large"));
    server.abort();
}

fn vllm_node(base_url: &str) -> Arc<Node> {
    vllm_node_with_id("vllm", base_url, 2_000)
}

fn vllm_node_with_id(id: &str, base_url: &str, request_timeout_ms: u64) -> Arc<Node> {
    Node::from_config(&NodeConfig {
        id: id.to_owned(),
        base_url: format!("{base_url}/v1"),
        models: HashMap::from([("public".to_owned(), "upstream".to_owned())]),
        provider: ProviderConfig {
            kind: ProviderKind::Vllm,
            request_timeout_ms,
            kv_events: Some(VllmKvEventsConfig::default()),
            ..ProviderConfig::default()
        },
        ..NodeConfig::default()
    })
    .unwrap()
}

#[test]
fn parses_vllm_metrics() {
    let body = br#"
# TYPE vllm:num_requests_running gauge
vllm:num_requests_running{model_name="a"} 2
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting{model_name="a"} 3
# TYPE vllm:kv_cache_usage_perc gauge
vllm:kv_cache_usage_perc{model_name="a"} 0.75
# TYPE vllm:prompt_tokens_total counter
vllm:prompt_tokens_total{model_name="a",engine="0"} 1200
vllm:prompt_tokens_total{model_name="a",engine="1"} 300
# TYPE vllm:generation_tokens_total counter
vllm:generation_tokens_total{model_name="a"} 450
# TYPE vllm:request_success_total counter
vllm:request_success_total{model_name="a",finished_reason="stop"} 9
vllm:request_success_total{model_name="a",finished_reason="length"} 1
# TYPE vllm:prefix_cache_queries_total counter
vllm:prefix_cache_queries_total{model_name="a"} 1000
# TYPE vllm:prefix_cache_hits_total counter
vllm:prefix_cache_hits_total{model_name="a"} 625
# TYPE vllm:num_preemptions_total counter
vllm:num_preemptions_total{model_name="a"} 2
"#;
    let telemetry = parse_metrics(body).unwrap();
    assert_eq!(telemetry.running, 2);
    assert_eq!(telemetry.waiting, 3);
    assert_eq!(telemetry.kv_cache_usage, Some(0.75));
    assert_eq!(telemetry.prompt_tokens_total, Some(1500.0));
    assert_eq!(telemetry.generation_tokens_total, Some(450.0));
    assert_eq!(telemetry.requests_total, Some(10.0));
    assert_eq!(telemetry.prefix_cache_queries_total, Some(1000.0));
    assert_eq!(telemetry.prefix_cache_hits_total, Some(625.0));
    assert_eq!(telemetry.preemptions_total, Some(2.0));
}

#[test]
fn decodes_v025_event_shape_and_ignores_optional_extensions() {
    let stored = Value::Map(vec![
        (Value::from("type"), Value::from("BlockStored")),
        (
            Value::from("block_hashes"),
            Value::Array(vec![Value::from(7)]),
        ),
        (Value::from("parent_block_hash"), Value::Nil),
        (
            Value::from("token_ids"),
            Value::Array(vec![Value::from(1), Value::from(2)]),
        ),
        (Value::from("block_size"), Value::from(2)),
        (Value::from("lora_id"), Value::Nil),
        (Value::from("medium"), Value::from("GPU")),
        (Value::from("lora_name"), Value::Nil),
        (Value::from("extra_keys"), Value::Array(vec![Value::Nil])),
        (Value::from("group_idx"), Value::from(0)),
    ]);
    let batch = Value::Array(vec![Value::F64(1.0), Value::Array(vec![stored])]);
    let mut encoded = Vec::new();
    write_value(&mut encoded, &batch).unwrap();
    let mutations = decode_event_batch(&encoded).unwrap();
    assert_eq!(mutations.len(), 1);
    assert!(matches!(
        mutations[0],
        CacheMutation::Store { block_size: 2, .. }
    ));
}

#[test]
fn skips_lora_and_remote_cache_events() {
    let event = Value::Map(vec![
        (Value::from("type"), Value::from("BlockRemoved")),
        (
            Value::from("block_hashes"),
            Value::Array(vec![Value::from(1)]),
        ),
        (Value::from("medium"), Value::from("GPU")),
        (Value::from("locality"), Value::from("REMOTE")),
    ]);
    assert!(decode_event(&event).is_none());
}

fn stored_event_payload() -> Vec<u8> {
    let stored = Value::Map(vec![
        (Value::from("type"), Value::from("BlockStored")),
        (
            Value::from("block_hashes"),
            Value::Array(vec![Value::from(7)]),
        ),
        (Value::from("parent_block_hash"), Value::Nil),
        (
            Value::from("token_ids"),
            Value::Array(vec![Value::from(1), Value::from(2)]),
        ),
        (Value::from("block_size"), Value::from(2)),
        (Value::from("lora_id"), Value::Nil),
        (Value::from("medium"), Value::from("GPU")),
        (Value::from("lora_name"), Value::Nil),
        (Value::from("extra_keys"), Value::Array(vec![Value::Nil])),
        (Value::from("group_idx"), Value::from(0)),
    ]);
    let batch = Value::Array(vec![Value::F64(1.0), Value::Array(vec![stored])]);
    let mut encoded = Vec::new();
    write_value(&mut encoded, &batch).unwrap();
    encoded
}

fn cleared_event_payload() -> Vec<u8> {
    let cleared = Value::Map(vec![(Value::from("type"), Value::from("AllBlocksCleared"))]);
    let batch = Value::Array(vec![Value::F64(1.0), Value::Array(vec![cleared])]);
    let mut encoded = Vec::new();
    write_value(&mut encoded, &batch).unwrap();
    encoded
}

#[test]
fn unsynchronized_events_stay_degraded_until_an_explicit_clear() {
    let node = vllm_node("http://127.0.0.1:1");
    let exact = ExactCacheDirectory::default();
    exact.configure_node_owned(node.id(), 10, usize::MAX, node.instance_id());
    let prefix = PrefixDirectory::new(&PrefixConfig::default());
    let config = VllmKvEventsConfig::default();

    let synchronized = apply_payload(
        &node,
        &exact,
        &prefix,
        &config,
        &stored_event_payload(),
        false,
    )
    .unwrap();
    assert!(!synchronized);
    assert!(!exact.snapshot(node.id()).authoritative);
    assert_eq!(exact.snapshot(node.id()).blocks, 0);

    let synchronized = apply_payload(
        &node,
        &exact,
        &prefix,
        &config,
        &cleared_event_payload(),
        false,
    )
    .unwrap();
    assert!(synchronized);
    assert!(exact.snapshot(node.id()).authoritative);

    apply_payload(
        &node,
        &exact,
        &prefix,
        &config,
        &stored_event_payload(),
        true,
    )
    .unwrap();
    assert_eq!(exact.snapshot(node.id()).blocks, 1);
}

#[test]
fn unsynchronized_replay_restarts_from_zero() {
    assert_eq!(replay_start_sequence(Some(41), false), 0);
    assert_eq!(replay_start_sequence(Some(41), true), 42);
}

#[test]
fn successful_empty_replay_establishes_an_authoritative_empty_directory() {
    let node = vllm_node("http://127.0.0.1:1");
    let exact = ExactCacheDirectory::default();
    exact.configure_node_owned(node.id(), 10, usize::MAX, node.instance_id());
    exact
        .apply_owned(
            node.id(),
            node.instance_id(),
            vec![CacheMutation::Store {
                hashes: vec![BlockHash::Integer(1)],
                parent: None,
                token_ids: vec![1, 2],
                block_size: 2,
                group: 0,
            }],
        )
        .unwrap();
    let prefix = PrefixDirectory::new(&PrefixConfig::default());
    let mut synchronized = false;

    synchronize_empty_replay(&node, &exact, &prefix, &mut synchronized).unwrap();

    let snapshot = exact.snapshot(node.id());
    assert!(synchronized);
    assert!(snapshot.authoritative);
    assert_eq!(snapshot.blocks, 0);
}

#[tokio::test]
async fn unhealthy_upstream_invalidates_a_silent_subscriber_session() {
    let mut publisher = PubSocket::new();
    let endpoint = publisher
        .bind("tcp://127.0.0.1:0")
        .await
        .unwrap()
        .to_string();
    let node = vllm_node("http://127.0.0.1:1");
    let health = HealthConfig {
        healthy_threshold: 1,
        unhealthy_threshold: 1,
        ..HealthConfig::default()
    };
    node.record_probe_success(&health);
    let exact = Arc::new(ExactCacheDirectory::default());
    exact.configure_node_owned(node.id(), 10, usize::MAX, node.instance_id());
    exact
        .apply_owned(
            node.id(),
            node.instance_id(),
            vec![CacheMutation::Store {
                hashes: vec![BlockHash::Integer(1)],
                parent: None,
                token_ids: vec![1, 2],
                block_size: 2,
                group: 0,
            }],
        )
        .unwrap();
    let prefix = Arc::new(PrefixDirectory::new(&PrefixConfig::default()));
    let config = VllmKvEventsConfig {
        endpoint,
        reconnect_ms: 10,
        ..VllmKvEventsConfig::default()
    };
    let (_shutdown, mut receiver) = watch::channel(false);
    let task_node = Arc::clone(&node);
    let task_exact = Arc::clone(&exact);
    let task_prefix = Arc::clone(&prefix);
    let task = tokio::spawn(async move {
        let mut last_seq = Some(0);
        let mut synchronized = true;
        run_event_session(
            &task_node,
            &task_exact,
            &task_prefix,
            &config,
            &mut last_seq,
            &mut synchronized,
            &mut receiver,
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(30)).await;
    node.record_probe_failure("upstream stopped", &health);
    let error = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("silent ZMQ subscriber should observe unhealthy upstream")
        .unwrap()
        .unwrap_err();

    assert!(error.is::<KvUpstreamUnhealthy>());
    let snapshot = exact.snapshot(node.id());
    assert!(!snapshot.authoritative);
    assert_eq!(snapshot.blocks, 0);
    assert_eq!(node.provider_generation(), 1);
}

#[tokio::test]
async fn consumes_the_vllm_pub_sub_frame_layout() {
    let mut publisher = PubSocket::new();
    let endpoint = publisher
        .bind("tcp://127.0.0.1:0")
        .await
        .unwrap()
        .to_string();
    let node = vllm_node("http://127.0.0.1:1");
    let exact = Arc::new(ExactCacheDirectory::default());
    exact.configure_node_owned(node.id(), 10, usize::MAX, node.instance_id());
    let prefix = Arc::new(PrefixDirectory::new(&PrefixConfig::default()));
    let config = VllmKvEventsConfig {
        endpoint,
        ..VllmKvEventsConfig::default()
    };
    let (shutdown, receiver) = watch::channel(false);
    let task_node = Arc::clone(&node);
    let task_exact = Arc::clone(&exact);
    let task_prefix = Arc::clone(&prefix);
    let task_config = config.clone();
    let task = tokio::spawn(async move {
        let mut last_seq = None;
        let mut synchronized = false;
        let mut receiver = receiver;
        run_event_session(
            &task_node,
            &task_exact,
            &task_prefix,
            &task_config,
            &mut last_seq,
            &mut synchronized,
            &mut receiver,
        )
        .await
    });

    let payload = stored_event_payload();
    for _ in 0..30 {
        let message = ZmqMessage::try_from(vec![
            Bytes::from_static(b"kv-events"),
            Bytes::copy_from_slice(&0_u64.to_be_bytes()),
            Bytes::copy_from_slice(&payload),
        ])
        .unwrap();
        publisher.send(message).await.unwrap();
        if exact.snapshot(node.id()).blocks == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(exact.snapshot(node.id()).blocks, 1);
    shutdown.send(true).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn accepts_supported_vllm_versions_in_monitor_and_preflight() {
    let client = Client::new();
    for version in [
        "0.25.0",
        "0.25",
        " v0.25.0 ",
        "0.25.0.dev123+gabcdef",
        "0.25.0-rc1",
        "0.25.0rc1",
        "0.25.dev123",
        "0.26.0+custom/build",
        "1.0.0",
        "dev",
        "main+gabcdef",
    ] {
        let (base_url, server) = management_server(version).await;
        let node = vllm_node(&base_url);
        poll_node(&client, &node, true).await;
        assert_eq!(node.provider_state(), ProviderState::Ready, "{version}");
        assert_eq!(node.scheduling_load(), 5, "{version}");
        assert_eq!(node.snapshot().provider_version.as_deref(), Some(version),);

        let candidate = vllm_node(&base_url);
        preflight_vllm(&client, &candidate).await.unwrap();
        assert_eq!(
            candidate.provider_state(),
            ProviderState::Ready,
            "{version}"
        );
        assert_eq!(candidate.scheduling_load(), 5, "{version}");
        assert_eq!(
            candidate.snapshot().provider_version.as_deref(),
            Some(version),
        );
        server.abort();
    }
}

#[test]
fn native_thinking_capability_tracks_confirmed_releases_and_version_changes() {
    let node = vllm_node("http://127.0.0.1:8000");
    for (version, native) in [
        ("0.31.0", true),
        (" v0.31 ", true),
        ("0.31.0+gabcdef", true),
        ("1.0.0", true),
        ("0.30.9", false),
        ("dev", false),
        ("main+gabcdef", false),
        ("0.31.0.dev123", false),
        ("0.31.0-rc1", false),
        ("0.31.0rc1", false),
        ("0.31.0.1", false),
    ] {
        node.record_vllm_ready(version.to_owned());
        assert_eq!(node.vllm_native_anthropic_thinking(), native, "{version}");
    }
    node.record_vllm_ready("0.31.0".to_owned());
    node.record_vllm_incompatible(Some("0.31.0".to_owned()), "probe failed".to_owned());
    assert!(!node.vllm_native_anthropic_thinking());
}

#[tokio::test]
async fn rejects_vllm_below_v025() {
    let client = Client::new();
    for version in ["0.24.1", "0.24", "v0.24.9", "0.24.1.dev123", "0.24.1-rc1"] {
        let (base_url, server) = management_server(version).await;
        let node = vllm_node(&base_url);
        poll_node(&client, &node, true).await;
        assert_eq!(
            node.provider_state(),
            ProviderState::Incompatible,
            "{version}"
        );
        assert!(!node.provider_is_ready());

        let candidate = vllm_node(&base_url);
        let error = preflight_vllm(&client, &candidate).await.unwrap_err();
        assert!(error.to_string().contains("requires >= 0.25.0"), "{error}");
        assert!(!candidate.provider_is_ready());
        server.abort();
    }
}

#[tokio::test]
async fn rejects_empty_vllm_versions() {
    let client = Client::new();
    for version in ["", " \t\n"] {
        let (base_url, server) = management_server(version).await;
        let node = vllm_node(&base_url);
        poll_node(&client, &node, true).await;
        assert!(!node.provider_is_ready());

        let candidate = vllm_node(&base_url);
        let error = preflight_vllm(&client, &candidate).await.unwrap_err();
        assert!(error.to_string().contains("version is empty"), "{error}");
        assert!(!candidate.provider_is_ready());
        server.abort();
    }
}

#[tokio::test]
async fn tokenizes_chat_with_the_upstream_model_name() {
    let (base_url, server) = management_server("0.25.0").await;
    let node = vllm_node(&base_url);
    node.record_vllm_ready("0.25.0".to_owned());
    let scheduler = Arc::new(Scheduler::new(
        vec![Arc::clone(&node)],
        crate::config::RoutingConfig::default(),
    ));
    scheduler.exact_cache_directory().configure_node_owned(
        node.id(),
        10,
        usize::MAX,
        node.instance_id(),
    );
    scheduler
        .exact_cache_directory()
        .apply_owned(
            node.id(),
            node.instance_id(),
            vec![CacheMutation::Store {
                hashes: vec![BlockHash::Integer(1)],
                parent: None,
                token_ids: vec![1, 2],
                block_size: 2,
                group: 0,
            }],
        )
        .unwrap();
    let manager = VllmManager::new(scheduler);
    let tokens = manager
        .tokenize_for_routing(
            &Client::new(),
            "chat/completions",
            "public",
            &json!({"messages": [{"role": "user", "content": "hello"}]}),
            true,
        )
        .await;
    assert_eq!(tokens.tokens, Some(vec![1, 2, 3]));
    assert_eq!(tokens.outcome, "upstream_success");
    server.abort();
}

#[tokio::test]
async fn tokenization_failure_does_not_fan_out_to_other_nodes() {
    let (first_url, first_calls, first_server) = tokenize_server(false, Duration::ZERO).await;
    let (second_url, second_calls, second_server) = tokenize_server(true, Duration::ZERO).await;
    let first = vllm_node_with_id("a", &first_url, 500);
    let second = vllm_node_with_id("b", &second_url, 500);
    first.record_vllm_ready("0.25.0".to_owned());
    second.record_vllm_ready("0.25.0".to_owned());
    let scheduler = Arc::new(Scheduler::new(
        vec![Arc::clone(&first), Arc::clone(&second)],
        crate::config::RoutingConfig::default(),
    ));
    for node in [&first, &second] {
        scheduler.exact_cache_directory().configure_node_owned(
            node.id(),
            10,
            usize::MAX,
            node.instance_id(),
        );
        scheduler
            .exact_cache_directory()
            .apply_owned(
                node.id(),
                node.instance_id(),
                vec![CacheMutation::Store {
                    hashes: vec![BlockHash::Integer(1)],
                    parent: None,
                    token_ids: vec![1, 2],
                    block_size: 2,
                    group: 0,
                }],
            )
            .unwrap();
    }
    let manager = VllmManager::new(scheduler);

    let result = manager
        .tokenize_for_routing(
            &Client::new(),
            "chat/completions",
            "public",
            &json!({"messages": [{"role": "user", "content": "hello"}]}),
            true,
        )
        .await;

    assert_eq!(result.outcome, "upstream_error");
    assert_eq!(first_calls.load(Ordering::Relaxed), 1);
    assert_eq!(second_calls.load(Ordering::Relaxed), 0);
    first_server.abort();
    second_server.abort();
}

#[tokio::test]
async fn tokenization_uses_one_total_deadline() {
    let (base_url, calls, server) = tokenize_server(true, Duration::from_millis(200)).await;
    let node = vllm_node_with_id("slow", &base_url, 25);
    node.record_vllm_ready("0.25.0".to_owned());
    let scheduler = Arc::new(Scheduler::new(
        vec![Arc::clone(&node)],
        crate::config::RoutingConfig::default(),
    ));
    scheduler.exact_cache_directory().configure_node_owned(
        node.id(),
        10,
        usize::MAX,
        node.instance_id(),
    );
    scheduler
        .exact_cache_directory()
        .apply_owned(
            node.id(),
            node.instance_id(),
            vec![CacheMutation::Store {
                hashes: vec![BlockHash::Integer(1)],
                parent: None,
                token_ids: vec![1, 2],
                block_size: 2,
                group: 0,
            }],
        )
        .unwrap();
    let manager = VllmManager::new(scheduler);

    let result = manager
        .tokenize_for_routing(
            &Client::new(),
            "chat/completions",
            "public",
            &json!({"messages": [{"role": "user", "content": "hello"}]}),
            true,
        )
        .await;

    assert_eq!(result.outcome, "deadline");
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert!(result.elapsed < Duration::from_millis(150));
    server.abort();
}

#[tokio::test]
async fn tokenization_prefix_gate_avoids_the_upstream_request() {
    let (base_url, calls, server) = tokenize_server(true, Duration::ZERO).await;
    let node = vllm_node_with_id("gated", &base_url, 500);
    node.record_vllm_ready("0.25.0".to_owned());
    let scheduler = Arc::new(Scheduler::new(
        vec![Arc::clone(&node)],
        crate::config::RoutingConfig::default(),
    ));
    scheduler.exact_cache_directory().configure_node_owned(
        node.id(),
        10,
        usize::MAX,
        node.instance_id(),
    );
    scheduler
        .exact_cache_directory()
        .apply_owned(
            node.id(),
            node.instance_id(),
            vec![CacheMutation::Store {
                hashes: vec![BlockHash::Integer(1)],
                parent: None,
                token_ids: vec![1, 2],
                block_size: 2,
                group: 0,
            }],
        )
        .unwrap();
    let manager = VllmManager::new(scheduler);

    let result = manager
        .tokenize_for_routing(
            &Client::new(),
            "chat/completions",
            "public",
            &json!({"messages": [{"role": "user", "content": "first request"}]}),
            false,
        )
        .await;

    assert_eq!(result.outcome, "prefix_gate");
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    server.abort();
}
