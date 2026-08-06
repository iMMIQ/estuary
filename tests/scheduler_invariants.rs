use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    task::Poll,
    time::Duration,
};

use estuary::{
    config::{HealthConfig, NodeConfig, PrefixConfig, ProviderConfig, ProviderKind, RoutingConfig},
    error::GatewayError,
    node::{HealthState, Node},
    prefix,
    scheduler::Scheduler,
};
use futures_util::poll;
use serde_json::Value;
use tokio::sync::Notify;

fn strict_health() -> HealthConfig {
    HealthConfig {
        healthy_threshold: 1,
        unhealthy_threshold: 1,
        passive_failure_threshold: 1,
        ..HealthConfig::default()
    }
}

fn healthy_node(id: &str, max_concurrency: usize) -> Arc<Node> {
    healthy_node_for_model(id, "model", max_concurrency)
}

fn healthy_node_for_model(id: &str, model: &str, max_concurrency: usize) -> Arc<Node> {
    let node = Node::from_config(&NodeConfig {
        id: id.to_owned(),
        base_url: format!("http://{id}.invalid/v1"),
        models: HashMap::from([(model.to_owned(), model.to_owned())]),
        max_concurrency,
        ..NodeConfig::default()
    })
    .expect("valid node configuration");
    node.record_probe_success(&strict_health());
    assert_eq!(node.health(), HealthState::Healthy);
    node
}

fn healthy_vllm_node(id: &str, max_concurrency: usize, waiting_threshold: usize) -> Arc<Node> {
    let node = Node::from_config(&NodeConfig {
        id: id.to_owned(),
        base_url: format!("http://{id}.invalid/v1"),
        models: HashMap::from([("model".to_owned(), "model".to_owned())]),
        max_concurrency,
        provider: ProviderConfig {
            kind: ProviderKind::Vllm,
            waiting_threshold,
            ..ProviderConfig::default()
        },
        ..NodeConfig::default()
    })
    .expect("valid vLLM node configuration");
    node.record_vllm_ready("0.25.0".to_owned());
    node.record_probe_success(&strict_health());
    node
}

fn routing(queue_max_requests: usize) -> RoutingConfig {
    RoutingConfig {
        queue_max_requests,
        ..RoutingConfig::default()
    }
}

#[tokio::test]
async fn queue_waits_until_capacity_is_released() {
    let node = healthy_node("only", 1);
    let scheduler = Scheduler::new(vec![node], routing(1));
    let excluded = HashSet::new();
    let held = scheduler
        .acquire(
            Some("model"),
            prefix::PrefixInput::default(),
            &excluded,
            128,
        )
        .await
        .expect("initial selection");

    let mut waiter = Box::pin(scheduler.acquire(
        Some("model"),
        prefix::PrefixInput::default(),
        &excluded,
        128,
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), waiter.as_mut())
            .await
            .is_err(),
        "capacity pressure must keep the request queued"
    );
    drop(held);
    let selected = tokio::time::timeout(Duration::from_millis(250), waiter.as_mut())
        .await
        .expect("permit release should wake the request")
        .expect("queued request should be selected");
    assert_eq!(selected.node.id(), "only");
}

#[tokio::test]
async fn multiple_node_waiters_remain_ordered_instead_of_rejecting() {
    let node = healthy_node("only", 1);
    let scheduler = Scheduler::new(vec![node], routing(1));
    let excluded = HashSet::new();
    let held = scheduler
        .acquire(
            Some("model"),
            prefix::PrefixInput::default(),
            &excluded,
            128,
        )
        .await
        .expect("initial selection");

    let mut first_waiter = Box::pin(scheduler.acquire(
        Some("model"),
        prefix::PrefixInput::default(),
        &excluded,
        128,
    ));
    assert!(matches!(poll!(first_waiter.as_mut()), Poll::Pending));
    let mut second_waiter = Box::pin(scheduler.acquire(
        Some("model"),
        prefix::PrefixInput::default(),
        &excluded,
        128,
    ));
    assert!(matches!(poll!(second_waiter.as_mut()), Poll::Pending));

    drop(held);
    let first = tokio::time::timeout(Duration::from_millis(250), first_waiter.as_mut())
        .await
        .expect("oldest waiter should receive released capacity")
        .expect("first queued selection");
    assert!(matches!(poll!(second_waiter.as_mut()), Poll::Pending));
    drop(first);
    let second = tokio::time::timeout(Duration::from_millis(250), second_waiter.as_mut())
        .await
        .expect("admission waiter should eventually enter the node queue")
        .expect("second queued selection");
    assert_eq!(second.node.id(), "only");
}

#[tokio::test]
async fn ingress_request_admission_waits_before_entering_the_gateway() {
    let scheduler = Scheduler::new(Vec::new(), routing(1));
    let first = scheduler.admit_ingress(128).await;
    let second = scheduler.admit_ingress(128);
    tokio::pin!(second);
    assert!(matches!(poll!(second.as_mut()), Poll::Pending));

    drop(first);
    tokio::time::timeout(Duration::from_millis(250), second.as_mut())
        .await
        .expect("request admission should wake after the budget is released");
}

#[tokio::test]
async fn ingress_byte_admission_is_bounded_independently_of_request_count() {
    let scheduler = Scheduler::new(
        Vec::new(),
        RoutingConfig {
            queue_max_requests: 2,
            queue_max_bytes: 1024,
            ..RoutingConfig::default()
        },
    );
    let first = scheduler.admit_ingress(1024).await;
    let second = scheduler.admit_ingress(1024);
    tokio::pin!(second);
    assert!(matches!(poll!(second.as_mut()), Poll::Pending));

    drop(first);
    tokio::time::timeout(Duration::from_millis(250), second.as_mut())
        .await
        .expect("byte admission should wake after the budget is released");
}

#[tokio::test]
async fn permit_release_wakes_waiter() {
    let node = healthy_node("only", 1);
    let scheduler = Scheduler::new(vec![node], routing(1));
    let excluded = HashSet::new();
    let held = scheduler
        .acquire(
            Some("model"),
            prefix::PrefixInput::default(),
            &excluded,
            128,
        )
        .await
        .expect("initial selection");

    let waiter = scheduler.acquire(
        Some("model"),
        prefix::PrefixInput::default(),
        &excluded,
        128,
    );
    tokio::pin!(waiter);
    assert!(matches!(poll!(waiter.as_mut()), Poll::Pending));

    drop(held);
    let selected = tokio::time::timeout(Duration::from_millis(250), waiter.as_mut())
        .await
        .expect("permit release should wake the queued request")
        .expect("queued request should acquire the released permit");
    assert_eq!(selected.node.id(), "only");
}

#[tokio::test(flavor = "current_thread")]
async fn queued_waiter_cannot_be_bypassed_by_new_fast_path() {
    let node = healthy_node("only", 1);
    let scheduler = Scheduler::new(vec![Arc::clone(&node)], routing(2));
    let excluded = HashSet::new();
    let held = scheduler
        .acquire(
            Some("model"),
            prefix::PrefixInput::default(),
            &excluded,
            128,
        )
        .await
        .expect("initial selection");

    let mut oldest = Box::pin(scheduler.acquire(
        Some("model"),
        prefix::PrefixInput::default(),
        &excluded,
        128,
    ));
    assert!(matches!(poll!(oldest.as_mut()), Poll::Pending));
    drop(held);

    let mut newcomer = Box::pin(scheduler.acquire(
        Some("model"),
        prefix::PrefixInput::default(),
        &excluded,
        128,
    ));
    assert!(matches!(poll!(newcomer.as_mut()), Poll::Pending));
    assert_eq!(node.active(), 0);
    assert_eq!(node.snapshot().available, 0);

    let oldest_selection = match poll!(oldest.as_mut()) {
        Poll::Ready(result) => result.expect("oldest waiter should receive the permit"),
        Poll::Pending => panic!("oldest waiter was bypassed"),
    };
    drop(oldest_selection);

    let newcomer_selection = tokio::time::timeout(Duration::from_millis(250), newcomer.as_mut())
        .await
        .expect("newcomer should receive the next permit")
        .expect("newcomer selection");
    assert_eq!(newcomer_selection.node.id(), "only");
}

#[tokio::test(flavor = "current_thread")]
async fn cancelling_assigned_waiter_hands_permit_to_next_waiter() {
    let node = healthy_node("only", 1);
    let scheduler = Scheduler::new(vec![node], routing(2));
    let excluded = HashSet::new();
    let held = scheduler
        .acquire(
            Some("model"),
            prefix::PrefixInput::default(),
            &excluded,
            128,
        )
        .await
        .expect("initial selection");

    let mut oldest = Box::pin(scheduler.acquire(
        Some("model"),
        prefix::PrefixInput::default(),
        &excluded,
        128,
    ));
    let mut younger = Box::pin(scheduler.acquire(
        Some("model"),
        prefix::PrefixInput::default(),
        &excluded,
        128,
    ));
    assert!(matches!(poll!(oldest.as_mut()), Poll::Pending));
    assert!(matches!(poll!(younger.as_mut()), Poll::Pending));

    drop(held);
    drop(oldest);

    let selection = tokio::time::timeout(Duration::from_millis(250), younger.as_mut())
        .await
        .expect("cancellation should hand the assigned permit to the next waiter")
        .expect("younger selection");
    assert_eq!(selection.node.id(), "only");
}

#[tokio::test(flavor = "current_thread")]
async fn queued_model_does_not_block_an_independent_model() {
    let first = healthy_node_for_model("first", "model-a", 1);
    let second = healthy_node_for_model("second", "model-b", 1);
    let scheduler = Scheduler::new(vec![first, second], routing(1));
    let excluded = HashSet::new();
    let held = scheduler
        .acquire(
            Some("model-a"),
            prefix::PrefixInput::default(),
            &excluded,
            128,
        )
        .await
        .expect("initial model-a selection");
    let mut queued = Box::pin(scheduler.acquire(
        Some("model-a"),
        prefix::PrefixInput::default(),
        &excluded,
        128,
    ));
    assert!(matches!(poll!(queued.as_mut()), Poll::Pending));

    let independent = scheduler
        .acquire(
            Some("model-b"),
            prefix::PrefixInput::default(),
            &excluded,
            128,
        )
        .await
        .expect("model-b should bypass the unrelated queue");
    assert_eq!(independent.node.id(), "second");

    drop(independent);
    drop(queued);
    drop(held);
}

#[tokio::test(flavor = "current_thread")]
async fn state_notification_does_not_reorder_node_waiters() {
    let node = healthy_node("only", 1);
    let scheduler = Scheduler::new(vec![node], routing(2));
    let excluded = HashSet::new();
    let held = scheduler
        .acquire(
            Some("model"),
            prefix::PrefixInput::default(),
            &excluded,
            128,
        )
        .await
        .expect("initial selection");
    let mut oldest = Box::pin(scheduler.acquire(
        Some("model"),
        prefix::PrefixInput::default(),
        &excluded,
        128,
    ));
    let mut younger = Box::pin(scheduler.acquire(
        Some("model"),
        prefix::PrefixInput::default(),
        &excluded,
        128,
    ));
    assert!(matches!(poll!(oldest.as_mut()), Poll::Pending));
    assert!(matches!(poll!(younger.as_mut()), Poll::Pending));

    scheduler.notify_state_change();
    assert!(matches!(poll!(younger.as_mut()), Poll::Pending));
    drop(held);
    assert!(matches!(poll!(younger.as_mut()), Poll::Pending));

    let oldest_selection = match poll!(oldest.as_mut()) {
        Poll::Ready(result) => result.expect("oldest waiter should retain its FIFO position"),
        Poll::Pending => panic!("state notification reordered the waiters"),
    };
    drop(oldest_selection);
    let younger_selection = tokio::time::timeout(Duration::from_millis(250), younger.as_mut())
        .await
        .expect("younger waiter should receive the following permit")
        .expect("younger selection");
    assert_eq!(younger_selection.node.id(), "only");
}

#[tokio::test(flavor = "current_thread")]
async fn recovered_node_is_added_without_losing_existing_fifo_position() {
    let saturated = healthy_node("saturated", 1);
    let recovered = healthy_node("recovered", 1);
    recovered.record_probe_failure("forced failure", &strict_health());
    assert_eq!(recovered.health(), HealthState::Unhealthy);

    let scheduler = Scheduler::new(vec![saturated, Arc::clone(&recovered)], routing(1));
    let excluded = HashSet::new();
    let held = scheduler
        .acquire(
            Some("model"),
            prefix::PrefixInput::default(),
            &excluded,
            128,
        )
        .await
        .expect("initial selection");
    assert_eq!(held.node.id(), "saturated");
    let mut queued = Box::pin(scheduler.acquire(
        Some("model"),
        prefix::PrefixInput::default(),
        &excluded,
        128,
    ));
    assert!(matches!(poll!(queued.as_mut()), Poll::Pending));

    recovered.record_probe_success(&strict_health());
    scheduler.notify_state_change();
    let selection = tokio::time::timeout(Duration::from_millis(250), queued.as_mut())
        .await
        .expect("recovered node should join the pending acquisition race")
        .expect("recovered-node selection");
    assert_eq!(selection.node.id(), "recovered");

    drop(selection);
    drop(held);
}

#[tokio::test]
async fn vllm_waiting_watermark_spills_a_cached_request_to_an_idle_node() {
    let cached = healthy_vllm_node("cached", 4, 2);
    let fallback = healthy_node("fallback", 4);
    cached.record_vllm_telemetry(0, 2, None);
    let mut config = RoutingConfig::default();
    config.prefix.balance_abs_threshold = usize::MAX;
    let scheduler = Scheduler::new(vec![Arc::clone(&cached), fallback], config.clone());
    let body = serde_json::json!({
        "messages": [{"role": "user", "content": "a cached agent conversation"}]
    });
    let input = prefix::routing_text(
        "chat/completions",
        Some("model"),
        Some(&body),
        &config.prefix,
    );
    scheduler.prefix_directory().record("cached", &input);

    let selected = scheduler
        .acquire(Some("model"), input, &HashSet::new(), 128)
        .await
        .expect("idle fallback should accept the request");
    assert_eq!(selected.node.id(), "fallback");
}

#[tokio::test]
async fn vllm_waiting_watermark_queues_until_fresh_telemetry_recovers() {
    let node = healthy_vllm_node("only", 4, 2);
    node.record_vllm_telemetry(1, 2, None);
    let scheduler = Scheduler::new(vec![Arc::clone(&node)], RoutingConfig::default());
    let excluded = HashSet::new();
    let mut waiting = Box::pin(scheduler.acquire(
        Some("model"),
        prefix::PrefixInput::default(),
        &excluded,
        128,
    ));
    assert!(matches!(poll!(waiting.as_mut()), Poll::Pending));

    node.record_vllm_telemetry(1, 0, None);
    scheduler.notify_state_change();
    let selected = tokio::time::timeout(Duration::from_millis(250), waiting.as_mut())
        .await
        .expect("fresh telemetry should release the queued request")
        .expect("recovered vLLM selection");
    assert_eq!(selected.node.id(), "only");
}

#[tokio::test]
async fn unhealthy_node_is_never_selected() {
    let preferred = healthy_node("preferred", 2);
    let fallback = healthy_node("fallback", 2);
    preferred.record_probe_failure("forced failure", &strict_health());
    assert_eq!(preferred.health(), HealthState::Unhealthy);

    let prefix_config = PrefixConfig::default();
    let routing = RoutingConfig {
        prefix: prefix_config.clone(),
        ..RoutingConfig::default()
    };
    let scheduler = Scheduler::new(vec![preferred, fallback], routing);
    let body = serde_json::json!({
        "messages": [{"role": "user", "content": "a shared prompt worth caching"}]
    });
    let prefix_input = prefix::routing_text(
        "chat/completions",
        Some("model"),
        Some(&body),
        &prefix_config,
    );
    scheduler
        .prefix_directory()
        .record("preferred", &prefix_input);

    let selected = scheduler
        .acquire(Some("model"), prefix_input, &HashSet::new(), 128)
        .await
        .expect("healthy fallback should remain available");
    assert_eq!(selected.node.id(), "fallback");
}

#[tokio::test]
async fn draining_stops_new_work_without_cancelling_active_lease() {
    let node = healthy_node("only", 1);
    let scheduler = Scheduler::new(vec![Arc::clone(&node)], routing(1));
    let held = scheduler
        .acquire(
            Some("model"),
            prefix::PrefixInput::default(),
            &HashSet::new(),
            128,
        )
        .await
        .expect("initial selection");

    scheduler.set_node_draining("only", true).unwrap();
    assert_eq!(node.active(), 1);
    assert!(matches!(
        scheduler
            .acquire(
                Some("model"),
                prefix::PrefixInput::default(),
                &HashSet::new(),
                128,
            )
            .await,
        Err(GatewayError::NoHealthyNode(_))
    ));

    drop(held);
    assert!(
        scheduler
            .wait_for_node_idle(&node, Duration::from_millis(100))
            .await
    );
    scheduler.set_node_draining("only", false).unwrap();
    let resumed = scheduler
        .acquire(
            Some("model"),
            prefix::PrefixInput::default(),
            &HashSet::new(),
            128,
        )
        .await
        .expect("resumed node should accept work");
    drop(resumed);
}

#[tokio::test]
async fn prefix_affinity_escapes_overloaded_node() {
    let cached = healthy_node("cached", 4);
    let idle = healthy_node("idle", 4);
    let prefix_config = PrefixConfig {
        balance_abs_threshold: 1,
        ..PrefixConfig::default()
    };
    let routing = RoutingConfig {
        load_weight: 1.0,
        latency_weight: 0.0,
        error_weight: 0.0,
        prefix: prefix_config.clone(),
        ..RoutingConfig::default()
    };
    let scheduler = Scheduler::new(vec![Arc::clone(&cached), idle], routing);
    let body = serde_json::json!({
        "messages": [{"role": "user", "content": "a shared prompt worth caching"}]
    });
    let prefix_input = prefix::routing_text(
        "chat/completions",
        Some("model"),
        Some(&body),
        &prefix_config,
    );
    scheduler.prefix_directory().record("cached", &prefix_input);

    let _held_one = cached
        .try_acquire(Arc::new(Notify::new()))
        .expect("first cached-node lease");
    let _held_two = cached
        .try_acquire(Arc::new(Notify::new()))
        .expect("second cached-node lease");

    let selected = scheduler
        .acquire(Some("model"), prefix_input, &HashSet::new(), 128)
        .await
        .expect("idle node should be selected");
    assert_eq!(selected.node.id(), "idle");
    assert_eq!(selected.prefix_match_chars, 0);
}

#[test]
fn routing_text_is_canonical_across_object_key_order() {
    let first: Value = serde_json::from_str(
        r#"{"model":"model","messages":[{"role":"user","content":{"b":2,"a":1}}]}"#,
    )
    .expect("first JSON body");
    let second: Value = serde_json::from_str(
        r#"{"messages":[{"content":{"a":1,"b":2},"role":"user"}],"model":"model"}"#,
    )
    .expect("second JSON body");
    let config = PrefixConfig::default();
    let first = prefix::routing_text("chat/completions", Some("model"), Some(&first), &config);
    let second = prefix::routing_text("chat/completions", Some("model"), Some(&second), &config);
    let directory = prefix::PrefixDirectory::new(&config);
    directory.record("node", &first);
    let matched = directory.best_match(&second);

    assert_eq!(matched.node_ids, ["node"]);
    assert_eq!(matched.matched_chars, matched.input_chars);
}
