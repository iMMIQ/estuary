use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    task::Poll,
    time::Duration,
};

use estuary::{
    config::{HealthConfig, NodeConfig, PrefixConfig, RoutingConfig},
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

fn routing(queue_timeout_ms: u64, queue_max_requests: usize) -> RoutingConfig {
    RoutingConfig {
        queue_timeout_ms,
        queue_max_requests,
        ..RoutingConfig::default()
    }
}

#[tokio::test]
async fn queue_wait_times_out() {
    let node = healthy_node("only", 1);
    let scheduler = Scheduler::new(vec![node], routing(20, 1));
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

    let result = tokio::time::timeout(
        Duration::from_millis(500),
        scheduler.acquire(
            Some("model"),
            prefix::PrefixInput::default(),
            &excluded,
            128,
        ),
    )
    .await
    .expect("scheduler should enforce its queue deadline");

    assert!(matches!(result, Err(GatewayError::CapacityTimeout)));
    drop(held);
}

#[tokio::test]
async fn queue_full_is_rejected() {
    let node = healthy_node("only", 1);
    let scheduler = Scheduler::new(vec![node], routing(1_000, 1));
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

    {
        let first_waiter = scheduler.acquire(
            Some("model"),
            prefix::PrefixInput::default(),
            &excluded,
            128,
        );
        tokio::pin!(first_waiter);
        assert!(matches!(poll!(first_waiter.as_mut()), Poll::Pending));

        let result = scheduler
            .acquire(
                Some("model"),
                prefix::PrefixInput::default(),
                &excluded,
                128,
            )
            .await;
        assert!(matches!(result, Err(GatewayError::QueueFull)));
    }
    drop(held);
}

#[tokio::test]
async fn permit_release_wakes_waiter() {
    let node = healthy_node("only", 1);
    let scheduler = Scheduler::new(vec![node], routing(1_000, 1));
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
    let scheduler = Scheduler::new(vec![Arc::clone(&node)], routing(1_000, 2));
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
    let scheduler = Scheduler::new(vec![node], routing(1_000, 2));
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
    let scheduler = Scheduler::new(vec![first, second], routing(1_000, 1));
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
    let scheduler = Scheduler::new(vec![node], routing(1_000, 2));
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

    let scheduler = Scheduler::new(vec![saturated, Arc::clone(&recovered)], routing(1_000, 1));
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
