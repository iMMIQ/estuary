use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use estuary::{
    config::{NodeConfig, PrefixConfig, RoutingConfig},
    node::Node,
    prefix::{self, PrefixInput},
    scheduler::Scheduler,
};
use serde_json::json;

const NODE_COUNT: usize = 8;
const NODE_CAPACITY: usize = 16;
const REQUEST_LATENCY: Duration = Duration::from_millis(100);

fn scheduler() -> Scheduler {
    let nodes = (0..NODE_COUNT)
        .map(|index| {
            Node::from_config(&NodeConfig {
                id: format!("node-{index}"),
                base_url: format!("http://node-{index}.invalid/v1"),
                models: HashMap::from([("model".to_owned(), "model".to_owned())]),
                max_concurrency: NODE_CAPACITY,
                ..NodeConfig::default()
            })
            .unwrap()
        })
        .collect::<Vec<Arc<Node>>>();
    for node in &nodes {
        node.record_request_success(REQUEST_LATENCY);
    }
    Scheduler::new(nodes, RoutingConfig::default())
}

async fn select(scheduler: &Scheduler, prefix_input: PrefixInput) -> estuary::scheduler::Selection {
    scheduler
        .acquire(Some("model"), prefix_input, &HashSet::new(), 128)
        .await
        .unwrap()
}

fn cached_prefix(scheduler: &Scheduler, node_id: &str) -> PrefixInput {
    let input = prefix::routing_text(
        "chat/completions",
        Some("model"),
        Some(&json!({"messages": [{"role": "user", "content": "shared hot prompt"}]})),
        &PrefixConfig::default(),
    );
    scheduler.prefix_directory().record(node_id, &input);
    input
}

fn assert_balanced(counts: &[usize]) {
    let total = counts.iter().sum::<usize>();
    let expected = total / counts.len();
    let largest_deviation = counts
        .iter()
        .map(|count| count.abs_diff(expected))
        .max()
        .unwrap();
    eprintln!("requests={total}, per_node={counts:?}, expected={expected}");
    assert!(
        largest_deviation <= total / 100,
        "node distribution deviated by more than 1%: {counts:?}"
    );
}

#[tokio::test]
#[ignore = "scheduler stress test"]
async fn sustained_capacity_pressure_stays_balanced() {
    let scheduler = scheduler();
    let mut counts = [0; NODE_COUNT];

    for _ in 0..1_000 {
        let batch_size = NODE_COUNT * NODE_CAPACITY;
        let mut held = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            let selection = select(&scheduler, PrefixInput::default()).await;
            counts[selection.node.id()[5..].parse::<usize>().unwrap()] += 1;
            selection.lease.record_success(REQUEST_LATENCY);
            held.push(selection);
        }
    }

    assert_balanced(&counts);
}

#[tokio::test]
#[ignore = "scheduler stress test"]
async fn long_running_low_overlap_traffic_stays_balanced() {
    let scheduler = scheduler();
    let mut counts = [0; NODE_COUNT];

    // ponytail: accelerated lease cycles model a long deployment; use an end-to-end soak for network drift.
    for _ in 0..100_000 {
        let selection = select(&scheduler, PrefixInput::default()).await;
        counts[selection.node.id()[5..].parse::<usize>().unwrap()] += 1;
        selection.lease.record_success(REQUEST_LATENCY);
    }

    assert_balanced(&counts);
}

#[tokio::test]
#[ignore = "scheduler stress test"]
async fn hot_prefix_low_overlap_concentrates_on_cache_owner() {
    let scheduler = scheduler();
    let input = cached_prefix(&scheduler, "node-7");
    let mut counts = [0; NODE_COUNT];

    for _ in 0..100_000 {
        let selection = select(&scheduler, input.clone()).await;
        counts[selection.node.id()[5..].parse::<usize>().unwrap()] += 1;
        selection.lease.record_success(REQUEST_LATENCY);
    }

    eprintln!("hot-prefix requests=100000, per_node={counts:?}");
    assert_eq!(counts[7], 100_000);
}

#[tokio::test]
#[ignore = "scheduler stress test"]
async fn hot_prefix_under_capacity_pressure_stays_balanced() {
    let scheduler = scheduler();
    let input = cached_prefix(&scheduler, "node-7");
    let mut counts = [0; NODE_COUNT];

    for _ in 0..1_000 {
        let batch_size = NODE_COUNT * NODE_CAPACITY;
        let mut held = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            let selection = select(&scheduler, input.clone()).await;
            counts[selection.node.id()[5..].parse::<usize>().unwrap()] += 1;
            selection.lease.record_success(REQUEST_LATENCY);
            held.push(selection);
        }
    }

    assert_balanced(&counts);
}
