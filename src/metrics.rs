use std::sync::{Arc, atomic::AtomicU64};

use anyhow::Result;
use prometheus_client::{
    encoding::{EncodeLabelSet, text::encode},
    metrics::{
        counter::Counter,
        family::Family,
        gauge::Gauge,
        histogram::{Histogram, exponential_buckets},
    },
    registry::Registry,
};

use crate::{
    node::{CircuitState, HealthState, LifecycleState, ProviderState},
    scheduler::Scheduler,
};

#[derive(Clone, Debug, Eq, Hash, PartialEq, EncodeLabelSet)]
struct RequestLabels {
    endpoint: String,
    status: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, EncodeLabelSet)]
struct AttemptLabels {
    node: String,
    outcome: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, EncodeLabelSet)]
struct NodeLabels {
    node: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, EncodeLabelSet)]
struct TokenizationLabels {
    outcome: String,
}

#[derive(Debug)]
pub struct Metrics {
    registry: Registry,
    requests: Family<RequestLabels, Counter>,
    attempts: Family<AttemptLabels, Counter>,
    retries: Family<AttemptLabels, Counter>,
    tokenizations: Family<TokenizationLabels, Counter>,
    stream_cancellations: Family<NodeLabels, Counter>,
    stream_errors: Family<NodeLabels, Counter>,
    node_active: Family<NodeLabels, Gauge>,
    node_health: Family<NodeLabels, Gauge>,
    node_accepting_requests: Family<NodeLabels, Gauge>,
    node_circuit_state: Family<NodeLabels, Gauge>,
    node_provider_ready: Family<NodeLabels, Gauge>,
    node_provider_state: Family<NodeLabels, Gauge>,
    node_upstream_running: Family<NodeLabels, Gauge>,
    node_upstream_waiting: Family<NodeLabels, Gauge>,
    node_kv_cache_usage: Family<NodeLabels, Gauge<f64, AtomicU64>>,
    node_exact_kv_blocks: Family<NodeLabels, Gauge>,
    node_exact_kv_ready: Family<NodeLabels, Gauge>,
    queue_requests: Gauge,
    queue_bytes: Gauge,
    request_duration: Histogram,
    queue_duration: Histogram,
    tokenization_duration: Histogram,
    prefix_match_chars: Histogram,
    prefix_match_tokens: Histogram,
}

impl Metrics {
    #[allow(clippy::too_many_lines)]
    pub fn new() -> Arc<Self> {
        let requests = Family::default();
        let attempts = Family::default();
        let retries = Family::default();
        let tokenizations = Family::default();
        let stream_cancellations = Family::default();
        let stream_errors = Family::default();
        let node_active = Family::default();
        let node_health = Family::default();
        let node_accepting_requests = Family::default();
        let node_circuit_state = Family::default();
        let node_provider_ready = Family::default();
        let node_provider_state = Family::default();
        let node_upstream_running = Family::default();
        let node_upstream_waiting = Family::default();
        let node_kv_cache_usage = Family::<NodeLabels, Gauge<f64, AtomicU64>>::default();
        let node_exact_kv_blocks = Family::default();
        let node_exact_kv_ready = Family::default();
        let queue_requests = Gauge::default();
        let queue_bytes = Gauge::default();
        let request_duration = Histogram::new(exponential_buckets(0.005, 2.0, 18));
        let queue_duration = Histogram::new(exponential_buckets(0.001, 2.0, 16));
        let tokenization_duration = Histogram::new(exponential_buckets(0.000_5, 2.0, 18));
        let prefix_match_chars = Histogram::new(exponential_buckets(128.0, 2.0, 14));
        let prefix_match_tokens = Histogram::new(exponential_buckets(16.0, 2.0, 16));

        let mut registry = Registry::with_prefix("estuary");
        registry.register(
            "requests",
            "Completed downstream responses by endpoint and HTTP status.",
            requests.clone(),
        );
        registry.register(
            "upstream_attempts",
            "Upstream attempts by node and outcome.",
            attempts.clone(),
        );
        registry.register(
            "retries",
            "Internal retry attempts by node and reason.",
            retries.clone(),
        );
        registry.register(
            "tokenization_outcomes",
            "Routing tokenization decisions and results by bounded outcome.",
            tokenizations.clone(),
        );
        registry.register(
            "stream_cancellations",
            "Response bodies dropped before upstream EOF, usually client cancellation.",
            stream_cancellations.clone(),
        );
        registry.register(
            "stream_errors",
            "Errors while reading an upstream response body.",
            stream_errors.clone(),
        );
        registry.register(
            "node_active",
            "Current in-flight requests on each node.",
            node_active.clone(),
        );
        registry.register(
            "node_health",
            "Node health state: starting=0, healthy=1, degraded=2, unhealthy=3.",
            node_health.clone(),
        );
        registry.register(
            "node_accepting_requests",
            "Whether a node is serving, healthy, provider-compatible, and accepted by its circuit breaker.",
            node_accepting_requests.clone(),
        );
        registry.register(
            "node_circuit_state",
            "Circuit breaker state: closed=0, open=1, half_open=2.",
            node_circuit_state.clone(),
        );
        registry.register(
            "node_provider_ready",
            "Whether provider-specific compatibility checks permit routing to the node.",
            node_provider_ready.clone(),
        );
        registry.register(
            "node_provider_state",
            "Provider compatibility state: generic=0, checking=1, ready=2, incompatible=3.",
            node_provider_state.clone(),
        );
        registry.register(
            "node_upstream_running",
            "Latest fresh vLLM running-request gauge by node.",
            node_upstream_running.clone(),
        );
        registry.register(
            "node_upstream_waiting",
            "Latest fresh vLLM waiting-request gauge by node.",
            node_upstream_waiting.clone(),
        );
        registry.register(
            "node_kv_cache_usage_ratio",
            "Latest vLLM KV-cache usage ratio by node.",
            node_kv_cache_usage.clone(),
        );
        registry.register(
            "node_exact_kv_blocks",
            "vLLM KV blocks represented in the exact routing directory.",
            node_exact_kv_blocks.clone(),
        );
        registry.register(
            "node_exact_kv_ready",
            "Whether the node's exact vLLM KV event directory is currently usable.",
            node_exact_kv_ready.clone(),
        );
        registry.register(
            "queue_requests",
            "Current requests waiting for queue admission or an upstream concurrency permit.",
            queue_requests.clone(),
        );
        registry.register(
            "queue_bytes",
            "KiB-rounded request-body bytes held by all queued requests.",
            queue_bytes.clone(),
        );
        registry.register(
            "request_duration_seconds",
            "Time from gateway admission to downstream response creation; includes the full buffered body for non-streaming successes.",
            request_duration.clone(),
        );
        registry.register(
            "queue_duration_seconds",
            "Time spent selecting or waiting for upstream capacity.",
            queue_duration.clone(),
        );
        registry.register(
            "tokenization_duration_seconds",
            "Time spent deciding, caching, or requesting exact routing tokenization.",
            tokenization_duration.clone(),
        );
        registry.register(
            "prefix_match_chars",
            "Longest approximate cached prompt prefix selected, in canonical characters.",
            prefix_match_chars.clone(),
        );
        registry.register(
            "prefix_match_tokens",
            "Longest exact vLLM cached prompt prefix selected, in tokens.",
            prefix_match_tokens.clone(),
        );

        Arc::new(Self {
            registry,
            requests,
            attempts,
            retries,
            tokenizations,
            stream_cancellations,
            stream_errors,
            node_active,
            node_health,
            node_accepting_requests,
            node_circuit_state,
            node_provider_ready,
            node_provider_state,
            node_upstream_running,
            node_upstream_waiting,
            node_kv_cache_usage,
            node_exact_kv_blocks,
            node_exact_kv_ready,
            queue_requests,
            queue_bytes,
            request_duration,
            queue_duration,
            tokenization_duration,
            prefix_match_chars,
            prefix_match_tokens,
        })
    }

    pub fn request(&self, endpoint: &str, status: u16) {
        self.requests
            .get_or_create(&RequestLabels {
                endpoint: endpoint.to_owned(),
                status: status.to_string(),
            })
            .inc();
    }

    pub fn attempt(&self, node: &str, outcome: &str) {
        self.attempts
            .get_or_create(&AttemptLabels {
                node: node.to_owned(),
                outcome: outcome.to_owned(),
            })
            .inc();
    }

    pub fn retry(&self, node: &str, reason: &str) {
        self.retries
            .get_or_create(&AttemptLabels {
                node: node.to_owned(),
                outcome: reason.to_owned(),
            })
            .inc();
    }

    pub fn tokenization(&self, outcome: &str, elapsed: std::time::Duration) {
        self.tokenizations
            .get_or_create(&TokenizationLabels {
                outcome: outcome.to_owned(),
            })
            .inc();
        self.tokenization_duration.observe(elapsed.as_secs_f64());
    }

    pub fn stream_cancelled(&self, node: &str) {
        self.stream_cancellations
            .get_or_create(&NodeLabels {
                node: node.to_owned(),
            })
            .inc();
    }

    pub fn stream_error(&self, node: &str) {
        self.stream_errors
            .get_or_create(&NodeLabels {
                node: node.to_owned(),
            })
            .inc();
    }

    pub fn observe_request_duration(&self, seconds: f64) {
        self.request_duration.observe(seconds);
    }

    pub fn observe_queue_duration(&self, seconds: f64) {
        self.queue_duration.observe(seconds);
    }

    pub fn observe_prefix_match(&self, chars: usize) {
        self.prefix_match_chars.observe(chars as f64);
    }

    pub fn observe_prefix_match_tokens(&self, tokens: usize) {
        self.prefix_match_tokens.observe(tokens as f64);
    }

    pub fn encode(&self, scheduler: &Scheduler) -> Result<String> {
        for node in scheduler.nodes() {
            let labels = NodeLabels {
                node: node.id().to_owned(),
            };
            self.node_active
                .get_or_create(&labels)
                .set(node.active().try_into().unwrap_or(i64::MAX));
            let health = match node.health() {
                HealthState::Starting => 0,
                HealthState::Healthy => 1,
                HealthState::Degraded => 2,
                HealthState::Unhealthy => 3,
            };
            self.node_health.get_or_create(&labels).set(health);
            let snapshot = node.snapshot();
            self.node_accepting_requests
                .get_or_create(&labels)
                .set(i64::from(
                    snapshot.lifecycle == LifecycleState::Serving && node.is_routable(),
                ));
            let circuit = match snapshot.circuit {
                CircuitState::Closed => 0,
                CircuitState::Open => 1,
                CircuitState::HalfOpen => 2,
            };
            self.node_circuit_state.get_or_create(&labels).set(circuit);
            self.node_provider_ready
                .get_or_create(&labels)
                .set(i64::from(node.provider_is_ready()));
            let provider_state = match snapshot.provider_state {
                ProviderState::Generic => 0,
                ProviderState::Checking => 1,
                ProviderState::Ready => 2,
                ProviderState::Incompatible => 3,
            };
            self.node_provider_state
                .get_or_create(&labels)
                .set(provider_state);
            self.node_upstream_running.get_or_create(&labels).set(
                snapshot
                    .upstream_running
                    .unwrap_or_default()
                    .try_into()
                    .unwrap_or(i64::MAX),
            );
            self.node_upstream_waiting.get_or_create(&labels).set(
                snapshot
                    .upstream_waiting
                    .unwrap_or_default()
                    .try_into()
                    .unwrap_or(i64::MAX),
            );
            self.node_kv_cache_usage
                .get_or_create(&labels)
                .set(snapshot.kv_cache_usage.unwrap_or_default());
            let cache = scheduler.exact_cache_directory().snapshot(node.id());
            self.node_exact_kv_blocks
                .get_or_create(&labels)
                .set(cache.blocks.try_into().unwrap_or(i64::MAX));
            self.node_exact_kv_ready
                .get_or_create(&labels)
                .set(i64::from(cache.authoritative));
        }
        let (queued_requests, queued_bytes) = scheduler.queue_snapshot();
        self.queue_requests
            .set(queued_requests.try_into().unwrap_or(i64::MAX));
        self.queue_bytes
            .set(queued_bytes.try_into().unwrap_or(i64::MAX));
        let mut output = String::new();
        encode(&mut output, &self.registry)?;
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{
        config::{NodeConfig, RoutingConfig},
        node::Node,
    };

    use super::*;

    #[test]
    fn encodes_provider_and_exact_cache_metrics() {
        let node = Node::from_config(&NodeConfig {
            id: "node".to_owned(),
            base_url: "http://node.invalid/v1".to_owned(),
            models: HashMap::from([("model".to_owned(), "model".to_owned())]),
            ..NodeConfig::default()
        })
        .unwrap();
        let scheduler = Scheduler::new(vec![node], RoutingConfig::default());
        let metrics = Metrics::new();
        metrics.tokenization("prefix_gate", std::time::Duration::from_millis(2));
        let output = metrics.encode(&scheduler).unwrap();
        assert!(output.contains("estuary_node_provider_ready"));
        assert!(output.contains("estuary_node_exact_kv_blocks"));
        assert!(output.contains("estuary_node_kv_cache_usage_ratio"));
        assert!(output.contains("estuary_tokenization_outcomes_total{outcome=\"prefix_gate\"} 1"));
        assert!(output.contains("estuary_tokenization_duration_seconds_count 1"));
    }
}
