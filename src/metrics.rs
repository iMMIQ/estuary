use std::sync::Arc;

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

use crate::{node::HealthState, scheduler::Scheduler};

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

#[derive(Debug)]
pub struct Metrics {
    registry: Registry,
    requests: Family<RequestLabels, Counter>,
    attempts: Family<AttemptLabels, Counter>,
    retries: Family<AttemptLabels, Counter>,
    stream_cancellations: Family<NodeLabels, Counter>,
    stream_errors: Family<NodeLabels, Counter>,
    node_active: Family<NodeLabels, Gauge>,
    node_health: Family<NodeLabels, Gauge>,
    queue_requests: Gauge,
    queue_bytes: Gauge,
    request_duration: Histogram,
    queue_duration: Histogram,
    prefix_match_chars: Histogram,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        let requests = Family::default();
        let attempts = Family::default();
        let retries = Family::default();
        let stream_cancellations = Family::default();
        let stream_errors = Family::default();
        let node_active = Family::default();
        let node_health = Family::default();
        let queue_requests = Gauge::default();
        let queue_bytes = Gauge::default();
        let request_duration = Histogram::new(exponential_buckets(0.005, 2.0, 18));
        let queue_duration = Histogram::new(exponential_buckets(0.001, 2.0, 16));
        let prefix_match_chars = Histogram::new(exponential_buckets(128.0, 2.0, 14));

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
            "queue_requests",
            "Current requests waiting for an upstream concurrency permit.",
            queue_requests.clone(),
        );
        registry.register(
            "queue_bytes",
            "Reserved raw request-body bytes for requests waiting in the queue.",
            queue_bytes.clone(),
        );
        registry.register(
            "request_duration_seconds",
            "Time from gateway admission to upstream response headers.",
            request_duration.clone(),
        );
        registry.register(
            "queue_duration_seconds",
            "Time spent selecting or waiting for upstream capacity.",
            queue_duration.clone(),
        );
        registry.register(
            "prefix_match_chars",
            "Longest approximate cached prompt prefix selected, in canonical characters.",
            prefix_match_chars.clone(),
        );

        Arc::new(Self {
            registry,
            requests,
            attempts,
            retries,
            stream_cancellations,
            stream_errors,
            node_active,
            node_health,
            queue_requests,
            queue_bytes,
            request_duration,
            queue_duration,
            prefix_match_chars,
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
