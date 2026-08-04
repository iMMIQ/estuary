use std::{cmp::Ordering, collections::HashSet, future::Future, pin::Pin, sync::Arc};

use futures_util::{StreamExt, stream::FuturesUnordered};
use tokio::{
    sync::{Notify, OwnedSemaphorePermit, Semaphore},
    time::{Instant, sleep_until},
};

use crate::{
    config::RoutingConfig,
    error::GatewayError,
    node::{HealthState, Node, NodeLease, NodeReservation},
    prefix::{PrefixDirectory, PrefixInput},
};

#[derive(Debug)]
pub struct Selection {
    pub node: Arc<Node>,
    pub lease: NodeLease,
    pub upstream_model: Option<String>,
    pub prefix_match_chars: usize,
    pub score: f64,
}

#[derive(Debug)]
struct Candidate {
    node_index: usize,
    node: Arc<Node>,
    upstream_model: Option<String>,
    prefix_match_chars: usize,
    cache_preferred: bool,
    score: f64,
}

type PendingAcquisition =
    Pin<Box<dyn Future<Output = (Candidate, NodeReservation)> + Send + 'static>>;

#[derive(Debug)]
pub struct Scheduler {
    nodes: Vec<Arc<Node>>,
    config: RoutingConfig,
    prefix: Arc<PrefixDirectory>,
    notify: Arc<Notify>,
    queue_requests: Arc<Semaphore>,
    queue_kib: Arc<Semaphore>,
}

impl Scheduler {
    pub fn new(nodes: Vec<Arc<Node>>, config: RoutingConfig) -> Self {
        let prefix = Arc::new(PrefixDirectory::new(&config.prefix));
        let queue_max_requests = config.queue_max_requests;
        let queue_max_kib = config.queue_max_bytes.div_ceil(1024).min(u32::MAX as usize);
        Self {
            nodes,
            config,
            prefix,
            notify: Arc::new(Notify::new()),
            queue_requests: Arc::new(Semaphore::new(queue_max_requests)),
            queue_kib: Arc::new(Semaphore::new(queue_max_kib)),
        }
    }

    pub fn nodes(&self) -> &[Arc<Node>] {
        &self.nodes
    }

    pub fn prefix_directory(&self) -> &Arc<PrefixDirectory> {
        &self.prefix
    }

    pub async fn acquire(
        &self,
        model: Option<&str>,
        prefix_input: PrefixInput,
        excluded: &HashSet<String>,
        body_bytes: usize,
    ) -> Result<Selection, GatewayError> {
        if let Some(selection) = self.try_acquire(model, &prefix_input, excluded)? {
            return Ok(selection);
        }

        let _queue_request = Arc::clone(&self.queue_requests)
            .try_acquire_owned()
            .map_err(|_| GatewayError::QueueFull)?;
        let body_kib = u32::try_from(body_bytes.div_ceil(1024).max(1).min(u32::MAX as usize))
            .unwrap_or(u32::MAX);
        let _queue_bytes: OwnedSemaphorePermit = Arc::clone(&self.queue_kib)
            .try_acquire_many_owned(body_kib)
            .map_err(|_| GatewayError::QueueFull)?;
        let deadline = Instant::now() + self.config.queue_timeout();

        let mut registered = HashSet::new();
        let mut acquisitions = FuturesUnordered::<PendingAcquisition>::new();
        loop {
            let state_changed = self.notify.notified();
            tokio::pin!(state_changed);
            state_changed.as_mut().enable();

            for candidate in self.ranked_candidates(model, &prefix_input, excluded)? {
                if registered.insert(candidate.node_index) {
                    acquisitions.push(Box::pin(async move {
                        let reservation = Arc::clone(&candidate.node).reserve().await;
                        (candidate, reservation)
                    }));
                }
            }

            tokio::select! {
                biased;
                acquired = acquisitions.next() => {
                    let Some((candidate, reservation)) = acquired else {
                        return Err(GatewayError::CapacityTimeout);
                    };
                    registered.remove(&candidate.node_index);
                    if !candidate.node.is_routable() {
                        drop(reservation);
                        continue;
                    }
                    let refreshed = self
                        .ranked_candidates(
                            model,
                            &prefix_input,
                            excluded,
                        )?
                        .into_iter()
                        .find(|item| item.node_index == candidate.node_index);
                    let Some(candidate) = refreshed else {
                        drop(reservation);
                        continue;
                    };
                    if !candidate.node.is_routable() {
                        drop(reservation);
                        continue;
                    }
                    let lease = reservation.commit(Arc::clone(&self.notify));
                    return Ok(Selection {
                        node: candidate.node,
                        lease,
                        upstream_model: candidate.upstream_model,
                        prefix_match_chars: candidate.prefix_match_chars,
                        score: candidate.score,
                    });
                }
                () = sleep_until(deadline) => return Err(GatewayError::CapacityTimeout),
                () = state_changed => {}
            }
        }
    }

    fn try_acquire(
        &self,
        model: Option<&str>,
        prefix_input: &PrefixInput,
        excluded: &HashSet<String>,
    ) -> Result<Option<Selection>, GatewayError> {
        for candidate in self.ranked_candidates(model, prefix_input, excluded)? {
            if let Some(lease) = candidate.node.try_acquire(Arc::clone(&self.notify)) {
                if !candidate.node.is_routable() {
                    drop(lease);
                    continue;
                }
                return Ok(Some(Selection {
                    node: candidate.node,
                    lease,
                    upstream_model: candidate.upstream_model,
                    prefix_match_chars: candidate.prefix_match_chars,
                    score: candidate.score,
                }));
            }
        }
        Ok(None)
    }

    fn ranked_candidates(
        &self,
        model: Option<&str>,
        prefix_input: &PrefixInput,
        excluded: &HashSet<String>,
    ) -> Result<Vec<Candidate>, GatewayError> {
        let mut model_nodes = 0usize;
        let mut healthy_nodes = 0usize;
        let prefix_match = self.prefix.best_match(prefix_input);
        let mut candidates = Vec::new();

        for (node_index, node) in self.nodes.iter().enumerate() {
            if excluded.contains(node.id()) {
                continue;
            }
            let Some(upstream_model) = node.upstream_model(model) else {
                continue;
            };
            model_nodes += 1;
            let health = node.health();
            if !node.is_health_state_routable(health) {
                continue;
            }
            healthy_nodes += 1;

            let active = node.active() as f64;
            let capacity = node.max_concurrency() as f64;
            let load = ((active + 1.0) / capacity) / node.weight();
            let (latency_ms, error_ewma) = node.score_stats();
            let normalized_latency = if latency_ms == 0.0 {
                1.0
            } else {
                latency_ms / self.config.target_latency_ms
            };
            let health_penalty = match health {
                HealthState::Healthy => 0.0,
                HealthState::Degraded => 0.35,
                HealthState::Starting => 0.15,
                HealthState::Unhealthy => unreachable!("unhealthy nodes were filtered"),
            };
            let base_score = self.config.load_weight * load
                + self.config.latency_weight * normalized_latency
                + self.config.error_weight * (error_ewma + health_penalty);
            candidates.push(Candidate {
                node_index,
                node: Arc::clone(node),
                upstream_model,
                prefix_match_chars: 0,
                cache_preferred: false,
                score: base_score,
            });
        }

        let model_name = model.unwrap_or("<unspecified>").to_owned();
        if model_nodes == 0 {
            return Err(GatewayError::UnknownModel(model_name));
        }
        if healthy_nodes == 0 {
            return Err(GatewayError::NoHealthyNode(model_name));
        }

        let (min_load, max_load) = candidates
            .iter()
            .map(|candidate| candidate.node.active())
            .fold((usize::MAX, 0), |(min, max), load| {
                (min.min(load), max.max(load))
            });
        let min_load = if min_load == usize::MAX { 0 } else { min_load };
        let load_imbalanced = max_load.saturating_sub(min_load)
            > self.config.prefix.balance_abs_threshold
            && (max_load as f64) > min_load as f64 * self.config.prefix.balance_rel_threshold;
        let match_ratio = if prefix_match.input_chars == 0 {
            0.0
        } else {
            prefix_match.matched_chars as f64 / prefix_match.input_chars as f64
        };
        let cache_mode = self.config.prefix.enabled
            && !load_imbalanced
            && match_ratio > self.config.prefix.cache_threshold;

        if cache_mode {
            for candidate in &mut candidates {
                if prefix_match
                    .node_ids
                    .iter()
                    .any(|node_id| node_id == candidate.node.id())
                {
                    candidate.cache_preferred = true;
                    candidate.prefix_match_chars = prefix_match.matched_chars;
                }
            }
        }

        candidates.sort_by(|left, right| {
            right
                .cache_preferred
                .cmp(&left.cache_preferred)
                .then_with(|| {
                    left.score
                        .partial_cmp(&right.score)
                        .unwrap_or(Ordering::Equal)
                })
                .then_with(|| left.node.id().cmp(right.node.id()))
        });
        Ok(candidates)
    }

    pub fn models(&self) -> Vec<String> {
        let mut models = self
            .nodes
            .iter()
            .flat_map(|node| node.explicit_models().map(|(public, _)| public.to_owned()))
            .collect::<Vec<_>>();
        models.sort();
        models.dedup();
        models
    }

    pub fn ready(&self) -> bool {
        self.nodes.iter().any(|node| node.health().is_ready())
    }

    pub fn queue_snapshot(&self) -> (usize, usize) {
        let requests = self
            .config
            .queue_max_requests
            .saturating_sub(self.queue_requests.available_permits());
        let max_kib = self.config.queue_max_bytes.div_ceil(1024);
        let used_kib = max_kib.saturating_sub(self.queue_kib.available_permits());
        (requests, used_kib.saturating_mul(1024))
    }

    pub fn notify_state_change(&self) {
        self.notify.notify_waiters();
    }

    pub fn has_alternative(&self, model: Option<&str>, excluded: &HashSet<String>) -> bool {
        self.nodes.iter().any(|node| {
            !excluded.contains(node.id())
                && node.is_routable()
                && node.upstream_model(model).is_some()
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use crate::{
        config::{NodeConfig, PrefixConfig},
        prefix,
    };

    use super::*;

    fn node(id: &str, concurrency: usize) -> Arc<Node> {
        Node::from_config(&NodeConfig {
            id: id.to_owned(),
            base_url: format!("http://{id}.invalid/v1"),
            models: HashMap::from([("model".to_owned(), "model".to_owned())]),
            max_concurrency: concurrency,
            ..NodeConfig::default()
        })
        .unwrap()
    }

    #[tokio::test]
    async fn skips_a_node_at_capacity() {
        let first = node("first", 1);
        let second = node("second", 1);
        let scheduler = Scheduler::new(
            vec![Arc::clone(&first), Arc::clone(&second)],
            RoutingConfig::default(),
        );
        let held = first
            .try_acquire(Arc::new(Notify::new()))
            .expect("first lease");
        let selected = scheduler
            .acquire(
                Some("model"),
                prefix::PrefixInput::default(),
                &HashSet::new(),
                128,
            )
            .await
            .unwrap();
        assert_eq!(selected.node.id(), "second");
        drop(held);
    }

    #[tokio::test]
    async fn learned_prefix_can_outweigh_idle_difference() {
        let first = node("first", 4);
        let second = node("second", 4);
        let scheduler = Scheduler::new(vec![first, second], RoutingConfig::default());
        let prefix_config = PrefixConfig::default();
        let prefix_input = prefix::routing_text(
            "chat/completions",
            Some("model"),
            Some(&json!({"messages": [{"role": "system", "content": "shared"}]})),
            &prefix_config,
        );
        scheduler.prefix_directory().record("second", &prefix_input);
        let selected = scheduler
            .acquire(Some("model"), prefix_input, &HashSet::new(), 128)
            .await
            .unwrap();
        assert_eq!(selected.node.id(), "second");
        assert!(selected.prefix_match_chars > 0);
    }

    #[tokio::test]
    async fn low_prefix_match_does_not_enable_cache_routing() {
        let idle = node("a-idle", 4);
        let cached = node("z-cached", 4);
        let scheduler = Scheduler::new(vec![idle, cached], RoutingConfig::default());
        let prefix_config = PrefixConfig::default();
        let recorded = prefix::routing_text(
            "completions",
            Some("model"),
            Some(&json!({"prompt": "alpha content that was previously handled"})),
            &prefix_config,
        );
        scheduler.prefix_directory().record("z-cached", &recorded);
        let request = prefix::routing_text(
            "completions",
            Some("model"),
            Some(&json!({"prompt": "beta content with no meaningful shared prefix"})),
            &prefix_config,
        );

        let selected = scheduler
            .acquire(Some("model"), request, &HashSet::new(), 128)
            .await
            .unwrap();
        assert_eq!(selected.node.id(), "a-idle");
        assert_eq!(selected.prefix_match_chars, 0);
    }
}
