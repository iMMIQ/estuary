use std::{
    cmp::Ordering,
    collections::HashSet,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
    },
    time::Duration,
};

use futures_util::{StreamExt, stream::FuturesUnordered};
use parking_lot::RwLock;
use tokio::{
    sync::{Notify, OwnedSemaphorePermit, Semaphore},
    time::Instant,
};

use crate::{
    config::RoutingConfig,
    error::GatewayError,
    kv_cache::ExactCacheDirectory,
    node::{HealthState, Node, NodeLease, NodeReservation},
    prefix::{PrefixDirectory, PrefixInput, PrefixMatch},
};

#[derive(Debug)]
pub struct Selection {
    pub node: Arc<Node>,
    pub lease: NodeLease,
    pub upstream_model: Option<String>,
    pub prefix_match_chars: usize,
    pub prefix_match_tokens: usize,
    pub score: f64,
}

#[derive(Debug)]
struct Candidate {
    node_instance_id: u64,
    node: Arc<Node>,
    upstream_model: Option<String>,
    prefix_match_chars: usize,
    prefix_match_tokens: usize,
    prefill_tokens: usize,
    decode_tokens: usize,
    normalized_load: f64,
    score: f64,
}

type PendingAcquisition =
    Pin<Box<dyn Future<Output = (Candidate, NodeReservation)> + Send + 'static>>;

#[derive(Debug)]
pub struct Scheduler {
    nodes: RwLock<Vec<Arc<Node>>>,
    config: RoutingConfig,
    prefix: Arc<PrefixDirectory>,
    exact_cache: Arc<ExactCacheDirectory>,
    notify: Arc<Notify>,
    idle_notify: Arc<Notify>,
    queue_slots: Arc<Semaphore>,
    queue_kib: Arc<Semaphore>,
    queued_requests: Arc<AtomicUsize>,
    queued_bytes: Arc<AtomicUsize>,
    admission_waiters: Arc<AtomicUsize>,
    tie_breaker: AtomicUsize,
}

#[derive(Debug)]
pub struct IngressAdmission {
    _request: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}

#[derive(Debug)]
struct QueueAccounting {
    requests: Arc<AtomicUsize>,
    bytes: Arc<AtomicUsize>,
    rounded_bytes: usize,
}

#[derive(Debug)]
struct AdmissionWaiter {
    waiters: Arc<AtomicUsize>,
}

impl AdmissionWaiter {
    fn new(waiters: Arc<AtomicUsize>) -> Self {
        waiters.fetch_add(1, AtomicOrdering::Relaxed);
        Self { waiters }
    }
}

impl Drop for AdmissionWaiter {
    fn drop(&mut self) {
        self.waiters.fetch_sub(1, AtomicOrdering::Relaxed);
    }
}

impl QueueAccounting {
    fn new(requests: Arc<AtomicUsize>, bytes: Arc<AtomicUsize>, rounded_bytes: usize) -> Self {
        requests.fetch_add(1, AtomicOrdering::Relaxed);
        bytes.fetch_add(rounded_bytes, AtomicOrdering::Relaxed);
        Self {
            requests,
            bytes,
            rounded_bytes,
        }
    }
}

impl Drop for QueueAccounting {
    fn drop(&mut self) {
        self.requests.fetch_sub(1, AtomicOrdering::Relaxed);
        self.bytes
            .fetch_sub(self.rounded_bytes, AtomicOrdering::Relaxed);
    }
}

impl Scheduler {
    pub fn new(nodes: Vec<Arc<Node>>, config: RoutingConfig) -> Self {
        let prefix = Arc::new(PrefixDirectory::new(&config.prefix));
        let exact_cache = Arc::new(ExactCacheDirectory::default());
        let queue_max_requests = config.queue_max_requests;
        let queue_max_kib = config.queue_max_bytes.div_ceil(1024).min(u32::MAX as usize);
        Self {
            nodes: RwLock::new(nodes),
            config,
            prefix,
            exact_cache,
            notify: Arc::new(Notify::new()),
            idle_notify: Arc::new(Notify::new()),
            queue_slots: Arc::new(Semaphore::new(queue_max_requests)),
            queue_kib: Arc::new(Semaphore::new(queue_max_kib)),
            queued_requests: Arc::new(AtomicUsize::new(0)),
            queued_bytes: Arc::new(AtomicUsize::new(0)),
            admission_waiters: Arc::new(AtomicUsize::new(0)),
            tie_breaker: AtomicUsize::new(0),
        }
    }

    pub fn nodes(&self) -> Vec<Arc<Node>> {
        self.nodes.read().clone()
    }

    pub fn prefix_directory(&self) -> &Arc<PrefixDirectory> {
        &self.prefix
    }

    pub fn exact_cache_directory(&self) -> &Arc<ExactCacheDirectory> {
        &self.exact_cache
    }

    pub fn approximate_prefix_worth_tokenizing(&self, input: &PrefixInput) -> bool {
        if !self.config.prefix.enabled {
            return false;
        }
        let matched = self.prefix.best_match(input);
        matched.input_chars > 0
            && matched.matched_chars as f64 / matched.input_chars as f64
                > self.config.prefix.cache_threshold
    }

    pub fn state_notifier(&self) -> Arc<Notify> {
        Arc::clone(&self.notify)
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

        let rounded_bytes = body_bytes.div_ceil(1024).max(1) * 1024;
        let _accounting = QueueAccounting::new(
            Arc::clone(&self.queued_requests),
            Arc::clone(&self.queued_bytes),
            rounded_bytes,
        );
        let mut registered = HashSet::new();
        let mut acquisitions = FuturesUnordered::<PendingAcquisition>::new();
        loop {
            let state_changed = self.notify.notified();
            tokio::pin!(state_changed);
            state_changed.as_mut().enable();

            for candidate in self.ranked_candidates(model, &prefix_input, excluded)? {
                if registered.insert(candidate.node_instance_id) {
                    acquisitions.push(Box::pin(async move {
                        let reservation = Arc::clone(&candidate.node).reserve().await;
                        (candidate, reservation)
                    }));
                }
            }

            tokio::select! {
                biased;
                acquired = acquisitions.next(), if !acquisitions.is_empty() => {
                    let Some((candidate, reservation)) = acquired else {
                        unreachable!("a guarded acquisition set is not empty");
                    };
                    registered.remove(&candidate.node_instance_id);
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
                        .find(|item| item.node_instance_id == candidate.node_instance_id);
                    let Some(candidate) = refreshed else {
                        drop(reservation);
                        continue;
                    };
                    if !candidate.node.is_routable() {
                        drop(reservation);
                        continue;
                    }
                    let Some(lease) = reservation.try_commit(Arc::clone(&self.idle_notify)) else {
                        continue;
                    };
                    lease.assign_workload(candidate.prefill_tokens, candidate.decode_tokens, prefix_input.generations());
                    return Ok(Selection {
                        node: candidate.node,
                        lease,
                        upstream_model: candidate.upstream_model,
                        prefix_match_chars: candidate.prefix_match_chars,
                        prefix_match_tokens: candidate.prefix_match_tokens,
                        score: candidate.score,
                    });
                }
                () = state_changed => {}
            }
        }
    }

    pub async fn admit_ingress(&self, body_bytes: usize) -> IngressAdmission {
        let body_kib = u32::try_from(body_bytes.div_ceil(1024).max(1).min(u32::MAX as usize))
            .unwrap_or(u32::MAX);
        let waiter = AdmissionWaiter::new(Arc::clone(&self.admission_waiters));
        let request = Arc::clone(&self.queue_slots)
            .acquire_owned()
            .await
            .expect("ingress request semaphore is never closed");
        let bytes = Arc::clone(&self.queue_kib)
            .acquire_many_owned(body_kib)
            .await
            .expect("ingress byte semaphore is never closed");
        drop(waiter);
        IngressAdmission {
            _request: request,
            _bytes: bytes,
        }
    }

    fn try_acquire(
        &self,
        model: Option<&str>,
        prefix_input: &PrefixInput,
        excluded: &HashSet<String>,
    ) -> Result<Option<Selection>, GatewayError> {
        for candidate in self.ranked_candidates(model, prefix_input, excluded)? {
            if let Some(lease) = candidate.node.try_acquire(Arc::clone(&self.idle_notify)) {
                lease.assign_workload(
                    candidate.prefill_tokens,
                    candidate.decode_tokens,
                    prefix_input.generations(),
                );
                return Ok(Some(Selection {
                    node: candidate.node,
                    lease,
                    upstream_model: candidate.upstream_model,
                    prefix_match_chars: candidate.prefix_match_chars,
                    prefix_match_tokens: candidate.prefix_match_tokens,
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

        let nodes = self.nodes();
        for node in &nodes {
            if excluded.contains(node.id()) {
                continue;
            }
            let Some(upstream_model) = node.upstream_model(model) else {
                continue;
            };
            model_nodes += 1;
            let health = node.health();
            if !node.is_routable() {
                continue;
            }
            healthy_nodes += 1;
            if node
                .fresh_vllm_waiting()
                .is_some_and(|waiting| waiting >= node.provider().waiting_threshold)
            {
                continue;
            }

            let active = node.scheduling_load() as f64;
            let capacity = node.max_concurrency() as f64;
            let load = ((active + 1.0) / capacity) / node.weight();
            let request_stats =
                node.score_stats(Duration::from_millis(self.config.request_stats_stale_ms));
            let normalized_latency = request_stats
                .map(|(latency_ms, _)| latency_ms / self.config.target_latency_ms)
                .unwrap_or_default();
            let error_ewma = request_stats
                .map(|(_, error_ewma)| error_ewma)
                .unwrap_or_default();
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
                node_instance_id: node.instance_id(),
                node: Arc::clone(node),
                upstream_model,
                prefix_match_chars: 0,
                prefix_match_tokens: 0,
                prefill_tokens: 0,
                decode_tokens: 0,
                normalized_load: load,
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

        self.apply_cache_affinity(&mut candidates, prefix_input, &prefix_match);

        candidates.sort_by(|left, right| left.node.id().cmp(right.node.id()));
        if !candidates.is_empty() {
            let offset = self.tie_breaker.fetch_add(1, AtomicOrdering::Relaxed) % candidates.len();
            candidates.rotate_left(offset);
        }
        candidates.sort_by(|left, right| {
            left.score
                .partial_cmp(&right.score)
                .unwrap_or(Ordering::Equal)
        });
        Ok(candidates)
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn apply_cache_affinity(
        &self,
        candidates: &mut [Candidate],
        prefix_input: &PrefixInput,
        prefix_match: &PrefixMatch,
    ) {
        let exact_match = prefix_input
            .token_ids()
            .map(|tokens| self.exact_cache.matches(tokens));
        let input_tokens = prefix_input.input_tokens();
        let affinity_tokens = prefix_input.affinity_tokens();
        let least_load = candidates
            .iter()
            .map(|candidate| candidate.normalized_load)
            .fold(f64::INFINITY, f64::min);
        for candidate in candidates {
            let mut cached_tokens = 0.0;
            if self.config.prefix.enabled {
                if let Some(tokens) = exact_match
                    .as_ref()
                    .and_then(|matched| matched.matched_tokens.get(candidate.node.id()))
                {
                    // A trustworthy zero is evidence of a miss, not permission to reuse stale history.
                    candidate.prefix_match_tokens = *tokens;
                    cached_tokens = (*tokens).min(input_tokens) as f64;
                } else if let Some(chars) = prefix_match.node_matches.get(candidate.node.id()) {
                    let ratio = if prefix_match.input_chars == 0 {
                        0.0
                    } else {
                        *chars as f64 / prefix_match.input_chars as f64
                    };
                    if ratio > self.config.prefix.cache_threshold {
                        candidate.prefix_match_chars = *chars;
                        let age = prefix_match
                            .node_age_seconds
                            .get(candidate.node.id())
                            .copied()
                            .unwrap_or(0.0);
                        let confidence = 0.5
                            * (-age * 1_000.0
                                / self.config.prefix.approximate_half_life_ms.max(1) as f64)
                                .exp2();
                        cached_tokens = affinity_tokens as f64 * ratio * confidence;
                    }
                }
            }
            // Gradually reduce locality credit on busier workers, using capacity and weight.
            let excess = (candidate.normalized_load - least_load).max(0.0)
                * candidate.node.max_concurrency() as f64
                * candidate.node.weight();
            let decay = 1.0
                + excess
                    / self.config.prefix.balance_abs_threshold.max(1) as f64
                    / self.config.prefix.balance_rel_threshold;
            let credited = (cached_tokens / decay).floor() as usize;
            candidate.prefill_tokens = input_tokens.saturating_sub(credited);
            candidate.decode_tokens =
                prefix_input.output_tokens(candidate.node.typical_output_tokens(
                    Duration::from_millis(self.config.request_stats_stale_ms),
                ));
            let work = candidate.node.scheduling_workload();
            candidate.score += (self.config.prefill_weight
                * work
                    .prefill_tokens
                    .saturating_add(candidate.prefill_tokens as u128) as f64
                / self.config.prefill_token_scale as f64
                + self.config.decode_weight
                    * work
                        .decode_tokens
                        .saturating_add(candidate.decode_tokens as u128)
                        as f64
                    / self.config.decode_token_scale as f64)
                / candidate.node.weight();
        }
    }

    pub fn models(&self) -> Vec<String> {
        let mut models = self
            .nodes()
            .into_iter()
            .flat_map(|node| {
                node.explicit_models()
                    .map(|(public, _)| public.to_owned())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        models.sort();
        models.dedup();
        models
    }

    pub fn model_supports_multimodal(&self, model: &str) -> bool {
        let matching = self
            .nodes()
            .into_iter()
            .filter(|node| node.upstream_model(Some(model)).is_some())
            .collect::<Vec<_>>();
        matching.is_empty() || matching.iter().all(|node| node.supports_multimodal(model))
    }

    pub fn ready(&self) -> bool {
        self.nodes().iter().any(|node| node.is_routable())
    }

    pub fn set_node_draining(&self, node_id: &str, draining: bool) -> Option<Arc<Node>> {
        let node = self.nodes().into_iter().find(|node| node.id() == node_id)?;
        if node.set_draining(draining) {
            self.notify.notify_waiters();
        }
        Some(node)
    }

    pub fn drain_all(&self) {
        let mut changed = false;
        for node in self.nodes() {
            changed |= node.set_draining(true);
        }
        if changed {
            self.notify.notify_waiters();
        }
    }

    pub async fn wait_for_node_idle(&self, node: &Node, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if node.active() == 0 {
                return true;
            }
            let notified = self.idle_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if node.active() == 0 {
                return true;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return node.active() == 0;
            }
        }
    }

    pub fn queue_snapshot(&self) -> (usize, usize) {
        (
            self.queued_requests.load(AtomicOrdering::Relaxed),
            self.queued_bytes.load(AtomicOrdering::Relaxed),
        )
    }

    pub fn admission_waiters(&self) -> usize {
        self.admission_waiters.load(AtomicOrdering::Relaxed)
    }

    pub fn notify_state_change(&self) {
        self.notify.notify_waiters();
    }

    pub fn has_alternative(&self, model: Option<&str>, excluded: &HashSet<String>) -> bool {
        self.nodes().iter().any(|node| {
            !excluded.contains(node.id())
                && node.is_routable()
                && node.upstream_model(model).is_some()
        })
    }

    pub fn node(&self, node_id: &str) -> Option<Arc<Node>> {
        self.nodes().into_iter().find(|node| node.id() == node_id)
    }

    pub fn add_node(&self, node: Arc<Node>) -> Result<(), GatewayError> {
        let mut nodes = self.nodes.write();
        if nodes.iter().any(|current| current.id() == node.id()) {
            return Err(GatewayError::InvalidRequest(format!(
                "node {:?} already exists",
                node.id()
            )));
        }
        if let Some(events) = node.provider().kv_events.as_ref() {
            self.exact_cache.configure_node_owned(
                node.id(),
                events.max_blocks,
                events.max_directory_bytes,
                node.instance_id(),
            );
        }
        nodes.push(node);
        nodes.sort_by(|left, right| left.id().cmp(right.id()));
        drop(nodes);
        self.notify.notify_waiters();
        Ok(())
    }

    pub fn replace_node(&self, node: &Arc<Node>) -> Result<Arc<Node>, GatewayError> {
        let mut nodes = self.nodes.write();
        let Some(index) = nodes.iter().position(|current| current.id() == node.id()) else {
            return Err(GatewayError::InvalidRequest(format!(
                "node {:?} does not exist",
                node.id()
            )));
        };
        let previous = std::mem::replace(&mut nodes[index], Arc::clone(node));
        previous.retire();
        self.prefix.clear_node(node.id());
        self.exact_cache
            .remove_node_owned(node.id(), previous.instance_id());
        if let Some(events) = node.provider().kv_events.as_ref() {
            self.exact_cache.configure_node_owned(
                node.id(),
                events.max_blocks,
                events.max_directory_bytes,
                node.instance_id(),
            );
        }
        drop(nodes);
        self.notify.notify_waiters();
        Ok(previous)
    }

    pub fn remove_node(&self, node_id: &str) -> Option<Arc<Node>> {
        let mut nodes = self.nodes.write();
        let index = nodes.iter().position(|node| node.id() == node_id)?;
        let node = nodes.remove(index);
        node.retire();
        self.prefix.clear_node(node_id);
        self.exact_cache
            .remove_node_owned(node_id, node.instance_id());
        drop(nodes);
        self.notify.notify_waiters();
        Some(node)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use crate::{
        config::{NodeConfig, PrefixConfig},
        kv_cache::{BlockHash, CacheMutation},
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
    async fn ingress_admission_reports_waiters_and_releases_them_on_cancel() {
        let config = RoutingConfig {
            queue_max_requests: 1,
            queue_max_bytes: 1_024,
            ..RoutingConfig::default()
        };
        let scheduler = Arc::new(Scheduler::new(Vec::new(), config));
        let held = scheduler.admit_ingress(1).await;
        let pending = tokio::spawn({
            let scheduler = Arc::clone(&scheduler);
            async move { scheduler.admit_ingress(1).await }
        });
        while scheduler.admission_waiters() == 0 {
            tokio::task::yield_now().await;
        }
        assert_eq!(scheduler.admission_waiters(), 1);
        pending.abort();
        let _ = pending.await;
        assert_eq!(scheduler.admission_waiters(), 0);
        drop(held);
    }

    #[tokio::test]
    async fn queued_reservations_keep_node_identity_when_registry_is_resorted() {
        let first = node("a", 1);
        let second = node("b", 1);
        let inserted = node("0", 1);
        let held_first = first.try_acquire(Arc::new(Notify::new())).unwrap();
        let held_second = second.try_acquire(Arc::new(Notify::new())).unwrap();
        let held_inserted = inserted.try_acquire(Arc::new(Notify::new())).unwrap();
        let scheduler = Arc::new(Scheduler::new(
            vec![Arc::clone(&first), Arc::clone(&second)],
            RoutingConfig::default(),
        ));
        let pending = {
            let scheduler = Arc::clone(&scheduler);
            tokio::spawn(async move {
                scheduler
                    .acquire(
                        Some("model"),
                        prefix::PrefixInput::default(),
                        &HashSet::new(),
                        128,
                    )
                    .await
                    .unwrap()
            })
        };
        while scheduler.queue_snapshot().0 == 0 {
            tokio::task::yield_now().await;
        }

        scheduler.add_node(Arc::clone(&inserted)).unwrap();
        drop(held_second);
        let selected = tokio::time::timeout(Duration::from_secs(1), pending)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(selected.node.id(), "b");
        assert_eq!(selected.lease.node().id(), "b");
        drop(held_first);
        drop(held_inserted);
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

    #[tokio::test]
    async fn exact_vllm_tokens_take_precedence_over_character_affinity() {
        let approximate = node("a-approximate", 4);
        let exact = node("z-exact", 4);
        let scheduler = Scheduler::new(vec![approximate, exact], RoutingConfig::default());
        let prefix_config = PrefixConfig::default();
        let mut request = prefix::routing_text(
            "chat/completions",
            Some("model"),
            Some(&json!({"messages": [{"role": "user", "content": "shared prompt"}]})),
            &prefix_config,
        );
        scheduler
            .prefix_directory()
            .record("a-approximate", &request);
        scheduler
            .exact_cache_directory()
            .configure_node("z-exact", 10);
        scheduler
            .exact_cache_directory()
            .apply(
                "z-exact",
                vec![CacheMutation::Store {
                    hashes: vec![BlockHash::Integer(1)],
                    parent: None,
                    token_ids: vec![1, 2, 3, 4],
                    block_size: 4,
                    group: 0,
                }],
            )
            .unwrap();
        request.set_token_ids(vec![1, 2, 3, 4, 5]);

        let selected = scheduler
            .acquire(Some("model"), request, &HashSet::new(), 128)
            .await
            .unwrap();
        assert_eq!(selected.node.id(), "z-exact");
        assert_eq!(selected.prefix_match_tokens, 4);
        assert_eq!(selected.prefix_match_chars, 0);
    }

    #[test]
    fn remote_tokenization_gate_requires_a_high_value_approximate_prefix() {
        let scheduler = Scheduler::new(vec![node("node", 4)], RoutingConfig::default());
        let request = prefix::routing_text(
            "chat/completions",
            Some("model"),
            Some(&json!({"messages": [{"role": "user", "content": "shared prompt"}]})),
            &PrefixConfig::default(),
        );
        assert!(!scheduler.approximate_prefix_worth_tokenizing(&request));

        scheduler.prefix_directory().record("node", &request);
        assert!(scheduler.approximate_prefix_worth_tokenizing(&request));
    }

    fn prompt(text: &str) -> PrefixInput {
        prefix::routing_text(
            "completions",
            Some("model"),
            Some(&json!({"prompt": text})),
            &PrefixConfig::default(),
        )
    }

    #[tokio::test]
    async fn cache_savings_can_pay_for_load_only_for_large_prompts() {
        let cached = node("cached", 4);
        let idle = node("idle", 4);
        let scheduler = Scheduler::new(vec![Arc::clone(&cached), idle], RoutingConfig::default());
        let held = cached.try_acquire(Arc::new(Notify::new())).unwrap();
        for (length, expected) in [(64, "idle"), (16_384, "cached")] {
            let input = prompt(&"x".repeat(length));
            scheduler.prefix_directory().record("cached", &input);
            let selected = scheduler
                .acquire(Some("model"), input, &HashSet::new(), length)
                .await
                .unwrap();
            assert_eq!(selected.node.id(), expected);
        }
        drop(held);
    }

    #[tokio::test]
    async fn exact_eviction_overrides_approximate_history_even_for_zero_matches() {
        let scheduler = Scheduler::new(
            vec![node("a-idle", 4), node("z-cached", 4)],
            RoutingConfig::default(),
        );
        let mut request = prompt("previously cached prompt");
        request.set_token_ids(vec![1, 2, 3, 4]);
        scheduler.prefix_directory().record("z-cached", &request);
        scheduler.exact_cache.configure_node("z-cached", 10);
        scheduler
            .exact_cache
            .apply(
                "z-cached",
                vec![CacheMutation::Store {
                    hashes: vec![BlockHash::Integer(1)],
                    parent: None,
                    token_ids: vec![1, 2, 3, 4],
                    block_size: 4,
                    group: 0,
                }],
            )
            .unwrap();
        scheduler
            .exact_cache
            .apply(
                "z-cached",
                vec![CacheMutation::Remove {
                    hashes: vec![BlockHash::Integer(1)],
                    group: 0,
                }],
            )
            .unwrap();
        let selected = scheduler
            .acquire(Some("model"), request.clone(), &HashSet::new(), 128)
            .await
            .unwrap();
        assert_eq!(selected.node.id(), "a-idle");
        drop(selected);
        // A loss of authority allows the conservative historical fallback again.
        scheduler.exact_cache.suspend_node("z-cached");
        let selected = scheduler
            .acquire(Some("model"), request, &HashSet::new(), 128)
            .await
            .unwrap();
        assert_eq!(selected.node.id(), "z-cached");
    }

    #[tokio::test]
    async fn exact_partial_matches_receive_credit_below_the_approximate_gate() {
        let scheduler = Scheduler::new(
            vec![node("a-idle", 4), node("z-partial", 4)],
            RoutingConfig::default(),
        );
        scheduler.exact_cache.configure_node("z-partial", 10);
        scheduler
            .exact_cache
            .apply(
                "z-partial",
                vec![CacheMutation::Store {
                    hashes: vec![BlockHash::Integer(1)],
                    parent: None,
                    token_ids: vec![1, 2, 3, 4],
                    block_size: 4,
                    group: 0,
                }],
            )
            .unwrap();
        let mut input = prompt("partial prompt");
        input.set_token_ids((1..=20).collect());
        let selected = scheduler
            .acquire(Some("model"), input, &HashSet::new(), 128)
            .await
            .unwrap();
        assert_eq!(selected.node.id(), "z-partial");
        assert_eq!(selected.prefix_match_tokens, 4);
    }

    #[tokio::test]
    async fn equal_request_counts_prefer_the_worker_with_less_prefill_work() {
        let long = node("a-long", 8);
        let short = node("z-short", 8);
        let scheduler = Scheduler::new(
            vec![Arc::clone(&long), Arc::clone(&short)],
            RoutingConfig::default(),
        );
        let first = scheduler
            .acquire(
                Some("model"),
                prompt(&"x".repeat(32_768)),
                &HashSet::from(["z-short".to_owned()]),
                32_768,
            )
            .await
            .unwrap();
        let second = scheduler
            .acquire(
                Some("model"),
                prompt("short"),
                &HashSet::from(["a-long".to_owned()]),
                128,
            )
            .await
            .unwrap();
        assert_eq!(long.active(), short.active());
        let selected = scheduler
            .acquire(Some("model"), prompt("new request"), &HashSet::new(), 128)
            .await
            .unwrap();
        assert_eq!(selected.node.id(), "z-short");
        drop((selected, first, second));
        for node in [&long, &short] {
            assert_eq!(node.snapshot().pending_prefill_tokens, 0);
            assert_eq!(node.snapshot().pending_decode_tokens, 0);
        }
    }

    #[tokio::test]
    async fn first_token_releases_prefill_work_without_releasing_capacity() {
        let only = node("only", 4);
        let scheduler = Scheduler::new(vec![Arc::clone(&only)], RoutingConfig::default());
        let selected = scheduler
            .acquire(
                Some("model"),
                prompt(&"x".repeat(4_096)),
                &HashSet::new(),
                4_096,
            )
            .await
            .unwrap();
        assert_eq!(only.snapshot().pending_prefill_tokens, 1_024);
        assert_eq!(only.snapshot().pending_decode_tokens, 256);
        selected
            .lease
            .record_first_token(Duration::from_millis(750));
        assert_eq!(only.snapshot().pending_prefill_tokens, 0);
        assert_eq!(only.snapshot().pending_decode_tokens, 256);
        assert_eq!(only.active(), 1);
        assert_eq!(only.score_stats(Duration::from_secs(1)), Some((750.0, 0.0)));
        selected.lease.record_output_tokens(64);
        assert_eq!(only.typical_output_tokens(Duration::from_secs(1)), 64);
        drop(selected);
        assert_eq!(only.snapshot().pending_decode_tokens, 0);
    }

    #[tokio::test]
    async fn truncated_affinity_cannot_credit_the_unindexed_prompt_suffix() {
        let config = RoutingConfig {
            prefix: PrefixConfig {
                max_request_chars: 32,
                ..PrefixConfig::default()
            },
            ..RoutingConfig::default()
        };
        let cached = node("cached", 4);
        let scheduler = Scheduler::new(vec![Arc::clone(&cached)], config.clone());
        let input = prefix::routing_text(
            "completions",
            Some("model"),
            Some(&json!({"prompt": "x".repeat(16_384)})),
            &config.prefix,
        );
        scheduler.prefix_directory().record("cached", &input);
        let selected = scheduler
            .acquire(Some("model"), input, &HashSet::new(), 16_384)
            .await
            .unwrap();
        assert_eq!(selected.prefix_match_chars, 32);
        assert!(cached.snapshot().pending_prefill_tokens >= 4_092);
    }

    #[tokio::test]
    async fn aged_character_history_loses_its_routing_credit() {
        let config = RoutingConfig {
            prefix: PrefixConfig {
                approximate_half_life_ms: 1,
                ..PrefixConfig::default()
            },
            ..RoutingConfig::default()
        };
        let scheduler = Scheduler::new(vec![node("a-idle", 4), node("z-cached", 4)], config);
        let input = prompt(&"x".repeat(64));
        scheduler.prefix_directory().record("z-cached", &input);
        tokio::time::sleep(Duration::from_millis(20)).await;
        let selected = scheduler
            .acquire(Some("model"), input, &HashSet::new(), 128)
            .await
            .unwrap();
        assert_eq!(selected.node.id(), "a-idle");
    }

    #[tokio::test]
    async fn equal_request_counts_prefer_less_output_work_and_normalize_generation_history() {
        let long = node("a-long", 8);
        let short = node("z-short", 8);
        let scheduler = Scheduler::new(
            vec![Arc::clone(&long), Arc::clone(&short)],
            RoutingConfig::default(),
        );
        let held_long = long.try_acquire(Arc::new(Notify::new())).unwrap();
        held_long.assign_workload(0, 8_192, 2);
        let held_short = short.try_acquire(Arc::new(Notify::new())).unwrap();
        held_short.assign_workload(0, 32, 1);
        let selected = scheduler
            .acquire(Some("model"), prompt("new"), &HashSet::new(), 128)
            .await
            .unwrap();
        assert_eq!(selected.node.id(), "z-short");
        held_long.record_output_tokens(100);
        assert_eq!(long.typical_output_tokens(Duration::from_secs(1)), 50);
    }

    #[tokio::test]
    async fn queued_admission_accounts_work_and_cancelled_waiters_do_not_leak() {
        let only = node("only", 1);
        let scheduler = Arc::new(Scheduler::new(
            vec![Arc::clone(&only)],
            RoutingConfig::default(),
        ));
        let held = only.try_acquire(Arc::new(Notify::new())).unwrap();
        let spawn_waiter = || {
            let scheduler = Arc::clone(&scheduler);
            tokio::spawn(async move {
                scheduler
                    .acquire(
                        Some("model"),
                        prompt(&"x".repeat(4_096)),
                        &HashSet::new(),
                        4_096,
                    )
                    .await
                    .unwrap()
            })
        };
        let cancelled = spawn_waiter();
        while scheduler.queue_snapshot().0 == 0 {
            tokio::task::yield_now().await;
        }
        cancelled.abort();
        assert!(cancelled.await.unwrap_err().is_cancelled());
        assert_eq!(only.snapshot().pending_prefill_tokens, 0);
        let pending = spawn_waiter();
        while scheduler.queue_snapshot().0 == 0 {
            tokio::task::yield_now().await;
        }
        drop(held);
        let selected = tokio::time::timeout(Duration::from_secs(1), pending)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(only.snapshot().pending_prefill_tokens, 1_024);
        assert_eq!(only.snapshot().pending_decode_tokens, 256);
        drop(selected);
        assert_eq!(only.snapshot().pending_prefill_tokens, 0);
        assert_eq!(only.snapshot().pending_decode_tokens, 0);
    }
}
