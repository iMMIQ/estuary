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
    cache_preferred: bool,
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
            .map(|candidate| candidate.node.scheduling_load())
            .fold((usize::MAX, 0), |(min, max), load| {
                (min.min(load), max.max(load))
            });
        let min_load = if min_load == usize::MAX { 0 } else { min_load };
        let load_imbalanced = max_load.saturating_sub(min_load)
            > self.config.prefix.balance_abs_threshold
            && (max_load as f64) > min_load as f64 * self.config.prefix.balance_rel_threshold;
        self.apply_cache_affinity(
            &mut candidates,
            prefix_input,
            &prefix_match,
            load_imbalanced,
        );

        candidates.sort_by(|left, right| left.node.id().cmp(right.node.id()));
        if !candidates.is_empty() {
            let offset = self.tie_breaker.fetch_add(1, AtomicOrdering::Relaxed) % candidates.len();
            candidates.rotate_left(offset);
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
        });
        Ok(candidates)
    }

    fn apply_cache_affinity(
        &self,
        candidates: &mut [Candidate],
        prefix_input: &PrefixInput,
        prefix_match: &PrefixMatch,
        load_imbalanced: bool,
    ) {
        let exact_match = prefix_input
            .token_ids()
            .map(|tokens| self.exact_cache.matches(tokens));
        let exact_tokens = exact_match
            .as_ref()
            .and_then(|matched| {
                candidates
                    .iter()
                    .filter_map(|candidate| matched.matched_tokens.get(candidate.node.id()))
                    .max()
                    .copied()
            })
            .unwrap_or_default();
        let input_tokens = prefix_input.token_ids().map_or(0, <[u64]>::len);
        let exact_ratio = if input_tokens == 0 {
            0.0
        } else {
            exact_tokens as f64 / input_tokens as f64
        };
        let approximate_ratio = if prefix_match.input_chars == 0 {
            0.0
        } else {
            prefix_match.matched_chars as f64 / prefix_match.input_chars as f64
        };
        let exact_cache_mode = self.config.prefix.enabled
            && !load_imbalanced
            && exact_ratio > self.config.prefix.cache_threshold;
        let approximate_cache_mode = self.config.prefix.enabled
            && !load_imbalanced
            && !exact_cache_mode
            && approximate_ratio > self.config.prefix.cache_threshold;

        if exact_cache_mode {
            let matched = exact_match.as_ref().expect("exact cache mode has matches");
            for candidate in candidates.iter_mut() {
                let tokens = matched
                    .matched_tokens
                    .get(candidate.node.id())
                    .copied()
                    .unwrap_or_default();
                if tokens == exact_tokens {
                    candidate.cache_preferred = true;
                    candidate.prefix_match_tokens = tokens;
                }
            }
        } else if approximate_cache_mode {
            for candidate in candidates.iter_mut() {
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
}
