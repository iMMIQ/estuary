use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Result, bail};
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use parking_lot::Mutex;
use reqwest::Client;
use semver::Version;
use serde_json::Value as JsonValue;
use tokio::{
    sync::{Notify, watch},
    task::JoinHandle,
    time::MissedTickBehavior,
};
use tracing::{debug, warn};

use crate::{
    config::ProviderKind,
    kv_cache::ExactCacheDirectory,
    node::{Node, ProviderState},
    prefix::PrefixDirectory,
    scheduler::Scheduler,
};

mod monitor;
use monitor::run_node_monitor;
mod tokenization;
use tokenization::{
    TokenizationCache, pretokenized_completion, request_tokenization, tokenization_key,
    tokenize_payload,
};
mod kv_events;
use kv_events::run_event_supervisor;
pub use monitor::preflight_vllm;

const MIN_VLLM_VERSION: Version = Version::new(0, 25, 0);
const MAX_MANAGEMENT_BODY_BYTES: usize = 8 * 1024 * 1024;
const VERSION_RECHECK_TICKS: u64 = 30;
const KV_HEALTH_POLL_MAX: Duration = Duration::from_millis(250);
const TASK_STABLE_UPTIME: Duration = Duration::from_secs(60);
const TASK_MAX_BACKOFF: Duration = Duration::from_secs(30);
const MAX_TOKENIZATION_CACHE_BYTES: usize = 128 * 1024 * 1024;
const TOKENIZATION_CACHE_ENTRY_OVERHEAD: usize = 128;

#[derive(Debug, thiserror::Error)]
#[error("vLLM upstream is unhealthy; full KV replay is required")]
struct KvUpstreamUnhealthy;

#[derive(Debug)]
pub struct VllmManager {
    scheduler: Arc<Scheduler>,
    exact_cache: Arc<ExactCacheDirectory>,
    prefix: Arc<PrefixDirectory>,
    state_notify: Arc<Notify>,
    token_cache: Mutex<TokenizationCache>,
}

#[derive(Debug)]
pub struct RoutingTokenization {
    pub tokens: Option<Vec<u64>>,
    pub outcome: &'static str,
    pub elapsed: Duration,
}

impl RoutingTokenization {
    fn new(tokens: Option<Vec<u64>>, outcome: &'static str, started: Instant) -> Self {
        Self {
            tokens,
            outcome,
            elapsed: started.elapsed(),
        }
    }

    pub(crate) fn skipped(outcome: &'static str) -> Self {
        Self {
            tokens: None,
            outcome,
            elapsed: Duration::ZERO,
        }
    }
}

#[derive(Debug)]
struct ManagedNodeTasks {
    node: Arc<Node>,
    shutdown: watch::Sender<bool>,
    handles: Vec<JoinHandle<()>>,
    started_at: Instant,
}

#[derive(Debug)]
struct TaskRestart {
    instance_id: u64,
    failures: u32,
    not_before: Instant,
}

impl VllmManager {
    pub fn new(scheduler: Arc<Scheduler>) -> Arc<Self> {
        let nodes = scheduler.nodes();
        let exact_cache = Arc::clone(scheduler.exact_cache_directory());
        let cache_entries = nodes
            .iter()
            .filter(|node| node.provider().kind == ProviderKind::Vllm)
            .map(|node| node.provider().tokenize_cache_entries)
            .max()
            .unwrap_or(1);
        for node in &nodes {
            if let Some(events) = node.provider().kv_events.as_ref() {
                exact_cache.configure_node_owned(
                    node.id(),
                    events.max_blocks,
                    events.max_directory_bytes,
                    node.instance_id(),
                );
            }
        }
        Arc::new(Self {
            exact_cache,
            prefix: Arc::clone(scheduler.prefix_directory()),
            state_notify: scheduler.state_notifier(),
            scheduler,
            token_cache: Mutex::new(TokenizationCache::new(cache_entries)),
        })
    }

    pub async fn run(self: Arc<Self>, client: Client, mut shutdown: watch::Receiver<bool>) {
        let mut tasks = HashMap::<String, ManagedNodeTasks>::new();
        let mut restarts = HashMap::<String, TaskRestart>::new();
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                _ = interval.tick() => self.reconcile_tasks(&client, &mut tasks, &mut restarts).await,
            }
        }
        for (_, task) in tasks {
            stop_managed_tasks(task).await;
        }
    }

    pub fn has_exact_cache_for_model(&self, public_model: &str) -> bool {
        self.scheduler.nodes().into_iter().any(|node| {
            node.provider().kind == ProviderKind::Vllm
                && node.provider_state() == ProviderState::Ready
                && node.is_routable()
                && node.upstream_model(Some(public_model)).is_some()
                && {
                    let snapshot = self.exact_cache.snapshot(node.id());
                    snapshot.authoritative && snapshot.blocks > 0
                }
        })
    }

    async fn reconcile_tasks(
        &self,
        client: &Client,
        tasks: &mut HashMap<String, ManagedNodeTasks>,
        restarts: &mut HashMap<String, TaskRestart>,
    ) {
        let nodes = self
            .scheduler
            .nodes()
            .into_iter()
            .filter(|node| node.provider().kind == ProviderKind::Vllm)
            .map(|node| (node.id().to_owned(), node))
            .collect::<HashMap<_, _>>();
        let stale = tasks
            .iter()
            .filter_map(|(id, task)| {
                nodes
                    .get(id)
                    .is_none_or(|node| !Arc::ptr_eq(node, &task.node))
                    .then_some(id.clone())
            })
            .collect::<Vec<_>>();
        for id in stale {
            if let Some(task) = tasks.remove(&id) {
                stop_managed_tasks(task).await;
            }
            restarts.remove(&id);
        }
        let failed = tasks
            .iter()
            .filter(|(_, task)| task.handles.iter().any(JoinHandle::is_finished))
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        for id in failed {
            if let Some(task) = tasks.remove(&id) {
                let instance_id = task.node.instance_id();
                warn!(
                    node = id,
                    "vLLM provider task exited unexpectedly; restarting node monitors"
                );
                stop_managed_tasks(task).await;
                let restart = restarts.entry(id).or_insert(TaskRestart {
                    instance_id,
                    failures: 0,
                    not_before: Instant::now(),
                });
                if restart.instance_id != instance_id {
                    restart.instance_id = instance_id;
                    restart.failures = 0;
                }
                restart.failures = restart.failures.saturating_add(1);
                let delay = task_restart_delay(restart.failures);
                restart.not_before = Instant::now() + delay;
            }
        }
        for (id, node) in nodes {
            if let Some(task) = tasks.get(&id) {
                if task.started_at.elapsed() >= TASK_STABLE_UPTIME {
                    restarts.remove(&id);
                }
                continue;
            }
            if let Some(restart) = restarts.get(&id)
                && restart.instance_id == node.instance_id()
                && Instant::now() < restart.not_before
            {
                continue;
            }
            self.token_cache
                .lock()
                .raise_capacity(node.provider().tokenize_cache_entries);
            tasks.insert(id, self.spawn_node_tasks(client.clone(), node));
        }
    }

    fn spawn_node_tasks(&self, client: Client, node: Arc<Node>) -> ManagedNodeTasks {
        let (shutdown, receiver) = watch::channel(false);
        let mut handles = vec![tokio::spawn(run_node_monitor(
            client,
            Arc::clone(&node),
            Arc::clone(&self.exact_cache),
            Arc::clone(&self.prefix),
            Arc::clone(&self.state_notify),
            receiver.clone(),
        ))];
        if let Some(config) = node.provider().kv_events.clone() {
            let event_node = Arc::clone(&node);
            let exact_cache = Arc::clone(&self.exact_cache);
            let prefix = Arc::clone(&self.prefix);
            handles.push(tokio::spawn(async move {
                run_event_supervisor(event_node, exact_cache, prefix, config, receiver).await;
            }));
        }
        ManagedNodeTasks {
            node,
            shutdown,
            handles,
            started_at: Instant::now(),
        }
    }

    pub async fn tokenize_for_routing(
        &self,
        client: &Client,
        endpoint: &str,
        public_model: &str,
        body: &JsonValue,
        allow_remote: bool,
    ) -> RoutingTokenization {
        let started = Instant::now();
        if body.get("cache_salt").is_some_and(|value| !value.is_null()) {
            return RoutingTokenization::new(None, "cache_salt", started);
        }
        if endpoint == "completions"
            && let Some(tokens) = pretokenized_completion(body)
        {
            return RoutingTokenization::new(Some(tokens), "pretokenized", started);
        }
        if !matches!(endpoint, "chat/completions" | "completions") {
            return RoutingTokenization::new(None, "unsupported", started);
        }
        if !allow_remote {
            return RoutingTokenization::new(None, "prefix_gate", started);
        }
        let Some(base_payload) = tokenize_payload(endpoint, body) else {
            return RoutingTokenization::new(None, "unsupported", started);
        };

        let mut candidates = self
            .scheduler
            .nodes()
            .into_iter()
            .filter(|node| {
                node.provider().kind == ProviderKind::Vllm
                    && node.provider_state() == ProviderState::Ready
                    && node.is_routable()
                    && node.upstream_model(Some(public_model)).is_some()
            })
            .collect::<Vec<_>>();
        let has_exact_blocks = candidates.iter().any(|node| {
            let snapshot = self.exact_cache.snapshot(node.id());
            snapshot.authoritative && snapshot.blocks > 0
        });
        if !has_exact_blocks {
            return RoutingTokenization::new(None, "directory_unavailable", started);
        }
        candidates.sort_by_key(|node| (node.scheduling_load(), node.id().to_owned()));

        let Some(node) = candidates.into_iter().next() else {
            return RoutingTokenization::new(None, "unavailable", started);
        };
        let Some(Some(upstream_model)) = node.upstream_model(Some(public_model)) else {
            return RoutingTokenization::new(None, "unavailable", started);
        };
        let mut payload = base_payload;
        payload.insert("model".to_owned(), JsonValue::String(upstream_model));
        let key = tokenization_key(
            endpoint,
            public_model,
            node.id(),
            node.provider_generation(),
            &payload,
        );
        if let Some(tokens) = self.token_cache.lock().get(&key) {
            return RoutingTokenization::new(Some(tokens), "cache_hit", started);
        }
        let timeout = Duration::from_millis(node.provider().request_timeout_ms);
        match tokio::time::timeout(timeout, request_tokenization(client, &node, payload)).await {
            Ok(Ok(tokens)) if !node.is_retired() => {
                self.token_cache.lock().insert(key, tokens.clone());
                RoutingTokenization::new(Some(tokens), "upstream_success", started)
            }
            Ok(Ok(_)) => RoutingTokenization::new(None, "node_retired", started),
            Ok(Err(error)) => {
                debug!(node = node.id(), error = %error, "vLLM tokenization failed");
                RoutingTokenization::new(None, "upstream_error", started)
            }
            Err(_) => {
                debug!(
                    node = node.id(),
                    "vLLM tokenization exceeded its total deadline"
                );
                RoutingTokenization::new(None, "deadline", started)
            }
        }
    }
}

fn task_restart_delay(failures: u32) -> Duration {
    let exponent = failures.saturating_sub(1).min(5);
    Duration::from_secs(1_u64 << exponent).min(TASK_MAX_BACKOFF)
}

async fn stop_managed_tasks(task: ManagedNodeTasks) {
    let _ = task.shutdown.send(true);
    for handle in task.handles {
        if let Err(error) = handle.await {
            warn!(error = %error, "vLLM provider task failed");
        }
    }
}

async fn read_bounded_response(response: reqwest::Response, name: &str) -> Result<Bytes> {
    let mut stream = response.bytes_stream();
    let mut body = BytesMut::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if body.len().saturating_add(chunk.len()) > MAX_MANAGEMENT_BODY_BYTES {
            bail!("{name} is too large");
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

#[cfg(test)]
mod tests;
