use std::{
    collections::{HashMap, VecDeque},
    io::{BufRead, BufReader, Cursor},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use parking_lot::Mutex;
use prometheus_parse::{Scrape, Value as MetricValue};
use reqwest::Client;
use rmpv::Value;
use semver::Version;
use serde::Deserialize;
use serde_json::{Map, Value as JsonValue};
use tokio::{
    sync::{Notify, watch},
    task::JoinHandle,
    time::MissedTickBehavior,
};
use tracing::{debug, info, warn};
use zeromq::{DealerSocket, Socket, SocketRecv, SocketSend, SubSocket, ZmqMessage};

use crate::{
    config::{ProviderKind, VllmKvEventsConfig},
    kv_cache::{BlockHash, CacheMutation, ExactCacheDirectory},
    node::{Node, ProviderState},
    prefix::PrefixDirectory,
    scheduler::Scheduler,
};

const MIN_VLLM_VERSION: Version = Version::new(0, 25, 0);
const MAX_MANAGEMENT_BODY_BYTES: usize = 8 * 1024 * 1024;
const VERSION_RECHECK_TICKS: u64 = 30;

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
                exact_cache.configure_node_owned(node.id(), events.max_blocks, node.instance_id());
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
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                _ = interval.tick() => self.reconcile_tasks(&client, &mut tasks).await,
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
        }
        for (id, node) in nodes {
            if tasks.contains_key(&id) {
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
        if endpoint == "completions" {
            if let Some(tokens) = pretokenized_completion(body) {
                return RoutingTokenization::new(Some(tokens), "pretokenized", started);
            }
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

async fn stop_managed_tasks(task: ManagedNodeTasks) {
    let _ = task.shutdown.send(true);
    for handle in task.handles {
        if let Err(error) = handle.await {
            warn!(error = %error, "vLLM provider task failed");
        }
    }
}

async fn run_node_monitor(
    client: Client,
    node: Arc<Node>,
    exact_cache: Arc<ExactCacheDirectory>,
    prefix: Arc<PrefixDirectory>,
    notify: Arc<Notify>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut interval =
        tokio::time::interval(Duration::from_millis(node.provider().monitor_interval_ms));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut tick = 0_u64;
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            _ = interval.tick() => {
                if node.is_retired() {
                    break;
                }
                let provider_was_ready = node.provider_is_ready();
                let waiting_was_blocked = node
                    .fresh_vllm_waiting()
                    .is_some_and(|waiting| waiting >= node.provider().waiting_threshold);
                let generation = node.provider_generation();
                poll_node(&client, &node, tick % VERSION_RECHECK_TICKS == 0).await;
                if node.is_retired() {
                    break;
                }
                if node.provider_generation() != generation {
                    exact_cache.invalidate_node_owned(node.id(), node.instance_id());
                    prefix.clear_node(node.id());
                }
                let provider_became_ready = !provider_was_ready && node.provider_is_ready();
                let waiting_is_blocked = node
                    .fresh_vllm_waiting()
                    .is_some_and(|waiting| waiting >= node.provider().waiting_threshold);
                if provider_became_ready || (waiting_was_blocked && !waiting_is_blocked) {
                    notify.notify_waiters();
                }
                tick = tick.wrapping_add(1);
            }
        }
    }
}

async fn poll_node(client: &Client, node: &Arc<Node>, check_version: bool) {
    if check_version || node.provider_state() == ProviderState::Checking {
        match fetch_version(client, node).await {
            Ok((raw, parsed)) if parsed >= MIN_VLLM_VERSION => {
                if node.provider_state() != ProviderState::Ready {
                    info!(node = node.id(), version = %raw, "vLLM provider is ready");
                }
                node.record_vllm_ready(raw);
            }
            Ok((raw, _)) => {
                let message =
                    format!("vLLM {raw} is unsupported; Estuary requires >= {MIN_VLLM_VERSION}");
                if node.record_vllm_incompatible(Some(raw.clone()), message.clone()) {
                    warn!(node = node.id(), version = %raw, required = %MIN_VLLM_VERSION, "{message}");
                }
                return;
            }
            Err(error) => {
                let changed =
                    node.record_provider_telemetry_error(format!("version probe failed: {error}"));
                if changed && node.provider_state() != ProviderState::Ready {
                    warn!(node = node.id(), error = %error, required = %MIN_VLLM_VERSION, "vLLM compatibility check failed; node remains out of rotation");
                } else {
                    debug!(node = node.id(), error = %error, "vLLM version probe failed");
                }
                if node.provider_state() != ProviderState::Ready {
                    return;
                }
            }
        }
    }

    if node.provider_state() != ProviderState::Ready {
        return;
    }
    match fetch_metrics(client, node).await {
        Ok(telemetry) => node.record_vllm_telemetry(
            telemetry.running,
            telemetry.waiting,
            telemetry.kv_cache_usage,
        ),
        Err(error) => {
            node.record_provider_telemetry_error(format!("metrics scrape failed: {error}"));
            debug!(node = node.id(), error = %error, "vLLM metrics scrape failed");
        }
    }
}

pub async fn preflight_vllm(client: &Client, node: &Arc<Node>) -> Result<()> {
    if node.provider().kind != ProviderKind::Vllm {
        return Ok(());
    }
    let (raw, parsed) = fetch_version(client, node).await?;
    if parsed < MIN_VLLM_VERSION {
        bail!("vLLM {raw} is unsupported; Estuary requires >= {MIN_VLLM_VERSION}");
    }
    node.record_vllm_ready(raw);
    let telemetry = fetch_metrics(client, node).await?;
    node.record_vllm_telemetry(
        telemetry.running,
        telemetry.waiting,
        telemetry.kv_cache_usage,
    );
    Ok(())
}

#[derive(Deserialize)]
struct VersionResponse {
    version: String,
}

async fn fetch_version(client: &Client, node: &Node) -> Result<(String, Version)> {
    let body = management_get(client, node, &node.provider().version_path).await?;
    let response: VersionResponse =
        serde_json::from_slice(&body).context("invalid /version JSON")?;
    let normalized = response.version.trim().trim_start_matches('v');
    let version = Version::parse(normalized)
        .with_context(|| format!("invalid vLLM version {:?}", response.version))?;
    Ok((response.version, version))
}

#[derive(Clone, Copy, Debug)]
struct VllmTelemetry {
    running: usize,
    waiting: usize,
    kv_cache_usage: Option<f64>,
}

async fn fetch_metrics(client: &Client, node: &Node) -> Result<VllmTelemetry> {
    let body = management_get(client, node, &node.provider().metrics_path).await?;
    parse_metrics(&body)
}

async fn management_get(client: &Client, node: &Node, path: &str) -> Result<Bytes> {
    let url = node.provider_url(path)?;
    let mut request = client
        .get(url)
        .timeout(Duration::from_millis(node.provider().request_timeout_ms));
    for (name, value) in node.headers() {
        request = request.header(name, value);
    }
    let response = request.send().await?.error_for_status()?;
    if response
        .content_length()
        .is_some_and(|length| length > MAX_MANAGEMENT_BODY_BYTES as u64)
    {
        bail!("vLLM management response is too large");
    }
    let body = response.bytes().await?;
    if body.len() > MAX_MANAGEMENT_BODY_BYTES {
        bail!("vLLM management response is too large");
    }
    Ok(body)
}

fn parse_metrics(body: &[u8]) -> Result<VllmTelemetry> {
    let reader = BufReader::new(body);
    // prometheus-parse accepts the Prometheus grammar except ':' in metric names.
    // Normalize vLLM's legal namespace separator before structured parsing.
    let lines = reader
        .lines()
        .map(|line| line.map(|line| line.replacen("vllm:", "vllm_", 1)));
    let scrape = Scrape::parse(lines).context("invalid Prometheus exposition")?;
    let mut running = None;
    let mut waiting = None;
    let mut kv_usage = None;
    for sample in scrape.samples {
        let value = scalar_metric(&sample.value);
        match sample.metric.as_str() {
            "vllm_num_requests_running" => {
                running = Some(running.unwrap_or(0.0) + value.unwrap_or(0.0));
            }
            "vllm_num_requests_waiting" => {
                waiting = Some(waiting.unwrap_or(0.0) + value.unwrap_or(0.0));
            }
            "vllm_kv_cache_usage_perc" => {
                kv_usage = Some(kv_usage.unwrap_or(0.0_f64).max(value.unwrap_or(0.0)));
            }
            _ => {}
        }
    }
    let running = finite_count(running.ok_or_else(|| anyhow!("running metric is missing"))?)?;
    let waiting = finite_count(waiting.ok_or_else(|| anyhow!("waiting metric is missing"))?)?;
    let kv_cache_usage = kv_usage
        .filter(|value| value.is_finite())
        .map(|value| value.clamp(0.0, 1.0));
    Ok(VllmTelemetry {
        running,
        waiting,
        kv_cache_usage,
    })
}

fn scalar_metric(value: &MetricValue) -> Option<f64> {
    match value {
        MetricValue::Counter(value) | MetricValue::Gauge(value) | MetricValue::Untyped(value) => {
            Some(*value)
        }
        MetricValue::Histogram(_) | MetricValue::Summary(_) => None,
    }
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn finite_count(value: f64) -> Result<usize> {
    if !value.is_finite() || value < 0.0 || value > usize::MAX as f64 {
        bail!("invalid vLLM request count {value}");
    }
    Ok(value.round() as usize)
}

fn tokenize_payload(endpoint: &str, body: &JsonValue) -> Option<Map<String, JsonValue>> {
    let object = body.as_object()?;
    let mut payload = Map::new();
    match endpoint {
        "chat/completions" => {
            if object
                .get("documents")
                .is_some_and(|value| !value.is_null())
            {
                return None;
            }
            payload.insert("messages".to_owned(), object.get("messages")?.clone());
            for key in [
                "tools",
                "add_generation_prompt",
                "continue_final_message",
                "add_special_tokens",
                "chat_template",
                "chat_template_kwargs",
                "media_io_kwargs",
                "mm_processor_kwargs",
            ] {
                if let Some(value) = object.get(key) {
                    payload.insert(key.to_owned(), value.clone());
                }
            }
        }
        "completions" => {
            payload.insert(
                "prompt".to_owned(),
                JsonValue::String(object.get("prompt")?.as_str()?.to_owned()),
            );
            if let Some(value) = object.get("add_special_tokens") {
                payload.insert("add_special_tokens".to_owned(), value.clone());
            }
        }
        _ => return None,
    }
    Some(payload)
}

fn pretokenized_completion(body: &JsonValue) -> Option<Vec<u64>> {
    body.get("prompt")?
        .as_array()?
        .iter()
        .map(JsonValue::as_u64)
        .collect()
}

#[derive(Deserialize)]
struct TokenizeResponse {
    tokens: Vec<u64>,
}

async fn request_tokenization(
    client: &Client,
    node: &Node,
    payload: Map<String, JsonValue>,
) -> Result<Vec<u64>> {
    let url = node.provider_url(&node.provider().tokenize_path)?;
    let mut request = client.post(url).json(&payload);
    for (name, value) in node.headers() {
        request = request.header(name, value);
    }
    let response = request.send().await?.error_for_status()?;
    if response
        .content_length()
        .is_some_and(|length| length > MAX_MANAGEMENT_BODY_BYTES as u64)
    {
        bail!("vLLM tokenize response is too large");
    }
    let body = response.bytes().await?;
    if body.len() > MAX_MANAGEMENT_BODY_BYTES {
        bail!("vLLM tokenize response is too large");
    }
    Ok(serde_json::from_slice::<TokenizeResponse>(&body)
        .context("invalid /tokenize JSON")?
        .tokens)
}

fn tokenization_key(
    endpoint: &str,
    public_model: &str,
    node_id: &str,
    generation: u64,
    payload: &Map<String, JsonValue>,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    for value in [endpoint, public_model, node_id] {
        hasher.update(value.as_bytes());
        hasher.update(&[0]);
    }
    hasher.update(&generation.to_le_bytes());
    if let Ok(encoded) = serde_json::to_vec(payload) {
        hasher.update(&encoded);
    }
    *hasher.finalize().as_bytes()
}

#[derive(Debug)]
struct TokenizationCache {
    values: HashMap<[u8; 32], (Vec<u64>, u64)>,
    order: VecDeque<([u8; 32], u64)>,
    epoch: u64,
    capacity: usize,
}

impl TokenizationCache {
    fn new(capacity: usize) -> Self {
        Self {
            values: HashMap::new(),
            order: VecDeque::new(),
            epoch: 0,
            capacity,
        }
    }

    fn raise_capacity(&mut self, capacity: usize) {
        self.capacity = self.capacity.max(capacity);
    }

    fn get(&mut self, key: &[u8; 32]) -> Option<Vec<u64>> {
        let tokens = self.values.get(key)?.0.clone();
        self.epoch = self.epoch.wrapping_add(1);
        self.values.insert(*key, (tokens.clone(), self.epoch));
        self.order.push_back((*key, self.epoch));
        Some(tokens)
    }

    fn insert(&mut self, key: [u8; 32], tokens: Vec<u64>) {
        self.epoch = self.epoch.wrapping_add(1);
        self.values.insert(key, (tokens, self.epoch));
        self.order.push_back((key, self.epoch));
        while self.values.len() > self.capacity {
            let Some((old_key, old_epoch)) = self.order.pop_front() else {
                break;
            };
            if self
                .values
                .get(&old_key)
                .is_some_and(|(_, epoch)| *epoch == old_epoch)
            {
                self.values.remove(&old_key);
            }
        }
    }
}

async fn run_event_supervisor(
    node: Arc<Node>,
    exact_cache: Arc<ExactCacheDirectory>,
    prefix: Arc<PrefixDirectory>,
    config: VllmKvEventsConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut last_seq = None;
    let mut synchronized = false;
    loop {
        if *shutdown.borrow() {
            break;
        }
        match run_event_session(
            &node,
            &exact_cache,
            &prefix,
            &config,
            &mut last_seq,
            &mut synchronized,
            &mut shutdown,
        )
        .await
        {
            Ok(()) => break,
            Err(error) => {
                exact_cache.suspend_node_owned(node.id(), node.instance_id());
                node.record_kv_event_error(format!("KV event subscriber failed: {error}"));
                warn!(node = node.id(), error = %error, "vLLM KV event subscriber disconnected");
            }
        }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            () = tokio::time::sleep(Duration::from_millis(config.reconnect_ms)) => {}
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn run_event_session(
    node: &Node,
    exact_cache: &ExactCacheDirectory,
    prefix: &PrefixDirectory,
    config: &VllmKvEventsConfig,
    last_seq: &mut Option<u64>,
    synchronized: &mut bool,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<()> {
    let mut socket = SubSocket::new();
    tokio::time::timeout(Duration::from_secs(5), socket.connect(&config.endpoint))
        .await
        .context("timed out connecting to KV event publisher")??;
    socket.subscribe(&config.topic).await?;
    info!(node = node.id(), endpoint = %config.endpoint, "subscribed to vLLM KV events");

    let mut replay_high_water = None;
    if let Some(replay_endpoint) = config.replay_endpoint.as_ref() {
        let start = replay_start_sequence(*last_seq, *synchronized);
        match replay_available(node, exact_cache, prefix, config, replay_endpoint, start).await {
            Ok(Some(replayed_through)) => {
                *last_seq = Some(replayed_through);
                *synchronized = true;
                replay_high_water = Some(replayed_through);
                exact_cache.resume_node_owned(node.id(), node.instance_id());
                node.record_kv_event_success();
            }
            Ok(None) if *synchronized => {
                exact_cache.resume_node_owned(node.id(), node.instance_id());
                node.record_kv_event_success();
            }
            Ok(None) => {}
            Err(error) => {
                invalidate_cache_state(node, exact_cache, prefix);
                *last_seq = None;
                *synchronized = false;
                node.record_kv_event_error(format!("KV replay synchronization failed: {error}"));
                warn!(node = node.id(), error = %error, "could not synchronize vLLM KV replay buffer");
            }
        }
    }

    loop {
        if node.is_retired() {
            return Ok(());
        }
        let message = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
                continue;
            }
            message = socket.recv() => message?,
        };
        let frames = message.into_vec();
        if frames
            .first()
            .is_none_or(|topic| topic.as_ref() != config.topic.as_bytes())
        {
            continue;
        }
        if frames.len() != 3 {
            invalidate_cache_state(node, exact_cache, prefix);
            *last_seq = None;
            *synchronized = false;
            bail!("KV publisher sent {} frames instead of 3", frames.len());
        }
        let seq = decode_sequence(&frames[1]).inspect_err(|_| {
            invalidate_cache_state(node, exact_cache, prefix);
            *last_seq = None;
            *synchronized = false;
        })?;
        if replay_high_water.is_some_and(|high_water| seq <= high_water) {
            continue;
        }
        replay_high_water = None;
        if let Some(previous) = *last_seq {
            if seq <= previous {
                if seq == previous {
                    continue;
                }
                invalidate_cache_state(node, exact_cache, prefix);
                *last_seq = None;
                *synchronized = false;
                warn!(
                    node = node.id(),
                    previous, seq, "vLLM KV sequence reset; cache state cleared"
                );
                node.record_kv_event_error(format!(
                    "KV sequence reset from {previous} to {seq}; awaiting full resynchronization"
                ));
            } else if *synchronized && seq > previous.saturating_add(1) {
                let recovered = recover_gap(
                    node,
                    exact_cache,
                    prefix,
                    config,
                    previous.saturating_add(1),
                    seq,
                )
                .await;
                if let Err(error) = recovered {
                    invalidate_cache_state(node, exact_cache, prefix);
                    *last_seq = None;
                    *synchronized = false;
                    node.record_kv_event_error(format!("KV replay gap recovery failed: {error}"));
                    warn!(node = node.id(), error = %error, "could not replay KV event gap; cache state cleared");
                }
            }
        }
        if last_seq.is_none() && !*synchronized {
            if seq == 0 {
                *synchronized = true;
            } else if config.replay_endpoint.is_some() {
                match recover_gap(node, exact_cache, prefix, config, 0, seq).await {
                    Ok(()) => *synchronized = true,
                    Err(error) => {
                        invalidate_cache_state(node, exact_cache, prefix);
                        node.record_kv_event_error(format!(
                            "initial KV replay synchronization failed: {error}"
                        ));
                        warn!(node = node.id(), error = %error, "could not establish initial KV event history");
                    }
                }
            }
        }
        match apply_payload(node, exact_cache, prefix, config, &frames[2], *synchronized) {
            Ok(became_synchronized) => *synchronized |= became_synchronized,
            Err(error) => {
                *last_seq = None;
                *synchronized = false;
                return Err(error);
            }
        }
        *last_seq = synchronized.then_some(seq);
    }
}

fn replay_start_sequence(last_seq: Option<u64>, synchronized: bool) -> u64 {
    if synchronized {
        last_seq.map_or(0, |seq| seq.saturating_add(1))
    } else {
        0
    }
}

async fn replay_available(
    node: &Node,
    exact_cache: &ExactCacheDirectory,
    prefix: &PrefixDirectory,
    config: &VllmKvEventsConfig,
    endpoint: &str,
    start: u64,
) -> Result<Option<u64>> {
    let mut socket = DealerSocket::new();
    tokio::time::timeout(Duration::from_secs(5), socket.connect(endpoint))
        .await
        .context("timed out connecting to KV replay endpoint")??;
    let request = ZmqMessage::try_from(vec![
        Bytes::new(),
        Bytes::copy_from_slice(&start.to_be_bytes()),
    ])
    .map_err(|error| anyhow!(error.to_string()))?;
    socket.send(request).await?;

    let mut expected = start;
    let mut replayed_through = None;
    loop {
        let message = tokio::time::timeout(
            Duration::from_millis(node.provider().request_timeout_ms),
            socket.recv(),
        )
        .await
        .context("timed out waiting for KV replay")??;
        let mut frames = message.into_vec();
        if frames.first().is_some_and(Bytes::is_empty) {
            frames.remove(0);
        }
        if frames.last().is_some_and(Bytes::is_empty) {
            break;
        }
        let (seq_frame, payload) = match frames.as_slice() {
            [seq, payload] => (seq, payload),
            [topic, seq, payload] if topic.as_ref() == config.topic.as_bytes() => (seq, payload),
            _ => bail!("invalid KV replay frame layout"),
        };
        let seq = decode_sequence(seq_frame)?;
        if seq != expected {
            bail!("KV replay was not contiguous at sequence {expected}");
        }
        apply_payload(node, exact_cache, prefix, config, payload, true)?;
        replayed_through = Some(seq);
        expected = expected
            .checked_add(1)
            .ok_or_else(|| anyhow!("KV replay sequence overflow"))?;
    }
    Ok(replayed_through)
}

async fn recover_gap(
    node: &Node,
    exact_cache: &ExactCacheDirectory,
    prefix: &PrefixDirectory,
    config: &VllmKvEventsConfig,
    start: u64,
    stop: u64,
) -> Result<()> {
    let endpoint = config
        .replay_endpoint
        .as_ref()
        .ok_or_else(|| anyhow!("replay endpoint is not configured"))?;
    let mut socket = DealerSocket::new();
    tokio::time::timeout(Duration::from_secs(5), socket.connect(endpoint))
        .await
        .context("timed out connecting to KV replay endpoint")??;
    let request = ZmqMessage::try_from(vec![
        Bytes::new(),
        Bytes::copy_from_slice(&start.to_be_bytes()),
    ])
    .map_err(|error| anyhow!(error.to_string()))?;
    socket.send(request).await?;

    let mut expected = start;
    loop {
        let message = tokio::time::timeout(
            Duration::from_millis(node.provider().request_timeout_ms),
            socket.recv(),
        )
        .await
        .context("timed out waiting for KV replay")??;
        let mut frames = message.into_vec();
        if frames.first().is_some_and(Bytes::is_empty) {
            frames.remove(0);
        }
        if frames.last().is_some_and(Bytes::is_empty) {
            break;
        }
        let (seq_frame, payload) = match frames.as_slice() {
            [seq, payload] => (seq, payload),
            [topic, seq, payload] if topic.as_ref() == config.topic.as_bytes() => (seq, payload),
            _ => bail!("invalid KV replay frame layout"),
        };
        let seq = decode_sequence(seq_frame)?;
        if seq != expected || seq >= stop {
            bail!("KV replay was not contiguous at sequence {expected}");
        }
        apply_payload(node, exact_cache, prefix, config, payload, true)?;
        expected = expected
            .checked_add(1)
            .ok_or_else(|| anyhow!("KV replay sequence overflow"))?;
    }
    if expected != stop {
        bail!("KV replay ended at {expected}, expected {stop}");
    }
    Ok(())
}

fn decode_sequence(frame: &[u8]) -> Result<u64> {
    let bytes: [u8; 8] = frame
        .try_into()
        .map_err(|_| anyhow!("KV event sequence is not 8 bytes"))?;
    Ok(u64::from_be_bytes(bytes))
}

fn apply_payload(
    node: &Node,
    exact_cache: &ExactCacheDirectory,
    prefix: &PrefixDirectory,
    config: &VllmKvEventsConfig,
    payload: &[u8],
    synchronized: bool,
) -> Result<bool> {
    if node.is_retired() {
        return Ok(false);
    }
    if payload.len() > config.max_event_bytes {
        bail!("KV event payload exceeds configured size limit");
    }
    let mut mutations = match decode_event_batch(payload) {
        Ok(mutations) => mutations,
        Err(error) => {
            invalidate_cache_state(node, exact_cache, prefix);
            return Err(error);
        }
    };
    let clears = mutations
        .iter()
        .any(|item| matches!(item, CacheMutation::Clear));
    let synchronized = if synchronized {
        true
    } else if let Some(last_clear) = mutations
        .iter()
        .rposition(|item| matches!(item, CacheMutation::Clear))
    {
        mutations.drain(..last_clear);
        true
    } else {
        exact_cache.suspend_node_owned(node.id(), node.instance_id());
        return Ok(false);
    };
    if let Err(error) = exact_cache.apply_owned(node.id(), node.instance_id(), mutations) {
        invalidate_cache_state(node, exact_cache, prefix);
        return Err(error);
    }
    if clears {
        prefix.clear_node(node.id());
    }
    node.record_kv_event_success();
    Ok(synchronized)
}

fn invalidate_cache_state(
    node: &Node,
    exact_cache: &ExactCacheDirectory,
    prefix: &PrefixDirectory,
) {
    exact_cache.invalidate_node_owned(node.id(), node.instance_id());
    prefix.clear_node(node.id());
    node.bump_provider_generation();
}

fn decode_event_batch(payload: &[u8]) -> Result<Vec<CacheMutation>> {
    let value = rmpv::decode::read_value_with_max_depth(&mut Cursor::new(payload), 64)
        .context("invalid vLLM KV MessagePack")?;
    let batch = value
        .as_array()
        .ok_or_else(|| anyhow!("vLLM KV batch is not an array"))?;
    let events = batch
        .get(1)
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("vLLM KV batch has no event array"))?;
    events.iter().filter_map(decode_event).collect()
}

fn decode_event(event: &Value) -> Option<Result<CacheMutation>> {
    let Some(map) = event.as_map() else {
        return Some(Err(anyhow!("vLLM KV event is not a map")));
    };
    match field(map, "type").and_then(Value::as_str) {
        Some("BlockStored") => decode_stored(map).transpose(),
        Some("BlockRemoved") => decode_removed(map).transpose(),
        Some("AllBlocksCleared") => Some(Ok(CacheMutation::Clear)),
        Some(_) | None => None,
    }
}

fn decode_stored(map: &[(Value, Value)]) -> Result<Option<CacheMutation>> {
    if !is_local_gpu_event(map) || has_special_cache_keys(map) {
        return Ok(None);
    }
    let hashes = hashes_field(map, "block_hashes")?;
    let parent = match field(map, "parent_block_hash") {
        None | Some(Value::Nil) => None,
        Some(value) => Some(decode_hash(value)?),
    };
    let token_ids = integer_array(field_required(map, "token_ids")?)?;
    let block_size = usize::try_from(unsigned(field_required(map, "block_size")?)?)
        .context("KV block size does not fit usize")?;
    let group = optional_group(map)?;
    Ok(Some(CacheMutation::Store {
        hashes,
        parent,
        token_ids,
        block_size,
        group,
    }))
}

fn decode_removed(map: &[(Value, Value)]) -> Result<Option<CacheMutation>> {
    if !is_local_gpu_event(map) {
        return Ok(None);
    }
    Ok(Some(CacheMutation::Remove {
        hashes: hashes_field(map, "block_hashes")?,
        group: optional_group(map)?,
    }))
}

fn is_local_gpu_event(map: &[(Value, Value)]) -> bool {
    let gpu = field(map, "medium").and_then(Value::as_str) == Some("GPU");
    let local = !matches!(
        field(map, "locality").and_then(Value::as_str),
        Some("REMOTE")
    );
    gpu && local
}

fn has_special_cache_keys(map: &[(Value, Value)]) -> bool {
    if field(map, "lora_name").is_some_and(|value| !value.is_nil()) {
        return true;
    }
    field(map, "extra_keys")
        .and_then(Value::as_array)
        .is_some_and(|values| values.iter().any(|value| !value.is_nil()))
}

fn hashes_field(map: &[(Value, Value)], name: &str) -> Result<Vec<BlockHash>> {
    field_required(map, name)?
        .as_array()
        .ok_or_else(|| anyhow!("KV {name} is not an array"))?
        .iter()
        .map(decode_hash)
        .collect()
}

fn decode_hash(value: &Value) -> Result<BlockHash> {
    match value {
        Value::Binary(bytes) => Ok(BlockHash::Bytes(bytes.clone())),
        Value::Integer(integer) => integer
            .as_u64()
            .map(BlockHash::Integer)
            .ok_or_else(|| anyhow!("KV block hash integer is negative")),
        _ => bail!("KV block hash is neither bytes nor integer"),
    }
}

fn integer_array(value: &Value) -> Result<Vec<u64>> {
    value
        .as_array()
        .ok_or_else(|| anyhow!("KV token_ids is not an array"))?
        .iter()
        .map(unsigned)
        .collect()
}

fn unsigned(value: &Value) -> Result<u64> {
    value
        .as_u64()
        .ok_or_else(|| anyhow!("KV integer is negative or out of range"))
}

fn optional_group(map: &[(Value, Value)]) -> Result<i64> {
    match field(map, "group_idx") {
        None | Some(Value::Nil) => Ok(0),
        Some(value) => value
            .as_i64()
            .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
            .ok_or_else(|| anyhow!("KV group_idx is invalid")),
    }
}

fn field_required<'a>(map: &'a [(Value, Value)], name: &str) -> Result<&'a Value> {
    field(map, name).ok_or_else(|| anyhow!("KV event is missing {name}"))
}

fn field<'a>(map: &'a [(Value, Value)], name: &str) -> Option<&'a Value> {
    map.iter()
        .find(|(key, _)| key.as_str() == Some(name))
        .map(|(_, value)| value)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use axum::{
        Json, Router,
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
    use crate::config::{NodeConfig, PrefixConfig, ProviderConfig, VllmKvEventsConfig};

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
"#;
        let telemetry = parse_metrics(body).unwrap();
        assert_eq!(telemetry.running, 2);
        assert_eq!(telemetry.waiting, 3);
        assert_eq!(telemetry.kv_cache_usage, Some(0.75));
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
        exact.configure_node_owned(node.id(), 10, node.instance_id());
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
        exact.configure_node_owned(node.id(), 10, node.instance_id());
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
    async fn accepts_v025_and_uses_external_load() {
        let (base_url, server) = management_server("0.25.0").await;
        let node = vllm_node(&base_url);
        poll_node(&Client::new(), &node, true).await;
        assert_eq!(node.provider_state(), ProviderState::Ready);
        assert_eq!(node.scheduling_load(), 5);
        server.abort();
    }

    #[tokio::test]
    async fn rejects_vllm_below_v025() {
        let (base_url, server) = management_server("0.24.1").await;
        let node = vllm_node(&base_url);
        poll_node(&Client::new(), &node, true).await;
        assert_eq!(node.provider_state(), ProviderState::Incompatible);
        assert!(!node.provider_is_ready());
        server.abort();
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
        scheduler
            .exact_cache_directory()
            .configure_node_owned(node.id(), 10, node.instance_id());
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
        scheduler
            .exact_cache_directory()
            .configure_node_owned(node.id(), 10, node.instance_id());
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
        scheduler
            .exact_cache_directory()
            .configure_node_owned(node.id(), 10, node.instance_id());
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
}
