use std::{
    collections::HashMap,
    env,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use http::{HeaderMap, HeaderName, HeaderValue, header::AUTHORIZATION};
use parking_lot::Mutex;
use serde::Serialize;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use url::Url;

use crate::config::{CircuitBreakerConfig, HealthConfig, NodeConfig, ProviderConfig, ProviderKind};

const EWMA_ALPHA: f64 = 0.20;
static NEXT_NODE_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthState {
    Starting,
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderState {
    Generic,
    Checking,
    Ready,
    Incompatible,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleState {
    Serving,
    Draining,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

impl LifecycleState {
    const fn encode(self) -> u8 {
        match self {
            Self::Serving => 0,
            Self::Draining => 1,
        }
    }

    const fn decode(value: u8) -> Self {
        if value == 1 {
            Self::Draining
        } else {
            Self::Serving
        }
    }
}

#[derive(Debug)]
struct CircuitRuntime {
    state: CircuitState,
    consecutive_failures: u32,
    half_open_successes: u32,
    half_open_in_flight: usize,
    epoch: u64,
    opened_until: Option<Instant>,
    opened_until_unix_ms: Option<u64>,
    last_change_unix_ms: u64,
}

impl Default for CircuitRuntime {
    fn default() -> Self {
        Self {
            state: CircuitState::Closed,
            consecutive_failures: 0,
            half_open_successes: 0,
            half_open_in_flight: 0,
            epoch: 0,
            opened_until: None,
            opened_until_unix_ms: None,
            last_change_unix_ms: unix_millis(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct CircuitTicket {
    epoch: u64,
    half_open: bool,
}

#[derive(Clone, Debug, Default)]
struct ProviderRuntime {
    state: Option<ProviderState>,
    version: Option<String>,
    running: usize,
    waiting: usize,
    kv_cache_usage: Option<f64>,
    telemetry_updated_unix_ms: Option<u64>,
    compatibility_error: Option<String>,
    telemetry_error: Option<String>,
    kv_event_error: Option<String>,
}

impl HealthState {
    fn encode(self) -> u8 {
        match self {
            Self::Starting => 0,
            Self::Healthy => 1,
            Self::Degraded => 2,
            Self::Unhealthy => 3,
        }
    }

    fn decode(value: u8) -> Self {
        match value {
            1 => Self::Healthy,
            2 => Self::Degraded,
            3 => Self::Unhealthy,
            _ => Self::Starting,
        }
    }

    pub fn is_routable(self) -> bool {
        matches!(self, Self::Healthy | Self::Degraded)
    }

    pub fn is_ready(self) -> bool {
        matches!(self, Self::Healthy | Self::Degraded)
    }
}

#[derive(Clone, Debug)]
struct RuntimeStats {
    latency_ewma_ms: f64,
    error_ewma: f64,
    consecutive_active_failures: u32,
    consecutive_passive_failures: u32,
    consecutive_probe_successes: u32,
    last_error: Option<String>,
    last_change_unix_ms: u64,
}

impl Default for RuntimeStats {
    fn default() -> Self {
        Self {
            latency_ewma_ms: 0.0,
            error_ewma: 0.0,
            consecutive_active_failures: 0,
            consecutive_passive_failures: 0,
            consecutive_probe_successes: 0,
            last_error: None,
            last_change_unix_ms: unix_millis(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct NodeSnapshot {
    pub id: String,
    pub base_url: String,
    pub health: HealthState,
    pub lifecycle: LifecycleState,
    pub circuit: CircuitState,
    pub circuit_open_until_unix_ms: Option<u64>,
    pub circuit_failures: u32,
    pub circuit_half_open_in_flight: usize,
    pub active: usize,
    pub available: usize,
    pub max_concurrency: usize,
    pub weight: f64,
    pub latency_ewma_ms: f64,
    pub error_ewma: f64,
    pub last_error: Option<String>,
    pub last_change_unix_ms: u64,
    pub provider: ProviderKind,
    pub provider_state: ProviderState,
    pub provider_version: Option<String>,
    pub provider_generation: u64,
    pub upstream_running: Option<usize>,
    pub upstream_waiting: Option<usize>,
    pub kv_cache_usage: Option<f64>,
    pub provider_telemetry_updated_unix_ms: Option<u64>,
    pub provider_last_error: Option<String>,
}

#[derive(Debug)]
pub struct Node {
    id: String,
    instance_id: u64,
    base_url: Url,
    health_url: Url,
    models: HashMap<String, String>,
    max_concurrency: usize,
    weight: f64,
    headers: HeaderMap,
    provider: ProviderConfig,
    provider_runtime: Mutex<ProviderRuntime>,
    provider_generation: AtomicU64,
    route_while_starting: bool,
    lifecycle: AtomicU8,
    retired: AtomicBool,
    circuit_config: CircuitBreakerConfig,
    circuit: Mutex<CircuitRuntime>,
    semaphore: Arc<Semaphore>,
    active: AtomicUsize,
    health: AtomicU8,
    stats: Mutex<RuntimeStats>,
}

impl Node {
    pub fn from_config(config: &NodeConfig) -> Result<Arc<Self>> {
        Self::from_config_with_policies(config, true, CircuitBreakerConfig::default())
    }

    pub fn from_config_with_startup_policy(
        config: &NodeConfig,
        route_while_starting: bool,
    ) -> Result<Arc<Self>> {
        Self::from_config_with_policies(
            config,
            route_while_starting,
            CircuitBreakerConfig::default(),
        )
    }

    pub fn from_config_with_policies(
        config: &NodeConfig,
        route_while_starting: bool,
        circuit_config: CircuitBreakerConfig,
    ) -> Result<Arc<Self>> {
        let mut base_url = Url::parse(&config.base_url)
            .with_context(|| format!("node {} has invalid base_url", config.id))?;
        if !base_url.path().ends_with('/') {
            let path = format!("{}/", base_url.path());
            base_url.set_path(&path);
        }
        let health_url = base_url
            .join(&config.health_path)
            .with_context(|| format!("node {} has invalid health_path", config.id))?;
        if health_url.origin() != base_url.origin() {
            bail!(
                "node {} health_path must stay on the upstream origin",
                config.id
            );
        }

        let mut headers = HeaderMap::new();
        for (name, value) in &config.headers {
            let name = HeaderName::from_bytes(name.as_bytes())?;
            let mut value = HeaderValue::from_str(value)
                .with_context(|| format!("node {} has invalid value for {name}", config.id))?;
            value.set_sensitive(true);
            headers.insert(name, value);
        }
        for (name, variable) in &config.headers_from_env {
            let name = HeaderName::from_bytes(name.as_bytes())?;
            let secret = env::var(variable).with_context(|| {
                format!(
                    "node {} requires missing environment variable {variable}",
                    config.id
                )
            })?;
            if secret.trim().is_empty() {
                bail!(
                    "node {} header environment variable {variable} is empty",
                    config.id
                );
            }
            let mut value = HeaderValue::from_str(&secret).with_context(|| {
                format!("node {} has invalid environment header {name}", config.id)
            })?;
            value.set_sensitive(true);
            headers.insert(name, value);
        }
        if let Some(variable) = &config.api_key_env {
            let key = env::var(variable).with_context(|| {
                format!(
                    "node {} requires missing environment variable {variable}",
                    config.id
                )
            })?;
            if key.trim().is_empty() {
                bail!("node {} API key environment variable is empty", config.id);
            }
            let mut value = HeaderValue::from_str(&format!("Bearer {key}")).with_context(|| {
                format!("node {} API key is not a valid header value", config.id)
            })?;
            value.set_sensitive(true);
            headers.insert(AUTHORIZATION, value);
        }

        Ok(Arc::new(Self {
            id: config.id.clone(),
            instance_id: NEXT_NODE_INSTANCE_ID.fetch_add(1, Ordering::Relaxed),
            base_url,
            health_url,
            models: config.models.clone(),
            max_concurrency: config.max_concurrency,
            weight: config.weight,
            headers,
            provider: config.provider.clone(),
            provider_runtime: Mutex::new(ProviderRuntime {
                state: Some(match config.provider.kind {
                    ProviderKind::Openai => ProviderState::Generic,
                    ProviderKind::Vllm => ProviderState::Checking,
                }),
                ..ProviderRuntime::default()
            }),
            provider_generation: AtomicU64::new(0),
            route_while_starting,
            lifecycle: AtomicU8::new(
                if config.draining {
                    LifecycleState::Draining
                } else {
                    LifecycleState::Serving
                }
                .encode(),
            ),
            retired: AtomicBool::new(false),
            circuit_config,
            circuit: Mutex::new(CircuitRuntime::default()),
            semaphore: Arc::new(Semaphore::new(config.max_concurrency)),
            active: AtomicUsize::new(0),
            health: AtomicU8::new(HealthState::Starting.encode()),
            stats: Mutex::new(RuntimeStats::default()),
        }))
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn instance_id(&self) -> u64 {
        self.instance_id
    }

    pub fn weight(&self) -> f64 {
        self.weight
    }

    pub fn max_concurrency(&self) -> usize {
        self.max_concurrency
    }

    pub fn active(&self) -> usize {
        self.active.load(Ordering::Relaxed)
    }

    pub fn health(&self) -> HealthState {
        HealthState::decode(self.health.load(Ordering::Acquire))
    }

    pub fn is_routable(&self) -> bool {
        self.lifecycle() == LifecycleState::Serving
            && self.is_health_state_routable(self.health())
            && self.provider_is_ready()
            && self.circuit_accepting_requests()
    }

    pub fn lifecycle(&self) -> LifecycleState {
        LifecycleState::decode(self.lifecycle.load(Ordering::Acquire))
    }

    pub fn is_retired(&self) -> bool {
        self.retired.load(Ordering::Acquire)
    }

    pub fn retire(&self) {
        self.retired.store(true, Ordering::Release);
        self.set_draining(true);
    }

    pub fn set_draining(&self, draining: bool) -> bool {
        let next = if draining {
            LifecycleState::Draining
        } else {
            LifecycleState::Serving
        };
        self.lifecycle.swap(next.encode(), Ordering::AcqRel) != next.encode()
    }

    pub fn circuit_state(&self) -> CircuitState {
        let mut circuit = self.circuit.lock();
        Self::refresh_circuit_locked(&mut circuit);
        circuit.state
    }

    pub fn is_health_state_routable(&self, health: HealthState) -> bool {
        match health {
            HealthState::Starting => self.route_while_starting,
            HealthState::Healthy | HealthState::Degraded => true,
            HealthState::Unhealthy => false,
        }
    }

    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    pub fn health_url(&self) -> &Url {
        &self.health_url
    }

    pub fn provider(&self) -> &ProviderConfig {
        &self.provider
    }

    pub fn provider_state(&self) -> ProviderState {
        self.provider_runtime
            .lock()
            .state
            .unwrap_or(ProviderState::Checking)
    }

    pub fn provider_is_ready(&self) -> bool {
        matches!(
            self.provider_state(),
            ProviderState::Generic | ProviderState::Ready
        )
    }

    pub fn provider_generation(&self) -> u64 {
        self.provider_generation.load(Ordering::Acquire)
    }

    pub fn bump_provider_generation(&self) {
        self.provider_generation.fetch_add(1, Ordering::AcqRel);
    }

    pub fn provider_url(&self, path: &str) -> Result<Url> {
        let url = self
            .base_url
            .join(path)
            .with_context(|| format!("failed to build provider URL for node {}", self.id))?;
        if url.origin() != self.base_url.origin() {
            bail!("provider URL for node {} changed origin", self.id);
        }
        Ok(url)
    }

    pub fn record_vllm_ready(&self, version: String) {
        let mut runtime = self.provider_runtime.lock();
        if runtime.state == Some(ProviderState::Incompatible)
            || runtime
                .version
                .as_ref()
                .is_some_and(|current| current != &version)
        {
            self.bump_provider_generation();
        }
        runtime.state = Some(ProviderState::Ready);
        runtime.version = Some(version);
        runtime.compatibility_error = None;
    }

    pub fn record_vllm_incompatible(&self, version: Option<String>, error: String) -> bool {
        let mut runtime = self.provider_runtime.lock();
        let changed = runtime.state != Some(ProviderState::Incompatible)
            || runtime.version.as_ref() != version.as_ref()
            || runtime.compatibility_error.as_ref() != Some(&error);
        if changed {
            self.bump_provider_generation();
        }
        runtime.state = Some(ProviderState::Incompatible);
        runtime.version = version;
        runtime.compatibility_error = Some(error);
        runtime.telemetry_updated_unix_ms = None;
        changed
    }

    pub fn record_provider_telemetry_error(&self, error: String) -> bool {
        let mut runtime = self.provider_runtime.lock();
        let changed = runtime.telemetry_error.as_ref() != Some(&error);
        runtime.telemetry_error = Some(error);
        changed
    }

    pub fn record_kv_event_error(&self, error: String) {
        self.provider_runtime.lock().kv_event_error = Some(error);
    }

    pub fn record_kv_event_success(&self) {
        self.provider_runtime.lock().kv_event_error = None;
    }

    pub fn record_vllm_telemetry(&self, running: usize, waiting: usize, kv_usage: Option<f64>) {
        let mut runtime = self.provider_runtime.lock();
        runtime.running = running;
        runtime.waiting = waiting;
        runtime.kv_cache_usage = kv_usage;
        runtime.telemetry_updated_unix_ms = Some(unix_millis());
        runtime.telemetry_error = None;
    }

    pub fn scheduling_load(&self) -> usize {
        let local = self.active();
        if self.provider.kind != ProviderKind::Vllm {
            return local;
        }
        let runtime = self.provider_runtime.lock();
        let fresh = runtime.telemetry_updated_unix_ms.is_some_and(|updated| {
            unix_millis().saturating_sub(updated) <= self.provider.telemetry_stale_ms
        });
        if fresh {
            local.max(runtime.running.saturating_add(runtime.waiting))
        } else {
            local
        }
    }

    pub fn fresh_vllm_waiting(&self) -> Option<usize> {
        if self.provider.kind != ProviderKind::Vllm {
            return None;
        }
        let runtime = self.provider_runtime.lock();
        runtime
            .telemetry_updated_unix_ms
            .is_some_and(|updated| {
                unix_millis().saturating_sub(updated) <= self.provider.telemetry_stale_ms
            })
            .then_some(runtime.waiting)
    }

    pub fn upstream_model(&self, public_model: Option<&str>) -> Option<Option<String>> {
        let Some(public_model) = public_model else {
            return Some(None);
        };
        if let Some(upstream) = self.models.get(public_model) {
            return Some(Some(if upstream == "*" {
                public_model.to_owned()
            } else {
                upstream.clone()
            }));
        }
        self.models.get("*").map(|upstream| {
            Some(if upstream == "*" {
                public_model.to_owned()
            } else {
                upstream.clone()
            })
        })
    }

    pub fn explicit_models(&self) -> impl Iterator<Item = (&str, &str)> {
        self.models
            .iter()
            .filter(|(public, _)| public.as_str() != "*")
            .map(|(public, upstream)| (public.as_str(), upstream.as_str()))
    }

    pub fn upstream_url(&self, endpoint: &str, query: Option<&str>) -> Result<Url> {
        let mut url = self
            .base_url
            .join(endpoint.trim_start_matches('/'))
            .with_context(|| format!("failed to build upstream URL for node {}", self.id))?;
        url.set_query(query);
        Ok(url)
    }

    fn circuit_accepting_requests(&self) -> bool {
        let mut circuit = self.circuit.lock();
        Self::refresh_circuit_locked(&mut circuit);
        match circuit.state {
            CircuitState::Closed => true,
            CircuitState::Open => false,
            CircuitState::HalfOpen => {
                circuit.half_open_in_flight < self.circuit_config.half_open_max_requests
            }
        }
    }

    fn begin_circuit_request(&self) -> Option<CircuitTicket> {
        if self.is_retired()
            || self.lifecycle() != LifecycleState::Serving
            || !self.is_health_state_routable(self.health())
            || !self.provider_is_ready()
        {
            return None;
        }
        let mut circuit = self.circuit.lock();
        if self.is_retired()
            || self.lifecycle() != LifecycleState::Serving
            || !self.is_health_state_routable(self.health())
        {
            return None;
        }
        Self::refresh_circuit_locked(&mut circuit);
        match circuit.state {
            CircuitState::Closed => Some(CircuitTicket {
                epoch: circuit.epoch,
                half_open: false,
            }),
            CircuitState::HalfOpen
                if circuit.half_open_in_flight < self.circuit_config.half_open_max_requests =>
            {
                circuit.half_open_in_flight += 1;
                Some(CircuitTicket {
                    epoch: circuit.epoch,
                    half_open: true,
                })
            }
            CircuitState::Open | CircuitState::HalfOpen => None,
        }
    }

    fn refresh_circuit_locked(circuit: &mut CircuitRuntime) {
        if circuit.state == CircuitState::Open
            && circuit
                .opened_until
                .is_some_and(|until| Instant::now() >= until)
        {
            circuit.state = CircuitState::HalfOpen;
            circuit.half_open_successes = 0;
            circuit.half_open_in_flight = 0;
            circuit.opened_until = None;
            circuit.opened_until_unix_ms = None;
            circuit.epoch = circuit.epoch.wrapping_add(1);
            circuit.last_change_unix_ms = unix_millis();
        }
    }

    fn release_circuit_ticket(&self, ticket: CircuitTicket) {
        if !ticket.half_open {
            return;
        }
        let mut circuit = self.circuit.lock();
        if circuit.state == CircuitState::HalfOpen && circuit.epoch == ticket.epoch {
            circuit.half_open_in_flight = circuit.half_open_in_flight.saturating_sub(1);
        }
    }

    fn record_circuit_success(&self, ticket: Option<CircuitTicket>) {
        let mut circuit = self.circuit.lock();
        Self::refresh_circuit_locked(&mut circuit);
        match circuit.state {
            CircuitState::Closed => circuit.consecutive_failures = 0,
            CircuitState::HalfOpen
                if ticket
                    .is_some_and(|ticket| ticket.half_open && ticket.epoch == circuit.epoch) =>
            {
                circuit.half_open_successes = circuit.half_open_successes.saturating_add(1);
                if circuit.half_open_successes >= self.circuit_config.half_open_success_threshold {
                    circuit.state = CircuitState::Closed;
                    circuit.consecutive_failures = 0;
                    circuit.half_open_successes = 0;
                    circuit.half_open_in_flight = 0;
                    circuit.opened_until = None;
                    circuit.opened_until_unix_ms = None;
                    circuit.epoch = circuit.epoch.wrapping_add(1);
                    circuit.last_change_unix_ms = unix_millis();
                }
            }
            CircuitState::Open | CircuitState::HalfOpen => {}
        }
    }

    fn record_circuit_failure(&self, ticket: Option<CircuitTicket>) {
        let mut circuit = self.circuit.lock();
        Self::refresh_circuit_locked(&mut circuit);
        let should_open = match circuit.state {
            CircuitState::Closed => {
                circuit.consecutive_failures = circuit.consecutive_failures.saturating_add(1);
                circuit.consecutive_failures >= self.circuit_config.failure_threshold
            }
            CircuitState::HalfOpen => {
                ticket.is_some_and(|ticket| ticket.half_open && ticket.epoch == circuit.epoch)
            }
            CircuitState::Open => false,
        };
        if should_open {
            let now = unix_millis();
            circuit.state = CircuitState::Open;
            circuit.half_open_successes = 0;
            circuit.half_open_in_flight = 0;
            circuit.opened_until = Some(Instant::now() + self.circuit_config.open_duration());
            circuit.opened_until_unix_ms = Some(now.saturating_add(self.circuit_config.open_ms));
            circuit.epoch = circuit.epoch.wrapping_add(1);
            circuit.last_change_unix_ms = now;
        }
    }

    pub fn try_acquire(self: &Arc<Self>, notify: Arc<Notify>) -> Option<NodeLease> {
        let permit = Arc::clone(&self.semaphore).try_acquire_owned().ok()?;
        let circuit_ticket = self.begin_circuit_request()?;
        self.active.fetch_add(1, Ordering::AcqRel);
        Some(NodeLease {
            node: Arc::clone(self),
            permit: Some(permit),
            circuit_ticket: Some(circuit_ticket),
            notify,
        })
    }

    pub(crate) async fn reserve(self: Arc<Self>) -> NodeReservation {
        let permit = Arc::clone(&self.semaphore)
            .acquire_owned()
            .await
            .expect("node semaphore is never closed");
        NodeReservation { node: self, permit }
    }

    pub fn score_stats(&self) -> (f64, f64) {
        let stats = self.stats.lock();
        (stats.latency_ewma_ms, stats.error_ewma)
    }

    pub fn record_request_success(&self, latency: Duration) {
        self.record_request_success_with_ticket(latency, None);
    }

    fn record_request_success_with_ticket(&self, latency: Duration, ticket: Option<CircuitTicket>) {
        let mut stats = self.stats.lock();
        update_ewma(&mut stats.latency_ewma_ms, latency.as_secs_f64() * 1_000.0);
        stats.error_ewma *= 1.0 - EWMA_ALPHA;
        stats.consecutive_passive_failures = 0;
        drop(stats);
        self.record_circuit_success(ticket);
    }

    pub fn record_overload(&self) {
        let mut stats = self.stats.lock();
        stats.error_ewma = ewma(stats.error_ewma, 0.5);
    }

    pub fn record_passive_failure(&self, message: impl Into<String>, health: &HealthConfig) {
        self.record_passive_failure_with_ticket(message, health, None);
    }

    fn record_passive_failure_with_ticket(
        &self,
        message: impl Into<String>,
        health: &HealthConfig,
        ticket: Option<CircuitTicket>,
    ) {
        let mut stats = self.stats.lock();
        stats.error_ewma = ewma(stats.error_ewma, 1.0);
        stats.consecutive_probe_successes = 0;
        stats.consecutive_passive_failures = stats.consecutive_passive_failures.saturating_add(1);
        stats.last_error = Some(message.into());
        if stats.consecutive_passive_failures >= health.passive_failure_threshold {
            self.set_health_locked(HealthState::Unhealthy, &mut stats);
        } else if self.health() == HealthState::Healthy {
            self.set_health_locked(HealthState::Degraded, &mut stats);
        }
        drop(stats);
        self.record_circuit_failure(ticket);
    }

    pub fn record_probe_success(&self, health: &HealthConfig) {
        let mut stats = self.stats.lock();
        let current_health = self.health();
        stats.consecutive_active_failures = 0;
        match current_health {
            HealthState::Starting => {
                stats.consecutive_probe_successes = 0;
                stats.consecutive_passive_failures = 0;
                stats.last_error = None;
                self.set_health_locked(HealthState::Healthy, &mut stats);
            }
            HealthState::Healthy => {
                stats.consecutive_probe_successes = 0;
                stats.last_error = None;
            }
            HealthState::Degraded | HealthState::Unhealthy => {
                stats.consecutive_probe_successes =
                    stats.consecutive_probe_successes.saturating_add(1);
                if stats.consecutive_probe_successes >= health.healthy_threshold {
                    stats.consecutive_probe_successes = 0;
                    stats.consecutive_passive_failures = 0;
                    stats.last_error = None;
                    self.set_health_locked(HealthState::Healthy, &mut stats);
                }
            }
        }
    }

    pub fn record_probe_failure(&self, message: impl Into<String>, health: &HealthConfig) {
        let mut stats = self.stats.lock();
        stats.consecutive_probe_successes = 0;
        stats.consecutive_active_failures = stats.consecutive_active_failures.saturating_add(1);
        stats.last_error = Some(message.into());
        if stats.consecutive_active_failures >= health.unhealthy_threshold {
            self.set_health_locked(HealthState::Unhealthy, &mut stats);
        } else if self.health() == HealthState::Healthy {
            self.set_health_locked(HealthState::Degraded, &mut stats);
        }
    }

    fn set_health_locked(&self, next_health: HealthState, runtime: &mut RuntimeStats) {
        let previous = self.health.swap(next_health.encode(), Ordering::AcqRel);
        if previous != next_health.encode() {
            runtime.last_change_unix_ms = unix_millis();
        }
    }

    pub fn snapshot(&self) -> NodeSnapshot {
        let active = self.active();
        let stats = self.stats.lock();
        let provider = self.provider_runtime.lock();
        let mut circuit = self.circuit.lock();
        Self::refresh_circuit_locked(&mut circuit);
        let has_telemetry = provider.telemetry_updated_unix_ms.is_some();
        NodeSnapshot {
            id: self.id.clone(),
            base_url: self.base_url.as_str().trim_end_matches('/').to_owned(),
            health: self.health(),
            lifecycle: self.lifecycle(),
            circuit: circuit.state,
            circuit_open_until_unix_ms: circuit.opened_until_unix_ms,
            circuit_failures: circuit.consecutive_failures,
            circuit_half_open_in_flight: circuit.half_open_in_flight,
            active,
            available: self.semaphore.available_permits(),
            max_concurrency: self.max_concurrency,
            weight: self.weight,
            latency_ewma_ms: stats.latency_ewma_ms,
            error_ewma: stats.error_ewma,
            last_error: stats.last_error.clone(),
            last_change_unix_ms: stats.last_change_unix_ms,
            provider: self.provider.kind,
            provider_state: provider.state.unwrap_or(ProviderState::Checking),
            provider_version: provider.version.clone(),
            provider_generation: self.provider_generation(),
            upstream_running: has_telemetry.then_some(provider.running),
            upstream_waiting: has_telemetry.then_some(provider.waiting),
            kv_cache_usage: provider.kv_cache_usage,
            provider_telemetry_updated_unix_ms: provider.telemetry_updated_unix_ms,
            provider_last_error: provider
                .compatibility_error
                .as_ref()
                .or(provider.kv_event_error.as_ref())
                .or(provider.telemetry_error.as_ref())
                .cloned(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct NodeReservation {
    node: Arc<Node>,
    permit: OwnedSemaphorePermit,
}

impl NodeReservation {
    pub(crate) fn try_commit(self, notify: Arc<Notify>) -> Option<NodeLease> {
        let Self { node, permit } = self;
        let circuit_ticket = node.begin_circuit_request()?;
        node.active.fetch_add(1, Ordering::AcqRel);
        Some(NodeLease {
            node,
            permit: Some(permit),
            circuit_ticket: Some(circuit_ticket),
            notify,
        })
    }
}

#[derive(Debug)]
pub struct NodeLease {
    node: Arc<Node>,
    permit: Option<OwnedSemaphorePermit>,
    circuit_ticket: Option<CircuitTicket>,
    notify: Arc<Notify>,
}

impl NodeLease {
    pub fn node(&self) -> &Arc<Node> {
        &self.node
    }

    pub fn record_success(&self, latency: Duration) {
        self.node
            .record_request_success_with_ticket(latency, self.circuit_ticket);
    }

    pub fn record_failure(&self, message: impl Into<String>, health: &HealthConfig) {
        self.node
            .record_passive_failure_with_ticket(message, health, self.circuit_ticket);
    }

    pub fn record_overload(&self) {
        self.node.record_overload();
    }
}

impl Drop for NodeLease {
    fn drop(&mut self) {
        self.node.active.fetch_sub(1, Ordering::AcqRel);
        if let Some(ticket) = self.circuit_ticket.take() {
            self.node.release_circuit_ticket(ticket);
        }
        drop(self.permit.take());
        self.notify.notify_waiters();
    }
}

fn update_ewma(current: &mut f64, sample: f64) {
    *current = if *current == 0.0 {
        sample
    } else {
        ewma(*current, sample)
    };
}

fn ewma(current: f64, sample: f64) -> f64 {
    EWMA_ALPHA.mul_add(sample, (1.0 - EWMA_ALPHA) * current)
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> NodeConfig {
        NodeConfig {
            id: "node-a".to_owned(),
            base_url: "http://localhost:8000/v1".to_owned(),
            models: HashMap::from([
                ("public".to_owned(), "internal".to_owned()),
                ("same".to_owned(), "*".to_owned()),
            ]),
            max_concurrency: 2,
            ..NodeConfig::default()
        }
    }

    #[test]
    fn maps_models_and_builds_urls() {
        let node = Node::from_config(&config()).unwrap();
        assert_eq!(
            node.upstream_model(Some("public")),
            Some(Some("internal".to_owned()))
        );
        assert_eq!(
            node.upstream_model(Some("same")),
            Some(Some("same".to_owned()))
        );
        assert_eq!(
            node.upstream_url("chat/completions", Some("a=b"))
                .unwrap()
                .as_str(),
            "http://localhost:8000/v1/chat/completions?a=b"
        );
    }

    #[test]
    fn enforces_hard_concurrency_limit() {
        let node = Node::from_config(&config()).unwrap();
        let notify = Arc::new(Notify::new());
        let first = node.try_acquire(Arc::clone(&notify)).unwrap();
        let second = node.try_acquire(Arc::clone(&notify)).unwrap();
        assert!(node.try_acquire(notify).is_none());
        drop(first);
        assert_eq!(node.active(), 1);
        drop(second);
    }

    #[test]
    fn unhealthy_recovery_requires_a_fresh_probe_success_streak() {
        let node = Node::from_config(&config()).unwrap();
        let health = HealthConfig {
            healthy_threshold: 2,
            passive_failure_threshold: 1,
            ..HealthConfig::default()
        };

        node.record_probe_success(&health);
        for _ in 0..5 {
            node.record_probe_success(&health);
        }
        node.record_passive_failure("generation failed", &health);
        assert_eq!(node.health(), HealthState::Unhealthy);

        node.record_probe_success(&health);
        assert_eq!(node.health(), HealthState::Unhealthy);
        assert_eq!(
            node.snapshot().last_error.as_deref(),
            Some("generation failed")
        );

        node.record_probe_success(&health);
        assert_eq!(node.health(), HealthState::Healthy);
        assert!(node.snapshot().last_error.is_none());
    }

    #[tokio::test]
    async fn circuit_opens_and_allows_one_half_open_probe() {
        let node = Node::from_config_with_policies(
            &config(),
            true,
            CircuitBreakerConfig {
                failure_threshold: 2,
                open_ms: 10,
                half_open_max_requests: 1,
                half_open_success_threshold: 1,
            },
        )
        .unwrap();
        let health = HealthConfig {
            passive_failure_threshold: 100,
            ..HealthConfig::default()
        };
        node.record_probe_success(&health);
        let notify = Arc::new(Notify::new());

        let first = node.try_acquire(Arc::clone(&notify)).unwrap();
        first.record_failure("first", &health);
        drop(first);
        assert_eq!(node.circuit_state(), CircuitState::Closed);

        let second = node.try_acquire(Arc::clone(&notify)).unwrap();
        second.record_failure("second", &health);
        drop(second);
        assert_eq!(node.circuit_state(), CircuitState::Open);
        assert!(node.try_acquire(Arc::clone(&notify)).is_none());

        tokio::time::sleep(Duration::from_millis(15)).await;
        let probe = node.try_acquire(Arc::clone(&notify)).unwrap();
        assert_eq!(node.circuit_state(), CircuitState::HalfOpen);
        assert!(node.try_acquire(notify).is_none());
        probe.record_success(Duration::from_millis(1));
        drop(probe);
        assert_eq!(node.circuit_state(), CircuitState::Closed);
    }
}
