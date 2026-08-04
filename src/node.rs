use std::{
    collections::HashMap,
    env,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use http::{HeaderMap, HeaderName, HeaderValue, header::AUTHORIZATION};
use parking_lot::Mutex;
use serde::Serialize;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use url::Url;

use crate::config::{HealthConfig, NodeConfig};

const EWMA_ALPHA: f64 = 0.20;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthState {
    Starting,
    Healthy,
    Degraded,
    Unhealthy,
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
    pub active: usize,
    pub available: usize,
    pub max_concurrency: usize,
    pub weight: f64,
    pub latency_ewma_ms: f64,
    pub error_ewma: f64,
    pub last_error: Option<String>,
    pub last_change_unix_ms: u64,
}

#[derive(Debug)]
pub struct Node {
    id: String,
    base_url: Url,
    health_url: Url,
    models: HashMap<String, String>,
    max_concurrency: usize,
    weight: f64,
    headers: HeaderMap,
    route_while_starting: bool,
    semaphore: Arc<Semaphore>,
    active: AtomicUsize,
    health: AtomicU8,
    stats: Mutex<RuntimeStats>,
}

impl Node {
    pub fn from_config(config: &NodeConfig) -> Result<Arc<Self>> {
        Self::from_config_with_startup_policy(config, true)
    }

    pub fn from_config_with_startup_policy(
        config: &NodeConfig,
        route_while_starting: bool,
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
            base_url,
            health_url,
            models: config.models.clone(),
            max_concurrency: config.max_concurrency,
            weight: config.weight,
            headers,
            route_while_starting,
            semaphore: Arc::new(Semaphore::new(config.max_concurrency)),
            active: AtomicUsize::new(0),
            health: AtomicU8::new(HealthState::Starting.encode()),
            stats: Mutex::new(RuntimeStats::default()),
        }))
    }

    pub fn id(&self) -> &str {
        &self.id
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
        self.is_health_state_routable(self.health())
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

    pub fn try_acquire(self: &Arc<Self>, notify: Arc<Notify>) -> Option<NodeLease> {
        let permit = Arc::clone(&self.semaphore).try_acquire_owned().ok()?;
        self.active.fetch_add(1, Ordering::AcqRel);
        Some(NodeLease {
            node: Arc::clone(self),
            permit: Some(permit),
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
        let mut stats = self.stats.lock();
        update_ewma(&mut stats.latency_ewma_ms, latency.as_secs_f64() * 1_000.0);
        stats.error_ewma *= 1.0 - EWMA_ALPHA;
        stats.consecutive_passive_failures = 0;
    }

    pub fn record_overload(&self) {
        let mut stats = self.stats.lock();
        stats.error_ewma = ewma(stats.error_ewma, 0.5);
    }

    pub fn record_passive_failure(&self, message: impl Into<String>, health: &HealthConfig) {
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
        NodeSnapshot {
            id: self.id.clone(),
            base_url: self.base_url.as_str().trim_end_matches('/').to_owned(),
            health: self.health(),
            active,
            available: self.semaphore.available_permits(),
            max_concurrency: self.max_concurrency,
            weight: self.weight,
            latency_ewma_ms: stats.latency_ewma_ms,
            error_ewma: stats.error_ewma,
            last_error: stats.last_error.clone(),
            last_change_unix_ms: stats.last_change_unix_ms,
        }
    }
}

#[derive(Debug)]
pub(crate) struct NodeReservation {
    node: Arc<Node>,
    permit: OwnedSemaphorePermit,
}

impl NodeReservation {
    pub(crate) fn commit(self, notify: Arc<Notify>) -> NodeLease {
        let Self { node, permit } = self;
        node.active.fetch_add(1, Ordering::AcqRel);
        NodeLease {
            node,
            permit: Some(permit),
            notify,
        }
    }
}

#[derive(Debug)]
pub struct NodeLease {
    node: Arc<Node>,
    permit: Option<OwnedSemaphorePermit>,
    notify: Arc<Notify>,
}

impl NodeLease {
    pub fn node(&self) -> &Arc<Node> {
        &self.node
    }
}

impl Drop for NodeLease {
    fn drop(&mut self) {
        self.node.active.fetch_sub(1, Ordering::AcqRel);
        drop(self.permit.take());
        self.notify.notify_one();
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
}
