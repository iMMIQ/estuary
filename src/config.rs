use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::PathBuf,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use url::Url;

pub const MAX_TOKENIZE_CACHE_ENTRIES: usize = 65_536;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Settings {
    pub server: ServerConfig,
    pub routing: RoutingConfig,
    pub health: HealthConfig,
    pub circuit_breaker: CircuitBreakerConfig,
    pub retry: RetryConfig,
    pub nodes: Vec<NodeConfig>,
}

impl Settings {
    #[allow(clippy::too_many_lines)]
    pub fn validate(&self) -> Result<()> {
        let public_address = self
            .server
            .listen
            .parse::<SocketAddr>()
            .with_context(|| format!("invalid server.listen: {}", self.server.listen))?;
        let admin_address = self
            .server
            .admin_listen
            .parse::<SocketAddr>()
            .with_context(|| {
                format!("invalid server.admin_listen: {}", self.server.admin_listen)
            })?;
        if public_address.port() == admin_address.port() {
            bail!("server.listen and server.admin_listen must use different ports");
        }
        if !admin_address.ip().is_loopback() && self.server.admin_token.is_none() {
            bail!("a non-loopback server.admin_listen requires server.admin_token");
        }
        if self
            .server
            .admin_token
            .as_deref()
            .is_some_and(|token| token.trim().is_empty())
        {
            bail!("server.admin_token must not be empty");
        }
        if self.server.connect_timeout_ms == 0
            || self.server.request_body_idle_timeout_ms == 0
            || self.server.request_body_timeout_ms == 0
            || self.server.upstream_header_timeout_ms == 0
            || self.server.stream_idle_timeout_ms == 0
            || self.server.upstream_body_timeout_ms == 0
            || self.server.downstream_stall_timeout_ms == 0
            || self.server.control_sync_interval_ms == 0
            || self.server.node_mutation_timeout_ms == 0
            || self.server.withdrawal_delay_ms == 0
            || self.server.shutdown_grace_ms == 0
        {
            bail!("server timeout values must be greater than zero");
        }
        if self.server.max_request_body_bytes == 0 {
            bail!("server.max_request_body_bytes must be greater than zero");
        }
        if self.server.max_connections == 0
            || self.server.max_connections > tokio::sync::Semaphore::MAX_PERMITS
            || self.server.max_admin_connections == 0
            || self.server.max_admin_connections > tokio::sync::Semaphore::MAX_PERMITS
        {
            bail!("server connection limits must be within the runtime semaphore limit");
        }
        if self.server.max_non_streaming_response_bytes == 0 {
            bail!("server.max_non_streaming_response_bytes must be greater than zero");
        }
        if self.server.max_non_streaming_response_bytes > u32::MAX as usize {
            bail!("server.max_non_streaming_response_bytes exceeds the supported 4 GiB limit");
        }
        if self.server.max_buffered_response_bytes < self.server.max_non_streaming_response_bytes {
            bail!(
                "server.max_buffered_response_bytes must cover one maximum-sized non-streaming response"
            );
        }
        if self.server.max_buffered_response_bytes > tokio::sync::Semaphore::MAX_PERMITS {
            bail!("server.max_buffered_response_bytes exceeds the runtime semaphore limit");
        }

        if self.routing.queue_max_requests == 0 || self.routing.queue_max_bytes < 1024 {
            bail!("routing queue limits must be greater than zero");
        }
        if self.routing.prefix.max_trees == 0 || self.routing.prefix.max_directory_chars == 0 {
            bail!("routing prefix directory limits must be greater than zero");
        }
        if self.routing.queue_max_requests > tokio::sync::Semaphore::MAX_PERMITS {
            bail!("routing.queue_max_requests exceeds the runtime semaphore limit");
        }
        if self.routing.queue_max_bytes.div_ceil(1024) > u32::MAX as usize {
            bail!("routing.queue_max_bytes exceeds the supported 4 TiB limit");
        }
        if self.routing.queue_max_bytes < self.server.max_request_body_bytes {
            bail!("routing.queue_max_bytes must cover one maximum-sized request body");
        }
        for (name, value) in [
            ("load_weight", self.routing.load_weight),
            ("latency_weight", self.routing.latency_weight),
            ("error_weight", self.routing.error_weight),
        ] {
            if !value.is_finite() || value < 0.0 {
                bail!("routing.{name} must be finite and non-negative");
            }
        }
        if !self.routing.target_latency_ms.is_finite() || self.routing.target_latency_ms <= 0.0 {
            bail!("routing.target_latency_ms must be finite and greater than zero");
        }
        if self.routing.request_stats_stale_ms == 0 {
            bail!("routing.request_stats_stale_ms must be greater than zero");
        }
        if self.routing.prefix.enabled {
            if !self.routing.prefix.cache_threshold.is_finite()
                || !(0.0..=1.0).contains(&self.routing.prefix.cache_threshold)
            {
                bail!("routing.prefix.cache_threshold must be between 0.0 and 1.0");
            }
            if !self.routing.prefix.balance_rel_threshold.is_finite()
                || self.routing.prefix.balance_rel_threshold <= 1.0
            {
                bail!("routing.prefix.balance_rel_threshold must be greater than 1.0");
            }
            if self.routing.prefix.max_request_chars == 0 {
                bail!("routing.prefix.max_request_chars must be greater than zero");
            }
            if self.routing.prefix.max_tree_chars_per_node == 0 {
                bail!("routing.prefix.max_tree_chars_per_node must be greater than zero");
            }
        }
        if self.health.interval_ms == 0 || self.health.timeout_ms == 0 {
            bail!("health interval and timeout must be greater than zero");
        }
        if self.health.jitter_percent > 50 {
            bail!("health.jitter_percent must not exceed 50");
        }
        if self.health.healthy_threshold == 0
            || self.health.unhealthy_threshold == 0
            || self.health.passive_failure_threshold == 0
        {
            bail!("health thresholds must be greater than zero");
        }
        if self.circuit_breaker.failure_threshold == 0
            || self.circuit_breaker.open_ms == 0
            || self.circuit_breaker.half_open_max_requests == 0
            || self.circuit_breaker.half_open_success_threshold == 0
        {
            bail!("circuit_breaker values must be greater than zero");
        }
        if self.circuit_breaker.half_open_max_requests > tokio::sync::Semaphore::MAX_PERMITS {
            bail!("circuit_breaker.half_open_max_requests exceeds the runtime limit");
        }
        if !(1..=3).contains(&self.retry.max_attempts) {
            bail!("retry.max_attempts must be between 1 and 3");
        }
        for status in &self.retry.statuses {
            if !matches!(*status, 408 | 409 | 425 | 429 | 500..=599) {
                bail!("retry status {status} is not a supported transient HTTP status");
            }
        }

        let mut ids = HashSet::new();
        for node in &self.nodes {
            if node.id.trim().is_empty() {
                bail!("node id must not be empty");
            }
            if !ids.insert(node.id.as_str()) {
                bail!("duplicate node id: {}", node.id);
            }
            let url = Url::parse(&node.base_url)
                .with_context(|| format!("node {} has an invalid base_url", node.id))?;
            if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
                bail!("node {} base_url must be an absolute http(s) URL", node.id);
            }
            if !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
            {
                bail!(
                    "node {} base_url must not contain credentials, query, or fragment",
                    node.id
                );
            }
            if node.max_concurrency == 0 {
                bail!("node {} max_concurrency must be greater than zero", node.id);
            }
            if node.max_concurrency > tokio::sync::Semaphore::MAX_PERMITS {
                bail!("node {} max_concurrency exceeds the runtime limit", node.id);
            }
            if node.health_path.trim().is_empty() {
                bail!("node {} health_path must not be empty", node.id);
            }
            if node
                .api_key
                .as_deref()
                .is_some_and(|key| key.trim().is_empty())
            {
                bail!("node {} api_key must not be empty", node.id);
            }
            if node
                .api_key_env
                .as_deref()
                .is_some_and(|variable| variable.trim().is_empty())
            {
                bail!("node {} api_key_env must not be empty", node.id);
            }
            if !node.weight.is_finite() || node.weight <= 0.0 {
                bail!(
                    "node {} weight must be finite and greater than zero",
                    node.id
                );
            }
            if node.models.is_empty() {
                bail!(
                    "node {} must declare at least one model or a '*' mapping",
                    node.id
                );
            }
            for (public, upstream) in &node.models {
                if public.trim().is_empty() || upstream.trim().is_empty() {
                    bail!("node {} has an empty model mapping", node.id);
                }
            }
            for name in node.headers.keys().chain(node.headers_from_env.keys()) {
                http::HeaderName::from_bytes(name.as_bytes()).with_context(|| {
                    format!("node {} has invalid header name {name:?}", node.id)
                })?;
                if is_reserved_upstream_header(name) {
                    bail!("node {} cannot configure reserved header {name:?}", node.id);
                }
            }
            for variable in node.headers_from_env.values() {
                if variable.trim().is_empty() {
                    bail!("node {} has an empty header environment variable", node.id);
                }
            }
            validate_provider(node, &url)?;
        }
        Ok(())
    }
}

pub fn validate_node_config(node: &NodeConfig) -> Result<()> {
    Settings {
        nodes: vec![node.clone()],
        ..Settings::default()
    }
    .validate()
}

fn validate_provider(node: &NodeConfig, base_url: &Url) -> Result<()> {
    let provider = &node.provider;
    if provider.kind == ProviderKind::Openai {
        if provider.kv_events.is_some() {
            bail!(
                "node {} configures KV events without provider.type: vllm",
                node.id
            );
        }
        return Ok(());
    }

    if provider.monitor_interval_ms < 100 {
        bail!(
            "node {} provider.monitor_interval_ms must be at least 100 ms",
            node.id
        );
    }
    if provider.request_timeout_ms == 0 || provider.telemetry_stale_ms == 0 {
        bail!(
            "node {} vLLM provider timeouts must be greater than zero",
            node.id
        );
    }
    if provider.waiting_threshold == 0 {
        bail!(
            "node {} provider.waiting_threshold must be greater than zero",
            node.id
        );
    }
    if provider.telemetry_stale_ms < provider.monitor_interval_ms {
        bail!(
            "node {} provider.telemetry_stale_ms must not be shorter than monitor_interval_ms",
            node.id
        );
    }
    if provider.tokenize_cache_entries == 0
        || provider.tokenize_cache_entries > MAX_TOKENIZE_CACHE_ENTRIES
    {
        bail!(
            "node {} provider.tokenize_cache_entries must be between 1 and {MAX_TOKENIZE_CACHE_ENTRIES}",
            node.id,
        );
    }
    for (name, path) in [
        ("version_path", &provider.version_path),
        ("metrics_path", &provider.metrics_path),
        ("tokenize_path", &provider.tokenize_path),
    ] {
        if !path.starts_with('/') {
            bail!("node {} provider.{name} must start with '/'", node.id);
        }
        let url = base_url
            .join(path)
            .with_context(|| format!("node {} has invalid provider.{name}", node.id))?;
        if url.origin() != base_url.origin() || url.query().is_some() || url.fragment().is_some() {
            bail!(
                "node {} provider.{name} must stay on the upstream origin",
                node.id
            );
        }
    }
    if let Some(events) = &provider.kv_events {
        validate_zmq_endpoint(&node.id, "endpoint", &events.endpoint)?;
        if let Some(endpoint) = &events.replay_endpoint {
            validate_zmq_endpoint(&node.id, "replay_endpoint", endpoint)?;
        }
        if events.reconnect_ms == 0
            || events.max_blocks == 0
            || events.max_directory_bytes == 0
            || events.max_event_bytes == 0
        {
            bail!("node {} KV event limits must be greater than zero", node.id);
        }
    }
    Ok(())
}

fn validate_zmq_endpoint(node_id: &str, field: &str, endpoint: &str) -> Result<()> {
    let url = Url::parse(endpoint)
        .with_context(|| format!("node {node_id} provider.kv_events.{field} is not a valid URL"))?;
    if url.scheme() != "tcp"
        || url.host_str().is_none_or(|host| host.contains('*'))
        || url.port().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !matches!(url.path(), "" | "/")
    {
        bail!(
            "node {node_id} provider.kv_events.{field} must be a connectable tcp://host:port endpoint"
        );
    }
    Ok(())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    pub listen: String,
    pub admin_listen: String,
    pub admin_token: Option<String>,
    pub admin_freeze_file: Option<PathBuf>,
    pub connect_timeout_ms: u64,
    pub request_body_idle_timeout_ms: u64,
    pub request_body_timeout_ms: u64,
    pub upstream_header_timeout_ms: u64,
    pub stream_idle_timeout_ms: u64,
    pub upstream_body_timeout_ms: u64,
    pub downstream_stall_timeout_ms: u64,
    pub control_sync_interval_ms: u64,
    pub node_mutation_timeout_ms: u64,
    pub withdrawal_delay_ms: u64,
    pub shutdown_grace_ms: u64,
    pub max_request_body_bytes: usize,
    pub max_connections: usize,
    pub max_admin_connections: usize,
    pub max_non_streaming_response_bytes: usize,
    pub max_buffered_response_bytes: usize,
    pub expose_node_header: bool,
    pub log_json: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: "0.0.0.0:8080".to_owned(),
            admin_listen: "127.0.0.1:9090".to_owned(),
            admin_token: None,
            admin_freeze_file: None,
            connect_timeout_ms: 5_000,
            request_body_idle_timeout_ms: 30_000,
            request_body_timeout_ms: 300_000,
            upstream_header_timeout_ms: 120_000,
            stream_idle_timeout_ms: 300_000,
            upstream_body_timeout_ms: 3_600_000,
            downstream_stall_timeout_ms: 30_000,
            control_sync_interval_ms: 500,
            node_mutation_timeout_ms: 30_000,
            withdrawal_delay_ms: 10_000,
            shutdown_grace_ms: 3_660_000,
            max_request_body_bytes: 16 * 1024 * 1024,
            max_connections: 2_048,
            max_admin_connections: 128,
            max_non_streaming_response_bytes: 64 * 1024 * 1024,
            max_buffered_response_bytes: 256 * 1024 * 1024,
            expose_node_header: false,
            log_json: false,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RoutingConfig {
    pub queue_max_requests: usize,
    pub queue_max_bytes: usize,
    pub load_weight: f64,
    pub latency_weight: f64,
    pub error_weight: f64,
    pub target_latency_ms: f64,
    pub request_stats_stale_ms: u64,
    pub prefix: PrefixConfig,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            queue_max_requests: 512,
            queue_max_bytes: 256 * 1024 * 1024,
            load_weight: 1.0,
            latency_weight: 0.20,
            error_weight: 1.0,
            target_latency_ms: 1_000.0,
            request_stats_stale_ms: 60_000,
            prefix: PrefixConfig::default(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct PrefixConfig {
    pub enabled: bool,
    pub cache_threshold: f64,
    pub balance_abs_threshold: usize,
    pub balance_rel_threshold: f64,
    pub max_request_chars: usize,
    pub max_tree_chars_per_node: usize,
    pub max_trees: usize,
    pub max_directory_chars: usize,
}

impl Default for PrefixConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            cache_threshold: 0.5,
            balance_abs_threshold: 2,
            balance_rel_threshold: 1.1,
            max_request_chars: 128 * 1024,
            max_tree_chars_per_node: 1_000_000,
            max_trees: 256,
            max_directory_chars: 16_000_000,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct HealthConfig {
    pub interval_ms: u64,
    pub timeout_ms: u64,
    pub unhealthy_threshold: u32,
    pub healthy_threshold: u32,
    pub passive_failure_threshold: u32,
    pub route_while_starting: bool,
    pub jitter_percent: u8,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            interval_ms: 5_000,
            timeout_ms: 2_000,
            unhealthy_threshold: 3,
            healthy_threshold: 2,
            passive_failure_threshold: 3,
            route_while_starting: false,
            jitter_percent: 20,
        }
    }
}

impl HealthConfig {
    pub fn interval(&self) -> Duration {
        Duration::from_millis(self.interval_ms)
    }

    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CircuitBreakerConfig {
    pub failure_threshold: u32,
    pub open_ms: u64,
    pub half_open_max_requests: usize,
    pub half_open_success_threshold: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            open_ms: 10_000,
            half_open_max_requests: 1,
            half_open_success_threshold: 2,
        }
    }
}

impl CircuitBreakerConfig {
    pub fn open_duration(&self) -> Duration {
        Duration::from_millis(self.open_ms)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RetryConfig {
    pub max_attempts: usize,
    pub statuses: Vec<u16>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            statuses: vec![429, 502, 503, 504],
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct NodeConfig {
    pub id: String,
    pub base_url: String,
    pub api_key: Option<String>,
    pub api_key_env: Option<String>,
    pub models: HashMap<String, String>,
    pub max_concurrency: usize,
    pub weight: f64,
    pub draining: bool,
    pub health_path: String,
    pub headers: HashMap<String, String>,
    pub headers_from_env: HashMap<String, String>,
    pub provider: ProviderConfig,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            id: String::new(),
            base_url: String::new(),
            api_key: None,
            api_key_env: None,
            models: HashMap::new(),
            max_concurrency: 1,
            weight: 1.0,
            draining: false,
            health_path: "/v1/models".to_owned(),
            headers: HashMap::new(),
            headers_from_env: HashMap::new(),
            provider: ProviderConfig::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    #[default]
    Openai,
    Vllm,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AnthropicProtocol {
    #[default]
    Auto,
    Native,
    Responses,
    Chat,
}

impl AnthropicProtocol {
    #[must_use]
    pub fn resolve(self, provider: ProviderKind) -> Self {
        match self {
            Self::Auto if provider == ProviderKind::Vllm => Self::Native,
            Self::Auto => Self::Chat,
            protocol => protocol,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProviderConfig {
    #[serde(rename = "type")]
    pub kind: ProviderKind,
    pub anthropic_protocol: AnthropicProtocol,
    pub version_path: String,
    pub metrics_path: String,
    pub tokenize_path: String,
    pub monitor_interval_ms: u64,
    pub request_timeout_ms: u64,
    pub telemetry_stale_ms: u64,
    pub waiting_threshold: usize,
    pub tokenize_cache_entries: usize,
    pub kv_events: Option<VllmKvEventsConfig>,
}

impl Default for ProviderConfig {
    fn default() -> Self {
        Self {
            kind: ProviderKind::Openai,
            anthropic_protocol: AnthropicProtocol::Auto,
            version_path: "/version".to_owned(),
            metrics_path: "/metrics".to_owned(),
            tokenize_path: "/tokenize".to_owned(),
            monitor_interval_ms: 1_000,
            request_timeout_ms: 2_000,
            telemetry_stale_ms: 5_000,
            waiting_threshold: 8,
            tokenize_cache_entries: 4_096,
            kv_events: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct VllmKvEventsConfig {
    pub endpoint: String,
    pub replay_endpoint: Option<String>,
    pub topic: String,
    pub reconnect_ms: u64,
    pub max_blocks: usize,
    pub max_directory_bytes: usize,
    pub max_event_bytes: usize,
}

impl Default for VllmKvEventsConfig {
    fn default() -> Self {
        Self {
            endpoint: "tcp://127.0.0.1:5557".to_owned(),
            replay_endpoint: None,
            topic: "kv-events".to_owned(),
            reconnect_ms: 1_000,
            max_blocks: 1_000_000,
            max_directory_bytes: 512 * 1024 * 1024,
            max_event_bytes: 16 * 1024 * 1024,
        }
    }
}

fn is_reserved_upstream_header(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "content-length"
            | "host"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "proxy-connection"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "x-gateway-request-id"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_duplicate_nodes() {
        let node = NodeConfig {
            id: "same".to_owned(),
            base_url: "http://localhost:8000/v1".to_owned(),
            models: HashMap::from([("model".to_owned(), "model".to_owned())]),
            ..NodeConfig::default()
        };
        let settings = Settings {
            nodes: vec![node.clone(), node],
            ..Settings::default()
        };
        assert!(settings.validate().is_err());
    }

    #[test]
    fn remote_admin_listener_requires_an_authentication_token() {
        let mut settings = Settings::default();
        settings.server.admin_listen = "0.0.0.0:9090".to_owned();
        assert!(settings.validate().is_err());
        settings.server.admin_token = Some("secret".to_owned());
        assert!(settings.validate().is_ok());
    }

    #[test]
    fn rejects_vllm_monitor_intervals_below_the_safe_floor() {
        let mut node = NodeConfig {
            id: "node".to_owned(),
            base_url: "http://localhost:8000/v1".to_owned(),
            models: HashMap::from([("model".to_owned(), "model".to_owned())]),
            provider: ProviderConfig {
                kind: ProviderKind::Vllm,
                monitor_interval_ms: 99,
                ..ProviderConfig::default()
            },
            ..NodeConfig::default()
        };
        assert!(validate_node_config(&node).is_err());
        node.provider.monitor_interval_ms = 100;
        assert!(validate_node_config(&node).is_ok());
    }

    #[test]
    fn rejects_zero_response_body_timeouts() {
        let node = NodeConfig {
            id: "node".to_owned(),
            base_url: "http://localhost:8000/v1".to_owned(),
            models: HashMap::from([("model".to_owned(), "model".to_owned())]),
            ..NodeConfig::default()
        };
        let settings = Settings {
            nodes: vec![node],
            ..Settings::default()
        };

        let mut zero_body_timeout = settings.clone();
        zero_body_timeout.server.upstream_body_timeout_ms = 0;
        assert!(zero_body_timeout.validate().is_err());

        let mut zero_stall_timeout = settings;
        zero_stall_timeout.server.downstream_stall_timeout_ms = 0;
        assert!(zero_stall_timeout.validate().is_err());

        let mut zero_response_limit = zero_stall_timeout;
        zero_response_limit.server.downstream_stall_timeout_ms = 1;
        zero_response_limit.server.max_non_streaming_response_bytes = 0;
        assert!(zero_response_limit.validate().is_err());

        let mut undersized_global_limit = Settings::default();
        undersized_global_limit.server.max_buffered_response_bytes = undersized_global_limit
            .server
            .max_non_streaming_response_bytes
            - 1;
        assert!(undersized_global_limit.validate().is_err());
    }

    #[test]
    fn rejects_zero_long_running_resource_limits() {
        let mut settings = Settings::default();
        settings.server.max_connections = 0;
        assert!(settings.validate().is_err());

        let mut settings = Settings::default();
        settings.routing.prefix.max_trees = 0;
        assert!(settings.validate().is_err());

        let mut settings = Settings::default();
        settings.routing.prefix.max_directory_chars = 0;
        assert!(settings.validate().is_err());

        let mut settings = Settings::default();
        settings.routing.request_stats_stale_ms = 0;
        assert!(settings.validate().is_err());
    }

    #[test]
    fn rejects_zero_circuit_breaker_limits() {
        let node = NodeConfig {
            id: "node".to_owned(),
            base_url: "http://localhost:8000/v1".to_owned(),
            models: HashMap::from([("model".to_owned(), "model".to_owned())]),
            ..NodeConfig::default()
        };
        let mut settings = Settings {
            nodes: vec![node],
            ..Settings::default()
        };
        settings.circuit_breaker.failure_threshold = 0;
        assert!(settings.validate().is_err());
    }

    #[test]
    fn validates_vllm_provider_endpoints() {
        let mut node = NodeConfig {
            id: "vllm".to_owned(),
            base_url: "http://localhost:8000/v1".to_owned(),
            models: HashMap::from([("model".to_owned(), "model".to_owned())]),
            ..NodeConfig::default()
        };
        node.provider.kind = ProviderKind::Vllm;
        node.provider.kv_events = Some(VllmKvEventsConfig::default());
        let settings = Settings {
            nodes: vec![node.clone()],
            ..Settings::default()
        };
        settings.validate().unwrap();

        node.provider.kv_events.as_mut().unwrap().endpoint = "tcp://*:5557".to_owned();
        let invalid = Settings {
            nodes: vec![node],
            ..Settings::default()
        };
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn defaults_waiting_watermark_for_existing_vllm_json() {
        let provider: ProviderConfig = serde_json::from_value(serde_json::json!({
            "type": "vllm"
        }))
        .unwrap();
        assert_eq!(provider.waiting_threshold, 8);
        assert_eq!(provider.anthropic_protocol, AnthropicProtocol::Auto);
    }

    #[test]
    fn rejects_zero_vllm_waiting_watermark() {
        let mut node = NodeConfig {
            id: "vllm".to_owned(),
            base_url: "http://localhost:8000/v1".to_owned(),
            models: HashMap::from([("model".to_owned(), "model".to_owned())]),
            ..NodeConfig::default()
        };
        node.provider.kind = ProviderKind::Vllm;
        node.provider.waiting_threshold = 0;
        let settings = Settings {
            nodes: vec![node],
            ..Settings::default()
        };
        assert!(settings.validate().is_err());
    }

    #[test]
    fn rejects_excessive_vllm_tokenization_cache_capacity() {
        let mut node = NodeConfig {
            id: "vllm".to_owned(),
            base_url: "http://localhost:8000/v1".to_owned(),
            models: HashMap::from([("model".to_owned(), "model".to_owned())]),
            ..NodeConfig::default()
        };
        node.provider.kind = ProviderKind::Vllm;
        node.provider.tokenize_cache_entries = MAX_TOKENIZE_CACHE_ENTRIES + 1;
        let settings = Settings {
            nodes: vec![node],
            ..Settings::default()
        };
        assert!(settings.validate().is_err());
    }

    #[test]
    fn rejects_kv_events_on_generic_provider() {
        let node = NodeConfig {
            id: "generic".to_owned(),
            base_url: "http://localhost:8000/v1".to_owned(),
            models: HashMap::from([("model".to_owned(), "model".to_owned())]),
            provider: ProviderConfig {
                kv_events: Some(VllmKvEventsConfig::default()),
                ..ProviderConfig::default()
            },
            ..NodeConfig::default()
        };
        let settings = Settings {
            nodes: vec![node],
            ..Settings::default()
        };
        assert!(settings.validate().is_err());
    }
}
