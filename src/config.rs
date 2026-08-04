use std::{
    collections::{HashMap, HashSet},
    fs,
    net::SocketAddr,
    path::Path,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Settings {
    pub server: ServerConfig,
    pub routing: RoutingConfig,
    pub health: HealthConfig,
    pub retry: RetryConfig,
    pub nodes: Vec<NodeConfig>,
}

impl Settings {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read config {}", path.display()))?;
        let settings: Self = serde_yaml_ng::from_str(&raw)
            .with_context(|| format!("failed to parse config {}", path.display()))?;
        settings.validate()?;
        Ok(settings)
    }

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
        if self.server.connect_timeout_ms == 0
            || self.server.upstream_header_timeout_ms == 0
            || self.server.stream_idle_timeout_ms == 0
            || self.server.upstream_body_timeout_ms == 0
            || self.server.downstream_stall_timeout_ms == 0
            || self.server.shutdown_grace_ms == 0
        {
            bail!("server timeout values must be greater than zero");
        }
        if self.server.max_request_body_bytes == 0 {
            bail!("server.max_request_body_bytes must be greater than zero");
        }

        if self.nodes.is_empty() {
            bail!("at least one upstream node is required");
        }
        if self.routing.queue_timeout_ms == 0 {
            bail!("routing.queue_timeout_ms must be greater than zero");
        }
        if self.routing.queue_max_requests == 0 || self.routing.queue_max_bytes < 1024 {
            bail!("routing queue limits must be greater than zero");
        }
        if self.routing.queue_max_requests > tokio::sync::Semaphore::MAX_PERMITS {
            bail!("routing.queue_max_requests exceeds the runtime semaphore limit");
        }
        if self.routing.queue_max_bytes.div_ceil(1024) > u32::MAX as usize {
            bail!("routing.queue_max_bytes exceeds the supported 4 TiB limit");
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
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    pub listen: String,
    pub admin_listen: String,
    pub connect_timeout_ms: u64,
    pub upstream_header_timeout_ms: u64,
    pub stream_idle_timeout_ms: u64,
    pub upstream_body_timeout_ms: u64,
    pub downstream_stall_timeout_ms: u64,
    pub shutdown_grace_ms: u64,
    pub max_request_body_bytes: usize,
    pub expose_node_header: bool,
    pub log_json: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: "0.0.0.0:8080".to_owned(),
            admin_listen: "127.0.0.1:9090".to_owned(),
            connect_timeout_ms: 5_000,
            upstream_header_timeout_ms: 120_000,
            stream_idle_timeout_ms: 300_000,
            upstream_body_timeout_ms: 3_600_000,
            downstream_stall_timeout_ms: 30_000,
            shutdown_grace_ms: 30_000,
            max_request_body_bytes: 16 * 1024 * 1024,
            expose_node_header: false,
            log_json: false,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RoutingConfig {
    pub queue_timeout_ms: u64,
    pub queue_max_requests: usize,
    pub queue_max_bytes: usize,
    pub load_weight: f64,
    pub latency_weight: f64,
    pub error_weight: f64,
    pub target_latency_ms: f64,
    pub prefix: PrefixConfig,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            queue_timeout_ms: 2_000,
            queue_max_requests: 512,
            queue_max_bytes: 256 * 1024 * 1024,
            load_weight: 1.0,
            latency_weight: 0.20,
            error_weight: 1.0,
            target_latency_ms: 1_000.0,
            prefix: PrefixConfig::default(),
        }
    }
}

impl RoutingConfig {
    pub fn queue_timeout(&self) -> Duration {
        Duration::from_millis(self.queue_timeout_ms)
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
    pub api_key_env: Option<String>,
    pub models: HashMap<String, String>,
    pub max_concurrency: usize,
    pub weight: f64,
    pub health_path: String,
    pub headers: HashMap<String, String>,
    pub headers_from_env: HashMap<String, String>,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            id: String::new(),
            base_url: String::new(),
            api_key_env: None,
            models: HashMap::new(),
            max_concurrency: 1,
            weight: 1.0,
            health_path: "/v1/models".to_owned(),
            headers: HashMap::new(),
            headers_from_env: HashMap::new(),
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
    }
}
