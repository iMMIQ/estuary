use std::{path::PathBuf, time::Duration};

use clap::{ArgAction, Args};
use serde::{Deserialize, Serialize};

pub mod contract;
mod validation;

pub use contract::validate_node as validate_node_config;
pub use estuary_contract::config::{
    MAX_TOKENIZE_CACHE_ENTRIES, ModelCapabilityConfig, NodeConfig, ProviderConfig, ProviderKind,
    VllmKvEventsConfig,
};

#[derive(Clone, Debug, Default, Deserialize, Serialize, Args)]
#[serde(default, deny_unknown_fields)]
pub struct Settings {
    #[command(flatten)]
    pub server: ServerConfig,
    #[command(flatten)]
    pub routing: RoutingConfig,
    #[command(flatten)]
    pub health: HealthConfig,
    #[command(flatten)]
    pub circuit_breaker: CircuitBreakerConfig,
    #[command(flatten)]
    pub retry: RetryConfig,
    #[command(flatten)]
    pub session_log: SessionLogConfig,
    #[arg(skip)]
    pub nodes: Vec<NodeConfig>,
}

#[derive(Clone, Debug, Deserialize, Serialize, Args)]
#[serde(default, deny_unknown_fields)]
pub struct SessionLogConfig {
    #[arg(
        id = "session-log-database",
        long = "session-log-database",
        env = "ESTUARY_SESSION_LOG_DATABASE"
    )]
    pub database: Option<PathBuf>,
    #[arg(
        id = "session-log-capture-content",
        long = "session-log-capture-content",
        env = "ESTUARY_SESSION_LOG_CAPTURE_CONTENT",
        action = ArgAction::Set,
        default_value_t = true
    )]
    pub capture_content: bool,
    #[arg(
        id = "session-log-queue-capacity",
        long = "session-log-queue-capacity",
        env = "ESTUARY_SESSION_LOG_QUEUE_CAPACITY",
        default_value_t = 4096
    )]
    pub queue_capacity: usize,
    #[arg(
        id = "session-log-max-content-bytes",
        long = "session-log-max-content-bytes",
        env = "ESTUARY_SESSION_LOG_MAX_CONTENT_BYTES",
        default_value_t = 64 * 1024 * 1024
    )]
    pub max_content_bytes: usize,
    #[arg(
        id = "session-log-max-payload-bytes",
        long = "session-log-max-payload-bytes",
        env = "ESTUARY_SESSION_LOG_MAX_PAYLOAD_BYTES",
        default_value_t = 2 * 1024 * 1024
    )]
    pub max_payload_bytes: usize,
    #[arg(
        id = "session-log-retention-days",
        long = "session-log-retention-days",
        env = "ESTUARY_SESSION_LOG_RETENTION_DAYS",
        default_value_t = 30
    )]
    pub retention_days: u32,
    #[arg(
        id = "session-log-content-retention-days",
        long = "session-log-content-retention-days",
        env = "ESTUARY_SESSION_LOG_CONTENT_RETENTION_DAYS",
        default_value_t = 7
    )]
    pub content_retention_days: u32,
    #[arg(
        id = "session-log-flush-interval-ms",
        long = "session-log-flush-interval-ms",
        env = "ESTUARY_SESSION_LOG_FLUSH_INTERVAL_MS",
        default_value_t = 100
    )]
    pub flush_interval_ms: u64,
}

impl Default for SessionLogConfig {
    fn default() -> Self {
        Self {
            database: None,
            capture_content: true,
            queue_capacity: 4096,
            max_content_bytes: 64 * 1024 * 1024,
            max_payload_bytes: 2 * 1024 * 1024,
            retention_days: 30,
            content_retention_days: 7,
            flush_interval_ms: 100,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Args)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    #[arg(long, env = "ESTUARY_LISTEN", default_value = "0.0.0.0:8080")]
    pub listen: String,
    #[arg(long, env = "ESTUARY_ADMIN_LISTEN", default_value = "127.0.0.1:9090")]
    pub admin_listen: String,
    #[arg(long, env = "ESTUARY_ADMIN_TOKEN")]
    pub admin_token: Option<String>,
    #[arg(long, env = "ESTUARY_CONNECT_TIMEOUT_MS", default_value_t = 5_000)]
    pub connect_timeout_ms: u64,
    #[arg(
        long,
        env = "ESTUARY_REQUEST_BODY_IDLE_TIMEOUT_MS",
        default_value_t = 30_000
    )]
    pub request_body_idle_timeout_ms: u64,
    #[arg(
        long,
        env = "ESTUARY_REQUEST_BODY_TIMEOUT_MS",
        default_value_t = 300_000
    )]
    pub request_body_timeout_ms: u64,
    #[arg(
        long,
        env = "ESTUARY_UPSTREAM_HEADER_TIMEOUT_MS",
        default_value_t = 120_000
    )]
    pub upstream_header_timeout_ms: u64,
    #[arg(
        long,
        env = "ESTUARY_STREAM_IDLE_TIMEOUT_MS",
        default_value_t = 300_000
    )]
    pub stream_idle_timeout_ms: u64,
    #[arg(
        long,
        env = "ESTUARY_UPSTREAM_BODY_TIMEOUT_MS",
        default_value_t = 3_600_000
    )]
    pub upstream_body_timeout_ms: u64,
    #[arg(
        long,
        env = "ESTUARY_DOWNSTREAM_STALL_TIMEOUT_MS",
        default_value_t = 30_000
    )]
    pub downstream_stall_timeout_ms: u64,
    #[arg(long, env = "ESTUARY_CONTROL_SYNC_INTERVAL_MS", default_value_t = 500)]
    pub control_sync_interval_ms: u64,
    #[arg(
        long,
        env = "ESTUARY_NODE_MUTATION_TIMEOUT_MS",
        default_value_t = 30_000
    )]
    pub node_mutation_timeout_ms: u64,
    #[arg(long, env = "ESTUARY_WITHDRAWAL_DELAY_MS", default_value_t = 10_000)]
    pub withdrawal_delay_ms: u64,
    #[arg(long, env = "ESTUARY_SHUTDOWN_GRACE_MS", default_value_t = 3_660_000)]
    pub shutdown_grace_ms: u64,
    #[arg(
        long,
        env = "ESTUARY_MAX_REQUEST_BODY_BYTES",
        default_value_t = 16 * 1024 * 1024
    )]
    pub max_request_body_bytes: usize,
    #[arg(long, env = "ESTUARY_MAX_CONNECTIONS", default_value_t = 2_048)]
    pub max_connections: usize,
    #[arg(long, env = "ESTUARY_MAX_ADMIN_CONNECTIONS", default_value_t = 128)]
    pub max_admin_connections: usize,
    #[arg(
        long,
        env = "ESTUARY_MAX_NON_STREAMING_RESPONSE_BYTES",
        default_value_t = 64 * 1024 * 1024
    )]
    pub max_non_streaming_response_bytes: usize,
    #[arg(
        long,
        env = "ESTUARY_MAX_BUFFERED_RESPONSE_BYTES",
        default_value_t = 256 * 1024 * 1024
    )]
    pub max_buffered_response_bytes: usize,
    #[arg(
        long,
        env = "ESTUARY_EXPOSE_NODE_HEADER",
        action = ArgAction::Set,
        default_value_t = false
    )]
    pub expose_node_header: bool,
    #[arg(
        long,
        env = "ESTUARY_LOG_JSON",
        num_args = 0..=1,
        default_missing_value = "true",
        action = ArgAction::Set,
        default_value_t = false
    )]
    pub log_json: bool,
    #[arg(long, env = "ESTUARY_PUBLIC_API_KEYS", value_delimiter = ',')]
    pub public_api_keys: Vec<String>,
    #[arg(long, env = "ESTUARY_PUBLIC_RPM_LIMIT")]
    pub public_rpm_limit: Option<u32>,
    #[arg(
        long,
        env = "ESTUARY_TRUST_PROXY_HEADERS",
        action = ArgAction::Set,
        default_value_t = false
    )]
    pub trust_proxy_headers: bool,
    #[arg(long, env = "ESTUARY_OBSERVABILITY_MAX_SCOPES", default_value_t = 32)]
    pub observability_max_scopes: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: "0.0.0.0:8080".to_owned(),
            admin_listen: "127.0.0.1:9090".to_owned(),
            admin_token: None,
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
            public_api_keys: Vec::new(),
            public_rpm_limit: None,
            trust_proxy_headers: false,
            observability_max_scopes: 32,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Args)]
#[serde(default, deny_unknown_fields)]
pub struct RoutingConfig {
    #[arg(long, env = "ESTUARY_QUEUE_MAX_REQUESTS", default_value_t = 512)]
    pub queue_max_requests: usize,
    #[arg(
        long,
        env = "ESTUARY_QUEUE_MAX_BYTES",
        default_value_t = 256 * 1024 * 1024
    )]
    pub queue_max_bytes: usize,
    #[arg(long, env = "ESTUARY_LOAD_WEIGHT", default_value_t = 1.0)]
    pub load_weight: f64,
    #[arg(long, env = "ESTUARY_LATENCY_WEIGHT", default_value_t = 0.20)]
    pub latency_weight: f64,
    #[arg(long, env = "ESTUARY_ERROR_WEIGHT", default_value_t = 1.0)]
    pub error_weight: f64,
    #[arg(long, env = "ESTUARY_PREFILL_WEIGHT", default_value_t = 1.0)]
    pub prefill_weight: f64,
    #[arg(long, env = "ESTUARY_DECODE_WEIGHT", default_value_t = 0.25)]
    pub decode_weight: f64,
    #[arg(long, env = "ESTUARY_PREFILL_TOKEN_SCALE", default_value_t = 4_096)]
    pub prefill_token_scale: usize,
    #[arg(long, env = "ESTUARY_DECODE_TOKEN_SCALE", default_value_t = 1_024)]
    pub decode_token_scale: usize,
    #[arg(long, env = "ESTUARY_TARGET_LATENCY_MS", default_value_t = 1_000.0)]
    pub target_latency_ms: f64,
    #[arg(long, env = "ESTUARY_REQUEST_STATS_STALE_MS", default_value_t = 60_000)]
    pub request_stats_stale_ms: u64,
    #[command(flatten)]
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
            prefill_weight: 1.0,
            decode_weight: 0.25,
            prefill_token_scale: 4_096,
            decode_token_scale: 1_024,
            target_latency_ms: 1_000.0,
            request_stats_stale_ms: 60_000,
            prefix: PrefixConfig::default(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Args)]
#[serde(default, deny_unknown_fields)]
pub struct PrefixConfig {
    #[arg(
        long = "prefix-enabled",
        env = "ESTUARY_PREFIX_ENABLED",
        action = ArgAction::Set,
        default_value_t = true
    )]
    pub enabled: bool,
    #[arg(
        long = "prefix-cache-threshold",
        env = "ESTUARY_PREFIX_CACHE_THRESHOLD",
        default_value_t = 0.5
    )]
    pub cache_threshold: f64,
    #[arg(
        long = "prefix-approximate-half-life-ms",
        env = "ESTUARY_PREFIX_APPROXIMATE_HALF_LIFE_MS",
        default_value_t = 300_000
    )]
    pub approximate_half_life_ms: u64,
    #[arg(
        long = "prefix-balance-abs-threshold",
        env = "ESTUARY_PREFIX_BALANCE_ABS_THRESHOLD",
        default_value_t = 2
    )]
    pub balance_abs_threshold: usize,
    #[arg(
        long = "prefix-balance-rel-threshold",
        env = "ESTUARY_PREFIX_BALANCE_REL_THRESHOLD",
        default_value_t = 1.1
    )]
    pub balance_rel_threshold: f64,
    #[arg(
        long = "prefix-max-request-chars",
        env = "ESTUARY_PREFIX_MAX_REQUEST_CHARS",
        default_value_t = 128 * 1024
    )]
    pub max_request_chars: usize,
    #[arg(
        long = "prefix-max-tree-chars-per-node",
        env = "ESTUARY_PREFIX_MAX_TREE_CHARS_PER_NODE",
        default_value_t = 1_000_000
    )]
    pub max_tree_chars_per_node: usize,
    #[arg(
        long = "prefix-max-trees",
        env = "ESTUARY_PREFIX_MAX_TREES",
        default_value_t = 256
    )]
    pub max_trees: usize,
    #[arg(
        long = "prefix-max-directory-chars",
        env = "ESTUARY_PREFIX_MAX_DIRECTORY_CHARS",
        default_value_t = 16_000_000
    )]
    pub max_directory_chars: usize,
}

impl Default for PrefixConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            cache_threshold: 0.5,
            approximate_half_life_ms: 300_000,
            balance_abs_threshold: 2,
            balance_rel_threshold: 1.1,
            max_request_chars: 128 * 1024,
            max_tree_chars_per_node: 1_000_000,
            max_trees: 256,
            max_directory_chars: 16_000_000,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Args)]
#[serde(default, deny_unknown_fields)]
pub struct HealthConfig {
    #[arg(
        long = "health-interval-ms",
        env = "ESTUARY_HEALTH_INTERVAL_MS",
        default_value_t = 5_000
    )]
    pub interval_ms: u64,
    #[arg(
        long = "health-timeout-ms",
        env = "ESTUARY_HEALTH_TIMEOUT_MS",
        default_value_t = 2_000
    )]
    pub timeout_ms: u64,
    #[arg(
        long = "health-unhealthy-threshold",
        env = "ESTUARY_HEALTH_UNHEALTHY_THRESHOLD",
        default_value_t = 3
    )]
    pub unhealthy_threshold: u32,
    #[arg(
        long = "health-healthy-threshold",
        env = "ESTUARY_HEALTH_HEALTHY_THRESHOLD",
        default_value_t = 2
    )]
    pub healthy_threshold: u32,
    #[arg(
        long = "health-passive-failure-threshold",
        env = "ESTUARY_HEALTH_PASSIVE_FAILURE_THRESHOLD",
        default_value_t = 3
    )]
    pub passive_failure_threshold: u32,
    #[arg(
        long = "health-route-while-starting",
        env = "ESTUARY_HEALTH_ROUTE_WHILE_STARTING",
        action = ArgAction::Set,
        default_value_t = false
    )]
    pub route_while_starting: bool,
    #[arg(
        long = "health-jitter-percent",
        env = "ESTUARY_HEALTH_JITTER_PERCENT",
        default_value_t = 20
    )]
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

#[derive(Clone, Debug, Deserialize, Serialize, Args)]
#[serde(default, deny_unknown_fields)]
pub struct CircuitBreakerConfig {
    #[arg(
        long = "circuit-failure-threshold",
        env = "ESTUARY_CIRCUIT_FAILURE_THRESHOLD",
        default_value_t = 5
    )]
    pub failure_threshold: u32,
    #[arg(
        long = "circuit-open-ms",
        env = "ESTUARY_CIRCUIT_OPEN_MS",
        default_value_t = 10_000
    )]
    pub open_ms: u64,
    #[arg(
        long = "circuit-half-open-max-requests",
        env = "ESTUARY_CIRCUIT_HALF_OPEN_MAX_REQUESTS",
        default_value_t = 1
    )]
    pub half_open_max_requests: usize,
    #[arg(
        long = "circuit-half-open-success-threshold",
        env = "ESTUARY_CIRCUIT_HALF_OPEN_SUCCESS_THRESHOLD",
        default_value_t = 2
    )]
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

#[derive(Clone, Debug, Deserialize, Serialize, Args)]
#[serde(default, deny_unknown_fields)]
pub struct RetryConfig {
    #[arg(
        long = "retry-max-attempts",
        env = "ESTUARY_RETRY_MAX_ATTEMPTS",
        default_value_t = 1
    )]
    pub max_attempts: usize,
    #[arg(
        long = "retry-statuses",
        env = "ESTUARY_RETRY_STATUSES",
        value_delimiter = ',',
        default_values_t = [429_u16, 502, 503, 504]
    )]
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

#[cfg(test)]
mod tests;
