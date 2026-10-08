use std::{collections::HashMap, path::PathBuf, time::Duration};

use serde::{Deserialize, Serialize};

pub mod contract;
mod validation;

pub use validation::validate_node_config;

pub const MAX_TOKENIZE_CACHE_ENTRIES: usize = 65_536;

#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
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

#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
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

#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RoutingConfig {
    pub queue_max_requests: usize,
    pub queue_max_bytes: usize,
    pub load_weight: f64,
    pub latency_weight: f64,
    pub error_weight: f64,
    #[serde(skip_serializing_if = "is_default_prefill_weight")]
    pub prefill_weight: f64,
    #[serde(skip_serializing_if = "is_default_decode_weight")]
    pub decode_weight: f64,
    #[serde(skip_serializing_if = "is_default_prefill_scale")]
    pub prefill_token_scale: usize,
    #[serde(skip_serializing_if = "is_default_decode_scale")]
    pub decode_token_scale: usize,
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

#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct PrefixConfig {
    pub enabled: bool,
    pub cache_threshold: f64,
    #[serde(skip_serializing_if = "is_default_approximate_half_life")]
    pub approximate_half_life_ms: u64,
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

// Keep default supervisor settings readable by workers predating workload routing.
// Serde serialization predicates require references to the fields.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_default_prefill_weight(value: &f64) -> bool {
    value.to_bits() == RoutingConfig::default().prefill_weight.to_bits()
}
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_default_decode_weight(value: &f64) -> bool {
    value.to_bits() == RoutingConfig::default().decode_weight.to_bits()
}
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_default_prefill_scale(value: &usize) -> bool {
    *value == RoutingConfig::default().prefill_token_scale
}
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_default_decode_scale(value: &usize) -> bool {
    *value == RoutingConfig::default().decode_token_scale
}
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_default_approximate_half_life(value: &u64) -> bool {
    *value == PrefixConfig::default().approximate_half_life_ms
}

#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
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

#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
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

#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
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

#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct NodeConfig {
    pub id: String,
    pub base_url: String,
    pub api_key: Option<String>,
    pub api_key_env: Option<String>,
    pub models: HashMap<String, String>,
    pub model_capabilities: HashMap<String, ModelCapabilityConfig>,
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
            model_capabilities: HashMap::new(),
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

#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ModelCapabilityConfig {
    pub multimodal: bool,
    pub family: ModelFamily,
}

impl Default for ModelCapabilityConfig {
    fn default() -> Self {
        Self {
            multimodal: true,
            family: ModelFamily::Generic,
        }
    }
}

#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelFamily {
    #[default]
    Generic,
    Deepseek,
}

#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    #[default]
    Openai,
    Vllm,
}

#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
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

#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
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

#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
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

#[cfg(test)]
mod tests;
