use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub const MAX_TOKENIZE_CACHE_ENTRIES: usize = 65_536;
pub const MIN_VLLM_MONITOR_INTERVAL_MS: u64 = 100;
pub const JS_MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
/// Matches `tokio::sync::Semaphore::MAX_PERMITS` (verified at compile time in `estuary`).
pub const MAX_SEMAPHORE_PERMITS: usize = usize::MAX >> 3;

pub const RESERVED_UPSTREAM_HEADERS: &[&str] = &[
    "connection",
    "content-length",
    "host",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "x-gateway-request-id",
];

#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
#[derive(Clone, Copy, Debug, Serialize)]
pub struct IntegerBounds {
    pub min: u64,
    pub max: u64,
}

#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
#[derive(Clone, Debug, Serialize)]
pub struct NodeIntegerLimits {
    pub max_concurrency: IntegerBounds,
    pub monitor_interval_ms: IntegerBounds,
    pub request_timeout_ms: IntegerBounds,
    pub telemetry_stale_ms: IntegerBounds,
    pub waiting_threshold: IntegerBounds,
    pub tokenize_cache_entries: IntegerBounds,
    pub kv_reconnect_ms: IntegerBounds,
    pub kv_max_blocks: IntegerBounds,
    pub kv_max_directory_bytes: IntegerBounds,
    pub kv_max_event_bytes: IntegerBounds,
}

pub const fn js_integer_limits() -> NodeIntegerLimits {
    const fn js_bounds(min: u64, max: u64) -> IntegerBounds {
        IntegerBounds {
            min,
            max: if max < JS_MAX_SAFE_INTEGER {
                max
            } else {
                JS_MAX_SAFE_INTEGER
            },
        }
    }
    NodeIntegerLimits {
        max_concurrency: js_bounds(1, MAX_SEMAPHORE_PERMITS as u64),
        monitor_interval_ms: js_bounds(MIN_VLLM_MONITOR_INTERVAL_MS, u64::MAX),
        request_timeout_ms: js_bounds(1, u64::MAX),
        telemetry_stale_ms: js_bounds(1, u64::MAX),
        waiting_threshold: js_bounds(1, usize::MAX as u64),
        tokenize_cache_entries: js_bounds(1, MAX_TOKENIZE_CACHE_ENTRIES as u64),
        kv_reconnect_ms: js_bounds(1, u64::MAX),
        kv_max_blocks: js_bounds(1, usize::MAX as u64),
        kv_max_directory_bytes: js_bounds(1, usize::MAX as u64),
        kv_max_event_bytes: js_bounds(1, usize::MAX as u64),
    }
}

/// Editor presets deliberately differ from the minimal deserialization defaults.
/// Define that policy here so the browser never copies backend default literals.
pub fn editor_node_defaults() -> NodeConfig {
    NodeConfig {
        base_url: "http://127.0.0.1:8000/v1".to_owned(),
        max_concurrency: 16,
        provider: ProviderConfig {
            kind: ProviderKind::Vllm,
            ..ProviderConfig::default()
        },
        ..NodeConfig::default()
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
#[derive(Clone, Debug, Serialize)]
pub struct ModelCapabilityConfig {
    pub multimodal: bool,
}

impl Default for ModelCapabilityConfig {
    fn default() -> Self {
        Self { multimodal: true }
    }
}

impl<'de> Deserialize<'de> for ModelCapabilityConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Existing SQLite records and imported configs may contain the retired
        // family selector. Accept it only as input; it cannot affect routing.
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Input {
            #[serde(default = "default_multimodal")]
            multimodal: bool,
            #[serde(default, rename = "family")]
            _retired_family: Option<serde::de::IgnoredAny>,
        }
        fn default_multimodal() -> bool {
            ModelCapabilityConfig::default().multimodal
        }
        let input = Input::deserialize(deserializer)?;
        Ok(Self {
            multimodal: input.multimodal,
        })
    }
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
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProviderConfig {
    #[serde(rename = "type")]
    pub kind: ProviderKind,
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
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
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
