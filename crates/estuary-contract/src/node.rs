use serde::Serialize;

use crate::{config::ProviderKind, observability::Rate};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum HealthState {
    Starting,
    Healthy,
    Degraded,
    Unhealthy,
}

impl HealthState {
    pub const fn encode(self) -> u8 {
        match self {
            Self::Starting => 0,
            Self::Healthy => 1,
            Self::Degraded => 2,
            Self::Unhealthy => 3,
        }
    }

    pub const fn decode(value: u8) -> Self {
        match value {
            1 => Self::Healthy,
            2 => Self::Degraded,
            3 => Self::Unhealthy,
            _ => Self::Starting,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum ProviderState {
    Generic,
    Checking,
    Ready,
    Incompatible,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum LifecycleState {
    Serving,
    Draining,
}

impl LifecycleState {
    pub const fn encode(self) -> u8 {
        match self {
            Self::Serving => 0,
            Self::Draining => 1,
        }
    }

    pub const fn decode(value: u8) -> Self {
        if value == 1 {
            Self::Draining
        } else {
            Self::Serving
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

#[derive(Clone, Debug, Default, Serialize)]
#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
pub struct EngineTelemetry {
    pub input: Rate,
    pub output: Rate,
    pub requests: Rate,
    pub cache_hit_ratio: Option<f64>,
    pub cache_hits_60s: Option<f64>,
    pub cache_queries_60s: Option<f64>,
    pub preemptions_per_minute: Option<f64>,
    pub reporting_nodes: usize,
    pub expected_nodes: usize,
    pub input_nodes: usize,
    pub output_nodes: usize,
    pub request_nodes: usize,
    pub resets: u64,
}

pub fn prefix_cache_hit_rate(hits: Option<f64>, queries: Option<f64>) -> Option<f64> {
    let (hits, queries) = (hits?, queries?);
    (queries > 0.0 && hits.is_finite() && queries.is_finite())
        .then(|| (hits / queries).clamp(0.0, 1.0))
}

impl EngineTelemetry {
    pub fn aggregate(values: &[Self], expected_nodes: usize) -> Self {
        let sum = |read: fn(&Self) -> Option<f64>| {
            let mut valid = values.iter().filter_map(read).peekable();
            valid.peek()?;
            Some(valid.sum::<f64>())
        };
        let rate = |raw: fn(&Self) -> Option<f64>,
                    smoothed: fn(&Self) -> Option<f64>,
                    mean_60s: fn(&Self) -> Option<f64>| Rate {
            raw: sum(raw),
            smoothed: sum(smoothed),
            mean_60s: sum(mean_60s),
        };
        // Pair numerator and denominator from the same reporting engines.
        let (has_cache, hits, queries) = values
            .iter()
            .filter_map(|v| Some((v.cache_hits_60s?, v.cache_queries_60s?)))
            .fold((false, 0.0, 0.0), |(_, hits, queries), (h, q)| {
                (true, hits + h, queries + q)
            });
        Self {
            input: rate(|v| v.input.raw, |v| v.input.smoothed, |v| v.input.mean_60s),
            output: rate(
                |v| v.output.raw,
                |v| v.output.smoothed,
                |v| v.output.mean_60s,
            ),
            requests: rate(
                |v| v.requests.raw,
                |v| v.requests.smoothed,
                |v| v.requests.mean_60s,
            ),
            cache_hit_ratio: prefix_cache_hit_rate(Some(hits), Some(queries)),
            cache_hits_60s: has_cache.then_some(hits),
            cache_queries_60s: has_cache.then_some(queries),
            preemptions_per_minute: sum(|v| v.preemptions_per_minute),
            reporting_nodes: values.len(),
            expected_nodes,
            input_nodes: values.iter().filter(|v| v.input.raw.is_some()).count(),
            output_nodes: values.iter().filter(|v| v.output.raw.is_some()).count(),
            request_nodes: values.iter().filter(|v| v.requests.raw.is_some()).count(),
            resets: values.iter().map(|v| v.resets).sum(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
pub struct NodeSnapshot {
    pub instance_id: u64,
    pub engine: EngineTelemetry,
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
    pub ttft_ewma_ms: Option<f64>,
    pub pending_prefill_tokens: usize,
    pub pending_decode_tokens: usize,
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
    pub prompt_tokens_per_second: Option<f64>,
    pub generation_tokens_per_second: Option<f64>,
    pub requests_per_second: Option<f64>,
    pub prefix_cache_queries_total: Option<f64>,
    pub prefix_cache_hits_total: Option<f64>,
    pub prefix_cache_hit_rate: Option<f64>,
    pub preemptions_total: Option<f64>,
    pub provider_telemetry_updated_unix_ms: Option<u64>,
    pub provider_last_error: Option<String>,
}
