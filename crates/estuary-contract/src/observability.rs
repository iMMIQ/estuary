use std::collections::BTreeMap;

use serde::Serialize;

use crate::node::EngineTelemetry;

macro_rules! counters {
    ($($field:ident),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, Default, Serialize)]
        #[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
        pub struct Counts { $(pub $field: u64,)+ }
        impl Counts {
            pub fn add(&mut self, other: Self) {
                $(self.$field = self.$field.saturating_add(other.$field);)+
            }
            #[must_use]
            pub fn delta(self, previous: Self) -> Self {
                Self { $($field: self.$field.saturating_sub(previous.$field),)+ }
            }
        }
    };
}

counters!(
    started,
    success,
    failed,
    cancelled,
    unknown,
    retries,
    retried_requests,
    generation_requests,
    input_tokens,
    output_tokens,
    cached_tokens,
    cache_input_tokens,
    input_samples,
    output_samples,
    cache_samples,
    tpot_reported,
    tpot_estimated,
    route_exact,
    route_approximate,
    route_none,
);

impl Counts {
    pub const fn completed(self) -> u64 {
        self.success + self.failed + self.cancelled + self.unknown
    }
}

#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
pub struct Distribution {
    pub samples: u64,
    pub p50: Option<f64>,
    pub p95: Option<f64>,
    pub mean: Option<f64>,
    pub overflow: u64,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
pub struct Rate {
    pub raw: Option<f64>,
    pub smoothed: Option<f64>,
    pub mean_60s: Option<f64>,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
pub struct Rates {
    pub incoming: Rate,
    pub completed: Rate,
    pub input: Rate,
    pub output: Rate,
}

#[derive(Clone, Debug, Default, Serialize)]
#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
pub struct Pressure {
    pub active: usize,
    pub upstream_active: usize,
    pub capacity: usize,
    pub queued: usize,
    pub admission_waiters: usize,
    pub buffer_waiters: usize,
    pub buffer_bytes: usize,
    pub buffer_limit: usize,
    pub running: Option<usize>,
    pub waiting: Option<usize>,
    pub kv_usage: Option<f64>,
    pub fresh_nodes: usize,
    pub provider_nodes: usize,
    pub telemetry_age_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
pub struct Point {
    pub peaks: Peaks,
    pub at_unix_ms: u64,
    pub rates: Rates,
    pub ttft_p95: Option<f64>,
    pub queue_p95: Option<f64>,
    pub pressure: Pressure,
    pub engine: EngineTelemetry,
}

#[derive(Clone, Debug, Default, Serialize)]
#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
pub struct Peaks {
    pub output: Option<f64>,
    pub active: usize,
    pub queued: usize,
    pub waiting: Option<usize>,
    pub kv_usage: Option<f64>,
}

impl Peaks {
    pub fn include(&mut self, rates: Rates, pressure: &Pressure) {
        self.merge(&Self {
            output: rates.output.raw,
            active: pressure.active,
            queued: pressure.queued,
            waiting: pressure.waiting,
            kv_usage: pressure.kv_usage,
        });
    }

    pub fn merge(&mut self, other: &Self) {
        if let Some(value) = other.output {
            self.output = Some(self.output.unwrap_or(value).max(value));
        }
        self.active = self.active.max(other.active);
        self.queued = self.queued.max(other.queued);
        if let Some(value) = other.waiting {
            self.waiting = Some(self.waiting.unwrap_or(value).max(value));
        }
        if let Some(value) = other.kv_usage {
            self.kv_usage = Some(self.kv_usage.unwrap_or(value).max(value));
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
pub struct ScopeSnapshot {
    pub peaks: Peaks,
    #[cfg_attr(
        feature = "config-contract",
        ts(type = "\"fleet\" | \"node\" | \"model\"")
    )]
    pub kind: String,
    pub id: String,
    pub instance: Option<u64>,
    pub retired: bool,
    pub total: Counts,
    pub window: Counts,
    pub observed_seconds: f64,
    pub rates: Rates,
    #[cfg_attr(
        feature = "config-contract",
        ts(
            type = "Record<\"ttft\" | \"upstream_ttft\" | \"duration\" | \"ingress\" | \"queue\" | \"tpot\" | \"generation_rate\", Distribution>"
        )
    )]
    pub latency: BTreeMap<&'static str, Distribution>,
    pub success_ratio: Option<f64>,
    pub failure_ratio: Option<f64>,
    pub cancellation_ratio: Option<f64>,
    pub unknown_ratio: Option<f64>,
    pub usage_coverage: Option<f64>,
    pub cache_hit_ratio: Option<f64>,
    pub pressure: Pressure,
    pub engine: EngineTelemetry,
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
pub struct Snapshot {
    pub instance_id: String,
    pub generated_at_unix_ms: u64,
    pub retention_seconds: u64,
    pub step_seconds: u64,
    pub window_seconds: u64,
    pub scope_limit_per_kind: usize,
    pub limited_events: u64,
    pub fleet: ScopeSnapshot,
    pub nodes: Vec<ScopeSnapshot>,
    pub models: Vec<ScopeSnapshot>,
    pub selected: Option<ScopeSnapshot>,
    pub history: Vec<Point>,
}
