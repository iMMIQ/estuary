//! Bounded in-memory operational statistics, independent of optional session logging.
mod prometheus;
mod request;
mod window;

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use serde::Serialize;

use crate::{
    node::{EngineTelemetry, NodeSnapshot},
    time::unix_millis,
};
pub use request::Observation;
pub(crate) use request::{Delivery, Outcome};
pub(crate) use window::smooth;
pub use window::{Counts, Distribution, Rate, Rates};
use window::{Histogram, LATENCIES, RETENTION, STEP, Window};

const MAX_SCOPES_PER_KIND: usize = 32;

#[derive(Clone, Debug, Default, Serialize)]
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
pub struct Peaks {
    pub output: Option<f64>,
    pub active: usize,
    pub queued: usize,
    pub waiting: Option<usize>,
    pub kv_usage: Option<f64>,
}
impl Peaks {
    fn include(&mut self, rates: Rates, pressure: &Pressure) {
        if let Some(value) = rates.output.raw {
            self.output = Some(self.output.unwrap_or(value).max(value));
        }
        self.active = self.active.max(pressure.active);
        self.queued = self.queued.max(pressure.queued);
        if let Some(value) = pressure.waiting {
            self.waiting = Some(self.waiting.unwrap_or(value).max(value));
        }
        if let Some(value) = pressure.kv_usage {
            self.kv_usage = Some(self.kv_usage.unwrap_or(value).max(value));
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ScopeSnapshot {
    pub peaks: Peaks,
    pub kind: String,
    pub id: String,
    pub instance: Option<u64>,
    pub retired: bool,
    pub total: Counts,
    pub window: Counts,
    pub observed_seconds: f64,
    pub rates: Rates,
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

#[derive(Debug)]
struct Series {
    peak_active: usize,
    created_at: f64,
    instance: Option<u64>,
    retired: bool,
    total: Counts,
    lifetime: [Histogram; 7],
    window: Window,
    rates: Rates,
    sampled_total: Counts,
    sampled_at: f64,
    pressure: Pressure,
    engine: EngineTelemetry,
    history: VecDeque<Point>,
}
impl Default for Series {
    fn default() -> Self {
        Self {
            peak_active: 0,
            created_at: 0.0,
            instance: None,
            retired: false,
            total: Counts::default(),
            lifetime: std::array::from_fn(|_| Histogram::default()),
            window: Window::default(),
            rates: Rates::default(),
            sampled_total: Counts::default(),
            sampled_at: 0.0,
            pressure: Pressure::default(),
            engine: EngineTelemetry::default(),
            history: VecDeque::new(),
        }
    }
}
impl Series {
    fn new(created_at: f64) -> Self {
        Self {
            created_at,
            sampled_at: created_at,
            ..Self::default()
        }
    }
    fn record(&mut self, now: u64, counts: Counts, values: [Option<f64>; 7]) {
        self.total.add(counts);
        self.peak_active = self.peak_active.max(
            self.total
                .started
                .saturating_sub(self.total.completed())
                .try_into()
                .unwrap_or(usize::MAX),
        );
        self.window.record(now, counts, values);
        for (histogram, value) in self.lifetime.iter_mut().zip(values) {
            if let Some(value) = value {
                histogram.observe(value);
            }
        }
    }
    fn sample(&mut self, now: f64, at_unix_ms: u64) {
        let elapsed = now - self.sampled_at;
        if elapsed <= 0.0 {
            return;
        }
        self.window.expire(Duration::from_secs_f64(now).as_secs());
        let delta = self.total.delta(self.sampled_total);
        let (recent, _, covered) = self.window.read(now, 60, self.created_at);
        for (rate, count, minute, supported) in [
            (
                &mut self.rates.incoming,
                delta.started,
                recent.started,
                true,
            ),
            (
                &mut self.rates.completed,
                delta.success,
                recent.success,
                true,
            ),
            (
                &mut self.rates.input,
                delta.input_tokens,
                recent.input_tokens,
                recent.input_samples > 0 || recent.completed() == 0,
            ),
            (
                &mut self.rates.output,
                delta.output_tokens,
                recent.output_tokens,
                recent.output_samples > 0 || recent.completed() == 0,
            ),
        ] {
            rate.raw = supported.then_some(count as f64 / elapsed);
            // Gateway counters start at zero when the scope is created. Use that
            // known baseline so a model's first burst doesn't initialize its EWMA
            // to a much higher rate than the already-sampled fleet.
            rate.smoothed = window::smooth(rate.smoothed.or(Some(0.0)), rate.raw, elapsed);
            rate.mean_60s = (supported && covered > 0.0).then_some(minute as f64 / covered);
        }
        if self
            .history
            .back()
            .is_some_and(|point| point.at_unix_ms > at_unix_ms)
        {
            self.history.clear();
        }
        self.pressure.active = self
            .total
            .started
            .saturating_sub(self.total.completed())
            .try_into()
            .unwrap_or(usize::MAX);
        self.sampled_at = now;
        self.sampled_total = self.total;
        if self
            .history
            .back()
            .is_none_or(|point| point.at_unix_ms / (STEP * 1000) != at_unix_ms / (STEP * 1000))
        {
            let (_, histograms, _) = self.window.read(now, 300, self.created_at);
            self.history.push_back(Point {
                peaks: Peaks::default(),
                at_unix_ms,
                rates: self.rates,
                ttft_p95: histograms[0].snapshot().p95,
                queue_p95: histograms[4].snapshot().p95,
                pressure: self.pressure.clone(),
                engine: self.engine.clone(),
            });
        }
        if let Some(point) = self.history.back_mut() {
            point.peaks.include(self.rates, &self.pressure);
            point.peaks.active = point.peaks.active.max(self.peak_active);
        }
        self.peak_active = self
            .total
            .started
            .saturating_sub(self.total.completed())
            .try_into()
            .unwrap_or(usize::MAX);
        while self.history.len() > usize::try_from(RETENTION / STEP).unwrap_or(180)
            || self
                .history
                .front()
                .is_some_and(|point| point.at_unix_ms.saturating_add(RETENTION * 1000) < at_unix_ms)
        {
            self.history.pop_front();
        }
    }
    fn snapshot(&self, kind: &str, id: &str, now: f64, seconds: u64) -> ScopeSnapshot {
        let (counts, histograms, covered) = self.window.read(now, seconds, self.created_at);
        let completed = counts.completed();
        let mut pressure = self.pressure.clone();
        pressure.active = self
            .total
            .started
            .saturating_sub(self.total.completed())
            .try_into()
            .unwrap_or(usize::MAX);
        let mut peaks = Peaks::default();
        let at_unix_ms = unix_millis();
        for point in self
            .history
            .iter()
            .rev()
            .take_while(|point| point.at_unix_ms.saturating_add(seconds * 1000) >= at_unix_ms)
        {
            if let Some(value) = point.peaks.output {
                peaks.output = Some(peaks.output.unwrap_or(value).max(value));
            }
            peaks.active = peaks.active.max(point.peaks.active);
            peaks.queued = peaks.queued.max(point.peaks.queued);
            if let Some(value) = point.peaks.waiting {
                peaks.waiting = Some(peaks.waiting.unwrap_or(value).max(value));
            }
            if let Some(value) = point.peaks.kv_usage {
                peaks.kv_usage = Some(peaks.kv_usage.unwrap_or(value).max(value));
            }
        }
        ScopeSnapshot {
            peaks,
            kind: kind.to_owned(),
            id: id.to_owned(),
            instance: self.instance,
            retired: self.retired,
            total: self.total,
            window: counts,
            observed_seconds: covered,
            rates: self.rates,
            latency: LATENCIES
                .into_iter()
                .zip(histograms.iter().map(Histogram::snapshot))
                .collect(),
            success_ratio: ratio(counts.success, completed),
            failure_ratio: ratio(counts.failed, completed),
            cancellation_ratio: ratio(counts.cancelled, completed),
            unknown_ratio: ratio(counts.unknown, completed),
            usage_coverage: ratio(counts.output_samples, counts.generation_requests),
            cache_hit_ratio: ratio(counts.cached_tokens, counts.cache_input_tokens),
            pressure,
            engine: self.engine.clone(),
        }
    }
}
fn ratio(numerator: u64, denominator: u64) -> Option<f64> {
    (denominator > 0).then(|| (numerator as f64 / denominator as f64).clamp(0.0, 1.0))
}

#[derive(Debug, Default)]
struct State {
    fleet: Series,
    nodes: HashMap<String, Series>,
    models: HashMap<String, Series>,
    limited_events: u64,
}

#[derive(Debug)]
pub struct Observability {
    started: Instant,
    instance_id: String,
    state: Mutex<State>,
}
impl Observability {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            started: Instant::now(),
            instance_id: uuid::Uuid::now_v7().to_string(),
            state: Mutex::new(State::default()),
        })
    }
    pub(crate) fn begin(self: &Arc<Self>) -> Arc<Observation> {
        Observation::new(Arc::clone(self))
    }
    pub(crate) fn configure_models(&self, models: Vec<String>) {
        let mut state = self.state.lock();
        for id in models.into_iter().filter(|id| id.len() <= 1024) {
            if state.models.len() >= MAX_SCOPES_PER_KIND && !state.models.contains_key(&id) {
                break;
            }
            state
                .models
                .entry(id)
                .or_insert_with(|| Series::new(self.started.elapsed().as_secs_f64()));
        }
    }
    fn record(
        &self,
        model: Option<&str>,
        node: Option<(&str, u64)>,
        fleet: bool,
        counts: Counts,
        values: [Option<f64>; 7],
    ) {
        let mut state = self.state.lock();
        let elapsed = self.started.elapsed();
        let now = elapsed.as_secs();
        let created_at = elapsed.as_secs_f64();
        if fleet {
            state.fleet.record(now, counts, values);
        }
        if let Some(model) = model {
            if model.len() <= 1024
                && (state.models.contains_key(model) || state.models.len() < MAX_SCOPES_PER_KIND)
            {
                state
                    .models
                    .entry(model.to_owned())
                    .or_insert_with(|| Series::new(created_at))
                    .record(now, counts, values);
            } else {
                state.limited_events += 1;
            }
        }
        if let Some((node, instance)) = node {
            if node.len() <= 1024
                && (state.nodes.contains_key(node) || state.nodes.len() < MAX_SCOPES_PER_KIND)
            {
                let series = state
                    .nodes
                    .entry(node.to_owned())
                    .or_insert_with(|| Series::new(created_at));
                if series.instance.is_some_and(|previous| previous > instance) {
                    return;
                }
                if series.instance.is_some_and(|previous| previous < instance) {
                    *series = Series::new(created_at);
                }
                series.instance = Some(instance);
                series.record(now, counts, values);
            } else {
                state.limited_events += 1;
            }
        }
    }
    pub(crate) fn sample(&self, nodes: Vec<(NodeSnapshot, bool)>, mut pressure: Pressure) {
        let mut state = self.state.lock();
        let now = self.started.elapsed().as_secs_f64();
        let at_unix_ms = unix_millis();
        for series in state.nodes.values_mut() {
            series.retired = true;
        }
        let mut engines = Vec::new();
        for (node, fresh) in nodes {
            pressure.upstream_active += node.active;
            pressure.capacity += node.max_concurrency;
            let is_vllm = node.provider == crate::config::ProviderKind::Vllm;
            let age = node
                .provider_telemetry_updated_unix_ms
                .map(|at| at_unix_ms.saturating_sub(at));
            if is_vllm {
                pressure.provider_nodes += 1;
                pressure.fresh_nodes += usize::from(fresh);
                if fresh {
                    if let Some(running) = node.upstream_running {
                        pressure.running = Some(pressure.running.unwrap_or(0) + running);
                    }
                    if let Some(waiting) = node.upstream_waiting {
                        pressure.waiting = Some(pressure.waiting.unwrap_or(0) + waiting);
                    }
                    if let Some(kv) = node.kv_cache_usage {
                        pressure.kv_usage = Some(pressure.kv_usage.unwrap_or(0.0).max(kv));
                    }
                    if let Some(age) = age {
                        pressure.telemetry_age_ms =
                            Some(pressure.telemetry_age_ms.unwrap_or(0).max(age));
                    }
                    engines.push(node.engine.clone());
                }
            }
            if node.id.len() > 1024
                || (!state.nodes.contains_key(&node.id) && state.nodes.len() >= MAX_SCOPES_PER_KIND)
            {
                state.limited_events += 1;
                continue;
            }
            let series = state
                .nodes
                .entry(node.id)
                .or_insert_with(|| Series::new(now));
            if series.instance.is_some_and(|id| id > node.instance_id) {
                series.retired = false;
                continue;
            }
            if series.instance.is_some_and(|id| id < node.instance_id) {
                *series = Series::new(now);
            }
            series.instance = Some(node.instance_id);
            series.retired = false;
            series.pressure = Pressure {
                active: node.active,
                upstream_active: node.active,
                capacity: node.max_concurrency,
                running: fresh.then_some(node.upstream_running).flatten(),
                waiting: fresh.then_some(node.upstream_waiting).flatten(),
                kv_usage: fresh.then_some(node.kv_cache_usage).flatten(),
                fresh_nodes: usize::from(is_vllm && fresh),
                provider_nodes: usize::from(is_vllm),
                telemetry_age_ms: age,
                ..Pressure::default()
            };
            series.engine = if fresh {
                node.engine
            } else {
                EngineTelemetry::aggregate(&[], usize::from(is_vllm))
            };
        }
        state.fleet.engine = EngineTelemetry::aggregate(&engines, pressure.provider_nodes);
        state.fleet.pressure = pressure;
        state.fleet.sample(now, at_unix_ms);
        for series in state.nodes.values_mut() {
            if series.retired {
                series.pressure = Pressure::default();
                series.engine = EngineTelemetry::default();
            }
            series.sample(now, at_unix_ms);
        }
        for series in state.models.values_mut() {
            series.pressure.active = series
                .total
                .started
                .saturating_sub(series.total.completed())
                .try_into()
                .unwrap_or(usize::MAX);
            series.sample(now, at_unix_ms);
        }
    }
    pub fn snapshot(&self, kind: &str, id: &str, seconds: u64) -> Snapshot {
        let state = self.state.lock();
        let now = self.started.elapsed().as_secs_f64();
        let selected = match kind {
            "node" => state.nodes.get(id),
            "model" => state.models.get(id),
            _ => Some(&state.fleet),
        };
        let mut nodes = state
            .nodes
            .iter()
            .map(|(id, series)| series.snapshot("node", id, now, seconds))
            .collect::<Vec<_>>();
        let mut models = state
            .models
            .iter()
            .map(|(id, series)| series.snapshot("model", id, now, seconds))
            .collect::<Vec<_>>();
        nodes.sort_unstable_by(|a, b| a.id.cmp(&b.id));
        models.sort_unstable_by(|a, b| a.id.cmp(&b.id));
        Snapshot {
            instance_id: self.instance_id.clone(),
            generated_at_unix_ms: unix_millis(),
            retention_seconds: RETENTION,
            step_seconds: STEP,
            window_seconds: seconds,
            scope_limit_per_kind: MAX_SCOPES_PER_KIND,
            limited_events: state.limited_events,
            fleet: state.fleet.snapshot("fleet", "", now, seconds),
            nodes,
            models,
            selected: selected.map(|series| series.snapshot(kind, id, now, seconds)),
            history: selected
                .map(|series| series.history.iter().cloned().collect())
                .unwrap_or_default(),
        }
    }
    pub(crate) fn encode_metrics(&self, body: String) -> String {
        prometheus::encode(self, body)
    }
}

#[cfg(test)]
mod tests;
