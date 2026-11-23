use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

pub use estuary_contract::node::EngineTelemetry;

use crate::observability::{Rate, smooth};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub(crate) enum CounterKind {
    PromptTokens = 0,
    GenerationTokens = 1,
    Requests = 2,
    PrefixQueries = 3,
    PrefixHits = 4,
    Preemptions = 5,
}

#[derive(Clone, Debug)]
pub(crate) struct ProviderCounter {
    pub key: String,
    pub kind: CounterKind,
    pub value: f64,
}

pub(crate) fn sum_counter_kind(counters: &[ProviderCounter], kind: CounterKind) -> Option<f64> {
    let mut iter = counters
        .iter()
        .filter(|counter| counter.kind == kind && counter.value.is_finite() && counter.value >= 0.0)
        .map(|counter| counter.value)
        .peekable();
    iter.peek()?;
    Some(iter.sum())
}

#[derive(Clone, Debug)]
struct Delta {
    at: Instant,
    elapsed: f64,
    values: [Option<f64>; 6],
}

impl Delta {
    fn included_seconds(&self, now: Instant) -> f64 {
        self.elapsed
            .min((60.0 - now.duration_since(self.at).as_secs_f64()).max(0.0))
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct Tracker {
    previous: HashMap<String, f64>,
    at: Option<Instant>,
    history: VecDeque<Delta>,
    pub snapshot: EngineTelemetry,
}

impl Tracker {
    pub fn update(&mut self, counters: Vec<ProviderCounter>, now: Instant, max_gap: Duration) {
        if counters.len() > 512 {
            self.previous.clear();
            self.history.clear();
            self.at = Some(now);
            self.snapshot = EngineTelemetry {
                expected_nodes: 1,
                reporting_nodes: 1,
                resets: self.snapshot.resets,
                ..EngineTelemetry::default()
            };
            return;
        }
        let elapsed = self
            .at
            .map(|at| now.saturating_duration_since(at).as_secs_f64());
        if elapsed.is_some_and(|elapsed| elapsed > max_gap.as_secs_f64()) {
            self.previous.clear();
            self.history.clear();
        }
        let values = self.deltas(counters, elapsed);
        self.at = Some(now);
        if let Some(elapsed) = elapsed.filter(|elapsed| *elapsed > 0.0 && *elapsed <= 60.0) {
            self.history.push_back(Delta {
                at: now,
                elapsed,
                values,
            });
        } else {
            self.history.clear();
        }
        while self
            .history
            .front()
            .is_some_and(|delta| now.duration_since(delta.at).as_secs_f64() >= 60.0)
            || self.history.len() > 600
        {
            self.history.pop_front();
        }
        let rate = |kind: CounterKind, previous: Rate| {
            let index = kind as usize;
            let raw = values[index].zip(elapsed).and_then(|(delta, elapsed)| {
                (elapsed > 0.0 && elapsed <= 60.0).then_some(delta / elapsed)
            });
            Rate {
                raw,
                smoothed: smooth(previous.smoothed, raw, elapsed.unwrap_or(0.0)),
                mean_60s: self.mean(index, now),
            }
        };
        let (queries_60s, hits_60s) = self
            .history
            .iter()
            .filter_map(|delta| {
                Some((
                    delta.values[CounterKind::PrefixQueries as usize]?,
                    delta.values[CounterKind::PrefixHits as usize]?,
                    delta,
                ))
            })
            .fold((0.0, 0.0), |(queries, hits), (q, h, delta)| {
                let frac = delta.included_seconds(now) / delta.elapsed;
                (queries + q * frac, hits + h * frac)
            });
        let has_cache = values[CounterKind::PrefixQueries as usize].is_some()
            && values[CounterKind::PrefixHits as usize].is_some();
        self.snapshot = EngineTelemetry {
            input: rate(CounterKind::PromptTokens, self.snapshot.input),
            output: rate(CounterKind::GenerationTokens, self.snapshot.output),
            requests: rate(CounterKind::Requests, self.snapshot.requests),
            cache_hit_ratio: (has_cache && queries_60s > 0.0)
                .then(|| (hits_60s / queries_60s).clamp(0.0, 1.0)),
            cache_queries_60s: has_cache.then_some(queries_60s),
            cache_hits_60s: has_cache.then_some(hits_60s),
            preemptions_per_minute: values[CounterKind::Preemptions as usize].and_then(|_| {
                self.mean(CounterKind::Preemptions as usize, now)
                    .map(|mean| mean * 60.0)
            }),
            reporting_nodes: 1,
            expected_nodes: 1,
            input_nodes: usize::from(values[CounterKind::PromptTokens as usize].is_some()),
            output_nodes: usize::from(values[CounterKind::GenerationTokens as usize].is_some()),
            request_nodes: usize::from(values[CounterKind::Requests as usize].is_some()),
            resets: self.snapshot.resets,
        };
    }

    fn deltas(&mut self, counters: Vec<ProviderCounter>, elapsed: Option<f64>) -> [Option<f64>; 6] {
        let mut values = [None; 6];
        let mut invalid = [false; 6];
        let mut current = HashMap::new();
        let mut resets = self.snapshot.resets;
        let valid_elapsed = elapsed.is_some_and(|e| e.is_finite() && e > 0.0);
        for counter in counters {
            if !counter.value.is_finite() || counter.value < 0.0 {
                continue;
            }
            let kind = counter.kind as usize;
            let previous = self.previous.get(&counter.key).copied();
            if previous.is_some_and(|previous| counter.value < previous) {
                resets += 1;
            }
            match previous {
                Some(prev) if valid_elapsed && prev.is_finite() && counter.value >= prev => {
                    values[kind] = Some(values[kind].unwrap_or(0.0) + (counter.value - prev));
                }
                _ => invalid[kind] = true,
            }
            current.insert(counter.key, counter.value);
        }
        for (value, invalid) in values.iter_mut().zip(invalid) {
            if invalid {
                *value = None;
            }
        }
        if invalid.iter().any(|invalid| *invalid) {
            self.history.clear();
        }
        self.previous = current;
        self.snapshot.resets = resets;
        values
    }

    fn mean(&self, index: usize, now: Instant) -> Option<f64> {
        let mut tokens = 0.0;
        let mut covered = 0.0;
        for delta in &self.history {
            if let Some(value) = delta.values[index] {
                let included = delta.included_seconds(now);
                tokens += value * included / delta.elapsed;
                covered += included;
            }
        }
        (covered > 0.0).then_some(tokens / covered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn counters(a: f64, b: f64) -> Vec<ProviderCounter> {
        vec![
            ProviderCounter {
                key: "a".into(),
                kind: CounterKind::GenerationTokens,
                value: a,
            },
            ProviderCounter {
                key: "b".into(),
                kind: CounterKind::GenerationTokens,
                value: b,
            },
        ]
    }

    #[test]
    fn each_engine_counter_resets_before_aggregation() {
        let mut tracker = Tracker::default();
        let now = Instant::now();
        tracker.update(counters(100.0, 100.0), now, Duration::from_secs(5));
        assert!(tracker.snapshot.output.raw.is_none());
        tracker.update(
            counters(1.0, 300.0),
            now + Duration::from_secs(1),
            Duration::from_secs(5),
        );
        assert!(tracker.snapshot.output.raw.is_none());
        assert_eq!(tracker.snapshot.resets, 1);
        tracker.update(
            counters(11.0, 310.0),
            now + Duration::from_secs(2),
            Duration::from_secs(5),
        );
        assert_eq!(tracker.snapshot.output.raw, Some(20.0));
        tracker.update(
            counters(11.0, 310.0),
            now + Duration::from_secs(3),
            Duration::from_secs(5),
        );
        assert_eq!(tracker.snapshot.output.raw, Some(0.0));
        assert!(tracker.snapshot.output.smoothed.unwrap() < 20.0);
        tracker.update(
            counters(50.0, 350.0),
            now + Duration::from_secs(100),
            Duration::from_secs(5),
        );
        assert!(tracker.snapshot.output.raw.is_none());
    }
}
