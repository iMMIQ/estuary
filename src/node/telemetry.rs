use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

use serde::Serialize;

use crate::observability::{Rate, smooth};

#[derive(Clone, Debug)]
pub(crate) struct ProviderCounter {
    pub key: String,
    pub kind: usize,
    pub value: f64,
}

#[derive(Clone, Debug, Default, Serialize)]
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
impl EngineTelemetry {
    pub(crate) fn aggregate(values: &[Self], expected_nodes: usize) -> Self {
        let sum = |read: fn(&Self) -> Option<f64>| {
            let mut valid = values.iter().filter_map(read).peekable();
            valid.peek()?;
            Some(valid.sum::<f64>())
        };
        let rate = |read: fn(&Self) -> Rate| Rate {
            raw: {
                let values = values
                    .iter()
                    .filter_map(|v| read(v).raw)
                    .collect::<Vec<_>>();
                (!values.is_empty()).then(|| values.iter().sum())
            },
            smoothed: {
                let values = values
                    .iter()
                    .filter_map(|v| read(v).smoothed)
                    .collect::<Vec<_>>();
                (!values.is_empty()).then(|| values.iter().sum())
            },
            mean_60s: {
                let values = values
                    .iter()
                    .filter_map(|v| read(v).mean_60s)
                    .collect::<Vec<_>>();
                (!values.is_empty()).then(|| values.iter().sum())
            },
        };
        // Pair numerator and denominator from the same reporting engines.
        let paired = values
            .iter()
            .filter_map(|v| Some((v.cache_hits_60s?, v.cache_queries_60s?)))
            .collect::<Vec<_>>();
        let hits = paired.iter().map(|v| v.0).sum::<f64>();
        let queries = paired.iter().map(|v| v.1).sum::<f64>();
        Self {
            input: rate(|v| v.input),
            output: rate(|v| v.output),
            requests: rate(|v| v.requests),
            cache_hit_ratio: super::prefix_cache_hit_rate(Some(hits), Some(queries)),
            cache_hits_60s: (!paired.is_empty()).then_some(hits),
            cache_queries_60s: (!paired.is_empty()).then_some(queries),
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

#[derive(Clone, Debug)]
struct Delta {
    at: Instant,
    elapsed: f64,
    values: [Option<f64>; 6],
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
        let rate = |index: usize, previous: Rate| {
            let raw = values[index].zip(elapsed).and_then(|(delta, elapsed)| {
                (elapsed > 0.0 && elapsed <= 60.0).then_some(delta / elapsed)
            });
            Rate {
                raw,
                smoothed: smooth(previous.smoothed, raw, elapsed.unwrap_or(0.0)),
                mean_60s: self.mean(index, now),
            }
        };
        let paired = self
            .history
            .iter()
            .filter_map(|delta| Some((delta.values[3]?, delta.values[4]?, delta)))
            .fold((0.0, 0.0), |(queries, hits), (q, h, delta)| {
                let included = delta
                    .elapsed
                    .min((60.0 - now.duration_since(delta.at).as_secs_f64()).max(0.0))
                    / delta.elapsed;
                (queries + q * included, hits + h * included)
            });
        self.snapshot = EngineTelemetry {
            input: rate(0, self.snapshot.input),
            output: rate(1, self.snapshot.output),
            requests: rate(2, self.snapshot.requests),
            cache_hit_ratio: (values[3].is_some() && values[4].is_some() && paired.0 > 0.0)
                .then(|| (paired.1 / paired.0).clamp(0.0, 1.0)),
            cache_queries_60s: values[3].and(values[4]).map(|_| paired.0),
            cache_hits_60s: values[3].and(values[4]).map(|_| paired.1),
            preemptions_per_minute: values[5]
                .and_then(|_| self.mean(5, now).map(|mean| mean * 60.0)),
            reporting_nodes: 1,
            expected_nodes: 1,
            input_nodes: usize::from(values[0].is_some()),
            output_nodes: usize::from(values[1].is_some()),
            request_nodes: usize::from(values[2].is_some()),
            resets: self.snapshot.resets,
        };
    }
    fn deltas(&mut self, counters: Vec<ProviderCounter>, elapsed: Option<f64>) -> [Option<f64>; 6] {
        let mut values = [None; 6];
        let mut invalid = [false; 6];
        let mut current = HashMap::new();
        let mut resets = self.snapshot.resets;
        for counter in counters.into_iter().take(512) {
            if counter.kind >= 6 || !counter.value.is_finite() || counter.value < 0.0 {
                continue;
            }
            let previous = self.previous.get(&counter.key).copied();
            if previous.is_some_and(|previous| counter.value < previous) {
                resets += 1;
            }
            match super::counter_rate(Some(counter.value), previous, elapsed) {
                Some(rate) => {
                    values[counter.kind] =
                        Some(values[counter.kind].unwrap_or(0.0) + rate * elapsed.unwrap_or(0.0));
                }
                None => invalid[counter.kind] = true,
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
                let included = delta
                    .elapsed
                    .min((60.0 - now.duration_since(delta.at).as_secs_f64()).max(0.0));
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
                kind: 1,
                value: a,
            },
            ProviderCounter {
                key: "b".into(),
                kind: 1,
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
