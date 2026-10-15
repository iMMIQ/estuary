use std::collections::VecDeque;

use serde::Serialize;

pub(super) const RETENTION: u64 = 900;
pub(super) const STEP: u64 = 5;
const BUCKET_SECONDS: u64 = 10;
const BINS: usize = 80;
pub(super) const LATENCIES: [&str; 7] = [
    "ttft",
    "upstream_ttft",
    "duration",
    "ingress",
    "queue",
    "tpot",
    "generation_rate",
];

const fn histogram_bounds() -> [f64; BINS] {
    let mut bounds = [0.0; BINS];
    let mut index = 1;
    let mut value = 0.000_5;
    while index < BINS {
        bounds[index] = value;
        value *= 1.25;
        index += 1;
    }
    bounds[BINS - 1] = f64::INFINITY;
    bounds
}
const BOUNDS: [f64; BINS] = histogram_bounds();

macro_rules! counters {
    ($($field:ident),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, Default, Serialize)]
        pub struct Counts { $(pub $field: u64,)+ }
        impl Counts {
            pub(super) fn add(&mut self, other: Self) {
                $(self.$field = self.$field.saturating_add(other.$field);)+
            }
            pub(super) fn delta(self, previous: Self) -> Self {
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
    pub(super) fn completed(self) -> u64 {
        self.success + self.failed + self.cancelled + self.unknown
    }
}

#[derive(Clone, Debug)]
pub(super) struct Histogram {
    pub bins: [u64; BINS],
    pub sum: f64,
    pub count: u64,
}
impl Default for Histogram {
    fn default() -> Self {
        Self {
            bins: [0; BINS],
            sum: 0.0,
            count: 0,
        }
    }
}
pub(super) fn bound(index: usize) -> f64 {
    BOUNDS[index]
}
impl Histogram {
    pub fn observe(&mut self, value: f64) {
        if !value.is_finite() || value < 0.0 {
            return;
        }
        let index = BOUNDS.partition_point(|bound| *bound < value).min(BINS - 1);
        self.bins[index] += 1;
        self.count += 1;
        self.sum += value;
    }
    fn add(&mut self, other: &Self) {
        for (sum, value) in self.bins.iter_mut().zip(other.bins) {
            *sum += value;
        }
        self.sum += other.sum;
        self.count += other.count;
    }
    fn quantile(&self, fraction: f64) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        let rank = (self.count as f64 * fraction).max(1.0);
        let mut seen = 0;
        for (index, count) in self.bins.iter().copied().enumerate() {
            if count > 0 && (seen + count) as f64 >= rank {
                if index == BINS - 1 {
                    return None;
                }
                let lower = bound(index.saturating_sub(1));
                return Some(lower + (bound(index) - lower) * (rank - seen as f64) / count as f64);
            }
            seen += count;
        }
        None
    }
    pub fn snapshot(&self) -> Distribution {
        Distribution {
            samples: self.count,
            p50: self.quantile(0.5),
            p95: self.quantile(0.95),
            mean: (self.count > 0).then(|| self.sum / self.count as f64),
            overflow: self.bins[BINS - 1],
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Distribution {
    pub samples: u64,
    pub p50: Option<f64>,
    pub p95: Option<f64>,
    pub mean: Option<f64>,
    pub overflow: u64,
}
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct Rate {
    pub raw: Option<f64>,
    pub smoothed: Option<f64>,
    pub mean_60s: Option<f64>,
}
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct Rates {
    pub incoming: Rate,
    pub completed: Rate,
    pub input: Rate,
    pub output: Rate,
}
pub(crate) fn smooth(previous: Option<f64>, sample: Option<f64>, elapsed: f64) -> Option<f64> {
    let sample = sample.filter(|value| value.is_finite() && *value >= 0.0)?;
    Some(previous.map_or(sample, |previous| {
        let alpha = 1.0 - 2.0_f64.powf(-elapsed / 10.0);
        alpha.mul_add(sample, (1.0 - alpha) * previous)
    }))
}

#[derive(Debug)]
struct Bucket {
    tick: u64,
    counts: Counts,
    histograms: [Histogram; 7],
}
#[derive(Debug, Default)]
pub(super) struct Window {
    buckets: VecDeque<Bucket>,
}
impl Window {
    pub fn expire(&mut self, now: u64) {
        while self
            .buckets
            .front()
            .is_some_and(|bucket| bucket.tick + RETENTION <= now)
        {
            self.buckets.pop_front();
        }
    }
    pub fn record(&mut self, now: u64, counts: Counts, values: [Option<f64>; 7]) {
        self.expire(now);
        let tick = now / BUCKET_SECONDS * BUCKET_SECONDS;
        if self.buckets.back().is_none_or(|bucket| bucket.tick != tick) {
            self.buckets.push_back(Bucket {
                tick,
                counts: Counts::default(),
                histograms: std::array::from_fn(|_| Histogram::default()),
            });
        }
        let bucket = self.buckets.back_mut().expect("current bucket exists");
        bucket.counts.add(counts);
        for (histogram, value) in bucket.histograms.iter_mut().zip(values) {
            if let Some(value) = value {
                histogram.observe(value);
            }
        }
    }
    pub fn read(&self, now: f64, seconds: u64, created_at: f64) -> (Counts, [Histogram; 7], f64) {
        // Complete ten-second boundaries avoid counting observations outside the window.
        let lower = ((now - seconds as f64).max(0.0) / BUCKET_SECONDS as f64).ceil()
            * BUCKET_SECONDS as f64;
        let lower = lower.max(created_at);
        let mut counts = Counts::default();
        let mut histograms: [Histogram; 7] = std::array::from_fn(|_| Histogram::default());
        for bucket in self
            .buckets
            .iter()
            .rev()
            .take_while(|bucket| (bucket.tick + BUCKET_SECONDS) as f64 > lower)
        {
            counts.add(bucket.counts);
            for (sum, value) in histograms.iter_mut().zip(&bucket.histograms) {
                sum.add(value);
            }
        }
        (counts, histograms, (now - lower).max(0.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn time_weighted_smoothing_is_independent_of_poll_frequency() {
        let once = smooth(Some(0.0), Some(100.0), 10.0).unwrap();
        let mut repeated = Some(0.0);
        for _ in 0..10 {
            repeated = smooth(repeated, Some(100.0), 1.0);
        }
        assert!((once - 50.0).abs() < 1e-9);
        assert!((once - repeated.unwrap()).abs() < 1e-9);
        assert!(smooth(repeated, None, 1.0).is_none());
        assert!(smooth(repeated, Some(0.0), 10.0).unwrap() < once);
    }
    #[test]
    fn windows_expire_and_quantiles_merge_observations() {
        let mut window = Window::default();
        for time in 0..1000 {
            window.record(
                time,
                Counts {
                    success: 1,
                    ..Counts::default()
                },
                [Some(0.1); 7],
            );
        }
        assert!(window.buckets.len() <= 90);
        let (counts, histograms, seconds) = window.read(1000.0, 60, 0.0);
        assert_eq!(counts.success, 60);
        assert_eq!(histograms[0].snapshot().samples, 60);
        assert!((histograms[0].snapshot().p95.unwrap() - 0.1).abs() < 0.025);
        assert_eq!(seconds, 60.0);
        window.expire(1900);
        assert_eq!(window.read(1900.0, 300, 0.0).0.success, 0);
    }
}
