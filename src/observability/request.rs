use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::Mutex;

use super::{Counts, Observability};
use crate::inference_stats::Usage;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    Success,
    Failed,
    Cancelled,
    Unknown,
}

#[derive(Debug, Default)]
// Independent request mode, provenance and finalization flags.
#[allow(clippy::struct_excessive_bools)]
struct Pending {
    model: Option<String>,
    node: Option<(String, u64)>,
    status: Option<u16>,
    streaming: bool,
    upstream: Option<Outcome>,
    first_output: Option<Duration>,
    last_output: Option<Duration>,
    upstream_ttft: Option<Duration>,
    ingress: Duration,
    queue: Duration,
    waiting: Option<(bool, Instant)>,
    usage: Usage,
    tpot: Option<f64>,
    tpot_reported: bool,
    route: Option<usize>,
    retried: bool,
    finished: bool,
    generating: bool,
}

#[derive(Debug)]
pub struct Observation {
    hub: Arc<Observability>,
    started: Instant,
    pending: Mutex<Pending>,
}
impl Observation {
    pub(super) fn new(hub: Arc<Observability>) -> Arc<Self> {
        hub.record(
            None,
            None,
            true,
            Counts {
                started: 1,
                ..Counts::default()
            },
            [None; 7],
        );
        Arc::new(Self {
            hub,
            started: Instant::now(),
            pending: Mutex::new(Pending {
                generating: true,
                ..Pending::default()
            }),
        })
    }
    pub(crate) fn non_generating(&self) {
        self.pending.lock().generating = false;
    }
    pub(crate) fn model(&self, model: &str) {
        if model.len() > 1024 {
            return;
        }
        let mut pending = self.pending.lock();
        if pending.model.is_none() {
            pending.model = Some(model.to_owned());
            self.hub.record(
                Some(model),
                None,
                false,
                Counts {
                    started: 1,
                    ..Counts::default()
                },
                [None; 7],
            );
        }
    }
    pub(crate) fn assign(&self, node: &str, instance: u64) {
        let mut pending = self.pending.lock();
        if node.len() > 1024 {
            pending.node = None;
            return;
        }
        pending.node = Some((node.to_owned(), instance));
        pending.upstream = None;
        pending.usage = Usage::default();
        self.hub.record(
            None,
            Some((node, instance)),
            false,
            Counts {
                started: 1,
                ..Counts::default()
            },
            [None; 7],
        );
    }
    pub(crate) fn retry(&self) {
        let mut pending = self.pending.lock();
        pending.retried = true;
        self.hub.record(
            pending.model.as_deref(),
            None,
            true,
            Counts {
                retries: 1,
                ..Counts::default()
            },
            [None; 7],
        );
        self.hub.record(
            None,
            pending
                .node
                .as_ref()
                .map(|(id, instance)| (id.as_str(), *instance)),
            false,
            Counts {
                failed: 1,
                retries: 1,
                generation_requests: u64::from(pending.generating),
                ..Counts::default()
            },
            [None; 7],
        );
    }
    pub(crate) fn ingress(&self, duration: Duration) {
        let mut pending = self.pending.lock();
        pending.ingress += duration;
        pending.waiting = None;
    }
    pub(crate) fn queue(&self, duration: Duration) {
        let mut pending = self.pending.lock();
        pending.queue += duration;
        pending.waiting = None;
    }
    pub(crate) fn wait_started(&self, ingress: bool) {
        self.pending.lock().waiting = Some((ingress, Instant::now()));
    }
    pub(crate) fn route(&self, exact: usize, approximate: usize) {
        self.pending.lock().route = Some(if exact > 0 {
            0
        } else if approximate > 0 {
            1
        } else {
            2
        });
    }
    pub(crate) fn generation(&self, upstream_elapsed: Duration) {
        let mut pending = self.pending.lock();
        let elapsed = self.started.elapsed();
        pending.first_output.get_or_insert(elapsed);
        pending.upstream_ttft.get_or_insert(upstream_elapsed);
        pending.last_output = Some(elapsed);
    }
    pub(crate) fn upstream(
        &self,
        outcome: Outcome,
        usage: Usage,
        tpot: Option<f64>,
        generations: usize,
    ) {
        let mut pending = self.pending.lock();
        pending.upstream = Some(outcome);
        pending.usage = usage;
        pending.tpot = tpot.filter(|value| value.is_finite() && *value > 0.0);
        pending.tpot_reported = pending.tpot.is_some();
        if pending.tpot.is_none()
            && generations == 1
            && let (Some(first), Some(last), Some(tokens)) = (
                pending.first_output,
                pending.last_output,
                usage.output_tokens,
            )
            && tokens > 1
            && last > first
        {
            pending.tpot = Some(last.saturating_sub(first).as_secs_f64() / (tokens - 1) as f64);
        }
    }
    pub(crate) fn failed(&self) {
        self.pending.lock().upstream = Some(Outcome::Failed);
    }
    pub(crate) fn headers(&self, status: u16, streaming: bool) {
        let mut pending = self.pending.lock();
        pending.status = Some(status);
        pending.streaming = streaming;
    }
    fn finish(&self, delivered: bool) {
        let mut pending = self.pending.lock();
        if pending.finished {
            return;
        }
        pending.finished = true;
        if let Some((ingress, started)) = pending.waiting.take() {
            if ingress {
                pending.ingress += started.elapsed();
            } else {
                pending.queue += started.elapsed();
            }
        }
        let outcome = if pending.status.is_some_and(|status| status >= 400)
            || pending.upstream == Some(Outcome::Failed)
        {
            Outcome::Failed
        } else if !delivered {
            Outcome::Cancelled
        } else if pending.streaming {
            pending.upstream.unwrap_or(Outcome::Unknown)
        } else {
            Outcome::Success
        };
        let usage = pending.usage;
        let mut counts = Counts {
            generation_requests: u64::from(pending.generating),
            retried_requests: u64::from(pending.retried),
            input_tokens: usage.input_tokens.unwrap_or(0) as u64,
            output_tokens: usage.output_tokens.unwrap_or(0) as u64,
            cached_tokens: usage
                .cached_tokens
                .zip(usage.input_tokens)
                .map_or(0, |(cached, input)| cached.min(input)) as u64,
            cache_input_tokens: usage.cached_tokens.and(usage.input_tokens).unwrap_or(0) as u64,
            input_samples: u64::from(usage.input_tokens.is_some()),
            output_samples: u64::from(pending.generating && usage.output_tokens.is_some()),
            cache_samples: u64::from(usage.cached_tokens.is_some() && usage.input_tokens.is_some()),
            tpot_reported: u64::from(pending.tpot.is_some() && pending.tpot_reported),
            tpot_estimated: u64::from(pending.tpot.is_some() && !pending.tpot_reported),
            ..Counts::default()
        };
        match outcome {
            Outcome::Success => counts.success = 1,
            Outcome::Failed => counts.failed = 1,
            Outcome::Cancelled => counts.cancelled = 1,
            Outcome::Unknown => counts.unknown = 1,
        }
        match pending.route {
            Some(0) => counts.route_exact = 1,
            Some(1) => counts.route_approximate = 1,
            Some(_) => counts.route_none = 1,
            None => {}
        }
        self.hub.record(
            pending.model.as_deref(),
            pending
                .node
                .as_ref()
                .map(|(id, instance)| (id.as_str(), *instance)),
            true,
            counts,
            [
                pending.first_output.map(|duration| duration.as_secs_f64()),
                pending.upstream_ttft.map(|duration| duration.as_secs_f64()),
                Some(self.started.elapsed().as_secs_f64()),
                Some(pending.ingress.as_secs_f64()),
                Some(pending.queue.as_secs_f64()),
                pending.tpot,
                pending.tpot.map(f64::recip),
            ],
        );
    }
}

pub(crate) struct Delivery {
    observation: Arc<Observation>,
    pub complete: bool,
}
impl Delivery {
    pub(crate) fn new(observation: Arc<Observation>) -> Self {
        Self {
            observation,
            complete: false,
        }
    }
}
impl Drop for Delivery {
    fn drop(&mut self) {
        self.observation.finish(self.complete);
    }
}
