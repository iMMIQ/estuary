use axum::http::HeaderMap;
use parking_lot::Mutex;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};
use tokio::sync::mpsc;

use super::{
    Command, LogSink,
    model::{AttemptRecord, LogEvent, RequestRecord},
    unix_ms,
};

pub(super) struct CapturedPayload {
    pub stage: String,
    pub attempt: usize,
    pub bytes: Vec<u8>,
    pub bytes_seen: usize,
    pub state: String,
    pub streaming: bool,
    pub anthropic: bool,
    budget: Arc<AtomicUsize>,
}

impl Drop for CapturedPayload {
    fn drop(&mut self) {
        self.budget.fetch_sub(self.bytes.len(), Ordering::Relaxed);
    }
}

struct Pending {
    record: RequestRecord,
    payloads: BTreeMap<(String, usize), CapturedPayload>,
    active_attempts: usize,
    downstream_done: bool,
    permit: Option<mpsc::OwnedPermit<Command>>,
}

pub struct Observation {
    sink: Arc<LogSink>,
    started: Instant,
    pending: Mutex<Pending>,
}

impl Observation {
    pub(super) fn begin(
        sink: &Arc<LogSink>,
        endpoint: &str,
        headers: &HeaderMap,
        external_id: &str,
    ) -> Option<Arc<Self>> {
        let sender = sink.sender.as_ref()?;
        let Ok(permit) = sender.clone().try_reserve_owned() else {
            sink.stats.dropped.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        let session_id = header(headers, "x-estuary-session-id");
        let record = RequestRecord {
            id: uuid::Uuid::now_v7().to_string(),
            external_request_id: external_id.chars().take(128).collect(),
            session_source: session_id.as_ref().map(|_| "explicit_header".to_owned()),
            session_id,
            boot_id: sink.boot_id.clone(),
            process_id: std::process::id(),
            process_token: sink.process_token.clone(),
            gateway_version: crate::VERSION.to_owned(),
            endpoint: endpoint.chars().take(128).collect(),
            protocol: if endpoint.starts_with("/v1/messages") {
                "anthropic_messages"
            } else if endpoint == "/v1/responses" {
                "openai_responses"
            } else {
                "openai"
            }
            .to_owned(),
            client: header(headers, "user-agent"),
            started_at_ms: unix_ms(),
            outcome: "started".to_owned(),
            delivery: "pending".to_owned(),
            capture_state: if sink.config.capture_content {
                "pending"
            } else {
                "metadata_only"
            }
            .to_owned(),
            ..RequestRecord::default()
        };
        sink.stats.queued.fetch_add(1, Ordering::Relaxed);
        if sender
            .try_send(Command::Start(Box::new(record.clone())))
            .is_err()
        {
            sink.stats.queued.fetch_sub(1, Ordering::Relaxed);
            sink.stats.dropped.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        Some(Arc::new(Self {
            sink: Arc::clone(sink),
            started: Instant::now(),
            pending: Mutex::new(Pending {
                record,
                payloads: BTreeMap::new(),
                active_attempts: 0,
                downstream_done: false,
                permit: Some(permit),
            }),
        }))
    }

    pub(crate) fn timing(&self, name: &str, micros: u64) {
        self.pending
            .lock()
            .record
            .timings_us
            .insert(name.to_owned(), micros);
    }

    pub(crate) fn request_body(&self, body: &[u8], elapsed: u64) {
        {
            let mut pending = self.pending.lock();
            pending.record.request_bytes = body.len() as u64;
            pending
                .record
                .timings_us
                .insert("body_read".to_owned(), elapsed);
        }
        self.capture("client_input", 0, body, false, false);
    }

    pub(crate) fn parsed(&self, body: Option<&Value>) {
        let mut pending = self.pending.lock();
        pending.record.model = body
            .and_then(|v| v.get("model"))
            .and_then(Value::as_str)
            .map(|s| s.chars().take(256).collect());
        pending.record.streaming = body
            .and_then(|v| v.get("stream"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if pending.record.session_id.is_none() {
            pending.record.session_id = body
                .and_then(|v| v.pointer("/metadata/session_id"))
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty() && s.len() <= 128)
                .map(str::to_owned);
            if pending.record.session_id.is_some() {
                pending.record.session_source = Some("explicit_metadata".to_owned());
            }
        }
    }

    pub(crate) fn capture(
        &self,
        stage: &str,
        attempt: usize,
        bytes: &[u8],
        streaming: bool,
        anthropic: bool,
    ) {
        if !self.sink.config.capture_content {
            return;
        }
        let mut pending = self.pending.lock();
        let capture = pending
            .payloads
            .entry((stage.to_owned(), attempt))
            .or_insert_with(|| CapturedPayload {
                stage: stage.to_owned(),
                attempt,
                bytes: Vec::new(),
                bytes_seen: 0,
                state: "complete".to_owned(),
                streaming,
                anthropic,
                budget: Arc::clone(&self.sink.content_bytes),
            });
        capture.bytes_seen = capture.bytes_seen.saturating_add(bytes.len());
        if capture.state == "partial" {
            return;
        }
        let remaining = self
            .sink
            .config
            .max_payload_bytes
            .saturating_sub(capture.bytes.len());
        let wanted = bytes.len().min(remaining);
        let reserved = self
            .sink
            .content_bytes
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                (used.saturating_add(wanted) <= self.sink.config.max_content_bytes)
                    .then_some(used + wanted)
            })
            .is_ok();
        if reserved {
            capture.bytes.extend_from_slice(&bytes[..wanted]);
        }
        if wanted < bytes.len() || !reserved {
            if capture.state == "complete" {
                self.sink.stats.truncated.fetch_add(1, Ordering::Relaxed);
            }
            "partial".clone_into(&mut capture.state);
        }
    }

    pub(crate) fn error(&self, phase: &str, class: &str) {
        let mut pending = self.pending.lock();
        "error".clone_into(&mut pending.record.outcome);
        pending.record.error_phase = Some(phase.to_owned());
        pending.record.error_class = Some(class.to_owned());
    }

    pub(crate) fn event(&self, kind: &str) {
        let mut pending = self.pending.lock();
        if pending.record.events.len() < 32 {
            pending.record.events.push(LogEvent {
                kind: kind.to_owned(),
                elapsed_us: super::micros(self.started.elapsed()),
            });
        }
    }

    pub(crate) fn headers(&self, status: u16, streaming: bool) {
        let mut pending = self.pending.lock();
        pending.record.http_status = Some(status);
        // A streaming request can fail with a JSON error response. Preserve the
        // requested mode so those failures remain visible in stream diagnostics.
        pending.record.streaming |= streaming;
        pending.record.timings_us.insert(
            "headers_ready".to_owned(),
            super::micros(self.started.elapsed()),
        );
        if status >= 400 && pending.record.outcome != "error" {
            "error".clone_into(&mut pending.record.outcome);
            pending.record.error_phase = Some("gateway".to_owned());
            pending.record.error_class = Some(format!("http_{status}"));
        }
    }

    pub(crate) fn downstream_bytes(&self, bytes: &[u8], streaming: bool) {
        self.pending.lock().record.response_bytes += bytes.len() as u64;
        self.capture("client_output", 0, bytes, streaming, false);
    }

    pub(crate) fn downstream_done(&self, consumed: bool) {
        let mut pending = self.pending.lock();
        pending.downstream_done = true;
        if consumed {
            "body_consumed"
        } else {
            "body_dropped"
        }
        .clone_into(&mut pending.record.delivery);
        pending.record.ended_at_ms = Some(unix_ms());
        pending
            .record
            .timings_us
            .insert("total".to_owned(), super::micros(self.started.elapsed()));
        if pending.record.outcome == "started" {
            if consumed { "success" } else { "cancelled" }.clone_into(&mut pending.record.outcome);
        }
        self.finalize(&mut pending);
    }

    pub(crate) fn attempt(self: &Arc<Self>, record: AttemptRecord) -> AttemptGuard {
        let mut pending = self.pending.lock();
        let index = pending.record.attempts.len();
        pending.record.attempts.push(record);
        pending.active_attempts += 1;
        AttemptGuard {
            observation: Arc::clone(self),
            index,
            started: Instant::now(),
            done: false,
        }
    }

    fn finalize(&self, pending: &mut Pending) {
        if pending.downstream_done
            && pending.active_attempts == 0
            && let Some(permit) = pending.permit.take()
        {
            let mut record = std::mem::take(&mut pending.record);
            record.usage = record
                .attempts
                .last()
                .map_or(Value::Null, |attempt| attempt.usage.clone());
            let payloads = std::mem::take(&mut pending.payloads)
                .into_values()
                .collect::<Vec<_>>();
            if self.sink.config.capture_content {
                if payloads.iter().any(|p| p.state == "partial") {
                    "partial"
                } else if payloads.is_empty() {
                    "missing"
                } else {
                    "captured"
                }
                .clone_into(&mut record.capture_state);
            }
            self.sink.stats.queued.fetch_add(1, Ordering::Relaxed);
            permit.send(Command::Finish(Box::new(record), payloads));
        }
    }
}

pub(crate) struct AttemptGuard {
    observation: Arc<Observation>,
    index: usize,
    started: Instant,
    done: bool,
}

impl AttemptGuard {
    pub(crate) fn update(&self, change: impl FnOnce(&mut AttemptRecord)) {
        change(&mut self.observation.pending.lock().record.attempts[self.index]);
    }
    pub(crate) fn capture(&self, body: &[u8], streaming: bool, anthropic: bool) {
        let number = self.index + 1;
        self.observation
            .capture("upstream_output", number, body, streaming, anthropic);
    }
    pub(crate) fn input(&self, body: &[u8]) {
        self.observation
            .capture("upstream_input", self.index + 1, body, false, false);
    }
    pub(crate) fn first_output(&self) {
        let elapsed = super::micros(self.started.elapsed());
        self.update(|attempt| {
            attempt
                .timings_us
                .entry("first_output".to_owned())
                .or_insert(elapsed);
        });
        let mut pending = self.observation.pending.lock();
        pending
            .record
            .timings_us
            .entry("first_output".to_owned())
            .or_insert_with(|| super::micros(self.observation.started.elapsed()));
    }
    pub(crate) fn first_visible_text(&self) {
        self.update(|attempt| {
            attempt
                .timings_us
                .entry("first_visible_text".to_owned())
                .or_insert_with(|| super::micros(self.started.elapsed()));
        });
        let mut pending = self.observation.pending.lock();
        pending
            .record
            .timings_us
            .entry("first_visible_text".to_owned())
            .or_insert_with(|| super::micros(self.observation.started.elapsed()));
    }
    pub(crate) fn requires_terminal_marker(&self) -> bool {
        matches!(
            self.observation.pending.lock().record.endpoint.as_str(),
            "/v1/chat/completions" | "/v1/completions" | "/v1/responses" | "/v1/messages"
        )
    }
    pub(crate) fn terminal_unknown(&self, class: &str) {
        let mut pending = self.observation.pending.lock();
        "unknown".clone_into(&mut pending.record.outcome);
        pending.record.error_phase = Some("observer".to_owned());
        pending.record.error_class = Some(class.to_owned());
    }
    pub(crate) fn terminal_error(&self, phase: &str, class: &str) {
        self.observation.error(phase, class);
    }
    pub(crate) fn finish(&mut self, outcome: &str, error: Option<&str>) {
        self.update(|attempt| {
            outcome.clone_into(&mut attempt.outcome);
            attempt.error_class = error.map(str::to_owned);
            attempt
                .timings_us
                .insert("total".to_owned(), super::micros(self.started.elapsed()));
        });
        self.done = true;
    }
}

impl Drop for AttemptGuard {
    fn drop(&mut self) {
        if !self.done {
            self.update(|attempt| {
                "cancelled".clone_into(&mut attempt.outcome);
            });
        }
        let mut pending = self.observation.pending.lock();
        pending.active_attempts -= 1;
        self.observation.finalize(&mut pending);
    }
}

fn header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty() && s.len() <= 128)
        .map(str::to_owned)
}

pub(crate) struct DeliveryGuard(pub Arc<Observation>, pub bool);
impl Drop for DeliveryGuard {
    fn drop(&mut self) {
        self.0.downstream_done(self.1);
    }
}
