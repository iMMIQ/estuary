use std::{
    io,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    body::Body,
    response::{IntoResponse, Response, Sse},
};
use bytes::Bytes;
use futures_util::StreamExt;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::{
    anthropic, chat_bridge, error::GatewayError, metrics::Metrics, node::NodeLease,
    observability::Outcome, prefix::PrefixInput, server::AppState, sse, time::micros,
};

use super::headers::apply_upstream_response_headers;
use super::{MAX_SSE_EVENT_BYTES, UpstreamResponseMode};

enum StreamingOutput {
    Raw(Bytes),
    Events(Vec<sse::Event>),
}

pub(super) struct StreamingContext {
    pub(super) state: Arc<AppState>,
    pub(super) lease: NodeLease,
    pub(super) prefix_input: PrefixInput,
    pub(super) header_latency: Duration,
    pub(super) upstream_started: Instant,
    pub(super) response_mode: UpstreamResponseMode,
    pub(super) public_model: String,
    pub(super) record_prefix: bool,
    pub(super) log_attempt: Option<crate::session_log::AttemptGuard>,
}

enum BridgeChunk {
    Done,
    Json(Value),
}

#[allow(clippy::too_many_lines)]
pub(super) fn streaming_response(upstream: reqwest::Response, ctx: StreamingContext) -> Response {
    let raw_length = upstream.content_length();
    let status = upstream.status();
    let headers = upstream.headers().clone();
    let node_id = ctx.lease.node().id().to_owned();
    let stream_node = Arc::clone(ctx.lease.node());
    let (sender, receiver) = mpsc::channel::<StreamingOutput>(1);
    let terminal_failure = Arc::new(parking_lot::Mutex::new(None));
    let pump_failure = Arc::clone(&terminal_failure);
    let is_anthropic = ctx.response_mode.is_anthropic();
    let rewrites_body = ctx.response_mode.rewrites_body();
    let expose_node_header = ctx.state.settings.server.expose_node_header;

    let mut response = Response::new(build_downstream_body(
        receiver,
        Arc::clone(&terminal_failure),
        rewrites_body,
        is_anthropic,
    ));
    apply_upstream_response_headers(
        &mut response,
        status,
        &headers,
        &node_id,
        expose_node_header,
        &ctx.response_mode,
        true,
    );

    tokio::spawn(async move {
        let state = ctx.state;
        let settings = &state.settings;
        let stream_idle_timeout = Duration::from_millis(settings.server.stream_idle_timeout_ms);
        let upstream_body_timeout = Duration::from_millis(settings.server.upstream_body_timeout_ms);
        let downstream_stall_timeout =
            Duration::from_millis(settings.server.downstream_stall_timeout_ms);
        let mut guard = BodyGuard::new(
            ctx.lease,
            Arc::clone(&state.metrics),
            node_id,
            ctx.header_latency,
            ctx.upstream_started,
            ctx.log_attempt,
        );
        let keepalive_period = Duration::from_secs(10);
        let mut keepalive = tokio::time::interval_at(
            tokio::time::Instant::now() + keepalive_period,
            keepalive_period,
        );
        keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut indexed = false;
        let mut raw_bytes_seen = 0u64;
        let body_deadline = tokio::time::Instant::now() + upstream_body_timeout;
        let mut idle_deadline = tokio::time::Instant::now() + stream_idle_timeout;
        let mut bridge = match ctx.response_mode {
            UpstreamResponseMode::Passthrough => None,
            UpstreamResponseMode::ChatBridge { context } => Some((
                sse::SseParser::new(MAX_SSE_EVENT_BYTES),
                chat_bridge::StreamConverter::new(*context, ctx.public_model),
            )),
            UpstreamResponseMode::TokenCount => unreachable!("token counting is buffered"),
        };
        let mut raw_body = upstream.bytes_stream();

        loop {
            let downstream_wait_started = Instant::now();
            let permit = tokio::select! {
                biased;
                () = sender.closed() => return,
                () = tokio::time::sleep_until(body_deadline) => {
                    fail_response_stream(
                        &pump_failure,
                        &settings.health,
                        &mut guard,
                        StreamFailure::timed_out("upstream response body total timeout"),
                    );
                    return;
                }
                result = tokio::time::timeout(downstream_stall_timeout, sender.reserve()) => {
                    match result {
                        Ok(Ok(permit)) => permit,
                        Ok(Err(_)) => return,
                        Err(_) => {
                            if let Some(log) = &mut guard.log_attempt {
                                log.terminal_error("downstream", "downstream_stall");
                                log.finish("cancelled", Some("downstream_stall"));
                            }
                            *pump_failure.lock() = Some(StreamFailure::timed_out(
                                "downstream response body stalled",
                            ));
                            return;
                        }
                    }
                }
            };

            if let Some(log) = &guard.log_attempt {
                log.update(|attempt| {
                    *attempt
                        .timings_us
                        .entry("downstream_blocked".to_owned())
                        .or_default() += micros(downstream_wait_started.elapsed());
                });
            }
            let item = tokio::select! {
                biased;
                () = sender.closed() => return,
                () = tokio::time::sleep_until(body_deadline) => {
                    fail_response_stream(
                        &pump_failure,
                        &settings.health,
                        &mut guard,
                        StreamFailure::timed_out("upstream response body total timeout"),
                    );
                    return;
                }
                () = tokio::time::sleep_until(idle_deadline) => {
                    fail_response_stream(
                        &pump_failure,
                        &settings.health,
                        &mut guard,
                        StreamFailure::timed_out("upstream response body idle timeout"),
                    );
                    return;
                },
                _ = keepalive.tick(), if is_anthropic => {
                    permit.send(StreamingOutput::Events(vec![anthropic::ping_event()]));
                    continue;
                },
                item = raw_body.next() => item,
            };

            match item {
                Some(Ok(bytes)) => {
                    idle_deadline = tokio::time::Instant::now() + stream_idle_timeout;
                    guard.capture_raw(&bytes);
                    let output = if let Some((parser, adapter)) = bridge.as_mut() {
                        process_bridged_chunk(parser, adapter, &mut guard, &bytes).await
                    } else {
                        guard.observe_raw(&bytes);
                        raw_bytes_seen = raw_bytes_seen.saturating_add(bytes.len() as u64);
                        // Hyper may finish a Content-Length response without another
                        // body poll. Classify the last raw chunk before forwarding it.
                        if raw_length == Some(raw_bytes_seen) {
                            guard.completed();
                        }
                        Ok(StreamingOutput::Raw(bytes))
                    };
                    if ctx.record_prefix && !indexed && guard.first_token_observed {
                        state
                            .scheduler
                            .prefix_directory()
                            .record_for_node(&stream_node, &ctx.prefix_input);
                        indexed = true;
                    }
                    match output {
                        Ok(StreamingOutput::Events(events)) if events.is_empty() => {}
                        Ok(output) => permit.send(output),
                        Err(error) => {
                            fail_response_stream(
                                &pump_failure,
                                &settings.health,
                                &mut guard,
                                StreamFailure::upstream(error),
                            );
                            return;
                        }
                    }
                }
                Some(Err(error)) => {
                    fail_response_stream(
                        &pump_failure,
                        &settings.health,
                        &mut guard,
                        StreamFailure::upstream(error.to_string()),
                    );
                    return;
                }
                None => {
                    if let Some((_, adapter)) = bridge.as_mut() {
                        match adapter.finish().await {
                            Ok(events) if !events.is_empty() => {
                                permit.send(StreamingOutput::Events(events));
                            }
                            Ok(_) => {}
                            Err(error) => {
                                fail_response_stream(
                                    &pump_failure,
                                    &settings.health,
                                    &mut guard,
                                    StreamFailure::upstream(error.to_string()),
                                );
                                return;
                            }
                        }
                    }
                    if ctx.record_prefix && !indexed {
                        state
                            .scheduler
                            .prefix_directory()
                            .record_for_node(&stream_node, &ctx.prefix_input);
                    }
                    guard.completed();
                    return;
                }
            }
        }
    });

    response
}

async fn process_bridged_chunk(
    parser: &mut sse::SseParser,
    adapter: &mut chat_bridge::StreamConverter,
    guard: &mut BodyGuard,
    bytes: &[u8],
) -> Result<StreamingOutput, String> {
    let mut parsed = Vec::new();
    let mut parse_error: Option<String> = None;
    parser.push(bytes, |event| {
        if parse_error.is_some() {
            return;
        }
        match event {
            Ok(b"[DONE]") => parsed.push(BridgeChunk::Done),
            Ok(data) => match serde_json::from_slice::<Value>(data) {
                Ok(value) => parsed.push(BridgeChunk::Json(value)),
                Err(_) => parse_error = Some(GatewayError::InvalidUpstreamResponse.to_string()),
            },
            Err(()) => parse_error = Some("upstream SSE event exceeded 16 MiB".to_owned()),
        }
    });
    if let Some(error) = parse_error {
        return Err(error);
    }
    let mut events = Vec::new();
    for item in parsed {
        match item {
            BridgeChunk::Done => {
                guard.observe_done();
                events.extend(adapter.finish().await.map_err(|error| error.to_string())?);
            }
            BridgeChunk::Json(value) => {
                guard.observe_value(&value);
                events.extend(
                    adapter
                        .push_value(&value)
                        .await
                        .map_err(|error| error.to_string())?,
                );
            }
        }
    }
    Ok(StreamingOutput::Events(events))
}

fn build_downstream_body(
    mut receiver: mpsc::Receiver<StreamingOutput>,
    terminal_failure: Arc<parking_lot::Mutex<Option<StreamFailure>>>,
    rewrites_body: bool,
    is_anthropic: bool,
) -> Body {
    if rewrites_body {
        let stream = async_stream::stream! {
            while let Some(output) = receiver.recv().await {
                match output {
                    StreamingOutput::Events(events) => {
                        for event in events {
                            yield Ok::<_, io::Error>(event.into_axum());
                        }
                    }
                    StreamingOutput::Raw(_) => {
                        yield Err(io::Error::other("raw bytes in rewritten SSE stream"));
                    }
                }
            }
            let failure = { terminal_failure.lock().take() };
            if let Some(failure) = failure {
                if is_anthropic {
                    yield Ok(anthropic::stream_error_event(&failure.message).into_axum());
                } else {
                    yield Err(failure.into_io_error());
                }
            }
        };
        Sse::new(stream).into_response().into_body()
    } else {
        let stream = async_stream::stream! {
            while let Some(output) = receiver.recv().await {
                match output {
                    StreamingOutput::Raw(bytes) => yield Ok::<_, io::Error>(bytes),
                    StreamingOutput::Events(_) => {
                        yield Err(io::Error::other("SSE events in passthrough stream"));
                    }
                }
            }
            let failure = { terminal_failure.lock().take() };
            if let Some(failure) = failure {
                yield Err(failure.into_io_error());
            }
        };
        Body::from_stream(stream)
    }
}

#[derive(Debug)]
struct StreamFailure {
    kind: io::ErrorKind,
    message: String,
}

impl StreamFailure {
    fn timed_out(message: &'static str) -> Self {
        Self {
            kind: io::ErrorKind::TimedOut,
            message: message.to_owned(),
        }
    }

    fn upstream(message: String) -> Self {
        Self {
            kind: io::ErrorKind::Other,
            message,
        }
    }

    fn into_io_error(self) -> io::Error {
        io::Error::new(self.kind, self.message)
    }
}

fn fail_response_stream(
    terminal_failure: &parking_lot::Mutex<Option<StreamFailure>>,
    health_config: &crate::config::HealthConfig,
    guard: &mut BodyGuard,
    failure: StreamFailure,
) {
    guard.failed(failure.message.clone(), health_config);
    *terminal_failure.lock() = Some(failure);
}

struct BodyGuard {
    lease: Option<NodeLease>,
    metrics: Arc<Metrics>,
    node_id: String,
    header_latency: Duration,
    upstream_started: Instant,
    observation: crate::inference_stats::StreamObservation,
    first_token_observed: bool,
    terminal: bool,
    log_attempt: Option<crate::session_log::AttemptGuard>,
}

impl BodyGuard {
    fn new(
        lease: NodeLease,
        metrics: Arc<Metrics>,
        node_id: String,
        header_latency: Duration,
        upstream_started: Instant,
        log_attempt: Option<crate::session_log::AttemptGuard>,
    ) -> Self {
        Self {
            lease: Some(lease),
            metrics,
            node_id,
            header_latency,
            upstream_started,
            observation: crate::inference_stats::StreamObservation::default(),
            first_token_observed: false,
            terminal: false,
            log_attempt,
        }
    }

    fn report_telemetry(&self, outcome: Outcome) {
        if let Some(lease) = &self.lease
            && let Some(telemetry) = &lease.observation
        {
            telemetry.upstream(
                outcome,
                self.observation.usage,
                self.observation.provider_timings.mean_itl_seconds(),
                lease.generations(),
            );
        }
    }

    fn capture_raw(&self, bytes: &[u8]) {
        if let Some(log) = &self.log_attempt {
            let elapsed = micros(self.upstream_started.elapsed());
            log.update(|attempt| {
                attempt
                    .timings_us
                    .entry("first_chunk".to_owned())
                    .or_insert(elapsed);
            });
            log.capture(bytes, true);
        }
    }

    fn observe_raw(&mut self, bytes: &[u8]) {
        let before = self.observation.generation_events;
        self.observation.observe_bytes(bytes);
        self.after_observe(before);
    }

    fn observe_value(&mut self, value: &Value) {
        let before = self.observation.generation_events;
        self.observation.observe_value(value);
        self.after_observe(before);
    }

    fn observe_done(&mut self) {
        self.observation.observe_done();
    }

    fn after_observe(&mut self, previous_generation_events: u64) {
        if self.observation.generation_events > previous_generation_events
            && let Some(lease) = &self.lease
            && let Some(telemetry) = &lease.observation
        {
            telemetry.generation(self.upstream_started.elapsed());
        }
        if self.observation.has_visible_text
            && let Some(log) = &self.log_attempt
        {
            log.first_visible_text();
        }
        if self.observation.has_output && !self.first_token_observed {
            let elapsed = self.upstream_started.elapsed();
            if let Some(lease) = &self.lease {
                lease.record_first_token(elapsed);
            }
            self.metrics.observe_first_token(elapsed);
            if let Some(log) = &self.log_attempt {
                log.first_output();
            }
            self.first_token_observed = true;
        }
    }

    fn completed(&mut self) {
        if self.terminal {
            return;
        }
        if let Some(log) = &mut self.log_attempt {
            log.update(|attempt| {
                attempt.timings_us.insert(
                    "upstream_done".to_owned(),
                    micros(self.upstream_started.elapsed()),
                );
                attempt.usage = self.observation.usage.log_value();
                self.observation
                    .provider_timings
                    .write_to(&mut attempt.timings_us);
            });
            if self.observation.error_seen {
                log.terminal_error("stream", "upstream_stream_error");
                log.finish("error", Some("upstream_stream_error"));
            } else if self.observation.incomplete {
                log.terminal_unknown("observation_incomplete");
                log.finish("unknown", Some("observation_incomplete"));
            } else if !self.observation.terminal_marker_seen {
                if log.requires_terminal_marker() {
                    log.terminal_error("stream", "missing_terminal_marker");
                    log.finish("unknown", Some("missing_terminal_marker"));
                } else {
                    log.terminal_unknown("unrecognized_stream_completion");
                    log.finish("unknown", Some("unrecognized_stream_completion"));
                }
            } else {
                log.finish("success", None);
            }
        }
        if let Some(lease) = &self.lease {
            if let Some(tokens) = self.observation.usage.output_tokens {
                lease.record_output_tokens(tokens);
            }
            lease.record_success(self.header_latency);
        }
        self.metrics.observe_usage(self.observation.usage);
        let outcome = if self.observation.error_seen {
            Outcome::Failed
        } else if self.observation.incomplete || !self.observation.terminal_marker_seen {
            Outcome::Unknown
        } else {
            Outcome::Success
        };
        self.report_telemetry(outcome);
        self.terminal = true;
    }

    fn failed(&mut self, message: String, health_config: &crate::config::HealthConfig) {
        self.report_telemetry(Outcome::Failed);
        if let Some(log) = &mut self.log_attempt {
            let class = if message.contains("idle timeout") {
                "body_idle_timeout"
            } else if message.contains("total timeout") {
                "body_total_timeout"
            } else {
                "stream_error"
            };
            log.terminal_error("stream", class);
            log.finish("error", Some(class));
        }
        if let Some(lease) = &self.lease {
            lease.record_failure(message, health_config);
        }
        self.metrics.stream_error(&self.node_id);
        self.terminal = true;
    }
}

impl Drop for BodyGuard {
    fn drop(&mut self) {
        if !self.terminal {
            self.report_telemetry(Outcome::Cancelled);
            self.metrics.stream_cancelled(&self.node_id);
        }
    }
}
