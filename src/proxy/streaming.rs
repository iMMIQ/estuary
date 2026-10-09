use std::{
    fmt, io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use axum::{
    body::Body,
    http::{
        HeaderName, HeaderValue,
        header::{CONTENT_LENGTH, CONTENT_TYPE},
    },
    response::{IntoResponse, Response, Sse},
};
use bytes::Bytes;
use eventsource_stream::Eventsource;
use futures_util::{Stream, StreamExt};
use tokio::sync::mpsc;

use crate::{
    anthropic, anthropic_responses, codex,
    error::GatewayError,
    metrics::Metrics,
    node::{Node, NodeLease},
    prefix::PrefixInput,
    sse,
};

use super::headers::copy_response_headers;
use super::{MAX_SSE_EVENT_BYTES, UpstreamResponseMode};

use super::response::set_thinking_budget_warning;

pub(super) enum ResponseStreamAdapter {
    Codex(codex::StreamRewriter),
    Chat(anthropic::StreamConverter),
    Responses(anthropic_responses::StreamConverter),
    Native(anthropic::NativeStreamRewriter),
}

impl ResponseStreamAdapter {
    pub(super) fn push_event(
        &mut self,
        event: sse::Event,
    ) -> Result<Vec<sse::Event>, GatewayError> {
        match self {
            Self::Codex(rewriter) => rewriter.push_event(event),
            Self::Chat(converter) => converter.push_event(&event),
            Self::Responses(converter) => converter.push_event(&event),
            Self::Native(rewriter) => rewriter.push_event(event),
        }
    }

    pub(super) fn finish(&mut self) -> Result<Vec<sse::Event>, GatewayError> {
        match self {
            Self::Codex(_) | Self::Native(_) => Ok(Vec::new()),
            Self::Chat(converter) => converter.finish(),
            Self::Responses(converter) => converter.finish(),
        }
    }
}

pub(super) enum StreamingInput {
    Raw(Bytes),
    Event(sse::Event),
}

pub(super) enum StreamingOutput {
    Raw(Bytes),
    Events(Vec<sse::Event>),
}

pub(super) struct LimitedSseInput<S> {
    pub(super) inner: Pin<Box<S>>,
    pub(super) event_bytes: usize,
    pub(super) line_has_data: bool,
    pub(super) after_cr: bool,
    pub(super) max_event_bytes: usize,
}

impl<S> LimitedSseInput<S> {
    pub(super) fn new(inner: S, max_event_bytes: usize) -> Self {
        Self {
            inner: Box::pin(inner),
            event_bytes: 0,
            line_has_data: false,
            after_cr: false,
            max_event_bytes,
        }
    }

    fn inspect(&mut self, bytes: &[u8]) -> bool {
        for byte in bytes {
            self.event_bytes = self.event_bytes.saturating_add(1);
            if self.event_bytes > self.max_event_bytes {
                return false;
            }
            match *byte {
                b'\r' => {
                    if !self.line_has_data {
                        self.event_bytes = 0;
                    }
                    self.line_has_data = false;
                    self.after_cr = true;
                }
                b'\n' if self.after_cr => {
                    self.after_cr = false;
                }
                b'\n' => {
                    if !self.line_has_data {
                        self.event_bytes = 0;
                    }
                    self.line_has_data = false;
                }
                _ => {
                    self.line_has_data = true;
                    self.after_cr = false;
                }
            }
        }
        true
    }
}

#[derive(Debug)]
pub(super) enum SseInputError<E> {
    Transport(E),
    EventTooLarge,
}

impl<E: fmt::Display> fmt::Display for SseInputError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(error) => error.fmt(formatter),
            Self::EventTooLarge => formatter.write_str("upstream SSE event exceeded 16 MiB"),
        }
    }
}

impl<S, B, E> Stream for LimitedSseInput<S>
where
    S: Stream<Item = Result<B, E>>,
    B: AsRef<[u8]>,
{
    type Item = Result<B, SseInputError<E>>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.inner.as_mut().poll_next(context) {
            Poll::Ready(Some(Ok(bytes))) if this.inspect(bytes.as_ref()) => {
                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(Some(Ok(_))) => Poll::Ready(Some(Err(SseInputError::EventTooLarge))),
            Poll::Ready(Some(Err(error))) => {
                Poll::Ready(Some(Err(SseInputError::Transport(error))))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(super) fn streaming_response(
    upstream: reqwest::Response,
    lease: NodeLease,
    node: &Node,
    metrics: Arc<Metrics>,
    prefix_directory: Arc<crate::prefix::PrefixDirectory>,
    prefix_input: PrefixInput,
    health_config: crate::config::HealthConfig,
    header_latency: Duration,
    upstream_started: Instant,
    stream_idle_timeout: Duration,
    upstream_body_timeout: Duration,
    downstream_stall_timeout: Duration,
    expose_node_header: bool,
    response_mode: UpstreamResponseMode,
    public_model: String,
    record_prefix: bool,
    log_attempt: Option<crate::session_log::AttemptGuard>,
) -> Response {
    let raw_length = upstream.content_length();
    let status = upstream.status();
    let headers = upstream.headers().clone();
    let upstream_request_id = headers.get("x-request-id").cloned();
    let node_id = node.id().to_owned();
    let stream_node_id = node_id.clone();
    let (sender, mut receiver) = mpsc::channel::<StreamingOutput>(1);
    let terminal_failure = Arc::new(parking_lot::Mutex::new(None));
    let pump_failure = Arc::clone(&terminal_failure);
    let is_anthropic = response_mode.is_anthropic();
    let rewrites_body = response_mode.rewrites_body();
    let thinking_budget_approximated = response_mode.thinking_budget_approximated();

    tokio::spawn(async move {
        let mut guard = BodyGuard::new(
            lease,
            Arc::clone(&metrics),
            stream_node_id.clone(),
            header_latency,
            upstream_started,
        );
        guard.anthropic_usage =
            matches!(&response_mode, UpstreamResponseMode::NativeAnthropic { .. });
        guard.log_attempt = log_attempt;
        let needs_keepalive = is_anthropic;
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
        let mut stream_adapter = match response_mode {
            UpstreamResponseMode::Passthrough => None,
            UpstreamResponseMode::Codex { namespaces } => Some(ResponseStreamAdapter::Codex(
                codex::StreamRewriter::new(namespaces),
            )),
            UpstreamResponseMode::ChatToAnthropic { expose_thinking } => {
                Some(ResponseStreamAdapter::Chat(
                    anthropic::StreamConverter::new(public_model, expose_thinking),
                ))
            }
            UpstreamResponseMode::ResponsesToAnthropic { expose_thinking } => {
                Some(ResponseStreamAdapter::Responses(
                    anthropic_responses::StreamConverter::new(public_model, expose_thinking),
                ))
            }
            UpstreamResponseMode::NativeAnthropic {
                expose_thinking, ..
            } => Some(ResponseStreamAdapter::Native(
                anthropic::NativeStreamRewriter::new(public_model, expose_thinking),
            )),
        };
        let raw_body = upstream.bytes_stream();
        let mut body: Pin<Box<dyn Stream<Item = Result<StreamingInput, String>> + Send>> =
            if stream_adapter.is_some() {
                Box::pin(
                    LimitedSseInput::new(raw_body, MAX_SSE_EVENT_BYTES)
                        .eventsource()
                        .map(|result| {
                            result
                                .map(sse::Event::from_parsed)
                                .map(StreamingInput::Event)
                                .map_err(|error| error.to_string())
                        }),
                )
            } else {
                Box::pin(raw_body.map(|result| {
                    result
                        .map(StreamingInput::Raw)
                        .map_err(|error| error.to_string())
                }))
            };

        loop {
            let downstream_wait_started = Instant::now();
            let permit = tokio::select! {
                biased;
                () = sender.closed() => return,
                () = tokio::time::sleep_until(body_deadline) => {
                    fail_response_stream(
                        &pump_failure,
                        &health_config,
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
                        .or_default() +=
                        crate::session_log::micros(downstream_wait_started.elapsed());
                });
            }
            let item = tokio::select! {
                biased;
                () = sender.closed() => return,
                () = tokio::time::sleep_until(body_deadline) => {
                    fail_response_stream(
                        &pump_failure,
                        &health_config,
                        &mut guard,
                        StreamFailure::timed_out("upstream response body total timeout"),
                    );
                    return;
                }
                () = tokio::time::sleep_until(idle_deadline) => {
                        fail_response_stream(
                            &pump_failure,
                            &health_config,
                            &mut guard,
                            StreamFailure::timed_out(
                                "upstream response body idle timeout",
                            ),
                        );
                        return;
                },
                _ = keepalive.tick(), if needs_keepalive => {
                    permit.send(StreamingOutput::Events(vec![anthropic::ping_event()]));
                    continue;
                },
                item = body.next() => item,
            };

            match item {
                Some(Ok(input)) => {
                    idle_deadline = tokio::time::Instant::now() + stream_idle_timeout;
                    guard.observe(&input);
                    if let StreamingInput::Raw(bytes) = &input {
                        raw_bytes_seen = raw_bytes_seen.saturating_add(bytes.len() as u64);
                        // Hyper may finish a Content-Length response without another
                        // body poll. Classify the last raw chunk before forwarding it.
                        if raw_length == Some(raw_bytes_seen) {
                            guard.completed();
                        }
                    }
                    if record_prefix && !indexed && guard.first_token_observed {
                        prefix_directory.record(&stream_node_id, &prefix_input);
                        indexed = true;
                    }
                    let output = match (input, stream_adapter.as_mut()) {
                        (StreamingInput::Raw(bytes), None) => Ok(StreamingOutput::Raw(bytes)),
                        (StreamingInput::Event(event), Some(adapter)) => {
                            adapter.push_event(event).map(StreamingOutput::Events)
                        }
                        _ => Err(GatewayError::InvalidUpstreamResponse),
                    };
                    match output {
                        Ok(StreamingOutput::Events(events)) if events.is_empty() => {}
                        Ok(output) => permit.send(output),
                        Err(error) => {
                            fail_response_stream(
                                &pump_failure,
                                &health_config,
                                &mut guard,
                                StreamFailure::upstream(error.to_string()),
                            );
                            return;
                        }
                    }
                }
                Some(Err(error)) => {
                    fail_response_stream(
                        &pump_failure,
                        &health_config,
                        &mut guard,
                        StreamFailure::upstream(error),
                    );
                    return;
                }
                None => {
                    if let Some(adapter) = stream_adapter.as_mut() {
                        match adapter.finish() {
                            Ok(events) if !events.is_empty() => {
                                permit.send(StreamingOutput::Events(events));
                            }
                            Ok(_) => {}
                            Err(error) => {
                                fail_response_stream(
                                    &pump_failure,
                                    &health_config,
                                    &mut guard,
                                    StreamFailure::upstream(error.to_string()),
                                );
                                return;
                            }
                        }
                    }
                    if record_prefix && !indexed {
                        prefix_directory.record(&stream_node_id, &prefix_input);
                    }
                    guard.completed();
                    return;
                }
            }
        }
    });

    let body = if rewrites_body {
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
    };
    let mut response = Response::new(body);
    *response.status_mut() = status;
    copy_response_headers(&headers, response.headers_mut());
    if rewrites_body {
        response.headers_mut().remove(CONTENT_LENGTH);
    }
    if is_anthropic {
        anthropic::set_anthropic_content_type(&mut response, true);
    }
    set_thinking_budget_warning(thinking_budget_approximated, response.headers_mut());
    if let Some(value) = upstream_request_id {
        response
            .headers_mut()
            .insert(HeaderName::from_static("x-upstream-request-id"), value);
    }
    if expose_node_header && let Ok(value) = HeaderValue::from_str(&node_id) {
        response
            .headers_mut()
            .insert(HeaderName::from_static("x-gateway-node"), value);
    }
    if headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("text/event-stream"))
    {
        response.headers_mut().insert(
            HeaderName::from_static("x-accel-buffering"),
            HeaderValue::from_static("no"),
        );
    }
    response
}

#[derive(Debug)]
pub(super) struct StreamFailure {
    pub(super) kind: io::ErrorKind,
    pub(super) message: String,
}

impl StreamFailure {
    pub(super) fn timed_out(message: &'static str) -> Self {
        Self {
            kind: io::ErrorKind::TimedOut,
            message: message.to_owned(),
        }
    }

    pub(super) fn upstream(message: String) -> Self {
        Self {
            kind: io::ErrorKind::Other,
            message,
        }
    }

    pub(super) fn into_io_error(self) -> io::Error {
        io::Error::new(self.kind, self.message)
    }
}

pub(super) fn fail_response_stream(
    terminal_failure: &parking_lot::Mutex<Option<StreamFailure>>,
    health_config: &crate::config::HealthConfig,
    guard: &mut BodyGuard,
    failure: StreamFailure,
) {
    guard.failed(failure.message.clone(), health_config);
    *terminal_failure.lock() = Some(failure);
}

pub(super) struct BodyGuard {
    pub(super) lease: Option<NodeLease>,
    pub(super) metrics: Arc<Metrics>,
    pub(super) node_id: String,
    pub(super) header_latency: Duration,
    pub(super) upstream_started: Instant,
    pub(super) observation: crate::inference_stats::StreamObservation,
    pub(super) first_token_observed: bool,
    pub(super) terminal: bool,
    pub(super) log_attempt: Option<crate::session_log::AttemptGuard>,
    anthropic_usage: bool,
}

impl BodyGuard {
    pub(super) fn new(
        lease: NodeLease,
        metrics: Arc<Metrics>,
        node_id: String,
        header_latency: Duration,
        upstream_started: Instant,
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
            log_attempt: None,
            anthropic_usage: false,
        }
    }

    pub(super) fn completed(&mut self) {
        if self.terminal {
            return;
        }
        if let Some(log) = &mut self.log_attempt {
            log.update(|attempt| {
                attempt.timings_us.insert(
                    "upstream_done".to_owned(),
                    crate::session_log::micros(self.upstream_started.elapsed()),
                );
                attempt.usage = self.observation.usage.log_value(self.anthropic_usage);
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
        self.terminal = true;
    }

    pub(super) fn observe(&mut self, input: &StreamingInput) {
        if let Some(log) = &self.log_attempt {
            let elapsed = crate::session_log::micros(self.upstream_started.elapsed());
            log.update(|attempt| {
                attempt
                    .timings_us
                    .entry("first_chunk".to_owned())
                    .or_insert(elapsed);
            });
            match input {
                StreamingInput::Raw(bytes) => log.capture(bytes, true, self.anthropic_usage),
                StreamingInput::Event(event) => {
                    for line in event.data().split('\n') {
                        log.capture(b"data: ", true, self.anthropic_usage);
                        log.capture(line.as_bytes(), true, self.anthropic_usage);
                        log.capture(b"\n", true, self.anthropic_usage);
                    }
                    log.capture(b"\n", true, self.anthropic_usage);
                }
            }
        }
        match input {
            StreamingInput::Raw(bytes) => self.observation.observe_bytes(bytes),
            StreamingInput::Event(event) => self.observation.observe_json(event.data().as_bytes()),
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

    pub(super) fn failed(&mut self, message: String, health_config: &crate::config::HealthConfig) {
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
            self.metrics.stream_cancelled(&self.node_id);
        }
    }
}
