use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use axum::{
    body::{Body, HttpBody as _},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use bytes::{Bytes, BytesMut};
use futures_util::{Stream, StreamExt};
use serde_json::{Value, json};

use crate::{
    anthropic, chat_bridge,
    error::{GatewayError, openai_error_response},
    inference_stats::{ProviderTimings, Usage, observe_response, observe_response_value},
    node::{Node, NodeLease},
    prefix::PrefixInput,
    response_buffer::{ResponseBufferBudget, ResponseBufferReservation},
    server::AppState,
};

use super::headers::{apply_upstream_response_headers, copy_response_headers};
use super::{ClientProtocol, MAX_ERROR_BODY_BYTES, UpstreamResponseMode};

pub(super) struct BufferedContext<'a> {
    pub(super) state: &'a AppState,
    pub(super) lease: &'a NodeLease,
    pub(super) node: &'a Node,
    pub(super) prefix_input: &'a PrefixInput,
    pub(super) header_latency: Duration,
    pub(super) response_mode: &'a UpstreamResponseMode,
    pub(super) public_model: &'a str,
    pub(super) record_prefix: bool,
}

pub(super) async fn buffered_success_response(
    upstream: reqwest::Response,
    ctx: &BufferedContext<'_>,
    mut log_attempt: Option<&mut crate::session_log::AttemptGuard>,
) -> Result<Response, GatewayError> {
    let status = upstream.status();
    let headers = upstream.headers().clone();
    let settings = &ctx.state.settings;
    let max_body_bytes = settings.server.max_non_streaming_response_bytes;
    let buffered = match read_limited(
        upstream,
        max_body_bytes,
        Arc::clone(&ctx.state.response_buffer),
        Duration::from_millis(settings.server.stream_idle_timeout_ms),
        Duration::from_millis(settings.server.upstream_body_timeout_ms),
    )
    .await
    {
        Ok(body) => body,
        Err(error) => {
            ctx.lease.record_failure(
                format!("non-streaming upstream body failed: {error}"),
                &settings.health,
            );
            return Err(error);
        }
    };
    let BufferedUpstreamBody {
        bytes: upstream_body,
        mut reservation,
    } = buffered;
    if let Some(log) = &log_attempt {
        log.capture(&upstream_body, false);
    }
    let (body, (usage, provider_timings)) =
        match convert_buffered_body(&upstream_body, ctx.response_mode, ctx.public_model).await {
            Ok(result) => result,
            Err(error) => {
                ctx.lease.record_failure(
                    "response adapter received an invalid upstream body",
                    &settings.health,
                );
                return Err(error);
            }
        };
    if body.len() > max_body_bytes {
        ctx.lease.record_failure(
            "response adapter exceeded the configured non-streaming body limit",
            &settings.health,
        );
        return Err(GatewayError::InvalidUpstreamResponse);
    }
    if let Some(tokens) = usage.output_tokens {
        ctx.lease.record_output_tokens(tokens);
    }
    ctx.state.metrics.observe_usage(usage);
    if let Some(telemetry) = &ctx.lease.observation {
        telemetry.upstream(
            crate::observability::Outcome::Success,
            usage,
            provider_timings.mean_itl_seconds(),
            ctx.lease.generations(),
        );
    }
    if let Some(log) = log_attempt.as_mut() {
        log.update(|attempt| {
            provider_timings.write_to(&mut attempt.timings_us);
            attempt.usage = usage.log_value();
        });
        log.finish("success", None);
    }
    drop(upstream_body);
    reservation.shrink_to(body.len());
    ctx.lease.record_success(ctx.header_latency);
    if ctx.record_prefix {
        ctx.state
            .scheduler
            .prefix_directory()
            .record_for_node(ctx.node, ctx.prefix_input);
    }
    let mut response = Response::new(budgeted_body(body, reservation));
    apply_upstream_response_headers(
        &mut response,
        status,
        &headers,
        ctx.node.id(),
        settings.server.expose_node_header,
        ctx.response_mode,
        false,
    );
    Ok(response)
}

async fn convert_buffered_body(
    upstream_body: &Bytes,
    response_mode: &UpstreamResponseMode,
    public_model: &str,
) -> Result<(Bytes, (Usage, ProviderTimings)), GatewayError> {
    match response_mode {
        UpstreamResponseMode::Passthrough => {
            Ok((upstream_body.clone(), observe_response(upstream_body)))
        }
        UpstreamResponseMode::ChatBridge { context } => {
            let value: Value = serde_json::from_slice(upstream_body)
                .map_err(|_| GatewayError::InvalidUpstreamResponse)?;
            let observed = observe_response_value(&value);
            let body = chat_bridge::convert_response_value(&value, context, public_model).await?;
            Ok((body, observed))
        }
        UpstreamResponseMode::TokenCount => Ok((
            parse_token_count(upstream_body)?,
            (Usage::default(), ProviderTimings::default()),
        )),
    }
}

fn parse_token_count(body: &[u8]) -> Result<Bytes, GatewayError> {
    let value: Value =
        serde_json::from_slice(body).map_err(|_| GatewayError::InvalidUpstreamResponse)?;
    let tokens = value["token_ids"]
        .as_array()
        .or_else(|| value["tokens"].as_array())
        .ok_or(GatewayError::InvalidUpstreamResponse)?;
    if tokens.iter().any(|token| token.as_u64().is_none()) {
        return Err(GatewayError::InvalidUpstreamResponse);
    }
    serde_json::to_vec(&json!({"input_tokens": tokens.len()}))
        .map(Bytes::from)
        .map_err(|_| GatewayError::Internal)
}

pub(super) async fn proxy_error_response(
    upstream: reqwest::Response,
    _lease: NodeLease,
    state: &AppState,
    client_protocol: ClientProtocol,
    request_id: &str,
    mut log_attempt: Option<crate::session_log::AttemptGuard>,
) -> Result<Response, GatewayError> {
    let status = upstream.status();
    let headers = upstream.headers().clone();
    if !status.is_client_error() && !status.is_server_error() {
        return Err(GatewayError::InvalidUpstreamResponse);
    }
    let BufferedUpstreamBody {
        bytes: body,
        mut reservation,
    } = read_limited(
        upstream,
        MAX_ERROR_BODY_BYTES,
        Arc::clone(&state.response_buffer),
        Duration::from_millis(state.settings.server.stream_idle_timeout_ms),
        Duration::from_millis(state.settings.server.upstream_body_timeout_ms),
    )
    .await?;
    if let Some(log) = &mut log_attempt {
        log.capture(&body, false);
        log.terminal_error("upstream", "upstream_status");
        log.finish("error", Some("upstream_status"));
    }
    let converted = if client_protocol == ClientProtocol::Anthropic {
        Some(anthropic::convert_error_response(status, &body, request_id))
    } else {
        let parsed = serde_json::from_slice::<Value>(&body).ok();
        let valid_openai_error = parsed
            .as_ref()
            .and_then(|value| value.get("error"))
            .is_some_and(Value::is_object);
        if !valid_openai_error {
            let mut response = GatewayError::UpstreamStatus(status.as_u16()).into_response();
            if let Some(value) = headers.get("retry-after") {
                response.headers_mut().insert("retry-after", value.clone());
            }
            return Ok(response);
        }
        parsed
            .filter(|value| value["type"] == "error")
            .map(|value| {
                // Anthropic's error object also passes the OpenAI object check above.
                // Preserve its diagnostic without leaking its client-protocol envelope.
                let error = &value["error"];
                let error_type = match status {
                    StatusCode::TOO_MANY_REQUESTS => "rate_limit_error",
                    StatusCode::UNAUTHORIZED => "authentication_error",
                    StatusCode::FORBIDDEN => "permission_error",
                    _ if status.is_client_error() => "invalid_request_error",
                    _ => "api_error",
                };
                openai_error_response(
                    status,
                    error_type,
                    error["type"].as_str().unwrap_or("upstream_error"),
                    error["message"]
                        .as_str()
                        .unwrap_or("upstream returned an error")
                        .to_owned(),
                )
            })
    };
    if let Some(mut response) = converted {
        if let Some(value) = headers.get("retry-after") {
            response.headers_mut().insert("retry-after", value.clone());
        }
        let actual =
            usize::try_from(response.body().size_hint().exact().unwrap_or(0)).unwrap_or(usize::MAX);
        reservation.shrink_to(actual);
        return Ok(hold_response_buffer(response, reservation));
    }
    reservation.shrink_to(body.len());
    let mut response = Response::new(budgeted_body(body, reservation));
    *response.status_mut() = status;
    copy_response_headers(&headers, response.headers_mut());
    Ok(response)
}

pub(super) struct BufferedUpstreamBody {
    pub(super) bytes: Bytes,
    pub(super) reservation: ResponseBufferReservation,
}

#[derive(Default)]
enum BodyBuffer {
    #[default]
    Empty,
    Single(Bytes),
    Multiple(BytesMut),
}

impl BodyBuffer {
    fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Single(bytes) => bytes.len(),
            Self::Multiple(bytes) => bytes.len(),
        }
    }

    fn push(&mut self, bytes: Bytes, length_hint: usize) {
        if bytes.is_empty() {
            return;
        }
        match self {
            Self::Empty => *self = Self::Single(bytes),
            Self::Single(first) => {
                // Keep a single chunk without copying. Allocate a contiguous
                // buffer only when the body actually spans chunks.
                let mut output =
                    BytesMut::with_capacity(length_hint.max(first.len() + bytes.len()));
                output.extend_from_slice(first);
                output.extend_from_slice(&bytes);
                *self = Self::Multiple(output);
            }
            Self::Multiple(output) => output.extend_from_slice(&bytes),
        }
    }

    fn freeze(self) -> Bytes {
        match self {
            Self::Empty => Bytes::new(),
            Self::Single(bytes) => bytes,
            Self::Multiple(bytes) => bytes.freeze(),
        }
    }
}

pub(super) struct BodyReadLimits {
    pub(super) limit: usize,
    pub(super) length_hint: usize,
    pub(super) idle_timeout: Duration,
    pub(super) total_timeout: Duration,
}

pub(super) async fn read_body_stream<S, E>(
    mut stream: S,
    limits: BodyReadLimits,
    on_timeout: impl Fn() -> GatewayError,
    on_read_error: impl Fn() -> GatewayError,
    on_too_large: impl Fn() -> GatewayError,
) -> Result<Bytes, GatewayError>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
{
    let mut output = BodyBuffer::default();
    let deadline = tokio::time::Instant::now() + limits.total_timeout;
    loop {
        let Ok(Ok(item)) = tokio::time::timeout_at(
            deadline,
            tokio::time::timeout(limits.idle_timeout, stream.next()),
        )
        .await
        else {
            return Err(on_timeout());
        };
        let Some(item) = item else {
            break;
        };
        let bytes = item.map_err(|_| on_read_error())?;
        if output.len().saturating_add(bytes.len()) > limits.limit {
            return Err(on_too_large());
        }
        output.push(bytes, limits.length_hint);
    }
    Ok(output.freeze())
}

pub(super) async fn read_limited(
    response: reqwest::Response,
    limit: usize,
    response_buffer: Arc<ResponseBufferBudget>,
    stream_idle_timeout: Duration,
    upstream_body_timeout: Duration,
) -> Result<BufferedUpstreamBody, GatewayError> {
    let length_hint = response.content_length();
    if length_hint.is_some_and(|length| length > limit as u64) {
        return Err(GatewayError::InvalidUpstreamResponse);
    }
    let length_hint = length_hint
        .and_then(|length| usize::try_from(length).ok())
        .unwrap_or_default();
    let reservation = response_buffer.reserve(limit).await;
    let bytes = read_body_stream(
        response.bytes_stream(),
        BodyReadLimits {
            limit,
            length_hint,
            idle_timeout: stream_idle_timeout,
            total_timeout: upstream_body_timeout,
        },
        || GatewayError::InvalidUpstreamResponse,
        || GatewayError::InvalidUpstreamResponse,
        || GatewayError::InvalidUpstreamResponse,
    )
    .await?;
    Ok(BufferedUpstreamBody { bytes, reservation })
}

struct HeldStream<S> {
    inner: S,
    _reservation: ResponseBufferReservation,
}

impl<S: Stream + Unpin> Stream for HeldStream<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().inner).poll_next(cx)
    }
}

pub(super) fn budgeted_body(body: Bytes, reservation: ResponseBufferReservation) -> Body {
    Body::from_stream(HeldStream {
        inner: futures_util::stream::once(std::future::ready(Ok::<Bytes, io::Error>(body))),
        _reservation: reservation,
    })
}

pub(super) fn hold_response_buffer(
    mut response: Response,
    reservation: ResponseBufferReservation,
) -> Response {
    let body = std::mem::replace(response.body_mut(), Body::empty());
    *response.body_mut() = Body::from_stream(HeldStream {
        inner: body.into_data_stream(),
        _reservation: reservation,
    });
    response
}

#[cfg(test)]
mod tests {
    use futures_util::stream;

    use crate::metrics::Metrics;

    use super::*;

    async fn read_body(
        response: reqwest::Response,
        limit: usize,
        budget: Arc<ResponseBufferBudget>,
    ) -> Result<BufferedUpstreamBody, GatewayError> {
        read_limited(
            response,
            limit,
            budget,
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .await
    }

    #[tokio::test]
    async fn single_chunk_body_preserves_upstream_storage() {
        let bytes = Bytes::from(vec![b'x'; 64 * 1024]);
        let upstream_pointer = bytes.as_ptr();
        let response = reqwest::Response::from(http::Response::new(bytes.clone()));
        let budget = ResponseBufferBudget::new(bytes.len(), Metrics::new());
        let body = read_body(response, bytes.len(), Arc::clone(&budget))
            .await
            .unwrap();

        assert_eq!(body.bytes, bytes);
        assert_eq!(body.bytes.as_ptr(), upstream_pointer);
        assert_eq!(budget.snapshot().used_bytes, bytes.len());
        drop(body);
        assert_eq!(budget.snapshot().used_bytes, 0);
    }

    #[tokio::test]
    async fn fragmented_body_handles_empty_chunks_and_enforces_limit() {
        for limit in [3, 4] {
            let chunks = [
                Bytes::new(),
                Bytes::from_static(b"ab"),
                Bytes::new(),
                Bytes::from_static(b"cd"),
                Bytes::new(),
            ];
            let response = reqwest::Response::from(http::Response::new(
                reqwest::Body::wrap_stream(stream::iter(chunks.map(Ok::<_, io::Error>))),
            ));
            let budget = ResponseBufferBudget::new(limit, Metrics::new());
            let result = read_body(response, limit, Arc::clone(&budget)).await;
            if limit == 4 {
                assert_eq!(result.as_ref().unwrap().bytes, b"abcd"[..]);
            } else {
                assert!(matches!(result, Err(GatewayError::InvalidUpstreamResponse)));
            }
            drop(result);
            assert_eq!(budget.snapshot().used_bytes, 0);
        }
    }

    #[tokio::test]
    async fn empty_body_and_transport_failure_release_reservations() {
        let budget = ResponseBufferBudget::new(16, Metrics::new());
        let empty = reqwest::Response::from(http::Response::new(Bytes::new()));
        let body = read_body(empty, 16, Arc::clone(&budget)).await.unwrap();
        assert!(body.bytes.is_empty());
        drop(body);
        assert_eq!(budget.snapshot().used_bytes, 0);

        let failed = reqwest::Response::from(http::Response::new(reqwest::Body::wrap_stream(
            stream::iter([
                Ok(Bytes::from_static(b"first")),
                Err(io::Error::other("upstream disconnected")),
            ]),
        )));
        assert!(matches!(
            read_body(failed, 16, Arc::clone(&budget)).await,
            Err(GatewayError::InvalidUpstreamResponse)
        ));
        assert_eq!(budget.snapshot().used_bytes, 0);
    }
}
