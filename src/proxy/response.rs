use std::{io, sync::Arc, time::Duration};

use axum::{
    Json,
    body::Body,
    http::{HeaderName, HeaderValue, StatusCode, header::CONTENT_LENGTH},
    response::{IntoResponse, Response},
};
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use serde_json::{Value, json};

use crate::{
    anthropic, chat_bridge,
    error::GatewayError,
    metrics::Metrics,
    node::{Node, NodeLease},
    prefix::PrefixInput,
    response_buffer::{ResponseBufferBudget, ResponseBufferReservation},
};

use super::ClientProtocol;
use super::headers::copy_response_headers;
use super::{MAX_ERROR_BODY_BYTES, UpstreamResponseMode};

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(super) async fn buffered_success_response(
    upstream: reqwest::Response,
    lease: &NodeLease,
    node: &Node,
    prefix_directory: &crate::prefix::PrefixDirectory,
    prefix_input: &PrefixInput,
    health_config: &crate::config::HealthConfig,
    header_latency: Duration,
    stream_idle_timeout: Duration,
    upstream_body_timeout: Duration,
    max_body_bytes: usize,
    response_buffer: Arc<ResponseBufferBudget>,
    expose_node_header: bool,
    response_mode: &UpstreamResponseMode,
    public_model: &str,
    record_prefix: bool,
    metrics: &Metrics,
    mut log_attempt: Option<&mut crate::session_log::AttemptGuard>,
) -> Result<Response, GatewayError> {
    let status = upstream.status();
    let headers = upstream.headers().clone();
    let upstream_request_id = headers.get("x-request-id").cloned();
    let buffered = match read_limited(
        upstream,
        max_body_bytes,
        response_buffer,
        stream_idle_timeout,
        upstream_body_timeout,
    )
    .await
    {
        Ok(body) => body,
        Err(error) => {
            lease.record_failure(
                format!("non-streaming upstream body failed: {error}"),
                health_config,
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
    let body = match response_mode {
        UpstreamResponseMode::Passthrough => Ok(upstream_body.clone()),
        UpstreamResponseMode::ChatBridge { context } => {
            chat_bridge::convert_response(&upstream_body, context, public_model).await
        }
        UpstreamResponseMode::TokenCount => (|| {
            let value: Value = serde_json::from_slice(&upstream_body)
                .map_err(|_| GatewayError::InvalidUpstreamResponse)?;
            let tokens = value["token_ids"]
                .as_array()
                .or_else(|| value["tokens"].as_array())
                .ok_or(GatewayError::InvalidUpstreamResponse)?;
            if tokens.iter().any(|token| token.as_u64().is_none()) {
                return Err(GatewayError::InvalidUpstreamResponse);
            }
            let count = tokens.len();
            Ok(Bytes::from(
                serde_json::to_vec(&json!({"input_tokens":count}))
                    .map_err(|_| GatewayError::Internal)?,
            ))
        })(),
    };
    let body = match body {
        Ok(body) => body,
        Err(error) => {
            lease.record_failure(
                "response adapter received an invalid upstream body",
                health_config,
            );
            return Err(error);
        }
    };
    if body.len() > max_body_bytes {
        lease.record_failure(
            "response adapter exceeded the configured non-streaming body limit",
            health_config,
        );
        return Err(GatewayError::InvalidUpstreamResponse);
    }
    let (usage, provider_timings) = crate::inference_stats::observe_response(&upstream_body);
    if let Some(tokens) = usage.output_tokens {
        lease.record_output_tokens(tokens);
    }
    metrics.observe_usage(usage);
    if let Some(telemetry) = &lease.observation {
        telemetry.upstream(
            crate::observability::Outcome::Success,
            usage,
            provider_timings.mean_itl_seconds(),
            lease.generations(),
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
    lease.record_success(header_latency);
    if record_prefix {
        prefix_directory.record(node.id(), prefix_input);
    }
    let mut response = Response::new(budgeted_body(body, reservation));
    *response.status_mut() = status;
    copy_response_headers(&headers, response.headers_mut());
    if response_mode.rewrites_body() {
        response.headers_mut().remove(CONTENT_LENGTH);
    }
    if response_mode.is_anthropic() {
        anthropic::set_anthropic_content_type(&mut response, false);
    }
    if let Some(value) = upstream_request_id {
        response
            .headers_mut()
            .insert(HeaderName::from_static("x-upstream-request-id"), value);
    }
    if expose_node_header && let Ok(value) = HeaderValue::from_str(node.id()) {
        response
            .headers_mut()
            .insert(HeaderName::from_static("x-gateway-node"), value);
    }
    Ok(response)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn proxy_error_response(
    upstream: reqwest::Response,
    _lease: NodeLease,
    response_buffer: Arc<ResponseBufferBudget>,
    stream_idle_timeout: Duration,
    upstream_body_timeout: Duration,
    client_protocol: ClientProtocol,
    request_id: &str,
    mut log_attempt: Option<crate::session_log::AttemptGuard>,
) -> Result<Response, GatewayError> {
    let status = upstream.status();
    let headers = upstream.headers().clone();
    if !status.is_client_error() && !status.is_server_error() {
        return Err(GatewayError::InvalidUpstreamResponse);
    }
    let buffered = read_limited(
        upstream,
        MAX_ERROR_BODY_BYTES,
        response_buffer,
        stream_idle_timeout,
        upstream_body_timeout,
    )
    .await?;
    let body = &buffered.bytes;
    if let Some(log) = &mut log_attempt {
        log.capture(body, false);
        log.terminal_error("upstream", "upstream_status");
        log.finish("error", Some("upstream_status"));
    }
    if client_protocol == ClientProtocol::Anthropic {
        let mut response = anthropic::convert_error_response(status, body, request_id);
        if let Some(value) = headers.get("retry-after") {
            response.headers_mut().insert("retry-after", value.clone());
        }
        return Ok(hold_response_buffer(response, buffered.reservation));
    }
    let parsed_error = serde_json::from_slice::<Value>(body).ok();
    let valid_openai_error = parsed_error
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
    if let Some(value) = parsed_error.filter(|value| value["type"] == "error") {
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
        let mut response = (
            status,
            Json(json!({"error": {
                "message": error["message"].as_str().unwrap_or("upstream returned an error"),
                "type": error_type,
                "param": null,
                "code": error["type"].as_str().unwrap_or("upstream_error"),
            }})),
        )
            .into_response();
        if let Some(value) = headers.get("retry-after") {
            response.headers_mut().insert("retry-after", value.clone());
        }
        return Ok(hold_response_buffer(response, buffered.reservation));
    }
    let mut reservation = buffered.reservation;
    reservation.shrink_to(body.len());
    let mut response = Response::new(budgeted_body(body.clone(), reservation));
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
                // buffer only when the upstream body actually spans chunks.
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
    let mut output = BodyBuffer::default();
    let mut stream = response.bytes_stream();
    let body_deadline = tokio::time::Instant::now() + upstream_body_timeout;
    loop {
        let Ok(Ok(item)) = tokio::time::timeout_at(
            body_deadline,
            tokio::time::timeout(stream_idle_timeout, stream.next()),
        )
        .await
        else {
            return Err(GatewayError::InvalidUpstreamResponse);
        };
        let Some(item) = item else {
            break;
        };
        let bytes = item.map_err(|_| GatewayError::InvalidUpstreamResponse)?;
        if output.len().saturating_add(bytes.len()) > limit {
            return Err(GatewayError::InvalidUpstreamResponse);
        }
        output.push(bytes, length_hint);
    }
    Ok(BufferedUpstreamBody {
        bytes: output.freeze(),
        reservation,
    })
}

pub(super) fn budgeted_body(body: Bytes, reservation: ResponseBufferReservation) -> Body {
    Body::from_stream(async_stream::stream! {
        let _reservation = reservation;
        yield Ok::<Bytes, io::Error>(body);
    })
}

pub(super) fn hold_response_buffer(
    mut response: Response,
    reservation: ResponseBufferReservation,
) -> Response {
    let body = std::mem::replace(response.body_mut(), Body::empty());
    let mut stream = body.into_data_stream();
    *response.body_mut() = Body::from_stream(async_stream::stream! {
        let _reservation = reservation;
        while let Some(item) = stream.next().await {
            yield item;
        }
    });
    response
}

#[cfg(test)]
mod tests {
    use futures_util::stream;

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
