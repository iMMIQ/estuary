use std::{
    collections::HashSet,
    io,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    Json,
    body::Body,
    extract::{Extension, Path, State},
    http::{
        HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri,
        header::{CONTENT_LENGTH, CONTENT_TYPE},
    },
    response::{IntoResponse, Response},
};
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::{
    anthropic,
    config::ProviderKind,
    error::GatewayError,
    metrics::Metrics,
    node::{Node, NodeLease},
    prefix::{PrefixInput, routing_text},
    server::{AppState, RequestId},
};

const MAX_ERROR_BODY_BYTES: usize = 1024 * 1024;

#[derive(Serialize)]
pub(crate) struct ModelList {
    object: &'static str,
    data: Vec<ModelObject>,
}

#[derive(Serialize)]
pub(crate) struct ModelObject {
    id: String,
    object: &'static str,
    created: u64,
    owned_by: &'static str,
}

pub(crate) async fn list_models(State(state): State<Arc<AppState>>) -> Json<ModelList> {
    Json(ModelList {
        object: "list",
        data: state
            .scheduler
            .models()
            .into_iter()
            .map(model_object)
            .collect(),
    })
}

pub(crate) async fn get_model(
    State(state): State<Arc<AppState>>,
    Path(model): Path<String>,
) -> Result<Json<ModelObject>, GatewayError> {
    if state.scheduler.models().binary_search(&model).is_err() {
        return Err(GatewayError::UnknownModel(model));
    }
    Ok(Json(model_object(model)))
}

fn model_object(id: String) -> ModelObject {
    ModelObject {
        id,
        object: "model",
        created: 0,
        owned_by: "estuary",
    }
}

pub async fn proxy(
    State(state): State<Arc<AppState>>,
    Path(endpoint): Path<String>,
    Extension(request_id): Extension<RequestId>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let is_anthropic = matches!(endpoint.as_str(), "messages" | "messages/count_tokens");
    let gateway_request_id = request_id.0.clone();
    match proxy_inner(state, endpoint, request_id, method, uri, headers, body).await {
        Ok(response) => response,
        Err(error) if is_anthropic => anthropic::error_response(&error, &gateway_request_id),
        Err(error) => error.into_response(),
    }
}

#[allow(clippy::too_many_lines)]
async fn proxy_inner(
    state: Arc<AppState>,
    mut endpoint: String,
    request_id: RequestId,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, GatewayError> {
    if endpoint.starts_with("responses/") {
        return Err(GatewayError::UnsupportedFeature(
            "Responses retrieve, delete, and cancel endpoints require durable node affinity",
        ));
    }

    let is_inference_json = matches!(
        endpoint.as_str(),
        "chat/completions"
            | "responses"
            | "completions"
            | "embeddings"
            | "messages"
            | "messages/count_tokens"
    );
    if method != Method::POST || !is_inference_json {
        return Err(GatewayError::RouteNotFound);
    }
    let mut parsed = if body.is_empty() {
        None
    } else {
        serde_json::from_slice::<Value>(&body).ok()
    };
    if is_inference_json && parsed.is_none() {
        return Err(GatewayError::InvalidJson);
    }
    if endpoint == "responses" {
        reject_stateful_responses(parsed.as_ref())?;
    }
    let mut original_body = if parsed
        .as_mut()
        .is_some_and(strip_claude_code_billing_blocks)
    {
        serde_json::to_vec(parsed.as_ref().expect("parsed body exists"))
            .map(Bytes::from)
            .map_err(|_| GatewayError::Internal)?
    } else {
        body
    };

    let public_model = parsed
        .as_ref()
        .and_then(|value| value.get("model"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    if is_inference_json && public_model.is_none() {
        return Err(GatewayError::MissingModel);
    }

    let mut native_anthropic = None;
    let mut native_requirement_error = None;
    let (protocol, native_only, record_prefix) = if endpoint == "messages" {
        anthropic::validate_request_shape(
            parsed.as_ref().expect("inference JSON was validated above"),
            true,
        )?;
        native_anthropic = Some(NativeAnthropicPayload {
            endpoint: "messages".to_owned(),
            body: original_body.clone(),
            parsed: parsed
                .as_ref()
                .expect("inference JSON was validated above")
                .clone(),
            query: uri.query().map(str::to_owned),
        });
        let converted = anthropic::convert_request(
            parsed.as_ref().expect("inference JSON was validated above"),
        );
        match converted {
            Ok(converted) => {
                original_body = serde_json::to_vec(&converted)
                    .map(Bytes::from)
                    .map_err(|_| GatewayError::Internal)?;
                parsed = Some(converted);
                "chat/completions".clone_into(&mut endpoint);
                (ClientProtocol::Anthropic, false, true)
            }
            Err(error) => {
                native_requirement_error = Some(error);
                (ClientProtocol::Anthropic, true, true)
            }
        }
    } else if endpoint == "messages/count_tokens" {
        anthropic::validate_request_shape(
            parsed.as_ref().expect("inference JSON was validated above"),
            false,
        )?;
        (ClientProtocol::Anthropic, true, false)
    } else {
        (ClientProtocol::OpenAi, false, true)
    };
    if native_only
        && !state.scheduler.nodes().iter().any(|node| {
            node.provider().kind == ProviderKind::Vllm
                && node.upstream_model(public_model.as_deref()).is_some()
        })
    {
        return Err(
            native_requirement_error.unwrap_or(GatewayError::UnsupportedFeature(
                "Anthropic token counting requires a vLLM 0.25 or newer node",
            )),
        );
    }
    let routing_parsed = (endpoint == "messages/count_tokens")
        .then(|| {
            anthropic::convert_count_request(
                parsed.as_ref().expect("inference JSON was validated above"),
            )
            .ok()
        })
        .flatten();
    let streaming = parsed
        .as_ref()
        .and_then(|value| value.get("stream"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let mut prefix_input = routing_text(
        if protocol == ClientProtocol::Anthropic {
            "chat/completions"
        } else {
            &endpoint
        },
        public_model.as_deref(),
        routing_parsed.as_ref().or(parsed.as_ref()),
        &state.settings.routing.prefix,
    );
    if !native_only && let (Some(model), Some(parsed)) = (public_model.as_deref(), parsed.as_ref())
    {
        if let Some(tokens) = state
            .vllm
            .tokenize_for_routing(&state.client, &endpoint, model, parsed)
            .await
        {
            prefix_input.set_token_ids(tokens);
        }
    }

    let upstream_query =
        if protocol == ClientProtocol::OpenAi || endpoint == "messages/count_tokens" {
            uri.query().map(str::to_owned)
        } else {
            None
        };
    proxy_with_retries(
        state,
        ProxyRequest {
            endpoint,
            method,
            query: upstream_query,
            headers,
            original_body,
            parsed_body: parsed,
            public_model,
            prefix_input,
            streaming,
            request_id,
            client_protocol: protocol,
            native_anthropic,
            native_only,
            record_prefix,
        },
    )
    .await
}

const CLAUDE_CODE_BILLING_PREFIX: &str = "x-anthropic-billing-header:";

fn strip_claude_code_billing_blocks(body: &mut Value) -> bool {
    let Some(object) = body.as_object_mut() else {
        return false;
    };
    let mut changed = false;

    let remove_system = match object.get_mut("system") {
        Some(Value::String(text)) => is_claude_code_billing_text(text),
        Some(Value::Array(blocks)) => {
            let before = blocks.len();
            blocks.retain(|block| !is_claude_code_billing_block(block));
            changed |= blocks.len() != before;
            blocks.is_empty()
        }
        _ => false,
    };
    if remove_system {
        object.remove("system");
        changed = true;
    }

    if let Some(Value::Array(messages)) = object.get_mut("messages") {
        messages.retain_mut(|message| {
            if message.get("role").and_then(Value::as_str) != Some("system") {
                return true;
            }
            let Some(content) = message.get_mut("content") else {
                return true;
            };
            match content {
                Value::String(text) if is_claude_code_billing_text(text) => {
                    changed = true;
                    false
                }
                Value::Array(blocks) => {
                    let before = blocks.len();
                    blocks.retain(|block| !is_claude_code_billing_block(block));
                    changed |= blocks.len() != before;
                    !blocks.is_empty()
                }
                _ => true,
            }
        });
    }

    changed
}

fn is_claude_code_billing_block(value: &Value) -> bool {
    value.as_str().is_some_and(is_claude_code_billing_text)
        || value
            .as_object()
            .filter(|block| block.get("type").and_then(Value::as_str) == Some("text"))
            .and_then(|block| block.get("text"))
            .and_then(Value::as_str)
            .is_some_and(is_claude_code_billing_text)
}

fn is_claude_code_billing_text(text: &str) -> bool {
    let text = text.trim();
    text.starts_with(CLAUDE_CODE_BILLING_PREFIX) && !text.contains(['\r', '\n'])
}

fn reject_stateful_responses(body: Option<&Value>) -> Result<(), GatewayError> {
    let Some(body) = body else {
        return Ok(());
    };
    if body.get("background").and_then(Value::as_bool) == Some(true) {
        return Err(GatewayError::UnsupportedFeature(
            "Responses background mode is deferred to the durable-affinity phase",
        ));
    }
    if body
        .get("previous_response_id")
        .is_some_and(|value| !value.is_null())
    {
        return Err(GatewayError::UnsupportedFeature(
            "Responses previous_response_id requires durable node affinity",
        ));
    }
    if body
        .get("conversation")
        .is_some_and(|value| !value.is_null())
    {
        return Err(GatewayError::UnsupportedFeature(
            "Responses conversation state requires durable node affinity",
        ));
    }
    Ok(())
}

struct ProxyRequest {
    endpoint: String,
    method: Method,
    query: Option<String>,
    headers: HeaderMap,
    original_body: Bytes,
    parsed_body: Option<Value>,
    public_model: Option<String>,
    prefix_input: PrefixInput,
    streaming: bool,
    request_id: RequestId,
    client_protocol: ClientProtocol,
    native_anthropic: Option<NativeAnthropicPayload>,
    native_only: bool,
    record_prefix: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClientProtocol {
    OpenAi,
    Anthropic,
}

struct NativeAnthropicPayload {
    endpoint: String,
    body: Bytes,
    parsed: Value,
    query: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UpstreamResponseMode {
    Passthrough,
    ConvertToAnthropic,
    NativeAnthropic,
}

enum ResponseStreamAdapter {
    Converted(anthropic::StreamConverter),
    Native(anthropic::NativeStreamRewriter),
}

impl ResponseStreamAdapter {
    fn push(&mut self, bytes: &[u8]) -> Result<Bytes, GatewayError> {
        match self {
            Self::Converted(converter) => converter.push(bytes),
            Self::Native(rewriter) => rewriter.push(bytes),
        }
    }

    fn finish(&mut self) -> Result<Bytes, GatewayError> {
        match self {
            Self::Converted(converter) => converter.finish(),
            Self::Native(rewriter) => rewriter.finish(),
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn proxy_with_retries(
    state: Arc<AppState>,
    request: ProxyRequest,
) -> Result<Response, GatewayError> {
    let mut excluded = if request.native_only {
        state
            .scheduler
            .nodes()
            .into_iter()
            .filter(|node| node.provider().kind != ProviderKind::Vllm)
            .map(|node| node.id().to_owned())
            .collect()
    } else {
        HashSet::new()
    };
    let mut attempt = 0usize;
    loop {
        attempt += 1;
        let queue_started = Instant::now();
        let selection = state
            .scheduler
            .acquire(
                request.public_model.as_deref(),
                request.prefix_input.clone(),
                &excluded,
                request.original_body.len(),
            )
            .await;
        state
            .metrics
            .observe_queue_duration(queue_started.elapsed().as_secs_f64());
        let selection = selection?;
        state
            .metrics
            .observe_prefix_match(selection.prefix_match_chars);
        state
            .metrics
            .observe_prefix_match_tokens(selection.prefix_match_tokens);

        let node = Arc::clone(&selection.node);
        let use_native_anthropic = request.native_only
            || (request.native_anthropic.is_some() && node.provider().kind == ProviderKind::Vllm);
        let (upstream_endpoint, upstream_original, upstream_parsed, upstream_query) =
            if use_native_anthropic && !request.native_only {
                let payload = request
                    .native_anthropic
                    .as_ref()
                    .expect("native Anthropic payload was checked above");
                (
                    payload.endpoint.as_str(),
                    &payload.body,
                    Some(&payload.parsed),
                    payload.query.as_deref(),
                )
            } else {
                (
                    request.endpoint.as_str(),
                    &request.original_body,
                    request.parsed_body.as_ref(),
                    request.query.as_deref(),
                )
            };
        let response_mode = match request.client_protocol {
            ClientProtocol::OpenAi => UpstreamResponseMode::Passthrough,
            ClientProtocol::Anthropic if use_native_anthropic => {
                UpstreamResponseMode::NativeAnthropic
            }
            ClientProtocol::Anthropic => UpstreamResponseMode::ConvertToAnthropic,
        };
        let upstream_url = node
            .upstream_url(upstream_endpoint, upstream_query)
            .map_err(|error| {
                warn!(node = node.id(), error = %error, "failed to build upstream URL");
                GatewayError::Internal
            })?;
        let upstream_body = mapped_body(
            upstream_original,
            upstream_parsed,
            selection.upstream_model.as_deref(),
            request.public_model.as_deref(),
        )?;
        let mut upstream_headers = HeaderMap::new();
        let connection_headers = connection_header_names(&request.headers);
        for (name, value) in &request.headers {
            if should_forward_request_header(name) && !connection_headers.contains(name) {
                upstream_headers.append(name, value.clone());
            }
        }
        for (name, value) in node.headers() {
            upstream_headers.insert(name, value.clone());
        }
        if let Ok(value) = HeaderValue::from_str(&request.request_id.0) {
            upstream_headers.insert(HeaderName::from_static("x-gateway-request-id"), value);
        }
        let upstream_request = state
            .client
            .request(request.method.clone(), upstream_url)
            .headers(upstream_headers)
            .body(upstream_body);

        let upstream_started = Instant::now();
        let result = tokio::time::timeout(
            Duration::from_millis(state.settings.server.upstream_header_timeout_ms),
            upstream_request.send(),
        )
        .await;
        let response = match result {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                let retryable = error.is_connect()
                    && attempt < state.settings.retry.max_attempts
                    && has_untried_alternative(
                        &state,
                        request.public_model.as_deref(),
                        &excluded,
                        node.id(),
                    );
                selection
                    .lease
                    .record_failure(error.to_string(), &state.settings.health);
                state.metrics.attempt(node.id(), "transport_error");
                if retryable {
                    state.metrics.retry(node.id(), "connect_error");
                    excluded.insert(node.id().to_owned());
                    drop(selection.lease);
                    continue;
                }
                warn!(node = node.id(), error = %error, "upstream request failed");
                return Err(GatewayError::Upstream("transport failure".to_owned()));
            }
            Err(_) => {
                selection
                    .lease
                    .record_failure("upstream response header timeout", &state.settings.health);
                state.metrics.attempt(node.id(), "header_timeout");
                return Err(GatewayError::UpstreamTimeout);
            }
        };

        let status = response.status();
        let header_latency = upstream_started.elapsed();
        let configured_retry_status = state.settings.retry.statuses.contains(&status.as_u16());
        let retryable_status = configured_retry_status
            && attempt < state.settings.retry.max_attempts
            && has_untried_alternative(
                &state,
                request.public_model.as_deref(),
                &excluded,
                node.id(),
            );
        if status == StatusCode::TOO_MANY_REQUESTS
            || (configured_retry_status && !status.is_server_error())
        {
            selection.lease.record_overload();
        } else if status.is_server_error() {
            selection.lease.record_failure(
                format!("upstream returned {status}"),
                &state.settings.health,
            );
        }
        state.metrics.attempt(node.id(), status.as_str());

        if retryable_status {
            state.metrics.retry(node.id(), status.as_str());
            excluded.insert(node.id().to_owned());
            drop(response);
            drop(selection.lease);
            continue;
        }

        debug!(
            node = node.id(),
            score = selection.score,
            prefix_match_chars = selection.prefix_match_chars,
            status = %status,
            "upstream selected"
        );
        let stream_idle_timeout =
            Duration::from_millis(state.settings.server.stream_idle_timeout_ms);
        let upstream_body_timeout =
            Duration::from_millis(state.settings.server.upstream_body_timeout_ms);
        if !status.is_success() {
            if !status.is_server_error() && status != StatusCode::TOO_MANY_REQUESTS {
                selection.lease.record_success(header_latency);
            }
            return proxy_error_response(
                response,
                selection.lease,
                stream_idle_timeout,
                upstream_body_timeout,
                request.client_protocol,
                &request.request_id.0,
            )
            .await;
        }
        if !request.streaming {
            let buffered = buffered_success_response(
                response,
                &selection.lease,
                &node,
                state.scheduler.prefix_directory(),
                &request.prefix_input,
                &state.settings.health,
                header_latency,
                stream_idle_timeout,
                upstream_body_timeout,
                state.settings.server.max_non_streaming_response_bytes,
                state.settings.server.expose_node_header,
                response_mode,
                request.public_model.as_deref().unwrap_or_default(),
                request.record_prefix,
            )
            .await;
            match buffered {
                Ok(response) => return Ok(response),
                Err(error) => {
                    let retryable = attempt < state.settings.retry.max_attempts
                        && has_untried_alternative(
                            &state,
                            request.public_model.as_deref(),
                            &excluded,
                            node.id(),
                        );
                    if retryable {
                        state.metrics.retry(node.id(), "body_error");
                        excluded.insert(node.id().to_owned());
                        drop(selection.lease);
                        continue;
                    }
                    return Err(error);
                }
            }
        }
        return Ok(streaming_response(
            response,
            selection.lease,
            &node,
            Arc::clone(&state.metrics),
            Arc::clone(state.scheduler.prefix_directory()),
            request.prefix_input,
            state.settings.health.clone(),
            header_latency,
            stream_idle_timeout,
            upstream_body_timeout,
            Duration::from_millis(state.settings.server.downstream_stall_timeout_ms),
            state.settings.server.expose_node_header,
            response_mode,
            request.public_model.clone().unwrap_or_default(),
            request.record_prefix,
        ));
    }
}

fn has_untried_alternative(
    state: &AppState,
    model: Option<&str>,
    excluded: &HashSet<String>,
    current: &str,
) -> bool {
    let mut next_excluded = excluded.clone();
    next_excluded.insert(current.to_owned());
    state.scheduler.has_alternative(model, &next_excluded)
}

fn mapped_body(
    original: &Bytes,
    parsed: Option<&Value>,
    upstream_model: Option<&str>,
    public_model: Option<&str>,
) -> Result<Bytes, GatewayError> {
    if upstream_model == public_model || upstream_model.is_none() {
        return Ok(original.clone());
    }
    let mut value = parsed.cloned().ok_or(GatewayError::InvalidJson)?;
    let object = value.as_object_mut().ok_or_else(|| {
        GatewayError::InvalidRequest("JSON request body must be an object".to_owned())
    })?;
    object.insert(
        "model".to_owned(),
        Value::String(upstream_model.unwrap_or_default().to_owned()),
    );
    serde_json::to_vec(&value)
        .map(Bytes::from)
        .map_err(|_| GatewayError::Internal)
}

#[allow(clippy::too_many_arguments)]
async fn buffered_success_response(
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
    expose_node_header: bool,
    response_mode: UpstreamResponseMode,
    public_model: &str,
    record_prefix: bool,
) -> Result<Response, GatewayError> {
    let status = upstream.status();
    let headers = upstream.headers().clone();
    let upstream_request_id = headers.get("x-request-id").cloned();
    let body = match read_limited(
        upstream,
        max_body_bytes,
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
    let body = match response_mode {
        UpstreamResponseMode::Passthrough => Ok(body),
        UpstreamResponseMode::ConvertToAnthropic => {
            anthropic::convert_response(&body, public_model)
        }
        UpstreamResponseMode::NativeAnthropic => {
            anthropic::rewrite_native_response(&body, public_model)
        }
    };
    let body = match body {
        Ok(body) => body,
        Err(error) => {
            lease.record_failure(
                "Anthropic adapter received an invalid upstream response",
                health_config,
            );
            return Err(error);
        }
    };
    lease.record_success(header_latency);
    if record_prefix {
        prefix_directory.record(node.id(), prefix_input);
    }
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    copy_response_headers(&headers, response.headers_mut());
    if response_mode != UpstreamResponseMode::Passthrough {
        response.headers_mut().remove(CONTENT_LENGTH);
        anthropic::set_anthropic_content_type(&mut response, false);
    }
    if let Some(value) = upstream_request_id {
        response
            .headers_mut()
            .insert(HeaderName::from_static("x-upstream-request-id"), value);
    }
    if expose_node_header {
        if let Ok(value) = HeaderValue::from_str(node.id()) {
            response
                .headers_mut()
                .insert(HeaderName::from_static("x-gateway-node"), value);
        }
    }
    Ok(response)
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn streaming_response(
    upstream: reqwest::Response,
    lease: NodeLease,
    node: &Node,
    metrics: Arc<Metrics>,
    prefix_directory: Arc<crate::prefix::PrefixDirectory>,
    prefix_input: PrefixInput,
    health_config: crate::config::HealthConfig,
    header_latency: Duration,
    stream_idle_timeout: Duration,
    upstream_body_timeout: Duration,
    downstream_stall_timeout: Duration,
    expose_node_header: bool,
    response_mode: UpstreamResponseMode,
    public_model: String,
    record_prefix: bool,
) -> Response {
    let status = upstream.status();
    let headers = upstream.headers().clone();
    let expected_body_bytes = upstream.content_length();
    let upstream_request_id = headers.get("x-request-id").cloned();
    let node_id = node.id().to_owned();
    let stream_node_id = node_id.clone();
    let (sender, mut receiver) = mpsc::channel::<Bytes>(1);
    let terminal_failure = Arc::new(parking_lot::Mutex::new(None));
    let pump_failure = Arc::clone(&terminal_failure);

    tokio::spawn(async move {
        let mut guard = BodyGuard::new(
            lease,
            Arc::clone(&metrics),
            stream_node_id.clone(),
            header_latency,
        );
        let mut body = upstream.bytes_stream();
        let mut indexed = false;
        let mut received_body_bytes = 0_u64;
        let body_deadline = tokio::time::Instant::now() + upstream_body_timeout;
        let mut anthropic_converter = match response_mode {
            UpstreamResponseMode::Passthrough => None,
            UpstreamResponseMode::ConvertToAnthropic => Some(ResponseStreamAdapter::Converted(
                anthropic::StreamConverter::new(public_model),
            )),
            UpstreamResponseMode::NativeAnthropic => Some(ResponseStreamAdapter::Native(
                anthropic::NativeStreamRewriter::new(public_model),
            )),
        };

        loop {
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
                            *pump_failure.lock() = Some(StreamFailure::timed_out(
                                "downstream response body stalled",
                            ));
                            return;
                        }
                    }
                }
            };

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
                result = tokio::time::timeout(stream_idle_timeout, body.next()) => {
                    let Ok(item) = result else {
                        fail_response_stream(
                            &pump_failure,
                            &health_config,
                            &mut guard,
                            StreamFailure::timed_out(
                                "upstream response body idle timeout",
                            ),
                        );
                        return;
                    };
                    item
                }
            };

            match item {
                Some(Ok(bytes)) => {
                    if record_prefix && !indexed && !bytes.is_empty() {
                        prefix_directory.record(&stream_node_id, &prefix_input);
                        indexed = true;
                    }
                    received_body_bytes = received_body_bytes
                        .saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
                    let reached_expected =
                        expected_body_bytes.is_some_and(|expected| received_body_bytes >= expected);
                    let mut bytes = if let Some(converter) = anthropic_converter.as_mut() {
                        match converter.push(&bytes) {
                            Ok(bytes) => bytes,
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
                    } else {
                        bytes
                    };
                    if reached_expected {
                        if let Some(converter) = anthropic_converter.as_mut() {
                            match converter.finish() {
                                Ok(tail) if !tail.is_empty() => {
                                    let mut joined =
                                        BytesMut::with_capacity(bytes.len() + tail.len());
                                    joined.extend_from_slice(&bytes);
                                    joined.extend_from_slice(&tail);
                                    bytes = joined.freeze();
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
                    }
                    if !bytes.is_empty() {
                        permit.send(bytes);
                    }
                    if reached_expected {
                        guard.completed();
                        return;
                    }
                }
                Some(Err(error)) => {
                    fail_response_stream(
                        &pump_failure,
                        &health_config,
                        &mut guard,
                        StreamFailure::upstream(error.to_string()),
                    );
                    return;
                }
                None => {
                    if let Some(converter) = anthropic_converter.as_mut() {
                        match converter.finish() {
                            Ok(bytes) if !bytes.is_empty() => permit.send(bytes),
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

    let is_anthropic = response_mode != UpstreamResponseMode::Passthrough;
    let stream = async_stream::stream! {
        while let Some(bytes) = receiver.recv().await {
            yield Ok::<Bytes, io::Error>(bytes);
        }
        let failure = terminal_failure.lock().take();
        if let Some(failure) = failure {
            if is_anthropic {
                yield Ok(anthropic::stream_error_event(&failure.message));
            } else {
                yield Err(failure.into_io_error());
            }
        }
    };
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = status;
    copy_response_headers(&headers, response.headers_mut());
    if is_anthropic {
        response.headers_mut().remove(CONTENT_LENGTH);
        anthropic::set_anthropic_content_type(&mut response, true);
    }
    if let Some(value) = upstream_request_id {
        response
            .headers_mut()
            .insert(HeaderName::from_static("x-upstream-request-id"), value);
    }
    if expose_node_header {
        if let Ok(value) = HeaderValue::from_str(&node_id) {
            response
                .headers_mut()
                .insert(HeaderName::from_static("x-gateway-node"), value);
        }
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
    terminal: bool,
}

impl BodyGuard {
    fn new(
        lease: NodeLease,
        metrics: Arc<Metrics>,
        node_id: String,
        header_latency: Duration,
    ) -> Self {
        Self {
            lease: Some(lease),
            metrics,
            node_id,
            header_latency,
            terminal: false,
        }
    }

    fn completed(&mut self) {
        if let Some(lease) = &self.lease {
            lease.record_success(self.header_latency);
        }
        self.terminal = true;
    }

    fn failed(&mut self, message: String, health_config: &crate::config::HealthConfig) {
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

async fn proxy_error_response(
    upstream: reqwest::Response,
    _lease: NodeLease,
    stream_idle_timeout: Duration,
    upstream_body_timeout: Duration,
    client_protocol: ClientProtocol,
    request_id: &str,
) -> Result<Response, GatewayError> {
    let status = upstream.status();
    let headers = upstream.headers().clone();
    if !status.is_client_error() && !status.is_server_error() {
        return Err(GatewayError::InvalidUpstreamResponse);
    }
    let body = read_limited(
        upstream,
        MAX_ERROR_BODY_BYTES,
        stream_idle_timeout,
        upstream_body_timeout,
    )
    .await?;
    if client_protocol == ClientProtocol::Anthropic {
        let mut response = anthropic::convert_error_response(status, &body, request_id);
        if let Some(value) = headers.get("retry-after") {
            response.headers_mut().insert("retry-after", value.clone());
        }
        return Ok(response);
    }
    let valid_openai_error = serde_json::from_slice::<Value>(&body)
        .ok()
        .and_then(|value| value.get("error").cloned())
        .is_some_and(|error| error.is_object());
    if !valid_openai_error {
        let mut response = GatewayError::UpstreamStatus(status.as_u16()).into_response();
        if let Some(value) = headers.get("retry-after") {
            response.headers_mut().insert("retry-after", value.clone());
        }
        return Ok(response);
    }
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    copy_response_headers(&headers, response.headers_mut());
    Ok(response)
}

async fn read_limited(
    response: reqwest::Response,
    limit: usize,
    stream_idle_timeout: Duration,
    upstream_body_timeout: Duration,
) -> Result<Bytes, GatewayError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(GatewayError::InvalidUpstreamResponse);
    }
    let mut output = BytesMut::new();
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
        output.extend_from_slice(&bytes);
    }
    Ok(output.freeze())
}

fn copy_response_headers(source: &HeaderMap, destination: &mut HeaderMap) {
    let connection_headers = connection_header_names(source);
    for (name, value) in source {
        if should_forward_response_header(name) && !connection_headers.contains(name) {
            destination.append(name, value.clone());
        }
    }
}

fn connection_header_names(headers: &HeaderMap) -> HashSet<HeaderName> {
    headers
        .get_all("connection")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect()
}

fn should_forward_request_header(name: &HeaderName) -> bool {
    !is_hop_by_hop(name)
        && name != CONTENT_LENGTH
        && !matches!(
            name.as_str(),
            "authorization"
                | "cookie"
                | "openai-organization"
                | "openai-project"
                | "x-api-key"
                | "x-request-id"
                | "x-gateway-request-id"
        )
}

fn should_forward_response_header(name: &HeaderName) -> bool {
    !is_hop_by_hop(name) && name.as_str() != "x-request-id"
}

fn is_hop_by_hop(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "proxy-connection"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "host"
    )
}

pub async fn not_found() -> impl IntoResponse {
    GatewayError::RouteNotFound
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn strips_client_credentials_and_hop_headers() {
        for name in [
            "authorization",
            "openai-organization",
            "openai-project",
            "connection",
            "host",
            "content-length",
        ] {
            assert!(!should_forward_request_header(
                &HeaderName::from_bytes(name.as_bytes()).unwrap()
            ));
        }
        assert!(should_forward_request_header(&HeaderName::from_static(
            "openai-beta"
        )));

        let headers = HeaderMap::from_iter([
            (
                HeaderName::from_static("connection"),
                HeaderValue::from_static("keep-alive, x-remove-me"),
            ),
            (
                HeaderName::from_static("x-remove-me"),
                HeaderValue::from_static("secret"),
            ),
        ]);
        assert!(
            connection_header_names(&headers).contains(&HeaderName::from_static("x-remove-me"))
        );
    }

    #[test]
    fn strips_real_claude_code_billing_blocks_without_touching_prompt_context() {
        let mut body = json!({
            "model": "claude-sonnet-4-5",
            "metadata": {
                "user_id": "{\"device_id\":\"device-hash\",\"session_id\":\"session-id\"}"
            },
            "system": [
                {
                    "type": "text",
                    "text": "x-anthropic-billing-header: cc_version=2.1.220.8a5; cc_entrypoint=sdk-cli;"
                },
                {
                    "type": "text",
                    "text": "You are a Claude agent, built on Anthropic's Claude Agent SDK.",
                    "cache_control": {"type": "ephemeral"}
                },
                {
                    "type": "text",
                    "text": "# Environment\n - Primary working directory: /workspace\n - Is a git repository: true"
                }
            ],
            "messages": [{"role": "user", "content": "Implement the change"}]
        });

        assert!(strip_claude_code_billing_blocks(&mut body));
        assert_eq!(body["system"].as_array().unwrap().len(), 2);
        assert_eq!(
            body["system"][0]["text"],
            "You are a Claude agent, built on Anthropic's Claude Agent SDK."
        );
        assert!(
            body["system"][1]["text"]
                .as_str()
                .unwrap()
                .contains("# Environment")
        );
        assert!(
            body["metadata"]["user_id"]
                .as_str()
                .unwrap()
                .contains("device-hash")
        );
    }

    #[test]
    fn claude_code_versions_share_the_same_sanitized_prefix() {
        let config = crate::config::PrefixConfig::default();
        let request = |version: &str| {
            json!({
                "system": [
                    {
                        "type": "text",
                        "text": format!("x-anthropic-billing-header: cc_version={version}; cc_entrypoint=sdk-cli;")
                    },
                    {"type": "text", "text": "Stable agent instructions"}
                ],
                "messages": [{"role": "user", "content": "shared task"}]
            })
        };
        let mut first = request("2.1.220.8a5");
        let mut second = request("2.1.221.9b6");
        assert!(strip_claude_code_billing_blocks(&mut first));
        assert!(strip_claude_code_billing_blocks(&mut second));

        let first = routing_text("chat/completions", Some("model"), Some(&first), &config);
        let second = routing_text("chat/completions", Some("model"), Some(&second), &config);
        let directory = crate::prefix::PrefixDirectory::new(&config);
        directory.record("node-a", &first);
        let matched = directory.best_match(&second);
        assert_eq!(matched.node_ids, ["node-a"]);
        assert_eq!(matched.matched_chars, matched.input_chars);
    }

    #[test]
    fn does_not_strip_multiline_or_non_system_user_content() {
        let mut body = json!({
            "system": "x-anthropic-billing-header: explain this value\nDo not delete this instruction",
            "messages": [{
                "role": "user",
                "content": "x-anthropic-billing-header: cc_version=user-supplied;"
            }]
        });
        assert!(!strip_claude_code_billing_blocks(&mut body));
    }

    #[test]
    fn rejects_stateful_responses_features() {
        assert!(reject_stateful_responses(Some(&json!({"background": true}))).is_err());
        assert!(
            reject_stateful_responses(Some(&json!({"previous_response_id": "resp_1"}))).is_err()
        );
    }
}
