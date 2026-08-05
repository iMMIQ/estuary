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
) -> Result<Response, GatewayError> {
    if endpoint.starts_with("responses/") {
        return Err(GatewayError::UnsupportedFeature(
            "Responses retrieve, delete, and cancel endpoints require durable node affinity",
        ));
    }

    let is_inference_json = matches!(
        endpoint.as_str(),
        "chat/completions" | "responses" | "completions" | "embeddings"
    );
    if method != Method::POST || !is_inference_json {
        return Err(GatewayError::RouteNotFound);
    }
    let parsed = if body.is_empty() {
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

    let public_model = parsed
        .as_ref()
        .and_then(|value| value.get("model"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    if is_inference_json && public_model.is_none() {
        return Err(GatewayError::MissingModel);
    }
    let streaming = parsed
        .as_ref()
        .and_then(|value| value.get("stream"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let mut prefix_input = routing_text(
        &endpoint,
        public_model.as_deref(),
        parsed.as_ref(),
        &state.settings.routing.prefix,
    );
    if let (Some(model), Some(parsed)) = (public_model.as_deref(), parsed.as_ref()) {
        if let Some(tokens) = state
            .vllm
            .tokenize_for_routing(&state.client, &endpoint, model, parsed)
            .await
        {
            prefix_input.set_token_ids(tokens);
        }
    }

    proxy_with_retries(
        state,
        ProxyRequest {
            endpoint,
            method,
            query: uri.query().map(str::to_owned),
            headers,
            original_body: body,
            parsed_body: parsed,
            public_model,
            prefix_input,
            streaming,
            request_id,
        },
    )
    .await
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
}

#[allow(clippy::too_many_lines)]
async fn proxy_with_retries(
    state: Arc<AppState>,
    request: ProxyRequest,
) -> Result<Response, GatewayError> {
    let mut excluded = HashSet::new();
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
        let upstream_url = node
            .upstream_url(&request.endpoint, request.query.as_deref())
            .map_err(|error| {
                warn!(node = node.id(), error = %error, "failed to build upstream URL");
                GatewayError::Internal
            })?;
        let upstream_body = mapped_body(
            &request.original_body,
            request.parsed_body.as_ref(),
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
    lease.record_success(header_latency);
    prefix_directory.record(node.id(), prefix_input);

    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    copy_response_headers(&headers, response.headers_mut());
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

        if expected_body_bytes == Some(0) {
            prefix_directory.record(&stream_node_id, &prefix_input);
            guard.completed();
            return;
        }

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
                    if !indexed && !bytes.is_empty() {
                        prefix_directory.record(&stream_node_id, &prefix_input);
                        indexed = true;
                    }
                    received_body_bytes = received_body_bytes
                        .saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
                    permit.send(bytes);
                    if expected_body_bytes.is_some_and(|expected| received_body_bytes >= expected) {
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
                    if !indexed {
                        prefix_directory.record(&stream_node_id, &prefix_input);
                    }
                    guard.completed();
                    return;
                }
            }
        }
    });

    let stream = async_stream::stream! {
        while let Some(bytes) = receiver.recv().await {
            yield Ok::<Bytes, io::Error>(bytes);
        }
        let failure = terminal_failure.lock().take();
        if let Some(failure) = failure {
            yield Err(failure.into_io_error());
        }
    };
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = status;
    copy_response_headers(&headers, response.headers_mut());
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
    fn rejects_stateful_responses_features() {
        assert!(reject_stateful_responses(Some(&json!({"background": true}))).is_err());
        assert!(
            reject_stateful_responses(Some(&json!({"previous_response_id": "resp_1"}))).is_err()
        );
    }
}
