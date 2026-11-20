use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context as TaskContext, Poll},
    time::{Duration, Instant},
};

use axum::{
    Json,
    body::{Body, HttpBody as _},
    extract::{Request, State},
    http::{
        HeaderValue, Method, StatusCode,
        header::{AUTHORIZATION, CONTENT_LENGTH, WWW_AUTHENTICATE},
    },
    middleware::Next,
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use bytes::Bytes;
use futures_util::Stream;
use parking_lot::Mutex;
use serde_json::json;
use subtle::ConstantTimeEq;
use tracing::info;
use uuid::Uuid;

use crate::{
    anthropic, error::GatewayError, lifecycle::ResponseGuard, scheduler::IngressAdmission,
};

use super::{AppState, RequestId};

/// Handle stored in request extensions so the proxy handler can shrink or
/// release its ingress byte-budget reservation as soon as the actual request
/// body size is known or forwarded upstream.
#[derive(Clone, Debug)]
pub struct IngressAdmissionHandle {
    inner: Arc<Mutex<Option<IngressAdmission>>>,
}

impl IngressAdmissionHandle {
    fn new(admission: IngressAdmission) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(admission))),
        }
    }

    pub(crate) fn adjust_bytes(&self, actual_bytes: usize) {
        if let Some(admission) = self.inner.lock().as_mut() {
            admission.adjust_bytes(actual_bytes);
        }
    }

    pub(crate) fn release_bytes(&self) {
        if let Some(admission) = self.inner.lock().as_mut() {
            admission.release_bytes();
        }
    }

    fn release_all(&self) {
        *self.inner.lock() = None;
    }
}

struct AdmissionDropGuard(IngressAdmissionHandle);

impl Drop for AdmissionDropGuard {
    fn drop(&mut self) {
        self.0.release_all();
    }
}

/// Shared response-body hooks collected across public middleware layers so
/// `track_public_response` wraps the response body at most once.
#[derive(Clone, Default)]
struct ResponseHooks {
    inner: Arc<Mutex<ResponseHooksState>>,
}

#[derive(Default)]
struct ResponseHooksState {
    telemetry: Option<TelemetryHook>,
    session_log: Option<SessionLogHook>,
}

struct TelemetryHook {
    observation: Arc<crate::observability::Observation>,
    delivery: crate::observability::Delivery,
    remaining: Option<u64>,
}

struct SessionLogHook {
    observation: Arc<crate::session_log::Observation>,
    guard: crate::session_log::DeliveryGuard,
    streaming: bool,
    remaining: Option<u64>,
}

/// Single custom `Stream` adapter replacing three nested `async_stream::stream!`
/// wrappers on the public inference path.
struct ObservedStream {
    inner: axum::body::BodyDataStream,
    _response_guard: ResponseGuard,
    telemetry: Option<TelemetryHook>,
    session_log: Option<SessionLogHook>,
    finished: bool,
}

impl ObservedStream {
    fn into_body(inner: Body, response_guard: ResponseGuard, hooks: ResponseHooksState) -> Body {
        Body::from_stream(Self {
            inner: inner.into_data_stream(),
            _response_guard: response_guard,
            telemetry: hooks.telemetry,
            session_log: hooks.session_log,
            finished: false,
        })
    }

    fn mark_complete(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        if let Some(telemetry) = &mut self.telemetry {
            telemetry.delivery.complete = true;
        }
        if let Some(log) = &mut self.session_log {
            log.guard.1 = true;
        }
    }
}

impl Stream for ObservedStream {
    type Item = Result<Bytes, axum::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                this.mark_complete();
                Poll::Ready(None)
            }
            Poll::Ready(Some(Ok(bytes))) => {
                let len = bytes.len() as u64;
                if let Some(telemetry) = &mut this.telemetry
                    && let Some(left) = &mut telemetry.remaining
                {
                    *left = left.saturating_sub(len);
                    telemetry.delivery.complete = *left == 0;
                }
                if let Some(log) = &mut this.session_log {
                    log.observation.downstream_bytes(&bytes, log.streaming);
                    if let Some(left) = &mut log.remaining {
                        *left = left.saturating_sub(len);
                        log.guard.1 = *left == 0;
                    }
                }
                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(Some(Err(error))) => {
                if let Some(telemetry) = &this.telemetry {
                    telemetry.observation.failed();
                }
                if let Some(log) = &this.session_log {
                    log.observation.error("downstream", "body_error");
                }
                Poll::Ready(Some(Err(error)))
            }
        }
    }
}

/// Bounded per-client fixed-window RPM limiter for optional public API throttling.
#[derive(Debug, Default)]
pub(crate) struct PublicRateLimiter {
    buckets: Mutex<HashMap<String, (Instant, u32)>>,
}

impl PublicRateLimiter {
    const WINDOW: Duration = Duration::from_secs(60);
    const MAX_KEYS: usize = 16_384;

    /// Returns `Ok(())` if allowed, or `Err(retry_after_secs)` if the limit is exceeded.
    pub(crate) fn check(&self, key: &str, limit: u32) -> Result<(), u64> {
        let now = Instant::now();
        let mut buckets = self.buckets.lock();
        if buckets.len() >= Self::MAX_KEYS && !buckets.contains_key(key) {
            buckets.retain(|_, (started, _)| now.duration_since(*started) < Self::WINDOW);
            if buckets.len() >= Self::MAX_KEYS {
                // Evict one arbitrary stale/oldest bucket to keep memory strictly bounded.
                if let Some(oldest) = buckets
                    .iter()
                    .min_by_key(|(_, (started, _))| *started)
                    .map(|(k, _)| k.clone())
                {
                    buckets.remove(&oldest);
                }
            }
        }
        let entry = buckets.entry(key.to_owned()).or_insert((now, 0));
        let elapsed = now.duration_since(entry.0);
        if elapsed >= Self::WINDOW {
            *entry = (now, 1);
            return Ok(());
        }
        if entry.1 >= limit {
            let retry_after = Self::WINDOW.saturating_sub(elapsed).as_secs().max(1);
            return Err(retry_after);
        }
        entry.1 += 1;
        Ok(())
    }
}

fn extract_public_api_key(request: &Request) -> Option<String> {
    if let Some(value) = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        && let Some((scheme, token)) = value.split_once(' ')
        && scheme.eq_ignore_ascii_case("bearer")
    {
        return Some(token.trim().to_owned());
    }
    request
        .headers()
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim().to_owned())
}

fn client_rate_limit_key(request: &Request, api_key: Option<&str>, trust_proxy: bool) -> String {
    if let Some(key) = api_key {
        let digest = blake3::hash(key.as_bytes());
        return format!("key:{}", &digest.to_hex()[..16]);
    }
    if trust_proxy
        && let Some(forwarded) = request
            .headers()
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
        && let Some(first) = forwarded.split(',').next().map(str::trim)
        && !first.is_empty()
    {
        return format!("ip:{first}");
    }
    "ip:default".to_owned()
}

pub(super) async fn authorize_and_rate_limit_public(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let keys = &state.settings.server.public_api_keys;
    let rpm_limit = state.settings.server.public_rpm_limit;
    if keys.is_empty() && rpm_limit.is_none() {
        return next.run(request).await;
    }

    let path = request.uri().path();
    let is_anthropic = path.starts_with("/v1/messages");
    let request_id = request
        .extensions()
        .get::<RequestId>()
        .map(|id| id.0.clone())
        .unwrap_or_default();

    let candidate = extract_public_api_key(&request);
    if !keys.is_empty() {
        let authorized = candidate.as_deref().is_some_and(|provided| {
            keys.iter()
                .any(|expected| bool::from(provided.as_bytes().ct_eq(expected.as_bytes())))
        });
        if !authorized {
            return if is_anthropic {
                (
                    StatusCode::UNAUTHORIZED,
                    Json(json!({
                        "type": "error",
                        "request_id": request_id,
                        "error": {
                            "type": "authentication_error",
                            "message": "invalid or missing API key"
                        }
                    })),
                )
                    .into_response()
            } else {
                (
                    StatusCode::UNAUTHORIZED,
                    Json(json!({
                        "error": {
                            "message": "invalid or missing API key",
                            "type": "authentication_error",
                            "code": "invalid_api_key"
                        }
                    })),
                )
                    .into_response()
            };
        }
    }

    if let Some(limit) = rpm_limit {
        let bucket_key = client_rate_limit_key(
            &request,
            candidate.as_deref(),
            state.settings.server.trust_proxy_headers,
        );
        if let Err(retry_after) = state.rate_limiter.check(&bucket_key, limit) {
            let mut response = if is_anthropic {
                (
                    StatusCode::TOO_MANY_REQUESTS,
                    Json(json!({
                        "type": "error",
                        "request_id": request_id,
                        "error": {
                            "type": "rate_limit_error",
                            "message": "rate limit exceeded"
                        }
                    })),
                )
                    .into_response()
            } else {
                (
                    StatusCode::TOO_MANY_REQUESTS,
                    Json(json!({
                        "error": {
                            "message": "rate limit exceeded",
                            "type": "rate_limit_error",
                            "code": "rate_limit_exceeded"
                        }
                    })),
                )
                    .into_response()
            };
            if let Ok(value) = HeaderValue::from_str(&retry_after.to_string()) {
                response.headers_mut().insert("retry-after", value);
            }
            return response;
        }
    }

    next.run(request).await
}

pub(super) async fn log_public_request(
    State(state): State<Arc<AppState>>,
    mut request: Request,
    next: Next,
) -> Response {
    if request.method() != Method::POST || !request.uri().path().starts_with("/v1/") {
        return next.run(request).await;
    }
    let id = request
        .extensions()
        .get::<RequestId>()
        .map(|id| id.0.as_str())
        .unwrap_or_default();
    let Some(observation) = state
        .session_log
        .begin(request.uri().path(), request.headers(), id)
    else {
        return next.run(request).await;
    };
    let hooks = request.extensions().get::<ResponseHooks>().cloned();
    let mut guard = crate::session_log::DeliveryGuard(Arc::clone(&observation), false);
    request.extensions_mut().insert(Arc::clone(&observation));
    let response = next.run(request).await;
    let streaming = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("text/event-stream"));
    observation.headers(response.status().as_u16(), streaming);
    let length = response
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    let forbids_body = matches!(
        response.status(),
        StatusCode::NO_CONTENT | StatusCode::NOT_MODIFIED
    );
    guard.1 = forbids_body || length == Some(0) || response.body().is_end_stream();
    if let Some(hooks) = hooks {
        hooks.inner.lock().session_log = Some(SessionLogHook {
            observation,
            guard,
            streaming,
            remaining: length,
        });
        return response;
    }
    let (parts, body) = response.into_parts();
    let wrapped = ObservedStream::into_body(
        body,
        state.process.track_response(),
        ResponseHooksState {
            telemetry: None,
            session_log: Some(SessionLogHook {
                observation,
                guard,
                streaming,
                remaining: length,
            }),
        },
    );
    Response::from_parts(parts, wrapped)
}

pub(super) async fn admit_public_request(
    State(state): State<Arc<AppState>>,
    mut request: Request,
    next: Next,
) -> Response {
    if request.method() != Method::POST || !request.uri().path().starts_with("/v1/") {
        return next.run(request).await;
    }

    let max_body = state.settings.server.max_request_body_bytes;
    let reserved_bytes = request
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(max_body);
    if reserved_bytes > max_body {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    }

    let started = std::time::Instant::now();
    if let Some(telemetry) = request
        .extensions()
        .get::<Arc<crate::observability::Observation>>()
    {
        telemetry.wait_started(true);
    }
    let admission = state.scheduler.admit_ingress(reserved_bytes).await;
    let handle = IngressAdmissionHandle::new(admission);
    let _drop_guard = AdmissionDropGuard(handle.clone());
    request.extensions_mut().insert(handle);
    if let Some(telemetry) = request
        .extensions()
        .get::<Arc<crate::observability::Observation>>()
    {
        telemetry.ingress(started.elapsed());
    }
    if let Some(log) = request
        .extensions()
        .get::<Arc<crate::session_log::Observation>>()
    {
        log.timing(
            "ingress_wait",
            crate::session_log::micros(started.elapsed()),
        );
    }
    next.run(request).await
}

pub(super) async fn authorize_admin(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    if let Some(expected) = state.settings.server.admin_token.as_deref() {
        let candidate = request
            .headers()
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(admin_authorization_token);
        if !candidate
            .is_some_and(|candidate| bool::from(candidate.as_bytes().ct_eq(expected.as_bytes())))
        {
            return Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .header(
                    WWW_AUTHENTICATE,
                    "Basic realm=\"Estuary Admin\", charset=\"UTF-8\"",
                )
                .body(Body::from("authentication required"))
                .unwrap_or_else(|_| StatusCode::UNAUTHORIZED.into_response());
        }
    }

    let is_process_control = request.uri().path().starts_with("/admin/api/process/");
    let mutating = matches!(
        *request.method(),
        Method::POST | Method::PUT | Method::DELETE | Method::PATCH
    );
    if mutating
        && !is_process_control
        && state
            .settings
            .server
            .admin_freeze_file
            .as_ref()
            .is_some_and(|path| path.exists())
    {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": {
                    "code": "rollout_in_progress",
                    "message": "Management writes are frozen while a binary rollout is in progress"
                }
            })),
        )
            .into_response();
    }

    next.run(request).await
}

pub(super) fn admin_authorization_token(value: &str) -> Option<String> {
    let (scheme, credentials) = value.split_once(' ')?;
    if scheme.eq_ignore_ascii_case("bearer") {
        return Some(credentials.to_owned());
    }
    if !scheme.eq_ignore_ascii_case("basic") {
        return None;
    }
    let decoded = BASE64_STANDARD.decode(credentials).ok()?;
    let decoded = std::str::from_utf8(&decoded).ok()?;
    decoded
        .split_once(':')
        .map(|(_, password)| password.to_owned())
}

pub(super) async fn assign_request_id(mut request: Request, next: Next) -> Response {
    let anthropic = request.uri().path().starts_with("/v1/messages");
    let id = request
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty() && value.len() <= 128)
        .map_or_else(|| Uuid::now_v7().to_string(), str::to_owned);
    request.extensions_mut().insert(RequestId(id.clone()));
    let mut response = next.run(request).await;
    if let Ok(value) = HeaderValue::from_str(&id) {
        response.headers_mut().insert("x-request-id", value.clone());
        if anthropic {
            response.headers_mut().insert("request-id", value);
        }
    }
    response
}

pub(super) async fn track_public_response(
    State(state): State<Arc<AppState>>,
    mut request: Request,
    next: Next,
) -> Response {
    let guard = state.process.track_response();
    let hooks = ResponseHooks::default();
    request.extensions_mut().insert(hooks.clone());
    let response = next.run(request).await;
    let (parts, body) = response.into_parts();
    let collected = std::mem::take(&mut *hooks.inner.lock());
    Response::from_parts(parts, ObservedStream::into_body(body, guard, collected))
}

pub(super) async fn observe_request(
    State(state): State<Arc<AppState>>,
    mut request: Request,
    next: Next,
) -> Response {
    let started = std::time::Instant::now();
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let endpoint = metric_endpoint(&path);
    let hooks = request.extensions().get::<ResponseHooks>().cloned();
    let telemetry = (method == Method::POST
        && matches!(
            path.as_str(),
            "/v1/chat/completions"
                | "/v1/responses"
                | "/v1/completions"
                | "/v1/embeddings"
                | "/v1/messages"
        ))
    .then(|| {
        let observation = state.telemetry.begin();
        if path == "/v1/embeddings" {
            observation.non_generating();
        }
        observation
    });
    let mut delivery = telemetry.as_ref().map(|observation| {
        request.extensions_mut().insert(Arc::clone(observation));
        crate::observability::Delivery::new(Arc::clone(observation))
    });
    let request_id = request
        .extensions()
        .get::<RequestId>()
        .map(|id| id.0.clone())
        .unwrap_or_default();
    let mut response = next.run(request).await;
    if response.status() == StatusCode::PAYLOAD_TOO_LARGE
        && !response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("application/json"))
    {
        response = if path.starts_with("/v1/messages") {
            anthropic::error_response(&GatewayError::PayloadTooLarge, &request_id)
        } else {
            GatewayError::PayloadTooLarge.into_response()
        };
    }
    let elapsed = started.elapsed();
    state.metrics.request(endpoint, response.status().as_u16());
    state
        .metrics
        .observe_request_duration(elapsed.as_secs_f64());
    info!(
        request_id,
        method = %method,
        path,
        status = response.status().as_u16(),
        elapsed_ms = elapsed.as_millis(),
        "response created"
    );
    if let (Some(telemetry), Some(mut delivery)) = (telemetry, delivery.take()) {
        let streaming = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/event-stream"));
        telemetry.headers(response.status().as_u16(), streaming);
        let length = response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok());
        delivery.complete = length == Some(0) || response.body().is_end_stream();
        if let Some(hooks) = hooks {
            hooks.inner.lock().telemetry = Some(TelemetryHook {
                observation: telemetry,
                delivery,
                remaining: length,
            });
            return response;
        }
        let (parts, body) = response.into_parts();
        let wrapped = ObservedStream::into_body(
            body,
            state.process.track_response(),
            ResponseHooksState {
                telemetry: Some(TelemetryHook {
                    observation: telemetry,
                    delivery,
                    remaining: length,
                }),
                session_log: None,
            },
        );
        return Response::from_parts(parts, wrapped);
    }
    response
}

pub(super) fn metric_endpoint(path: &str) -> &'static str {
    match path {
        "/v1/chat/completions" => "chat_completions",
        "/v1/messages" => "anthropic_messages",
        "/v1/messages/count_tokens" => "anthropic_count_tokens",
        "/v1/responses" => "responses",
        "/v1/completions" => "completions",
        "/v1/embeddings" => "embeddings",
        "/v1/models" => "models",
        "/health/live" => "health_live",
        "/health/ready" => "health_ready",
        "/metrics" => "metrics",
        "/admin/nodes" => "admin_nodes",
        "/admin/api/status" => "admin_status",
        "/admin/api/nodes/preflight" => "admin_node_preflight",
        path if path.starts_with("/admin/nodes/") && path.ends_with("/drain") => "admin_node_drain",
        _ => "other",
    }
}
