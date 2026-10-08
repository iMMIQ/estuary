use std::sync::Arc;

use axum::{
    Json,
    body::Body,
    extract::{Request, State},
    http::{
        HeaderValue, Method, StatusCode,
        header::{AUTHORIZATION, CONTENT_LENGTH, WWW_AUTHENTICATE},
    },
    middleware::Next,
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use futures_util::StreamExt;
use serde_json::json;
use subtle::ConstantTimeEq;
use tracing::info;
use uuid::Uuid;

use crate::{anthropic, error::GatewayError};

use super::{AppState, RequestId};

pub(super) async fn admit_public_request(
    State(state): State<Arc<AppState>>,
    request: Request,
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

    let _admission = state.scheduler.admit_ingress(reserved_bytes).await;
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
    request: Request,
    next: Next,
) -> Response {
    let guard = state.process.track_response();
    let response = next.run(request).await;
    let (parts, body) = response.into_parts();
    let stream = async_stream::stream! {
        let _guard = guard;
        let mut body = body.into_data_stream();
        while let Some(item) = body.next().await {
            yield item;
        }
    };
    Response::from_parts(parts, Body::from_stream(stream))
}

pub(super) async fn observe_request(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let started = std::time::Instant::now();
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let endpoint = metric_endpoint(&path);
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
        "request completed"
    );
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
