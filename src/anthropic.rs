//! Anthropic client error envelopes and transport headers.
use crate::{error::GatewayError, sse};
use axum::{
    Json,
    http::{HeaderValue, StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};

fn error_type(status: StatusCode) -> &'static str {
    match status {
        StatusCode::BAD_REQUEST | StatusCode::PAYLOAD_TOO_LARGE => "invalid_request_error",
        StatusCode::UNAUTHORIZED => "authentication_error",
        StatusCode::FORBIDDEN => "permission_error",
        StatusCode::NOT_FOUND => "not_found_error",
        StatusCode::TOO_MANY_REQUESTS => "rate_limit_error",
        StatusCode::SERVICE_UNAVAILABLE => "overloaded_error",
        _ => "api_error",
    }
}

fn build_error(status: StatusCode, message: &str, request_id: &str) -> Response {
    (
        status,
        Json(json!({
            "type": "error",
            "error": {"type": error_type(status), "message": message},
            "request_id": request_id
        })),
    )
        .into_response()
}

pub(crate) fn error_response(error: &GatewayError, request_id: &str) -> Response {
    let mut response = build_error(error.status(), &error.to_string(), request_id);
    if let Some(retry_after) = error.retry_after() {
        response.headers_mut().insert("retry-after", retry_after);
    }
    response
}

pub(crate) fn convert_error_response(
    status: StatusCode,
    body: &[u8],
    request_id: &str,
) -> Response {
    let value = serde_json::from_slice::<Value>(body).ok();
    let message = value
        .as_ref()
        .and_then(|value| value.pointer("/error/message"))
        .and_then(Value::as_str)
        .map_or_else(|| format!("upstream returned HTTP {status}"), str::to_owned);
    build_error(status, &message, request_id)
}

pub(crate) fn set_anthropic_content_type(response: &mut Response, streaming: bool) {
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static(if streaming {
            "text/event-stream; charset=utf-8"
        } else {
            "application/json"
        }),
    );
}

pub(crate) fn stream_error_event(message: &str) -> sse::Event {
    sse::Event::json(
        "error",
        &json!({"type":"error","error":{"type":"api_error","message":message}}),
    )
}
pub(crate) fn ping_event() -> sse::Event {
    sse::Event::json("ping", &json!({"type":"ping"}))
}
