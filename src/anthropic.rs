//! Anthropic client error envelopes and transport headers.
use crate::{error::GatewayError, sse};
use axum::{
    Json,
    http::{HeaderValue, StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};

pub(crate) fn error_response(error: &GatewayError, request_id: &str) -> Response {
    let status = error.status();
    let error_type = match status {
        StatusCode::BAD_REQUEST | StatusCode::PAYLOAD_TOO_LARGE => "invalid_request_error",
        StatusCode::NOT_FOUND => "not_found_error",
        StatusCode::TOO_MANY_REQUESTS => "rate_limit_error",
        StatusCode::SERVICE_UNAVAILABLE => "overloaded_error",
        _ => "api_error",
    };
    let retryable = matches!(error, GatewayError::NoHealthyNode(_));
    let mut response = (
        status,
        Json(json!({
            "type": "error",
            "error": {"type": error_type, "message": error.to_string()},
            "request_id": request_id
        })),
    )
        .into_response();
    if retryable {
        response
            .headers_mut()
            .insert("retry-after", HeaderValue::from_static("1"));
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
        .and_then(|value| value.get("error"))
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
        .map_or_else(|| format!("upstream returned HTTP {status}"), str::to_owned);
    let error_type = match status {
        StatusCode::BAD_REQUEST | StatusCode::PAYLOAD_TOO_LARGE => "invalid_request_error",
        StatusCode::NOT_FOUND => "not_found_error",
        StatusCode::TOO_MANY_REQUESTS => "rate_limit_error",
        StatusCode::SERVICE_UNAVAILABLE => "overloaded_error",
        _ => "api_error",
    };
    (
        status,
        Json(json!({
            "type": "error",
            "error": {"type": error_type, "message": message},
            "request_id": request_id
        })),
    )
        .into_response()
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
