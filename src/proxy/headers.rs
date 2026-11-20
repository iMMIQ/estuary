use std::collections::HashSet;

use axum::{
    http::{
        HeaderMap, HeaderName, HeaderValue, StatusCode,
        header::{CONTENT_LENGTH, CONTENT_TYPE},
    },
    response::Response,
};

use crate::anthropic;

use super::UpstreamResponseMode;

pub(super) fn copy_response_headers(source: &HeaderMap, destination: &mut HeaderMap) {
    let connection_headers = connection_header_names(source);
    for (name, value) in source {
        if should_forward_response_header(name) && !connection_headers.contains(name) {
            destination.append(name, value.clone());
        }
    }
}

pub(super) fn apply_upstream_response_headers(
    response: &mut Response,
    status: StatusCode,
    headers: &HeaderMap,
    node_id: &str,
    expose_node_header: bool,
    response_mode: &UpstreamResponseMode,
    streaming: bool,
) {
    *response.status_mut() = status;
    copy_response_headers(headers, response.headers_mut());
    if response_mode.rewrites_body() {
        response.headers_mut().remove(CONTENT_LENGTH);
    }
    if response_mode.is_anthropic() {
        anthropic::set_anthropic_content_type(response, streaming);
    }
    if let Some(value) = headers.get("x-request-id") {
        response.headers_mut().insert(
            HeaderName::from_static("x-upstream-request-id"),
            value.clone(),
        );
    }
    if expose_node_header && let Ok(value) = HeaderValue::from_str(node_id) {
        response
            .headers_mut()
            .insert(HeaderName::from_static("x-gateway-node"), value);
    }
    if streaming
        && headers
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/event-stream"))
    {
        response.headers_mut().insert(
            HeaderName::from_static("x-accel-buffering"),
            HeaderValue::from_static("no"),
        );
    }
}

pub(super) fn connection_header_names(headers: &HeaderMap) -> HashSet<HeaderName> {
    headers
        .get_all("connection")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect()
}

pub(super) fn should_forward_request_header(name: &HeaderName) -> bool {
    !is_hop_by_hop(name)
        && name != CONTENT_LENGTH
        && !name.as_str().starts_with("anthropic-")
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

pub(super) fn should_forward_response_header(name: &HeaderName) -> bool {
    !is_hop_by_hop(name) && name.as_str() != "x-request-id"
}

pub(super) fn is_hop_by_hop(name: &HeaderName) -> bool {
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
