use std::collections::HashSet;

use axum::http::{HeaderMap, HeaderName, header::CONTENT_LENGTH};

use crate::config::AnthropicProtocol;

pub(super) fn copy_response_headers(source: &HeaderMap, destination: &mut HeaderMap) {
    let connection_headers = connection_header_names(source);
    for (name, value) in source {
        if should_forward_response_header(name) && !connection_headers.contains(name) {
            destination.append(name, value.clone());
        }
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

pub(super) fn should_forward_protocol_header(
    name: &HeaderName,
    protocol: Option<AnthropicProtocol>,
) -> bool {
    protocol.is_none_or(|protocol| {
        protocol == AnthropicProtocol::Native || !name.as_str().starts_with("anthropic-")
    })
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
