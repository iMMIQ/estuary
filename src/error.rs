use axum::{
    Json,
    http::{HeaderValue, StatusCode, header::RETRY_AFTER},
    response::{IntoResponse, Response},
};
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum GatewayError {
    #[error("request body must be valid JSON for this endpoint")]
    InvalidJson,
    #[error("request body is missing the required 'model' field")]
    MissingModel,
    #[error("no upstream is configured for model '{0}'")]
    UnknownModel(String),
    #[error("no healthy upstream is available for model '{0}'")]
    NoHealthyNode(String),
    #[error("request body exceeds the configured gateway limit")]
    PayloadTooLarge,
    #[error("request body was not received before the configured timeout")]
    RequestTimeout,
    #[error("this feature is not supported by the gateway yet: {0}")]
    UnsupportedFeature(&'static str),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("upstream returned HTTP {0}")]
    UpstreamStatus(u16),
    #[error("upstream did not return response headers before the timeout")]
    UpstreamTimeout,
    #[error("route not found")]
    RouteNotFound,
    #[error("invalid upstream response")]
    InvalidUpstreamResponse,
    #[error("upstream request failed: {0}")]
    Upstream(String),
    #[error("invalid or missing API key")]
    Unauthorized,
    #[error("rate limit exceeded")]
    RateLimited { retry_after: u64 },
    #[error("internal gateway error")]
    Internal,
}

impl GatewayError {
    pub(crate) fn status(&self) -> StatusCode {
        match self {
            Self::InvalidJson
            | Self::MissingModel
            | Self::UnsupportedFeature(_)
            | Self::InvalidRequest(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::UnknownModel(_) | Self::RouteNotFound => StatusCode::NOT_FOUND,
            Self::RateLimited { .. } => StatusCode::TOO_MANY_REQUESTS,
            Self::NoHealthyNode(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::RequestTimeout => StatusCode::REQUEST_TIMEOUT,
            Self::InvalidUpstreamResponse | Self::Upstream(_) => StatusCode::BAD_GATEWAY,
            Self::UpstreamTimeout => StatusCode::GATEWAY_TIMEOUT,
            Self::UpstreamStatus(status) => {
                StatusCode::from_u16(*status).unwrap_or(StatusCode::BAD_GATEWAY)
            }
            Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::InvalidJson => "invalid_json",
            Self::MissingModel => "missing_model",
            Self::Unauthorized => "invalid_api_key",
            Self::RateLimited { .. } => "rate_limit_exceeded",
            Self::UnknownModel(_) => "model_not_found",
            Self::NoHealthyNode(_) => "no_healthy_upstream",
            Self::PayloadTooLarge => "request_too_large",
            Self::RequestTimeout => "request_timeout",
            Self::UnsupportedFeature(_) => "unsupported_feature",
            Self::InvalidRequest(_) => "invalid_request",
            Self::InvalidUpstreamResponse => "invalid_upstream_response",
            Self::Upstream(_) | Self::UpstreamStatus(_) => "upstream_error",
            Self::UpstreamTimeout => "upstream_timeout",
            Self::RouteNotFound => "route_not_found",
            Self::Internal => "internal_error",
        }
    }

    pub(crate) fn retry_after(&self) -> Option<HeaderValue> {
        match self {
            Self::NoHealthyNode(_) => Some(HeaderValue::from_static("1")),
            Self::RateLimited { retry_after } => {
                HeaderValue::from_str(&retry_after.to_string()).ok()
            }
            _ => None,
        }
    }
}

#[derive(Serialize)]
struct ErrorEnvelope<'a> {
    error: ErrorBody<'a>,
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    message: String,
    r#type: &'static str,
    param: Option<String>,
    code: &'a str,
}

pub(crate) fn openai_error_response(
    status: StatusCode,
    error_type: &'static str,
    code: &str,
    message: String,
) -> Response {
    (
        status,
        Json(ErrorEnvelope {
            error: ErrorBody {
                message,
                r#type: error_type,
                param: None,
                code,
            },
        }),
    )
        .into_response()
}

impl IntoResponse for GatewayError {
    fn into_response(self) -> Response {
        let error_type = match self {
            Self::Unauthorized => "authentication_error",
            Self::RateLimited { .. } => "rate_limit_error",
            Self::InvalidJson
            | Self::MissingModel
            | Self::UnknownModel(_)
            | Self::PayloadTooLarge
            | Self::RequestTimeout
            | Self::UnsupportedFeature(_)
            | Self::InvalidRequest(_)
            | Self::RouteNotFound => "invalid_request_error",
            _ => "api_error",
        };
        let mut response =
            openai_error_response(self.status(), error_type, self.code(), self.to_string());
        if let Some(retry_after) = self.retry_after() {
            response.headers_mut().insert(RETRY_AFTER, retry_after);
        }
        response
    }
}
