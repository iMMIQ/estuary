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
    #[error("all upstreams are at capacity; queue wait expired")]
    CapacityTimeout,
    #[error("the gateway request queue is full")]
    QueueFull,
    #[error("request body exceeds the configured gateway limit")]
    PayloadTooLarge,
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
    #[error("internal gateway error")]
    Internal,
}

impl GatewayError {
    fn status(&self) -> StatusCode {
        match self {
            Self::InvalidJson
            | Self::MissingModel
            | Self::UnsupportedFeature(_)
            | Self::InvalidRequest(_) => StatusCode::BAD_REQUEST,
            Self::UnknownModel(_) | Self::RouteNotFound => StatusCode::NOT_FOUND,
            Self::NoHealthyNode(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::CapacityTimeout | Self::QueueFull => StatusCode::TOO_MANY_REQUESTS,
            Self::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::InvalidUpstreamResponse | Self::Upstream(_) => StatusCode::BAD_GATEWAY,
            Self::UpstreamTimeout => StatusCode::GATEWAY_TIMEOUT,
            Self::UpstreamStatus(status) => {
                StatusCode::from_u16(*status).unwrap_or(StatusCode::BAD_GATEWAY)
            }
            Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn code(&self) -> &'static str {
        match self {
            Self::InvalidJson => "invalid_json",
            Self::MissingModel => "missing_model",
            Self::UnknownModel(_) => "model_not_found",
            Self::NoHealthyNode(_) => "no_healthy_upstream",
            Self::CapacityTimeout => "upstream_capacity_timeout",
            Self::QueueFull => "gateway_queue_full",
            Self::PayloadTooLarge => "request_too_large",
            Self::UnsupportedFeature(_) => "unsupported_feature",
            Self::InvalidRequest(_) => "invalid_request",
            Self::InvalidUpstreamResponse => "invalid_upstream_response",
            Self::Upstream(_) | Self::UpstreamStatus(_) => "upstream_error",
            Self::UpstreamTimeout => "upstream_timeout",
            Self::RouteNotFound => "route_not_found",
            Self::Internal => "internal_error",
        }
    }
}

#[derive(Serialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Serialize)]
struct ErrorBody {
    message: String,
    r#type: &'static str,
    param: Option<String>,
    code: &'static str,
}

impl IntoResponse for GatewayError {
    fn into_response(self) -> Response {
        let status = self.status();
        let retryable = matches!(
            self,
            Self::NoHealthyNode(_) | Self::CapacityTimeout | Self::QueueFull
        );
        let error_type = match self {
            Self::InvalidJson
            | Self::MissingModel
            | Self::UnknownModel(_)
            | Self::PayloadTooLarge
            | Self::UnsupportedFeature(_)
            | Self::InvalidRequest(_)
            | Self::RouteNotFound => "invalid_request_error",
            Self::CapacityTimeout | Self::QueueFull => "rate_limit_error",
            _ => "api_error",
        };
        let body = Json(ErrorEnvelope {
            error: ErrorBody {
                message: self.to_string(),
                r#type: error_type,
                param: None,
                code: self.code(),
            },
        });
        let mut response = (status, body).into_response();
        if retryable {
            response
                .headers_mut()
                .insert(RETRY_AFTER, HeaderValue::from_static("1"));
        }
        response
    }
}
