use std::{sync::Arc, time::Duration};

use axum::{
    Json,
    body::Body,
    extract::{Extension, Path, State},
    http::{HeaderMap, Method, Uri},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use serde::Serialize;
use serde_json::Value;

use crate::{
    anthropic, chat_bridge,
    error::GatewayError,
    prefix::{PrefixInput, routing_text},
    server::{AppState, IngressAdmissionHandle, RequestId},
    time::micros,
};

mod headers;
mod request_compat;
mod response;
mod streaming;
mod upstream;

use request_compat::{
    normalize_context_management, reject_stateful_responses, replace_unsupported_images,
    strip_claude_code_billing_blocks,
};
use upstream::proxy_with_retries;

const MAX_ERROR_BODY_BYTES: usize = 1024 * 1024;
const MAX_SSE_EVENT_BYTES: usize = 16 * 1024 * 1024;

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

fn error_response(error: GatewayError, is_anthropic: bool, request_id: &str) -> Response {
    if is_anthropic {
        anthropic::error_response(&error, request_id)
    } else {
        error.into_response()
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn proxy(
    State(state): State<Arc<AppState>>,
    Path(endpoint): Path<String>,
    Extension(request_id): Extension<RequestId>,
    observation: Option<Extension<Arc<crate::session_log::Observation>>>,
    telemetry: Option<Extension<Arc<crate::observability::Observation>>>,
    admission: Option<Extension<IngressAdmissionHandle>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let observation = observation.map(|Extension(log)| log);
    let telemetry = telemetry.map(|Extension(telemetry)| telemetry);
    let admission = admission.map(|Extension(handle)| handle);
    let body_started = std::time::Instant::now();
    let is_anthropic = matches!(endpoint.as_str(), "messages" | "messages/count_tokens")
        || endpoint.starts_with("files/");
    let gateway_request_id = request_id.0.clone();
    let body = match read_request_body(
        body,
        state.settings.server.max_request_body_bytes,
        Duration::from_millis(state.settings.server.request_body_idle_timeout_ms),
        Duration::from_millis(state.settings.server.request_body_timeout_ms),
    )
    .await
    {
        Ok(body) => body,
        Err(error) => {
            if let Some(log) = &observation {
                log.error("request_body", error.code());
            }
            return error_response(error, is_anthropic, &gateway_request_id);
        }
    };
    if let Some(admission) = &admission {
        admission.adjust_bytes(body.len());
    }
    if let Some(log) = &observation {
        log.request_body(&body, micros(body_started.elapsed()));
    }
    let result = proxy_inner(
        state,
        endpoint,
        request_id,
        method,
        uri,
        headers,
        body,
        observation.clone(),
        telemetry,
        admission,
    )
    .await;
    if let Err(error) = &result
        && let Some(log) = &observation
    {
        log.error("proxy", error.code());
    }
    result.unwrap_or_else(|error| error_response(error, is_anthropic, &gateway_request_id))
}

async fn read_request_body(
    body: Body,
    limit: usize,
    idle_timeout: Duration,
    total_timeout: Duration,
) -> Result<Bytes, GatewayError> {
    response::read_body_stream(
        body.into_data_stream(),
        response::BodyReadLimits {
            limit,
            length_hint: 0,
            idle_timeout,
            total_timeout,
        },
        || GatewayError::RequestTimeout,
        || GatewayError::InvalidRequest("failed to read request body".to_owned()),
        || GatewayError::PayloadTooLarge,
    )
    .await
}

#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn proxy_inner(
    state: Arc<AppState>,
    endpoint: String,
    request_id: RequestId,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
    observation: Option<Arc<crate::session_log::Observation>>,
    telemetry: Option<Arc<crate::observability::Observation>>,
    admission: Option<IngressAdmissionHandle>,
) -> Result<Response, GatewayError> {
    if endpoint.starts_with("files/") {
        return Err(GatewayError::UnsupportedFeature(
            "Claude Code file downloads require a file service; vLLM does not provide one",
        ));
    }
    if endpoint.starts_with("responses/") {
        return Err(GatewayError::UnsupportedFeature(
            "Responses retrieve, delete, and cancel endpoints require durable node affinity",
        ));
    }
    if method != Method::POST
        || !matches!(
            endpoint.as_str(),
            "chat/completions"
                | "responses"
                | "completions"
                | "embeddings"
                | "messages"
                | "messages/count_tokens"
        )
    {
        return Err(GatewayError::RouteNotFound);
    }

    let parsed = (!body.is_empty())
        .then(|| serde_json::from_slice::<Value>(&body).ok())
        .flatten();
    if let Some(log) = &observation {
        log.parsed(parsed.as_ref());
    }
    let mut parsed = parsed.ok_or(GatewayError::InvalidJson)?;
    if endpoint == "responses" {
        reject_stateful_responses(Some(&parsed))?;
    }
    let mut body_changed = false;
    if matches!(endpoint.as_str(), "messages" | "messages/count_tokens") {
        body_changed |= normalize_context_management(&mut parsed)?;
    }
    body_changed |= strip_claude_code_billing_blocks(&mut parsed);

    let public_model = parsed
        .get("model")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or(GatewayError::MissingModel)?;
    if let Some(telemetry) = &telemetry
        && state.scheduler.has_explicit_model(&public_model)
    {
        telemetry.model(&public_model);
    }
    if !state.scheduler.model_supports_multimodal(&public_model) {
        body_changed |= replace_unsupported_images(&mut parsed, &public_model) > 0;
    }

    let count_tokens = endpoint == "messages/count_tokens";
    let client_protocol = if matches!(endpoint.as_str(), "messages" | "messages/count_tokens") {
        ClientProtocol::Anthropic
    } else {
        ClientProtocol::OpenAi
    };
    let (parsed, bridge_context) = chat_bridge::prepare(&endpoint, parsed)?;
    let endpoint = if bridge_context.is_some() {
        "chat/completions".to_owned()
    } else {
        endpoint
    };
    let original_body = if body_changed || bridge_context.is_some() {
        serde_json::to_vec(&parsed)
            .map(Bytes::from)
            .map_err(|_| GatewayError::Internal)?
    } else {
        body
    };
    let record_prefix = !count_tokens;
    let streaming = !count_tokens
        && parsed
            .get("stream")
            .and_then(Value::as_bool)
            .unwrap_or(false);
    let mut prefix_input = routing_text(
        &endpoint,
        Some(&public_model),
        Some(&parsed),
        &state.settings.routing.prefix,
    );
    if !count_tokens {
        let allow_remote = state
            .scheduler
            .approximate_prefix_worth_tokenizing(&prefix_input);
        let tokenization = state
            .vllm
            .tokenize_for_routing(
                &state.control_client,
                &endpoint,
                &public_model,
                &parsed,
                allow_remote,
            )
            .await;
        state
            .metrics
            .tokenization(tokenization.outcome, tokenization.elapsed);
        if let Some(log) = &observation {
            log.timing("tokenization", micros(tokenization.elapsed));
            log.event(tokenization.outcome);
        }
        if let Some(tokens) = tokenization.tokens {
            prefix_input.set_scoped_token_ids(tokens, tokenization.node_ids);
        }
    }

    proxy_with_retries(
        state,
        ProxyRequest {
            endpoint,
            method,
            query: uri.query().map(str::to_owned),
            headers,
            original_body,
            parsed_body: parsed,
            public_model,
            prefix_input,
            streaming,
            request_id,
            client_protocol,
            bridge_context,
            count_tokens,
            record_prefix,
            observation,
            telemetry,
            admission,
        },
    )
    .await
}

struct ProxyRequest {
    telemetry: Option<Arc<crate::observability::Observation>>,
    admission: Option<IngressAdmissionHandle>,
    endpoint: String,
    method: Method,
    query: Option<String>,
    headers: HeaderMap,
    original_body: Bytes,
    parsed_body: Value,
    public_model: String,
    prefix_input: PrefixInput,
    streaming: bool,
    request_id: RequestId,
    client_protocol: ClientProtocol,
    bridge_context: Option<chat_bridge::Context>,
    count_tokens: bool,
    record_prefix: bool,
    observation: Option<Arc<crate::session_log::Observation>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClientProtocol {
    OpenAi,
    Anthropic,
}

#[derive(Clone, Debug)]
enum UpstreamResponseMode {
    Passthrough,
    ChatBridge { context: Box<chat_bridge::Context> },
    TokenCount,
}

impl UpstreamResponseMode {
    fn name(&self) -> &'static str {
        match self {
            Self::Passthrough => "passthrough",
            Self::ChatBridge { context } if context.messages => "chat_to_messages",
            Self::ChatBridge { .. } => "chat_to_responses",
            Self::TokenCount => "chat_token_count",
        }
    }
    fn is_anthropic(&self) -> bool {
        matches!(self, Self::ChatBridge { context } if context.messages)
            || matches!(self, Self::TokenCount)
    }
    fn rewrites_body(&self) -> bool {
        !matches!(self, Self::Passthrough)
    }
}

pub async fn not_found() -> impl IntoResponse {
    GatewayError::RouteNotFound
}

#[cfg(test)]
mod tests;
