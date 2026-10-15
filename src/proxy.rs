use std::{sync::Arc, time::Duration};

use axum::{
    Json,
    body::Body,
    extract::{Extension, Path, State},
    http::{HeaderMap, Method, Uri},
    response::{IntoResponse, Response},
};
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use serde::Serialize;
use serde_json::Value;

use crate::{
    anthropic, chat_bridge,
    error::GatewayError,
    prefix::{PrefixInput, routing_text},
    server::{AppState, RequestId},
};

mod upstream;
use upstream::proxy_with_retries;
mod request_compat;
use request_compat::{
    normalize_context_management, reject_stateful_responses, replace_unsupported_images,
    strip_claude_code_billing_blocks,
};
mod response;
mod streaming;

mod headers;

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

#[allow(clippy::too_many_arguments)]
pub async fn proxy(
    State(state): State<Arc<AppState>>,
    Path(endpoint): Path<String>,
    Extension(request_id): Extension<RequestId>,
    observation: Option<Extension<Arc<crate::session_log::Observation>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let observation = observation.map(|Extension(log)| log);
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
        Err(error) if is_anthropic => {
            if let Some(log) = &observation {
                log.error("request_body", error.code());
            }
            return anthropic::error_response(&error, &gateway_request_id);
        }
        Err(error) => {
            if let Some(log) = &observation {
                log.error("request_body", error.code());
            }
            return error.into_response();
        }
    };
    if let Some(log) = &observation {
        log.request_body(&body, crate::session_log::micros(body_started.elapsed()));
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
    )
    .await;
    if let Err(error) = &result
        && let Some(log) = &observation
    {
        log.error("proxy", error.code());
    }
    match result {
        Ok(response) => response,
        Err(error) if is_anthropic => anthropic::error_response(&error, &gateway_request_id),
        Err(error) => error.into_response(),
    }
}

async fn read_request_body(
    body: Body,
    limit: usize,
    idle_timeout: Duration,
    total_timeout: Duration,
) -> Result<Bytes, GatewayError> {
    let mut stream = body.into_data_stream();
    let mut output = BytesMut::new();
    let deadline = tokio::time::Instant::now() + total_timeout;
    loop {
        let item =
            tokio::time::timeout_at(deadline, tokio::time::timeout(idle_timeout, stream.next()))
                .await
                .map_err(|_| GatewayError::RequestTimeout)?
                .map_err(|_| GatewayError::RequestTimeout)?;
        let Some(item) = item else {
            break;
        };
        let bytes = item
            .map_err(|_| GatewayError::InvalidRequest("failed to read request body".to_owned()))?;
        if output.len().saturating_add(bytes.len()) > limit {
            return Err(GatewayError::PayloadTooLarge);
        }
        output.extend_from_slice(&bytes);
    }
    Ok(output.freeze())
}

#[allow(clippy::too_many_lines)]
#[allow(clippy::too_many_arguments)]
async fn proxy_inner(
    state: Arc<AppState>,
    endpoint: String,
    request_id: RequestId,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
    observation: Option<Arc<crate::session_log::Observation>>,
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

    let is_inference_json = matches!(
        endpoint.as_str(),
        "chat/completions"
            | "responses"
            | "completions"
            | "embeddings"
            | "messages"
            | "messages/count_tokens"
    );
    if method != Method::POST || !is_inference_json {
        return Err(GatewayError::RouteNotFound);
    }
    let mut parsed = if body.is_empty() {
        None
    } else {
        sonic_rs::from_slice::<Value>(&body).ok()
    };
    if let Some(log) = &observation {
        log.parsed(parsed.as_ref());
    }
    if is_inference_json && parsed.is_none() {
        return Err(GatewayError::InvalidJson);
    }
    if endpoint == "responses" {
        reject_stateful_responses(parsed.as_ref())?;
    }
    let mut body_changed = false;
    if matches!(endpoint.as_str(), "messages" | "messages/count_tokens") {
        body_changed |= normalize_context_management(
            parsed.as_mut().expect("inference JSON was validated above"),
        )?;
    }
    body_changed |= parsed
        .as_mut()
        .is_some_and(strip_claude_code_billing_blocks);

    let public_model = parsed
        .as_ref()
        .and_then(|value| value.get("model"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    if is_inference_json && public_model.is_none() {
        return Err(GatewayError::MissingModel);
    }
    if let (Some(model), Some(parsed)) = (public_model.as_deref(), parsed.as_mut())
        && !state.scheduler.model_supports_multimodal(model)
    {
        body_changed |= replace_unsupported_images(parsed, model) > 0;
    }

    let count_tokens = endpoint == "messages/count_tokens";
    let protocol = if matches!(endpoint.as_str(), "messages" | "messages/count_tokens") {
        ClientProtocol::Anthropic
    } else {
        ClientProtocol::OpenAi
    };
    let (converted, bridge_context) =
        chat_bridge::prepare(&endpoint, parsed.take().expect("validated JSON"))?;
    let endpoint = if bridge_context.is_some() {
        "chat/completions".to_owned()
    } else {
        endpoint
    };
    parsed = Some(converted);
    let original_body = if body_changed || bridge_context.is_some() {
        sonic_rs::to_vec(parsed.as_ref().expect("validated JSON"))
            .map(Bytes::from)
            .map_err(|_| GatewayError::Internal)?
    } else {
        body
    };
    let record_prefix = !count_tokens;
    let streaming = !count_tokens
        && parsed
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
    if !count_tokens && let (Some(model), Some(parsed)) = (public_model.as_deref(), parsed.as_ref())
    {
        let allow_remote = state
            .scheduler
            .approximate_prefix_worth_tokenizing(&prefix_input);
        let tokenization = state
            .vllm
            .tokenize_for_routing(&state.client, &endpoint, model, parsed, allow_remote)
            .await;
        state
            .metrics
            .tokenization(tokenization.outcome, tokenization.elapsed);
        if let Some(log) = &observation {
            log.timing(
                "tokenization",
                crate::session_log::micros(tokenization.elapsed),
            );
            log.event(tokenization.outcome);
        }
        if let Some(tokens) = tokenization.tokens {
            prefix_input.set_scoped_token_ids(tokens, tokenization.node_ids);
        }
    }

    let upstream_query = uri.query().map(str::to_owned);
    proxy_with_retries(
        state,
        ProxyRequest {
            endpoint,
            method,
            query: upstream_query,
            headers,
            original_body,
            parsed_body: parsed,
            public_model,
            prefix_input,
            streaming,
            request_id,
            client_protocol: protocol,
            bridge_context,
            count_tokens,
            record_prefix,
            observation,
        },
    )
    .await
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
    ChatBridge { context: Arc<chat_bridge::Context> },
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
