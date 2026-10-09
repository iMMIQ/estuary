use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode},
    response::Response,
};
use bytes::Bytes;
use serde_json::Value;
use tracing::{debug, warn};

use crate::{codex, config::AnthropicProtocol, error::GatewayError, server::AppState};

use super::headers::{
    connection_header_names, should_forward_protocol_header, should_forward_request_header,
};
use super::payload::prepared_payload;
use super::request_compat::apply_vllm_native_thinking_compat;
use super::response::{buffered_success_response, proxy_error_response};
use super::streaming::streaming_response;
use super::{ProxyRequest, UpstreamResponseMode};

#[allow(clippy::too_many_lines)]
pub(super) async fn proxy_with_retries(
    state: Arc<AppState>,
    mut request: ProxyRequest,
) -> Result<Response, GatewayError> {
    let mut excluded = HashSet::new();
    let mut adapter_errors = Vec::new();
    let mut attempt = 0usize;
    loop {
        let queue_started = Instant::now();
        let selection = state
            .scheduler
            .acquire(
                request.public_model.as_deref(),
                request.prefix_input.clone(),
                &excluded,
                request.original_body.len(),
            )
            .await;
        state
            .metrics
            .observe_queue_duration(queue_started.elapsed().as_secs_f64());
        let scheduler_wait = queue_started.elapsed();
        let selection = selection?;
        state
            .metrics
            .observe_prefix_match(selection.prefix_match_chars);
        state
            .metrics
            .observe_prefix_match_tokens(selection.prefix_match_tokens);

        let node = Arc::clone(&selection.node);
        let recipe = if matches!(request.endpoint.as_str(), "messages" | "responses")
            && node.model_family(request.public_model.as_deref().unwrap_or_default())
                == crate::config::ModelFamily::Deepseek
        {
            let (mut payload, prepared) = crate::deepseek::Prepared::new(
                &request.endpoint,
                request.parsed_body.as_ref().expect("inference JSON"),
            )?;
            if node.provider().kind == crate::config::ProviderKind::Vllm {
                apply_vllm_native_thinking_compat(
                    payload.as_object_mut().expect("recipe chat object"),
                )?;
            }
            Some((
                prepared_payload("chat/completions", payload),
                Arc::new(prepared),
            ))
        } else {
            None
        };
        let selected_protocol = request
            .anthropic_payloads
            .as_ref()
            .filter(|_| recipe.is_none())
            .map(|_| {
                node.provider()
                    .anthropic_protocol
                    .resolve(node.provider().kind)
            });
        let selected_payload = if let Some((payload, _)) = &recipe {
            Some(payload)
        } else if let Some(protocol) = selected_protocol {
            let source = request
                .parsed_body
                .as_ref()
                .expect("Anthropic inference requests contain parsed JSON");
            match request
                .anthropic_payloads
                .as_mut()
                .expect("Anthropic requests have adapter state")
                .prepare(protocol, source)
            {
                Ok(payload) => payload,
                Err(error) => {
                    adapter_errors.push(format!("{} ({protocol:?}): {error}", node.id()));
                    for candidate in state.scheduler.nodes() {
                        if candidate
                            .provider()
                            .anthropic_protocol
                            .resolve(candidate.provider().kind)
                            == protocol
                        {
                            excluded.insert(candidate.id().to_owned());
                        }
                    }
                    drop(selection.lease);
                    if state
                        .scheduler
                        .has_alternative(request.public_model.as_deref(), &excluded)
                    {
                        continue;
                    }
                    return Err(GatewayError::InvalidRequest(format!(
                        "no configured upstream protocol can represent this Anthropic request: {}",
                        adapter_errors.join("; ")
                    )));
                }
            }
        } else {
            None
        };
        let (upstream_endpoint, upstream_original, upstream_parsed, upstream_query) =
            selected_payload.map_or_else(
                || {
                    (
                        request.endpoint.as_str(),
                        &request.original_body,
                        request.parsed_body.as_ref(),
                        request.query.as_deref(),
                    )
                },
                |payload| {
                    (
                        payload.endpoint.as_str(),
                        &payload.body,
                        Some(&payload.parsed),
                        payload.query.as_deref(),
                    )
                },
            );
        let upstream_url = node
            .upstream_url(upstream_endpoint, upstream_query)
            .map_err(|error| {
                warn!(node = node.id(), error = %error, "failed to build upstream URL");
                GatewayError::Internal
            })?;
        let native_vllm_messages = selected_protocol == Some(AnthropicProtocol::Native)
            && node.provider().kind == crate::config::ProviderKind::Vllm
            && upstream_endpoint == "messages";
        let vllm_codex_responses = request.codex_request
            && selected_protocol.is_none()
            && node.provider().kind == crate::config::ProviderKind::Vllm
            && upstream_endpoint == "responses";
        let upstream_endpoint_log = upstream_endpoint.to_owned();
        let (upstream_body, thinking_budget_approximated, codex_namespaces) = mapped_body(
            upstream_original,
            upstream_parsed,
            selection.upstream_model.as_deref(),
            request.public_model.as_deref(),
            native_vllm_messages,
            vllm_codex_responses,
        )?;
        let expose_thinking = request
            .anthropic_payloads
            .as_ref()
            .is_some_and(|payloads| payloads.expose_thinking);
        let response_mode = if let Some((_, prepared)) = recipe {
            UpstreamResponseMode::Deepseek(prepared)
        } else {
            match selected_protocol {
                None => codex_namespaces.map_or(UpstreamResponseMode::Passthrough, |namespaces| {
                    UpstreamResponseMode::Codex { namespaces }
                }),
                Some(AnthropicProtocol::Native) => UpstreamResponseMode::NativeAnthropic {
                    expose_thinking: native_vllm_messages || expose_thinking,
                    thinking_budget_approximated,
                },
                Some(AnthropicProtocol::Responses) => {
                    UpstreamResponseMode::ResponsesToAnthropic { expose_thinking }
                }
                Some(AnthropicProtocol::Chat) => {
                    UpstreamResponseMode::ChatToAnthropic { expose_thinking }
                }
                Some(AnthropicProtocol::Auto) => {
                    unreachable!("Anthropic protocol must be resolved")
                }
            }
        };
        let mut upstream_headers = HeaderMap::new();
        let connection_headers = connection_header_names(&request.headers);
        for (name, value) in &request.headers {
            if should_forward_request_header(name)
                && should_forward_protocol_header(
                    name,
                    if matches!(&response_mode, UpstreamResponseMode::Deepseek(_)) {
                        Some(AnthropicProtocol::Chat)
                    } else {
                        selected_protocol
                    },
                )
                && !connection_headers.contains(name)
            {
                upstream_headers.append(name, value.clone());
            }
        }
        for (name, value) in node.headers() {
            upstream_headers.insert(name, value.clone());
        }
        if let Ok(value) = HeaderValue::from_str(&request.request_id.0) {
            upstream_headers.insert(HeaderName::from_static("x-gateway-request-id"), value);
        }
        let mut log_attempt = request.observation.as_ref().map(|log| {
            let snapshot = node.snapshot();
            let guard = log.attempt(crate::session_log::AttemptRecord {
                number: attempt + 1,
                node: node.id().chars().take(128).collect(), node_instance: node.instance_id(),
                provider: serde_json::to_value(node.provider().kind).ok().and_then(|v| v.as_str().map(str::to_owned)).unwrap_or_default(),
                endpoint: upstream_endpoint_log.chars().take(128).collect(), model: selection.upstream_model.as_ref().map(|m| m.chars().take(256).collect()),
                adapter: response_mode.name().to_owned(), started_at_ms: crate::session_log::now_ms(), outcome: "started".to_owned(),
                route: serde_json::json!({"score":selection.score,"prefix_match_chars":selection.prefix_match_chars,"prefix_match_tokens":selection.prefix_match_tokens,
                    "active":snapshot.active,"capacity":snapshot.max_concurrency,"upstream_running":snapshot.upstream_running,"upstream_waiting":snapshot.upstream_waiting,
                    "kv_utilization":snapshot.kv_cache_usage,"telemetry_updated_at_ms":snapshot.provider_telemetry_updated_unix_ms}),
                ..crate::session_log::AttemptRecord::default()
            });
            guard.update(|record| {record.timings_us.insert("scheduler_wait".to_owned(), crate::session_log::micros(scheduler_wait));});
            guard.input(&upstream_body);
            guard
        });
        let upstream_request = state
            .client
            .request(request.method.clone(), upstream_url)
            .headers(upstream_headers)
            .body(upstream_body);

        attempt += 1;
        let upstream_started = Instant::now();
        let result = tokio::time::timeout(
            Duration::from_millis(state.settings.server.upstream_header_timeout_ms),
            upstream_request.send(),
        )
        .await;
        let response = match result {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                let retryable = error.is_connect()
                    && attempt < state.settings.retry.max_attempts
                    && has_untried_alternative(
                        &state,
                        request.public_model.as_deref(),
                        &excluded,
                        node.id(),
                    );
                if let Some(log) = &mut log_attempt {
                    log.finish("error", Some("transport_error"));
                    if retryable {
                        log.update(|a| a.retry_reason = Some("connect_error".to_owned()));
                    }
                }
                selection
                    .lease
                    .record_failure(error.to_string(), &state.settings.health);
                state.metrics.attempt(node.id(), "transport_error");
                if retryable {
                    state.metrics.retry(node.id(), "connect_error");
                    excluded.insert(node.id().to_owned());
                    drop(selection.lease);
                    continue;
                }
                warn!(node = node.id(), error = %error, "upstream request failed");
                return Err(GatewayError::Upstream("transport failure".to_owned()));
            }
            Err(_) => {
                if let Some(log) = &mut log_attempt {
                    log.finish("error", Some("header_timeout"));
                }
                selection
                    .lease
                    .record_failure("upstream response header timeout", &state.settings.health);
                state.metrics.attempt(node.id(), "header_timeout");
                return Err(GatewayError::UpstreamTimeout);
            }
        };

        let status = response.status();
        let header_latency = upstream_started.elapsed();
        if let Some(log) = &log_attempt {
            log.update(|record| {
                record.http_status = Some(status.as_u16());
                record.upstream_request_id = response
                    .headers()
                    .get("x-request-id")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.chars().take(128).collect());
                record.timings_us.insert(
                    "headers".to_owned(),
                    crate::session_log::micros(header_latency),
                );
            });
        }
        let configured_retry_status = state.settings.retry.statuses.contains(&status.as_u16());
        let retryable_status = configured_retry_status
            && attempt < state.settings.retry.max_attempts
            && has_untried_alternative(
                &state,
                request.public_model.as_deref(),
                &excluded,
                node.id(),
            );
        if status == StatusCode::TOO_MANY_REQUESTS
            || (configured_retry_status && !status.is_server_error())
        {
            selection.lease.record_overload();
        } else if status.is_server_error() {
            selection.lease.record_failure(
                format!("upstream returned {status}"),
                &state.settings.health,
            );
        }
        state.metrics.attempt(node.id(), status.as_str());

        if retryable_status {
            if let Some(log) = &mut log_attempt {
                log.finish("error", Some("upstream_status"));
                log.update(|a| a.retry_reason = Some(status.as_str().to_owned()));
            }
            state.metrics.retry(node.id(), status.as_str());
            excluded.insert(node.id().to_owned());
            drop(response);
            drop(selection.lease);
            continue;
        }

        debug!(
            node = node.id(),
            client_endpoint = %request.endpoint,
            upstream_endpoint = %upstream_endpoint_log,
            adapter = response_mode.name(),
            score = selection.score,
            prefix_match_chars = selection.prefix_match_chars,
            status = %status,
            "upstream selected"
        );
        let stream_idle_timeout =
            Duration::from_millis(state.settings.server.stream_idle_timeout_ms);
        let upstream_body_timeout =
            Duration::from_millis(state.settings.server.upstream_body_timeout_ms);
        if !status.is_success() {
            if !status.is_server_error() && status != StatusCode::TOO_MANY_REQUESTS {
                selection.lease.record_success(header_latency);
            }
            return proxy_error_response(
                response,
                selection.lease,
                Arc::clone(&state.response_buffer),
                stream_idle_timeout,
                upstream_body_timeout,
                request.client_protocol,
                &request.request_id.0,
                log_attempt,
            )
            .await;
        }
        if !request.streaming {
            let buffered = buffered_success_response(
                response,
                &selection.lease,
                &node,
                state.scheduler.prefix_directory(),
                &request.prefix_input,
                &state.settings.health,
                header_latency,
                stream_idle_timeout,
                upstream_body_timeout,
                state.settings.server.max_non_streaming_response_bytes,
                Arc::clone(&state.response_buffer),
                state.settings.server.expose_node_header,
                &response_mode,
                request.public_model.as_deref().unwrap_or_default(),
                request.record_prefix,
                &state.metrics,
                log_attempt.as_mut(),
            )
            .await;
            match buffered {
                Ok(response) => return Ok(response),
                Err(error) => {
                    let retryable = attempt < state.settings.retry.max_attempts
                        && has_untried_alternative(
                            &state,
                            request.public_model.as_deref(),
                            &excluded,
                            node.id(),
                        );
                    if let Some(log) = &mut log_attempt {
                        log.finish("error", Some("body_error"));
                        if retryable {
                            log.update(|a| a.retry_reason = Some("body_error".to_owned()));
                        }
                    }
                    if retryable {
                        state.metrics.retry(node.id(), "body_error");
                        excluded.insert(node.id().to_owned());
                        drop(selection.lease);
                        continue;
                    }
                    return Err(error);
                }
            }
        }
        return Ok(streaming_response(
            response,
            selection.lease,
            &node,
            Arc::clone(&state.metrics),
            Arc::clone(state.scheduler.prefix_directory()),
            request.prefix_input,
            state.settings.health.clone(),
            header_latency,
            upstream_started,
            stream_idle_timeout,
            upstream_body_timeout,
            Duration::from_millis(state.settings.server.downstream_stall_timeout_ms),
            state.settings.server.expose_node_header,
            response_mode,
            request.public_model.clone().unwrap_or_default(),
            request.record_prefix,
            log_attempt,
        ));
    }
}

pub(super) fn has_untried_alternative(
    state: &AppState,
    model: Option<&str>,
    excluded: &HashSet<String>,
    current: &str,
) -> bool {
    let mut next_excluded = excluded.clone();
    next_excluded.insert(current.to_owned());
    state.scheduler.has_alternative(model, &next_excluded)
}

pub(super) fn mapped_body(
    original: &Bytes,
    parsed: Option<&Value>,
    upstream_model: Option<&str>,
    public_model: Option<&str>,
    native_vllm_messages: bool,
    vllm_codex_responses: bool,
) -> Result<(Bytes, bool, Option<Arc<codex::NamespaceMap>>), GatewayError> {
    let rewrite_native_thinking = native_vllm_messages
        && parsed
            .and_then(|value| value.get("thinking"))
            .is_some_and(|value| !value.is_null());
    let remove_empty_native_tools = native_vllm_messages
        && parsed
            .and_then(Value::as_object)
            .is_some_and(crate::anthropic::empty_tools_are_noop);
    if !rewrite_native_thinking
        && !remove_empty_native_tools
        && !vllm_codex_responses
        && (upstream_model == public_model || upstream_model.is_none())
    {
        return Ok((original.clone(), false, None));
    }
    let mut value = parsed.cloned().ok_or(GatewayError::InvalidJson)?;
    let object = value.as_object_mut().ok_or_else(|| {
        GatewayError::InvalidRequest("JSON request body must be an object".to_owned())
    })?;
    let thinking_budget_approximated = if rewrite_native_thinking {
        apply_vllm_native_thinking_compat(object)?
    } else {
        false
    };
    if remove_empty_native_tools {
        // vLLM treats tools: [] as auto tool choice and requires a parser even
        // for a text-only Claude Code request. Omitting both keeps its meaning.
        object.remove("tools");
        object.remove("tool_choice");
    }
    let codex_namespaces = if vllm_codex_responses {
        codex::normalize_vllm_request(&mut value)?
    } else {
        None
    };
    let object = value.as_object_mut().ok_or_else(|| {
        GatewayError::InvalidRequest("JSON request body must be an object".to_owned())
    })?;
    if upstream_model != public_model
        && let Some(upstream_model) = upstream_model
    {
        object.insert("model".to_owned(), Value::String(upstream_model.to_owned()));
    }
    let body = sonic_rs::to_vec(&value)
        .map(Bytes::from)
        .map_err(|_| GatewayError::Internal)?;
    Ok((body, thinking_budget_approximated, codex_namespaces))
}
