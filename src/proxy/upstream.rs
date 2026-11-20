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
use serde_json::{Map, Value};
use tracing::{debug, warn};

use crate::{error::GatewayError, server::AppState, time::micros};

use super::headers::{connection_header_names, should_forward_request_header};
use super::response::{BufferedContext, buffered_success_response, proxy_error_response};
use super::streaming::{StreamingContext, streaming_response};
use super::{ProxyRequest, UpstreamResponseMode};

#[allow(clippy::too_many_lines)]
pub(super) async fn proxy_with_retries(
    state: Arc<AppState>,
    mut request: ProxyRequest,
) -> Result<Response, GatewayError> {
    let mut excluded = HashSet::new();
    let mut attempt = 0usize;
    loop {
        let queue_started = Instant::now();
        if let Some(telemetry) = &request.telemetry {
            telemetry.wait_started(false);
        }
        let selection = state
            .scheduler
            .acquire(
                Some(&request.public_model),
                request.prefix_input.clone(),
                &excluded,
                request.original_body.len(),
            )
            .await;
        let scheduler_wait = queue_started.elapsed();
        state
            .metrics
            .observe_queue_duration(scheduler_wait.as_secs_f64());
        if let Some(telemetry) = &request.telemetry {
            telemetry.queue(scheduler_wait);
        }
        let mut selection = selection?;
        if let Some(telemetry) = &request.telemetry {
            telemetry.model(&request.public_model);
            telemetry.assign(selection.node.id(), selection.node.instance_id());
            telemetry.route(selection.prefix_match_tokens, selection.prefix_match_chars);
            selection.lease.observation = Some(Arc::clone(telemetry));
        }
        state
            .metrics
            .observe_prefix_match(selection.prefix_match_chars);
        state
            .metrics
            .observe_prefix_match_tokens(selection.prefix_match_tokens);

        let node = Arc::clone(&selection.node);
        let upstream_endpoint = if request.count_tokens {
            "chat/completions/render"
        } else {
            &request.endpoint
        };
        let upstream_url = node
            .upstream_url(upstream_endpoint, request.query.as_deref())
            .map_err(|error| {
                warn!(node = node.id(), error = %error, "failed to build upstream URL");
                GatewayError::Internal
            })?;
        let upstream_body = mapped_body_in_place(
            &request.original_body,
            Some(&mut request.parsed_body),
            selection.upstream_model.as_deref(),
            Some(&request.public_model),
            request.count_tokens,
        )?;
        let response_mode =
            if request.count_tokens {
                UpstreamResponseMode::TokenCount
            } else {
                request.bridge_context.as_ref().map_or(
                    UpstreamResponseMode::Passthrough,
                    |context| UpstreamResponseMode::ChatBridge {
                        context: Box::new(context.clone()),
                    },
                )
            };
        let upstream_headers = build_upstream_headers(&request, &node);
        let mut log_attempt = request.observation.as_ref().map(|log| {
            let snapshot = node.snapshot();
            let guard = log.attempt(crate::session_log::AttemptRecord {
                number: attempt + 1,
                node: node.id().chars().take(128).collect(),
                node_instance: node.instance_id(),
                provider: serde_json::to_value(node.provider().kind)
                    .ok()
                    .and_then(|v| v.as_str().map(str::to_owned))
                    .unwrap_or_default(),
                endpoint: upstream_endpoint.chars().take(128).collect(),
                model: selection
                    .upstream_model
                    .as_ref()
                    .map(|m| m.chars().take(256).collect()),
                adapter: response_mode.name().to_owned(),
                started_at_ms: crate::time::unix_millis(),
                outcome: "started".to_owned(),
                route: serde_json::json!({
                    "score": selection.score,
                    "prefix_match_chars": selection.prefix_match_chars,
                    "prefix_match_tokens": selection.prefix_match_tokens,
                    "active": snapshot.active,
                    "capacity": snapshot.max_concurrency,
                    "upstream_running": snapshot.upstream_running,
                    "upstream_waiting": snapshot.upstream_waiting,
                    "kv_utilization": snapshot.kv_cache_usage,
                    "telemetry_updated_at_ms": snapshot.provider_telemetry_updated_unix_ms
                }),
                ..crate::session_log::AttemptRecord::default()
            });
            guard.update(|record| {
                record
                    .timings_us
                    .insert("scheduler_wait".to_owned(), micros(scheduler_wait));
            });
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
                selection
                    .lease
                    .record_failure(error.to_string(), &state.settings.health);
                state.metrics.attempt(node.id(), "transport_error");
                if try_retry(
                    error.is_connect(),
                    attempt,
                    &state,
                    &request,
                    node.id(),
                    &mut excluded,
                    &mut log_attempt,
                    "transport_error",
                    "connect_error",
                ) {
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
                record
                    .timings_us
                    .insert("headers".to_owned(), micros(header_latency));
            });
        }
        let configured_retry_status = state.settings.retry.statuses.contains(&status.as_u16());
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

        if try_retry(
            configured_retry_status,
            attempt,
            &state,
            &request,
            node.id(),
            &mut excluded,
            &mut log_attempt,
            "upstream_status",
            status.as_str(),
        ) {
            drop(response);
            drop(selection.lease);
            continue;
        }

        debug!(
            node = node.id(),
            client_endpoint = %request.endpoint,
            upstream_endpoint = %upstream_endpoint,
            adapter = response_mode.name(),
            score = selection.score,
            prefix_match_chars = selection.prefix_match_chars,
            status = %status,
            "upstream selected"
        );
        if !status.is_success() {
            if let Some(admission) = &request.admission {
                admission.release_bytes();
            }
            if !status.is_server_error() && status != StatusCode::TOO_MANY_REQUESTS {
                selection.lease.record_success(header_latency);
            }
            return proxy_error_response(
                response,
                selection.lease,
                &state,
                request.client_protocol,
                &request.request_id.0,
                log_attempt,
            )
            .await;
        }
        if !request.streaming {
            let buffered = buffered_success_response(
                response,
                &BufferedContext {
                    state: &state,
                    lease: &selection.lease,
                    node: &node,
                    prefix_input: &request.prefix_input,
                    header_latency,
                    response_mode: &response_mode,
                    public_model: &request.public_model,
                    record_prefix: request.record_prefix,
                },
                log_attempt.as_mut(),
            )
            .await;
            match buffered {
                Ok(response) => {
                    if let Some(admission) = &request.admission {
                        admission.release_bytes();
                    }
                    return Ok(response);
                }
                Err(error) => {
                    if try_retry(
                        true,
                        attempt,
                        &state,
                        &request,
                        node.id(),
                        &mut excluded,
                        &mut log_attempt,
                        "body_error",
                        "body_error",
                    ) {
                        drop(selection.lease);
                        continue;
                    }
                    return Err(error);
                }
            }
        }
        if let Some(admission) = &request.admission {
            admission.release_bytes();
        }
        return Ok(streaming_response(
            response,
            StreamingContext {
                state,
                lease: selection.lease,
                prefix_input: request.prefix_input,
                header_latency,
                upstream_started,
                response_mode,
                public_model: request.public_model,
                record_prefix: request.record_prefix,
                log_attempt,
            },
        ));
    }
}

fn build_upstream_headers(request: &ProxyRequest, node: &crate::node::Node) -> HeaderMap {
    let mut headers = HeaderMap::new();
    let connection_headers = connection_header_names(&request.headers);
    for (name, value) in &request.headers {
        if should_forward_request_header(name) && !connection_headers.contains(name) {
            headers.append(name, value.clone());
        }
    }
    for (name, value) in node.headers() {
        headers.insert(name, value.clone());
    }
    if let Ok(value) = HeaderValue::from_str(&request.request_id.0) {
        headers.insert(HeaderName::from_static("x-gateway-request-id"), value);
    }
    headers
}

#[allow(clippy::too_many_arguments)]
fn try_retry(
    eligible: bool,
    attempt: usize,
    state: &AppState,
    request: &ProxyRequest,
    node_id: &str,
    excluded: &mut HashSet<String>,
    log_attempt: &mut Option<crate::session_log::AttemptGuard>,
    error_class: &str,
    retry_reason: &str,
) -> bool {
    let retryable = eligible
        && attempt < state.settings.retry.max_attempts
        && has_untried_alternative(state, Some(&request.public_model), excluded, node_id);
    if (retryable || error_class != "upstream_status")
        && let Some(log) = log_attempt
    {
        log.finish("error", Some(error_class));
        if retryable {
            log.update(|a| a.retry_reason = Some(retry_reason.to_owned()));
        }
    }
    if !retryable {
        return false;
    }
    if let Some(telemetry) = &request.telemetry {
        telemetry.retry();
    }
    state.metrics.retry(node_id, retry_reason);
    excluded.insert(node_id.to_owned());
    true
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

enum PatchedField {
    Unchanged,
    Inserted,
    Replaced(Value),
}

fn patch_field(object: &mut Map<String, Value>, key: &str, value: Option<Value>) -> PatchedField {
    match value {
        None => PatchedField::Unchanged,
        Some(new) => match object.insert(key.to_owned(), new) {
            Some(old) => PatchedField::Replaced(old),
            None => PatchedField::Inserted,
        },
    }
}

fn restore_field(object: &mut Map<String, Value>, key: &str, patched: PatchedField) {
    match patched {
        PatchedField::Unchanged => {}
        PatchedField::Inserted => {
            object.remove(key);
        }
        PatchedField::Replaced(old) => {
            object.insert(key.to_owned(), old);
        }
    }
}

/// Serialize the upstream request body by temporarily patching only the
/// top-level fields (`model`, `return_mm_kwargs`, `stream`) in place and
/// restoring them afterwards, avoiding a full deep clone of large JSON prompts.
pub(super) fn mapped_body_in_place(
    original: &Bytes,
    parsed: Option<&mut Value>,
    upstream_model: Option<&str>,
    public_model: Option<&str>,
    count_tokens: bool,
) -> Result<Bytes, GatewayError> {
    if upstream_model == public_model && !count_tokens {
        return Ok(original.clone());
    }
    let value = parsed.ok_or(GatewayError::InvalidJson)?;
    let object = value.as_object_mut().ok_or_else(|| {
        GatewayError::InvalidRequest("JSON request body must be an object".to_owned())
    })?;
    let previous_model = patch_field(object, "model", upstream_model.map(Into::into));
    let previous_mm = patch_field(
        object,
        "return_mm_kwargs",
        count_tokens.then_some(false.into()),
    );
    let previous_stream = patch_field(object, "stream", count_tokens.then_some(false.into()));

    let encoded = serde_json::to_vec(&*object)
        .map(Bytes::from)
        .map_err(|_| GatewayError::Internal);

    restore_field(object, "model", previous_model);
    restore_field(object, "return_mm_kwargs", previous_mm);
    restore_field(object, "stream", previous_stream);
    encoded
}

#[cfg(test)]
pub(super) fn mapped_body(
    original: &Bytes,
    parsed: Option<&Value>,
    upstream_model: Option<&str>,
    public_model: Option<&str>,
    count_tokens: bool,
) -> Result<Bytes, GatewayError> {
    let mut cloned = parsed.cloned();
    mapped_body_in_place(
        original,
        cloned.as_mut(),
        upstream_model,
        public_model,
        count_tokens,
    )
}
