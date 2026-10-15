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

use crate::{error::GatewayError, server::AppState};

use super::headers::{
    connection_header_names, should_forward_protocol_header, should_forward_request_header,
};
use super::response::{buffered_success_response, proxy_error_response};
use super::streaming::streaming_response;
use super::{ProxyRequest, UpstreamResponseMode};

#[allow(clippy::too_many_lines)]
pub(super) async fn proxy_with_retries(
    state: Arc<AppState>,
    request: ProxyRequest,
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
        if let Some(telemetry) = &request.telemetry {
            telemetry.queue(scheduler_wait);
        }
        let mut selection = selection?;
        if let Some(telemetry) = &request.telemetry {
            if let Some(model) = &request.public_model {
                telemetry.model(model);
            }
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
                warn!(node=node.id(), error=%error, "failed to build upstream URL");
                GatewayError::Internal
            })?;
        let upstream_endpoint_log = upstream_endpoint.to_owned();
        let upstream_body = mapped_body(
            &request.original_body,
            request.parsed_body.as_ref(),
            selection.upstream_model.as_deref(),
            request.public_model.as_deref(),
            request.count_tokens,
        )?;
        let response_mode =
            if request.count_tokens {
                UpstreamResponseMode::TokenCount
            } else {
                request.bridge_context.as_ref().map_or(
                    UpstreamResponseMode::Passthrough,
                    |context| UpstreamResponseMode::ChatBridge {
                        context: Arc::new(context.clone()),
                    },
                )
            };
        let mut upstream_headers = HeaderMap::new();
        let connection_headers = connection_header_names(&request.headers);
        for (name, value) in &request.headers {
            if should_forward_request_header(name)
                && should_forward_protocol_header(name)
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
                adapter: response_mode.name().to_owned(), started_at_ms: crate::time::unix_millis(), outcome: "started".to_owned(),
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
                    if let Some(telemetry) = &request.telemetry {
                        telemetry.retry();
                    }
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
            if let Some(telemetry) = &request.telemetry {
                telemetry.retry();
            }
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
                        if let Some(telemetry) = &request.telemetry {
                            telemetry.retry();
                        }
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
    count_tokens: bool,
) -> Result<Bytes, GatewayError> {
    if upstream_model == public_model && !count_tokens {
        return Ok(original.clone());
    }
    let mut value = parsed.cloned().ok_or(GatewayError::InvalidJson)?;
    let object = value.as_object_mut().ok_or_else(|| {
        GatewayError::InvalidRequest("JSON request body must be an object".to_owned())
    })?;
    if let Some(model) = upstream_model {
        object.insert("model".into(), model.into());
    }
    if count_tokens {
        object.insert("return_mm_kwargs".into(), false.into());
        object.insert("stream".into(), false.into());
    }
    sonic_rs::to_vec(&value)
        .map(Bytes::from)
        .map_err(|_| GatewayError::Internal)
}
