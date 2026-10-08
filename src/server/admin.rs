use std::{net::IpAddr, sync::Arc, time::Duration};

use anyhow::Result;
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::error;

use crate::{
    config::{NodeConfig, validate_node_config},
    health::preflight_health,
    node::{CircuitState, LifecycleState, Node, NodeSnapshot},
    store::StoredNode,
    vllm::preflight_vllm,
};

use super::{AppState, unix_millis};

pub(super) async fn live() -> Json<serde_json::Value> {
    Json(json!({"status": "ok"}))
}

pub(super) async fn ready(State(state): State<Arc<AppState>>) -> Response {
    let ready = state.process.accepting_traffic() && state.scheduler.ready();
    (
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(json!({
            "status": if ready { "ready" } else { "not_ready" }
        })),
    )
        .into_response()
}

pub(super) async fn process_status(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let response_buffer = state.response_buffer.snapshot();
    Json(json!({
        "process": state.process.snapshot(),
        "runtime_ready": state.scheduler.ready(),
        "queue": {
            "requests": state.scheduler.queue_snapshot().0,
            "bytes": state.scheduler.queue_snapshot().1,
        },
        "response_buffer": {
            "used_bytes": response_buffer.used_bytes,
            "max_bytes": response_buffer.max_bytes,
            "waiting_responses": response_buffer.waiting_responses,
        },
    }))
}

pub(super) async fn activate_process(State(state): State<Arc<AppState>>) -> Response {
    let activated = state.process.activate();
    (
        StatusCode::OK,
        Json(json!({
            "activated": activated,
            "process": state.process.snapshot(),
            "runtime_ready": state.scheduler.ready(),
        })),
    )
        .into_response()
}

pub(super) async fn drain_process(State(state): State<Arc<AppState>>) -> Response {
    let initiated = state.process.request_shutdown();
    (
        StatusCode::ACCEPTED,
        Json(json!({
            "initiated": initiated,
            "process": state.process.snapshot(),
        })),
    )
        .into_response()
}

pub(super) async fn metrics(State(state): State<Arc<AppState>>) -> Response {
    match state.metrics.encode(&state.scheduler) {
        Ok(body) => (
            [(
                "content-type",
                "application/openmetrics-text; version=1.0.0; charset=utf-8",
            )],
            body,
        )
            .into_response(),
        Err(error) => {
            error!(error = %error, "failed to encode metrics");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub(super) async fn admin_nodes(State(state): State<Arc<AppState>>) -> Response {
    match state.store.list_async().await {
        Ok(nodes) => Json(json!({
            "nodes": nodes
                .into_iter()
                .filter_map(|stored| {
                    let node = state.scheduler.node(&stored.config.id)?;
                    Some(admin_node_payload(&state, &stored, &node))
                })
                .collect::<Vec<_>>()
        }))
        .into_response(),
        Err(error) => admin_internal_error("could not load node configurations", &error),
    }
}

#[derive(Debug, Serialize)]
// These independent flags are part of the admin diagnostic contract, not one state machine.
#[allow(clippy::struct_excessive_bools)]
pub(super) struct AdminAdmissionSnapshot {
    pub(super) state: &'static str,
    pub(super) reason: &'static str,
    pub(super) routable: bool,
    pub(super) accepting_assignments: bool,
    pub(super) telemetry_fresh: bool,
    pub(super) waiting_watermark_blocked: bool,
}

pub(super) fn admin_admission_snapshot(
    node: &Node,
    snapshot: &NodeSnapshot,
) -> AdminAdmissionSnapshot {
    let fresh_waiting = node.fresh_vllm_waiting();
    let telemetry_fresh =
        node.provider().kind != crate::config::ProviderKind::Vllm || fresh_waiting.is_some();
    let waiting_watermark_blocked =
        fresh_waiting.is_some_and(|waiting| waiting >= node.provider().waiting_threshold);
    let routable = node.is_routable();

    let (state, reason) = if snapshot.lifecycle == LifecycleState::Draining {
        ("draining", "Node is draining")
    } else if !node.is_health_state_routable(snapshot.health) {
        ("health_blocked", "Health state does not permit routing")
    } else if !node.provider_is_ready() {
        (
            "provider_blocked",
            "Provider compatibility check does not permit routing",
        )
    } else if snapshot.circuit == CircuitState::Open {
        ("circuit_open", "Circuit breaker is open")
    } else if !routable {
        (
            "circuit_limited",
            "Circuit breaker half-open capacity is exhausted",
        )
    } else if waiting_watermark_blocked {
        (
            "waiting_watermark",
            "Fresh upstream waiting depth reached its watermark",
        )
    } else if snapshot.available == 0 {
        ("at_capacity", "All local concurrency permits are in use")
    } else {
        ("accepting", "Eligible for a new assignment")
    };

    AdminAdmissionSnapshot {
        state,
        reason,
        routable,
        accepting_assignments: state == "accepting",
        telemetry_fresh,
        waiting_watermark_blocked,
    }
}

pub(super) async fn admin_status(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let nodes = state.scheduler.nodes();
    let mut routable_nodes = 0;
    let mut accepting_nodes = 0;
    let mut active_requests = 0;
    let mut total_concurrency = 0;
    let mut available_concurrency = 0;

    for node in &nodes {
        let snapshot = node.snapshot();
        let admission = admin_admission_snapshot(node, &snapshot);
        routable_nodes += usize::from(admission.routable);
        accepting_nodes += usize::from(admission.accepting_assignments);
        active_requests += snapshot.active;
        total_concurrency += snapshot.max_concurrency;
        if admission.accepting_assignments {
            available_concurrency += snapshot.available;
        }
    }
    let (queued_requests, queued_bytes) = state.scheduler.queue_snapshot();
    let response_buffer = state.response_buffer.snapshot();
    let (top_ips, ip_limits) = state.connections.snapshot();
    let ready = state.process.accepting_traffic() && routable_nodes > 0;

    Json(json!({
        "status": if ready { "ready" } else { "not_ready" },
        "live": true,
        "ready": ready,
        "version": crate::VERSION,
        "process": state.process.snapshot(),
        "generated_at_unix_ms": unix_millis(),
        "fleet": {
            "total_nodes": nodes.len(),
            "routable_nodes": routable_nodes,
            "accepting_nodes": accepting_nodes,
            "models": state.scheduler.models().len(),
            "active_requests": active_requests,
            "total_concurrency": total_concurrency,
            "available_concurrency": available_concurrency,
        },
        "queue": {
            "requests": queued_requests,
            "bytes": queued_bytes,
            "admission_waiters": state.scheduler.admission_waiters(),
            "max_requests": state.settings.routing.queue_max_requests,
            "max_bytes": state.settings.routing.queue_max_bytes,
        },
        "connections": {
            "public": state.metrics.public_connections(),
            "max_public": state.settings.server.max_connections,
            "top_ips": top_ips.into_iter().map(|(ip, active)| json!({
                "ip": ip,
                "active": active,
            })).collect::<Vec<_>>(),
            "ip_limits": ip_limits.into_iter().map(|(ip, limit)| json!({
                "ip": ip,
                "limit": limit,
            })).collect::<Vec<_>>(),
        },
        "response_buffer": {
            "used_bytes": response_buffer.used_bytes,
            "max_bytes": response_buffer.max_bytes,
            "waiting_responses": response_buffer.waiting_responses,
        },
        "routing": {
            "prefix_enabled": state.settings.routing.prefix.enabled,
        }
    }))
}

#[derive(Deserialize)]
pub(super) struct IpLimitInput {
    pub(super) limit: usize,
}

pub(super) async fn set_ip_limit(
    State(state): State<Arc<AppState>>,
    Path(ip): Path<String>,
    Json(input): Json<IpLimitInput>,
) -> Response {
    let Ok(ip) = ip.parse::<IpAddr>() else {
        return admin_message(StatusCode::BAD_REQUEST, "invalid_ip", "invalid IP address");
    };
    if input.limit == 0 || input.limit > state.settings.server.max_connections {
        return admin_message(
            StatusCode::BAD_REQUEST,
            "invalid_limit",
            "limit must be between 1 and the public connection limit",
        );
    }
    state.connections.set_limit(ip, input.limit);
    Json(json!({"ip": ip, "limit": input.limit})).into_response()
}

pub(super) async fn delete_ip_limit(
    State(state): State<Arc<AppState>>,
    Path(ip): Path<String>,
) -> Response {
    let Ok(ip) = ip.parse::<IpAddr>() else {
        return admin_message(StatusCode::BAD_REQUEST, "invalid_ip", "invalid IP address");
    };
    Json(json!({"deleted": state.connections.remove_limit(ip)})).into_response()
}

pub(super) async fn admin_node(
    State(state): State<Arc<AppState>>,
    Path(node_id): Path<String>,
) -> Response {
    let stored = match state.store.get_async(&node_id).await {
        Ok(Some(stored)) => stored,
        Ok(None) => return admin_node_not_found(&node_id),
        Err(error) => return admin_internal_error("could not load node configuration", &error),
    };
    let Some(node) = state.scheduler.node(&node_id) else {
        return admin_internal_message("node is persisted but missing from the runtime registry");
    };
    Json(admin_node_payload(&state, &stored, &node)).into_response()
}

pub(super) fn admin_node_payload(
    state: &AppState,
    stored: &StoredNode,
    node: &Node,
) -> serde_json::Value {
    let cache = state.scheduler.exact_cache_directory().snapshot(node.id());
    let snapshot = node.snapshot();
    let admission = admin_admission_snapshot(node, &snapshot);
    let mut public_config = stored.config.clone();
    public_config.api_key = None;
    public_config.headers.clear();
    let mut header_names = stored.config.headers.keys().cloned().collect::<Vec<_>>();
    header_names.sort();
    let api_key_source = if stored.config.api_key.is_some() {
        "database"
    } else if stored.config.api_key_env.is_some() {
        "environment"
    } else {
        "none"
    };
    json!({
        "config": public_config,
        "credentials": {
            "api_key_configured": api_key_source != "none",
            "api_key_source": api_key_source,
            "header_names": header_names,
        },
        "revision": stored.revision,
        "created_at_unix_ms": stored.created_at_unix_ms,
        "updated_at_unix_ms": stored.updated_at_unix_ms,
        "runtime": snapshot,
        "admission": admission,
        "exact_kv_authoritative": cache.authoritative,
        "exact_kv_blocks": cache.blocks,
        "exact_kv_bytes": cache.bytes,
    })
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct CredentialMutationQuery {
    pub(super) clear_api_key: bool,
    pub(super) clear_headers: bool,
}

pub(super) async fn preflight_node(
    State(state): State<Arc<AppState>>,
    Query(query): Query<CredentialMutationQuery>,
    Json(mut config): Json<NodeConfig>,
) -> Response {
    if config.api_key.is_none() && !query.clear_api_key {
        match state.store.get_async(&config.id).await {
            Ok(Some(stored)) => config.api_key = stored.config.api_key,
            Ok(None) => {}
            Err(error) => {
                return admin_internal_error("could not load stored credentials", &error);
            }
        }
    }
    if config.headers.is_empty() && !query.clear_headers {
        match state.store.get_async(&config.id).await {
            Ok(Some(stored)) => config.headers = stored.config.headers,
            Ok(None) => {}
            Err(error) => {
                return admin_internal_error("could not load stored headers", &error);
            }
        }
    }
    let node = match prepare_node(&state, &config).await {
        Ok(node) => node,
        Err(error) => return admin_validation_error(&error),
    };
    let snapshot = node.snapshot();
    let admission = admin_admission_snapshot(&node, &snapshot);
    Json(json!({
        "ok": true,
        "runtime": snapshot,
        "admission": admission,
        "checks": {
            "configuration": "passed",
            "provider": "passed",
            "health": "passed",
        }
    }))
    .into_response()
}

pub(super) async fn create_node(
    State(state): State<Arc<AppState>>,
    Json(config): Json<NodeConfig>,
) -> Response {
    let _mutation = state.admin_mutation.lock().await;
    if state.scheduler.node(&config.id).is_some() {
        return admin_conflict("node_already_exists", "a node with this id already exists");
    }
    let node = match prepare_node(&state, &config).await {
        Ok(node) => node,
        Err(error) => return admin_validation_error(&error),
    };
    if matches!(state.store.get_async(&config.id).await, Ok(Some(_))) {
        return admin_conflict("node_already_exists", "a node with this id already exists");
    }
    let stored = match state.store.insert_async(&config).await {
        Ok(stored) => stored,
        Err(error) => return admin_internal_error("could not persist node", &error),
    };
    if let Err(error) = state.scheduler.add_node(Arc::clone(&node)) {
        let _ = state
            .store
            .delete_async(&config.id, Some(stored.revision))
            .await;
        return admin_internal_error("could not add node to runtime registry", &error);
    }
    state
        .runtime_revisions
        .write()
        .insert(config.id.clone(), stored.revision);
    (
        StatusCode::CREATED,
        Json(admin_node_payload(&state, &stored, &node)),
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct UpdateNodeRequest {
    pub(super) revision: u64,
    pub(super) config: NodeConfig,
    #[serde(default)]
    pub(super) clear_api_key: bool,
    #[serde(default)]
    pub(super) clear_headers: bool,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct MutationQuery {
    pub(super) timeout_ms: Option<u64>,
    pub(super) revision: Option<u64>,
}

pub(super) async fn update_node(
    State(state): State<Arc<AppState>>,
    Path(node_id): Path<String>,
    Query(query): Query<MutationQuery>,
    Json(request): Json<UpdateNodeRequest>,
) -> Response {
    let _mutation = state.admin_mutation.lock().await;
    if request.config.id != node_id {
        return admin_message(
            StatusCode::UNPROCESSABLE_ENTITY,
            "node_id_mismatch",
            "the request path and config node id must match",
        );
    }
    let stored = match state.store.get_async(&node_id).await {
        Ok(Some(stored)) => stored,
        Ok(None) => return admin_node_not_found(&node_id),
        Err(error) => return admin_internal_error("could not load node configuration", &error),
    };
    if stored.revision != request.revision {
        return admin_conflict(
            "revision_conflict",
            "the node changed after this editor loaded it; refresh and retry",
        );
    }
    let Some(previous) = state.scheduler.node(&node_id) else {
        return admin_internal_message("node is persisted but missing from the runtime registry");
    };
    let mut requested_config = request.config;
    if requested_config.api_key.is_none() && !request.clear_api_key {
        requested_config.api_key.clone_from(&stored.config.api_key);
    }
    if requested_config.headers.is_empty() && !request.clear_headers {
        requested_config.headers.clone_from(&stored.config.headers);
    }
    let replacement = match prepare_node(&state, &requested_config).await {
        Ok(node) => node,
        Err(error) => return admin_validation_error(&error),
    };
    let was_draining = previous.lifecycle() == crate::node::LifecycleState::Draining;
    previous.set_draining(true);
    state.scheduler.notify_state_change();
    let timeout = mutation_timeout(&state, query.timeout_ms);
    if !state.scheduler.wait_for_node_idle(&previous, timeout).await {
        return admin_conflict(
            "node_still_active",
            "the node is draining but still has active requests; retry the update later",
        );
    }
    let updated = match state
        .store
        .update_async(&node_id, request.revision, &requested_config)
        .await
    {
        Ok(Some(updated)) => updated,
        Ok(None) => {
            previous.set_draining(was_draining);
            return admin_conflict(
                "revision_conflict",
                "the node changed while the update was being applied",
            );
        }
        Err(error) => {
            previous.set_draining(was_draining);
            return admin_internal_error("could not persist node update", &error);
        }
    };
    if let Err(error) = state.scheduler.replace_node(&replacement) {
        return admin_internal_error("could not replace runtime node", &error);
    }
    state
        .runtime_revisions
        .write()
        .insert(node_id, updated.revision);
    Json(admin_node_payload(&state, &updated, &replacement)).into_response()
}

pub(super) async fn delete_node(
    State(state): State<Arc<AppState>>,
    Path(node_id): Path<String>,
    Query(query): Query<MutationQuery>,
) -> Response {
    let _mutation = state.admin_mutation.lock().await;
    let stored = match state.store.get_async(&node_id).await {
        Ok(Some(stored)) => stored,
        Ok(None) => return admin_node_not_found(&node_id),
        Err(error) => return admin_internal_error("could not load node configuration", &error),
    };
    if query
        .revision
        .is_some_and(|revision| revision != stored.revision)
    {
        return admin_conflict(
            "revision_conflict",
            "the node changed after this editor loaded it; refresh and retry",
        );
    }
    let Some(node) = state.scheduler.node(&node_id) else {
        return admin_internal_message("node is persisted but missing from the runtime registry");
    };
    let was_draining = node.lifecycle() == crate::node::LifecycleState::Draining;
    node.set_draining(true);
    state.scheduler.notify_state_change();
    if !state
        .scheduler
        .wait_for_node_idle(&node, mutation_timeout(&state, query.timeout_ms))
        .await
    {
        return admin_conflict(
            "node_still_active",
            "the node is draining but still has active requests; retry deletion later",
        );
    }
    match state
        .store
        .delete_async(&node_id, Some(stored.revision))
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            node.set_draining(was_draining);
            return admin_conflict(
                "revision_conflict",
                "the node changed while deletion was being applied",
            );
        }
        Err(error) => {
            node.set_draining(was_draining);
            return admin_internal_error("could not delete persisted node", &error);
        }
    }
    state.scheduler.remove_node(&node_id);
    state.metrics.remove_node(&node_id);
    state.runtime_revisions.write().remove(&node_id);
    Json(json!({"deleted": true, "node": node_id})).into_response()
}

pub(super) async fn prepare_node(state: &AppState, config: &NodeConfig) -> Result<Arc<Node>> {
    validate_node_config(config)?;
    let node = Node::from_config_with_policies(
        config,
        state.settings.health.route_while_starting,
        state.settings.circuit_breaker.clone(),
    )?;
    preflight_vllm(&state.client, &node).await?;
    preflight_health(&state.client, &node, &state.settings.health).await?;
    Ok(node)
}

pub(super) fn mutation_timeout(state: &AppState, timeout_ms: Option<u64>) -> Duration {
    Duration::from_millis(
        timeout_ms
            .unwrap_or(state.settings.server.node_mutation_timeout_ms)
            .min(3_600_000),
    )
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct DrainQuery {
    pub(super) wait: bool,
    pub(super) timeout_ms: Option<u64>,
}

pub(super) async fn drain_node(
    State(state): State<Arc<AppState>>,
    Path(node_id): Path<String>,
    Query(query): Query<DrainQuery>,
) -> Response {
    let _mutation = state.admin_mutation.lock().await;
    let mut stored = match state.store.get_async(&node_id).await {
        Ok(Some(stored)) => stored,
        Ok(None) => return admin_node_not_found(&node_id),
        Err(error) => return admin_internal_error("could not load node configuration", &error),
    };
    stored.config.draining = true;
    let stored = match state
        .store
        .update_async(&node_id, stored.revision, &stored.config)
        .await
    {
        Ok(Some(stored)) => stored,
        Ok(None) => {
            return admin_conflict(
                "revision_conflict",
                "the node changed while draining was being applied",
            );
        }
        Err(error) => return admin_internal_error("could not persist draining state", &error),
    };
    let Some(node) = state.scheduler.set_node_draining(&node_id, true) else {
        return admin_internal_message("node is persisted but missing from the runtime registry");
    };
    state
        .runtime_revisions
        .write()
        .insert(node_id.clone(), stored.revision);
    let drained = if query.wait {
        let timeout_ms = query
            .timeout_ms
            .unwrap_or(state.settings.server.node_mutation_timeout_ms)
            .min(3_600_000);
        state
            .scheduler
            .wait_for_node_idle(&node, Duration::from_millis(timeout_ms))
            .await
    } else {
        node.active() == 0
    };
    let status = if drained {
        StatusCode::OK
    } else {
        StatusCode::ACCEPTED
    };
    (
        status,
        Json(json!({
            "node": node.id(),
            "lifecycle": node.lifecycle(),
            "active": node.active(),
            "drained": drained,
            "revision": stored.revision,
        })),
    )
        .into_response()
}

pub(super) async fn resume_node(
    State(state): State<Arc<AppState>>,
    Path(node_id): Path<String>,
) -> Response {
    let _mutation = state.admin_mutation.lock().await;
    let mut stored = match state.store.get_async(&node_id).await {
        Ok(Some(stored)) => stored,
        Ok(None) => return admin_node_not_found(&node_id),
        Err(error) => return admin_internal_error("could not load node configuration", &error),
    };
    stored.config.draining = false;
    let stored = match state
        .store
        .update_async(&node_id, stored.revision, &stored.config)
        .await
    {
        Ok(Some(stored)) => stored,
        Ok(None) => {
            return admin_conflict(
                "revision_conflict",
                "the node changed while resume was being applied",
            );
        }
        Err(error) => return admin_internal_error("could not persist serving state", &error),
    };
    let Some(node) = state.scheduler.set_node_draining(&node_id, false) else {
        return admin_internal_message("node is persisted but missing from the runtime registry");
    };
    state
        .runtime_revisions
        .write()
        .insert(node_id, stored.revision);
    Json(json!({
        "node": node.id(),
        "lifecycle": node.lifecycle(),
        "routable": node.is_routable(),
        "revision": stored.revision,
    }))
    .into_response()
}

pub(super) fn admin_node_not_found(node_id: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({
            "error": {
                "message": format!("node {node_id:?} does not exist"),
                "type": "invalid_request_error",
                "code": "node_not_found"
            }
        })),
    )
        .into_response()
}

pub(super) fn admin_validation_error(error: &anyhow::Error) -> Response {
    admin_message(
        StatusCode::UNPROCESSABLE_ENTITY,
        "node_validation_failed",
        &error.to_string(),
    )
}

pub(super) fn admin_conflict(code: &'static str, message: &'static str) -> Response {
    admin_message(StatusCode::CONFLICT, code, message)
}

pub(super) fn admin_internal_message(message: &'static str) -> Response {
    error!(message, "admin runtime consistency error");
    admin_message(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", message)
}

pub(super) fn admin_internal_error(
    message: &'static str,
    error: &dyn std::fmt::Display,
) -> Response {
    error!(error = %error, message, "admin operation failed");
    admin_message(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", message)
}

pub(super) fn admin_message(status: StatusCode, code: &'static str, message: &str) -> Response {
    (
        status,
        Json(json!({
            "error": {
                "message": message,
                "type": "admin_error",
                "code": code,
            }
        })),
    )
        .into_response()
}
