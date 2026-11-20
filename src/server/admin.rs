use std::{
    convert::Infallible,
    net::IpAddr,
    sync::{Arc, atomic::Ordering as AtomicOrdering},
    time::Duration,
};

use anyhow::Result;
use axum::{
    Json,
    extract::{Path, Query, State},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{ETAG, IF_NONE_MATCH},
    },
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
};
use futures_util::Stream;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::error;

use crate::{
    config::{NodeConfig, validate_node_config},
    health::preflight_health,
    node::{CircuitState, LifecycleState, Node, NodeSnapshot},
    store::StoredNode,
    time::unix_millis,
    vllm::preflight_vllm,
};

use super::AppState;

pub(super) struct AdminError {
    status: StatusCode,
    error_type: &'static str,
    code: &'static str,
    message: String,
}

impl AdminError {
    pub(super) fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            error_type: "admin_error",
            code,
            message: message.into(),
        }
    }

    pub(super) fn bad_request(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, code, message)
    }

    pub(super) fn not_found(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, code, message)
    }

    pub(super) fn node_not_found(node_id: &str) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            error_type: "invalid_request_error",
            code: "node_not_found",
            message: format!("node {node_id:?} does not exist"),
        }
    }

    pub(super) fn validation(error: &anyhow::Error) -> Self {
        Self::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "node_validation_failed",
            error.to_string(),
        )
    }

    pub(super) fn conflict(code: &'static str, message: &'static str) -> Self {
        Self::new(StatusCode::CONFLICT, code, message)
    }

    pub(super) fn internal_message(message: &'static str) -> Self {
        error!(message, "admin runtime consistency error");
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", message)
    }

    pub(super) fn internal(message: &'static str, error: &dyn std::fmt::Display) -> Self {
        error!(error = %error, message, "admin operation failed");
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", message)
    }
}

impl IntoResponse for AdminError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({
                "error": {
                    "message": self.message,
                    "type": self.error_type,
                    "code": self.code,
                }
            })),
        )
            .into_response()
    }
}

type AdminResult<T = Response> = std::result::Result<T, AdminError>;

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
    let (requests, bytes) = state.scheduler.queue_snapshot();
    let response_buffer = state.response_buffer.snapshot();
    Json(json!({
        "process": state.process.snapshot(),
        "runtime_ready": state.scheduler.ready(),
        "queue": {
            "requests": requests,
            "bytes": bytes,
        },
        "response_buffer": {
            "used_bytes": response_buffer.used_bytes,
            "max_bytes": response_buffer.max_bytes,
            "waiting_responses": response_buffer.waiting_responses,
        },
    }))
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
    match state.metrics.encode(
        &state.scheduler,
        Some(&state.telemetry),
        Some(&state.session_log),
    ) {
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

pub(super) async fn admin_nodes(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> AdminResult {
    let revision = state
        .store
        .revision_async()
        .await
        .unwrap_or_else(|_| state.control_revision.load(AtomicOrdering::Acquire));
    let etag = format!("W/\"rev-{revision}\"");
    if headers
        .get(IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|candidate| candidate.trim() == etag)
    {
        let mut response = StatusCode::NOT_MODIFIED.into_response();
        if let Ok(value) = HeaderValue::from_str(&etag) {
            response.headers_mut().insert(ETAG, value);
        }
        return Ok(response);
    }
    let nodes = state
        .store
        .list_async()
        .await
        .map_err(|error| AdminError::internal("could not load node configurations", &error))?;
    let mut response = Json(json!({
        "nodes": nodes
            .into_iter()
            .filter_map(|stored| {
                let node = state.scheduler.node(&stored.config.id)?;
                Some(admin_node_payload(&state, &stored, &node))
            })
            .collect::<Vec<_>>()
    }))
    .into_response();
    if let Ok(value) = HeaderValue::from_str(&etag) {
        response.headers_mut().insert(ETAG, value);
    }
    Ok(response)
}

pub(super) async fn admin_events(
    State(state): State<Arc<AppState>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let mut rx = state.control_events.subscribe();
    let initial = *rx.borrow_and_update();
    let stream = async_stream::stream! {
        yield Ok(Event::default().event("revision").data(initial.to_string()));
        while rx.changed().await.is_ok() {
            let revision = *rx.borrow_and_update();
            yield Ok(Event::default().event("revision").data(revision.to_string()));
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

async fn publish_control_revision(state: &AppState) {
    if let Ok(revision) = state.store.revision_async().await {
        state
            .control_revision
            .store(revision, AtomicOrdering::Release);
        let _ = state.control_events.send(revision);
    }
}

#[derive(Default, Deserialize)]
pub(super) struct ObservabilityQuery {
    #[serde(default)]
    scope: String,
    #[serde(default)]
    id: String,
    window: Option<u64>,
}

pub(super) async fn observability(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ObservabilityQuery>,
) -> AdminResult<Json<crate::observability::Snapshot>> {
    let seconds = query.window.unwrap_or(300);
    let scope = if query.scope.is_empty() {
        "fleet"
    } else {
        &query.scope
    };
    if ![60, 300, 900].contains(&seconds)
        || !["fleet", "node", "model"].contains(&scope)
        || query.id.len() > 1024
        || (scope != "fleet" && query.id.is_empty())
    {
        return Err(AdminError::bad_request(
            "invalid_observability_query",
            "Use scope fleet/node/model and window 60/300/900 seconds",
        ));
    }
    Ok(Json(state.telemetry.snapshot(scope, &query.id, seconds)))
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
) -> AdminResult<Json<serde_json::Value>> {
    let ip = ip
        .parse::<IpAddr>()
        .map_err(|_| AdminError::bad_request("invalid_ip", "invalid IP address"))?;
    if input.limit == 0 || input.limit > state.settings.server.max_connections {
        return Err(AdminError::bad_request(
            "invalid_limit",
            "limit must be between 1 and the public connection limit",
        ));
    }
    let _mutation = state.admin_mutation.lock().await;
    state
        .store
        .set_ip_limit_async(ip, input.limit)
        .await
        .map_err(|error| AdminError::internal("could not persist IP connection limit", &error))?;
    state.connections.set_limit(ip, input.limit);
    publish_control_revision(&state).await;
    Ok(Json(json!({"ip": ip, "limit": input.limit})))
}

pub(super) async fn delete_ip_limit(
    State(state): State<Arc<AppState>>,
    Path(ip): Path<String>,
) -> AdminResult<Json<serde_json::Value>> {
    let ip = ip
        .parse::<IpAddr>()
        .map_err(|_| AdminError::bad_request("invalid_ip", "invalid IP address"))?;
    let _mutation = state.admin_mutation.lock().await;
    let deleted = state
        .store
        .delete_ip_limit_async(ip)
        .await
        .map_err(|error| {
            AdminError::internal("could not delete persisted IP connection limit", &error)
        })?;
    let removed_live = state.connections.remove_limit(ip);
    if deleted {
        publish_control_revision(&state).await;
    }
    Ok(Json(json!({"deleted": deleted || removed_live})))
}

async fn load_stored_and_runtime(
    state: &AppState,
    node_id: &str,
) -> AdminResult<(StoredNode, Arc<Node>)> {
    let stored = state
        .store
        .get_async(node_id)
        .await
        .map_err(|error| AdminError::internal("could not load node configuration", &error))?
        .ok_or_else(|| AdminError::node_not_found(node_id))?;
    let node = state.scheduler.node(node_id).ok_or_else(|| {
        AdminError::internal_message("node is persisted but missing from the runtime registry")
    })?;
    Ok((stored, node))
}

pub(super) async fn admin_node(
    State(state): State<Arc<AppState>>,
    Path(node_id): Path<String>,
) -> AdminResult<Json<serde_json::Value>> {
    let (stored, node) = load_stored_and_runtime(&state, &node_id).await?;
    Ok(Json(admin_node_payload(&state, &stored, &node)))
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

fn preserve_stored_credentials(
    config: &mut NodeConfig,
    stored: &NodeConfig,
    clear_api_key: bool,
    clear_headers: bool,
) {
    if config.api_key.is_none() && !clear_api_key {
        config.api_key.clone_from(&stored.api_key);
    }
    if config.headers.is_empty() && !clear_headers {
        config.headers.clone_from(&stored.headers);
    }
}

pub(super) async fn preflight_node(
    State(state): State<Arc<AppState>>,
    Query(query): Query<CredentialMutationQuery>,
    Json(mut config): Json<NodeConfig>,
) -> AdminResult<Json<serde_json::Value>> {
    let preserve_api_key = config.api_key.is_none() && !query.clear_api_key;
    let preserve_headers = config.headers.is_empty() && !query.clear_headers;
    if preserve_api_key || preserve_headers {
        let stored = state.store.get_async(&config.id).await.map_err(|error| {
            AdminError::internal(
                if preserve_api_key {
                    "could not load stored credentials"
                } else {
                    "could not load stored headers"
                },
                &error,
            )
        })?;
        if let Some(stored) = stored {
            preserve_stored_credentials(
                &mut config,
                &stored.config,
                query.clear_api_key,
                query.clear_headers,
            );
        }
    }
    let node = prepare_node_candidate(&state, &config, None)
        .await
        .map_err(|error| AdminError::validation(&error))?;
    let snapshot = node.snapshot();
    let admission = admin_admission_snapshot(&node, &snapshot);
    Ok(Json(json!({
        "ok": true,
        "runtime": snapshot,
        "admission": admission,
        "checks": {
            "configuration": "passed",
            "provider": "passed",
            "health": "passed",
        }
    })))
}

pub(super) async fn create_node(
    State(state): State<Arc<AppState>>,
    Json(config): Json<NodeConfig>,
) -> AdminResult<(StatusCode, Json<serde_json::Value>)> {
    let _mutation = state.admin_mutation.lock().await;
    if state.scheduler.node(&config.id).is_some() {
        return Err(AdminError::conflict(
            "node_already_exists",
            "a node with this id already exists",
        ));
    }
    let node = prepare_node_candidate(&state, &config, None)
        .await
        .map_err(|error| AdminError::validation(&error))?;
    if matches!(state.store.get_async(&config.id).await, Ok(Some(_))) {
        return Err(AdminError::conflict(
            "node_already_exists",
            "a node with this id already exists",
        ));
    }
    let stored = state
        .store
        .insert_async(&config)
        .await
        .map_err(|error| AdminError::internal("could not persist node", &error))?;
    if let Err(error) = state.scheduler.add_node(Arc::clone(&node)) {
        let _ = state
            .store
            .delete_async(&config.id, Some(stored.revision))
            .await;
        return Err(AdminError::internal(
            "could not add node to runtime registry",
            &error,
        ));
    }
    state
        .runtime_revisions
        .write()
        .insert(config.id.clone(), stored.revision);
    publish_control_revision(&state).await;
    Ok((
        StatusCode::CREATED,
        Json(admin_node_payload(&state, &stored, &node)),
    ))
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
    Json(request): Json<UpdateNodeRequest>,
) -> AdminResult<Json<serde_json::Value>> {
    let _mutation = state.admin_mutation.lock().await;
    if request.config.id != node_id {
        return Err(AdminError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "node_id_mismatch",
            "the request path and config node id must match",
        ));
    }
    let (stored, previous) = load_stored_and_runtime(&state, &node_id).await?;
    if stored.revision != request.revision {
        return Err(AdminError::conflict(
            "revision_conflict",
            "the node changed after this editor loaded it; refresh and retry",
        ));
    }
    let mut requested_config = request.config;
    preserve_stored_credentials(
        &mut requested_config,
        &stored.config,
        request.clear_api_key,
        request.clear_headers,
    );
    let replacement = prepare_node_candidate(&state, &requested_config, Some(&previous))
        .await
        .map_err(|error| AdminError::validation(&error))?;
    let updated = state
        .store
        .update_async(&node_id, request.revision, &requested_config)
        .await
        .map_err(|error| AdminError::internal("could not persist node update", &error))?
        .ok_or_else(|| {
            AdminError::conflict(
                "revision_conflict",
                "the node changed while the update was being applied",
            )
        })?;
    let replacement = state
        .scheduler
        .replace_node(&replacement)
        .map_err(|error| AdminError::internal("could not replace runtime node", &error))?;
    state
        .runtime_revisions
        .write()
        .insert(node_id, updated.revision);
    publish_control_revision(&state).await;
    Ok(Json(admin_node_payload(&state, &updated, &replacement)))
}

pub(super) async fn delete_node(
    State(state): State<Arc<AppState>>,
    Path(node_id): Path<String>,
    Query(query): Query<MutationQuery>,
) -> AdminResult<Json<serde_json::Value>> {
    let _mutation = state.admin_mutation.lock().await;
    let (stored, node) = load_stored_and_runtime(&state, &node_id).await?;
    if query
        .revision
        .is_some_and(|revision| revision != stored.revision)
    {
        return Err(AdminError::conflict(
            "revision_conflict",
            "the node changed after this editor loaded it; refresh and retry",
        ));
    }
    let was_draining = node.lifecycle() == LifecycleState::Draining;
    node.set_draining(true);
    state.scheduler.notify_state_change();
    if !state
        .scheduler
        .wait_for_node_idle(&node, mutation_timeout(&state, query.timeout_ms))
        .await
    {
        return Err(AdminError::conflict(
            "node_still_active",
            "the node is draining but still has active requests; retry deletion later",
        ));
    }
    match state
        .store
        .delete_async(&node_id, Some(stored.revision))
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            node.set_draining(was_draining);
            return Err(AdminError::conflict(
                "revision_conflict",
                "the node changed while deletion was being applied",
            ));
        }
        Err(error) => {
            node.set_draining(was_draining);
            return Err(AdminError::internal(
                "could not delete persisted node",
                &error,
            ));
        }
    }
    state.scheduler.remove_node(&node_id);
    state.metrics.remove_node(&node_id);
    state.runtime_revisions.write().remove(&node_id);
    publish_control_revision(&state).await;
    Ok(Json(json!({"deleted": true, "node": node_id})))
}

pub(super) async fn prepare_node_candidate(
    state: &AppState,
    config: &NodeConfig,
    previous: Option<&Node>,
) -> Result<Arc<Node>> {
    validate_node_config(config)?;
    let node = Node::from_config_with_policies(
        config,
        state.settings.health.route_while_starting,
        state.settings.circuit_breaker.clone(),
    )?;
    if !previous.is_some_and(|previous| node.same_upstream_config(previous)) {
        preflight_vllm(&state.control_client, &node).await?;
        preflight_health(&state.control_client, &node, &state.settings.health).await?;
    }
    Ok(node)
}

fn mutation_timeout(state: &AppState, timeout_ms: Option<u64>) -> Duration {
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
) -> AdminResult<(StatusCode, Json<serde_json::Value>)> {
    let _mutation = state.admin_mutation.lock().await;
    let (node, revision) = persist_node_draining(&state, &node_id, true).await?;
    let drained = if query.wait {
        state
            .scheduler
            .wait_for_node_idle(&node, mutation_timeout(&state, query.timeout_ms))
            .await
    } else {
        node.active() == 0
    };
    let status = if drained {
        StatusCode::OK
    } else {
        StatusCode::ACCEPTED
    };
    Ok((
        status,
        Json(json!({
            "node": node.id(),
            "lifecycle": node.lifecycle(),
            "active": node.active(),
            "drained": drained,
            "revision": revision,
        })),
    ))
}

pub(super) async fn resume_node(
    State(state): State<Arc<AppState>>,
    Path(node_id): Path<String>,
) -> AdminResult<Json<serde_json::Value>> {
    let _mutation = state.admin_mutation.lock().await;
    let (node, revision) = persist_node_draining(&state, &node_id, false).await?;
    Ok(Json(json!({
        "node": node.id(),
        "lifecycle": node.lifecycle(),
        "routable": node.is_routable(),
        "revision": revision,
    })))
}

async fn persist_node_draining(
    state: &AppState,
    node_id: &str,
    draining: bool,
) -> AdminResult<(Arc<Node>, u64)> {
    let (mut stored, _) = load_stored_and_runtime(state, node_id).await?;
    stored.config.draining = draining;
    let (conflict_message, persist_message) = if draining {
        (
            "the node changed while draining was being applied",
            "could not persist draining state",
        )
    } else {
        (
            "the node changed while resume was being applied",
            "could not persist serving state",
        )
    };
    let stored = state
        .store
        .update_async(node_id, stored.revision, &stored.config)
        .await
        .map_err(|error| AdminError::internal(persist_message, &error))?
        .ok_or_else(|| AdminError::conflict("revision_conflict", conflict_message))?;
    let node = state
        .scheduler
        .set_node_draining(node_id, draining)
        .ok_or_else(|| {
            AdminError::internal_message("node is persisted but missing from the runtime registry")
        })?;
    state
        .runtime_revisions
        .write()
        .insert(node_id.to_owned(), stored.revision);
    publish_control_revision(state).await;
    Ok((node, stored.revision))
}
