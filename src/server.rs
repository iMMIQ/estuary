use std::{
    net::SocketAddr,
    path::Path as FsPath,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Query, Request, State},
    http::{
        HeaderValue, StatusCode,
        header::{CACHE_CONTROL, CONTENT_SECURITY_POLICY, CONTENT_TYPE},
    },
    middleware::{self, Next},
    response::{IntoResponse, Redirect, Response},
    routing::{any, get, put},
};
use reqwest::redirect::Policy;
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::{
    net::TcpListener,
    sync::{Mutex as AsyncMutex, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    Settings,
    config::{NodeConfig, validate_node_config},
    error::GatewayError,
    health::{preflight_health, run_health_monitor},
    metrics::Metrics,
    node::{CircuitState, LifecycleState, Node, NodeSnapshot},
    proxy,
    scheduler::Scheduler,
    store::{NodeStore, StoredNode},
    vllm::{VllmManager, preflight_vllm},
};

#[derive(Clone, Debug)]
pub struct RequestId(pub String);

pub struct AppState {
    pub(crate) client: reqwest::Client,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) settings: Arc<Settings>,
    pub(crate) vllm: Arc<VllmManager>,
    pub(crate) store: Arc<NodeStore>,
    admin_mutation: AsyncMutex<()>,
}

pub struct Gateway {
    state: Arc<AppState>,
}

#[derive(RustEmbed)]
#[folder = "web/dist"]
struct AdminAssets;

impl Gateway {
    pub fn build(settings: Settings) -> Result<Self> {
        let store = NodeStore::memory()?;
        store.seed_if_empty(&settings.nodes)?;
        Self::build_with_store(settings, store)
    }

    pub fn build_with_database(settings: Settings, path: impl AsRef<FsPath>) -> Result<Self> {
        let store = NodeStore::open(path)?;
        Self::build_with_store(settings, store)
    }

    fn build_with_store(settings: Settings, store: Arc<NodeStore>) -> Result<Self> {
        settings.validate()?;
        let nodes = store
            .list()?
            .into_iter()
            .map(|stored| {
                Node::from_config_with_policies(
                    &stored.config,
                    settings.health.route_while_starting,
                    settings.circuit_breaker.clone(),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(settings.server.connect_timeout_ms))
            .pool_idle_timeout(Duration::from_secs(90))
            .no_proxy()
            .redirect(Policy::none())
            .user_agent(concat!("estuary/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("failed to build upstream HTTP client")?;
        let scheduler = Arc::new(Scheduler::new(nodes.clone(), settings.routing.clone()));
        let vllm = VllmManager::new(Arc::clone(&scheduler));
        Ok(Self {
            state: Arc::new(AppState {
                client,
                scheduler,
                metrics: Metrics::new(),
                settings: Arc::new(settings),
                vllm,
                store,
                admin_mutation: AsyncMutex::new(()),
            }),
        })
    }

    pub fn public_router(&self) -> Router {
        let max_body = self.state.settings.server.max_request_body_bytes;
        Router::new()
            .route("/v1/models", get(proxy::list_models))
            .route("/v1/models/{model}", get(proxy::get_model))
            .route("/v1/{*path}", any(proxy::proxy))
            .fallback(proxy::not_found)
            .layer(DefaultBodyLimit::max(max_body))
            .layer(middleware::from_fn_with_state(
                Arc::clone(&self.state),
                observe_request,
            ))
            .layer(middleware::from_fn(assign_request_id))
            .with_state(Arc::clone(&self.state))
    }

    pub fn admin_router(&self) -> Router {
        Router::new()
            .route("/", get(admin_redirect))
            .route("/admin", get(admin_redirect))
            .route("/admin/", get(admin_index))
            .route("/health/live", get(live))
            .route("/health/ready", get(ready))
            .route("/metrics", get(metrics))
            .route("/admin/nodes", get(nodes))
            .route("/admin/api/status", get(admin_status))
            .route(
                "/admin/api/nodes/preflight",
                axum::routing::post(preflight_node),
            )
            .route("/admin/api/nodes", get(admin_nodes).post(create_node))
            .route(
                "/admin/api/nodes/{node}",
                get(admin_node).put(update_node).delete(delete_node),
            )
            .route(
                "/admin/nodes/{node}/drain",
                put(drain_node).delete(resume_node),
            )
            .route(
                "/admin/api/nodes/{node}/drain",
                put(drain_node).delete(resume_node),
            )
            .route("/admin/{*asset}", get(admin_asset))
            .fallback(proxy::not_found)
            .layer(middleware::from_fn_with_state(
                Arc::clone(&self.state),
                observe_request,
            ))
            .layer(middleware::from_fn(assign_request_id))
            .with_state(Arc::clone(&self.state))
    }

    pub async fn run(self) -> Result<()> {
        let public_address: SocketAddr = self.state.settings.server.listen.parse()?;
        let admin_address: SocketAddr = self.state.settings.server.admin_listen.parse()?;
        let public_listener = TcpListener::bind(public_address)
            .await
            .with_context(|| format!("failed to bind public listener on {public_address}"))?;
        let admin_listener = TcpListener::bind(admin_address)
            .await
            .with_context(|| format!("failed to bind admin listener on {admin_address}"))?;
        info!(address = %public_address, "public API listening");
        info!(address = %admin_address, "admin API listening");

        let cancellation = CancellationToken::new();
        let (health_shutdown, health_receiver) = watch::channel(false);
        let (provider_shutdown, provider_receiver) = watch::channel(false);
        let health_handle = tokio::spawn(run_health_monitor(
            self.state.client.clone(),
            Arc::clone(&self.state.scheduler),
            self.state.settings.health.clone(),
            health_receiver,
        ));
        let provider_handle = tokio::spawn(
            Arc::clone(&self.state.vllm).run(self.state.client.clone(), provider_receiver),
        );

        let public_router = self.public_router();
        let admin_router = self.admin_router();
        let public_token = cancellation.clone();
        let mut public_handle: JoinHandle<std::io::Result<()>> = tokio::spawn(async move {
            axum::serve(public_listener, public_router)
                .with_graceful_shutdown(public_token.cancelled_owned())
                .await
        });
        let admin_token = cancellation.clone();
        let mut admin_handle: JoinHandle<std::io::Result<()>> = tokio::spawn(async move {
            axum::serve(admin_listener, admin_router)
                .with_graceful_shutdown(admin_token.cancelled_owned())
                .await
        });

        let mut public_done = false;
        let mut admin_done = false;
        let mut first_error: Option<anyhow::Error> = None;
        tokio::select! {
            result = &mut public_handle => {
                public_done = true;
                if let Err(error) = flatten_server_result(result) {
                    first_error = Some(error);
                }
            }
            result = &mut admin_handle => {
                admin_done = true;
                if let Err(error) = flatten_server_result(result) {
                    first_error = Some(error);
                }
            }
            () = shutdown_signal() => {
                info!("shutdown signal received");
            }
        }

        self.state.scheduler.drain_all();
        info!("all upstream nodes are draining");
        cancellation.cancel();
        let _ = health_shutdown.send(true);
        let _ = provider_shutdown.send(true);
        let shutdown_grace = Duration::from_millis(self.state.settings.server.shutdown_grace_ms);
        if !public_done {
            if let Err(error) = finish_server("public", &mut public_handle, shutdown_grace).await {
                first_error.get_or_insert(error);
            }
        }
        if !admin_done {
            if let Err(error) = finish_server("admin", &mut admin_handle, shutdown_grace).await {
                first_error.get_or_insert(error);
            }
        }
        if let Err(error) = health_handle.await {
            error!(error = %error, "health monitor task failed");
        }
        if let Err(error) = provider_handle.await {
            error!(error = %error, "vLLM provider monitor task failed");
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }
}

async fn finish_server(
    name: &'static str,
    handle: &mut JoinHandle<std::io::Result<()>>,
    grace: Duration,
) -> Result<()> {
    if let Ok(result) = tokio::time::timeout(grace, &mut *handle).await {
        flatten_server_result(result)
    } else {
        warn!(
            server = name,
            ?grace,
            "graceful shutdown timed out; aborting server task"
        );
        handle.abort();
        let _ = handle.await;
        Ok(())
    }
}

fn flatten_server_result(
    result: Result<std::io::Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    result.context("server task panicked")??;
    Ok(())
}

async fn assign_request_id(mut request: Request, next: Next) -> Response {
    let id = request
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty() && value.len() <= 128)
        .map_or_else(|| Uuid::now_v7().to_string(), str::to_owned);
    request.extensions_mut().insert(RequestId(id.clone()));
    let mut response = next.run(request).await;
    if let Ok(value) = HeaderValue::from_str(&id) {
        response.headers_mut().insert("x-request-id", value);
    }
    response
}

async fn observe_request(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let started = std::time::Instant::now();
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let endpoint = metric_endpoint(&path);
    let request_id = request
        .extensions()
        .get::<RequestId>()
        .map(|id| id.0.clone())
        .unwrap_or_default();
    let mut response = next.run(request).await;
    if response.status() == StatusCode::PAYLOAD_TOO_LARGE
        && !response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("application/json"))
    {
        response = GatewayError::PayloadTooLarge.into_response();
    }
    let elapsed = started.elapsed();
    state.metrics.request(endpoint, response.status().as_u16());
    state
        .metrics
        .observe_request_duration(elapsed.as_secs_f64());
    info!(
        request_id,
        method = %method,
        path,
        status = response.status().as_u16(),
        elapsed_ms = elapsed.as_millis(),
        "request completed"
    );
    response
}

fn metric_endpoint(path: &str) -> &'static str {
    match path {
        "/v1/chat/completions" => "chat_completions",
        "/v1/responses" => "responses",
        "/v1/completions" => "completions",
        "/v1/embeddings" => "embeddings",
        "/v1/models" => "models",
        "/health/live" => "health_live",
        "/health/ready" => "health_ready",
        "/metrics" => "metrics",
        "/admin/nodes" => "admin_nodes",
        "/admin/api/status" => "admin_status",
        "/admin/api/nodes/preflight" => "admin_node_preflight",
        path if path.starts_with("/admin/nodes/") && path.ends_with("/drain") => "admin_node_drain",
        _ => "other",
    }
}

async fn admin_redirect() -> Redirect {
    Redirect::temporary("/admin/")
}

async fn admin_index() -> Response {
    embedded_admin_response("index.html", false)
}

async fn admin_asset(Path(asset): Path<String>) -> Response {
    if AdminAssets::get(&asset).is_some() {
        return embedded_admin_response(&asset, asset.starts_with("assets/"));
    }
    if !asset.rsplit('/').next().unwrap_or_default().contains('.') {
        return embedded_admin_response("index.html", false);
    }
    StatusCode::NOT_FOUND.into_response()
}

fn embedded_admin_response(path: &str, immutable: bool) -> Response {
    let Some(asset) = AdminAssets::get(path) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let content_type = mime_guess::from_path(path).first_or_octet_stream();
    let cache_control = if immutable {
        "public, max-age=31536000, immutable"
    } else {
        "no-store"
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, content_type.as_ref())
        .header(CACHE_CONTROL, cache_control)
        .header("x-content-type-options", "nosniff")
        .header("x-frame-options", "DENY")
        .header("referrer-policy", "no-referrer")
        .header("permissions-policy", "camera=(), microphone=(), geolocation=()")
        .header(
            CONTENT_SECURITY_POLICY,
            "default-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'self'; frame-ancestors 'none'",
        )
        .body(axum::body::Body::from(asset.data.into_owned()))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

async fn live() -> Json<serde_json::Value> {
    Json(json!({"status": "ok"}))
}

async fn ready(State(state): State<Arc<AppState>>) -> Response {
    let ready = state.scheduler.ready();
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

async fn metrics(State(state): State<Arc<AppState>>) -> Response {
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

async fn nodes(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    Json(json!({
        "nodes": state
            .scheduler
            .nodes()
            .iter()
            .map(|node| {
                let mut snapshot = json!(node.snapshot());
                let cache = state.scheduler.exact_cache_directory().snapshot(node.id());
                snapshot["exact_kv_authoritative"] = json!(cache.authoritative);
                snapshot["exact_kv_blocks"] = json!(cache.blocks);
                snapshot
            })
            .collect::<Vec<_>>()
    }))
}

async fn admin_nodes(State(state): State<Arc<AppState>>) -> Response {
    match state.store.list() {
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
struct AdminAdmissionSnapshot {
    state: &'static str,
    reason: &'static str,
    routable: bool,
    accepting_assignments: bool,
    telemetry_fresh: bool,
    waiting_watermark_blocked: bool,
}

fn admin_admission_snapshot(node: &Node, snapshot: &NodeSnapshot) -> AdminAdmissionSnapshot {
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

async fn admin_status(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
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
    let ready = routable_nodes > 0;

    Json(json!({
        "status": if ready { "ready" } else { "not_ready" },
        "live": true,
        "ready": ready,
        "version": env!("CARGO_PKG_VERSION"),
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
            "max_requests": state.settings.routing.queue_max_requests,
            "max_bytes": state.settings.routing.queue_max_bytes,
        },
        "routing": {
            "prefix_enabled": state.settings.routing.prefix.enabled,
        }
    }))
}

async fn admin_node(State(state): State<Arc<AppState>>, Path(node_id): Path<String>) -> Response {
    let stored = match state.store.get(&node_id) {
        Ok(Some(stored)) => stored,
        Ok(None) => return admin_node_not_found(&node_id),
        Err(error) => return admin_internal_error("could not load node configuration", &error),
    };
    let Some(node) = state.scheduler.node(&node_id) else {
        return admin_internal_message("node is persisted but missing from the runtime registry");
    };
    Json(admin_node_payload(&state, &stored, &node)).into_response()
}

fn admin_node_payload(state: &AppState, stored: &StoredNode, node: &Node) -> serde_json::Value {
    let cache = state.scheduler.exact_cache_directory().snapshot(node.id());
    let snapshot = node.snapshot();
    let admission = admin_admission_snapshot(node, &snapshot);
    json!({
        "config": stored.config,
        "revision": stored.revision,
        "created_at_unix_ms": stored.created_at_unix_ms,
        "updated_at_unix_ms": stored.updated_at_unix_ms,
        "runtime": snapshot,
        "admission": admission,
        "exact_kv_authoritative": cache.authoritative,
        "exact_kv_blocks": cache.blocks,
    })
}

async fn preflight_node(
    State(state): State<Arc<AppState>>,
    Json(config): Json<NodeConfig>,
) -> Response {
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

async fn create_node(
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
    if matches!(state.store.get(&config.id), Ok(Some(_))) {
        return admin_conflict("node_already_exists", "a node with this id already exists");
    }
    let stored = match state.store.insert(&config) {
        Ok(stored) => stored,
        Err(error) => return admin_internal_error("could not persist node", &error),
    };
    if let Err(error) = state.scheduler.add_node(Arc::clone(&node)) {
        let _ = state.store.delete(&config.id, Some(stored.revision));
        return admin_internal_error("could not add node to runtime registry", &error);
    }
    (
        StatusCode::CREATED,
        Json(admin_node_payload(&state, &stored, &node)),
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateNodeRequest {
    revision: u64,
    config: NodeConfig,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct MutationQuery {
    timeout_ms: Option<u64>,
    revision: Option<u64>,
}

async fn update_node(
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
    let stored = match state.store.get(&node_id) {
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
    let replacement = match prepare_node(&state, &request.config).await {
        Ok(node) => node,
        Err(error) => return admin_validation_error(&error),
    };
    let was_draining = previous.lifecycle() == crate::node::LifecycleState::Draining;
    previous.set_draining(true);
    let timeout = mutation_timeout(&state, query.timeout_ms);
    if !state.scheduler.wait_for_node_idle(&previous, timeout).await {
        return admin_conflict(
            "node_still_active",
            "the node is draining but still has active requests; retry the update later",
        );
    }
    let updated = match state
        .store
        .update(&node_id, request.revision, &request.config)
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
    Json(admin_node_payload(&state, &updated, &replacement)).into_response()
}

async fn delete_node(
    State(state): State<Arc<AppState>>,
    Path(node_id): Path<String>,
    Query(query): Query<MutationQuery>,
) -> Response {
    let _mutation = state.admin_mutation.lock().await;
    let stored = match state.store.get(&node_id) {
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
    match state.store.delete(&node_id, Some(stored.revision)) {
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
    Json(json!({"deleted": true, "node": node_id})).into_response()
}

async fn prepare_node(state: &AppState, config: &NodeConfig) -> Result<Arc<Node>> {
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

fn mutation_timeout(state: &AppState, timeout_ms: Option<u64>) -> Duration {
    Duration::from_millis(
        timeout_ms
            .unwrap_or(state.settings.server.shutdown_grace_ms)
            .min(3_600_000),
    )
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct DrainQuery {
    wait: bool,
    timeout_ms: Option<u64>,
}

async fn drain_node(
    State(state): State<Arc<AppState>>,
    Path(node_id): Path<String>,
    Query(query): Query<DrainQuery>,
) -> Response {
    let _mutation = state.admin_mutation.lock().await;
    let mut stored = match state.store.get(&node_id) {
        Ok(Some(stored)) => stored,
        Ok(None) => return admin_node_not_found(&node_id),
        Err(error) => return admin_internal_error("could not load node configuration", &error),
    };
    stored.config.draining = true;
    let stored = match state
        .store
        .update(&node_id, stored.revision, &stored.config)
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
    let drained = if query.wait {
        let timeout_ms = query
            .timeout_ms
            .unwrap_or(state.settings.server.shutdown_grace_ms)
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

async fn resume_node(State(state): State<Arc<AppState>>, Path(node_id): Path<String>) -> Response {
    let _mutation = state.admin_mutation.lock().await;
    let mut stored = match state.store.get(&node_id) {
        Ok(Some(stored)) => stored,
        Ok(None) => return admin_node_not_found(&node_id),
        Err(error) => return admin_internal_error("could not load node configuration", &error),
    };
    stored.config.draining = false;
    let stored = match state
        .store
        .update(&node_id, stored.revision, &stored.config)
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
    Json(json!({
        "node": node.id(),
        "lifecycle": node.lifecycle(),
        "routable": node.is_routable(),
        "revision": stored.revision,
    }))
    .into_response()
}

fn admin_node_not_found(node_id: &str) -> Response {
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

fn admin_validation_error(error: &anyhow::Error) -> Response {
    admin_message(
        StatusCode::UNPROCESSABLE_ENTITY,
        "node_validation_failed",
        &error.to_string(),
    )
}

fn admin_conflict(code: &'static str, message: &'static str) -> Response {
    admin_message(StatusCode::CONFLICT, code, message)
}

fn admin_internal_message(message: &'static str) -> Response {
    error!(message, "admin runtime consistency error");
    admin_message(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", message)
}

fn admin_internal_error(message: &'static str, error: &dyn std::fmt::Display) -> Response {
    error!(error = %error, message, "admin operation failed");
    admin_message(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", message)
}

fn admin_message(status: StatusCode, code: &'static str, message: &str) -> Response {
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

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            error!(error = %error, "failed to install Ctrl-C handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => error!(error = %error, "failed to install SIGTERM handler"),
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_paths_have_bounded_cardinality() {
        assert_eq!(metric_endpoint("/v1/chat/completions"), "chat_completions");
        assert_eq!(metric_endpoint("/v1/unknown/user-value"), "other");
    }
}
