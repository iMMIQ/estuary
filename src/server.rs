use std::{
    collections::HashMap,
    io,
    net::{SocketAddr, TcpListener as StdTcpListener},
    path::Path as FsPath,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering as AtomicOrdering},
    },
    task::{Context as TaskContext, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Path, Query, Request, State},
    http::{
        HeaderValue, Method, StatusCode,
        header::{
            AUTHORIZATION, CACHE_CONTROL, CONTENT_LENGTH, CONTENT_SECURITY_POLICY, CONTENT_TYPE,
            WWW_AUTHENTICATE,
        },
    },
    middleware::{self, Next},
    response::{IntoResponse, Redirect, Response},
    routing::{any, get, put},
    serve::Listener,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use futures_util::StreamExt;
use parking_lot::RwLock;
use reqwest::redirect::Policy;
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};
use serde_json::json;
use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpListener,
    sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    Settings, anthropic,
    config::{NodeConfig, validate_node_config},
    error::GatewayError,
    health::{preflight_health, run_health_monitor},
    lifecycle::ProcessLifecycle,
    metrics::Metrics,
    node::{CircuitState, LifecycleState, Node, NodeSnapshot},
    proxy,
    response_buffer::ResponseBufferBudget,
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
    pub(crate) process: Arc<ProcessLifecycle>,
    pub(crate) response_buffer: Arc<ResponseBufferBudget>,
    runtime_revisions: RwLock<HashMap<String, u64>>,
    control_revision: AtomicU64,
    admin_mutation: AsyncMutex<()>,
}

pub struct Gateway {
    state: Arc<AppState>,
}

#[derive(RustEmbed)]
#[folder = "web/dist"]
struct AdminAssets;

#[derive(Debug)]
struct BoundedTcpListener {
    inner: TcpListener,
    permits: Arc<Semaphore>,
    metrics: Arc<Metrics>,
    track_public: bool,
    accept_cancellation: CancellationToken,
}

impl BoundedTcpListener {
    fn new(
        inner: TcpListener,
        max_connections: usize,
        metrics: Arc<Metrics>,
        track_public: bool,
        accept_cancellation: CancellationToken,
    ) -> Self {
        Self {
            inner,
            permits: Arc::new(Semaphore::new(max_connections)),
            metrics,
            track_public,
            accept_cancellation,
        }
    }
}

impl Listener for BoundedTcpListener {
    type Io = BoundedTcpStream;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        let permit = tokio::select! {
            biased;
            () = self.accept_cancellation.cancelled() => std::future::pending().await,
            permit = Arc::clone(&self.permits).acquire_owned() => {
                permit.expect("public connection semaphore is never closed")
            }
        };
        loop {
            let accepted = tokio::select! {
                biased;
                () = self.accept_cancellation.cancelled() => std::future::pending().await,
                accepted = self.inner.accept() => accepted,
            };
            match accepted {
                Ok((stream, address)) => {
                    if self.track_public {
                        self.metrics.public_connection_opened();
                    }
                    return (
                        BoundedTcpStream {
                            inner: stream,
                            _permit: permit,
                            metrics: Arc::clone(&self.metrics),
                            track_public: self.track_public,
                        },
                        address,
                    );
                }
                Err(error) => {
                    warn!(%error, "failed to accept public connection");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}

#[derive(Debug)]
struct BoundedTcpStream {
    inner: tokio::net::TcpStream,
    _permit: OwnedSemaphorePermit,
    metrics: Arc<Metrics>,
    track_public: bool,
}

impl AsyncRead for BoundedTcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

impl AsyncWrite for BoundedTcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(context, buffer)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

impl Drop for BoundedTcpStream {
    fn drop(&mut self) {
        if self.track_public {
            self.metrics.public_connection_closed();
        }
    }
}

impl Gateway {
    pub fn build(settings: Settings) -> Result<Self> {
        let store = NodeStore::memory()?;
        store.seed_if_empty(&settings.nodes)?;
        Self::build_with_store(settings, store, false)
    }

    pub fn build_with_database(settings: Settings, path: impl AsRef<FsPath>) -> Result<Self> {
        let store = NodeStore::open(path)?;
        Self::build_with_store(settings, store, false)
    }

    pub fn build_with_database_paused(
        settings: Settings,
        path: impl AsRef<FsPath>,
    ) -> Result<Self> {
        let store = NodeStore::open(path)?;
        Self::build_with_store(settings, store, true)
    }

    fn build_with_store(settings: Settings, store: Arc<NodeStore>, paused: bool) -> Result<Self> {
        settings.validate()?;
        let stored_nodes = store.list()?;
        let runtime_revisions = stored_nodes
            .iter()
            .map(|stored| (stored.config.id.clone(), stored.revision))
            .collect();
        let control_revision = store.revision()?;
        let nodes = stored_nodes
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
        let metrics = Metrics::new();
        let response_buffer = ResponseBufferBudget::new(
            settings.server.max_buffered_response_bytes,
            Arc::clone(&metrics),
        );
        Ok(Self {
            state: Arc::new(AppState {
                client,
                scheduler,
                metrics,
                settings: Arc::new(settings),
                vllm,
                store,
                process: if paused {
                    ProcessLifecycle::new_paused()
                } else {
                    ProcessLifecycle::new()
                },
                response_buffer,
                runtime_revisions: RwLock::new(runtime_revisions),
                control_revision: AtomicU64::new(control_revision),
                admin_mutation: AsyncMutex::new(()),
            }),
        })
    }

    pub fn public_router(&self) -> Router {
        let max_body = self.state.settings.server.max_request_body_bytes;
        Router::new()
            .route("/api/hello", get(api_hello))
            .route("/v1/models", get(proxy::list_models))
            .route("/v1/models/{model}", get(proxy::get_model))
            .route("/v1/{*path}", any(proxy::proxy))
            .fallback(proxy::not_found)
            .layer(DefaultBodyLimit::max(max_body))
            .layer(middleware::from_fn_with_state(
                Arc::clone(&self.state),
                admit_public_request,
            ))
            .layer(middleware::from_fn_with_state(
                Arc::clone(&self.state),
                observe_request,
            ))
            .layer(middleware::from_fn(assign_request_id))
            .layer(middleware::from_fn_with_state(
                Arc::clone(&self.state),
                track_public_response,
            ))
            .with_state(Arc::clone(&self.state))
    }

    pub fn admin_router(&self) -> Router {
        let protected = Router::new()
            .route("/", get(admin_redirect))
            .route("/admin", get(admin_redirect))
            .route("/admin/", get(admin_index))
            .route("/metrics", get(metrics))
            .route("/admin/nodes", get(nodes))
            .route("/admin/api/status", get(admin_status))
            .route("/admin/api/process", get(process_status))
            .route("/admin/api/process/activate", put(activate_process))
            .route("/admin/api/process/drain", put(drain_process))
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
            .layer(middleware::from_fn_with_state(
                Arc::clone(&self.state),
                authorize_admin,
            ));
        Router::new()
            .route("/health/live", get(live))
            .route("/health/ready", get(ready))
            .merge(protected)
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
        let public_listener = TcpListener::bind(public_address)
            .await
            .with_context(|| format!("failed to bind public listener on {public_address}"))?;
        self.run_with_listener(public_listener, false).await
    }

    pub async fn run_with_public_listener(self, listener: StdTcpListener) -> Result<()> {
        listener
            .set_nonblocking(true)
            .context("failed to make inherited public listener non-blocking")?;
        let listener = TcpListener::from_std(listener)
            .context("failed to register inherited public listener with Tokio")?;
        self.run_with_listener(listener, true).await
    }

    #[allow(clippy::too_many_lines)]
    async fn run_with_listener(
        self,
        public_listener: TcpListener,
        stop_accept_before_withdrawal: bool,
    ) -> Result<()> {
        let public_address = public_listener.local_addr()?;
        let admin_address: SocketAddr = self.state.settings.server.admin_listen.parse()?;
        let admin_listener = TcpListener::bind(admin_address)
            .await
            .with_context(|| format!("failed to bind admin listener on {admin_address}"))?;
        info!(address = %public_address, "public API listening");
        info!(address = %admin_address, "admin API listening");

        let public_cancellation = CancellationToken::new();
        let admin_cancellation = CancellationToken::new();
        let public_accept_cancellation = CancellationToken::new();
        let (health_shutdown, health_receiver) = watch::channel(false);
        let (provider_shutdown, provider_receiver) = watch::channel(false);
        let (control_shutdown, control_receiver) = watch::channel(false);
        let mut health_handle = tokio::spawn(run_health_monitor(
            self.state.client.clone(),
            Arc::clone(&self.state.scheduler),
            self.state.settings.health.clone(),
            health_receiver,
        ));
        let mut provider_handle = tokio::spawn(
            Arc::clone(&self.state.vllm).run(self.state.client.clone(), provider_receiver),
        );
        let mut control_handle = tokio::spawn(run_control_reconciler(
            Arc::clone(&self.state),
            control_receiver,
        ));

        let public_listener = BoundedTcpListener::new(
            public_listener,
            self.state.settings.server.max_connections,
            Arc::clone(&self.state.metrics),
            true,
            public_accept_cancellation.clone(),
        );
        let admin_listener = BoundedTcpListener::new(
            admin_listener,
            self.state.settings.server.max_admin_connections,
            Arc::clone(&self.state.metrics),
            false,
            admin_cancellation.clone(),
        );
        let public_router = self.public_router();
        let admin_router = self.admin_router();
        let public_token = public_cancellation.clone();
        let public_shutdown = public_cancellation.clone();
        let public_process = Arc::clone(&self.state.process);
        let mut public_handle: JoinHandle<std::io::Result<()>> = tokio::spawn(async move {
            tokio::select! {
                () = public_process.activated() => {}
                () = public_shutdown.cancelled() => return Ok(()),
            }
            if !public_process.accepting_traffic() {
                return Ok(());
            }
            axum::serve(public_listener, public_router)
                .with_graceful_shutdown(public_token.cancelled_owned())
                .await
        });
        let admin_token = admin_cancellation.clone();
        let mut admin_handle: JoinHandle<std::io::Result<()>> = tokio::spawn(async move {
            axum::serve(admin_listener, admin_router)
                .with_graceful_shutdown(admin_token.cancelled_owned())
                .await
        });

        let mut public_done = false;
        let mut admin_done = false;
        let mut health_done = false;
        let mut provider_done = false;
        let mut control_done = false;
        let mut first_error: Option<anyhow::Error> = None;
        tokio::select! {
            result = &mut public_handle => {
                public_done = true;
                if let Err(error) = flatten_server_result(result) {
                    first_error = Some(error);
                }
                self.state.process.request_shutdown();
            }
            result = &mut admin_handle => {
                admin_done = true;
                if let Err(error) = flatten_server_result(result) {
                    first_error = Some(error);
                }
                self.state.process.request_shutdown();
            }
            result = &mut health_handle => {
                health_done = true;
                first_error = Some(unexpected_background_exit("health monitor", result));
                self.state.process.request_shutdown();
            }
            result = &mut provider_handle => {
                provider_done = true;
                first_error = Some(unexpected_background_exit("vLLM provider monitor", result));
                self.state.process.request_shutdown();
            }
            result = &mut control_handle => {
                control_done = true;
                first_error = Some(unexpected_background_exit("control-plane reconciler", result));
                self.state.process.request_shutdown();
            }
            () = shutdown_signal() => {
                info!("shutdown signal received");
                self.state.process.request_shutdown();
            }
            () = self.state.process.shutdown_requested() => {
                info!("process drain requested");
            }
        }

        if let Some(error) = self
            .drain_http_servers(
                public_done,
                admin_done,
                &mut public_handle,
                &mut admin_handle,
                &public_cancellation,
                &public_accept_cancellation,
                &admin_cancellation,
                stop_accept_before_withdrawal,
            )
            .await
        {
            first_error.get_or_insert(error);
        }
        let _ = health_shutdown.send(true);
        let _ = provider_shutdown.send(true);
        let _ = control_shutdown.send(true);
        if !health_done {
            if let Err(error) = health_handle.await {
                first_error
                    .get_or_insert_with(|| anyhow::anyhow!("health monitor task failed: {error}"));
            }
        }
        if !provider_done {
            if let Err(error) = provider_handle.await {
                first_error.get_or_insert_with(|| {
                    anyhow::anyhow!("vLLM provider monitor task failed: {error}")
                });
            }
        }
        if !control_done {
            if let Err(error) = control_handle.await {
                first_error.get_or_insert_with(|| {
                    anyhow::anyhow!("control-plane reconciler task failed: {error}")
                });
            }
        }
        self.state.process.mark_drained();
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn drain_http_servers(
        &self,
        public_done: bool,
        admin_done: bool,
        public_handle: &mut JoinHandle<std::io::Result<()>>,
        admin_handle: &mut JoinHandle<std::io::Result<()>>,
        public_cancellation: &CancellationToken,
        public_accept_cancellation: &CancellationToken,
        admin_cancellation: &CancellationToken,
        stop_accept_before_withdrawal: bool,
    ) -> Option<anyhow::Error> {
        let withdrawal_delay =
            Duration::from_millis(self.state.settings.server.withdrawal_delay_ms);
        if !public_done && stop_accept_before_withdrawal {
            public_accept_cancellation.cancel();
        }
        if !public_done && !withdrawal_delay.is_zero() {
            info!(
                ?withdrawal_delay,
                "readiness disabled; waiting for load balancer withdrawal"
            );
            tokio::time::sleep(withdrawal_delay).await;
        }

        self.state.process.mark_draining();
        public_accept_cancellation.cancel();
        public_cancellation.cancel();
        let shutdown_grace = Duration::from_millis(self.state.settings.server.shutdown_grace_ms);
        let deadline = tokio::time::Instant::now() + shutdown_grace;
        let mut first_error = None;
        if !public_done {
            if let Err(error) = finish_server("public", public_handle, deadline).await {
                first_error = Some(error);
            }
        }
        if tokio::time::timeout_at(deadline, self.state.process.wait_for_idle())
            .await
            .is_err()
        {
            warn!(
                in_flight = self.state.process.in_flight_responses(),
                "response drain timed out"
            );
        }

        admin_cancellation.cancel();
        if !admin_done {
            if let Err(error) = finish_server("admin", admin_handle, deadline).await {
                first_error.get_or_insert(error);
            }
        }
        first_error
    }
}

fn unexpected_background_exit(
    name: &'static str,
    result: Result<(), tokio::task::JoinError>,
) -> anyhow::Error {
    match result {
        Ok(()) => anyhow::anyhow!("{name} exited unexpectedly"),
        Err(error) => anyhow::anyhow!("{name} task failed: {error}"),
    }
}

async fn admit_public_request(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    if request.method() != Method::POST || !request.uri().path().starts_with("/v1/") {
        return next.run(request).await;
    }

    let max_body = state.settings.server.max_request_body_bytes;
    let reserved_bytes = request
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(max_body);
    if reserved_bytes > max_body {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    }

    let _admission = state.scheduler.admit_ingress(reserved_bytes).await;
    next.run(request).await
}

async fn authorize_admin(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    if let Some(expected) = state.settings.server.admin_token.as_deref() {
        let candidate = request
            .headers()
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(admin_authorization_token);
        if !candidate
            .is_some_and(|candidate| bool::from(candidate.as_bytes().ct_eq(expected.as_bytes())))
        {
            return Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .header(
                    WWW_AUTHENTICATE,
                    "Basic realm=\"Estuary Admin\", charset=\"UTF-8\"",
                )
                .body(Body::from("authentication required"))
                .unwrap_or_else(|_| StatusCode::UNAUTHORIZED.into_response());
        }
    }

    let is_process_control = request.uri().path().starts_with("/admin/api/process/");
    let mutating = matches!(
        *request.method(),
        Method::POST | Method::PUT | Method::DELETE | Method::PATCH
    );
    if mutating
        && !is_process_control
        && state
            .settings
            .server
            .admin_freeze_file
            .as_ref()
            .is_some_and(|path| path.exists())
    {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": {
                    "code": "rollout_in_progress",
                    "message": "Management writes are frozen while a binary rollout is in progress"
                }
            })),
        )
            .into_response();
    }

    next.run(request).await
}

fn admin_authorization_token(value: &str) -> Option<String> {
    let (scheme, credentials) = value.split_once(' ')?;
    if scheme.eq_ignore_ascii_case("bearer") {
        return Some(credentials.to_owned());
    }
    if !scheme.eq_ignore_ascii_case("basic") {
        return None;
    }
    let decoded = BASE64_STANDARD.decode(credentials).ok()?;
    let decoded = std::str::from_utf8(&decoded).ok()?;
    decoded
        .split_once(':')
        .map(|(_, password)| password.to_owned())
}

async fn run_control_reconciler(state: Arc<AppState>, mut shutdown: watch::Receiver<bool>) {
    let mut interval = tokio::time::interval(Duration::from_millis(
        state.settings.server.control_sync_interval_ms,
    ));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut failure_backoff = Duration::ZERO;
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            _ = interval.tick() => {
                if let Err(error) = reconcile_control_plane(&state).await {
                    warn!(error = %error, "failed to reconcile shared node configuration");
                    failure_backoff = if failure_backoff.is_zero() {
                        Duration::from_secs(1)
                    } else {
                        failure_backoff.saturating_mul(2).min(Duration::from_secs(30))
                    };
                    tokio::select! {
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                break;
                            }
                        }
                        () = tokio::time::sleep(failure_backoff) => {}
                    }
                } else {
                    failure_backoff = Duration::ZERO;
                }
            }
        }
    }
}

async fn reconcile_control_plane(state: &Arc<AppState>) -> Result<()> {
    let observed = state.store.revision_async().await?;
    if observed == state.control_revision.load(AtomicOrdering::Acquire) {
        return Ok(());
    }

    let _mutation = state.admin_mutation.lock().await;
    let before = state.store.revision_async().await?;
    let stored_nodes = state.store.list_async().await?;
    let after = state.store.revision_async().await?;
    if before != after {
        return Ok(());
    }

    let persisted_ids = stored_nodes
        .iter()
        .map(|stored| stored.config.id.clone())
        .collect::<std::collections::HashSet<_>>();
    let mut failures = Vec::new();
    for stored in stored_nodes {
        let current_revision = state
            .runtime_revisions
            .read()
            .get(&stored.config.id)
            .copied();
        if current_revision == Some(stored.revision)
            && state.scheduler.node(&stored.config.id).is_some()
        {
            continue;
        }

        let replacement = match prepare_node(state, &stored.config).await {
            Ok(node) => node,
            Err(error) => {
                if let Some(previous) = state.scheduler.node(&stored.config.id) {
                    previous.set_draining(true);
                    state.scheduler.notify_state_change();
                }
                failures.push(format!("{}: {error:#}", stored.config.id));
                continue;
            }
        };
        if let Some(previous) = state.scheduler.node(&stored.config.id) {
            previous.set_draining(true);
            state.scheduler.notify_state_change();
            let timeout = Duration::from_millis(state.settings.server.node_mutation_timeout_ms);
            if !state.scheduler.wait_for_node_idle(&previous, timeout).await {
                failures.push(format!(
                    "{}: active requests did not drain",
                    stored.config.id
                ));
                continue;
            }
            if let Err(error) = state.scheduler.replace_node(&replacement) {
                failures.push(format!("{}: {error}", stored.config.id));
                continue;
            }
        } else if let Err(error) = state.scheduler.add_node(Arc::clone(&replacement)) {
            failures.push(format!("{}: {error}", stored.config.id));
            continue;
        }
        state
            .runtime_revisions
            .write()
            .insert(stored.config.id, stored.revision);
    }

    for node in state.scheduler.nodes() {
        if persisted_ids.contains(node.id()) {
            continue;
        }
        node.set_draining(true);
        state.scheduler.notify_state_change();
        let timeout = Duration::from_millis(state.settings.server.node_mutation_timeout_ms);
        if !state.scheduler.wait_for_node_idle(&node, timeout).await {
            failures.push(format!(
                "{}: active requests did not drain before removal",
                node.id()
            ));
            continue;
        }
        state.scheduler.remove_node(node.id());
        state.metrics.remove_node(node.id());
        state.runtime_revisions.write().remove(node.id());
    }

    if failures.is_empty() {
        state.control_revision.store(after, AtomicOrdering::Release);
        Ok(())
    } else {
        anyhow::bail!(failures.join("; "))
    }
}

async fn finish_server(
    name: &'static str,
    handle: &mut JoinHandle<std::io::Result<()>>,
    deadline: tokio::time::Instant,
) -> Result<()> {
    if let Ok(result) = tokio::time::timeout_at(deadline, &mut *handle).await {
        flatten_server_result(result)
    } else {
        warn!(
            server = name,
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
    let anthropic = request.uri().path().starts_with("/v1/messages");
    let id = request
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty() && value.len() <= 128)
        .map_or_else(|| Uuid::now_v7().to_string(), str::to_owned);
    request.extensions_mut().insert(RequestId(id.clone()));
    let mut response = next.run(request).await;
    if let Ok(value) = HeaderValue::from_str(&id) {
        response.headers_mut().insert("x-request-id", value.clone());
        if anthropic {
            response.headers_mut().insert("request-id", value);
        }
    }
    response
}

async fn track_public_response(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let guard = state.process.track_response();
    let response = next.run(request).await;
    let (parts, body) = response.into_parts();
    let stream = async_stream::stream! {
        let _guard = guard;
        let mut body = body.into_data_stream();
        while let Some(item) = body.next().await {
            yield item;
        }
    };
    Response::from_parts(parts, Body::from_stream(stream))
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
        response = if path.starts_with("/v1/messages") {
            anthropic::error_response(&GatewayError::PayloadTooLarge, &request_id)
        } else {
            GatewayError::PayloadTooLarge.into_response()
        };
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
        "/v1/messages" => "anthropic_messages",
        "/v1/messages/count_tokens" => "anthropic_count_tokens",
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

async fn api_hello() -> StatusCode {
    StatusCode::NO_CONTENT
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

async fn process_status(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
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

async fn activate_process(State(state): State<Arc<AppState>>) -> Response {
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

async fn drain_process(State(state): State<Arc<AppState>>) -> Response {
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
                snapshot["exact_kv_bytes"] = json!(cache.bytes);
                snapshot
            })
            .collect::<Vec<_>>()
    }))
}

async fn admin_nodes(State(state): State<Arc<AppState>>) -> Response {
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
    let response_buffer = state.response_buffer.snapshot();
    let ready = state.process.accepting_traffic() && routable_nodes > 0;

    Json(json!({
        "status": if ready { "ready" } else { "not_ready" },
        "live": true,
        "ready": ready,
        "version": env!("CARGO_PKG_VERSION"),
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

async fn admin_node(State(state): State<Arc<AppState>>, Path(node_id): Path<String>) -> Response {
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

fn admin_node_payload(state: &AppState, stored: &StoredNode, node: &Node) -> serde_json::Value {
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
struct CredentialMutationQuery {
    clear_api_key: bool,
    clear_headers: bool,
}

async fn preflight_node(
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
struct UpdateNodeRequest {
    revision: u64,
    config: NodeConfig,
    #[serde(default)]
    clear_api_key: bool,
    #[serde(default)]
    clear_headers: bool,
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

async fn delete_node(
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
            .unwrap_or(state.settings.server.node_mutation_timeout_ms)
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

async fn resume_node(State(state): State<Arc<AppState>>, Path(node_id): Path<String>) -> Response {
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
        assert_eq!(metric_endpoint("/v1/messages"), "anthropic_messages");
        assert_eq!(metric_endpoint("/v1/unknown/user-value"), "other");
    }

    #[tokio::test]
    async fn public_listener_waits_before_accepting_above_connection_limit() {
        let inner = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = inner.local_addr().unwrap();
        let metrics = Metrics::new();
        let mut listener = BoundedTcpListener::new(
            inner,
            1,
            Arc::clone(&metrics),
            true,
            CancellationToken::new(),
        );

        let first_client = tokio::net::TcpStream::connect(address).await.unwrap();
        let (first_server, _) = listener.accept().await;
        assert_eq!(metrics.public_connections(), 1);
        let second_client = tokio::net::TcpStream::connect(address).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );

        drop(first_server);
        let (second_server, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
            .await
            .unwrap();
        assert_eq!(metrics.public_connections(), 1);
        drop((first_client, second_client, second_server));
        assert_eq!(metrics.public_connections(), 0);
    }
}
