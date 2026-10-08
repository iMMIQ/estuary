use std::{
    collections::HashMap,
    net::{SocketAddr, TcpListener as StdTcpListener},
    path::Path as FsPath,
    sync::{Arc, atomic::AtomicU64},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::StatusCode,
    middleware::{self as axum_middleware},
    routing::{any, get, put},
};
use parking_lot::RwLock;
use reqwest::redirect::Policy;
use serde_json::json;
use tokio::{
    net::TcpListener,
    sync::{Mutex as AsyncMutex, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    Settings,
    connection_drain::{PendingConnection, PendingConnections},
    health::run_health_monitor,
    lifecycle::ProcessLifecycle,
    metrics::Metrics,
    node::Node,
    proxy,
    response_buffer::ResponseBufferBudget,
    scheduler::Scheduler,
    store::NodeStore,
    vllm::VllmManager,
};

mod transport;
use transport::{BoundedTcpListener, ConnectionTracker, connection_request_started};
mod assets;
use assets::{admin_asset, admin_index, admin_redirect};
mod middleware;
use middleware::{
    admit_public_request, assign_request_id, authorize_admin, observe_request,
    track_public_response,
};
mod reconcile;
use reconcile::run_control_reconciler;
mod admin;
use admin::{
    activate_process, admin_node, admin_nodes, admin_status, create_node, delete_ip_limit,
    delete_node, drain_node, drain_process, live, metrics, preflight_node, process_status, ready,
    resume_node, set_ip_limit, update_node,
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
    connections: Arc<ConnectionTracker>,
    runtime_revisions: RwLock<HashMap<String, u64>>,
    control_revision: AtomicU64,
    admin_mutation: AsyncMutex<()>,
}

pub struct Gateway {
    state: Arc<AppState>,
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
            .user_agent(concat!("estuary/", env!("ESTUARY_BUILD_VERSION")))
            .build()
            .context("failed to build upstream HTTP client")?;
        let scheduler = Arc::new(Scheduler::new(nodes.clone(), settings.routing.clone()));
        let vllm = VllmManager::new(Arc::clone(&scheduler));
        let metrics = Metrics::new();
        let response_buffer = ResponseBufferBudget::new(
            settings.server.max_buffered_response_bytes,
            Arc::clone(&metrics),
        );
        let connections = Arc::new(ConnectionTracker::default());
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
                connections,
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
            .layer(axum_middleware::from_fn_with_state(
                Arc::clone(&self.state),
                admit_public_request,
            ))
            .layer(axum_middleware::from_fn_with_state(
                Arc::clone(&self.state),
                observe_request,
            ))
            .layer(axum_middleware::from_fn(assign_request_id))
            .layer(axum_middleware::from_fn_with_state(
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
            .route(
                "/admin/api/ip-limits/{ip}",
                put(set_ip_limit).delete(delete_ip_limit),
            )
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
            .layer(axum_middleware::from_fn_with_state(
                Arc::clone(&self.state),
                authorize_admin,
            ));
        Router::new()
            .route("/health/live", get(live))
            .route("/health/ready", get(ready))
            .merge(protected)
            .fallback(proxy::not_found)
            .layer(axum_middleware::from_fn_with_state(
                Arc::clone(&self.state),
                observe_request,
            ))
            .layer(axum_middleware::from_fn(assign_request_id))
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
            Arc::clone(&self.state.connections),
            true,
            public_accept_cancellation.clone(),
        );
        let public_pending = Arc::clone(&public_listener.pending_connections);
        let admin_listener = BoundedTcpListener::new(
            admin_listener,
            self.state.settings.server.max_admin_connections,
            Arc::clone(&self.state.metrics),
            Arc::clone(&self.state.connections),
            false,
            admin_cancellation.clone(),
        );
        let public_router = self
            .public_router()
            .layer(axum_middleware::from_fn(connection_request_started))
            .into_make_service_with_connect_info::<PendingConnection>();
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
                &public_pending,
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
        if !health_done && let Err(error) = health_handle.await {
            first_error
                .get_or_insert_with(|| anyhow::anyhow!("health monitor task failed: {error}"));
        }
        if !provider_done && let Err(error) = provider_handle.await {
            first_error.get_or_insert_with(|| {
                anyhow::anyhow!("vLLM provider monitor task failed: {error}")
            });
        }
        if !control_done && let Err(error) = control_handle.await {
            first_error.get_or_insert_with(|| {
                anyhow::anyhow!("control-plane reconciler task failed: {error}")
            });
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
        public_pending: &PendingConnections,
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
        let shutdown_grace = Duration::from_millis(self.state.settings.server.shutdown_grace_ms);
        let deadline = tokio::time::Instant::now() + shutdown_grace;
        if !public_done
            && tokio::time::timeout_at(deadline, public_pending.wait_for_requests())
                .await
                .is_err()
        {
            warn!("timed out waiting for accepted connections to begin their first request");
        }
        public_cancellation.cancel();
        let mut first_error = None;
        if !public_done && let Err(error) = finish_server("public", public_handle, deadline).await {
            first_error = Some(error);
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
        if !admin_done && let Err(error) = finish_server("admin", admin_handle, deadline).await {
            first_error.get_or_insert(error);
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

async fn api_hello() -> StatusCode {
    StatusCode::NO_CONTENT
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
mod tests;
