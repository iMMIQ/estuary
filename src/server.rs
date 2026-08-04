use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Request, State},
    http::{HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{any, get},
};
use reqwest::redirect::Policy;
use serde_json::json;
use tokio::{net::TcpListener, sync::watch, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    Settings, error::GatewayError, health::run_health_monitor, metrics::Metrics, node::Node, proxy,
    scheduler::Scheduler,
};

#[derive(Clone, Debug)]
pub struct RequestId(pub String);

pub struct AppState {
    pub(crate) client: reqwest::Client,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) settings: Arc<Settings>,
}

pub struct Gateway {
    state: Arc<AppState>,
}

impl Gateway {
    pub fn build(settings: Settings) -> Result<Self> {
        settings.validate()?;
        let nodes = settings
            .nodes
            .iter()
            .map(|config| {
                Node::from_config_with_startup_policy(config, settings.health.route_while_starting)
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
        let scheduler = Arc::new(Scheduler::new(nodes, settings.routing.clone()));
        Ok(Self {
            state: Arc::new(AppState {
                client,
                scheduler,
                metrics: Metrics::new(),
                settings: Arc::new(settings),
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
            .route("/health/live", get(live))
            .route("/health/ready", get(ready))
            .route("/metrics", get(metrics))
            .route("/admin/nodes", get(nodes))
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
        let health_handle = tokio::spawn(run_health_monitor(
            self.state.client.clone(),
            self.state.scheduler.nodes().to_vec(),
            Arc::clone(&self.state.scheduler),
            self.state.settings.health.clone(),
            health_receiver,
        ));

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

        cancellation.cancel();
        let _ = health_shutdown.send(true);
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
        _ => "other",
    }
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
            .map(|node| node.snapshot())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_paths_have_bounded_cardinality() {
        assert_eq!(metric_endpoint("/v1/chat/completions"), "chat_completions");
        assert_eq!(metric_endpoint("/v1/unknown/user-value"), "other");
    }
}
