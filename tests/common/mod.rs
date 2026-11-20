#![allow(dead_code)]

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use axum::Router;
use estuary::{
    Gateway, Settings,
    config::{HealthConfig, NodeConfig, RetryConfig},
    node::{HealthState, Node},
    prefix::PrefixInput,
    scheduler::{Scheduler, Selection},
};
use tokio::{net::TcpListener, task::JoinHandle, time::timeout};

pub const IO_TIMEOUT: Duration = Duration::from_secs(3);

pub struct TestServer {
    pub address: SocketAddr,
    pub base_url: String,
    task: JoinHandle<()>,
}

impl TestServer {
    pub async fn spawn(router: Router) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("test server failed");
        });
        Self {
            address,
            base_url: format!("http://{address}"),
            task,
        }
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub struct TempSqlite(pub PathBuf);

impl TempSqlite {
    pub fn new(prefix: &str) -> Self {
        Self(std::env::temp_dir().join(format!("{prefix}-{}.sqlite", uuid::Uuid::now_v7())))
    }
}

impl Drop for TempSqlite {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
        }
    }
}

pub fn test_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("test client")
}

pub fn test_client_with_timeout(limit: Duration) -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .timeout(limit)
        .build()
        .expect("test client")
}

pub fn node_config(
    id: &str,
    base_url: &str,
    models: impl IntoIterator<Item = (&'static str, &'static str)>,
) -> NodeConfig {
    NodeConfig {
        id: id.to_owned(),
        base_url: format!("{base_url}/v1"),
        models: models
            .into_iter()
            .map(|(public, upstream)| (public.to_owned(), upstream.to_owned()))
            .collect(),
        ..NodeConfig::default()
    }
}

pub fn gateway_settings(nodes: Vec<NodeConfig>) -> Settings {
    let mut settings = Settings {
        nodes,
        retry: RetryConfig {
            max_attempts: 1,
            ..RetryConfig::default()
        },
        ..Settings::default()
    };
    settings.health.route_while_starting = true;
    settings
}

pub async fn spawn_gateway(nodes: Vec<NodeConfig>) -> TestServer {
    let gateway = Gateway::build(gateway_settings(nodes)).expect("build gateway");
    TestServer::spawn(gateway.public_router()).await
}

pub async fn unused_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral address");
    listener.local_addr().expect("local address")
}

pub async fn wait_until<F, Fut>(limit: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    timeout(limit, async {
        while !check().await {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("condition timed out");
}

pub fn strict_health() -> HealthConfig {
    HealthConfig {
        healthy_threshold: 1,
        unhealthy_threshold: 1,
        passive_failure_threshold: 1,
        ..HealthConfig::default()
    }
}

pub fn healthy_node_for_model(id: &str, model: &str, max_concurrency: usize) -> Arc<Node> {
    let node = Node::from_config(&NodeConfig {
        id: id.to_owned(),
        base_url: format!("http://{id}.invalid/v1"),
        models: HashMap::from([(model.to_owned(), model.to_owned())]),
        max_concurrency,
        ..NodeConfig::default()
    })
    .expect("valid node configuration");
    node.record_probe_success(&strict_health());
    assert_eq!(node.health(), HealthState::Healthy);
    node
}

pub async fn acquire_model(
    scheduler: &Scheduler,
    model: Option<&str>,
    prefix_input: PrefixInput,
) -> Selection {
    scheduler
        .acquire(model, prefix_input, &HashSet::new(), 128)
        .await
        .expect("scheduler acquire")
}
