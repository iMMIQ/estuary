use std::{
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use futures_util::future::join_all;
use reqwest::Client;
use tokio::{sync::watch, time::MissedTickBehavior};
use tracing::{debug, warn};

use crate::{config::HealthConfig, node::Node, scheduler::Scheduler};

pub async fn run_health_monitor(
    client: Client,
    scheduler: Arc<Scheduler>,
    config: HealthConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    let jitter_seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .to_le_bytes();
    probe_all(&client, &scheduler, &config, &jitter_seed).await;
    let mut interval = tokio::time::interval(config.interval());
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval.tick().await;

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            _ = interval.tick() => {
                probe_all(&client, &scheduler, &config, &jitter_seed).await;
            },
        }
    }
}

async fn probe_all(
    client: &Client,
    scheduler: &Scheduler,
    config: &HealthConfig,
    jitter_seed: &[u8; 16],
) {
    let nodes = scheduler.nodes();
    join_all(nodes.iter().map(|node| async move {
        let delay = probe_jitter(node.id(), config, jitter_seed);
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        probe(client, node, config).await;
        scheduler.notify_state_change();
    }))
    .await;
}

fn probe_jitter(
    node_id: &str,
    config: &HealthConfig,
    jitter_seed: &[u8; 16],
) -> std::time::Duration {
    if config.jitter_percent == 0 {
        return std::time::Duration::ZERO;
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(jitter_seed);
    hasher.update(node_id.as_bytes());
    let digest = hasher.finalize();
    let fraction = f64::from(u16::from_le_bytes([
        digest.as_bytes()[0],
        digest.as_bytes()[1],
    ])) / f64::from(u16::MAX);
    config
        .interval()
        .mul_f64(fraction * f64::from(config.jitter_percent) / 100.0)
}

async fn probe(client: &Client, node: &Arc<Node>, config: &HealthConfig) {
    let started = Instant::now();
    let mut request = client
        .get(node.health_url().clone())
        .timeout(config.timeout());
    for (name, value) in node.headers() {
        request = request.header(name, value);
    }
    match request.send().await {
        Ok(response) if response.status().is_success() => {
            node.record_probe_success(config);
            debug!(
                node = node.id(),
                elapsed_ms = started.elapsed().as_millis(),
                "health probe succeeded"
            );
        }
        Ok(response) => {
            let message = format!("health probe returned {}", response.status());
            if node.record_probe_failure(&message, config) {
                warn!(node = node.id(), status = %response.status(), "health state changed after probe failure");
            } else {
                debug!(node = node.id(), status = %response.status(), "health probe remains unsuccessful");
            }
        }
        Err(error) => {
            if node.record_probe_failure(error.to_string(), config) {
                warn!(node = node.id(), error = %error, "health state changed after probe failure");
            } else {
                debug!(node = node.id(), error = %error, "health probe remains unsuccessful");
            }
        }
    }
}

pub async fn preflight_health(
    client: &Client,
    node: &Arc<Node>,
    config: &HealthConfig,
) -> Result<()> {
    let mut request = client
        .get(node.health_url().clone())
        .timeout(config.timeout());
    for (name, value) in node.headers() {
        request = request.header(name, value);
    }
    let response = request
        .send()
        .await
        .with_context(|| format!("health check for node {:?} failed", node.id()))?;
    if !response.status().is_success() {
        bail!(
            "health check for node {:?} returned {}",
            node.id(),
            response.status()
        );
    }
    node.record_probe_success(config);
    Ok(())
}
