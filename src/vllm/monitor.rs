use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use reqwest::Client;
use serde::Deserialize;
use tokio::{
    sync::{Notify, watch},
    time::MissedTickBehavior,
};
use tracing::{debug, info, warn};

use crate::{
    config::ProviderKind,
    kv_cache::ExactCacheDirectory,
    node::{CounterKind, Node, ProviderCounter, ProviderState, VllmMetricsSnapshot},
    prefix::PrefixDirectory,
};

use super::{MAX_MANAGEMENT_BODY_BYTES, VERSION_RECHECK_TICKS, read_bounded_response};

const MIN_VLLM_VERSION: (u64, u64, u64) = (0, 25, 0);
const MIN_VLLM_VERSION_DISPLAY: &str = "0.25.0";

pub(super) async fn run_node_monitor(
    client: Client,
    node: Arc<Node>,
    exact_cache: Arc<ExactCacheDirectory>,
    prefix: Arc<PrefixDirectory>,
    notify: Arc<Notify>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut interval =
        tokio::time::interval(Duration::from_millis(node.provider().monitor_interval_ms));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut tick = 0_u64;
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            _ = interval.tick() => {
                if node.is_retired() {
                    break;
                }
                let provider_was_ready = node.provider_is_ready();
                let waiting_was_blocked = node
                    .fresh_vllm_waiting()
                    .is_some_and(|waiting| waiting >= node.provider().waiting_threshold);
                let generation = node.provider_generation();
                poll_node(&client, &node, tick.is_multiple_of(VERSION_RECHECK_TICKS)).await;
                if node.is_retired() {
                    break;
                }
                if node.provider_generation() != generation {
                    exact_cache.invalidate_node_owned(node.id(), node.instance_id());
                    prefix.clear_node(node.id());
                }
                let provider_became_ready = !provider_was_ready && node.provider_is_ready();
                let waiting_is_blocked = node
                    .fresh_vllm_waiting()
                    .is_some_and(|waiting| waiting >= node.provider().waiting_threshold);
                if provider_became_ready || (waiting_was_blocked && !waiting_is_blocked) {
                    notify.notify_waiters();
                }
                tick = tick.wrapping_add(1);
            }
        }
    }
}

pub(super) async fn poll_node(client: &Client, node: &Arc<Node>, check_version: bool) {
    if check_version || node.provider_state() == ProviderState::Checking {
        match fetch_version(client, node).await {
            Ok(raw) if is_supported_vllm_version(&raw) => {
                if node.provider_state() != ProviderState::Ready {
                    info!(node = node.id(), version = %raw, "vLLM provider is ready");
                }
                node.record_vllm_ready(raw);
            }
            Ok(raw) => {
                let message = format!(
                    "vLLM {raw} is unsupported; Estuary requires >= {MIN_VLLM_VERSION_DISPLAY}"
                );
                if node.record_vllm_incompatible(Some(raw.clone()), message.clone()) {
                    warn!(node = node.id(), version = %raw, required = MIN_VLLM_VERSION_DISPLAY, "{message}");
                }
                return;
            }
            Err(error) => {
                let changed =
                    node.record_provider_telemetry_error(format!("version probe failed: {error}"));
                if changed && node.provider_state() != ProviderState::Ready {
                    warn!(node = node.id(), error = %error, required = MIN_VLLM_VERSION_DISPLAY, "vLLM compatibility check failed; node remains out of rotation");
                } else {
                    debug!(node = node.id(), error = %error, "vLLM version probe failed");
                }
                if node.provider_state() != ProviderState::Ready {
                    return;
                }
            }
        }
    }

    if node.provider_state() != ProviderState::Ready {
        return;
    }
    match fetch_metrics(client, node).await {
        Ok(telemetry) => node.record_vllm_metrics(telemetry),
        Err(error) => {
            node.record_provider_telemetry_error(format!("metrics scrape failed: {error}"));
            debug!(node = node.id(), error = %error, "vLLM metrics scrape failed");
        }
    }
}

pub async fn preflight_vllm(client: &Client, node: &Arc<Node>) -> Result<()> {
    if node.provider().kind != ProviderKind::Vllm {
        return Ok(());
    }
    let raw = fetch_version(client, node).await?;
    if !is_supported_vllm_version(&raw) {
        bail!("vLLM {raw} is unsupported; Estuary requires >= {MIN_VLLM_VERSION_DISPLAY}");
    }
    node.record_vllm_ready(raw);
    let telemetry = fetch_metrics(client, node).await?;
    node.record_vllm_metrics(telemetry);
    Ok(())
}

#[derive(Deserialize)]
struct VersionResponse {
    version: String,
}

async fn fetch_version(client: &Client, node: &Node) -> Result<String> {
    let body = management_get(client, node, &node.provider().version_path).await?;
    let response: VersionResponse =
        serde_json::from_slice(&body).context("invalid /version JSON")?;
    if response.version.trim().is_empty() {
        bail!(
            "invalid vLLM version {:?}: version is empty",
            response.version
        );
    }
    Ok(response.version)
}

fn release_version(raw: &str) -> Option<(u64, u64, u64)> {
    let normalized = raw.trim().trim_start_matches('v');
    let prefix = normalized
        .split(|ch: char| !ch.is_ascii_digit() && ch != '.')
        .next()
        .unwrap_or_default()
        .trim_end_matches('.');
    let mut components = prefix.split('.');
    let major = components.next()?.parse().ok()?;
    let minor = components.next()?.parse().ok()?;
    let patch = components
        .next()
        .and_then(|part| part.parse().ok())
        .unwrap_or(0);
    Some((major, minor, patch))
}

pub(super) fn is_supported_vllm_version(raw: &str) -> bool {
    // Python package versions can use dev/rc suffixes or omit the patch number.
    // Compare only the numeric release prefix; opaque build labels such as
    // "dev" cannot establish an old release and are allowed through the gate.
    release_version(raw).is_none_or(|version| version >= MIN_VLLM_VERSION)
}

async fn fetch_metrics(client: &Client, node: &Node) -> Result<VllmMetricsSnapshot> {
    let body = management_get(client, node, &node.provider().metrics_path).await?;
    parse_metrics(&body)
}

async fn management_get(client: &Client, node: &Node, path: &str) -> Result<Bytes> {
    let url = node.provider_url(path)?;
    let response = node
        .apply_headers(
            client
                .get(url)
                .timeout(Duration::from_millis(node.provider().request_timeout_ms)),
        )
        .send()
        .await?
        .error_for_status()?;
    if response
        .content_length()
        .is_some_and(|length| length > MAX_MANAGEMENT_BODY_BYTES as u64)
    {
        bail!("vLLM management response is too large");
    }
    read_bounded_response(response, "vLLM management response").await
}

/// Parse only the scalar `vllm:*` / `vllm_*` gauges and counters Estuary consumes
/// without pulling in a full Prometheus exposition AST parser.
pub(super) fn parse_metrics(body: &[u8]) -> Result<VllmMetricsSnapshot> {
    let text = std::str::from_utf8(body).context("invalid UTF-8 in Prometheus exposition")?;
    let mut running = None;
    let mut waiting = None;
    let mut kv_usage = None;
    let mut counter_series = Vec::new();

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((metric, labels, value)) = parse_sample_line(line) else {
            continue;
        };
        let Some(suffix) = metric
            .strip_prefix("vllm:")
            .or_else(|| metric.strip_prefix("vllm_"))
        else {
            continue;
        };
        match suffix {
            "num_requests_running" => running = Some(running.unwrap_or(0.0) + value),
            "num_requests_waiting" => waiting = Some(waiting.unwrap_or(0.0) + value),
            "kv_cache_usage_perc" => kv_usage = Some(kv_usage.unwrap_or(0.0_f64).max(value)),
            other => {
                let kind = match other.trim_end_matches("_total") {
                    "prompt_tokens" => Some(CounterKind::PromptTokens),
                    "generation_tokens" => Some(CounterKind::GenerationTokens),
                    "request_success" => Some(CounterKind::Requests),
                    "prefix_cache_queries" => Some(CounterKind::PrefixQueries),
                    "prefix_cache_hits" => Some(CounterKind::PrefixHits),
                    "num_preemptions" => Some(CounterKind::Preemptions),
                    _ => None,
                };
                if let Some(kind) = kind
                    && value.is_finite()
                    && value >= 0.0
                    && counter_series.len() <= 512
                {
                    let key = format!(
                        "{}:{}",
                        kind as usize,
                        blake3::hash(labels.as_bytes()).to_hex()
                    );
                    counter_series.push(ProviderCounter { key, kind, value });
                }
            }
        }
    }

    let running = finite_count(running.ok_or_else(|| anyhow!("running metric is missing"))?)?;
    let waiting = finite_count(waiting.ok_or_else(|| anyhow!("waiting metric is missing"))?)?;
    let kv_cache_usage = kv_usage
        .filter(|value| value.is_finite())
        .map(|value| value.clamp(0.0, 1.0));
    Ok(VllmMetricsSnapshot {
        counter_series,
        running,
        waiting,
        kv_cache_usage,
    })
}

fn parse_sample_line(line: &str) -> Option<(&str, &str, f64)> {
    let bytes = line.as_bytes();
    let name_end = bytes
        .iter()
        .position(|&b| b == b'{' || b.is_ascii_whitespace())?;
    let metric = &line[..name_end];
    let rest = &line[name_end..];
    let (labels, value_part) = if let Some(after_open) = rest.strip_prefix('{') {
        let close = find_label_close(after_open)?;
        (&after_open[..close], after_open[close + 1..].trim_start())
    } else {
        ("", rest.trim_start())
    };
    let token = value_part.split_ascii_whitespace().next()?;
    let value = match token {
        "+Inf" | "Inf" | "+inf" | "inf" => f64::INFINITY,
        "-Inf" | "-inf" => f64::NEG_INFINITY,
        "NaN" | "nan" => f64::NAN,
        other => other.parse::<f64>().ok()?,
    };
    Some((metric, labels, value))
}

fn find_label_close(input: &str) -> Option<usize> {
    let bytes = input.as_bytes();
    let mut in_quotes = false;
    let mut escaped = false;
    for (index, &byte) in bytes.iter().enumerate() {
        if in_quotes {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_quotes = false;
            }
        } else if byte == b'"' {
            in_quotes = true;
        } else if byte == b'}' {
            return Some(index);
        }
    }
    None
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn finite_count(value: f64) -> Result<usize> {
    if !value.is_finite() || value < 0.0 || value > usize::MAX as f64 {
        bail!("invalid vLLM request count {value}");
    }
    Ok(value.round() as usize)
}
