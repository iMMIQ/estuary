use std::{
    io::{BufRead, BufReader},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use prometheus_parse::{Scrape, Value as MetricValue};
use reqwest::Client;
use semver::Version;
use serde::Deserialize;
use tokio::{
    sync::{Notify, watch},
    time::MissedTickBehavior,
};
use tracing::{debug, info, warn};

use crate::{
    config::ProviderKind,
    kv_cache::ExactCacheDirectory,
    node::{Node, ProviderState, VllmMetricsSnapshot},
    prefix::PrefixDirectory,
};

use super::{
    MAX_MANAGEMENT_BODY_BYTES, MIN_VLLM_VERSION, VERSION_RECHECK_TICKS, read_bounded_response,
};

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
                let message =
                    format!("vLLM {raw} is unsupported; Estuary requires >= {MIN_VLLM_VERSION}");
                if node.record_vllm_incompatible(Some(raw.clone()), message.clone()) {
                    warn!(node = node.id(), version = %raw, required = %MIN_VLLM_VERSION, "{message}");
                }
                return;
            }
            Err(error) => {
                let changed =
                    node.record_provider_telemetry_error(format!("version probe failed: {error}"));
                if changed && node.provider_state() != ProviderState::Ready {
                    warn!(node = node.id(), error = %error, required = %MIN_VLLM_VERSION, "vLLM compatibility check failed; node remains out of rotation");
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
        bail!("vLLM {raw} is unsupported; Estuary requires >= {MIN_VLLM_VERSION}");
    }
    node.record_vllm_ready(raw);
    let telemetry = fetch_metrics(client, node).await?;
    node.record_vllm_metrics(telemetry);
    Ok(())
}

#[derive(Deserialize)]
pub(super) struct VersionResponse {
    pub(super) version: String,
}

pub(super) async fn fetch_version(client: &Client, node: &Node) -> Result<String> {
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

pub(super) fn is_supported_vllm_version(raw: &str) -> bool {
    let normalized = raw.trim().trim_start_matches('v');
    // Python package versions can use dev/rc suffixes or omit the patch number.
    // Compare only the numeric release prefix; opaque build labels such as
    // "dev" cannot establish an old release and are allowed through the gate.
    let release = normalized
        .split(|ch: char| !ch.is_ascii_digit() && ch != '.')
        .next()
        .unwrap_or_default()
        .trim_end_matches('.');
    let mut components = release.split('.');
    let (Ok(major), Some(Ok(minor))) = (
        components.next().unwrap_or_default().parse(),
        components.next().map(str::parse),
    ) else {
        return true;
    };
    let patch = components
        .next()
        .and_then(|component| component.parse().ok())
        .unwrap_or(0);
    Version::new(major, minor, patch) >= MIN_VLLM_VERSION
}

pub(super) async fn fetch_metrics(client: &Client, node: &Node) -> Result<VllmMetricsSnapshot> {
    let body = management_get(client, node, &node.provider().metrics_path).await?;
    parse_metrics(&body)
}

pub(super) async fn management_get(client: &Client, node: &Node, path: &str) -> Result<Bytes> {
    let url = node.provider_url(path)?;
    let mut request = client
        .get(url)
        .timeout(Duration::from_millis(node.provider().request_timeout_ms));
    for (name, value) in node.headers() {
        request = request.header(name, value);
    }
    let response = request.send().await?.error_for_status()?;
    if response
        .content_length()
        .is_some_and(|length| length > MAX_MANAGEMENT_BODY_BYTES as u64)
    {
        bail!("vLLM management response is too large");
    }
    read_bounded_response(response, "vLLM management response").await
}

pub(super) fn parse_metrics(body: &[u8]) -> Result<VllmMetricsSnapshot> {
    let reader = BufReader::new(body);
    // prometheus-parse accepts the Prometheus grammar except ':' in metric names.
    // Normalize vLLM's legal namespace separator before structured parsing.
    let lines = reader
        .lines()
        .map(|line| line.map(|line| line.replacen("vllm:", "vllm_", 1)));
    let scrape = Scrape::parse(lines).context("invalid Prometheus exposition")?;
    let mut running = None;
    let mut waiting = None;
    let mut kv_usage = None;
    let mut prompt_tokens = None;
    let mut generation_tokens = None;
    let mut requests = None;
    let mut prefix_queries = None;
    let mut prefix_hits = None;
    let mut preemptions = None;
    for sample in scrape.samples {
        let value = scalar_metric(&sample.value);
        match sample.metric.as_str() {
            "vllm_num_requests_running" => {
                running = Some(running.unwrap_or(0.0) + value.unwrap_or(0.0));
            }
            "vllm_num_requests_waiting" => {
                waiting = Some(waiting.unwrap_or(0.0) + value.unwrap_or(0.0));
            }
            "vllm_kv_cache_usage_perc" => {
                kv_usage = Some(kv_usage.unwrap_or(0.0_f64).max(value.unwrap_or(0.0)));
            }
            "vllm_prompt_tokens" | "vllm_prompt_tokens_total" => {
                accumulate_counter(&mut prompt_tokens, value);
            }
            "vllm_generation_tokens" | "vllm_generation_tokens_total" => {
                accumulate_counter(&mut generation_tokens, value);
            }
            "vllm_request_success" | "vllm_request_success_total" => {
                accumulate_counter(&mut requests, value);
            }
            "vllm_prefix_cache_queries" | "vllm_prefix_cache_queries_total" => {
                accumulate_counter(&mut prefix_queries, value);
            }
            "vllm_prefix_cache_hits" | "vllm_prefix_cache_hits_total" => {
                accumulate_counter(&mut prefix_hits, value);
            }
            "vllm_num_preemptions" | "vllm_num_preemptions_total" => {
                accumulate_counter(&mut preemptions, value);
            }
            _ => {}
        }
    }
    let running = finite_count(running.ok_or_else(|| anyhow!("running metric is missing"))?)?;
    let waiting = finite_count(waiting.ok_or_else(|| anyhow!("waiting metric is missing"))?)?;
    let kv_cache_usage = kv_usage
        .filter(|value| value.is_finite())
        .map(|value| value.clamp(0.0, 1.0));
    Ok(VllmMetricsSnapshot {
        running,
        waiting,
        kv_cache_usage,
        prompt_tokens_total: prompt_tokens,
        generation_tokens_total: generation_tokens,
        requests_total: requests,
        prefix_cache_queries_total: prefix_queries,
        prefix_cache_hits_total: prefix_hits,
        preemptions_total: preemptions,
    })
}

pub(super) fn accumulate_counter(total: &mut Option<f64>, value: Option<f64>) {
    if let Some(value) = value.filter(|value| value.is_finite() && *value >= 0.0) {
        *total = Some(total.unwrap_or_default() + value);
    }
}

pub(super) fn scalar_metric(value: &MetricValue) -> Option<f64> {
    match value {
        MetricValue::Counter(value) | MetricValue::Gauge(value) | MetricValue::Untyped(value) => {
            Some(*value)
        }
        MetricValue::Histogram(_) | MetricValue::Summary(_) => None,
    }
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub(super) fn finite_count(value: f64) -> Result<usize> {
    if !value.is_finite() || value < 0.0 || value > usize::MAX as f64 {
        bail!("invalid vLLM request count {value}");
    }
    Ok(value.round() as usize)
}
