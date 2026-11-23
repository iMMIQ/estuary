//! Canonical node defaults, numeric limits, and typed validation shared with the admin editor.
//! Generate the checked-in web contract with `estuary-contract`'s `config_contract` example.
use anyhow::{Result, ensure};
use tokio::sync::Semaphore;
use url::Url;

pub use estuary_contract::config::{
    IntegerBounds, JS_MAX_SAFE_INTEGER, MIN_VLLM_MONITOR_INTERVAL_MS, NodeIntegerLimits,
    RESERVED_UPSTREAM_HEADERS, editor_node_defaults, js_integer_limits,
};

use super::{MAX_TOKENIZE_CACHE_ENTRIES, NodeConfig, ProviderKind};

const _: () = assert!(
    estuary_contract::config::MAX_SEMAPHORE_PERMITS == Semaphore::MAX_PERMITS,
    "estuary-contract MAX_SEMAPHORE_PERMITS must match tokio::sync::Semaphore::MAX_PERMITS"
);

fn valid_header(name: &str) -> bool {
    http::HeaderName::from_bytes(name.as_bytes()).is_ok()
        && !RESERVED_UPSTREAM_HEADERS.contains(&name.to_ascii_lowercase().as_str())
}

fn without_url_parts(url: &Url) -> bool {
    url.username().is_empty()
        && url.password().is_none()
        && url.query().is_none()
        && url.fragment().is_none()
}

fn valid_provider_path(base: &Url, path: &str) -> bool {
    path.starts_with('/')
        && base.join(path).is_ok_and(|url| {
            url.origin() == base.origin() && url.query().is_none() && url.fragment().is_none()
        })
}

fn valid_tcp_endpoint(value: &str) -> bool {
    Url::parse(value).is_ok_and(|url| {
        url.scheme() == "tcp"
            && url.host_str().is_some_and(|host| !host.contains('*'))
            && url.port().is_some()
            && without_url_parts(&url)
            && matches!(url.path(), "" | "/")
    })
}

pub fn validate_node(node: &NodeConfig) -> Result<()> {
    ensure!(!node.id.trim().is_empty(), "node id must not be empty");
    let base = Url::parse(&node.base_url)
        .ok()
        .filter(|url| matches!(url.scheme(), "http" | "https") && url.host_str().is_some());
    let Some(base) = base else {
        anyhow::bail!("node {} base_url must be an absolute HTTP(S) URL", node.id);
    };
    ensure!(
        without_url_parts(&base),
        "node {} base_url must not contain credentials, query or fragment",
        node.id
    );
    ensure!(
        !node.api_key.as_deref().is_some_and(|s| s.trim().is_empty()),
        "node {} api_key must not be empty when configured",
        node.id
    );
    ensure!(
        !node
            .api_key_env
            .as_deref()
            .is_some_and(|s| s.trim().is_empty()),
        "node {} api_key_env must not be empty when configured",
        node.id
    );
    ensure!(
        !node.health_path.trim().is_empty(),
        "node {} health_path must not be empty",
        node.id
    );
    ensure!(
        (1..=Semaphore::MAX_PERMITS).contains(&node.max_concurrency),
        "node {} max_concurrency must be a positive integer within the runtime limit",
        node.id
    );
    ensure!(
        node.weight.is_finite() && node.weight > 0.0,
        "node {} weight must be finite and greater than zero",
        node.id
    );
    ensure!(
        !node.models.is_empty()
            && node
                .models
                .iter()
                .all(|(k, v)| !k.trim().is_empty() && !v.trim().is_empty()),
        "node {} models must declare non-empty model mappings",
        node.id
    );
    ensure!(
        node.model_capabilities.keys().all(|key| {
            !key.trim().is_empty()
                && (key == "*" || node.models.contains_key(key) || node.models.contains_key("*"))
        }),
        "node {} model_capabilities must refer to mapped models or a wildcard",
        node.id
    );
    ensure!(
        node.headers.keys().all(|k| valid_header(k)),
        "node {} headers contains an invalid or reserved header name",
        node.id
    );
    ensure!(
        node.headers_from_env
            .iter()
            .all(|(k, v)| valid_header(k) && !v.trim().is_empty()),
        "node {} headers_from_env contains an invalid/reserved header name or empty env var",
        node.id
    );

    let provider = &node.provider;
    if provider.kind != ProviderKind::Vllm {
        ensure!(
            provider.kv_events.is_none(),
            "node {} provider.kv_events requires provider.type: vllm",
            node.id
        );
        return Ok(());
    }
    validate_vllm_provider(&node.id, &base, provider)
}

fn validate_vllm_provider(
    node_id: &str,
    base: &Url,
    provider: &super::ProviderConfig,
) -> Result<()> {
    for (field, path) in [
        ("version_path", &provider.version_path),
        ("metrics_path", &provider.metrics_path),
        ("tokenize_path", &provider.tokenize_path),
    ] {
        ensure!(
            valid_provider_path(base, path),
            "node {node_id} provider.{field} must start with '/' and stay on the upstream origin without query or fragment"
        );
    }
    ensure!(
        provider.monitor_interval_ms >= MIN_VLLM_MONITOR_INTERVAL_MS
            && provider.request_timeout_ms >= 1
            && provider.telemetry_stale_ms >= 1
            && provider.waiting_threshold >= 1
            && (1..=MAX_TOKENIZE_CACHE_ENTRIES).contains(&provider.tokenize_cache_entries),
        "node {node_id} provider integer limits are outside the supported range"
    );
    ensure!(
        provider.telemetry_stale_ms >= provider.monitor_interval_ms,
        "node {node_id} provider.telemetry_stale_ms must not be shorter than monitor_interval_ms"
    );

    if let Some(kv) = &provider.kv_events {
        ensure!(
            valid_tcp_endpoint(&kv.endpoint),
            "node {node_id} provider.kv_events.endpoint must be a connectable tcp://host:port endpoint"
        );
        ensure!(
            kv.replay_endpoint.as_deref().is_none_or(valid_tcp_endpoint),
            "node {node_id} provider.kv_events.replay_endpoint must be a connectable tcp://host:port endpoint"
        );
        ensure!(
            kv.reconnect_ms >= 1
                && kv.max_blocks >= 1
                && kv.max_directory_bytes >= 1
                && kv.max_event_bytes >= 1,
            "node {node_id} provider.kv_events limits must be positive integers"
        );
    }
    Ok(())
}
