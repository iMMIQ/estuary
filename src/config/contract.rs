//! Canonical node defaults and validation rules shared with the admin editor.
//! Generate the checked-in web contract with the `config_contract` example.
use anyhow::{Result, bail};
use serde::Serialize;
use serde_json::Value;
use url::Url;

use super::{MAX_TOKENIZE_CACHE_ENTRIES, NodeConfig, ProviderKind};

pub const RESERVED_UPSTREAM_HEADERS: &[&str] = &[
    "connection",
    "content-length",
    "host",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "x-gateway-request-id",
];

#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleCondition {
    Always,
    Vllm,
    KvEvents,
}

#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RuleKind {
    NotBlank,
    HttpUrl,
    UrlWithoutParts,
    Integer { minimum: u64, maximum: u64 },
    Positive,
    ModelMappings,
    CapabilityMappings,
    Headers,
    EnvironmentHeaders,
    ProviderPath,
    TcpEndpoint,
    AtLeastField { other: &'static str },
    KvProvider,
}

#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
#[derive(Clone, Debug, Serialize)]
pub struct NodeValidationRule {
    pub path: &'static str,
    pub error_field: &'static str,
    pub error: &'static str,
    pub message: &'static str,
    pub condition: RuleCondition,
    pub rule: RuleKind,
}

/// Editor presets deliberately differ from the minimal deserialization defaults.
/// Define that policy here so the browser never copies backend default literals.
pub fn editor_node_defaults() -> NodeConfig {
    NodeConfig {
        base_url: "http://127.0.0.1:8000/v1".to_owned(),
        max_concurrency: 16,
        provider: super::ProviderConfig {
            kind: ProviderKind::Vllm,
            ..super::ProviderConfig::default()
        },
        ..NodeConfig::default()
    }
}

#[allow(clippy::too_many_lines)]
pub fn node_validation_rules() -> Vec<NodeValidationRule> {
    use RuleCondition::{Always, KvEvents, Vllm};
    use RuleKind::{
        AtLeastField, CapabilityMappings, EnvironmentHeaders, Headers, HttpUrl, KvProvider,
        ModelMappings, NotBlank, Positive, ProviderPath, TcpEndpoint,
    };
    let mut rules = Vec::new();
    let mut add = |path, error_field, error, message, condition, rule| {
        rules.push(NodeValidationRule {
            path,
            error_field,
            error,
            message,
            condition,
            rule,
        });
    };
    add(
        "id",
        "id",
        "validation.nodeIdRequired",
        "must not be empty",
        Always,
        NotBlank,
    );
    add(
        "base_url",
        "base_url",
        "validation.absoluteUrl",
        "must be an absolute HTTP(S) URL",
        Always,
        HttpUrl,
    );
    add(
        "base_url",
        "base_url",
        "validation.urlParts",
        "must not contain credentials, query or fragment",
        Always,
        RuleKind::UrlWithoutParts,
    );
    add(
        "api_key",
        "api_key",
        "validation.notBlank",
        "must not be empty when configured",
        Always,
        NotBlank,
    );
    add(
        "api_key_env",
        "api_key_env",
        "validation.notBlank",
        "must not be empty when configured",
        Always,
        NotBlank,
    );
    add(
        "health_path",
        "health_path",
        "validation.healthPathRequired",
        "must not be empty",
        Always,
        NotBlank,
    );
    add(
        "max_concurrency",
        "max_concurrency",
        "validation.concurrency",
        "must be a positive integer within the runtime limit",
        Always,
        RuleKind::Integer {
            minimum: 1,
            maximum: tokio::sync::Semaphore::MAX_PERMITS as u64,
        },
    );
    add(
        "weight",
        "weight",
        "validation.weight",
        "must be finite and greater than zero",
        Always,
        Positive,
    );
    add(
        "models",
        "models",
        "validation.modelRequired",
        "must declare non-empty model mappings",
        Always,
        ModelMappings,
    );
    add(
        "model_capabilities",
        "models",
        "validation.capabilityMapping",
        "must refer to mapped models or a wildcard",
        Always,
        CapabilityMappings,
    );
    add(
        "headers",
        "headers",
        "validation.headerInvalid",
        "contains an invalid or reserved header name",
        Always,
        Headers,
    );
    add(
        "headers_from_env",
        "headers_from_env",
        "validation.headerInvalid",
        "contains an invalid/reserved header name or an empty environment variable",
        Always,
        EnvironmentHeaders,
    );
    add(
        "provider.kv_events",
        "kv_endpoint",
        "validation.kvProvider",
        "requires provider.type: vllm",
        Always,
        KvProvider,
    );
    for (path, field) in [
        ("provider.version_path", "version_path"),
        ("provider.metrics_path", "metrics_path"),
        ("provider.tokenize_path", "tokenize_path"),
    ] {
        add(
            path,
            field,
            "validation.pathSlash",
            "must start with '/' and stay on the upstream origin without query or fragment",
            Vllm,
            ProviderPath,
        );
    }
    for (path, field, minimum, maximum, error) in [
        (
            "provider.monitor_interval_ms",
            "monitor_interval_ms",
            100,
            u64::MAX,
            "validation.min100ms",
        ),
        (
            "provider.request_timeout_ms",
            "request_timeout_ms",
            1,
            u64::MAX,
            "validation.min1ms",
        ),
        (
            "provider.telemetry_stale_ms",
            "telemetry_stale_ms",
            1,
            u64::MAX,
            "validation.min1ms",
        ),
        (
            "provider.waiting_threshold",
            "waiting_threshold",
            1,
            usize::MAX as u64,
            "validation.min1",
        ),
        (
            "provider.tokenize_cache_entries",
            "tokenize_cache_entries",
            1,
            MAX_TOKENIZE_CACHE_ENTRIES as u64,
            "validation.tokenizeEntries",
        ),
    ] {
        add(
            path,
            field,
            error,
            "is outside the supported integer range",
            Vllm,
            RuleKind::Integer { minimum, maximum },
        );
    }
    add(
        "provider.telemetry_stale_ms",
        "telemetry_stale_ms",
        "validation.telemetryInterval",
        "must not be shorter than monitor_interval_ms",
        Vllm,
        AtLeastField {
            other: "provider.monitor_interval_ms",
        },
    );
    for (path, field) in [
        ("provider.kv_events.endpoint", "kv_endpoint"),
        ("provider.kv_events.replay_endpoint", "kv_replay_endpoint"),
    ] {
        add(
            path,
            field,
            "validation.tcpEndpoint",
            "must be a connectable tcp://host:port endpoint",
            KvEvents,
            TcpEndpoint,
        );
    }
    for (path, field, maximum, error) in [
        (
            "provider.kv_events.reconnect_ms",
            "kv_reconnect_ms",
            u64::MAX,
            "validation.min1ms",
        ),
        (
            "provider.kv_events.max_blocks",
            "kv_max_blocks",
            usize::MAX as u64,
            "validation.min1",
        ),
        (
            "provider.kv_events.max_directory_bytes",
            "kv_max_directory_bytes",
            usize::MAX as u64,
            "validation.min1byte",
        ),
        (
            "provider.kv_events.max_event_bytes",
            "kv_max_event_bytes",
            usize::MAX as u64,
            "validation.min1byte",
        ),
    ] {
        add(
            path,
            field,
            error,
            "must be a positive integer",
            KvEvents,
            RuleKind::Integer {
                minimum: 1,
                maximum,
            },
        );
    }
    rules
}

fn at_path<'a>(value: &'a Value, path: &str) -> &'a Value {
    path.split('.').fold(value, |value, key| &value[key])
}

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

fn valid_rule(rule: &RuleKind, value: &Value, config: &Value) -> bool {
    if value.is_null() {
        return matches!(
            rule,
            RuleKind::NotBlank | RuleKind::TcpEndpoint | RuleKind::KvProvider
        );
    }
    match rule {
        RuleKind::NotBlank => value.as_str().is_some_and(|s| !s.trim().is_empty()),
        RuleKind::HttpUrl => value
            .as_str()
            .and_then(|s| Url::parse(s).ok())
            .is_some_and(|url| {
                matches!(url.scheme(), "http" | "https") && url.host_str().is_some()
            }),
        RuleKind::UrlWithoutParts => value
            .as_str()
            .and_then(|s| Url::parse(s).ok())
            .is_some_and(|url| without_url_parts(&url)),
        RuleKind::Integer { minimum, maximum } => value
            .as_u64()
            .is_some_and(|n| (*minimum..=*maximum).contains(&n)),
        RuleKind::Positive => value.as_f64().is_some_and(|n| n.is_finite() && n > 0.0),
        RuleKind::ModelMappings => value.as_object().is_some_and(|map| {
            !map.is_empty()
                && map.iter().all(|(key, val)| {
                    !key.trim().is_empty() && val.as_str().is_some_and(|s| !s.trim().is_empty())
                })
        }),
        RuleKind::CapabilityMappings => value.as_object().is_some_and(|map| {
            map.keys().all(|key| {
                !key.trim().is_empty()
                    && (key == "*"
                        || config["models"].get(key).is_some()
                        || config["models"].get("*").is_some())
            })
        }),
        RuleKind::Headers | RuleKind::EnvironmentHeaders => value.as_object().is_some_and(|map| {
            map.iter().all(|(key, val)| {
                valid_header(key)
                    && (!matches!(rule, RuleKind::EnvironmentHeaders)
                        || val.as_str().is_some_and(|s| !s.trim().is_empty()))
            })
        }),
        RuleKind::ProviderPath => {
            let Some(path) = value.as_str().filter(|s| s.starts_with('/')) else {
                return false;
            };
            let Some(base) = config["base_url"].as_str().and_then(|s| Url::parse(s).ok()) else {
                return false;
            };
            base.join(path).is_ok_and(|url| {
                url.origin() == base.origin() && url.query().is_none() && url.fragment().is_none()
            })
        }
        RuleKind::TcpEndpoint => {
            value
                .as_str()
                .and_then(|s| Url::parse(s).ok())
                .is_some_and(|url| {
                    url.scheme() == "tcp"
                        && url.host_str().is_some_and(|host| !host.contains('*'))
                        && url.port().is_some()
                        && without_url_parts(&url)
                        && matches!(url.path(), "" | "/")
                })
        }
        RuleKind::AtLeastField { other } => value
            .as_u64()
            .zip(at_path(config, other).as_u64())
            .is_some_and(|(n, other)| n >= other),
        RuleKind::KvProvider => config["provider"]["type"] == "vllm",
    }
}

pub(super) fn validate_node(node: &NodeConfig) -> Result<()> {
    let config = serde_json::to_value(node)?;
    for rule in node_validation_rules() {
        let enabled = match rule.condition {
            RuleCondition::Always => true,
            RuleCondition::Vllm => node.provider.kind == ProviderKind::Vllm,
            RuleCondition::KvEvents => {
                node.provider.kind == ProviderKind::Vllm && node.provider.kv_events.is_some()
            }
        };
        if enabled && !valid_rule(&rule.rule, at_path(&config, rule.path), &config) {
            bail!("node {} {} {}", node.id, rule.path, rule.message);
        }
    }
    Ok(())
}
