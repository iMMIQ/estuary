use super::*;

#[test]
fn default_workload_settings_preserve_legacy_worker_payloads() {
    let mut settings = Settings::default();
    let value = serde_json::to_value(&settings).unwrap();
    for key in [
        "prefill_weight",
        "decode_weight",
        "prefill_token_scale",
        "decode_token_scale",
    ] {
        assert!(value["routing"].get(key).is_none());
    }
    assert!(
        value["routing"]["prefix"]
            .get("approximate_half_life_ms")
            .is_none()
    );
    let restored: Settings = serde_json::from_value(value).unwrap();
    assert_eq!(restored.routing.prefill_token_scale, 4_096);
    settings.routing.prefill_weight = 0.0;
    settings.routing.prefix.approximate_half_life_ms = 60_000;
    let value = serde_json::to_value(&settings).unwrap();
    assert_eq!(value["routing"]["prefill_weight"], 0.0);
    assert_eq!(
        value["routing"]["prefix"]["approximate_half_life_ms"],
        60_000
    );
}

#[test]
fn rejects_invalid_workload_scoring_parameters() {
    for routing in [
        RoutingConfig {
            prefill_weight: f64::NAN,
            ..RoutingConfig::default()
        },
        RoutingConfig {
            decode_weight: -1.0,
            ..RoutingConfig::default()
        },
        RoutingConfig {
            prefill_token_scale: 0,
            ..RoutingConfig::default()
        },
        RoutingConfig {
            decode_token_scale: 0,
            ..RoutingConfig::default()
        },
        RoutingConfig {
            prefix: PrefixConfig {
                approximate_half_life_ms: 0,
                ..PrefixConfig::default()
            },
            ..RoutingConfig::default()
        },
    ] {
        assert!(
            Settings {
                routing,
                ..Settings::default()
            }
            .validate()
            .is_err()
        );
    }
    assert!(
        Settings {
            routing: RoutingConfig {
                prefill_weight: 0.0,
                decode_weight: 0.0,
                ..RoutingConfig::default()
            },
            ..Settings::default()
        }
        .validate()
        .is_ok()
    );
}

#[test]
fn rejects_duplicate_nodes() {
    let node = NodeConfig {
        id: "same".to_owned(),
        base_url: "http://localhost:8000/v1".to_owned(),
        models: HashMap::from([("model".to_owned(), "model".to_owned())]),
        ..NodeConfig::default()
    };
    let settings = Settings {
        nodes: vec![node.clone(), node],
        ..Settings::default()
    };
    assert!(settings.validate().is_err());
}

#[test]
fn remote_admin_listener_requires_an_authentication_token() {
    let mut settings = Settings::default();
    settings.server.admin_listen = "0.0.0.0:9090".to_owned();
    assert!(settings.validate().is_err());
    settings.server.admin_token = Some("secret".to_owned());
    assert!(settings.validate().is_ok());
}

#[test]
fn rejects_vllm_monitor_intervals_below_the_safe_floor() {
    let mut node = NodeConfig {
        id: "node".to_owned(),
        base_url: "http://localhost:8000/v1".to_owned(),
        models: HashMap::from([("model".to_owned(), "model".to_owned())]),
        provider: ProviderConfig {
            kind: ProviderKind::Vllm,
            monitor_interval_ms: 99,
            ..ProviderConfig::default()
        },
        ..NodeConfig::default()
    };
    assert!(validate_node_config(&node).is_err());
    node.provider.monitor_interval_ms = 100;
    assert!(validate_node_config(&node).is_ok());
}

#[test]
fn rejects_zero_response_body_timeouts() {
    let node = NodeConfig {
        id: "node".to_owned(),
        base_url: "http://localhost:8000/v1".to_owned(),
        models: HashMap::from([("model".to_owned(), "model".to_owned())]),
        ..NodeConfig::default()
    };
    let settings = Settings {
        nodes: vec![node],
        ..Settings::default()
    };

    let mut zero_body_timeout = settings.clone();
    zero_body_timeout.server.upstream_body_timeout_ms = 0;
    assert!(zero_body_timeout.validate().is_err());

    let mut zero_stall_timeout = settings;
    zero_stall_timeout.server.downstream_stall_timeout_ms = 0;
    assert!(zero_stall_timeout.validate().is_err());

    let mut zero_response_limit = zero_stall_timeout;
    zero_response_limit.server.downstream_stall_timeout_ms = 1;
    zero_response_limit.server.max_non_streaming_response_bytes = 0;
    assert!(zero_response_limit.validate().is_err());

    let mut undersized_global_limit = Settings::default();
    undersized_global_limit.server.max_buffered_response_bytes = undersized_global_limit
        .server
        .max_non_streaming_response_bytes
        - 1;
    assert!(undersized_global_limit.validate().is_err());
}

#[test]
fn rejects_zero_long_running_resource_limits() {
    let mut settings = Settings::default();
    settings.server.max_connections = 0;
    assert!(settings.validate().is_err());

    let mut settings = Settings::default();
    settings.routing.prefix.max_trees = 0;
    assert!(settings.validate().is_err());

    let mut settings = Settings::default();
    settings.routing.prefix.max_directory_chars = 0;
    assert!(settings.validate().is_err());

    let mut settings = Settings::default();
    settings.routing.request_stats_stale_ms = 0;
    assert!(settings.validate().is_err());
}

#[test]
fn rejects_zero_circuit_breaker_limits() {
    let node = NodeConfig {
        id: "node".to_owned(),
        base_url: "http://localhost:8000/v1".to_owned(),
        models: HashMap::from([("model".to_owned(), "model".to_owned())]),
        ..NodeConfig::default()
    };
    let mut settings = Settings {
        nodes: vec![node],
        ..Settings::default()
    };
    settings.circuit_breaker.failure_threshold = 0;
    assert!(settings.validate().is_err());
}

#[test]
fn validates_vllm_provider_endpoints() {
    let mut node = NodeConfig {
        id: "vllm".to_owned(),
        base_url: "http://localhost:8000/v1".to_owned(),
        models: HashMap::from([("model".to_owned(), "model".to_owned())]),
        ..NodeConfig::default()
    };
    node.provider.kind = ProviderKind::Vllm;
    node.provider.kv_events = Some(VllmKvEventsConfig::default());
    let settings = Settings {
        nodes: vec![node.clone()],
        ..Settings::default()
    };
    settings.validate().unwrap();

    node.provider.kv_events.as_mut().unwrap().endpoint = "tcp://*:5557".to_owned();
    let invalid = Settings {
        nodes: vec![node],
        ..Settings::default()
    };
    assert!(invalid.validate().is_err());
}

#[test]
fn defaults_waiting_watermark_for_existing_vllm_json() {
    let provider: ProviderConfig = serde_json::from_value(serde_json::json!({
        "type": "vllm"
    }))
    .unwrap();
    assert_eq!(provider.waiting_threshold, 8);
    assert_eq!(provider.anthropic_protocol, AnthropicProtocol::Auto);
}

#[test]
fn rejects_zero_vllm_waiting_watermark() {
    let mut node = NodeConfig {
        id: "vllm".to_owned(),
        base_url: "http://localhost:8000/v1".to_owned(),
        models: HashMap::from([("model".to_owned(), "model".to_owned())]),
        ..NodeConfig::default()
    };
    node.provider.kind = ProviderKind::Vllm;
    node.provider.waiting_threshold = 0;
    let settings = Settings {
        nodes: vec![node],
        ..Settings::default()
    };
    assert!(settings.validate().is_err());
}

#[test]
fn rejects_excessive_vllm_tokenization_cache_capacity() {
    let mut node = NodeConfig {
        id: "vllm".to_owned(),
        base_url: "http://localhost:8000/v1".to_owned(),
        models: HashMap::from([("model".to_owned(), "model".to_owned())]),
        ..NodeConfig::default()
    };
    node.provider.kind = ProviderKind::Vllm;
    node.provider.tokenize_cache_entries = MAX_TOKENIZE_CACHE_ENTRIES + 1;
    let settings = Settings {
        nodes: vec![node],
        ..Settings::default()
    };
    assert!(settings.validate().is_err());
}

#[test]
fn rejects_kv_events_on_generic_provider() {
    let node = NodeConfig {
        id: "generic".to_owned(),
        base_url: "http://localhost:8000/v1".to_owned(),
        models: HashMap::from([("model".to_owned(), "model".to_owned())]),
        provider: ProviderConfig {
            kv_events: Some(VllmKvEventsConfig::default()),
            ..ProviderConfig::default()
        },
        ..NodeConfig::default()
    };
    let settings = Settings {
        nodes: vec![node],
        ..Settings::default()
    };
    assert!(settings.validate().is_err());
}
