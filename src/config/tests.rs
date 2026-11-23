use std::collections::HashMap;

use super::*;

fn test_node(id: &str) -> NodeConfig {
    NodeConfig {
        id: id.to_owned(),
        base_url: "http://localhost:8000/v1".to_owned(),
        models: HashMap::from([("model".to_owned(), "model".to_owned())]),
        ..NodeConfig::default()
    }
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
    let settings = Settings {
        nodes: vec![test_node("same"), test_node("same")],
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
fn rejects_zero_response_body_timeouts() {
    let settings = Settings {
        nodes: vec![test_node("node")],
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
    let mut settings = Settings {
        nodes: vec![test_node("node")],
        ..Settings::default()
    };
    settings.circuit_breaker.failure_threshold = 0;
    assert!(settings.validate().is_err());
}

#[test]
fn defaults_waiting_watermark_for_existing_vllm_json() {
    let provider: ProviderConfig = serde_json::from_value(serde_json::json!({
        "type": "vllm"
    }))
    .unwrap();
    assert_eq!(provider.waiting_threshold, 8);
}

#[test]
fn retired_family_is_input_only_and_other_capability_fields_stay_strict() {
    for family in ["generic", "deepseek"] {
        let capability: ModelCapabilityConfig =
            serde_json::from_value(serde_json::json!({"family":family,"multimodal":false}))
                .unwrap();
        assert!(!capability.multimodal);
        assert_eq!(
            serde_json::to_value(capability).unwrap(),
            serde_json::json!({"multimodal":false})
        );
        let capability: ModelCapabilityConfig =
            serde_json::from_value(serde_json::json!({"family":family})).unwrap();
        assert!(capability.multimodal);
    }
    assert!(
        serde_json::from_value::<ModelCapabilityConfig>(serde_json::json!({"unknown":true}))
            .is_err()
    );
    assert!(
        serde_json::from_value::<ModelCapabilityConfig>(serde_json::json!({"multimodal":null}))
            .is_err()
    );
}

#[test]
fn provider_rejects_retired_protocol_settings_in_new_configuration() {
    for value in [
        serde_json::json!({"anthropic_protocol":"native"}),
        serde_json::json!({"flatten_codex_namespaces":true}),
    ] {
        assert!(serde_json::from_value::<ProviderConfig>(value).is_err());
    }
}
