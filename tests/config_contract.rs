use estuary::{
    Settings,
    config::{NodeConfig, validate_node_config},
};
use serde_json::Value;

#[test]
fn shared_node_configuration_cases() {
    let fixtures: Value =
        serde_json::from_str(include_str!("fixtures/node-config-contract.json")).unwrap();
    for case in fixtures["cases"].as_array().unwrap() {
        let mut config = fixtures["base"].clone();
        for (path, value) in case["changes"].as_object().unwrap() {
            let mut target = &mut config;
            let mut keys = path.split('.').peekable();
            while let Some(key) = keys.next() {
                if keys.peek().is_none() {
                    target[key] = value.clone();
                } else {
                    target = &mut target[key];
                }
            }
        }
        let valid = match serde_json::from_value::<NodeConfig>(config) {
            Ok(node) => {
                let valid = validate_node_config(&node).is_ok();
                assert_eq!(
                    valid,
                    Settings {
                        nodes: vec![node],
                        ..Settings::default()
                    }
                    .validate()
                    .is_ok()
                );
                valid
            }
            Err(_) => false,
        };
        assert_eq!(valid, case["valid"].as_bool().unwrap(), "{}", case["name"]);
    }
}

#[test]
fn rejects_non_finite_weights_constructed_without_json_deserialization() {
    for weight in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let node = NodeConfig {
            id: "node".to_owned(),
            base_url: "http://localhost:8000/v1".to_owned(),
            models: [("chat".to_owned(), "model".to_owned())].into(),
            weight,
            ..NodeConfig::default()
        };
        assert!(validate_node_config(&node).is_err());
    }
}
