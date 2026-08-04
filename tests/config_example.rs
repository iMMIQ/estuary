use estuary::Settings;

#[test]
fn example_configuration_parses_and_validates() {
    let settings = Settings::load("config.example.yaml").expect("valid example configuration");
    assert_eq!(settings.nodes.len(), 2);
    assert_ne!(settings.server.listen, settings.server.admin_listen);
}
