use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use estuary::{Gateway, Settings};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(version = estuary::VERSION, about)]
struct Cli {
    #[arg(long, env = "ESTUARY_DATABASE", default_value = "estuary.db")]
    database: PathBuf,
    #[command(flatten)]
    settings: Settings,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.settings.server.log_json);
    cli.settings.validate()?;
    Gateway::build_with_database(cli.settings, &cli.database)?
        .run()
        .await
}

fn init_tracing(json: bool) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    if json {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .compact()
            .init();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_defaults_match_settings_default() {
        let cli = Cli::try_parse_from(["estuary"]).unwrap();
        let expected = Settings::default();
        assert_eq!(
            serde_json::to_value(&cli.settings).unwrap(),
            serde_json::to_value(&expected).unwrap()
        );
    }

    #[test]
    fn cli_overrides_global_runtime_policies() {
        let cli = Cli::try_parse_from([
            "estuary",
            "--log-json",
            "--session-log-database",
            "session-test.sqlite",
            "--session-log-capture-content",
            "false",
            "--session-log-queue-capacity",
            "128",
            "--queue-max-requests",
            "42",
            "--request-stats-stale-ms",
            "30000",
            "--prefill-weight",
            "0.75",
            "--decode-weight",
            "0.5",
            "--prefill-token-scale",
            "8192",
            "--decode-token-scale",
            "2048",
            "--prefix-approximate-half-life-ms",
            "120000",
            "--retry-max-attempts",
            "2",
            "--retry-statuses",
            "429,503",
            "--prefix-enabled",
            "false",
            "--health-interval-ms",
            "2500",
            "--max-buffered-response-bytes",
            "134217728",
        ])
        .unwrap();
        let settings = cli.settings;
        assert!(settings.server.log_json);
        assert_eq!(
            settings.session_log.database,
            Some(PathBuf::from("session-test.sqlite"))
        );
        assert!(!settings.session_log.capture_content);
        assert_eq!(settings.session_log.queue_capacity, 128);
        assert_eq!(settings.routing.queue_max_requests, 42);
        assert_eq!(settings.routing.request_stats_stale_ms, 30_000);
        assert!((settings.routing.prefill_weight - 0.75).abs() < f64::EPSILON);
        assert!((settings.routing.decode_weight - 0.5).abs() < f64::EPSILON);
        assert_eq!(settings.routing.prefill_token_scale, 8_192);
        assert_eq!(settings.routing.decode_token_scale, 2_048);
        assert_eq!(settings.routing.prefix.approximate_half_life_ms, 120_000);
        assert_eq!(settings.retry.max_attempts, 2);
        assert_eq!(settings.retry.statuses, vec![429, 503]);
        assert!(!settings.routing.prefix.enabled);
        assert_eq!(settings.health.interval_ms, 2500);
        assert_eq!(settings.server.max_buffered_response_bytes, 134_217_728);
    }
}
