use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use estuary::{Gateway, Settings};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    #[arg(long, env = "ESTUARY_DATABASE", default_value = "estuary.db")]
    database: PathBuf,
    #[arg(long, env = "ESTUARY_LISTEN", default_value = "0.0.0.0:8080")]
    listen: String,
    #[arg(long, env = "ESTUARY_ADMIN_LISTEN", default_value = "127.0.0.1:9090")]
    admin_listen: String,
    #[arg(long, env = "ESTUARY_LOG_JSON", default_value_t = false)]
    log_json: bool,
    #[arg(long, env = "ESTUARY_CONTROL_SYNC_INTERVAL_MS", default_value_t = 500)]
    control_sync_interval_ms: u64,
    #[arg(
        long,
        env = "ESTUARY_NODE_MUTATION_TIMEOUT_MS",
        default_value_t = 30_000
    )]
    node_mutation_timeout_ms: u64,
    #[arg(long, env = "ESTUARY_WITHDRAWAL_DELAY_MS", default_value_t = 10_000)]
    withdrawal_delay_ms: u64,
    #[arg(long, env = "ESTUARY_SHUTDOWN_GRACE_MS", default_value_t = 3_660_000)]
    shutdown_grace_ms: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut settings = Settings::default();
    settings.server.listen = cli.listen;
    settings.server.admin_listen = cli.admin_listen;
    settings.server.log_json = cli.log_json;
    settings.server.control_sync_interval_ms = cli.control_sync_interval_ms;
    settings.server.node_mutation_timeout_ms = cli.node_mutation_timeout_ms;
    settings.server.withdrawal_delay_ms = cli.withdrawal_delay_ms;
    settings.server.shutdown_grace_ms = cli.shutdown_grace_ms;
    settings.validate()?;
    init_tracing(cli.log_json);
    Gateway::build_with_database(settings, cli.database)?
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
