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
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut settings = Settings::default();
    settings.server.listen = cli.listen;
    settings.server.admin_listen = cli.admin_listen;
    settings.server.log_json = cli.log_json;
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
