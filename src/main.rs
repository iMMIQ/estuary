use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use estuary::{Gateway, Settings};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    #[arg(short, long, env = "ESTUARY_CONFIG", default_value = "config.yaml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let settings = Settings::load(&cli.config)?;
    init_tracing(settings.server.log_json);
    Gateway::build(settings)?.run().await
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
