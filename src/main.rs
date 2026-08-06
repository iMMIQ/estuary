use std::{net::SocketAddr, path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use estuary::{
    Gateway, Settings,
    supervisor::{self, SupervisorConfig},
};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    #[arg(long, env = "ESTUARY_DATABASE", default_value = "estuary.db")]
    database: PathBuf,
    #[arg(long, env = "ESTUARY_LISTEN")]
    listen: Option<String>,
    #[arg(long, env = "ESTUARY_ADMIN_LISTEN")]
    admin_listen: Option<String>,
    #[arg(long, env = "ESTUARY_ADMIN_TOKEN")]
    admin_token: Option<String>,
    #[arg(long, env = "ESTUARY_ADMIN_FREEZE_FILE", hide = true)]
    admin_freeze_file: Option<PathBuf>,
    #[arg(long, env = "ESTUARY_LOG_JSON", default_value_t = false)]
    log_json: bool,
    #[arg(long, env = "ESTUARY_CONNECT_TIMEOUT_MS")]
    connect_timeout_ms: Option<u64>,
    #[arg(long, env = "ESTUARY_REQUEST_BODY_IDLE_TIMEOUT_MS")]
    request_body_idle_timeout_ms: Option<u64>,
    #[arg(long, env = "ESTUARY_REQUEST_BODY_TIMEOUT_MS")]
    request_body_timeout_ms: Option<u64>,
    #[arg(long, env = "ESTUARY_UPSTREAM_HEADER_TIMEOUT_MS")]
    upstream_header_timeout_ms: Option<u64>,
    #[arg(long, env = "ESTUARY_STREAM_IDLE_TIMEOUT_MS")]
    stream_idle_timeout_ms: Option<u64>,
    #[arg(long, env = "ESTUARY_UPSTREAM_BODY_TIMEOUT_MS")]
    upstream_body_timeout_ms: Option<u64>,
    #[arg(long, env = "ESTUARY_DOWNSTREAM_STALL_TIMEOUT_MS")]
    downstream_stall_timeout_ms: Option<u64>,
    #[arg(long, env = "ESTUARY_CONTROL_SYNC_INTERVAL_MS")]
    control_sync_interval_ms: Option<u64>,
    #[arg(long, env = "ESTUARY_NODE_MUTATION_TIMEOUT_MS")]
    node_mutation_timeout_ms: Option<u64>,
    #[arg(long, env = "ESTUARY_WITHDRAWAL_DELAY_MS")]
    withdrawal_delay_ms: Option<u64>,
    #[arg(long, env = "ESTUARY_SHUTDOWN_GRACE_MS")]
    shutdown_grace_ms: Option<u64>,
    #[arg(long, env = "ESTUARY_MAX_REQUEST_BODY_BYTES")]
    max_request_body_bytes: Option<usize>,
    #[arg(long, env = "ESTUARY_MAX_CONNECTIONS")]
    max_connections: Option<usize>,
    #[arg(long, env = "ESTUARY_MAX_ADMIN_CONNECTIONS")]
    max_admin_connections: Option<usize>,
    #[arg(long, env = "ESTUARY_MAX_NON_STREAMING_RESPONSE_BYTES")]
    max_non_streaming_response_bytes: Option<usize>,
    #[arg(long, env = "ESTUARY_MAX_BUFFERED_RESPONSE_BYTES")]
    max_buffered_response_bytes: Option<usize>,
    #[arg(long, env = "ESTUARY_EXPOSE_NODE_HEADER")]
    expose_node_header: Option<bool>,
    #[arg(long, env = "ESTUARY_QUEUE_MAX_REQUESTS")]
    queue_max_requests: Option<usize>,
    #[arg(long, env = "ESTUARY_QUEUE_MAX_BYTES")]
    queue_max_bytes: Option<usize>,
    #[arg(long, env = "ESTUARY_LOAD_WEIGHT")]
    load_weight: Option<f64>,
    #[arg(long, env = "ESTUARY_LATENCY_WEIGHT")]
    latency_weight: Option<f64>,
    #[arg(long, env = "ESTUARY_ERROR_WEIGHT")]
    error_weight: Option<f64>,
    #[arg(long, env = "ESTUARY_TARGET_LATENCY_MS")]
    target_latency_ms: Option<f64>,
    #[arg(long, env = "ESTUARY_PREFIX_ENABLED")]
    prefix_enabled: Option<bool>,
    #[arg(long, env = "ESTUARY_PREFIX_CACHE_THRESHOLD")]
    prefix_cache_threshold: Option<f64>,
    #[arg(long, env = "ESTUARY_PREFIX_BALANCE_ABS_THRESHOLD")]
    prefix_balance_abs_threshold: Option<usize>,
    #[arg(long, env = "ESTUARY_PREFIX_BALANCE_REL_THRESHOLD")]
    prefix_balance_rel_threshold: Option<f64>,
    #[arg(long, env = "ESTUARY_PREFIX_MAX_REQUEST_CHARS")]
    prefix_max_request_chars: Option<usize>,
    #[arg(long, env = "ESTUARY_PREFIX_MAX_TREES")]
    prefix_max_trees: Option<usize>,
    #[arg(long, env = "ESTUARY_PREFIX_MAX_DIRECTORY_CHARS")]
    prefix_max_directory_chars: Option<usize>,
    #[arg(long, env = "ESTUARY_PREFIX_MAX_TREE_CHARS_PER_NODE")]
    prefix_max_tree_chars_per_node: Option<usize>,
    #[arg(long, env = "ESTUARY_HEALTH_INTERVAL_MS")]
    health_interval_ms: Option<u64>,
    #[arg(long, env = "ESTUARY_HEALTH_TIMEOUT_MS")]
    health_timeout_ms: Option<u64>,
    #[arg(long, env = "ESTUARY_HEALTH_UNHEALTHY_THRESHOLD")]
    health_unhealthy_threshold: Option<u32>,
    #[arg(long, env = "ESTUARY_HEALTH_HEALTHY_THRESHOLD")]
    health_healthy_threshold: Option<u32>,
    #[arg(long, env = "ESTUARY_HEALTH_PASSIVE_FAILURE_THRESHOLD")]
    health_passive_failure_threshold: Option<u32>,
    #[arg(long, env = "ESTUARY_HEALTH_ROUTE_WHILE_STARTING")]
    health_route_while_starting: Option<bool>,
    #[arg(long, env = "ESTUARY_HEALTH_JITTER_PERCENT")]
    health_jitter_percent: Option<u8>,
    #[arg(long, env = "ESTUARY_CIRCUIT_FAILURE_THRESHOLD")]
    circuit_failure_threshold: Option<u32>,
    #[arg(long, env = "ESTUARY_CIRCUIT_OPEN_MS")]
    circuit_open_ms: Option<u64>,
    #[arg(long, env = "ESTUARY_CIRCUIT_HALF_OPEN_MAX_REQUESTS")]
    circuit_half_open_max_requests: Option<usize>,
    #[arg(long, env = "ESTUARY_CIRCUIT_HALF_OPEN_SUCCESS_THRESHOLD")]
    circuit_half_open_success_threshold: Option<u32>,
    #[arg(long, env = "ESTUARY_RETRY_MAX_ATTEMPTS")]
    retry_max_attempts: Option<usize>,
    #[arg(long, env = "ESTUARY_RETRY_STATUSES", value_delimiter = ',')]
    retry_statuses: Option<Vec<u16>>,
    #[command(subcommand)]
    command: Option<CommandMode>,
}

#[derive(Debug, Subcommand)]
enum CommandMode {
    /// Run the built-in A/B worker supervisor.
    Supervisor(SupervisorArgs),
    /// Atomically roll a staged binary through both workers.
    Rollout(RolloutArgs),
    /// Show the local supervisor and worker state.
    Status(StatusArgs),
    #[command(hide = true)]
    Worker(WorkerArgs),
}

#[derive(Clone, Debug, Args)]
struct SupervisorPaths {
    #[arg(
        long,
        env = "ESTUARY_RELEASE_ROOT",
        default_value = "/opt/estuary/releases"
    )]
    release_root: PathBuf,
    #[arg(long, env = "ESTUARY_STATE_ROOT", default_value = "/opt/estuary/state")]
    state_root: PathBuf,
    #[arg(
        long,
        env = "ESTUARY_RUNTIME_DIR",
        default_value = "/var/lib/estuary/run"
    )]
    runtime_dir: PathBuf,
}

#[derive(Debug, Args)]
struct SupervisorArgs {
    #[command(flatten)]
    paths: SupervisorPaths,
    #[arg(
        long,
        env = "ESTUARY_SLOT_B_ADMIN_LISTEN",
        default_value = "127.0.0.1:19092"
    )]
    slot_b_admin_listen: SocketAddr,
    #[arg(long, env = "ESTUARY_START_TIMEOUT_SECONDS", default_value_t = 180)]
    start_timeout_seconds: u64,
    #[arg(long, env = "ESTUARY_DRAIN_TIMEOUT_SECONDS", default_value_t = 3700)]
    drain_timeout_seconds: u64,
}

#[derive(Debug, Args)]
struct RolloutArgs {
    /// New Estuary executable to stage and deploy.
    binary: PathBuf,
    #[command(flatten)]
    paths: SupervisorPaths,
}

#[derive(Debug, Args)]
struct StatusArgs {
    #[arg(
        long,
        env = "ESTUARY_RUNTIME_DIR",
        default_value = "/var/lib/estuary/run"
    )]
    runtime_dir: PathBuf,
}

#[derive(Debug, Args)]
struct WorkerArgs {
    #[arg(long)]
    slot: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut settings = Settings::default();
    apply_cli(&mut settings, &cli);
    init_tracing(settings.server.log_json);
    match &cli.command {
        None => {
            settings.validate()?;
            Gateway::build_with_database(settings, &cli.database)?
                .run()
                .await
        }
        Some(CommandMode::Worker(worker)) => {
            settings.validate()?;
            run_worker(settings, &cli.database, worker).await
        }
        Some(CommandMode::Supervisor(arguments)) => {
            settings.validate()?;
            let slot_a_admin = settings
                .server
                .admin_listen
                .parse()
                .context("invalid slot A admin listener")?;
            supervisor::run(SupervisorConfig {
                settings,
                database: cli.database.clone(),
                release_root: arguments.paths.release_root.clone(),
                state_root: arguments.paths.state_root.clone(),
                runtime_dir: arguments.paths.runtime_dir.clone(),
                slot_a_admin,
                slot_b_admin: arguments.slot_b_admin_listen,
                start_timeout: Duration::from_secs(arguments.start_timeout_seconds),
                drain_timeout: Duration::from_secs(arguments.drain_timeout_seconds),
            })
            .await
        }
        Some(CommandMode::Rollout(arguments)) => {
            let message = supervisor::request_rollout(
                &arguments.paths.release_root,
                &arguments.paths.runtime_dir.join("supervisor.sock"),
                &arguments.binary,
            )
            .await?;
            println!("{message}");
            Ok(())
        }
        Some(CommandMode::Status(arguments)) => {
            println!(
                "{}",
                supervisor::request_status(&arguments.runtime_dir.join("supervisor.sock")).await?
            );
            Ok(())
        }
    }
}

async fn run_worker(settings: Settings, database: &PathBuf, worker: &WorkerArgs) -> Result<()> {
    if !matches!(worker.slot.as_str(), "a" | "b") {
        anyhow::bail!("worker slot must be a or b");
    }
    let mut inherited = listenfd::ListenFd::from_env();
    let listener = inherited
        .take_tcp_listener(0)
        .context("failed to inspect inherited public listener")?
        .context("worker requires an inherited public listener")?;
    tracing::info!(slot = worker.slot, "starting supervised worker");
    Gateway::build_with_database_paused(settings, database)?
        .run_with_public_listener(listener)
        .await
}

macro_rules! apply {
    ($target:expr, $value:expr) => {
        if let Some(value) = $value.as_ref() {
            $target = value.clone();
        }
    };
}

#[allow(clippy::too_many_lines)]
fn apply_cli(settings: &mut Settings, cli: &Cli) {
    apply!(settings.server.listen, cli.listen);
    apply!(settings.server.admin_listen, cli.admin_listen);
    settings.server.admin_token.clone_from(&cli.admin_token);
    settings
        .server
        .admin_freeze_file
        .clone_from(&cli.admin_freeze_file);
    settings.server.log_json = cli.log_json;
    apply!(settings.server.connect_timeout_ms, cli.connect_timeout_ms);
    apply!(
        settings.server.request_body_idle_timeout_ms,
        cli.request_body_idle_timeout_ms
    );
    apply!(
        settings.server.request_body_timeout_ms,
        cli.request_body_timeout_ms
    );
    apply!(
        settings.server.upstream_header_timeout_ms,
        cli.upstream_header_timeout_ms
    );
    apply!(
        settings.server.stream_idle_timeout_ms,
        cli.stream_idle_timeout_ms
    );
    apply!(
        settings.server.upstream_body_timeout_ms,
        cli.upstream_body_timeout_ms
    );
    apply!(
        settings.server.downstream_stall_timeout_ms,
        cli.downstream_stall_timeout_ms
    );
    apply!(
        settings.server.control_sync_interval_ms,
        cli.control_sync_interval_ms
    );
    apply!(
        settings.server.node_mutation_timeout_ms,
        cli.node_mutation_timeout_ms
    );
    apply!(settings.server.withdrawal_delay_ms, cli.withdrawal_delay_ms);
    apply!(settings.server.shutdown_grace_ms, cli.shutdown_grace_ms);
    apply!(
        settings.server.max_request_body_bytes,
        cli.max_request_body_bytes
    );
    apply!(settings.server.max_connections, cli.max_connections);
    apply!(
        settings.server.max_admin_connections,
        cli.max_admin_connections
    );
    apply!(
        settings.server.max_non_streaming_response_bytes,
        cli.max_non_streaming_response_bytes
    );
    apply!(
        settings.server.max_buffered_response_bytes,
        cli.max_buffered_response_bytes
    );
    apply!(settings.server.expose_node_header, cli.expose_node_header);
    apply!(settings.routing.queue_max_requests, cli.queue_max_requests);
    apply!(settings.routing.queue_max_bytes, cli.queue_max_bytes);
    apply!(settings.routing.load_weight, cli.load_weight);
    apply!(settings.routing.latency_weight, cli.latency_weight);
    apply!(settings.routing.error_weight, cli.error_weight);
    apply!(settings.routing.target_latency_ms, cli.target_latency_ms);
    apply!(settings.routing.prefix.enabled, cli.prefix_enabled);
    apply!(
        settings.routing.prefix.cache_threshold,
        cli.prefix_cache_threshold
    );
    apply!(
        settings.routing.prefix.balance_abs_threshold,
        cli.prefix_balance_abs_threshold
    );
    apply!(
        settings.routing.prefix.balance_rel_threshold,
        cli.prefix_balance_rel_threshold
    );
    apply!(
        settings.routing.prefix.max_request_chars,
        cli.prefix_max_request_chars
    );
    apply!(settings.routing.prefix.max_trees, cli.prefix_max_trees);
    apply!(
        settings.routing.prefix.max_directory_chars,
        cli.prefix_max_directory_chars
    );
    apply!(
        settings.routing.prefix.max_tree_chars_per_node,
        cli.prefix_max_tree_chars_per_node
    );
    apply!(settings.health.interval_ms, cli.health_interval_ms);
    apply!(settings.health.timeout_ms, cli.health_timeout_ms);
    apply!(
        settings.health.unhealthy_threshold,
        cli.health_unhealthy_threshold
    );
    apply!(
        settings.health.healthy_threshold,
        cli.health_healthy_threshold
    );
    apply!(
        settings.health.passive_failure_threshold,
        cli.health_passive_failure_threshold
    );
    apply!(
        settings.health.route_while_starting,
        cli.health_route_while_starting
    );
    apply!(settings.health.jitter_percent, cli.health_jitter_percent);
    apply!(
        settings.circuit_breaker.failure_threshold,
        cli.circuit_failure_threshold
    );
    apply!(settings.circuit_breaker.open_ms, cli.circuit_open_ms);
    apply!(
        settings.circuit_breaker.half_open_max_requests,
        cli.circuit_half_open_max_requests
    );
    apply!(
        settings.circuit_breaker.half_open_success_threshold,
        cli.circuit_half_open_success_threshold
    );
    apply!(settings.retry.max_attempts, cli.retry_max_attempts);
    apply!(settings.retry.statuses, cli.retry_statuses);
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
    fn cli_overrides_global_runtime_policies() {
        let cli = Cli::try_parse_from([
            "estuary",
            "--queue-max-requests",
            "42",
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
        let mut settings = Settings::default();
        apply_cli(&mut settings, &cli);
        assert_eq!(settings.routing.queue_max_requests, 42);
        assert_eq!(settings.retry.max_attempts, 2);
        assert_eq!(settings.retry.statuses, vec![429, 503]);
        assert!(!settings.routing.prefix.enabled);
        assert_eq!(settings.health.interval_ms, 2500);
        assert_eq!(settings.server.max_buffered_response_bytes, 134_217_728);
    }
}
