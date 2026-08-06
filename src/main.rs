use std::{net::SocketAddr, path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use estuary::{
    Gateway, Settings,
    supervisor::{self, SupervisorConfig},
};
use serde::Serialize;
use serde_json::Value;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    #[arg(long, env = "ESTUARY_DATABASE", default_value = "estuary.db")]
    database: PathBuf,
    #[command(flatten)]
    runtime: RuntimeOverrides,
    #[command(subcommand)]
    command: Option<CommandMode>,
}

#[derive(Debug, Default, Args, Serialize)]
struct RuntimeOverrides {
    #[command(flatten)]
    server: ServerOverrides,
    #[command(flatten)]
    routing: RoutingOverrides,
    #[command(flatten)]
    health: HealthOverrides,
    #[command(flatten)]
    circuit_breaker: CircuitBreakerOverrides,
    #[command(flatten)]
    retry: RetryOverrides,
}

#[derive(Debug, Default, Args, Serialize)]
struct ServerOverrides {
    #[arg(long, env = "ESTUARY_LISTEN")]
    listen: Option<String>,
    #[arg(long, env = "ESTUARY_ADMIN_LISTEN")]
    admin_listen: Option<String>,
    #[arg(long, env = "ESTUARY_ADMIN_TOKEN")]
    admin_token: Option<String>,
    #[arg(long, env = "ESTUARY_ADMIN_FREEZE_FILE", hide = true)]
    admin_freeze_file: Option<PathBuf>,
    #[arg(
        long,
        env = "ESTUARY_LOG_JSON",
        num_args = 0..=1,
        default_missing_value = "true"
    )]
    log_json: Option<bool>,
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
}

#[derive(Debug, Default, Args, Serialize)]
struct RoutingOverrides {
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
    #[command(flatten)]
    prefix: PrefixOverrides,
}

#[derive(Debug, Default, Args, Serialize)]
struct PrefixOverrides {
    #[arg(long = "prefix-enabled", env = "ESTUARY_PREFIX_ENABLED")]
    enabled: Option<bool>,
    #[arg(
        long = "prefix-cache-threshold",
        env = "ESTUARY_PREFIX_CACHE_THRESHOLD"
    )]
    cache_threshold: Option<f64>,
    #[arg(
        long = "prefix-balance-abs-threshold",
        env = "ESTUARY_PREFIX_BALANCE_ABS_THRESHOLD"
    )]
    balance_abs_threshold: Option<usize>,
    #[arg(
        long = "prefix-balance-rel-threshold",
        env = "ESTUARY_PREFIX_BALANCE_REL_THRESHOLD"
    )]
    balance_rel_threshold: Option<f64>,
    #[arg(
        long = "prefix-max-request-chars",
        env = "ESTUARY_PREFIX_MAX_REQUEST_CHARS"
    )]
    max_request_chars: Option<usize>,
    #[arg(long = "prefix-max-trees", env = "ESTUARY_PREFIX_MAX_TREES")]
    max_trees: Option<usize>,
    #[arg(
        long = "prefix-max-directory-chars",
        env = "ESTUARY_PREFIX_MAX_DIRECTORY_CHARS"
    )]
    max_directory_chars: Option<usize>,
    #[arg(
        long = "prefix-max-tree-chars-per-node",
        env = "ESTUARY_PREFIX_MAX_TREE_CHARS_PER_NODE"
    )]
    max_tree_chars_per_node: Option<usize>,
}

#[derive(Debug, Default, Args, Serialize)]
struct HealthOverrides {
    #[arg(long = "health-interval-ms", env = "ESTUARY_HEALTH_INTERVAL_MS")]
    interval_ms: Option<u64>,
    #[arg(long = "health-timeout-ms", env = "ESTUARY_HEALTH_TIMEOUT_MS")]
    timeout_ms: Option<u64>,
    #[arg(
        long = "health-unhealthy-threshold",
        env = "ESTUARY_HEALTH_UNHEALTHY_THRESHOLD"
    )]
    unhealthy_threshold: Option<u32>,
    #[arg(
        long = "health-healthy-threshold",
        env = "ESTUARY_HEALTH_HEALTHY_THRESHOLD"
    )]
    healthy_threshold: Option<u32>,
    #[arg(
        long = "health-passive-failure-threshold",
        env = "ESTUARY_HEALTH_PASSIVE_FAILURE_THRESHOLD"
    )]
    passive_failure_threshold: Option<u32>,
    #[arg(
        long = "health-route-while-starting",
        env = "ESTUARY_HEALTH_ROUTE_WHILE_STARTING"
    )]
    route_while_starting: Option<bool>,
    #[arg(long = "health-jitter-percent", env = "ESTUARY_HEALTH_JITTER_PERCENT")]
    jitter_percent: Option<u8>,
}

#[derive(Debug, Default, Args, Serialize)]
struct CircuitBreakerOverrides {
    #[arg(
        long = "circuit-failure-threshold",
        env = "ESTUARY_CIRCUIT_FAILURE_THRESHOLD"
    )]
    failure_threshold: Option<u32>,
    #[arg(long = "circuit-open-ms", env = "ESTUARY_CIRCUIT_OPEN_MS")]
    open_ms: Option<u64>,
    #[arg(
        long = "circuit-half-open-max-requests",
        env = "ESTUARY_CIRCUIT_HALF_OPEN_MAX_REQUESTS"
    )]
    half_open_max_requests: Option<usize>,
    #[arg(
        long = "circuit-half-open-success-threshold",
        env = "ESTUARY_CIRCUIT_HALF_OPEN_SUCCESS_THRESHOLD"
    )]
    half_open_success_threshold: Option<u32>,
}

#[derive(Debug, Default, Args, Serialize)]
struct RetryOverrides {
    #[arg(long = "retry-max-attempts", env = "ESTUARY_RETRY_MAX_ATTEMPTS")]
    max_attempts: Option<usize>,
    #[arg(
        long = "retry-statuses",
        env = "ESTUARY_RETRY_STATUSES",
        value_delimiter = ','
    )]
    statuses: Option<Vec<u16>>,
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
    let settings = load_settings(&cli)?;
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

fn load_settings(cli: &Cli) -> Result<Settings> {
    if matches!(cli.command, Some(CommandMode::Worker(_))) {
        if let Some(encoded) = std::env::var_os(supervisor::WORKER_SETTINGS_ENV) {
            return serde_json::from_slice(encoded.as_encoded_bytes())
                .context("invalid serialized worker settings");
        }
    }
    settings_from_overrides(&cli.runtime)
}

fn settings_from_overrides(overrides: &RuntimeOverrides) -> Result<Settings> {
    let mut value = serde_json::to_value(overrides).context("failed to encode CLI settings")?;
    prune_nulls(&mut value);
    serde_json::from_value(value).context("failed to merge runtime settings")
}

fn prune_nulls(value: &mut Value) {
    match value {
        Value::Object(object) => {
            object.retain(|_, value| !value.is_null());
            for value in object.values_mut() {
                prune_nulls(value);
            }
        }
        Value::Array(array) => {
            for value in array {
                prune_nulls(value);
            }
        }
        _ => {}
    }
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
        let settings = settings_from_overrides(&cli.runtime).unwrap();
        assert_eq!(settings.routing.queue_max_requests, 42);
        assert_eq!(settings.retry.max_attempts, 2);
        assert_eq!(settings.retry.statuses, vec![429, 503]);
        assert!(!settings.routing.prefix.enabled);
        assert_eq!(settings.health.interval_ms, 2500);
        assert_eq!(settings.server.max_buffered_response_bytes, 134_217_728);
    }
}
