use std::{collections::HashSet, net::SocketAddr};

use anyhow::{Context, Result, ensure};
use tokio::sync::Semaphore;

use super::{Settings, validate_node_config};

impl Settings {
    pub fn validate(&self) -> Result<()> {
        self.validate_session_log()?;
        self.validate_server()?;
        self.validate_routing()?;
        self.validate_policies()?;

        let mut ids = HashSet::new();
        for node in &self.nodes {
            ensure!(
                ids.insert(node.id.as_str()),
                "duplicate node id: {}",
                node.id
            );
            validate_node_config(node)?;
        }
        Ok(())
    }

    fn validate_session_log(&self) -> Result<()> {
        let log = &self.session_log;
        ensure!(
            !log.database
                .as_ref()
                .is_some_and(|path| path.as_os_str().is_empty())
                && (2..=65_536).contains(&log.queue_capacity)
                && (1..=16 * 1024 * 1024).contains(&log.max_payload_bytes)
                && (log.max_payload_bytes..=1024 * 1024 * 1024).contains(&log.max_content_bytes)
                && log.retention_days > 0
                && (1..=log.retention_days).contains(&log.content_retention_days)
                && (1..=60_000).contains(&log.flush_interval_ms),
            "invalid session_log path, capture limits, retention or flush interval"
        );
        Ok(())
    }

    fn validate_server(&self) -> Result<()> {
        let srv = &self.server;
        let public_addr: SocketAddr = srv
            .listen
            .parse()
            .with_context(|| format!("invalid server.listen: {}", srv.listen))?;
        let admin_addr: SocketAddr = srv
            .admin_listen
            .parse()
            .with_context(|| format!("invalid server.admin_listen: {}", srv.admin_listen))?;
        ensure!(
            public_addr.port() != admin_addr.port(),
            "server.listen and server.admin_listen must use different ports"
        );
        ensure!(
            admin_addr.ip().is_loopback() || srv.admin_token.is_some(),
            "a non-loopback server.admin_listen requires server.admin_token"
        );
        ensure!(
            !srv.admin_token
                .as_deref()
                .is_some_and(|token| token.trim().is_empty()),
            "server.admin_token must not be empty"
        );
        ensure!(
            [
                srv.connect_timeout_ms,
                srv.request_body_idle_timeout_ms,
                srv.request_body_timeout_ms,
                srv.upstream_header_timeout_ms,
                srv.stream_idle_timeout_ms,
                srv.upstream_body_timeout_ms,
                srv.downstream_stall_timeout_ms,
                srv.control_sync_interval_ms,
                srv.node_mutation_timeout_ms,
                srv.withdrawal_delay_ms,
                srv.shutdown_grace_ms,
            ]
            .into_iter()
            .all(|ms| ms > 0),
            "server timeout values must be greater than zero"
        );
        ensure!(
            srv.max_request_body_bytes > 0,
            "server.max_request_body_bytes must be greater than zero"
        );
        ensure!(
            (1..=Semaphore::MAX_PERMITS).contains(&srv.max_connections)
                && (1..=Semaphore::MAX_PERMITS).contains(&srv.max_admin_connections),
            "server connection limits must be within the runtime semaphore limit"
        );
        ensure!(
            srv.max_non_streaming_response_bytes > 0
                && u32::try_from(srv.max_non_streaming_response_bytes).is_ok(),
            "server.max_non_streaming_response_bytes must be between 1 and 4 GiB"
        );
        ensure!(
            srv.max_buffered_response_bytes >= srv.max_non_streaming_response_bytes
                && srv.max_buffered_response_bytes <= Semaphore::MAX_PERMITS,
            "server.max_buffered_response_bytes must cover one non-streaming response within semaphore limits"
        );
        ensure!(
            srv.public_api_keys.iter().all(|key| !key.trim().is_empty()),
            "server.public_api_keys entries must not be empty"
        );
        ensure!(
            srv.public_rpm_limit != Some(0),
            "server.public_rpm_limit must be greater than zero"
        );
        ensure!(
            (1..=4096).contains(&srv.observability_max_scopes),
            "server.observability_max_scopes must be between 1 and 4096"
        );
        Ok(())
    }

    fn validate_routing(&self) -> Result<()> {
        let routing = &self.routing;
        ensure!(
            routing.queue_max_requests > 0
                && routing.queue_max_requests <= Semaphore::MAX_PERMITS
                && routing.queue_max_bytes >= 1024
                && u32::try_from(routing.queue_max_bytes.div_ceil(1024)).is_ok()
                && routing.queue_max_bytes >= self.server.max_request_body_bytes,
            "routing queue limits are invalid or smaller than max_request_body_bytes"
        );
        ensure!(
            routing.prefix.max_trees > 0 && routing.prefix.max_directory_chars > 0,
            "routing prefix directory limits must be greater than zero"
        );
        for (name, value) in [
            ("load_weight", routing.load_weight),
            ("latency_weight", routing.latency_weight),
            ("error_weight", routing.error_weight),
            ("prefill_weight", routing.prefill_weight),
            ("decode_weight", routing.decode_weight),
        ] {
            ensure!(
                value.is_finite() && value >= 0.0,
                "routing.{name} must be finite and non-negative"
            );
        }
        ensure!(
            routing.target_latency_ms.is_finite()
                && routing.target_latency_ms > 0.0
                && routing.request_stats_stale_ms > 0
                && routing.prefill_token_scale > 0
                && routing.decode_token_scale > 0,
            "routing latency, staleness, and token scale settings must be positive"
        );
        if routing.prefix.enabled {
            ensure!(
                routing.prefix.approximate_half_life_ms > 0
                    && routing.prefix.cache_threshold.is_finite()
                    && (0.0..=1.0).contains(&routing.prefix.cache_threshold)
                    && routing.prefix.balance_rel_threshold.is_finite()
                    && routing.prefix.balance_rel_threshold > 1.0
                    && routing.prefix.max_request_chars > 0
                    && routing.prefix.max_tree_chars_per_node > 0,
                "routing.prefix settings are out of range"
            );
        }
        Ok(())
    }

    fn validate_policies(&self) -> Result<()> {
        ensure!(
            self.health.interval_ms > 0
                && self.health.timeout_ms > 0
                && self.health.jitter_percent <= 50
                && self.health.healthy_threshold > 0
                && self.health.unhealthy_threshold > 0
                && self.health.passive_failure_threshold > 0,
            "invalid health check settings"
        );
        let cb = &self.circuit_breaker;
        ensure!(
            cb.failure_threshold > 0
                && cb.open_ms > 0
                && cb.half_open_max_requests > 0
                && cb.half_open_max_requests <= Semaphore::MAX_PERMITS
                && cb.half_open_success_threshold > 0,
            "invalid circuit_breaker settings"
        );
        ensure!(
            (1..=3).contains(&self.retry.max_attempts),
            "retry.max_attempts must be between 1 and 3"
        );
        for &status in &self.retry.statuses {
            ensure!(
                matches!(status, 408 | 409 | 425 | 429 | 500..=599),
                "retry status {status} is not a supported transient HTTP status"
            );
        }
        Ok(())
    }
}
