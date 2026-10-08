use std::{collections::HashSet, net::SocketAddr};

use anyhow::{Context, Result, bail};

use super::{NodeConfig, Settings, contract};

impl Settings {
    #[allow(clippy::too_many_lines)]
    pub fn validate(&self) -> Result<()> {
        let log = &self.session_log;
        if log
            .database
            .as_ref()
            .is_some_and(|path| path.as_os_str().is_empty())
            || log.queue_capacity < 2
            || log.queue_capacity > 65_536
            || log.max_payload_bytes == 0
            || log.max_payload_bytes > 16 * 1024 * 1024
            || log.max_content_bytes < log.max_payload_bytes
            || log.max_content_bytes > 1024 * 1024 * 1024
            || log.retention_days == 0
            || log.content_retention_days == 0
            || log.content_retention_days > log.retention_days
            || log.flush_interval_ms == 0
            || log.flush_interval_ms > 60_000
        {
            bail!("invalid session_log path, capture limits, retention or flush interval");
        }
        let public_address = self
            .server
            .listen
            .parse::<SocketAddr>()
            .with_context(|| format!("invalid server.listen: {}", self.server.listen))?;
        let admin_address = self
            .server
            .admin_listen
            .parse::<SocketAddr>()
            .with_context(|| {
                format!("invalid server.admin_listen: {}", self.server.admin_listen)
            })?;
        if public_address.port() == admin_address.port() {
            bail!("server.listen and server.admin_listen must use different ports");
        }
        if !admin_address.ip().is_loopback() && self.server.admin_token.is_none() {
            bail!("a non-loopback server.admin_listen requires server.admin_token");
        }
        if self
            .server
            .admin_token
            .as_deref()
            .is_some_and(|token| token.trim().is_empty())
        {
            bail!("server.admin_token must not be empty");
        }
        if self.server.connect_timeout_ms == 0
            || self.server.request_body_idle_timeout_ms == 0
            || self.server.request_body_timeout_ms == 0
            || self.server.upstream_header_timeout_ms == 0
            || self.server.stream_idle_timeout_ms == 0
            || self.server.upstream_body_timeout_ms == 0
            || self.server.downstream_stall_timeout_ms == 0
            || self.server.control_sync_interval_ms == 0
            || self.server.node_mutation_timeout_ms == 0
            || self.server.withdrawal_delay_ms == 0
            || self.server.shutdown_grace_ms == 0
        {
            bail!("server timeout values must be greater than zero");
        }
        if self.server.max_request_body_bytes == 0 {
            bail!("server.max_request_body_bytes must be greater than zero");
        }
        if self.server.max_connections == 0
            || self.server.max_connections > tokio::sync::Semaphore::MAX_PERMITS
            || self.server.max_admin_connections == 0
            || self.server.max_admin_connections > tokio::sync::Semaphore::MAX_PERMITS
        {
            bail!("server connection limits must be within the runtime semaphore limit");
        }
        if self.server.max_non_streaming_response_bytes == 0 {
            bail!("server.max_non_streaming_response_bytes must be greater than zero");
        }
        if self.server.max_non_streaming_response_bytes > u32::MAX as usize {
            bail!("server.max_non_streaming_response_bytes exceeds the supported 4 GiB limit");
        }
        if self.server.max_buffered_response_bytes < self.server.max_non_streaming_response_bytes {
            bail!(
                "server.max_buffered_response_bytes must cover one maximum-sized non-streaming response"
            );
        }
        if self.server.max_buffered_response_bytes > tokio::sync::Semaphore::MAX_PERMITS {
            bail!("server.max_buffered_response_bytes exceeds the runtime semaphore limit");
        }

        if self.routing.queue_max_requests == 0 || self.routing.queue_max_bytes < 1024 {
            bail!("routing queue limits must be greater than zero");
        }
        if self.routing.prefix.max_trees == 0 || self.routing.prefix.max_directory_chars == 0 {
            bail!("routing prefix directory limits must be greater than zero");
        }
        if self.routing.queue_max_requests > tokio::sync::Semaphore::MAX_PERMITS {
            bail!("routing.queue_max_requests exceeds the runtime semaphore limit");
        }
        if self.routing.queue_max_bytes.div_ceil(1024) > u32::MAX as usize {
            bail!("routing.queue_max_bytes exceeds the supported 4 TiB limit");
        }
        if self.routing.queue_max_bytes < self.server.max_request_body_bytes {
            bail!("routing.queue_max_bytes must cover one maximum-sized request body");
        }
        for (name, value) in [
            ("load_weight", self.routing.load_weight),
            ("latency_weight", self.routing.latency_weight),
            ("error_weight", self.routing.error_weight),
            ("prefill_weight", self.routing.prefill_weight),
            ("decode_weight", self.routing.decode_weight),
        ] {
            if !value.is_finite() || value < 0.0 {
                bail!("routing.{name} must be finite and non-negative");
            }
        }
        if !self.routing.target_latency_ms.is_finite() || self.routing.target_latency_ms <= 0.0 {
            bail!("routing.target_latency_ms must be finite and greater than zero");
        }
        if self.routing.request_stats_stale_ms == 0 {
            bail!("routing.request_stats_stale_ms must be greater than zero");
        }
        if self.routing.prefill_token_scale == 0 || self.routing.decode_token_scale == 0 {
            bail!("routing token scales must be greater than zero");
        }
        if self.routing.prefix.enabled {
            if self.routing.prefix.approximate_half_life_ms == 0 {
                bail!("routing.prefix.approximate_half_life_ms must be greater than zero");
            }
            if !self.routing.prefix.cache_threshold.is_finite()
                || !(0.0..=1.0).contains(&self.routing.prefix.cache_threshold)
            {
                bail!("routing.prefix.cache_threshold must be between 0.0 and 1.0");
            }
            if !self.routing.prefix.balance_rel_threshold.is_finite()
                || self.routing.prefix.balance_rel_threshold <= 1.0
            {
                bail!("routing.prefix.balance_rel_threshold must be greater than 1.0");
            }
            if self.routing.prefix.max_request_chars == 0 {
                bail!("routing.prefix.max_request_chars must be greater than zero");
            }
            if self.routing.prefix.max_tree_chars_per_node == 0 {
                bail!("routing.prefix.max_tree_chars_per_node must be greater than zero");
            }
        }
        if self.health.interval_ms == 0 || self.health.timeout_ms == 0 {
            bail!("health interval and timeout must be greater than zero");
        }
        if self.health.jitter_percent > 50 {
            bail!("health.jitter_percent must not exceed 50");
        }
        if self.health.healthy_threshold == 0
            || self.health.unhealthy_threshold == 0
            || self.health.passive_failure_threshold == 0
        {
            bail!("health thresholds must be greater than zero");
        }
        if self.circuit_breaker.failure_threshold == 0
            || self.circuit_breaker.open_ms == 0
            || self.circuit_breaker.half_open_max_requests == 0
            || self.circuit_breaker.half_open_success_threshold == 0
        {
            bail!("circuit_breaker values must be greater than zero");
        }
        if self.circuit_breaker.half_open_max_requests > tokio::sync::Semaphore::MAX_PERMITS {
            bail!("circuit_breaker.half_open_max_requests exceeds the runtime limit");
        }
        if !(1..=3).contains(&self.retry.max_attempts) {
            bail!("retry.max_attempts must be between 1 and 3");
        }
        for status in &self.retry.statuses {
            if !matches!(*status, 408 | 409 | 425 | 429 | 500..=599) {
                bail!("retry status {status} is not a supported transient HTTP status");
            }
        }

        let mut ids = HashSet::new();
        for node in &self.nodes {
            if !ids.insert(node.id.as_str()) {
                bail!("duplicate node id: {}", node.id);
            }
            validate_node_config(node)?;
        }
        Ok(())
    }
}

pub fn validate_node_config(node: &NodeConfig) -> Result<()> {
    contract::validate_node(node)
}
