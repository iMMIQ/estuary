use std::{
    os::fd::AsFd,
    process::{Command, Stdio},
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use command_fds::{CommandFdExt, FdMapping};
use reqwest::{Method, StatusCode};
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use super::releases::{read_release_link, validate_release};
use super::{
    PUBLIC_FD, RESTART_MAX_BACKOFF, RESTART_STABLE_UPTIME, SlotId, SlotRuntime, Supervisor,
    WORKER_CONTROL_TIMEOUT, apply_worker_environment,
};

impl Supervisor {
    pub(super) async fn start_slot(
        &self,
        slot: &mut SlotRuntime,
        require_ready: bool,
        activate: bool,
    ) -> Result<()> {
        let binary = validate_release(&self.config.release_root, &slot.release)?;
        let listener = self
            .listener
            .as_fd()
            .try_clone_to_owned()
            .context("failed to duplicate public listener for worker")?;
        let mut command = Command::new(binary);
        command
            .arg("--database")
            .arg(&self.config.database)
            .arg("worker")
            .arg("--slot")
            .arg(slot.id.name())
            .env("LISTEN_FDS", "1")
            .env_remove("LISTEN_PID")
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
        apply_worker_environment(
            &mut command,
            &self.config.settings,
            self.config.slot_admin(slot.id),
            &self.config.freeze_file(),
        )?;
        command
            .fd_mappings(vec![FdMapping {
                parent_fd: listener,
                child_fd: PUBLIC_FD,
            }])
            .context("failed to map public listener into worker")?;
        let child = command.spawn().with_context(|| {
            format!(
                "failed to start slot {} from {}",
                slot.id.name(),
                slot.release.display()
            )
        })?;
        info!(slot = slot.id.name(), pid = child.id(), release = %slot.release.display(), "worker started paused");
        slot.child = Some(child);

        if let Err(error) = self.wait_for_worker(slot, require_ready).await {
            stop_child(slot);
            return Err(error);
        }
        if activate {
            self.worker_request(slot.id, Method::PUT, "/admin/api/process/activate")
                .await
                .context("failed to activate worker")?;
        }
        if require_ready && activate {
            self.wait_for_http_ready(slot).await?;
            slot.must_be_ready = true;
        }
        info!(slot = slot.id.name(), release = %slot.release.display(), activate, "worker started");
        slot.started_at = Some(std::time::Instant::now());
        Ok(())
    }

    pub(super) async fn wait_for_worker(
        &self,
        slot: &mut SlotRuntime,
        require_ready: bool,
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + self.config.start_timeout;
        loop {
            if let Some(status) = slot
                .child
                .as_mut()
                .context("worker child is missing")?
                .try_wait()
                .context("failed to inspect worker process")?
            {
                bail!("slot {} exited before activation: {status}", slot.id.name());
            }
            if let Ok(response) = self
                .worker_request(slot.id, Method::GET, "/admin/api/process")
                .await
                && (!require_ready
                    || response
                        .get("runtime_ready")
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(false))
            {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                bail!("slot {} did not become warm before timeout", slot.id.name());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    pub(super) async fn wait_for_http_ready(&self, slot: &mut SlotRuntime) -> Result<()> {
        let deadline = tokio::time::Instant::now() + self.config.start_timeout;
        loop {
            if let Some(status) = slot
                .child
                .as_mut()
                .context("worker child is missing")?
                .try_wait()
                .context("failed to inspect activated worker")?
            {
                bail!("slot {} exited after activation: {status}", slot.id.name());
            }
            let url = format!("http://{}/health/ready", self.config.slot_admin(slot.id));
            if self
                .client
                .get(url)
                .timeout(WORKER_CONTROL_TIMEOUT)
                .send()
                .await
                .is_ok_and(|response| response.status() == StatusCode::OK)
            {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                bail!("slot {} failed readiness after activation", slot.id.name());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    pub(super) async fn worker_request(
        &self,
        slot: SlotId,
        method: Method,
        path: &str,
    ) -> Result<serde_json::Value> {
        let url = format!("http://{}{}", self.config.slot_admin(slot), path);
        let mut request = self
            .client
            .request(method, url)
            .timeout(WORKER_CONTROL_TIMEOUT);
        if let Some(token) = self.config.settings.server.admin_token.as_deref() {
            request = request.bearer_auth(token);
        }
        let response = request
            .send()
            .await
            .context("worker control request failed")?;
        let status = response.status();
        if !status.is_success() {
            bail!("worker control request returned {status}");
        }
        response
            .json()
            .await
            .context("worker returned an invalid control response")
    }

    pub(super) async fn wait_for_exit(&self, slot: &mut SlotRuntime) -> Result<()> {
        let deadline = tokio::time::Instant::now() + self.config.drain_timeout;
        loop {
            let child = slot.child.as_mut().context("worker child is missing")?;
            if let Some(status) = child
                .try_wait()
                .context("failed to wait for worker drain")?
            {
                info!(slot = slot.id.name(), %status, "worker drained");
                slot.child = None;
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "slot {} exceeded the drain deadline and was left alive",
                    slot.id.name()
                );
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    pub(super) async fn watch_slot(&self, slot_lock: Arc<Mutex<SlotRuntime>>) {
        loop {
            tokio::select! {
                () = self.shutdown.cancelled() => return,
                () = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
            let Ok(mut slot) = slot_lock.try_lock() else {
                continue;
            };
            let active = self.active_slot.load(Ordering::Acquire)
                == match slot.id {
                    SlotId::A => 0,
                    SlotId::B => 1,
                };
            let exited = match slot.child.as_mut() {
                Some(child) => match child.try_wait() {
                    Ok(Some(status)) => {
                        error!(slot = slot.id.name(), %status, "worker exited unexpectedly");
                        true
                    }
                    Ok(None) => false,
                    Err(error) => {
                        error!(slot = slot.id.name(), %error, "failed to inspect worker");
                        false
                    }
                },
                None => false,
            };
            if slot.child.is_some() && !exited {
                let ready = self
                    .client
                    .get(format!(
                        "http://{}/health/ready",
                        self.config.slot_admin(slot.id)
                    ))
                    .timeout(WORKER_CONTROL_TIMEOUT)
                    .send()
                    .await
                    .is_ok_and(|response| response.status() == StatusCode::OK);
                slot.must_be_ready |= ready;
                if slot
                    .started_at
                    .is_some_and(|started| started.elapsed() >= RESTART_STABLE_UPTIME)
                {
                    reset_restart_backoff(&mut slot);
                }
                continue;
            }
            if exited {
                slot.child = None;
                if !active {
                    continue;
                }
                schedule_restart(&mut slot);
                continue;
            }
            if !active {
                continue;
            }
            if std::time::Instant::now() < slot.restart_not_before {
                continue;
            }
            if slot.child.is_none() {
                match read_release_link(&self.config.slot_link(slot.id)) {
                    Ok(release) => slot.release = release,
                    Err(error) => {
                        error!(slot = slot.id.name(), %error, "cannot resolve worker release");
                        schedule_restart(&mut slot);
                        continue;
                    }
                }
                let require_ready = slot.must_be_ready;
                if let Err(error) = self.start_slot(&mut slot, require_ready, true).await {
                    error!(slot = slot.id.name(), %error, "worker restart failed");
                    schedule_restart(&mut slot);
                }
            }
        }
    }

    pub(super) async fn drain_all(&self) {
        self.shutdown.cancel();
        for slot_lock in self.slots.iter() {
            let mut slot = slot_lock.lock().await;
            if slot.child.is_none() {
                continue;
            }
            if let Err(error) = self
                .worker_request(slot.id, Method::PUT, "/admin/api/process/drain")
                .await
            {
                warn!(slot = slot.id.name(), %error, "failed to request worker drain");
                stop_child(&mut slot);
            }
        }
        for slot_lock in self.slots.iter() {
            let mut slot = slot_lock.lock().await;
            if slot.child.is_none() {
                continue;
            }
            if let Err(error) = self.wait_for_exit(&mut slot).await {
                warn!(slot = slot.id.name(), %error, "worker did not drain before supervisor exit");
                stop_child(&mut slot);
            }
        }
    }
}

pub(super) fn stop_child(slot: &mut SlotRuntime) {
    if let Some(mut child) = slot.child.take() {
        if let Err(error) = child.kill() {
            warn!(slot = slot.id.name(), %error, "failed to terminate rejected worker");
        }
        let _ = child.wait();
    }
}

pub(super) fn reset_restart_backoff(slot: &mut SlotRuntime) {
    slot.restart_failures = 0;
    slot.restart_not_before = std::time::Instant::now();
}

pub(super) fn schedule_restart(slot: &mut SlotRuntime) {
    slot.restart_failures = slot.restart_failures.saturating_add(1);
    let exponent = slot.restart_failures.saturating_sub(1).min(6);
    let delay = Duration::from_secs(1_u64 << exponent).min(RESTART_MAX_BACKOFF);
    slot.restart_not_before = std::time::Instant::now() + delay;
    warn!(
        slot = slot.id.name(),
        failures = slot.restart_failures,
        delay_ms = delay.as_millis(),
        "worker restart scheduled"
    );
}
