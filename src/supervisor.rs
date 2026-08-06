use std::{
    fs::{self, File},
    io::{Read, Write},
    net::{SocketAddr, TcpListener},
    os::{fd::AsFd, unix::fs::PermissionsExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use command_fds::{CommandFdExt, FdMapping};
use reqwest::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::Mutex,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::Settings;

const PUBLIC_FD: i32 = 3;
const CONTROL_REQUEST_LIMIT: u64 = 64 * 1024;
const WORKER_CONTROL_TIMEOUT: Duration = Duration::from_secs(5);
const RESTART_STABLE_UPTIME: Duration = Duration::from_secs(60);
const RESTART_MAX_BACKOFF: Duration = Duration::from_secs(60);
pub const WORKER_SETTINGS_ENV: &str = "ESTUARY_WORKER_SETTINGS_JSON";

#[derive(Clone, Debug)]
pub struct SupervisorConfig {
    pub settings: Settings,
    pub database: PathBuf,
    pub release_root: PathBuf,
    pub state_root: PathBuf,
    pub runtime_dir: PathBuf,
    pub slot_a_admin: SocketAddr,
    pub slot_b_admin: SocketAddr,
    pub start_timeout: Duration,
    pub drain_timeout: Duration,
}

impl SupervisorConfig {
    pub fn control_socket(&self) -> PathBuf {
        self.runtime_dir.join("supervisor.sock")
    }

    fn freeze_file(&self) -> PathBuf {
        self.runtime_dir.join("admin.freeze")
    }

    fn journal_file(&self) -> PathBuf {
        self.state_root.join("rollout.json")
    }

    fn current_link(&self) -> PathBuf {
        self.state_root.join("current")
    }

    fn slot_link(&self, slot: SlotId) -> PathBuf {
        self.state_root
            .join("slots")
            .join(slot.name())
            .join("current")
    }

    fn slot_admin(&self, slot: SlotId) -> SocketAddr {
        match slot {
            SlotId::A => self.slot_a_admin,
            SlotId::B => self.slot_b_admin,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum SlotId {
    A,
    B,
}

impl SlotId {
    const ALL: [Self; 2] = [Self::A, Self::B];

    const fn name(self) -> &'static str {
        match self {
            Self::A => "a",
            Self::B => "b",
        }
    }
}

#[derive(Debug)]
struct SlotRuntime {
    id: SlotId,
    release: PathBuf,
    child: Option<Child>,
    must_be_ready: bool,
    started_at: Option<std::time::Instant>,
    restart_failures: u32,
    restart_not_before: std::time::Instant,
}

#[derive(Clone)]
struct Supervisor {
    config: Arc<SupervisorConfig>,
    listener: Arc<TcpListener>,
    client: reqwest::Client,
    slots: Arc<Vec<Arc<Mutex<SlotRuntime>>>>,
    rollout_lock: Arc<Mutex<()>>,
    shutdown: CancellationToken,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "command", rename_all = "snake_case")]
enum SupervisorRequest {
    Status,
    Rollout { release: PathBuf },
}

#[derive(Debug, Deserialize, Serialize)]
struct SupervisorResponse {
    ok: bool,
    message: String,
    slots: Vec<SlotSnapshot>,
}

#[derive(Debug, Deserialize, Serialize)]
struct SlotSnapshot {
    slot: SlotId,
    release: PathBuf,
    pid: Option<u32>,
    running: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct RolloutJournal {
    target: PathBuf,
    previous_a: PathBuf,
    previous_b: PathBuf,
    phase: String,
}

#[allow(clippy::too_many_lines)]
pub async fn run(config: SupervisorConfig) -> Result<()> {
    fs::create_dir_all(&config.runtime_dir).with_context(|| {
        format!(
            "failed to create supervisor runtime directory {}",
            config.runtime_dir.display()
        )
    })?;
    ensure_state_layout(&config)?;
    let recovered_rollout = recover_rollout_state(&config)?;

    let listener = TcpListener::bind(&config.settings.server.listen).with_context(|| {
        format!(
            "failed to bind supervisor public listener on {}",
            config.settings.server.listen
        )
    })?;
    listener
        .set_nonblocking(true)
        .context("failed to make supervisor listener non-blocking")?;
    info!(address = %config.settings.server.listen, "supervisor owns public listener");

    let slots = SlotId::ALL
        .into_iter()
        .map(|id| {
            let release = read_release_link(&config.slot_link(id))?;
            Ok(Arc::new(Mutex::new(SlotRuntime {
                id,
                release,
                child: None,
                must_be_ready: false,
                started_at: None,
                restart_failures: 0,
                restart_not_before: std::time::Instant::now(),
            })))
        })
        .collect::<Result<Vec<_>>>()?;
    let supervisor = Supervisor {
        config: Arc::new(config),
        listener: Arc::new(listener),
        client: reqwest::Client::builder()
            .no_proxy()
            .connect_timeout(Duration::from_secs(2))
            .timeout(WORKER_CONTROL_TIMEOUT)
            .build()
            .context("failed to build supervisor control client")?,
        slots: Arc::new(slots),
        rollout_lock: Arc::new(Mutex::new(())),
        shutdown: CancellationToken::new(),
    };

    let control = bind_control_socket(&supervisor.config.control_socket())?;
    for slot in supervisor.slots.iter() {
        let mut slot = slot.lock().await;
        if let Err(error) = supervisor.start_slot(&mut slot, false).await {
            supervisor.shutdown.cancel();
            drop(slot);
            supervisor.drain_all().await;
            if let Some(journal) = recovered_rollout.as_ref() {
                if let Err(restore_error) = restore_rollout_links(&supervisor.config, journal) {
                    error!(error = %restore_error, "failed to restore pre-rollout slot links");
                }
            }
            let _ = fs::remove_file(supervisor.config.control_socket());
            return Err(error);
        }
        reset_restart_backoff(&mut slot);
    }
    if let Err(error) = supervisor.finalize_recovered_rollout().await {
        supervisor.shutdown.cancel();
        supervisor.drain_all().await;
        if let Some(journal) = recovered_rollout.as_ref() {
            if let Err(restore_error) = restore_rollout_links(&supervisor.config, journal) {
                error!(error = %restore_error, "failed to restore pre-rollout slot links");
            }
        }
        let _ = fs::remove_file(supervisor.config.control_socket());
        return Err(error);
    }
    for slot in supervisor.slots.iter().cloned() {
        let watcher = supervisor.clone();
        tokio::spawn(async move { watcher.watch_slot(slot).await });
    }

    info!(path = %supervisor.config.control_socket().display(), "supervisor control socket listening");
    loop {
        tokio::select! {
            accepted = control.accept() => {
                let (stream, _) = accepted.context("failed to accept supervisor control connection")?;
                let supervisor = supervisor.clone();
                tokio::spawn(async move {
                    if let Err(error) = supervisor.handle_control(stream).await {
                        warn!(error = %error, "supervisor control request failed");
                    }
                });
            }
            () = shutdown_signal() => {
                info!("supervisor shutdown requested; draining workers");
                supervisor.shutdown.cancel();
                supervisor.drain_all().await;
                break;
            }
        }
    }
    let _ = fs::remove_file(supervisor.config.control_socket());
    Ok(())
}

impl Supervisor {
    async fn finalize_recovered_rollout(&self) -> Result<()> {
        if !self.config.journal_file().exists() {
            return Ok(());
        }
        let release_a = self.slots[0].lock().await.release.clone();
        let release_b = self.slots[1].lock().await.release.clone();
        if release_a != release_b {
            warn!(
                slot_a = %release_a.display(),
                slot_b = %release_b.display(),
                "mixed worker releases are running; management writes remain frozen"
            );
            return Ok(());
        }
        atomic_symlink(&release_a, &self.config.current_link())?;
        self.unfreeze_writes()?;
        info!(release = %release_a.display(), "recovered rollout after both workers started");
        Ok(())
    }

    async fn start_slot(&self, slot: &mut SlotRuntime, require_ready: bool) -> Result<()> {
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
        self.worker_request(slot.id, Method::PUT, "/admin/api/process/activate")
            .await
            .context("failed to activate worker")?;
        if require_ready {
            self.wait_for_http_ready(slot).await?;
            slot.must_be_ready = true;
        }
        info!(slot = slot.id.name(), release = %slot.release.display(), "worker is accepting traffic");
        slot.started_at = Some(std::time::Instant::now());
        Ok(())
    }

    async fn wait_for_worker(&self, slot: &mut SlotRuntime, require_ready: bool) -> Result<()> {
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
            {
                if !require_ready
                    || response
                        .get("runtime_ready")
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(false)
                {
                    return Ok(());
                }
            }
            if tokio::time::Instant::now() >= deadline {
                bail!("slot {} did not become warm before timeout", slot.id.name());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    async fn wait_for_http_ready(&self, slot: &mut SlotRuntime) -> Result<()> {
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

    async fn worker_request(
        &self,
        slot: SlotId,
        method: Method,
        path: &str,
    ) -> Result<serde_json::Value> {
        let url = format!("http://{}{}", self.config.slot_admin(slot), path);
        let mut request = self.client.request(method, url);
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

    async fn roll_slot(&self, index: usize, target: &Path) -> Result<PathBuf> {
        let slot_lock = Arc::clone(&self.slots[index]);
        let mut slot = slot_lock.lock().await;
        let previous = slot.release.clone();
        if previous == target {
            return Ok(previous);
        }
        let require_ready = self
            .client
            .get(format!(
                "http://{}/health/ready",
                self.config.slot_admin(slot.id)
            ))
            .send()
            .await
            .is_ok_and(|response| response.status() == StatusCode::OK);
        slot.must_be_ready |= require_ready;

        info!(slot = slot.id.name(), target = %target.display(), "draining worker for rollout");
        self.worker_request(slot.id, Method::PUT, "/admin/api/process/drain")
            .await?;
        self.wait_for_exit(&mut slot).await?;
        atomic_symlink(target, &self.config.slot_link(slot.id))?;
        slot.release = target.to_path_buf();
        if let Err(error) = self.start_slot(&mut slot, require_ready).await {
            error!(slot = slot.id.name(), error = %error, "replacement failed; restoring previous worker");
            atomic_symlink(&previous, &self.config.slot_link(slot.id))?;
            slot.release.clone_from(&previous);
            self.start_slot(&mut slot, require_ready)
                .await
                .with_context(|| {
                    format!(
                        "slot {} replacement and rollback both failed",
                        slot.id.name()
                    )
                })?;
            return Err(error);
        }
        reset_restart_backoff(&mut slot);
        Ok(previous)
    }

    async fn wait_for_exit(&self, slot: &mut SlotRuntime) -> Result<()> {
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

    async fn perform_rollout(&self, target: PathBuf) -> Result<()> {
        let _rollout = self
            .rollout_lock
            .try_lock()
            .context("another rollout is already running")?;
        let target = validate_release_dir(&self.config.release_root, &target)?;
        let previous_a = self.slots[0].lock().await.release.clone();
        let previous_b = self.slots[1].lock().await.release.clone();
        let mut journal = RolloutJournal {
            target: target.clone(),
            previous_a: previous_a.clone(),
            previous_b: previous_b.clone(),
            phase: "starting".to_owned(),
        };
        self.freeze_writes(&journal)?;

        let result = async {
            "slot_a".clone_into(&mut journal.phase);
            write_json_atomic(&self.config.journal_file(), &journal)?;
            self.roll_slot(0, &target).await?;
            "slot_b".clone_into(&mut journal.phase);
            write_json_atomic(&self.config.journal_file(), &journal)?;
            if let Err(error) = self.roll_slot(1, &target).await {
                warn!(error = %error, "slot B failed; rolling slot A back to keep one version");
                self.roll_slot(0, &previous_a)
                    .await
                    .with_context(|| "slot B failed and slot A could not be rolled back")?;
                return Err(error);
            }
            atomic_symlink(&target, &self.config.current_link())?;
            Ok(())
        }
        .await;

        match result {
            Ok(()) => {
                self.unfreeze_writes()?;
                info!(release = %target.display(), "rollout completed");
                Ok(())
            }
            Err(error) => {
                let consistent = self.slots[0].lock().await.release == previous_a
                    && self.slots[1].lock().await.release == previous_b;
                if consistent {
                    self.unfreeze_writes()?;
                } else {
                    warn!("workers remain on mixed releases; management writes stay frozen");
                }
                Err(error)
            }
        }
    }

    fn freeze_writes(&self, journal: &RolloutJournal) -> Result<()> {
        write_json_atomic(&self.config.journal_file(), journal)?;
        fs::write(self.config.freeze_file(), b"binary rollout in progress\n")
            .context("failed to freeze management writes")
    }

    fn unfreeze_writes(&self) -> Result<()> {
        remove_if_exists(&self.config.freeze_file())?;
        remove_if_exists(&self.config.journal_file())
    }

    async fn watch_slot(&self, slot_lock: Arc<Mutex<SlotRuntime>>) {
        loop {
            tokio::select! {
                () = self.shutdown.cancelled() => return,
                () = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
            let Ok(mut slot) = slot_lock.try_lock() else {
                continue;
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
                schedule_restart(&mut slot);
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
                if let Err(error) = self.start_slot(&mut slot, require_ready).await {
                    error!(slot = slot.id.name(), %error, "worker restart failed");
                    schedule_restart(&mut slot);
                }
            }
        }
    }

    async fn drain_all(&self) {
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

    async fn handle_control(&self, mut stream: UnixStream) -> Result<()> {
        let mut body = Vec::new();
        (&mut stream)
            .take(CONTROL_REQUEST_LIMIT)
            .read_to_end(&mut body)
            .await
            .context("failed to read supervisor request")?;
        let request: SupervisorRequest =
            serde_json::from_slice(&body).context("invalid supervisor request")?;
        let (ok, message) = match request {
            SupervisorRequest::Status => (true, "supervisor is running".to_owned()),
            SupervisorRequest::Rollout { release } => match self.perform_rollout(release).await {
                Ok(()) => (true, "rollout completed".to_owned()),
                Err(error) => (false, format!("rollout failed: {error:#}")),
            },
        };
        let response = SupervisorResponse {
            ok,
            message,
            slots: self.snapshots().await,
        };
        let mut encoded = serde_json::to_vec(&response)?;
        encoded.push(b'\n');
        stream
            .write_all(&encoded)
            .await
            .context("failed to write supervisor response")?;
        stream.shutdown().await?;
        Ok(())
    }

    async fn snapshots(&self) -> Vec<SlotSnapshot> {
        let mut snapshots = Vec::with_capacity(self.slots.len());
        for slot in self.slots.iter() {
            let mut slot = slot.lock().await;
            let running = slot
                .child
                .as_mut()
                .and_then(|child| child.try_wait().ok())
                .is_some_and(|status| status.is_none());
            snapshots.push(SlotSnapshot {
                slot: slot.id,
                release: slot.release.clone(),
                pid: slot.child.as_ref().map(Child::id),
                running,
            });
        }
        snapshots
    }
}

pub async fn request_rollout(
    release_root: &Path,
    control_socket: &Path,
    binary: &Path,
) -> Result<String> {
    let release = stage_release(release_root, binary)?;
    let response = send_request(control_socket, &SupervisorRequest::Rollout { release }).await?;
    if !response.ok {
        bail!("{}", response.message);
    }
    Ok(response.message)
}

pub async fn request_status(control_socket: &Path) -> Result<String> {
    let response = send_request(control_socket, &SupervisorRequest::Status).await?;
    serde_json::to_string_pretty(&response).context("failed to encode supervisor status")
}

async fn send_request(
    control_socket: &Path,
    request: &SupervisorRequest,
) -> Result<SupervisorResponse> {
    let mut stream = UnixStream::connect(control_socket)
        .await
        .with_context(|| format!("failed to connect to {}", control_socket.display()))?;
    let body = serde_json::to_vec(request)?;
    stream.write_all(&body).await?;
    stream.shutdown().await?;
    let mut response = Vec::new();
    stream
        .take(CONTROL_REQUEST_LIMIT)
        .read_to_end(&mut response)
        .await?;
    serde_json::from_slice(&response).context("supervisor returned an invalid response")
}

fn apply_worker_environment(
    command: &mut Command,
    settings: &Settings,
    admin: SocketAddr,
    freeze_file: &Path,
) -> Result<()> {
    let mut worker = settings.clone();
    worker.server.admin_listen = admin.to_string();
    worker.server.admin_freeze_file = Some(freeze_file.to_owned());
    worker.server.withdrawal_delay_ms = 1;
    command.env(
        WORKER_SETTINGS_ENV,
        serde_json::to_string(&worker).context("failed to serialize worker settings")?,
    );
    Ok(())
}

fn ensure_state_layout(config: &SupervisorConfig) -> Result<()> {
    for slot in SlotId::ALL {
        fs::create_dir_all(config.state_root.join("slots").join(slot.name()))?;
    }
    let current = read_release_link(&config.current_link()).with_context(|| {
        format!(
            "missing current release link {}",
            config.current_link().display()
        )
    })?;
    for slot in SlotId::ALL {
        let link = config.slot_link(slot);
        if fs::symlink_metadata(&link).is_err() {
            atomic_symlink(&current, &link)?;
        }
    }
    Ok(())
}

fn recover_rollout_state(config: &SupervisorConfig) -> Result<Option<RolloutJournal>> {
    if !config.journal_file().exists() {
        return Ok(None);
    }
    let journal: RolloutJournal = serde_json::from_reader(File::open(config.journal_file())?)
        .context("failed to read rollout journal")?;
    validate_release_dir(&config.release_root, &journal.target)?;
    validate_release_dir(&config.release_root, &journal.previous_a)?;
    validate_release_dir(&config.release_root, &journal.previous_b)?;
    let a = read_release_link(&config.slot_link(SlotId::A))?;
    let b = read_release_link(&config.slot_link(SlotId::B))?;
    fs::write(
        config.freeze_file(),
        b"interrupted binary rollout recovery\n",
    )?;
    if a == b {
        info!(release = %a.display(), "interrupted rollout will be finalized after both workers start");
    } else {
        warn!(slot_a = %a.display(), slot_b = %b.display(), "mixed worker releases detected; management writes remain frozen");
    }
    Ok(Some(journal))
}

fn restore_rollout_links(config: &SupervisorConfig, journal: &RolloutJournal) -> Result<()> {
    let previous_a = validate_release_dir(&config.release_root, &journal.previous_a)?;
    let previous_b = validate_release_dir(&config.release_root, &journal.previous_b)?;
    atomic_symlink(&previous_a, &config.slot_link(SlotId::A))?;
    atomic_symlink(&previous_b, &config.slot_link(SlotId::B))?;
    warn!(
        slot_a = %previous_a.display(),
        slot_b = %previous_b.display(),
        "restored pre-rollout slot links after recovery startup failed"
    );
    Ok(())
}

fn bind_control_socket(path: &Path) -> Result<UnixListener> {
    if path.exists() {
        if std::os::unix::net::UnixStream::connect(path).is_ok() {
            bail!(
                "another supervisor is already listening on {}",
                path.display()
            );
        }
        fs::remove_file(path)
            .with_context(|| format!("failed to remove stale socket {}", path.display()))?;
    }
    UnixListener::bind(path)
        .with_context(|| format!("failed to bind control socket {}", path.display()))
}

fn validate_release(release_root: &Path, release: &Path) -> Result<PathBuf> {
    let release = validate_release_dir(release_root, release)?;
    let binary = release.join("estuary");
    if !binary.is_file() {
        bail!("release binary is missing: {}", binary.display());
    }
    Ok(binary)
}

fn validate_release_dir(release_root: &Path, release: &Path) -> Result<PathBuf> {
    let root = release_root
        .canonicalize()
        .with_context(|| format!("invalid release root {}", release_root.display()))?;
    let release = release
        .canonicalize()
        .with_context(|| format!("invalid release directory {}", release.display()))?;
    if release.parent() != Some(root.as_path()) {
        bail!("release must be an immediate child of {}", root.display());
    }
    Ok(release)
}

fn stage_release(release_root: &Path, binary: &Path) -> Result<PathBuf> {
    let binary = binary
        .canonicalize()
        .with_context(|| format!("invalid candidate binary {}", binary.display()))?;
    let output = Command::new(&binary)
        .arg("--version")
        .output()
        .context("failed to execute candidate binary")?;
    if !output.status.success() {
        bail!("candidate --version failed: {}", output.status);
    }
    let stdout = String::from_utf8(output.stdout).context("candidate version is not UTF-8")?;
    let version = stdout
        .split_whitespace()
        .nth(1)
        .context("candidate did not report a version")?;
    if !version
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || b"._+-".contains(&byte))
    {
        bail!("candidate reported an unsafe version: {version}");
    }
    fs::create_dir_all(release_root)?;
    let release = release_root.join(version);
    let destination = release.join("estuary");
    if destination.exists() {
        if file_hash(&destination)? != file_hash(&binary)? {
            bail!("release {version} already exists with different content");
        }
        return release
            .canonicalize()
            .context("failed to resolve existing release");
    }

    fs::create_dir(&release)
        .with_context(|| format!("failed to create release {}", release.display()))?;
    let temporary = release.join(".estuary.tmp");
    fs::copy(&binary, &temporary).context("failed to copy candidate binary")?;
    fs::set_permissions(&temporary, fs::Permissions::from_mode(0o755))?;
    File::open(&temporary)?.sync_all()?;
    fs::rename(&temporary, &destination)?;
    File::open(&release)?.sync_all()?;
    release
        .canonicalize()
        .context("failed to resolve staged release")
}

fn file_hash(path: &Path) -> Result<blake3::Hash> {
    let mut file = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize())
}

fn read_release_link(link: &Path) -> Result<PathBuf> {
    link.canonicalize()
        .with_context(|| format!("failed to resolve release link {}", link.display()))
}

fn atomic_symlink(target: &Path, link: &Path) -> Result<()> {
    let parent = link
        .parent()
        .with_context(|| format!("link has no parent: {}", link.display()))?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        link.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("link"),
        uuid::Uuid::now_v7()
    ));
    std::os::unix::fs::symlink(target, &temporary)?;
    fs::rename(&temporary, link)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path
        .parent()
        .with_context(|| format!("state file has no parent: {}", path.display()))?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(".rollout.{}.tmp", uuid::Uuid::now_v7()));
    let mut file = File::create(&temporary)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn remove_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("failed to remove {}", path.display())),
    }
}

fn stop_child(slot: &mut SlotRuntime) {
    if let Some(mut child) = slot.child.take() {
        if let Err(error) = child.kill() {
            warn!(slot = slot.id.name(), %error, "failed to terminate rejected worker");
        }
        let _ = child.wait();
    }
}

fn reset_restart_backoff(slot: &mut SlotRuntime) {
    slot.restart_failures = 0;
    slot.restart_not_before = std::time::Instant::now();
}

fn schedule_restart(slot: &mut SlotRuntime) {
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

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_restart_backoff_is_bounded_and_resets() {
        let mut slot = SlotRuntime {
            id: SlotId::A,
            release: PathBuf::from("release"),
            child: None,
            must_be_ready: false,
            started_at: None,
            restart_failures: 0,
            restart_not_before: std::time::Instant::now(),
        };
        for _ in 0..20 {
            schedule_restart(&mut slot);
        }
        assert_eq!(slot.restart_failures, 20);
        assert!(slot.restart_not_before <= std::time::Instant::now() + RESTART_MAX_BACKOFF);
        reset_restart_backoff(&mut slot);
        assert_eq!(slot.restart_failures, 0);
    }

    #[test]
    fn atomic_symlink_replaces_existing_target() {
        let root = std::env::temp_dir().join(format!("estuary-link-{}", uuid::Uuid::now_v7()));
        fs::create_dir_all(root.join("one")).unwrap();
        fs::create_dir_all(root.join("two")).unwrap();
        let link = root.join("current");
        atomic_symlink(&root.join("one"), &link).unwrap();
        assert_eq!(link.canonicalize().unwrap(), root.join("one"));
        atomic_symlink(&root.join("two"), &link).unwrap();
        assert_eq!(link.canonicalize().unwrap(), root.join("two"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn release_validation_rejects_nested_paths() {
        let root = std::env::temp_dir().join(format!("estuary-release-{}", uuid::Uuid::now_v7()));
        fs::create_dir_all(root.join("valid").join("nested")).unwrap();
        assert!(validate_release_dir(&root, &root.join("valid")).is_ok());
        assert!(validate_release_dir(&root, &root.join("valid/nested")).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn matching_links_do_not_finalize_an_interrupted_rollout_before_startup() {
        let root = std::env::temp_dir().join(format!("estuary-recovery-{}", uuid::Uuid::now_v7()));
        let releases = root.join("releases");
        let previous = releases.join("previous");
        let target = releases.join("target");
        let state = root.join("state");
        let runtime = root.join("run");
        fs::create_dir_all(&previous).unwrap();
        fs::create_dir_all(&target).unwrap();
        fs::create_dir_all(state.join("slots/a")).unwrap();
        fs::create_dir_all(state.join("slots/b")).unwrap();
        fs::create_dir_all(&runtime).unwrap();
        atomic_symlink(&target, &state.join("current")).unwrap();
        atomic_symlink(&target, &state.join("slots/a/current")).unwrap();
        atomic_symlink(&target, &state.join("slots/b/current")).unwrap();

        let config = SupervisorConfig {
            settings: Settings::default(),
            database: root.join("estuary.db"),
            release_root: releases,
            state_root: state,
            runtime_dir: runtime,
            slot_a_admin: "127.0.0.1:9090".parse().unwrap(),
            slot_b_admin: "127.0.0.1:19092".parse().unwrap(),
            start_timeout: Duration::from_secs(1),
            drain_timeout: Duration::from_secs(1),
        };
        let journal = RolloutJournal {
            target,
            previous_a: previous.clone(),
            previous_b: previous,
            phase: "slot_b".to_owned(),
        };
        write_json_atomic(&config.journal_file(), &journal).unwrap();

        assert!(recover_rollout_state(&config).unwrap().is_some());
        assert!(config.journal_file().exists());
        assert!(config.freeze_file().exists());
        fs::remove_dir_all(root).unwrap();
    }
}
