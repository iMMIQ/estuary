use std::{
    fs::{self, File},
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    process::{Child, Command},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use reqwest::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener as TokioTcpListener, UnixListener, UnixStream},
    sync::Mutex,
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::Settings;

mod worker;
use worker::reset_restart_backoff;
mod deploy;
use deploy::{ReleaseSnapshot, deploy_router};
mod releases;
use releases::{
    atomic_symlink, read_release_link, remove_if_exists, safe_version, stage_release,
    validate_release_dir, write_json_atomic,
};

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
    active_slot: Arc<AtomicUsize>,
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
    active_slot: SlotId,
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
            .build()
            .context("failed to build supervisor control client")?,
        slots: Arc::new(slots),
        active_slot: Arc::new(AtomicUsize::new(0)),
        rollout_lock: Arc::new(Mutex::new(())),
        shutdown: CancellationToken::new(),
    };

    let control = bind_control_socket(&supervisor.config.control_socket())?;
    let current = read_release_link(&supervisor.config.current_link())?;
    {
        let mut slot = supervisor.slots[0].lock().await;
        slot.release = current;
        supervisor.start_slot(&mut slot, false, true).await?;
        atomic_symlink(&slot.release, &supervisor.config.slot_link(SlotId::A))?;
        reset_restart_backoff(&mut slot);
    }
    if recovered_rollout.is_some() {
        supervisor.unfreeze_writes()?;
    }
    for slot in supervisor.slots.iter().cloned() {
        let watcher = supervisor.clone();
        tokio::spawn(async move { watcher.watch_slot(slot).await });
    }

    let deploy_address: SocketAddr = supervisor.config.settings.server.admin_listen.parse()?;
    let deploy_listener = TokioTcpListener::bind(deploy_address)
        .await
        .with_context(|| format!("failed to bind management listener on {deploy_address}"))?;
    let deploy_supervisor = supervisor.clone();
    let deploy = tokio::spawn(async move {
        axum::serve(deploy_listener, deploy_router(deploy_supervisor)).await
    });

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
    deploy.abort();
    Ok(())
}

impl Supervisor {
    async fn releases(&self) -> Result<Vec<ReleaseSnapshot>> {
        let current = read_release_link(&self.config.current_link())?;
        let active_index = self.active_slot.load(Ordering::Acquire);
        let active = self.slots[active_index].lock().await.release.clone();
        let mut releases = Vec::new();
        for entry in fs::read_dir(&self.config.release_root)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() || !entry.path().join("estuary").is_file() {
                continue;
            }
            let release = entry.path().canonicalize()?;
            releases.push(ReleaseSnapshot {
                version: entry.file_name().to_string_lossy().into_owned(),
                current: release == current,
                active: release == active,
                size_bytes: fs::metadata(release.join("estuary"))?.len(),
            });
        }
        releases.sort_unstable_by(|a, b| b.version.cmp(&a.version));
        Ok(releases)
    }

    async fn delete_release(&self, version: &str) -> Result<()> {
        if !safe_version(version) {
            bail!("invalid version");
        }
        let target = validate_release_dir(
            &self.config.release_root,
            &self.config.release_root.join(version),
        )?;
        if read_release_link(&self.config.current_link())? == target {
            bail!("cannot delete the current release");
        }
        for slot in self.slots.iter() {
            let mut slot = slot.lock().await;
            if slot
                .child
                .as_mut()
                .is_some_and(|child| child.try_wait().ok().flatten().is_some())
            {
                slot.child = None;
            }
            if slot.child.is_some() && slot.release == target {
                bail!("cannot delete a running release");
            }
        }
        fs::remove_dir_all(target)?;
        Ok(())
    }

    async fn perform_rollout(&self, target: PathBuf) -> Result<()> {
        let _rollout = self
            .rollout_lock
            .try_lock()
            .context("another rollout is already running")?;
        let target = validate_release_dir(&self.config.release_root, &target)?;
        let active_index = self.active_slot.load(Ordering::Acquire);
        let candidate_index = 1 - active_index;
        let previous = self.slots[active_index].lock().await.release.clone();
        if previous == target {
            return Ok(());
        }
        let mut journal = RolloutJournal {
            target: target.clone(),
            previous_a: previous.clone(),
            previous_b: previous,
            phase: "starting".to_owned(),
        };
        self.freeze_writes(&journal)?;

        let result = async {
            "warming".clone_into(&mut journal.phase);
            write_json_atomic(&self.config.journal_file(), &journal)?;
            let require_ready = {
                let active = self.slots[active_index].lock().await;
                self.client
                    .get(format!(
                        "http://{}/health/ready",
                        self.config.slot_admin(active.id)
                    ))
                    .timeout(WORKER_CONTROL_TIMEOUT)
                    .send()
                    .await
                    .is_ok_and(|response| response.status() == StatusCode::OK)
            };
            let candidate_id = {
                let mut candidate = self.slots[candidate_index].lock().await;
                if candidate.child.is_some() {
                    bail!("previous release is still draining");
                }
                candidate.release.clone_from(&target);
                atomic_symlink(&target, &self.config.slot_link(candidate.id))?;
                self.start_slot(&mut candidate, require_ready, false)
                    .await?;
                candidate.id
            };
            "switching".clone_into(&mut journal.phase);
            write_json_atomic(&self.config.journal_file(), &journal)?;
            self.worker_request(candidate_id, Method::PUT, "/admin/api/process/activate")
                .await?;
            if require_ready {
                let mut candidate = self.slots[candidate_index].lock().await;
                self.wait_for_http_ready(&mut candidate).await?;
                candidate.must_be_ready = true;
            }
            atomic_symlink(&target, &self.config.current_link())?;
            let active_id = self.slots[active_index].lock().await.id;
            if let Err(error) = self
                .worker_request(active_id, Method::PUT, "/admin/api/process/drain")
                .await
            {
                atomic_symlink(&journal.previous_a, &self.config.current_link())?;
                let _ = self
                    .worker_request(candidate_id, Method::PUT, "/admin/api/process/drain")
                    .await;
                return Err(error).context("failed to drain previous worker");
            }
            self.active_slot.store(candidate_index, Ordering::Release);
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
                if candidate_index != self.active_slot.load(Ordering::Acquire) {
                    let candidate_id = self.slots[candidate_index].lock().await.id;
                    let _ = self
                        .worker_request(candidate_id, Method::PUT, "/admin/api/process/drain")
                        .await;
                }
                self.unfreeze_writes()?;
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
            active_slot: if self.active_slot.load(Ordering::Acquire) == 0 {
                SlotId::A
            } else {
                SlotId::B
            },
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
        if read_release_link(&link).is_err() {
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
    info!(slot_a = %a.display(), slot_b = %b.display(), "interrupted switch will recover the stable current release");
    Ok(Some(journal))
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
mod tests;
