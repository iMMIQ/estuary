use std::{
    fs::{self, File},
    io::{Read, Write},
    net::{SocketAddr, TcpListener},
    os::{fd::AsFd, unix::fs::PermissionsExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use axum::{
    Router,
    body::Body,
    extract::{DefaultBodyLimit, Path as AxumPath, Request, State},
    http::{StatusCode as HttpStatusCode, header::AUTHORIZATION},
    response::{Html, IntoResponse, Response},
    routing::{get, put},
};
use base64::Engine as _;
use command_fds::{CommandFdExt, FdMapping};
use futures_util::StreamExt;
use reqwest::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;
use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener as TokioTcpListener, UnixListener, UnixStream},
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

const MAX_UPLOAD_BYTES: usize = 256 * 1024 * 1024;
const DEPLOY_HTML: &str = r#"<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Estuary Deploy</title><style>
:root{color-scheme:dark;font-family:system-ui,sans-serif;background:#0b0f14;color:#edf1f5}body{margin:0}main{max-width:900px;margin:auto;padding:32px 20px}header{display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid #29313c;padding-bottom:18px}h1{font-size:22px;margin:0}small,p{color:#9ca6b2}.panel{border:1px solid #29313c;border-radius:6px;margin-top:18px;background:#11171e}form,.row{display:flex;align-items:center;gap:10px;padding:14px 16px;border-bottom:1px solid #222a34}.row:last-child{border:0}.row strong{min-width:130px}.row code{flex:1;color:#9ddff1}button,input::file-selector-button{border:1px solid #3b4654;border-radius:4px;background:#1a222c;color:#edf1f5;padding:8px 12px;cursor:pointer}button.primary{background:#16724a;border-color:#258d62}button.danger{color:#ff9ca3}button:disabled{opacity:.45;cursor:default}.badge{font-size:11px;color:#62db9f}#message{min-height:20px;color:#e8c26a}@media(max-width:600px){main{padding:20px 12px}.row{align-items:flex-start;flex-wrap:wrap}.row strong,.row code{width:100%}form{align-items:stretch;flex-direction:column}}
</style></head><body><main><header><div><h1>Estuary Deploy</h1><small>网关版本部署与切换</small></div><button onclick="load()">刷新</button></header><section class="panel"><form id="upload"><input id="binary" type="file" required><button class="primary">上传版本</button></form><div id="releases"></div></section><p id="message"></p></main><script>
const api='/deploy/api/releases',msg=document.querySelector('#message');
async function request(url,options){msg.textContent='处理中...';const r=await fetch(url,options),b=await r.json().catch(()=>({}));if(!r.ok)throw Error(b.error?.message||`HTTP ${r.status}`);msg.textContent='';return b}
async function load(){try{const {releases}=await request(api);document.querySelector('#releases').innerHTML=releases.map(r=>`<div class="row"><strong>${escapeHtml(r.version)} ${r.active?'<span class="badge">当前</span>':''}</strong><code>${format(r.size_bytes)}</code><button class="primary" ${r.active?'disabled':''} onclick="activate('${encodeURIComponent(r.version)}')">切换</button><button class="danger" ${r.current||r.active?'disabled':''} onclick="removeVersion('${encodeURIComponent(r.version)}')">删除</button></div>`).join('')||'<div class="row"><p>没有可用版本</p></div>'}catch(e){msg.textContent=e.message}}
async function activate(v){try{await request(`${api}/${v}`,{method:'PUT'});await load()}catch(e){msg.textContent=e.message}}
async function removeVersion(v){if(!confirm('删除这个版本？'))return;try{await request(`${api}/${v}`,{method:'DELETE'});await load()}catch(e){msg.textContent=e.message}}
document.querySelector('#upload').onsubmit=async e=>{e.preventDefault();const f=document.querySelector('#binary').files[0];try{await request(api,{method:'POST',headers:{'content-type':'application/octet-stream'},body:f});e.target.reset();await load()}catch(e){msg.textContent=e.message}};
function escapeHtml(s){const d=document.createElement('div');d.textContent=s;return d.innerHTML}function format(n){return n<1048576?`${Math.ceil(n/1024)} KiB`:`${(n/1048576).toFixed(1)} MiB`}load();
</script></body></html>"#;

#[derive(Debug, Serialize)]
struct ReleaseSnapshot {
    version: String,
    current: bool,
    active: bool,
    size_bytes: u64,
}

fn deploy_router(supervisor: Supervisor) -> Router {
    let deploy = Router::new()
        .route("/deploy/", get(deploy_index))
        .route("/deploy/api/status", get(deploy_status))
        .route(
            "/deploy/api/releases",
            get(deploy_releases).post(upload_release),
        )
        .route(
            "/deploy/api/releases/{version}",
            put(activate_release).delete(delete_release),
        )
        .layer(DefaultBodyLimit::max(MAX_UPLOAD_BYTES))
        .layer(axum::middleware::from_fn_with_state(
            supervisor.clone(),
            authorize_deploy,
        ));
    Router::new()
        .merge(deploy)
        .fallback(proxy_admin)
        .with_state(supervisor)
}

async fn proxy_admin(State(supervisor): State<Supervisor>, request: Request) -> Response {
    let active = supervisor.active_slot.load(Ordering::Acquire);
    let admin = {
        let slot = supervisor.slots[active].lock().await;
        supervisor.config.slot_admin(slot.id)
    };
    let (mut parts, body) = request.into_parts();
    let path = parts
        .uri
        .path_and_query()
        .map_or("/", axum::http::uri::PathAndQuery::as_str);
    let url = format!("http://{admin}{path}");
    parts.headers.remove(axum::http::header::HOST);
    let Ok(body) = axum::body::to_bytes(
        body,
        supervisor.config.settings.server.max_request_body_bytes,
    )
    .await
    else {
        return HttpStatusCode::PAYLOAD_TOO_LARGE.into_response();
    };
    let upstream = match supervisor
        .client
        .request(parts.method, url)
        .headers(parts.headers)
        .body(body)
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => {
            error!(%error, "active worker admin request failed");
            return deploy_message(
                HttpStatusCode::SERVICE_UNAVAILABLE,
                "active gateway management endpoint is unavailable",
            );
        }
    };
    let status = upstream.status();
    let headers = upstream.headers().clone();
    let body = match upstream.bytes().await {
        Ok(body) => body,
        Err(error) => {
            error!(%error, "failed to read active worker admin response");
            return HttpStatusCode::BAD_GATEWAY.into_response();
        }
    };
    let mut response = Response::builder().status(status);
    if let Some(response_headers) = response.headers_mut() {
        response_headers.extend(headers);
    }
    response
        .body(Body::from(body))
        .unwrap_or_else(|_| HttpStatusCode::INTERNAL_SERVER_ERROR.into_response())
}

async fn authorize_deploy(
    State(supervisor): State<Supervisor>,
    request: Request,
    next: axum::middleware::Next,
) -> Response {
    let Some(expected) = supervisor.config.settings.server.admin_token.as_deref() else {
        return next.run(request).await;
    };
    let candidate = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(deploy_authorization_token);
    if candidate.is_some_and(|value| bool::from(value.as_bytes().ct_eq(expected.as_bytes()))) {
        return next.run(request).await;
    }
    (
        HttpStatusCode::UNAUTHORIZED,
        [("www-authenticate", "Basic realm=\"Estuary Deploy\"")],
        "authentication required",
    )
        .into_response()
}

fn deploy_authorization_token(value: &str) -> Option<String> {
    if let Some(token) = value.strip_prefix("Bearer ") {
        return Some(token.to_owned());
    }
    let encoded = value.strip_prefix("Basic ")?;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .ok()?;
    String::from_utf8(decoded)
        .ok()?
        .split_once(':')
        .map(|(_, password)| password.to_owned())
}

async fn deploy_index() -> Html<&'static str> {
    Html(DEPLOY_HTML)
}

async fn deploy_status(State(supervisor): State<Supervisor>) -> Response {
    let active = supervisor.active_slot.load(Ordering::Acquire);
    let slots = supervisor.snapshots().await;
    axum::Json(json!({
        "active_slot": slots.get(active).map(|slot| slot.slot),
        "active_version": slots.get(active).and_then(|slot| release_version(&slot.release)),
        "switching": supervisor.config.journal_file().exists(),
        "slots": slots,
    }))
    .into_response()
}

async fn deploy_releases(State(supervisor): State<Supervisor>) -> Response {
    match supervisor.releases().await {
        Ok(releases) => axum::Json(json!({"releases": releases})).into_response(),
        Err(error) => deploy_error(HttpStatusCode::INTERNAL_SERVER_ERROR, &error),
    }
}

async fn upload_release(State(supervisor): State<Supervisor>, body: Body) -> Response {
    let temporary = supervisor
        .config
        .runtime_dir
        .join(format!("upload-{}", uuid::Uuid::now_v7()));
    let result = async {
        let mut file = tokio::fs::File::create(&temporary).await?;
        let mut stream = body.into_data_stream();
        let mut size = 0_usize;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("failed to read upload")?;
            size = size.saturating_add(chunk.len());
            if size > MAX_UPLOAD_BYTES {
                bail!("binary is too large");
            }
            tokio::io::AsyncWriteExt::write_all(&mut file, &chunk).await?;
        }
        if size == 0 {
            bail!("empty upload");
        }
        file.sync_all().await?;
        drop(file);
        fs::set_permissions(&temporary, fs::Permissions::from_mode(0o700))?;
        stage_release(&supervisor.config.release_root, &temporary)
    }
    .await;
    let _ = fs::remove_file(&temporary);
    match result {
        Ok(release) => (
            HttpStatusCode::CREATED,
            axum::Json(json!({"version": release_version(&release), "release": release})),
        )
            .into_response(),
        Err(error) => deploy_error(HttpStatusCode::BAD_REQUEST, &error),
    }
}

async fn activate_release(
    State(supervisor): State<Supervisor>,
    AxumPath(version): AxumPath<String>,
) -> Response {
    let target = supervisor.config.release_root.join(&version);
    match supervisor.perform_rollout(target).await {
        Ok(()) => axum::Json(json!({"active_version": version})).into_response(),
        Err(error) => deploy_error(HttpStatusCode::CONFLICT, &error),
    }
}

async fn delete_release(
    State(supervisor): State<Supervisor>,
    AxumPath(version): AxumPath<String>,
) -> Response {
    match supervisor.delete_release(&version).await {
        Ok(()) => axum::Json(json!({"deleted": true})).into_response(),
        Err(error) => deploy_error(HttpStatusCode::CONFLICT, &error),
    }
}

fn deploy_message(status: HttpStatusCode, message: &str) -> Response {
    (status, axum::Json(json!({"error": {"message": message}}))).into_response()
}

fn deploy_error(status: HttpStatusCode, error: &anyhow::Error) -> Response {
    deploy_message(status, &format!("{error:#}"))
}

fn release_version(release: &Path) -> Option<String> {
    release.file_name()?.to_str().map(str::to_owned)
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

    async fn start_slot(
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

    async fn worker_request(
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

    async fn watch_slot(&self, slot_lock: Arc<Mutex<SlotRuntime>>) {
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
    if !safe_version(version) {
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

fn safe_version(version: &str) -> bool {
    !version.is_empty()
        && version
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._+-".contains(&byte))
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
    fn startup_repairs_a_slot_link_after_its_release_was_deleted() {
        let root = std::env::temp_dir().join(format!("estuary-layout-{}", uuid::Uuid::now_v7()));
        let releases = root.join("releases");
        let current = releases.join("current");
        let deleted = releases.join("deleted");
        let state = root.join("state");
        fs::create_dir_all(&current).unwrap();
        fs::create_dir_all(state.join("slots/a")).unwrap();
        fs::create_dir_all(state.join("slots/b")).unwrap();
        atomic_symlink(&current, &state.join("current")).unwrap();
        atomic_symlink(&deleted, &state.join("slots/a/current")).unwrap();

        let config = SupervisorConfig {
            settings: Settings::default(),
            database: root.join("estuary.db"),
            release_root: releases,
            state_root: state,
            runtime_dir: root.join("run"),
            slot_a_admin: "127.0.0.1:19091".parse().unwrap(),
            slot_b_admin: "127.0.0.1:19092".parse().unwrap(),
            start_timeout: Duration::from_secs(1),
            drain_timeout: Duration::from_secs(1),
        };
        ensure_state_layout(&config).unwrap();
        assert_eq!(
            read_release_link(&config.slot_link(SlotId::A)).unwrap(),
            current
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn deploy_authorization_accepts_bearer_and_basic_passwords() {
        assert_eq!(
            deploy_authorization_token("Bearer secret").as_deref(),
            Some("secret")
        );
        assert_eq!(
            deploy_authorization_token("Basic dXNlcjpzZWNyZXQ=").as_deref(),
            Some("secret")
        );
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
