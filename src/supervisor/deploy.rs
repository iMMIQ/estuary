use std::{
    fs::{self},
    os::unix::fs::PermissionsExt,
    path::Path,
    sync::atomic::Ordering,
};

use anyhow::{Context, bail};
use axum::{
    Router,
    body::Body,
    extract::{DefaultBodyLimit, Path as AxumPath, Request, State},
    http::{StatusCode as HttpStatusCode, header::AUTHORIZATION},
    response::{Html, IntoResponse, Response},
    routing::{get, put},
};
use base64::Engine as _;
use futures_util::StreamExt;
use serde::Serialize;
use serde_json::json;
use subtle::ConstantTimeEq;
use tracing::error;

use super::Supervisor;
use super::releases::stage_release;

pub(super) const MAX_UPLOAD_BYTES: usize = 256 * 1024 * 1024;
pub(super) const DEPLOY_HTML: &str = r#"<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Estuary Deploy</title><style>
:root{color-scheme:dark;font-family:system-ui,sans-serif;background:#0b0f14;color:#edf1f5}body{margin:0}main{max-width:900px;margin:auto;padding:32px 20px}header{display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid #29313c;padding-bottom:18px}h1{font-size:22px;margin:0}small,p{color:#9ca6b2}.panel{border:1px solid #29313c;border-radius:6px;margin-top:18px;background:#11171e}form,.row{display:flex;align-items:center;gap:10px;padding:14px 16px;border-bottom:1px solid #222a34}.row:last-child{border:0}.row strong{min-width:130px}.row code{flex:1;color:#9ddff1}button,input::file-selector-button{border:1px solid #3b4654;border-radius:4px;background:#1a222c;color:#edf1f5;padding:8px 12px;cursor:pointer}button.primary{background:#16724a;border-color:#258d62}button.danger{color:#ff9ca3}button:disabled{opacity:.45;cursor:default}.badge{font-size:11px;color:#62db9f}#message{min-height:20px;color:#e8c26a}@media(max-width:600px){main{padding:20px 12px}.row{align-items:flex-start;flex-wrap:wrap}.row strong,.row code{width:100%}form{align-items:stretch;flex-direction:column}}
</style></head><body><main><header><div><h1>Estuary Deploy</h1><small>网关版本部署与切换</small></div><button onclick="load()">刷新</button></header><section class="panel"><form id="upload"><input id="binary" type="file" required><button class="primary">上传版本</button></form><div id="releases"></div></section><p id="message"></p></main><script>
pub(super) const api='/deploy/api/releases',msg=document.querySelector('#message');
async function request(url,options){msg.textContent='处理中...';const r=await fetch(url,options),b=await r.json().catch(()=>({}));if(!r.ok)throw Error(b.error?.message||`HTTP ${r.status}`);msg.textContent='';return b}
async function load(){try{const {releases}=await request(api);document.querySelector('#releases').innerHTML=releases.map(r=>`<div class="row"><strong>${escapeHtml(r.version)} ${r.active?'<span class="badge">当前</span>':''}</strong><code>${format(r.size_bytes)}</code><button class="primary" ${r.active?'disabled':''} onclick="activate('${encodeURIComponent(r.version)}')">切换</button><button class="danger" ${r.current||r.active?'disabled':''} onclick="removeVersion('${encodeURIComponent(r.version)}')">删除</button></div>`).join('')||'<div class="row"><p>没有可用版本</p></div>'}catch(e){msg.textContent=e.message}}
async function activate(v){try{await request(`${api}/${v}`,{method:'PUT'});await load()}catch(e){msg.textContent=e.message}}
async function removeVersion(v){if(!confirm('删除这个版本？'))return;try{await request(`${api}/${v}`,{method:'DELETE'});await load()}catch(e){msg.textContent=e.message}}
document.querySelector('#upload').onsubmit=async e=>{e.preventDefault();const f=document.querySelector('#binary').files[0];try{await request(api,{method:'POST',headers:{'content-type':'application/octet-stream'},body:f});e.target.reset();await load()}catch(e){msg.textContent=e.message}};
function escapeHtml(s){const d=document.createElement('div');d.textContent=s;return d.innerHTML}function format(n){return n<1048576?`${Math.ceil(n/1024)} KiB`:`${(n/1048576).toFixed(1)} MiB`}load();
</script></body></html>"#;

#[derive(Debug, Serialize)]
pub(super) struct ReleaseSnapshot {
    pub(super) version: String,
    pub(super) current: bool,
    pub(super) active: bool,
    pub(super) size_bytes: u64,
}

pub(super) fn deploy_router(supervisor: Supervisor) -> Router {
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

pub(super) async fn proxy_admin(
    State(supervisor): State<Supervisor>,
    request: Request,
) -> Response {
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

pub(super) async fn authorize_deploy(
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

pub(super) fn deploy_authorization_token(value: &str) -> Option<String> {
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

pub(super) async fn deploy_index() -> Html<&'static str> {
    Html(DEPLOY_HTML)
}

pub(super) async fn deploy_status(State(supervisor): State<Supervisor>) -> Response {
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

pub(super) async fn deploy_releases(State(supervisor): State<Supervisor>) -> Response {
    match supervisor.releases().await {
        Ok(releases) => axum::Json(json!({"releases": releases})).into_response(),
        Err(error) => deploy_error(HttpStatusCode::INTERNAL_SERVER_ERROR, &error),
    }
}

pub(super) async fn upload_release(State(supervisor): State<Supervisor>, body: Body) -> Response {
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

pub(super) async fn activate_release(
    State(supervisor): State<Supervisor>,
    AxumPath(version): AxumPath<String>,
) -> Response {
    let target = supervisor.config.release_root.join(&version);
    match supervisor.perform_rollout(target).await {
        Ok(()) => axum::Json(json!({"active_version": version})).into_response(),
        Err(error) => deploy_error(HttpStatusCode::CONFLICT, &error),
    }
}

pub(super) async fn delete_release(
    State(supervisor): State<Supervisor>,
    AxumPath(version): AxumPath<String>,
) -> Response {
    match supervisor.delete_release(&version).await {
        Ok(()) => axum::Json(json!({"deleted": true})).into_response(),
        Err(error) => deploy_error(HttpStatusCode::CONFLICT, &error),
    }
}

pub(super) fn deploy_message(status: HttpStatusCode, message: &str) -> Response {
    (status, axum::Json(json!({"error": {"message": message}}))).into_response()
}

pub(super) fn deploy_error(status: HttpStatusCode, error: &anyhow::Error) -> Response {
    deploy_message(status, &format!("{error:#}"))
}

pub(super) fn release_version(release: &Path) -> Option<String> {
    release.file_name()?.to_str().map(str::to_owned)
}
