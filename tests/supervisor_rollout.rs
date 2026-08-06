#![cfg(unix)]

use std::{
    fs,
    net::{SocketAddr, TcpListener},
    os::unix::{fs::PermissionsExt, process::ExitStatusExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(15);

struct TestSupervisor {
    child: Child,
    root: PathBuf,
    runtime: PathBuf,
}

impl Drop for TestSupervisor {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = Command::new("kill")
                .args(["-TERM", &self.child.id().to_string()])
                .status();
            let _ = self.child.wait();
        }
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn supervisor_recovers_workers_and_rolls_back_as_one_unit() -> Result<()> {
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_estuary"));
    let root = std::env::temp_dir().join(format!("estuary-supervisor-{}", uuid::Uuid::now_v7()));
    let releases = root.join("releases");
    let state = root.join("state");
    let runtime = root.join("run");
    let stable = releases.join("stable");
    let candidate = releases.join("candidate");
    fs::create_dir_all(&stable)?;
    fs::create_dir_all(&candidate)?;
    copy_executable(&binary, &stable.join("estuary"))?;
    copy_executable(&binary, &candidate.join("estuary"))?;
    fs::create_dir_all(state.join("slots/a"))?;
    fs::create_dir_all(state.join("slots/b"))?;
    symlink(&stable, &state.join("current"))?;
    symlink(&stable, &state.join("slots/a/current"))?;
    symlink(&stable, &state.join("slots/b/current"))?;

    let public = unused_address()?;
    let admin_a = unused_address()?;
    let admin_b = unused_address()?;
    let child = Command::new(&binary)
        .arg("--database")
        .arg(root.join("estuary.db"))
        .arg("--listen")
        .arg(public.to_string())
        .arg("--admin-listen")
        .arg(admin_a.to_string())
        .arg("supervisor")
        .arg("--release-root")
        .arg(&releases)
        .arg("--state-root")
        .arg(&state)
        .arg("--runtime-dir")
        .arg(&runtime)
        .arg("--slot-b-admin-listen")
        .arg(admin_b.to_string())
        .arg("--start-timeout-seconds")
        .arg("5")
        .arg("--drain-timeout-seconds")
        .arg("5")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to start test supervisor")?;
    let mut supervisor = TestSupervisor {
        child,
        root,
        runtime,
    };

    wait_until(|| async {
        reqwest::get(format!("http://{public}/v1/models"))
            .await
            .is_ok_and(|response| response.status() == StatusCode::OK)
    })
    .await?;
    let initial = control_request(&supervisor.runtime, json!({"command": "status"})).await?;
    assert_running_slots(&initial, &stable)?;

    fs::write(supervisor.runtime.join("admin.freeze"), b"test freeze\n")?;
    let frozen = reqwest::Client::new()
        .post(format!("http://{admin_a}/admin/api/nodes/preflight"))
        .json(&json!({}))
        .send()
        .await?;
    assert_eq!(frozen.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        frozen.json::<Value>().await?["error"]["code"],
        "rollout_in_progress"
    );
    fs::remove_file(supervisor.runtime.join("admin.freeze"))?;

    let old_a_pid = slot_pid(&initial, "a")?;
    send_signal(old_a_pid, "KILL")?;
    let recovered = wait_for_status(&supervisor.runtime, |status| {
        slot_pid(status, "a").is_ok_and(|pid| pid != old_a_pid) && slots_running(status)
    })
    .await?;
    assert_ne!(slot_pid(&recovered, "a")?, old_a_pid);

    let broken = releases.join("broken");
    fs::create_dir(&broken)?;
    let broken_binary = broken.join("estuary");
    fs::write(&broken_binary, b"#!/bin/sh\nexit 42\n")?;
    fs::set_permissions(&broken_binary, fs::Permissions::from_mode(0o755))?;
    let failed = control_request(
        &supervisor.runtime,
        json!({"command": "rollout", "release": broken}),
    )
    .await?;
    assert_eq!(failed["ok"], false);
    assert_running_slots(&failed, &stable)?;
    assert!(!supervisor.runtime.join("admin.freeze").exists());
    assert_public_available(public).await?;

    let stop_requests = Arc::new(AtomicBool::new(false));
    let availability = (0..8)
        .map(|_| {
            let stop = Arc::clone(&stop_requests);
            tokio::spawn(assert_public_stays_available(public, stop))
        })
        .collect::<Vec<_>>();
    let rolled = control_request(
        &supervisor.runtime,
        json!({"command": "rollout", "release": candidate}),
    )
    .await?;
    stop_requests.store(true, Ordering::Release);
    for task in availability {
        assert!(task.await?? > 0);
    }
    assert_eq!(rolled["ok"], true, "{rolled:#}");
    assert_running_slots(&rolled, &releases.join("candidate"))?;
    assert_eq!(state.join("current").canonicalize()?, candidate);
    assert_public_available(public).await?;

    let worker_pids = [slot_pid(&rolled, "a")?, slot_pid(&rolled, "b")?];
    send_signal(supervisor.child.id(), "TERM")?;
    let status = supervisor.child.wait()?;
    assert!(status.success() || status.signal() == Some(15), "{status}");
    for pid in worker_pids {
        wait_until(|| async move { !process_exists(pid) }).await?;
    }
    Ok(())
}

fn unused_address() -> Result<SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?)
}

fn copy_executable(source: &Path, destination: &Path) -> Result<()> {
    fs::copy(source, destination)?;
    fs::set_permissions(destination, fs::Permissions::from_mode(0o755))?;
    Ok(())
}

fn symlink(target: &Path, link: &Path) -> Result<()> {
    std::os::unix::fs::symlink(target, link)?;
    Ok(())
}

async fn control_request(runtime: &Path, request: Value) -> Result<Value> {
    let mut stream = UnixStream::connect(runtime.join("supervisor.sock")).await?;
    stream.write_all(&serde_json::to_vec(&request)?).await?;
    stream.shutdown().await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    Ok(serde_json::from_slice(&response)?)
}

async fn wait_for_status(runtime: &Path, predicate: impl Fn(&Value) -> bool) -> Result<Value> {
    let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
    loop {
        if let Ok(status) = control_request(runtime, json!({"command": "status"})).await
            && predicate(&status)
        {
            return Ok(status);
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("supervisor status did not reach the expected state");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_until<F, Fut>(mut condition: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
    loop {
        if condition().await {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("condition did not become true before timeout");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn slot_pid(status: &Value, name: &str) -> Result<u32> {
    status["slots"]
        .as_array()
        .and_then(|slots| slots.iter().find(|slot| slot["slot"] == name))
        .and_then(|slot| slot["pid"].as_u64())
        .and_then(|pid| u32::try_from(pid).ok())
        .with_context(|| format!("missing PID for slot {name}: {status:#}"))
}

fn slots_running(status: &Value) -> bool {
    status["slots"]
        .as_array()
        .is_some_and(|slots| slots.len() == 2 && slots.iter().all(|slot| slot["running"] == true))
}

fn assert_running_slots(status: &Value, release: &Path) -> Result<()> {
    assert!(slots_running(status), "{status:#}");
    let expected = release.canonicalize()?;
    for slot in status["slots"].as_array().context("missing slots")? {
        assert_eq!(
            Path::new(slot["release"].as_str().context("missing release")?),
            expected
        );
    }
    Ok(())
}

fn send_signal(pid: u32, signal: &str) -> Result<()> {
    let status = Command::new("kill")
        .args([format!("-{signal}"), pid.to_string()])
        .status()?;
    if !status.success() {
        bail!("failed to send SIG{signal} to {pid}");
    }
    Ok(())
}

fn process_exists(pid: u32) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

async fn assert_public_available(public: SocketAddr) -> Result<()> {
    let response = reqwest::get(format!("http://{public}/v1/models")).await?;
    assert_eq!(response.status(), StatusCode::OK);
    Ok(())
}

async fn assert_public_stays_available(public: SocketAddr, stop: Arc<AtomicBool>) -> Result<usize> {
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(0)
        .http1_only()
        .build()?;
    let mut requests = 0;
    while !stop.load(Ordering::Acquire) {
        let response = client
            .get(format!("http://{public}/v1/models"))
            .header("connection", "close")
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            bail!(
                "public listener returned {} during rollout",
                response.status()
            );
        }
        requests += 1;
    }
    Ok(requests)
}
