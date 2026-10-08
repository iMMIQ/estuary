//! Best-effort session records in an independent database. Capture never waits
//! for storage; bounded finalization permits protect accepted request metadata.
mod capture;
mod content;
mod inspect;
mod model;
mod store;
#[cfg(test)]
mod tests;
mod writer;

use crate::config::SessionLogConfig;
use anyhow::{Context, Result};
pub(crate) use capture::AttemptGuard;
pub(crate) use capture::DeliveryGuard;
pub use capture::Observation;

pub use model::{
    AttemptRecord, LogEvent, LogStatus, PayloadDetail, RequestDetail, RequestPage, RequestRecord,
    SessionSummary,
};
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, oneshot};

pub struct LogSink {
    config: SessionLogConfig,
    sender: Option<mpsc::Sender<Command>>,
    stats: Arc<Stats>,
    content_bytes: Arc<AtomicUsize>,
    boot_id: String,
    process_token: Option<String>,
    query_slots: Arc<tokio::sync::Semaphore>,
}

#[derive(Default)]
struct Stats {
    available: AtomicBool,
    queued: AtomicUsize,
    dropped: AtomicU64,
    truncated: AtomicU64,
    write_errors: AtomicU64,
    committed: AtomicU64,
    last_commit_at_ms: AtomicU64,
}

enum Command {
    Start(Box<model::RequestRecord>),
    Finish(Box<model::RequestRecord>, Vec<capture::CapturedPayload>),
    Flush(oneshot::Sender<bool>),
}

impl LogSink {
    pub(crate) fn new(config: SessionLogConfig) -> Arc<Self> {
        let stats = Arc::new(Stats::default());
        let boot_id = uuid::Uuid::now_v7().to_string();
        let sender = if config.database.is_some() {
            let (sender, receiver) = mpsc::channel(config.queue_capacity);
            let worker_config = config.clone();
            let worker_stats = Arc::clone(&stats);
            let worker_boot = boot_id.clone();
            if std::thread::Builder::new()
                .name("session-log".to_owned())
                .spawn(move || writer::run(receiver, &worker_config, &worker_stats, &worker_boot))
                .is_ok()
            {
                Some(sender)
            } else {
                None
            }
        } else {
            None
        };
        Arc::new(Self {
            config,
            sender,
            stats,
            content_bytes: Arc::new(AtomicUsize::new(0)),
            boot_id,
            process_token: process_token(std::process::id()),
            query_slots: Arc::new(tokio::sync::Semaphore::new(4)),
        })
    }

    pub(crate) fn begin(
        self: &Arc<Self>,
        endpoint: &str,
        headers: &axum::http::HeaderMap,
        external_id: &str,
    ) -> Option<Arc<Observation>> {
        Observation::begin(self, endpoint, headers, external_id)
    }

    pub fn status(&self) -> LogStatus {
        LogStatus {
            enabled: self.config.database.is_some(),
            capture_content: self.config.capture_content,
            available: self.stats.available.load(Ordering::Relaxed),
            queued: self.stats.queued.load(Ordering::Relaxed),
            content_bytes: self.content_bytes.load(Ordering::Relaxed),
            dropped: self.stats.dropped.load(Ordering::Relaxed),
            truncated: self.stats.truncated.load(Ordering::Relaxed),
            write_errors: self.stats.write_errors.load(Ordering::Relaxed),
            committed: self.stats.committed.load(Ordering::Relaxed),
            last_commit_at_ms: self.stats.last_commit_at_ms.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn encode_metrics(&self, mut body: String) -> String {
        use std::fmt::Write;
        // Keep the OpenMetrics terminator last. No request/session IDs are labels.
        if body.ends_with("# EOF\n") {
            body.truncate(body.len() - 6);
        }
        let status = self.status();
        for (name, value) in [
            ("enabled", u64::from(status.enabled)),
            ("available", u64::from(status.available)),
            ("queued", status.queued as u64),
            ("content_bytes", status.content_bytes as u64),
            ("dropped_total", status.dropped),
            ("truncated_total", status.truncated),
            ("write_errors_total", status.write_errors),
            ("committed_total", status.committed),
            ("last_commit_timestamp_ms", status.last_commit_at_ms),
        ] {
            let kind = if name.ends_with("_total") {
                "counter"
            } else {
                "gauge"
            };
            let family = name.strip_suffix("_total").unwrap_or(name);
            let _ = writeln!(
                body,
                "# TYPE estuary_session_log_{family} {kind}\nestuary_session_log_{name} {value}"
            );
        }
        body.push_str("# EOF\n");
        body
    }

    pub async fn flush(&self, timeout: Duration) -> bool {
        let Some(sender) = &self.sender else {
            return true;
        };
        let (send, receive) = oneshot::channel();
        tokio::time::timeout(timeout, async {
            sender.send(Command::Flush(send)).await.ok()?;
            receive.await.ok()
        })
        .await
        .ok()
        .flatten()
        .unwrap_or(false)
    }

    fn path(&self) -> Result<PathBuf> {
        self.config
            .database
            .clone()
            .context("session logging is disabled")
    }

    pub async fn list(
        &self,
        session: Option<String>,
        cursor: Option<String>,
        since: u64,
        limit: usize,
    ) -> Result<RequestPage> {
        let path = self.path()?;
        let permit = Arc::clone(&self.query_slots)
            .try_acquire_owned()
            .context("too many session log queries")?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            store::list(
                &path,
                session.as_deref(),
                cursor.as_deref(),
                since,
                limit.clamp(1, 100),
            )
        })
        .await?
    }
    pub async fn detail(&self, id: String) -> Result<Option<RequestDetail>> {
        let path = self.path()?;
        let permit = Arc::clone(&self.query_slots)
            .try_acquire_owned()
            .context("too many session log queries")?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            store::detail(&path, &id)
        })
        .await?
    }
    pub async fn sessions(&self, since: u64) -> Result<Vec<SessionSummary>> {
        let path = self.path()?;
        let permit = Arc::clone(&self.query_slots)
            .try_acquire_owned()
            .context("too many session log queries")?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            store::sessions(&path, since)
        })
        .await?
    }
}

pub(crate) fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}
pub(crate) fn now_ms() -> u64 {
    unix_ms()
}
fn unix_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

pub(crate) fn separate_database(log: &SessionLogConfig, control: &Path) -> Result<()> {
    use anyhow::bail;
    if let Some(path) = &log.database {
        fn absolute(path: &Path) -> Result<PathBuf> {
            let path = if path.is_absolute() {
                path.to_owned()
            } else {
                std::env::current_dir()?.join(path)
            };
            let mut normalized = PathBuf::new();
            for component in path.components() {
                match component {
                    std::path::Component::ParentDir => {
                        normalized.pop();
                    }
                    std::path::Component::CurDir => {}
                    other => normalized.push(other.as_os_str()),
                }
            }
            let mut existing = normalized.as_path();
            let mut suffix = Vec::new();
            loop {
                if let Ok(mut resolved) = std::fs::canonicalize(existing) {
                    for name in suffix.iter().rev() {
                        resolved.push(name);
                    }
                    return Ok(resolved);
                }
                let Some(name) = existing.file_name() else {
                    return Ok(normalized);
                };
                suffix.push(name.to_owned());
                let Some(parent) = existing.parent() else {
                    return Ok(normalized);
                };
                existing = parent;
            }
        }
        if absolute(path)? == absolute(control)? {
            bail!("session log database must be separate from the configuration database");
        }
        #[cfg(unix)]
        if let (Ok(log), Ok(control)) = (std::fs::metadata(path), std::fs::metadata(control)) {
            use std::os::unix::fs::MetadataExt;
            if log.dev() == control.dev() && log.ino() == control.ino() {
                bail!("session log database must not alias the configuration database");
            }
        }
    }
    Ok(())
}

// PID reuse and a host reboot must not cause an old record to match a live worker.
#[cfg(target_os = "linux")]
fn process_token(pid: u32) -> Option<String> {
    let boot = std::fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()?;
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let (_, fields) = stat.rsplit_once(')')?;
    Some(format!(
        "{}:{}",
        boot.trim(),
        fields.split_whitespace().nth(19)?
    ))
}
#[cfg(not(target_os = "linux"))]
fn process_token(_pid: u32) -> Option<String> {
    None
}
