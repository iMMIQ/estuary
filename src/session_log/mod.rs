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
pub use capture::Observation;
pub(crate) use capture::{AttemptGuard, DeliveryGuard};

pub use model::{
    AttemptRecord, LogEvent, LogStatus, PayloadDetail, RequestDetail, RequestPage, RequestRecord,
    SessionSummary,
};
use parking_lot::{Condvar, Mutex};
use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

pub struct LogSink {
    config: SessionLogConfig,
    queue: Option<Arc<LogQueue>>,
    stats: Arc<Stats>,
    content_bytes: Arc<AtomicUsize>,
    boot_id: String,
    process_token: Option<String>,
    query_slots: Arc<tokio::sync::Semaphore>,
}

impl Drop for LogSink {
    fn drop(&mut self) {
        if let Some(queue) = &self.queue {
            queue.close();
        }
    }
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

struct QueueState {
    items: VecDeque<Command>,
    reserved: usize,
    closed: bool,
}

struct LogQueue {
    capacity: usize,
    state: Mutex<QueueState>,
    wake: Condvar,
}

struct QueuePermit {
    queue: Arc<LogQueue>,
    consumed: bool,
}

impl QueuePermit {
    fn send(mut self, command: Command) {
        self.consumed = true;
        let mut state = self.queue.state.lock();
        state.reserved = state.reserved.saturating_sub(1);
        state.items.push_back(command);
        self.queue.wake.notify_one();
    }
}

impl Drop for QueuePermit {
    fn drop(&mut self) {
        if !self.consumed {
            let mut state = self.queue.state.lock();
            state.reserved = state.reserved.saturating_sub(1);
        }
    }
}

impl LogQueue {
    fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            capacity,
            state: Mutex::new(QueueState {
                items: VecDeque::new(),
                reserved: 0,
                closed: false,
            }),
            wake: Condvar::new(),
        })
    }

    fn try_start(self: &Arc<Self>, command: Command) -> Option<QueuePermit> {
        let mut state = self.state.lock();
        if state.closed || state.items.len() + state.reserved + 2 > self.capacity {
            return None;
        }
        state.reserved += 1;
        state.items.push_back(command);
        self.wake.notify_one();
        Some(QueuePermit {
            queue: Arc::clone(self),
            consumed: false,
        })
    }

    fn push_flush(&self, reply: oneshot::Sender<bool>) -> bool {
        let mut state = self.state.lock();
        if state.closed {
            return false;
        }
        state.items.push_back(Command::Flush(reply));
        self.wake.notify_one();
        true
    }

    fn close(&self) {
        self.state.lock().closed = true;
        self.wake.notify_all();
    }

    fn recv_until(&self, deadline: Instant) -> RecvStatus {
        let mut state = self.state.lock();
        loop {
            if let Some(command) = state.items.pop_front() {
                return RecvStatus::Item(command);
            }
            if state.closed {
                return RecvStatus::Closed;
            }
            if self.wake.wait_until(&mut state, deadline).timed_out() {
                return RecvStatus::Timeout;
            }
        }
    }
}

enum RecvStatus {
    Item(Command),
    Timeout,
    Closed,
}

impl LogSink {
    pub(crate) fn new(config: SessionLogConfig) -> Arc<Self> {
        let stats = Arc::new(Stats::default());
        let boot_id = uuid::Uuid::now_v7().to_string();
        let queue = if config.database.is_some() {
            let queue = LogQueue::new(config.queue_capacity);
            let worker_queue = Arc::clone(&queue);
            let worker_config = config.clone();
            let worker_stats = Arc::clone(&stats);
            let worker_boot = boot_id.clone();
            if std::thread::Builder::new()
                .name("session-log".to_owned())
                .spawn(move || {
                    writer::run(&worker_queue, &worker_config, &worker_stats, &worker_boot);
                })
                .is_ok()
            {
                Some(queue)
            } else {
                None
            }
        } else {
            None
        };
        Arc::new(Self {
            config,
            queue,
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

    pub async fn flush(&self, timeout: Duration) -> bool {
        let Some(queue) = &self.queue else {
            return true;
        };
        let (send, receive) = oneshot::channel();
        if !queue.push_flush(send) {
            return false;
        }
        tokio::time::timeout(timeout, receive)
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or(false)
    }

    async fn run_query<T: Send + 'static>(
        &self,
        query: impl FnOnce(&Path) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let path = self
            .config
            .database
            .clone()
            .context("session logging is disabled")?;
        let permit = Arc::clone(&self.query_slots)
            .try_acquire_owned()
            .context("too many session log queries")?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            query(&path)
        })
        .await?
    }

    pub async fn list(
        &self,
        session: Option<String>,
        cursor: Option<String>,
        since: u64,
        limit: usize,
    ) -> Result<RequestPage> {
        self.run_query(move |path| {
            store::list(
                path,
                session.as_deref(),
                cursor.as_deref(),
                since,
                limit.clamp(1, 100),
            )
        })
        .await
    }

    pub async fn detail(&self, id: String) -> Result<Option<RequestDetail>> {
        self.run_query(move |path| store::detail(path, &id)).await
    }

    pub async fn sessions(&self, since: u64) -> Result<Vec<SessionSummary>> {
        self.run_query(move |path| store::sessions(path, since))
            .await
    }
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
