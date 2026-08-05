use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
};

use serde::Serialize;
use tokio::sync::Notify;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessState {
    Serving,
    Quiescing,
    Draining,
    Drained,
}

impl ProcessState {
    const fn encode(self) -> u8 {
        match self {
            Self::Serving => 0,
            Self::Quiescing => 1,
            Self::Draining => 2,
            Self::Drained => 3,
        }
    }

    const fn decode(value: u8) -> Self {
        match value {
            1 => Self::Quiescing,
            2 => Self::Draining,
            3 => Self::Drained,
            _ => Self::Serving,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct ProcessSnapshot {
    pub state: ProcessState,
    pub accepting_traffic: bool,
    pub in_flight_responses: usize,
}

#[derive(Debug)]
pub struct ProcessLifecycle {
    state: AtomicU8,
    shutdown_requested: AtomicBool,
    in_flight_responses: AtomicUsize,
    shutdown_notify: Notify,
    idle_notify: Notify,
}

impl ProcessLifecycle {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: AtomicU8::new(ProcessState::Serving.encode()),
            shutdown_requested: AtomicBool::new(false),
            in_flight_responses: AtomicUsize::new(0),
            shutdown_notify: Notify::new(),
            idle_notify: Notify::new(),
        })
    }

    pub fn state(&self) -> ProcessState {
        ProcessState::decode(self.state.load(Ordering::Acquire))
    }

    pub fn accepting_traffic(&self) -> bool {
        self.state() == ProcessState::Serving
    }

    pub fn request_shutdown(&self) -> bool {
        let first = !self.shutdown_requested.swap(true, Ordering::AcqRel);
        if first {
            self.state
                .store(ProcessState::Quiescing.encode(), Ordering::Release);
            self.shutdown_notify.notify_waiters();
        }
        first
    }

    pub async fn shutdown_requested(&self) {
        loop {
            if self.shutdown_requested.load(Ordering::Acquire) {
                return;
            }
            let notified = self.shutdown_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.shutdown_requested.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    pub fn mark_draining(&self) {
        self.state
            .store(ProcessState::Draining.encode(), Ordering::Release);
    }

    pub fn mark_drained(&self) {
        self.state
            .store(ProcessState::Drained.encode(), Ordering::Release);
    }

    pub fn track_response(self: &Arc<Self>) -> ResponseGuard {
        self.in_flight_responses.fetch_add(1, Ordering::AcqRel);
        ResponseGuard {
            lifecycle: Arc::clone(self),
        }
    }

    pub fn in_flight_responses(&self) -> usize {
        self.in_flight_responses.load(Ordering::Acquire)
    }

    pub async fn wait_for_idle(&self) {
        loop {
            if self.in_flight_responses() == 0 {
                return;
            }
            let notified = self.idle_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.in_flight_responses() == 0 {
                return;
            }
            notified.await;
        }
    }

    pub fn snapshot(&self) -> ProcessSnapshot {
        ProcessSnapshot {
            state: self.state(),
            accepting_traffic: self.accepting_traffic(),
            in_flight_responses: self.in_flight_responses(),
        }
    }
}

#[derive(Debug)]
pub struct ResponseGuard {
    lifecycle: Arc<ProcessLifecycle>,
}

impl Drop for ResponseGuard {
    fn drop(&mut self) {
        if self
            .lifecycle
            .in_flight_responses
            .fetch_sub(1, Ordering::AcqRel)
            == 1
        {
            self.lifecycle.idle_notify.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shutdown_and_response_tracking_are_monotonic() {
        let lifecycle = ProcessLifecycle::new();
        let guard = lifecycle.track_response();
        assert!(lifecycle.request_shutdown());
        assert!(!lifecycle.request_shutdown());
        assert_eq!(lifecycle.state(), ProcessState::Quiescing);

        let waiter = {
            let lifecycle = Arc::clone(&lifecycle);
            tokio::spawn(async move { lifecycle.wait_for_idle().await })
        };
        assert!(!waiter.is_finished());
        drop(guard);
        waiter.await.unwrap();

        lifecycle.mark_draining();
        lifecycle.mark_drained();
        assert_eq!(lifecycle.state(), ProcessState::Drained);
    }
}
