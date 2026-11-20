use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use serde::Serialize;
use tokio::sync::{Notify, watch};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessState {
    Serving,
    Quiescing,
    Draining,
    Drained,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct ProcessSnapshot {
    pub state: ProcessState,
    pub accepting_traffic: bool,
    pub in_flight_responses: usize,
}

#[derive(Debug)]
pub struct ProcessLifecycle {
    state: watch::Sender<ProcessState>,
    in_flight_responses: AtomicUsize,
    idle_notify: Notify,
}

impl ProcessLifecycle {
    pub fn new() -> Arc<Self> {
        let (state, _) = watch::channel(ProcessState::Serving);
        Arc::new(Self {
            state,
            in_flight_responses: AtomicUsize::new(0),
            idle_notify: Notify::new(),
        })
    }

    pub fn state(&self) -> ProcessState {
        *self.state.borrow()
    }

    pub fn accepting_traffic(&self) -> bool {
        self.state() == ProcessState::Serving
    }

    pub fn request_shutdown(&self) -> bool {
        self.state.send_if_modified(|state| {
            if *state == ProcessState::Serving {
                *state = ProcessState::Quiescing;
                true
            } else {
                false
            }
        })
    }

    pub async fn shutdown_requested(&self) {
        let mut rx = self.state.subscribe();
        let _ = rx.wait_for(|state| *state != ProcessState::Serving).await;
    }

    pub fn mark_draining(&self) {
        self.state.send_replace(ProcessState::Draining);
    }

    pub fn mark_drained(&self) {
        self.state.send_replace(ProcessState::Drained);
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
        lifecycle.shutdown_requested().await;

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
