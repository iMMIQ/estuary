use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use tokio::sync::Notify;

/// Accept operations and sockets whose first request has not reached HTTP yet.
/// They must cross this boundary before Hyper can safely shut down idle sockets.
#[derive(Debug, Default)]
pub(crate) struct PendingConnections {
    count: AtomicUsize,
    idle: Notify,
}

impl PendingConnections {
    pub(crate) fn track(self: &Arc<Self>) -> PendingConnection {
        self.count.fetch_add(1, Ordering::AcqRel);
        PendingConnection(Arc::new(ConnectionStart {
            pending: Arc::clone(self),
            finished: AtomicBool::new(false),
        }))
    }

    pub(crate) async fn wait_for_requests(&self) {
        loop {
            let notified = self.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.count.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PendingConnection(Arc<ConnectionStart>);

impl PendingConnection {
    pub(crate) fn request_started(&self) {
        self.0.finish();
    }
}

#[derive(Debug)]
struct ConnectionStart {
    pending: Arc<PendingConnections>,
    finished: AtomicBool,
}

impl ConnectionStart {
    fn finish(&self) {
        if !self.finished.swap(true, Ordering::AcqRel)
            && self.pending.count.fetch_sub(1, Ordering::AcqRel) == 1
        {
            self.pending.idle.notify_waiters();
        }
    }
}

impl Drop for ConnectionStart {
    fn drop(&mut self) {
        self.finish();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pending_accepts_and_closed_sockets_release_the_waiter() {
        let pending = Arc::new(PendingConnections::default());
        let accept = pending.track();
        let socket = pending.track();
        let clone = socket.clone();
        let waiter = {
            let pending = Arc::clone(&pending);
            tokio::spawn(async move { pending.wait_for_requests().await })
        };
        socket.request_started();
        clone.request_started();
        assert_eq!(pending.count.load(Ordering::Acquire), 1);
        drop(accept);
        waiter.await.unwrap();
        drop((socket, clone));
        assert_eq!(pending.count.load(Ordering::Acquire), 0);
        let closed = pending.track();
        drop(closed);
        pending.wait_for_requests().await;
    }
}
