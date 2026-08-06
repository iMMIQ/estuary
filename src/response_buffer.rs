use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::metrics::Metrics;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResponseBufferSnapshot {
    pub used_bytes: usize,
    pub max_bytes: usize,
    pub waiting_responses: usize,
}

#[derive(Debug)]
pub struct ResponseBufferBudget {
    semaphore: Arc<Semaphore>,
    max_bytes: usize,
    used_bytes: AtomicUsize,
    waiting_responses: AtomicUsize,
    metrics: Arc<Metrics>,
}

impl ResponseBufferBudget {
    pub fn new(max_bytes: usize, metrics: Arc<Metrics>) -> Arc<Self> {
        Arc::new(Self {
            semaphore: Arc::new(Semaphore::new(max_bytes)),
            max_bytes,
            used_bytes: AtomicUsize::new(0),
            waiting_responses: AtomicUsize::new(0),
            metrics,
        })
    }

    pub async fn reserve(self: &Arc<Self>, max_response_bytes: usize) -> ResponseBufferReservation {
        let waiting = self.waiting_responses.fetch_add(1, Ordering::AcqRel) + 1;
        self.metrics.response_buffer_waiters(waiting);
        let waiter = WaiterGuard {
            budget: Arc::clone(self),
        };
        let permit = Arc::clone(&self.semaphore)
            .acquire_many_owned(
                u32::try_from(max_response_bytes)
                    .expect("validated response buffer reservation exceeds u32"),
            )
            .await
            .expect("response buffer semaphore is never closed");
        drop(waiter);

        let used = self
            .used_bytes
            .fetch_add(max_response_bytes, Ordering::AcqRel)
            + max_response_bytes;
        self.metrics.response_buffer_bytes(used);
        ResponseBufferReservation {
            budget: Arc::clone(self),
            permit: Some(permit),
        }
    }

    pub fn snapshot(&self) -> ResponseBufferSnapshot {
        ResponseBufferSnapshot {
            used_bytes: self.used_bytes.load(Ordering::Acquire),
            max_bytes: self.max_bytes,
            waiting_responses: self.waiting_responses.load(Ordering::Acquire),
        }
    }
}

struct WaiterGuard {
    budget: Arc<ResponseBufferBudget>,
}

impl Drop for WaiterGuard {
    fn drop(&mut self) {
        let waiting = self.budget.waiting_responses.fetch_sub(1, Ordering::AcqRel) - 1;
        self.budget.metrics.response_buffer_waiters(waiting);
    }
}

#[derive(Debug)]
pub struct ResponseBufferReservation {
    budget: Arc<ResponseBufferBudget>,
    permit: Option<OwnedSemaphorePermit>,
}

impl ResponseBufferReservation {
    pub fn shrink_to(&mut self, actual_bytes: usize) {
        let permit = self
            .permit
            .as_mut()
            .expect("response buffer reservation has not been released");
        let unused = permit.num_permits().saturating_sub(actual_bytes);
        if unused == 0 {
            return;
        }
        drop(
            permit
                .split(unused)
                .expect("unused response buffer permits are available"),
        );
        let used = self.budget.used_bytes.fetch_sub(unused, Ordering::AcqRel) - unused;
        self.budget.metrics.response_buffer_bytes(used);
    }
}

impl Drop for ResponseBufferReservation {
    fn drop(&mut self) {
        let permits = self.permit.take().map_or(0, |permit| {
            let permits = permit.num_permits();
            drop(permit);
            permits
        });
        if permits > 0 {
            let used = self.budget.used_bytes.fetch_sub(permits, Ordering::AcqRel) - permits;
            self.budget.metrics.response_buffer_bytes(used);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reservation_waits_and_releases_actual_bytes() {
        let budget = ResponseBufferBudget::new(16, Metrics::new());
        let mut first = budget.reserve(16).await;
        assert_eq!(budget.snapshot().used_bytes, 16);

        first.shrink_to(8);
        assert_eq!(budget.snapshot().used_bytes, 8);

        let pending = tokio::spawn({
            let budget = Arc::clone(&budget);
            async move { budget.reserve(16).await }
        });
        tokio::task::yield_now().await;
        assert_eq!(budget.snapshot().waiting_responses, 1);

        drop(first);
        let second = pending.await.unwrap();
        assert_eq!(budget.snapshot().used_bytes, 16);
        drop(second);
        assert_eq!(budget.snapshot().used_bytes, 0);
    }
}
