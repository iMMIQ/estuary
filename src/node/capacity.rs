use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// A resizable semaphore shared by all live configuration generations.
#[derive(Debug)]
pub(super) struct Capacity {
    semaphore: Arc<Semaphore>,
    state: Mutex<State>,
}

#[derive(Debug)]
struct State {
    limit: usize,
    // Permits held by requests/reservations when the limit was lowered.
    debt: usize,
}

impl Capacity {
    pub(super) fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            semaphore: Arc::new(Semaphore::new(limit)),
            state: Mutex::new(State { limit, debt: 0 }),
        })
    }

    pub(super) fn limit(&self) -> usize {
        self.state.lock().limit
    }

    pub(super) fn available(&self) -> usize {
        self.semaphore.available_permits()
    }

    pub(super) fn resize(&self, limit: usize) {
        let mut state = self.state.lock();
        if limit > state.limit {
            let increase = limit - state.limit;
            let repaid = increase.min(state.debt);
            state.debt -= repaid;
            self.semaphore.add_permits(increase - repaid);
        } else {
            let decrease = state.limit - limit;
            let removed = self.semaphore.forget_permits(decrease);
            state.debt += decrease - removed;
        }
        state.limit = limit;
    }

    pub(super) fn try_acquire(self: &Arc<Self>) -> Option<CapacityPermit> {
        let permit = Arc::clone(&self.semaphore).try_acquire_owned().ok()?;
        Some(CapacityPermit {
            capacity: Arc::clone(self),
            permit: Some(permit),
        })
    }

    pub(super) async fn acquire(self: &Arc<Self>) -> CapacityPermit {
        let permit = Arc::clone(&self.semaphore)
            .acquire_owned()
            .await
            .expect("node semaphore is never closed");
        CapacityPermit {
            capacity: Arc::clone(self),
            permit: Some(permit),
        }
    }
}

/// Reservations must repay shrink debt even if cancelled before becoming leases.
#[derive(Debug)]
pub(super) struct CapacityPermit {
    capacity: Arc<Capacity>,
    permit: Option<OwnedSemaphorePermit>,
}

impl Drop for CapacityPermit {
    fn drop(&mut self) {
        let mut state = self.capacity.state.lock();
        if let Some(permit) = self.permit.take() {
            if state.debt > 0 {
                state.debt -= 1;
                permit.forget();
            } else {
                drop(permit);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{FutureExt, poll};
    use std::task::Poll;

    #[test]
    fn resize_reclaims_busy_permits_and_cancels_shrink_debt_on_growth() {
        let capacity = Capacity::new(3);
        let first = capacity.try_acquire().unwrap();
        let second = capacity.try_acquire().unwrap();
        let third = capacity.try_acquire().unwrap();
        capacity.resize(1);
        assert_eq!(capacity.available(), 0);
        drop(first);
        assert!(capacity.try_acquire().is_none());
        capacity.resize(2);
        assert_eq!(capacity.available(), 0);
        drop(second);
        assert_eq!(capacity.available(), 1);
        capacity.resize(4);
        assert_eq!(capacity.available(), 3);
        capacity.resize(1);
        assert_eq!(capacity.available(), 0);
        drop(third);
        assert_eq!(capacity.available(), 1);
    }

    #[tokio::test]
    async fn cancelled_reservations_repay_debt_and_growth_wakes_waiters() {
        let capacity = Capacity::new(2);
        let lease = capacity.try_acquire().unwrap();
        let reservation = capacity.acquire().await;
        capacity.resize(1);
        drop(reservation);
        let waiting = capacity.acquire();
        tokio::pin!(waiting);
        assert!(matches!(poll!(&mut waiting), Poll::Pending));
        capacity.resize(2);
        let reserved = waiting.await;
        assert_eq!(capacity.available(), 0);
        drop(lease);
        assert_eq!(capacity.available(), 1);
        // A pending reservation can be cancelled without leaking capacity.
        let held = capacity.try_acquire().unwrap();
        assert!(capacity.acquire().now_or_never().is_none());
        drop((reserved, held));
        assert_eq!(capacity.available(), 2);
    }
}
