//! A fair concurrency limiter whose limit can be changed while permits are held.
//!
//! Built on Tokio's FIFO [`Semaphore`], so a released permit goes to the longest waiter rather
//! than to whichever task asks next; otherwise the fetch loop could starve the reclaim loop.
//! Lowering the limit never preempts running work: permits that are free are removed at once,
//! and the rest are removed as they are released.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(crate) struct Limiter {
    semaphore: Arc<Semaphore>,
    limit: AtomicUsize,
    /// Permits still to be removed from circulation as they are released.
    debt: AtomicUsize,
    in_flight: AtomicUsize,
    /// Set when demand exceeded the available permits since the last [`Limiter::take_contended`].
    contended: AtomicBool,
    /// Serializes limit changes.
    resize: Mutex<()>,
}

impl Limiter {
    pub(crate) fn new(limit: usize) -> Arc<Self> {
        let limit = limit.max(1);
        Arc::new(Self {
            semaphore: Arc::new(Semaphore::new(limit)),
            limit: AtomicUsize::new(limit),
            debt: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
            contended: AtomicBool::new(false),
            resize: Mutex::new(()),
        })
    }

    /// Records that a caller wanted more permits than were available.
    pub(crate) fn mark_contended(&self) {
        self.contended.store(true, Ordering::Release);
    }

    /// Whether demand exceeded the limit since the previous call.
    pub(crate) fn take_contended(&self) -> bool {
        self.contended.swap(false, Ordering::AcqRel)
    }

    pub(crate) fn limit(&self) -> usize {
        self.limit.load(Ordering::Acquire)
    }

    pub(crate) fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }

    pub(crate) fn set_limit(&self, limit: usize) {
        let limit = limit.max(1);
        let _guard = self.resize.lock().unwrap();
        let old = self.limit.swap(limit, Ordering::AcqRel);
        if limit < old {
            let excess = old - limit;
            let removed = self.semaphore.forget_permits(excess);
            self.debt.fetch_add(excess - removed, Ordering::AcqRel);
        } else if limit > old {
            let mut added = limit - old;
            // Cancel outstanding debt before putting new permits into circulation.
            #[allow(deprecated)] // renamed `try_update`, which Rust 1.89 lacks
            let _ = self.debt.fetch_update(Ordering::AcqRel, Ordering::Acquire, |debt| {
                let paid = debt.min(added);
                added = limit - old - paid;
                Some(debt - paid)
            });
            self.semaphore.add_permits(added);
        }
    }

    fn wrap(self: &Arc<Self>, permit: OwnedSemaphorePermit) -> Permit {
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        Permit {
            limiter: self.clone(),
            permit: Some(permit),
        }
    }

    pub(crate) fn try_acquire(self: &Arc<Self>) -> Option<Permit> {
        let permit = self.semaphore.clone().try_acquire_owned().ok()?;
        Some(self.wrap(permit))
    }

    pub(crate) async fn acquire(self: &Arc<Self>) -> Permit {
        if let Some(permit) = self.try_acquire() {
            return permit;
        }
        self.mark_contended();
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("limiter semaphore is never closed");
        self.wrap(permit)
    }
}

/// Holds one unit of the limit; released on drop.
pub(crate) struct Permit {
    limiter: Arc<Limiter>,
    permit: Option<OwnedSemaphorePermit>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.limiter.in_flight.fetch_sub(1, Ordering::AcqRel);
        #[allow(deprecated)] // renamed `try_update`, which Rust 1.89 lacks
        let pays_debt = self
            .limiter
            .debt
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |d| d.checked_sub(1))
            .is_ok();
        if let Some(permit) = self.permit.take()
            && pays_debt
        {
            permit.forget();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn enforces_the_limit() {
        let l = Limiter::new(2);
        let a = l.try_acquire().unwrap();
        let _b = l.try_acquire().unwrap();
        assert!(l.try_acquire().is_none());
        assert_eq!(l.in_flight(), 2);
        drop(a);
        assert!(l.try_acquire().is_some());
    }

    #[test]
    fn lowering_the_limit_waits_for_releases() {
        let l = Limiter::new(3);
        let held: Vec<_> = (0..3).map(|_| l.try_acquire().unwrap()).collect();
        l.set_limit(1);
        let mut held = held.into_iter();
        drop(held.next());
        assert!(l.try_acquire().is_none(), "2 still held, limit 1");
        drop(held.next());
        assert!(l.try_acquire().is_none(), "1 still held, limit 1");
        drop(held.next());
        let p = l.try_acquire();
        assert!(p.is_some());
        assert!(l.try_acquire().is_none(), "limit is 1");
    }

    #[test]
    fn lowering_then_raising_restores_capacity() {
        let l = Limiter::new(4);
        let held: Vec<_> = (0..4).map(|_| l.try_acquire().unwrap()).collect();
        l.set_limit(1);
        l.set_limit(3);
        drop(held);
        let permits: Vec<_> = std::iter::from_fn(|| l.try_acquire()).take(10).collect();
        assert_eq!(permits.len(), 3);
    }

    #[tokio::test]
    async fn raising_the_limit_wakes_waiters() {
        let l = Limiter::new(1);
        let _held = l.try_acquire().unwrap();
        let waiter = tokio::spawn({
            let l = l.clone();
            async move { l.acquire().await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());
        l.set_limit(2);
        tokio::time::timeout(Duration::from_secs(1), waiter).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn waiting_marks_contention() {
        let l = Limiter::new(1);
        let held = l.acquire().await;
        assert!(!l.take_contended(), "a free permit is not contention");
        let waiter = tokio::spawn({
            let l = l.clone();
            async move { l.acquire().await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(l.take_contended());
        assert!(!l.take_contended(), "taking clears the flag");
        drop(held);
        waiter.await.unwrap();
    }

    #[tokio::test]
    async fn released_permit_goes_to_the_waiter_first() {
        let l = Limiter::new(1);
        let held = l.try_acquire().unwrap();
        let waiter = tokio::spawn({
            let l = l.clone();
            async move { l.acquire().await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(held);
        assert!(l.try_acquire().is_none(), "the waiting task must get the permit");
        tokio::time::timeout(Duration::from_secs(1), waiter).await.unwrap().unwrap();
    }
}
