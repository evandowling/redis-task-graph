//! Per-task control: how a running processor shows it is still making progress, and learns
//! that it should stop.

use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::{Duration, Instant};

use tokio::sync::Notify;
use tokio::task::AbortHandle;

static EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Microseconds since `EPOCH`, never 0 (0 means "never beat").
fn now_us() -> u64 {
    (EPOCH.elapsed().as_micros() as u64).max(1)
}

/// Why a processor is asked to stop, from [`TaskControl::cancel_reason`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CancelReason {
    /// Another worker took the task over, usually because this worker couldn't renew its
    /// lease in time. The task's outcome will be rejected, so stopping avoids duplicate work.
    LeaseLost,
    /// The worker is shutting down. A processor that stops and returns an error has its task
    /// released: requeued at once, without using up an attempt.
    Shutdown,
}

impl CancelReason {
    fn code(self) -> u8 {
        match self {
            Self::LeaseLost => 1,
            Self::Shutdown => 2,
        }
    }

    fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::LeaseLost),
            2 => Some(Self::Shutdown),
            _ => None,
        }
    }
}

/// Liveness and cancellation of one claimed task, shared by its processor's [`TaskControl`],
/// the worker and the monitor thread that renews its lease.
#[derive(Debug, Default)]
pub(crate) struct Liveness {
    /// Whether the processor is running, as opposed to the task being queued or its outcome
    /// being recorded. Heartbeats only matter while it runs.
    running: AtomicBool,
    /// Time of the most recent beat in microseconds since `EPOCH`, or 0 if the processor never
    /// beat. Processors that never beat have their lease renewed unconditionally.
    last_beat_us: AtomicU64,
    /// Aborts the processor's task. Set when the processor starts, and used by the monitor when
    /// it stops renewing the lease because heartbeats stopped. Aborting from the monitor thread
    /// works even if the worker's own tasks are starved.
    abort: OnceLock<AbortHandle>,
    /// The first [`CancelReason`]'s code, or 0 while not cancelled.
    cancel: AtomicU8,
    /// Wakes processors waiting in [`TaskControl::cancelled`].
    cancelled: Notify,
}

impl Liveness {
    pub(crate) fn set_running(&self, running: bool) {
        self.running.store(running, Ordering::Release);
    }

    /// How long a running processor that uses heartbeats has gone without one. `None` when
    /// heartbeats don't apply: the processor isn't running, or has never beaten.
    pub(crate) fn silence(&self) -> Option<Duration> {
        if !self.running.load(Ordering::Acquire) {
            return None;
        }
        let last = self.last_beat_us.load(Ordering::Acquire);
        (last != 0).then(|| Duration::from_micros(now_us().saturating_sub(last)))
    }

    /// Records how to abort the processor once it is running.
    pub(crate) fn set_abort(&self, handle: AbortHandle) {
        let _ = self.abort.set(handle);
    }

    /// Aborts the processor because its lease is no longer being renewed.
    pub(crate) fn abort(&self) {
        if let Some(handle) = self.abort.get() {
            handle.abort();
        }
    }

    /// Asks the processor to stop. The first reason given wins.
    pub(crate) fn cancel(&self, reason: CancelReason) {
        let first = self
            .cancel
            .compare_exchange(0, reason.code(), Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        if first {
            self.cancelled.notify_waiters();
        }
    }

    pub(crate) fn cancel_reason(&self) -> Option<CancelReason> {
        CancelReason::from_code(self.cancel.load(Ordering::Acquire))
    }
}

/// Lets a running processor show that it is still making progress, and learn that it should
/// stop.
///
/// Processors registered with
/// [`Registry::register_with_control`](crate::Registry::register_with_control) receive one.
///
/// **Heartbeats.** Once a processor has called [`beat`](TaskControl::beat), its task's lease
/// is renewed only while beats keep arriving at least once per lease. A processor that goes
/// silent for a whole lease is treated as hung: the worker aborts it and records a retryable
/// failure, and if the worker itself is stuck, the lease expires and another worker takes the
/// task over. Until the first beat, the lease is renewed as for processors without heartbeats,
/// so a processor can do slow setup before it starts beating.
///
/// **Cancellation.** [`cancelled`](TaskControl::cancelled) resolves, and
/// [`is_cancelled`](TaskControl::is_cancelled) turns true, when the task's lease is lost or the
/// worker shuts down; [`cancel_reason`](TaskControl::cancel_reason) says which. Cancellation is
/// only a request: a processor that ignores it runs to completion as usual.
///
/// `TaskControl` is cheap to clone and can be moved into `spawn_blocking` closures or other
/// threads.
#[derive(Clone, Debug)]
pub struct TaskControl(Arc<Liveness>);

/// The 0.1 name for [`TaskControl`].
#[deprecated(since = "0.2.0", note = "renamed to `TaskControl`")]
pub type Heartbeat = TaskControl;

impl TaskControl {
    pub(crate) fn new(liveness: Arc<Liveness>) -> Self {
        Self(liveness)
    }

    /// A control attached to no task, for calling a processor directly, for example in a unit
    /// test. Its beats have no effect and it is never cancelled.
    pub fn detached() -> Self {
        Self(Arc::default())
    }

    /// Records that the processor is still making progress. Cheap enough to call in a loop.
    pub fn beat(&self) {
        self.0.last_beat_us.store(now_us(), Ordering::Release);
    }

    /// Whether the processor has been asked to stop.
    pub fn is_cancelled(&self) -> bool {
        self.0.cancel_reason().is_some()
    }

    /// Why the processor has been asked to stop, or `None` if it hasn't been.
    pub fn cancel_reason(&self) -> Option<CancelReason> {
        self.0.cancel_reason()
    }

    /// Resolves once the processor has been asked to stop, for use in `tokio::select!`.
    /// Cancel-safe.
    pub async fn cancelled(&self) -> CancelReason {
        loop {
            let notified = self.0.cancelled.notified();
            tokio::pin!(notified);
            // Register for the wake-up before checking, so a cancel in between isn't missed.
            notified.as_mut().enable();
            if let Some(reason) = self.0.cancel_reason() {
                return reason;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn silence_applies_only_while_running_after_a_beat() {
        let liveness = Arc::new(Liveness::default());
        let control = TaskControl::new(liveness.clone());
        assert_eq!(liveness.silence(), None, "not running");
        liveness.set_running(true);
        assert_eq!(liveness.silence(), None, "never beat");
        control.beat();
        std::thread::sleep(Duration::from_millis(5));
        assert!(liveness.silence().unwrap() >= Duration::from_millis(5));
        control.beat();
        assert!(liveness.silence().unwrap() < Duration::from_millis(5));
        liveness.set_running(false);
        assert_eq!(liveness.silence(), None, "finished");
    }

    #[tokio::test]
    async fn cancellation_wakes_waiters_and_keeps_the_first_reason() {
        let liveness = Arc::new(Liveness::default());
        let control = TaskControl::new(liveness.clone());
        assert!(!control.is_cancelled());
        let waiter = tokio::spawn({
            let control = control.clone();
            async move { control.cancelled().await }
        });
        tokio::task::yield_now().await;
        // Cancelled from another thread, as the monitor does.
        std::thread::spawn({
            let liveness = liveness.clone();
            move || liveness.cancel(CancelReason::LeaseLost)
        })
        .join()
        .unwrap();
        liveness.cancel(CancelReason::Shutdown);
        assert_eq!(waiter.await.unwrap(), CancelReason::LeaseLost);
        assert_eq!(control.cancel_reason(), Some(CancelReason::LeaseLost));
        assert_eq!(
            control.cancelled().await,
            CancelReason::LeaseLost,
            "resolves at once when already cancelled"
        );
    }
}
