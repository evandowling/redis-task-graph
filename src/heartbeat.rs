//! Task heartbeats: how a running processor shows it is still making progress.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::{Duration, Instant};

use tokio::task::AbortHandle;

static EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Microseconds since `EPOCH`, never 0 (0 means "never beat").
fn now_us() -> u64 {
    (EPOCH.elapsed().as_micros() as u64).max(1)
}

/// Liveness of one claimed task, shared by its processor's [`Heartbeat`], the worker and the
/// monitor thread that renews its lease.
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
}

/// Lets a running processor show that it is still making progress.
///
/// Processors registered with
/// [`Registry::register_with_heartbeat`](crate::Registry::register_with_heartbeat) receive one.
/// Once a processor has called [`beat`](Heartbeat::beat), its task's lease is renewed only
/// while beats keep arriving at least once per lease. A processor that goes silent for a whole
/// lease is treated as hung: the worker aborts it and records a retryable failure, and if the
/// worker itself is stuck, the lease expires and another worker takes the task over.
///
/// Until the first beat, the lease is renewed as for processors without heartbeats, so a
/// processor can do slow setup before it starts beating. `Heartbeat` is cheap to clone and can
/// be moved into `spawn_blocking` closures or other threads.
#[derive(Clone, Debug)]
pub struct Heartbeat(Arc<Liveness>);

impl Heartbeat {
    pub(crate) fn new(liveness: Arc<Liveness>) -> Self {
        Self(liveness)
    }

    /// A heartbeat attached to no task, for calling a processor directly, for example in a
    /// unit test. Its beats have no effect.
    pub fn detached() -> Self {
        Self(Arc::default())
    }

    /// Records that the processor is still making progress. Cheap enough to call in a loop.
    pub fn beat(&self) {
        self.0.last_beat_us.store(now_us(), Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn silence_applies_only_while_running_after_a_beat() {
        let liveness = Arc::new(Liveness::default());
        let hb = Heartbeat::new(liveness.clone());
        assert_eq!(liveness.silence(), None, "not running");
        liveness.set_running(true);
        assert_eq!(liveness.silence(), None, "never beat");
        hb.beat();
        std::thread::sleep(Duration::from_millis(5));
        assert!(liveness.silence().unwrap() >= Duration::from_millis(5));
        hb.beat();
        assert!(liveness.silence().unwrap() < Duration::from_millis(5));
        liveness.set_running(false);
        assert_eq!(liveness.silence(), None, "finished");
    }
}
