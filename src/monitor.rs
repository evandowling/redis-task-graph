//! Per-worker monitor thread: measures runtime scheduling lag, adapts the concurrency limit to
//! it (pushback), and renews task leases.
//!
//! The monitor runs on its own OS thread with its own Redis connection, so lease renewal never
//! waits for a runtime thread. Even if processors block every runtime thread, leases stay alive.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tokio::runtime::Handle;
use tracing::{debug, warn};

use crate::limiter::Limiter;
use crate::scripts;

/// Multiplier applied to the limit when lag exceeds its budget.
const DECREASE_FACTOR: f64 = 0.7;

/// State shared between a worker's tasks and its monitor thread.
pub(crate) struct Monitored {
    pub(crate) limiter: Arc<Limiter>,
    /// Most recent scheduling-lag sample, in microseconds.
    pub(crate) lag_us: AtomicU64,
    /// Longest wait between spawning a processor and its first poll since the last sample, in
    /// microseconds: how long admitted work queues for a runtime thread.
    start_delay_us: AtomicU64,
    /// Claimed entries whose leases the monitor renews, keyed by (stream, entry id), with the
    /// time their next heartbeat is due.
    leases: Mutex<HashMap<(String, String), Instant>>,
    heartbeat_period: Duration,
}

impl Monitored {
    pub(crate) fn new(limiter: Arc<Limiter>, lease: Duration) -> Arc<Self> {
        Arc::new(Self {
            limiter,
            lag_us: AtomicU64::new(0),
            start_delay_us: AtomicU64::new(0),
            leases: Mutex::new(HashMap::new()),
            heartbeat_period: (lease / 3).max(Duration::from_millis(10)),
        })
    }

    /// Records how long a processor waited between being spawned and starting to run.
    pub(crate) fn record_start_delay(&self, delay: Duration) {
        self.start_delay_us.fetch_max(delay.as_micros() as u64, Ordering::AcqRel);
    }

    /// Starts renewing the lease on an entry until the returned guard is dropped.
    pub(crate) fn hold_lease(self: &Arc<Self>, stream: &str, id: &str) -> LeaseGuard {
        let key = (stream.to_owned(), id.to_owned());
        let due = Instant::now() + self.heartbeat_period;
        self.leases.lock().unwrap().insert(key.clone(), due);
        LeaseGuard {
            monitored: self.clone(),
            key,
        }
    }
}

pub(crate) struct LeaseGuard {
    monitored: Arc<Monitored>,
    key: (String, String),
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        self.monitored.leases.lock().unwrap().remove(&self.key);
    }
}

pub(crate) struct MonitorConfig {
    pub(crate) client: redis::Client,
    pub(crate) group: String,
    pub(crate) consumer: String,
    pub(crate) probe_interval: Duration,
    pub(crate) lag_budget: Duration,
    pub(crate) adaptive: bool,
    pub(crate) min_concurrency: usize,
    pub(crate) max_concurrency: usize,
}

/// Handle to a running monitor thread.
pub(crate) struct Monitor {
    stop: Arc<AtomicBool>,
    thread: JoinHandle<()>,
}

impl Monitor {
    pub(crate) fn spawn(config: MonitorConfig, monitored: &Arc<Monitored>, runtime: Handle) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let weak = Arc::downgrade(monitored);
        let thread = std::thread::Builder::new()
            .name(format!("rtg-monitor-{}", config.consumer))
            .spawn({
                let stop = stop.clone();
                move || run(config, weak, runtime, stop)
            })
            .expect("spawn monitor thread");
        Self { stop, thread }
    }

    /// Stops the thread and waits for it to exit.
    pub(crate) fn stop(self) {
        self.stop.store(true, Ordering::Release);
        let _ = self.thread.join();
    }
}

/// A probe task's progress: whether it is still waiting to be polled, and the last measured
/// delay between spawning a probe and its first poll.
#[derive(Default)]
struct Probe {
    pending: AtomicBool,
    last_us: AtomicU64,
}

fn run(config: MonitorConfig, monitored: Weak<Monitored>, runtime: Handle, stop: Arc<AtomicBool>) {
    let probe = Arc::new(Probe::default());
    let mut probe_sent = Instant::now();
    let mut controller = Controller::new(
        config.lag_budget,
        config.probe_interval,
        config.min_concurrency,
        config.max_concurrency,
    );
    let mut conn: Option<redis::Connection> = None;

    while !stop.load(Ordering::Acquire) {
        std::thread::sleep(config.probe_interval);
        // The worker's state is dropped once its runtime and tasks are gone.
        let Some(shared) = monitored.upgrade() else {
            return;
        };
        let now = Instant::now();

        // Scheduling lag is the worse of two views of how long ready work waits for a thread:
        // a probe injected from outside, and the processors admitted since the last sample.
        let start_delay = Duration::from_micros(shared.start_delay_us.swap(0, Ordering::AcqRel));
        let probe_lag = if probe.pending.load(Ordering::Acquire) {
            now - probe_sent
        } else {
            let lag = Duration::from_micros(probe.last_us.load(Ordering::Acquire));
            probe.pending.store(true, Ordering::Release);
            probe_sent = now;
            let p = probe.clone();
            runtime.spawn(async move {
                p.last_us.store(now.elapsed().as_micros() as u64, Ordering::Release);
                p.pending.store(false, Ordering::Release);
            });
            lag
        };
        let lag = probe_lag.max(start_delay);
        shared.lag_us.store(lag.as_micros() as u64, Ordering::Release);

        if config.adaptive {
            adjust_limit(&mut controller, &shared.limiter, lag, now);
        }

        renew_leases(&config, &shared, &mut conn, now);
    }
}

/// Adapts the concurrency limit to scheduling lag, like TCP congestion control: the limit
/// starts at the runtime's thread count and doubles while the runtime keeps up (slow start).
/// After the first time lag exceeds the budget, it grows by one per sample and shrinks
/// multiplicatively whenever lag is over budget.
pub(crate) struct Controller {
    lag_budget: Duration,
    cooldown: Duration,
    min: usize,
    max: usize,
    slow_start: bool,
    last_decrease: Option<Instant>,
}

impl Controller {
    pub(crate) fn new(lag_budget: Duration, probe_interval: Duration, min: usize, max: usize) -> Self {
        Self {
            lag_budget,
            cooldown: lag_budget.max(probe_interval),
            min,
            max,
            slow_start: true,
            last_decrease: None,
        }
    }

    /// The new limit given the current limit, permits in use, whether demand exceeded the limit
    /// since the last sample, and a lag sample.
    pub(crate) fn next_limit(&mut self, limit: usize, in_flight: usize, saturated: bool, lag: Duration, now: Instant) -> usize {
        if lag > self.lag_budget {
            self.slow_start = false;
            // Work admitted before the last cut keeps the runtime busy until it drains; cutting
            // again before then would overshoot. Cut only once in-flight work fits the limit.
            let settled = in_flight <= limit;
            let cooled = self.last_decrease.is_none_or(|t| now - t >= self.cooldown);
            if settled && cooled && limit > self.min {
                self.last_decrease = Some(now);
                return ((limit as f64 * DECREASE_FACTOR) as usize).min(limit - 1).max(self.min);
            }
        } else if lag <= self.lag_budget / 2 && saturated && limit < self.max {
            let grown = if self.slow_start { limit * 2 } else { limit + 1 };
            return grown.min(self.max);
        }
        limit
    }
}

fn adjust_limit(controller: &mut Controller, limiter: &Limiter, lag: Duration, now: Instant) {
    let limit = limiter.limit();
    let next = controller.next_limit(limit, limiter.in_flight(), limiter.take_contended(), lag, now);
    if next != limit {
        if next < limit {
            debug!(?lag, from = limit, to = next, "scheduling lag over budget; reducing concurrency");
        }
        limiter.set_limit(next);
    }
}

fn renew_leases(
    config: &MonitorConfig,
    shared: &Monitored,
    conn: &mut Option<redis::Connection>,
    now: Instant,
) {
    let due: Vec<(String, String)> = {
        let mut leases = shared.leases.lock().unwrap();
        leases
            .iter_mut()
            .filter(|(_, next)| **next <= now)
            .map(|(key, next)| {
                *next = now + shared.heartbeat_period;
                key.clone()
            })
            .collect()
    };
    for key in due {
        let result = connection(config, conn).and_then(|c| {
            scripts::HEARTBEAT
                .key(&key.0)
                .arg(&config.group)
                .arg(&key.1)
                .arg(&config.consumer)
                .invoke::<i64>(c)
        });
        match result {
            Ok(1) => {}
            Ok(-1) => {
                // Acked between the heartbeat being scheduled and sent.
                shared.leases.lock().unwrap().remove(&key);
            }
            Ok(_) => {
                warn!(stream = key.0, id = key.1, "lease lost; another worker may run this task");
                shared.leases.lock().unwrap().remove(&key);
            }
            Err(e) => {
                warn!(stream = key.0, id = key.1, error = %e, "heartbeat failed; retrying");
                *conn = None;
                if let Some(next) = shared.leases.lock().unwrap().get_mut(&key) {
                    *next = now;
                }
            }
        }
    }
}

fn connection<'a>(
    config: &MonitorConfig,
    conn: &'a mut Option<redis::Connection>,
) -> redis::RedisResult<&'a mut redis::Connection> {
    if conn.is_none() {
        let c = config.client.get_connection_with_timeout(Duration::from_secs(1))?;
        c.set_read_timeout(Some(Duration::from_secs(1)))?;
        c.set_write_timeout(Some(Duration::from_secs(1)))?;
        *conn = Some(c);
    }
    Ok(conn.as_mut().expect("connection was just set"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);

    #[test]
    fn slow_start_doubles_until_lag_then_grows_linearly() {
        let mut c = Controller::new(50 * MS, 10 * MS, 1, 64);
        let t = Instant::now();
        assert_eq!(c.next_limit(4, 4, true, MS, t), 8);
        assert_eq!(c.next_limit(8, 8, true, MS, t), 16);
        assert_eq!(c.next_limit(16, 3, false, MS, t), 16, "no growth without demand");
        assert_eq!(c.next_limit(16, 16, true, 80 * MS, t), 11, "cut by 30% on lag");
        assert_eq!(c.next_limit(11, 11, true, MS, t), 12, "linear growth after the first cut");
        assert_eq!(c.next_limit(64, 64, true, MS, t), 64, "capped at max");
    }

    #[test]
    fn cuts_wait_for_cooldown_and_for_admitted_work_to_drain() {
        let mut c = Controller::new(50 * MS, 10 * MS, 2, 64);
        let t = Instant::now();
        assert_eq!(c.next_limit(32, 32, true, 80 * MS, t), 22);
        assert_eq!(c.next_limit(22, 22, true, 80 * MS, t + 10 * MS), 22, "within cooldown");
        assert_eq!(c.next_limit(22, 30, true, 80 * MS, t + 60 * MS), 22, "earlier admissions still draining");
        assert_eq!(c.next_limit(22, 22, true, 80 * MS, t + 60 * MS), 15);
        assert_eq!(c.next_limit(2, 2, true, 80 * MS, t + 200 * MS), 2, "never below min");
    }
}
