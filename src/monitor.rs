//! Per-worker monitor thread: measures runtime scheduling lag, adapts the concurrency limit to
//! it (pushback), and renews task leases.
//!
//! The monitor runs on its own OS thread with its own Redis connection, so lease renewal never
//! waits for a runtime thread. Even if processors block every runtime thread, leases stay alive.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tokio::runtime::Handle;
use tracing::{debug, warn};

use crate::control::{CancelReason, Liveness};
use crate::limiter::Limiter;
use crate::scripts;

/// Multiplier applied to the limit when lag exceeds its budget.
const DECREASE_FACTOR: f64 = 0.7;

/// Number of independently locked parts of the lease table, so tasks starting and finishing on
/// different threads rarely contend, and a renewal scan never locks every task at once.
const SHARDS: usize = 16;

/// Most leases renewed by one script call. One call per batch keeps renewal to one round trip
/// per tick at typical loads, while bounding how long a single script blocks Redis.
const RENEW_BATCH: usize = 128;

/// A lease-table key: the stream's index in [`Monitored`]'s stream list, and the entry id.
type LeaseKey = (u32, Box<str>);

type LeaseShard = Mutex<HashMap<LeaseKey, Lease>>;

fn lock(shard: &LeaseShard) -> std::sync::MutexGuard<'_, HashMap<LeaseKey, Lease>> {
    shard.lock().unwrap_or_else(PoisonError::into_inner)
}

/// State shared between a worker's tasks and its monitor thread.
pub(crate) struct Monitored {
    pub(crate) limiter: Arc<Limiter>,
    /// Most recent scheduling-lag sample, in microseconds.
    pub(crate) lag_us: AtomicU64,
    /// Longest wait between spawning a processor and its first poll since the last sample, in
    /// microseconds: how long admitted work queues for a runtime thread.
    start_delay_us: AtomicU64,
    /// The worker's stream keys, indexed by the stream part of a [`LeaseKey`].
    streams: Vec<String>,
    /// Claimed entries whose leases the monitor renews, spread across shards round-robin.
    leases: [LeaseShard; SHARDS],
    next_shard: AtomicUsize,
}

/// A claimed entry's lease, as tracked by the monitor.
struct Lease {
    /// When the next renewal is due.
    next_due: Instant,
    /// Time between renewals: a third of the lease.
    period: Duration,
    lease: Duration,
    liveness: Arc<Liveness>,
}

impl Monitored {
    /// `streams` are the worker's stream keys; leases name their stream by its index here.
    pub(crate) fn new(limiter: Arc<Limiter>, streams: Vec<String>) -> Arc<Self> {
        Arc::new(Self {
            limiter,
            lag_us: AtomicU64::new(0),
            start_delay_us: AtomicU64::new(0),
            streams,
            leases: std::array::from_fn(|_| Mutex::default()),
            next_shard: AtomicUsize::new(0),
        })
    }

    /// Current limit, in-flight count and latest lag sample.
    pub(crate) fn stats(&self) -> crate::worker::WorkerStats {
        crate::worker::WorkerStats {
            concurrency_limit: self.limiter.limit(),
            in_flight: self.limiter.in_flight(),
            scheduling_lag: Duration::from_micros(self.lag_us.load(Ordering::Acquire)),
        }
    }

    /// Asks every task holding a lease to stop.
    pub(crate) fn cancel_all(&self, reason: CancelReason) {
        for shard in &self.leases {
            for lease in lock(shard).values() {
                lease.liveness.cancel(reason);
            }
        }
    }

    /// Records how long a processor waited between being spawned and starting to run.
    pub(crate) fn record_start_delay(&self, delay: Duration) {
        self.start_delay_us.fetch_max(delay.as_micros() as u64, Ordering::AcqRel);
    }

    /// Starts renewing the lease on an entry until the returned guard is dropped. While the
    /// task's processor runs and uses heartbeats, renewal also requires recent beats.
    pub(crate) fn hold_lease(self: &Arc<Self>, stream: u32, id: &str, lease: Duration) -> LeaseGuard {
        let key: LeaseKey = (stream, id.into());
        let shard = self.next_shard.fetch_add(1, Ordering::Relaxed) % SHARDS;
        let liveness = Arc::new(Liveness::default());
        let period = (lease / 3).max(Duration::from_millis(10));
        let entry = Lease {
            next_due: Instant::now() + period,
            period,
            lease,
            liveness: liveness.clone(),
        };
        lock(&self.leases[shard]).insert(key.clone(), entry);
        LeaseGuard {
            monitored: self.clone(),
            shard,
            key,
            liveness,
        }
    }
}

pub(crate) struct LeaseGuard {
    monitored: Arc<Monitored>,
    shard: usize,
    key: LeaseKey,
    liveness: Arc<Liveness>,
}

impl LeaseGuard {
    pub(crate) fn liveness(&self) -> &Arc<Liveness> {
        &self.liveness
    }
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        lock(&self.monitored.leases[self.shard]).remove(&self.key);
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
    // Collect due leases one shard at a time, so tasks on other shards can start and finish.
    let mut due: Vec<(usize, LeaseKey)> = Vec::new();
    for (shard, leases) in shared.leases.iter().enumerate() {
        let mut leases = lock(leases);
        for (key, lease) in leases.iter_mut().filter(|(_, l)| l.next_due <= now) {
            lease.next_due = now + lease.period;
            // A processor that uses heartbeats and has gone silent for a whole lease is treated
            // as hung: stop renewing, so the lease expires even if this worker never recovers,
            // and abort it.
            if lease.liveness.silence().is_some_and(|s| s >= lease.lease) {
                let stream = &shared.streams[key.0 as usize];
                debug!(stream, id = &*key.1, "no heartbeat for a whole lease; not renewing");
                lease.liveness.abort();
            } else {
                due.push((shard, key.clone()));
            }
        }
    }
    for (n, batch) in due.chunks(RENEW_BATCH).enumerate() {
        let result = connection(config, conn).and_then(|c| {
            let mut invocation = scripts::HEARTBEAT.prepare_invoke();
            invocation.arg(&config.group).arg(&config.consumer);
            for (_, (stream, id)) in batch {
                invocation.key(&shared.streams[*stream as usize]).arg(&**id);
            }
            invocation.invoke::<Vec<i64>>(c)
        });
        match result {
            Ok(statuses) => {
                for ((shard, key), status) in batch.iter().zip(statuses) {
                    if status == 1 {
                        continue;
                    }
                    let removed = lock(&shared.leases[*shard]).remove(key);
                    // -1: acked between the renewal being scheduled and sent.
                    if status != -1 {
                        let stream = &shared.streams[key.0 as usize];
                        warn!(stream, id = &*key.1, "lease lost; another worker may run this task");
                        crate::telemetry::lease_lost(stream);
                        if let Some(lease) = removed {
                            lease.liveness.cancel(CancelReason::LeaseLost);
                        }
                    }
                }
            }
            Err(e) => {
                // Retry this and the remaining renewals on the next tick, rather than waiting out a
                // connection timeout for each batch now.
                warn!(error = %e, leases = due.len() - n * RENEW_BATCH, "lease renewal failed; retrying");
                *conn = None;
                for (shard, key) in &due[n * RENEW_BATCH..] {
                    if let Some(lease) = lock(&shared.leases[*shard]).get_mut(key) {
                        lease.next_due = now;
                    }
                }
                return;
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
