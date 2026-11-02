use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use redis::streams::{StreamAutoClaimReply, StreamId, StreamPendingCountReply, StreamReadOptions, StreamReadReply};
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::client::TaskQueue;
use crate::error::{Error, Result};
use crate::keys::Keys;
use crate::control::{CancelReason, Liveness, TaskControl};
use crate::limiter::{Limiter, Permit};
use crate::monitor::{LeaseGuard, Monitor, MonitorConfig, Monitored};
use crate::registry::{HandlerError, Registry};
use crate::scripts;
use crate::task::{ChainLink, PAYLOAD_FIELD, TaskEnvelope, TaskId, now_millis};
use crate::telemetry::{self, ChainEvent, ClaimSource, Finished};

/// Shortest lease a worker accepts. Leases are renewed every third of a lease from a thread that
/// wakes every few milliseconds, so much shorter leases would expire between renewals.
pub const MIN_LEASE: Duration = Duration::from_millis(100);

/// Tuning knobs for a [`Worker`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct WorkerConfig {
    /// Maximum number of tasks processed at once by this worker. With adaptive concurrency the
    /// effective limit moves between `min_concurrency` and this value.
    pub concurrency: usize,
    /// Lower bound for the adaptive concurrency limit.
    pub min_concurrency: usize,
    /// Whether to lower the concurrency limit when the runtime falls behind (pushback), and raise
    /// it again as the runtime recovers.
    pub adaptive_concurrency: bool,
    /// Scheduling lag above which the concurrency limit is lowered: how long a ready task may
    /// wait for a runtime thread. Defaults to a tenth of the lease, clamped to 5–100 ms.
    pub max_scheduling_lag: Option<Duration>,
    /// How often the monitor thread samples scheduling lag.
    pub probe_interval: Duration,
    /// Maximum number of entries claimed per read.
    pub batch_size: usize,
    /// How long a read blocks waiting for new entries.
    pub block: Duration,
    /// How long a claimed task may go without its lease being renewed before other workers may
    /// reclaim it. Processor types can override it with [`Registry::set_lease`]. At least
    /// [`MIN_LEASE`].
    pub lease: Duration,
    /// Total number of times a task is run before it is dead-lettered.
    pub max_attempts: u32,
    /// Retry delay after the first failure; doubled for each further failure.
    pub backoff_base: Duration,
    /// Upper bound on the retry delay.
    pub backoff_max: Duration,
    /// How often due retries are moved back onto their streams.
    pub promote_interval: Duration,
    /// Approximate cap on the dead-letter stream's length.
    pub dead_letter_max_len: usize,
    /// Consumer name within the group. Must be unique per running worker.
    pub consumer_name: String,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            concurrency: 16,
            min_concurrency: 1,
            adaptive_concurrency: true,
            max_scheduling_lag: None,
            probe_interval: Duration::from_millis(10),
            batch_size: 16,
            block: Duration::from_secs(1),
            lease: Duration::from_secs(30),
            max_attempts: 5,
            backoff_base: Duration::from_secs(1),
            backoff_max: Duration::from_secs(300),
            promote_interval: Duration::from_millis(500),
            dead_letter_max_len: 100_000,
            consumer_name: format!("worker-{}", Uuid::new_v4()),
        }
    }
}

/// Longest duration any setting accepts, so deadline arithmetic and conversions to Redis
/// milliseconds can't overflow.
const MAX_SETTING: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// Shortest `promote_interval`; shorter ones make the retry loop hammer Redis.
const MIN_PROMOTE_INTERVAL: Duration = Duration::from_millis(10);

impl WorkerConfig {
    /// The effective scheduling-lag budget.
    pub fn lag_budget(&self) -> Duration {
        self.max_scheduling_lag.unwrap_or_else(|| {
            (self.lease / 10).clamp(Duration::from_millis(5), Duration::from_millis(100))
        })
    }

    /// Checks that the settings are safe to run with. [`Worker::spawn`] calls this, and also
    /// checks the leases set per processor type.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidConfig`] naming the first bad setting, for example a zero `block`
    /// (Redis's "block forever"), a lease below [`MIN_LEASE`] or not at least ten probe
    /// intervals long, or `min_concurrency` above `concurrency`.
    pub fn validate(&self) -> Result<()> {
        let invalid = |msg: String| Err(Error::InvalidConfig(msg));
        if self.concurrency == 0 || self.min_concurrency == 0 {
            return invalid("concurrency and min_concurrency must be at least 1".into());
        }
        if self.min_concurrency > self.concurrency {
            return invalid(format!(
                "min_concurrency ({}) is above concurrency ({})",
                self.min_concurrency, self.concurrency
            ));
        }
        if self.batch_size == 0 {
            return invalid("batch_size must be at least 1".into());
        }
        if self.max_attempts == 0 {
            return invalid("max_attempts must be at least 1".into());
        }
        if self.consumer_name.is_empty() {
            return invalid("consumer_name is empty".into());
        }
        let durations = [
            ("probe_interval", self.probe_interval),
            ("block", self.block),
            ("lease", self.lease),
            ("backoff_base", self.backoff_base),
            ("backoff_max", self.backoff_max),
            ("promote_interval", self.promote_interval),
        ];
        let lag = self.max_scheduling_lag.map(|d| ("max_scheduling_lag", d));
        if let Some((name, d)) = durations.into_iter().chain(lag).find(|(_, d)| *d > MAX_SETTING) {
            return invalid(format!("{name} is {d:?}; the maximum is {MAX_SETTING:?}"));
        }
        if self.probe_interval.is_zero() {
            return invalid("probe_interval must be above zero".into());
        }
        if self.block.is_zero() {
            return invalid("block must be above zero; Redis treats 0 as \"block forever\"".into());
        }
        if self.max_scheduling_lag.is_some_and(|d| d.is_zero()) {
            return invalid("max_scheduling_lag must be above zero".into());
        }
        if self.promote_interval < MIN_PROMOTE_INTERVAL {
            return invalid(format!(
                "promote_interval is {:?}; the minimum is {MIN_PROMOTE_INTERVAL:?}",
                self.promote_interval
            ));
        }
        if self.backoff_base > self.backoff_max {
            return invalid(format!(
                "backoff_base ({:?}) is above backoff_max ({:?})",
                self.backoff_base, self.backoff_max
            ));
        }
        check_lease("the worker", self.lease, self.probe_interval)
    }
}

/// A lease must outlast several renewal rounds: at least [`MIN_LEASE`], and at least ten probe
/// intervals since leases are renewed every third of a lease, checked once per probe.
fn check_lease(what: &str, lease: Duration, probe_interval: Duration) -> Result<()> {
    let minimum = MIN_LEASE.max(probe_interval.saturating_mul(10));
    if lease < minimum {
        return Err(Error::InvalidConfig(format!(
            "{what} lease is {lease:?}; the minimum is {minimum:?}"
        )));
    }
    if lease > MAX_SETTING {
        return Err(Error::InvalidConfig(format!(
            "{what} lease is {lease:?}; the maximum is {MAX_SETTING:?}"
        )));
    }
    Ok(())
}

/// A snapshot of a worker's load, from [`WorkerHandle::stats`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct WorkerStats {
    /// Current concurrency limit; below the configured maximum while pushback is active.
    pub concurrency_limit: usize,
    /// Tasks currently holding a concurrency permit.
    pub in_flight: usize,
    /// Most recent scheduling-lag sample.
    pub scheduling_lag: Duration,
}

/// Builds and starts a background worker that scans for tasks and dispatches them to the
/// processors in a [`Registry`].
#[derive(Debug)]
pub struct Worker {
    queue: TaskQueue,
    registry: Registry,
    config: WorkerConfig,
}

impl Worker {
    /// Starts configuring a worker that runs `registry`'s processors on tasks from `queue`,
    /// with [`WorkerConfig::default`] settings.
    pub fn builder(queue: TaskQueue, registry: Registry) -> Self {
        Self {
            queue,
            registry,
            config: WorkerConfig::default(),
        }
    }

    /// Replaces every setting at once.
    pub fn config(mut self, config: WorkerConfig) -> Self {
        self.config = config;
        self
    }

    /// Sets [`WorkerConfig::concurrency`]; at least 1.
    pub fn concurrency(mut self, n: usize) -> Self {
        self.config.concurrency = n.max(1);
        self
    }

    /// Sets [`WorkerConfig::min_concurrency`]; at least 1.
    pub fn min_concurrency(mut self, n: usize) -> Self {
        self.config.min_concurrency = n.max(1);
        self
    }

    /// Enables or disables pushback (adaptive concurrency). Enabled by default.
    pub fn adaptive_concurrency(mut self, enabled: bool) -> Self {
        self.config.adaptive_concurrency = enabled;
        self
    }

    /// Sets [`WorkerConfig::max_scheduling_lag`], the lag that triggers pushback.
    pub fn max_scheduling_lag(mut self, d: Duration) -> Self {
        self.config.max_scheduling_lag = Some(d);
        self
    }

    /// Sets [`WorkerConfig::batch_size`]; at least 1.
    pub fn batch_size(mut self, n: usize) -> Self {
        self.config.batch_size = n.max(1);
        self
    }

    /// Sets [`WorkerConfig::block`]. Shorter blocks make shutdown quicker.
    pub fn block(mut self, d: Duration) -> Self {
        self.config.block = d;
        self
    }

    /// Sets [`WorkerConfig::lease`], checked against [`MIN_LEASE`] when the worker spawns.
    pub fn lease(mut self, d: Duration) -> Self {
        self.config.lease = d;
        self
    }

    /// Sets [`WorkerConfig::max_attempts`]; at least 1.
    pub fn max_attempts(mut self, n: u32) -> Self {
        self.config.max_attempts = n.max(1);
        self
    }

    /// Sets the retry delay after the first failure and its upper bound.
    pub fn backoff(mut self, base: Duration, max: Duration) -> Self {
        self.config.backoff_base = base;
        self.config.backoff_max = max;
        self
    }

    /// Sets [`WorkerConfig::promote_interval`]: how soon a due retry runs.
    pub fn promote_interval(mut self, d: Duration) -> Self {
        self.config.promote_interval = d;
        self
    }

    /// How often the monitor thread samples scheduling lag and checks for leases to renew.
    pub fn probe_interval(mut self, d: Duration) -> Self {
        self.config.probe_interval = d.max(Duration::from_millis(1));
        self
    }

    /// Approximate cap on the dead-letter stream's length.
    pub fn dead_letter_max_len(mut self, n: usize) -> Self {
        self.config.dead_letter_max_len = n;
        self
    }

    /// Sets [`WorkerConfig::consumer_name`]. Defaults to `worker-<uuid>`.
    pub fn consumer_name(mut self, name: impl Into<String>) -> Self {
        self.config.consumer_name = name.into();
        self
    }

    /// Creates the consumer groups and starts the background loops.
    ///
    /// Dropping the returned handle signals shutdown without waiting; call
    /// [`WorkerHandle::shutdown`] to wait for in-flight tasks to finish.
    ///
    /// # Errors
    ///
    /// [`Error::NoProcessors`] for an empty registry, [`Error::InvalidConfig`] as for
    /// [`WorkerConfig::validate`] or for a per-type lease that is too short, [`Error::Thread`]
    /// if the monitor thread can't start, and [`Error::Redis`] if Redis can't be reached.
    pub async fn spawn(self) -> Result<WorkerHandle> {
        if self.registry.is_empty() {
            return Err(Error::NoProcessors);
        }
        self.config.validate()?;
        let mut conn = self.queue.conn.clone();
        warn_if_evicting(&mut conn).await;

        // Each processor type's stream, plus its 0.1 stream while one exists, so tasks written
        // before an upgrade are still run. Those are finalized in the 0.1 layout.
        let keys = self.queue.keys.clone();
        let legacy = Arc::new(keys.legacy());
        let mut layouts: Vec<(String, String, Arc<Keys>)> = Vec::new();
        for processor_type in self.registry.processor_types() {
            layouts.push((keys.stream(processor_type), processor_type.to_owned(), keys.clone()));
            let old = legacy.stream(processor_type);
            if conn.exists::<_, bool>(&old).await? {
                info!(stream = old, "reading a 0.1 stream until it is drained");
                layouts.push((old, processor_type.to_owned(), legacy.clone()));
            }
        }
        layouts.sort_by(|a, b| a.0.cmp(&b.0));
        let legacy_retries = layouts.iter().any(|(_, _, k)| k.is_legacy())
            || conn.exists::<_, bool>(legacy.delayed("")).await?;
        let streams: Vec<String> = layouts.iter().map(|(s, _, _)| s.clone()).collect();
        let per_stream: HashMap<String, StreamInfo> = layouts
            .into_iter()
            .enumerate()
            .map(|(index, (stream, processor_type, keys))| {
                // Each stream's lease: its processor type's, or the worker's.
                let lease = self.registry.get(&processor_type).and_then(|p| p.lease);
                let info = StreamInfo {
                    index: u32::try_from(index).expect("fewer than 2^32 streams"),
                    metrics: telemetry::handle(self.queue.namespace(), &processor_type),
                    lease: lease.unwrap_or(self.config.lease),
                    delayed: keys.delayed(&processor_type),
                    keys,
                };
                (stream, info)
            })
            .collect();
        for (stream, info) in &per_stream {
            check_lease(stream, info.lease, self.config.probe_interval)?;
        }

        let group = self.queue.group_name();
        for stream in &streams {
            create_group(&mut conn, stream, &group).await?;
        }
        let blocking = self.queue.blocking_connection(self.config.block).await?;

        let config = self.config;
        // With pushback, start at the runtime's thread count and let slow start find the limit.
        let initial = if config.adaptive_concurrency {
            let threads = tokio::runtime::Handle::current().metrics().num_workers();
            threads.clamp(config.min_concurrency, config.concurrency.max(config.min_concurrency))
        } else {
            config.concurrency
        };
        let monitored = Monitored::new(Limiter::new(initial), streams.clone());
        let monitor = Monitor::spawn(
            MonitorConfig {
                client: self.queue.client.clone(),
                group: group.clone(),
                consumer: config.consumer_name.clone(),
                probe_interval: config.probe_interval,
                lag_budget: config.lag_budget(),
                adaptive: config.adaptive_concurrency,
                min_concurrency: config.min_concurrency.min(config.concurrency),
                max_concurrency: config.concurrency,
            },
            &monitored,
            tokio::runtime::Handle::current(),
        )
        .map_err(Error::Thread)?;
        let stats = Arc::downgrade(&monitored);
        telemetry::register_worker(&config.consumer_name, stats.clone());
        let inner = Arc::new(Inner {
            monitored,
            in_flight: AtomicUsize::new(0),
            idle: Notify::new(),
            group,
            streams,
            per_stream,
            dead: keys.dead(),
            legacy_retries,
            queue: self.queue,
            registry: self.registry,
            config,
        });

        let (tx, rx) = watch::channel(false);
        #[allow(unused_mut)]
        let mut loops = vec![
            tokio::spawn(fetch_loop(inner.clone(), blocking, rx.clone())),
            tokio::spawn(reclaim_loop(inner.clone(), rx.clone())),
            tokio::spawn(promote_loop(inner.clone(), rx.clone())),
        ];
        #[cfg(feature = "otel")]
        loops.push(tokio::spawn(sample_queues(inner.clone(), rx)));
        let supervisor_inner = inner.clone();
        let join = tokio::spawn(async move {
            for handle in loops {
                if let Err(e) = handle.await {
                    error!(error = %e, "worker loop panicked");
                }
            }
            // Nothing new is claimed now; ask processors still running to stop.
            supervisor_inner.monitored.cancel_all(CancelReason::Shutdown);
            supervisor_inner.wait_idle().await;
            remove_consumer(&supervisor_inner).await;
            // In-flight tasks are done, so no leases remain to renew.
            let _ = tokio::task::spawn_blocking(move || monitor.stop()).await;
        });

        Ok(WorkerHandle {
            shutdown: tx,
            join,
            consumer_name: inner.config.consumer_name.clone(),
            stats,
        })
    }
}

/// Controls a running worker.
pub struct WorkerHandle {
    shutdown: watch::Sender<bool>,
    join: JoinHandle<()>,
    consumer_name: String,
    stats: Weak<Monitored>,
}

impl std::fmt::Debug for WorkerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerHandle")
            .field("consumer_name", &self.consumer_name)
            .field("running", &!self.join.is_finished())
            .finish_non_exhaustive()
    }
}

impl WorkerHandle {
    /// The worker's consumer name within the group.
    pub fn consumer_name(&self) -> &str {
        &self.consumer_name
    }

    /// Current concurrency limit, in-flight count and scheduling lag. All zero once the worker
    /// has stopped.
    pub fn stats(&self) -> WorkerStats {
        self.stats.upgrade().map(|m| m.stats()).unwrap_or_default()
    }

    /// Stops claiming new tasks and waits for in-flight tasks to finish.
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        if let Err(e) = self.join.await {
            error!(error = %e, "worker supervisor panicked");
        }
    }
}

/// What a worker knows about one of its streams.
struct StreamInfo {
    /// The stream's position in the worker's sorted stream list, which names it in the lease table.
    index: u32,
    /// Metrics for the stream's processor type.
    metrics: telemetry::TypeHandle,
    /// Lease for the stream's tasks: its processor type's, or the worker's.
    lease: Duration,
    /// The key layout the stream belongs to: current, or 0.1 while its data drains. A task
    /// is finalized in the layout it was claimed from.
    keys: Arc<Keys>,
    /// Where the stream's retries wait: its processor type's delayed set.
    delayed: String,
}

struct Inner {
    queue: TaskQueue,
    registry: Registry,
    config: WorkerConfig,
    group: String,
    streams: Vec<String>,
    /// Metrics, lease and key layout for each stream, resolved once at spawn.
    per_stream: HashMap<String, StreamInfo>,
    /// The dead-letter stream, shared by both layouts.
    dead: String,
    /// Whether 0.1 retries may exist, so the 0.1 delayed set must be promoted too.
    legacy_retries: bool,
    /// Concurrency limiter, renewed leases and lag stats, shared with the monitor thread.
    monitored: Arc<Monitored>,
    in_flight: AtomicUsize,
    idle: Notify,
}

impl Inner {
    fn consumer(&self) -> &str {
        &self.config.consumer_name
    }

    /// Metrics for the processor type of one of this worker's streams.
    fn metrics(&self, stream: &str) -> &telemetry::TypeHandle {
        &self.per_stream[stream].metrics
    }

    fn lease(&self, stream: &str) -> Duration {
        self.per_stream[stream].lease
    }

    fn limiter(&self) -> &Arc<Limiter> {
        &self.monitored.limiter
    }

    /// Takes one permit, waiting if necessary, then as many more as are free up to `max`.
    /// Waits first while the monitor is unhealthy, since claimed tasks' leases might not be
    /// renewed. Returns `None` if shutdown was signalled while waiting.
    async fn acquire_batch(
        &self,
        max: usize,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Option<Vec<Permit>> {
        while !self.monitored.healthy() {
            tokio::select! {
                _ = tokio::time::sleep(self.config.probe_interval) => {}
                _ = shutdown.changed() => return None,
            }
        }
        let first = tokio::select! {
            p = self.limiter().acquire() => p,
            _ = shutdown.changed() => return None,
        };
        let mut permits = vec![first];
        while permits.len() < max {
            match self.limiter().try_acquire() {
                Some(p) => permits.push(p),
                None => break,
            }
        }
        Some(permits)
    }

    async fn wait_idle(&self) {
        loop {
            let notified = self.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.in_flight.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }
}

/// Counts a task as in flight from dispatch until it is finalized.
struct InFlight(Arc<Inner>);

impl InFlight {
    fn new(inner: &Arc<Inner>) -> Self {
        inner.in_flight.fetch_add(1, Ordering::AcqRel);
        Self(inner.clone())
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        if self.0.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.idle.notify_waiters();
        }
    }
}

fn stopping(shutdown: &watch::Receiver<bool>) -> bool {
    *shutdown.borrow() || shutdown.has_changed().is_err()
}

/// Sleeps for `d`, returning `true` early if shutdown is signalled.
async fn sleep_or_stop(shutdown: &mut watch::Receiver<bool>, d: Duration) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(d) => stopping(shutdown),
        _ = shutdown.changed() => true,
    }
}

async fn create_group(conn: &mut ConnectionManager, stream: &str, group: &str) -> Result<()> {
    // Start at 0 so tasks submitted before any worker existed are still delivered.
    let res: redis::RedisResult<()> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(stream)
        .arg(group)
        .arg("0")
        .arg("MKSTREAM")
        .query_async(conn)
        .await;
    match res {
        Err(e) if e.code() != Some("BUSYGROUP") => Err(e.into()),
        _ => Ok(()),
    }
}

/// Removes this worker's consumer from each stream's group once nothing is in flight, so
/// restarts with fresh consumer names don't leave stale consumers behind. A stream where the
/// consumer still has pending entries, because a finalize failed, keeps it, so other workers
/// can reclaim them.
async fn remove_consumer(inner: &Inner) {
    let mut conn = inner.queue.conn.clone();
    for stream in &inner.streams {
        let removed: redis::RedisResult<i64> = scripts::REMOVE_CONSUMER
            .key(stream)
            .arg(&inner.group)
            .arg(inner.consumer())
            .invoke_async(&mut conn)
            .await;
        match removed {
            Ok(1) => {}
            Ok(_) => debug!(stream, "consumer still has pending entries; leaving it in the group"),
            Err(e) => debug!(stream, error = %e, "failed to remove consumer from the group"),
        }
    }
}

/// Warns if Redis may evict keys under memory pressure, which would silently delete queued
/// tasks and chain state. Best-effort: managed services often disable `CONFIG`.
async fn warn_if_evicting(conn: &mut ConnectionManager) {
    let reply: redis::RedisResult<HashMap<String, String>> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("maxmemory")
        .arg("maxmemory-policy")
        .query_async(conn)
        .await;
    match reply {
        Ok(config) => {
            let get = |k: &str| config.get(k).map_or("", String::as_str);
            let (maxmemory, policy) = (get("maxmemory"), get("maxmemory-policy"));
            if may_evict(maxmemory, policy) {
                warn!(
                    maxmemory,
                    policy,
                    "Redis may evict keys when full, which loses queued tasks; \
                     set maxmemory-policy to noeviction"
                );
            }
        }
        Err(e) => debug!(error = %e, "could not read the Redis eviction policy"),
    }
}

/// Whether a Redis with these settings may evict keys: it has a memory limit and a policy other
/// than `noeviction`. `volatile-*` policies are unsafe too, since chain state has a TTL.
fn may_evict(maxmemory: &str, policy: &str) -> bool {
    let limited = !maxmemory.is_empty() && maxmemory != "0";
    limited && !policy.is_empty() && policy != "noeviction"
}

/// Starts processing a claimed entry. `deliveries` is how many times Redis has delivered it,
/// counting this delivery.
fn dispatch(
    inner: &Arc<Inner>,
    stream: String,
    entry: StreamId,
    permit: Option<Permit>,
    deliveries: u64,
    info: &StreamInfo,
) {
    let guard = InFlight::new(inner);
    let dispatched_at = std::time::Instant::now();
    // Start renewing the lease now, at claim time: the task may wait in the run queue for longer
    // than the lease when the runtime is saturated.
    let lease = inner.monitored.hold_lease(info.index, &entry.id, info.lease);
    let claim = Claim { stream, entry, deliveries };
    tokio::spawn(process(inner.clone(), claim, permit, guard, dispatched_at, lease));
}

async fn fetch_loop(
    inner: Arc<Inner>,
    mut conn: ConnectionManager,
    mut shutdown: watch::Receiver<bool>,
) {
    let ids = vec![">"; inner.streams.len()];
    let block_ms = inner.config.block.as_millis() as usize;
    while !stopping(&shutdown) {
        let Some(mut permits) = inner.acquire_batch(inner.config.batch_size, &mut shutdown).await
        else {
            break;
        };
        let opts = StreamReadOptions::default()
            .group(&inner.group, inner.consumer())
            .count(permits.len())
            .block(block_ms);
        let reply: redis::RedisResult<Option<StreamReadReply>> =
            redis::AsyncCommands::xread_options(&mut conn, &inner.streams, &ids, &opts).await;
        match reply {
            Ok(Some(reply)) => {
                // Work filled every permit we could get, and we couldn't get a full batch: demand
                // exceeds the limit, which lets pushback raise it while the runtime keeps up.
                let entries: usize = reply.keys.iter().map(|k| k.ids.len()).sum();
                if entries >= permits.len() && permits.len() < inner.config.batch_size {
                    inner.limiter().mark_contended();
                }
                for key in reply.keys {
                    for entry in key.ids {
                        // COUNT applies per stream, so a multi-stream read can return more
                        // entries than permits; the excess waits for a permit inside `process`.
                        let info = &inner.per_stream[&key.key];
                        info.metrics.claimed(ClaimSource::Fetch);
                        dispatch(&inner, key.key.clone(), entry, permits.pop(), 1, info);
                    }
                }
            }
            Ok(None) => {}
            Err(e) => {
                warn!(error = %e, "failed to read tasks");
                drop(permits);
                if sleep_or_stop(&mut shutdown, Duration::from_secs(1)).await {
                    break;
                }
            }
        }
    }
}

/// Most `XAUTOCLAIM` calls per stream per reclaim pass. Each call examines at most ten times its
/// COUNT pending entries, so one pass covers up to `MAX_RECLAIM_CALLS * 10 * batch_size` of them;
/// the next pass continues from where this one stopped.
const MAX_RECLAIM_CALLS: usize = 16;

/// Reclaims tasks whose leases expired, scanning each stream every half of that stream's lease.
async fn reclaim_loop(inner: Arc<Inner>, mut shutdown: watch::Receiver<bool>) {
    let mut conn = inner.queue.conn.clone();
    let interval = |stream: &str| (inner.lease(stream) / 2).max(Duration::from_millis(50));
    let start = tokio::time::Instant::now();
    // Per stream: when its next pass is due, and where the last pass stopped in the pending list.
    let mut state: Vec<(tokio::time::Instant, String)> = inner
        .streams
        .iter()
        .map(|s| (start + interval(s), "0-0".to_owned()))
        .collect();
    loop {
        let next = state.iter().map(|(due, _)| *due).min().expect("a worker has at least one stream");
        tokio::select! {
            _ = tokio::time::sleep_until(next) => {}
            _ = shutdown.changed() => return,
        }
        if stopping(&shutdown) {
            return;
        }
        let now = tokio::time::Instant::now();
        for (stream, (due, cursor)) in inner.streams.iter().zip(state.iter_mut()) {
            if *due > now {
                continue;
            }
            *due = now + interval(stream);
            if !reclaim_stream(&inner, &mut conn, stream, cursor, &mut shutdown).await {
                return;
            }
        }
    }
}

/// One pass over a stream's pending entries, resuming from `cursor`. Returns `false` if shutdown
/// was signalled.
async fn reclaim_stream(
    inner: &Arc<Inner>,
    conn: &mut ConnectionManager,
    stream: &str,
    cursor: &mut String,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    let info = &inner.per_stream[stream];
    let min_idle = info.lease.as_millis() as u64;
    for _ in 0..MAX_RECLAIM_CALLS {
        let Some(mut permits) = inner.acquire_batch(inner.config.batch_size, shutdown).await else {
            return false;
        };
        let reply: redis::RedisResult<StreamAutoClaimReply> = redis::cmd("XAUTOCLAIM")
            .arg(stream)
            .arg(&inner.group)
            .arg(inner.consumer())
            .arg(min_idle)
            .arg(cursor.as_str())
            .arg("COUNT")
            .arg(permits.len())
            .query_async(conn)
            .await;
        let reply = match reply {
            Ok(reply) => reply,
            Err(e) => {
                warn!(error = %e, stream, "failed to reclaim tasks");
                return true;
            }
        };
        let deliveries = delivery_counts(inner, conn, stream, &reply.claimed).await;
        for (entry, deliveries) in reply.claimed.into_iter().zip(deliveries) {
            debug!(stream, id = %entry.id, deliveries, "reclaimed expired task");
            info.metrics.claimed(ClaimSource::Reclaim);
            dispatch(inner, stream.to_owned(), entry, permits.pop(), deliveries, info);
        }
        // XAUTOCLAIM returns 0-0 once it has scanned to the end of the pending list.
        *cursor = reply.next_stream_id;
        if cursor == "0-0" {
            break;
        }
    }
    true
}

/// How many times Redis has delivered each claimed entry, counting this claim. If the counts
/// can't be read, each entry counts as delivered once.
async fn delivery_counts(
    inner: &Inner,
    conn: &mut ConnectionManager,
    stream: &str,
    claimed: &[StreamId],
) -> Vec<u64> {
    if claimed.is_empty() {
        return Vec::new();
    }
    let mut pipe = redis::pipe();
    for entry in claimed {
        pipe.cmd("XPENDING").arg(stream).arg(&inner.group).arg(&entry.id).arg(&entry.id).arg(1);
    }
    match pipe.query_async::<Vec<StreamPendingCountReply>>(conn).await {
        Ok(replies) => replies
            .iter()
            .map(|r| r.ids.first().map_or(1, |p| p.times_delivered as u64))
            .collect(),
        Err(e) => {
            warn!(error = %e, stream, "failed to read delivery counts");
            vec![1; claimed.len()]
        }
    }
}

/// Every few seconds, samples each stream's length and pending count, and the number of tasks
/// waiting for a retry, for the queue gauges.
#[cfg(feature = "otel")]
async fn sample_queues(inner: Arc<Inner>, mut shutdown: watch::Receiver<bool>) {
    const INTERVAL: Duration = Duration::from_secs(5);
    let mut conn = inner.queue.conn.clone();
    // Every retry set this worker's streams use: one per type, plus the 0.1 shared set.
    let delayed_keys: std::collections::BTreeSet<&str> =
        inner.per_stream.values().map(|info| info.delayed.as_str()).collect();
    loop {
        let mut pipe = redis::pipe();
        for stream in &inner.streams {
            pipe.xlen(stream).cmd("XPENDING").arg(stream).arg(&inner.group);
        }
        for key in &delayed_keys {
            pipe.zcard(*key);
        }
        let reply: redis::RedisResult<Vec<redis::Value>> = pipe.query_async(&mut conn).await;
        match reply {
            Ok(values) => {
                for (stream, pair) in inner.streams.iter().zip(values.chunks(2)) {
                    let depth: u64 = redis::from_redis_value_ref(&pair[0]).unwrap_or(0);
                    // XPENDING's summary form starts with the total number of pending entries.
                    let pending: u64 = redis::from_redis_value_ref::<Vec<redis::Value>>(&pair[1])
                        .ok()
                        .and_then(|summary| summary.first().and_then(|v| redis::from_redis_value_ref(v).ok()))
                        .unwrap_or(0);
                    telemetry::record_queue(stream, depth, pending);
                }
                for (set, value) in delayed_keys.iter().zip(&values[inner.streams.len() * 2..]) {
                    if let Ok(delayed) = redis::from_redis_value_ref::<u64>(value) {
                        telemetry::record_delayed(set, delayed);
                    }
                }
            }
            Err(e) => debug!(error = %e, "failed to sample queue sizes"),
        }
        if sleep_or_stop(&mut shutdown, INTERVAL).await {
            return;
        }
    }
}

async fn promote_loop(inner: Arc<Inner>, mut shutdown: watch::Receiver<bool>) {
    const LIMIT: usize = 100;
    let mut conn = inner.queue.conn.clone();
    let legacy = inner.queue.keys.legacy();
    // Each current-layout stream's retries, promoted by this worker because it runs that type.
    let targets: Vec<(&str, &str)> = inner
        .streams
        .iter()
        .map(|s| (s.as_str(), &inner.per_stream[s]))
        .filter(|(_, info)| !info.keys.is_legacy())
        .map(|(s, info)| (info.delayed.as_str(), s))
        .collect();
    while !sleep_or_stop(&mut shutdown, inner.config.promote_interval).await {
        for &(delayed, stream) in &targets {
            let promote = || {
                let mut invocation = scripts::PROMOTE.prepare_invoke();
                invocation.key(delayed).key(stream).arg(LIMIT).arg(PAYLOAD_FIELD);
                invocation
            };
            promote_all(&mut conn, promote, LIMIT).await;
        }
        if inner.legacy_retries {
            let promote = || {
                let mut invocation = scripts::LEGACY_PROMOTE.prepare_invoke();
                invocation
                    .key(legacy.delayed(""))
                    .key(&inner.dead)
                    .arg(legacy.stream_prefix())
                    .arg(LIMIT)
                    .arg(PAYLOAD_FIELD)
                    .arg(inner.config.dead_letter_max_len);
                invocation
            };
            promote_all(&mut conn, promote, LIMIT).await;
        }
    }
}

/// Runs a promotion script until it moves fewer than `limit` retries, or fails.
async fn promote_all<'a>(
    conn: &mut ConnectionManager,
    invocation: impl Fn() -> redis::ScriptInvocation<'a>,
    limit: usize,
) {
    loop {
        match invocation().invoke_async::<usize>(conn).await {
            Ok(n) if n >= limit => continue,
            Ok(_) => return,
            Err(e) => {
                warn!(error = %e, "failed to promote delayed tasks");
                return;
            }
        }
    }
}

enum Outcome {
    Succeeded(Box<serde_json::value::RawValue>),
    Failed(String),
    /// Not retryable: dead-letter immediately.
    Fatal(String),
    /// Given back because the worker is shutting down: requeue at once, without using up an
    /// attempt.
    Released(String),
}

/// A claimed stream entry.
struct Claim {
    stream: String,
    entry: StreamId,
    /// How many times Redis has delivered the entry, counting this delivery.
    deliveries: u64,
}

async fn process(
    inner: Arc<Inner>,
    claim: Claim,
    permit: Option<Permit>,
    _guard: InFlight,
    dispatched_at: std::time::Instant,
    lease: LeaseGuard,
) {
    // How long this task sat in the runtime queue: a direct measure of scheduling lag.
    inner.monitored.record_start_delay(dispatched_at.elapsed());
    // `lease`, taken at claim time, keeps the lease renewed until this function returns: while
    // queued, waiting for a permit, running, and recording the outcome.
    let _permit = match permit {
        Some(p) => p,
        None => inner.limiter().acquire().await,
    };

    let Claim { stream, entry, deliveries } = claim;
    let id = &entry.id;
    let metrics = inner.metrics(&stream);
    let payload: String = entry.get(PAYLOAD_FIELD).unwrap_or_default();
    let (finalized, report) = match serde_json::from_str::<TaskEnvelope>(&payload) {
        Ok(mut envelope) => {
            // Earlier deliveries of this entry never recorded an outcome: their workers crashed or
            // stalled, possibly because of this task. Count them as failed attempts, so a task
            // that keeps crashing workers is dead-lettered instead of retried forever.
            let abandoned = u32::try_from(deliveries.saturating_sub(1)).unwrap_or(u32::MAX);
            envelope.attempt = envelope.attempt.saturating_add(abandoned);
            let since_submit = now_millis().saturating_sub(envelope.enqueued_at);
            let queue_time = Duration::from_millis(since_submit);
            let started = std::time::Instant::now();
            let outcome = if abandoned > 0 && envelope.attempt >= inner.config.max_attempts {
                error!(stream, id, deliveries, "dead-lettering task whose deliveries never finished");
                Outcome::Fatal(format!(
                    "delivered {deliveries} times without an outcome being recorded; workers \
                     crashed or stalled while running it"
                ))
            } else if lease.liveness().cancel_reason() == Some(CancelReason::Shutdown) {
                Outcome::Released("the worker shut down before the task started".into())
            } else {
                run(&inner, &envelope, &stream, lease.liveness()).await
            };
            let run_time = started.elapsed();
            let report = classify(&outcome, &envelope, inner.config.max_attempts);
            let started = std::time::Instant::now();
            let finalized = finalize(&inner, &stream, id, envelope, outcome).await;
            metrics.times(queue_time, run_time, started.elapsed());
            (finalized, report)
        }
        Err(e) => {
            error!(stream, id, error = %e, "dead-lettering undecodable entry");
            let err = format!("invalid task payload: {e}");
            let raw = serde_json::json!({ "raw": payload, "last_error": err }).to_string();
            let finalized = retry_or_dead_raw(&inner, &stream, id, "dead", raw, Duration::ZERO, None).await;
            (finalized, (Finished::Dead, None))
        }
    };

    match finalized {
        Ok(1) => {
            metrics.finished(report.0);
            if let Some(event) = report.1 {
                metrics.chain_step(event);
            }
        }
        Ok(_) => {
            metrics.finished(Finished::LeaseLost);
            warn!(stream, id, "lease lost before task was finalized; another worker owns it");
        }
        Err(e) => error!(stream, id, error = %e, "failed to finalize task; it will be redelivered"),
    }
}

/// How a task's outcome will be reported once `finalize` has recorded it.
fn classify(outcome: &Outcome, env: &TaskEnvelope, max_attempts: u32) -> (Finished, Option<ChainEvent>) {
    let failed_chain = env.chain.as_ref().map(|_| ChainEvent::Failed);
    match outcome {
        Outcome::Succeeded(_) => {
            let event = env.chain.as_ref().map(|link| {
                if link.remaining.is_empty() { ChainEvent::Completed } else { ChainEvent::Advanced }
            });
            (Finished::Success, event)
        }
        Outcome::Released(_) => (Finished::Retry, None),
        Outcome::Failed(_) if env.attempt + 1 < max_attempts => (Finished::Retry, None),
        Outcome::Failed(_) | Outcome::Fatal(_) => (Finished::Dead, failed_chain),
    }
}

/// Records a task's outcome: ack it (advancing or completing its chain), retry it, or
/// dead-letter it.
async fn finalize(
    inner: &Inner,
    stream: &str,
    id: &str,
    mut env: TaskEnvelope,
    outcome: Outcome,
) -> redis::RedisResult<i64> {
    match outcome {
        Outcome::Succeeded(output) => succeed(inner, stream, id, env, output).await,
        Outcome::Failed(err) => {
            env.attempt += 1;
            env.last_error = Some(err.clone());
            if env.attempt < inner.config.max_attempts {
                let delay = backoff(
                    env.attempt,
                    inner.config.backoff_base,
                    inner.config.backoff_max,
                );
                warn!(task = %env.id, attempt = env.attempt, error = %err, "task failed; retrying");
                retry_or_dead(inner, stream, id, "retry", &env, delay).await
            } else {
                error!(task = %env.id, attempt = env.attempt, error = %err, "task failed; dead-lettering");
                retry_or_dead(inner, stream, id, "dead", &env, Duration::ZERO).await
            }
        }
        Outcome::Fatal(err) => {
            error!(task = %env.id, error = %err, "task cannot be processed; dead-lettering");
            env.last_error = Some(err);
            retry_or_dead(inner, stream, id, "dead", &env, Duration::ZERO).await
        }
        Outcome::Released(reason) => {
            debug!(task = %env.id, reason, "task released at shutdown; requeueing");
            retry_or_dead(inner, stream, id, "retry", &env, Duration::ZERO).await
        }
    }
}

async fn succeed(
    inner: &Inner,
    stream: &str,
    id: &str,
    env: TaskEnvelope,
    output: Box<serde_json::value::RawValue>,
) -> redis::RedisResult<i64> {
    // Finalize in the layout the task was claimed from, so a 0.1 chain stays in the 0.1 layout.
    let keys = &inner.per_stream[stream].keys;
    let conn = inner.queue.conn.clone();
    let Some(link) = env.chain else {
        return with_retries(conn, |mut conn| async move {
            scripts::ACK
                .key(stream)
                .arg(&inner.group)
                .arg(id)
                .arg(inner.consumer())
                .invoke_async::<i64>(&mut conn)
                .await
        })
        .await;
    };
    let chain_key = keys.chain(&link.chain_id);

    let Some((next_type, rest)) = link.remaining.split_first() else {
        let (chain_key, output) = (&chain_key, output.get());
        return with_retries(conn, |mut conn| async move {
            scripts::COMPLETE
                .key(stream)
                .key(chain_key)
                .arg(&inner.group)
                .arg(id)
                .arg(inner.consumer())
                .arg(output)
                .arg(link.result_ttl_secs)
                .invoke_async::<i64>(&mut conn)
                .await
        })
        .await;
    };

    let next_step = link.step + 1;
    let next = TaskEnvelope {
        id: TaskId::new(),
        processor_type: next_type.clone(),
        context: output,
        attempt: 0,
        enqueued_at: now_millis(),
        last_error: None,
        chain: Some(ChainLink {
            step: next_step,
            remaining: rest.to_vec(),
            ..link.clone()
        }),
    };
    let next_payload = serde_json::to_string(&next).expect("envelope serializes");
    let next_stream = keys.stream(next_type);
    let (chain_key, next_stream, next_payload) = (&chain_key, &next_stream, &next_payload);
    with_retries(conn, |mut conn| async move {
        scripts::ADVANCE
            .key(stream)
            .key(next_stream)
            .key(chain_key)
            .arg(&inner.group)
            .arg(id)
            .arg(inner.consumer())
            .arg(next_payload)
            .arg(PAYLOAD_FIELD)
            .arg(next_step)
            .invoke_async::<i64>(&mut conn)
            .await
    })
    .await
}

async fn run(inner: &Inner, envelope: &TaskEnvelope, stream: &str, liveness: &Arc<Liveness>) -> Outcome {
    let Some(processor) = inner.registry.get(&envelope.processor_type) else {
        return Outcome::Fatal(format!(
            "no processor registered for type `{}`",
            envelope.processor_type
        ));
    };
    let control = TaskControl::new(liveness.clone());
    let task = tokio::spawn((processor.handler)(&envelope.context, control));
    let joined = if processor.controlled {
        // The monitor aborts the task if it stops sending heartbeats.
        liveness.set_abort(task.abort_handle());
        liveness.set_running(true);
        let joined = task.await;
        liveness.set_running(false);
        joined
    } else {
        task.await
    };
    match joined {
        Ok(Ok(output)) => Outcome::Succeeded(output),
        // A processor that stops because the worker is shutting down gives its task back.
        Ok(Err(HandlerError::Failed(e)))
            if processor.controlled && liveness.cancel_reason() == Some(CancelReason::Shutdown) =>
        {
            Outcome::Released(e)
        }
        Ok(Err(HandlerError::Failed(e))) => Outcome::Failed(e),
        Ok(Err(HandlerError::Fatal(e))) => Outcome::Fatal(e),
        Err(join) if join.is_cancelled() => {
            let lease = inner.lease(stream);
            warn!(task = %envelope.id, ?lease, "no heartbeat for a whole lease; processor aborted");
            Outcome::Failed(format!("processor sent no heartbeat for {lease:?} and was aborted"))
        }
        Err(join) => Outcome::Failed(panic_message(join)),
    }
}

fn panic_message(err: tokio::task::JoinError) -> String {
    match err.try_into_panic() {
        Ok(payload) => {
            let msg = payload
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic payload".into());
            format!("processor panicked: {msg}")
        }
        Err(err) => format!("processor task failed: {err}"),
    }
}

async fn retry_or_dead(
    inner: &Inner,
    stream: &str,
    id: &str,
    mode: &str,
    env: &TaskEnvelope,
    delay: Duration,
) -> redis::RedisResult<i64> {
    let payload = serde_json::to_string(env).expect("envelope serializes");
    let chain = env
        .chain
        .as_ref()
        .map(|link| (link, env.last_error.as_deref().unwrap_or_default()));
    retry_or_dead_raw(inner, stream, id, mode, payload, delay, chain).await
}

/// `chain` is the task's chain link and error message, used to mark the chain failed when the
/// task is dead-lettered.
async fn retry_or_dead_raw(
    inner: &Inner,
    stream: &str,
    id: &str,
    mode: &str,
    payload: String,
    delay: Duration,
    chain: Option<(&ChainLink, &str)>,
) -> redis::RedisResult<i64> {
    let info = &inner.per_stream[stream];
    let (delayed, dead) = (&info.delayed, &inner.dead);
    let chain_key = chain.map(|(link, _)| info.keys.chain(&link.chain_id));
    let delay_ms = delay.as_millis() as u64;
    with_retries(inner.queue.conn.clone(), |mut conn| {
        let (payload, chain_key) = (&payload, &chain_key);
        async move {
            let mut invocation = scripts::RETRY_OR_DEAD.prepare_invoke();
            invocation
                .key(stream)
                .key(delayed)
                .key(dead)
                .arg(&inner.group)
                .arg(id)
                .arg(inner.consumer())
                .arg(mode)
                .arg(payload)
                .arg(delay_ms)
                .arg(inner.config.dead_letter_max_len)
                .arg(PAYLOAD_FIELD);
            if let (Some(key), Some((link, error))) = (chain_key, chain) {
                invocation
                    .key(key)
                    .arg(link.step)
                    .arg(error)
                    .arg(link.result_ttl_secs);
            }
            invocation.invoke_async::<i64>(&mut conn).await
        }
    })
    .await
}

/// Runs a finalize call up to three times, since losing it causes a duplicate delivery.
///
/// A retry that finds the entry no longer pending (-1) counts as success: most likely the
/// previous attempt committed and only its reply was lost. Rarely, another worker reclaimed and
/// finished the entry in between, which this then reports as ours.
async fn with_retries<F, Fut>(conn: ConnectionManager, f: F) -> redis::RedisResult<i64>
where
    F: Fn(ConnectionManager) -> Fut,
    Fut: std::future::Future<Output = redis::RedisResult<i64>>,
{
    let mut last = None;
    for attempt in 0..3u32 {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_millis(100 * u64::from(attempt))).await;
        }
        match f(conn.clone()).await {
            Ok(-1) if attempt > 0 => return Ok(1),
            Ok(v) => return Ok(v),
            Err(e) => last = Some(e),
        }
    }
    Err(last.expect("at least one attempt ran"))
}

/// Delay before retry number `attempt` (1-based): `base * 2^(attempt-1)` capped at `max`, with
/// jitter drawn uniformly from the upper half of that range.
pub(crate) fn backoff(attempt: u32, base: Duration, max: Duration) -> Duration {
    let exp = base
        .saturating_mul(2u32.saturating_pow(attempt.saturating_sub(1)))
        .min(max);
    let ms = exp.as_millis() as u64;
    if ms < 2 {
        return exp;
    }
    Duration::from_millis(rand::random_range(ms / 2..=ms))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_caps() {
        let base = Duration::from_millis(100);
        let max = Duration::from_secs(1);
        for _ in 0..100 {
            let d1 = backoff(1, base, max);
            assert!((50..=100).contains(&(d1.as_millis() as u64)));
            let d3 = backoff(3, base, max);
            assert!((200..=400).contains(&(d3.as_millis() as u64)));
            let d20 = backoff(20, base, max);
            assert!((500..=1000).contains(&(d20.as_millis() as u64)));
        }
        assert_eq!(backoff(1, Duration::ZERO, max), Duration::ZERO);
    }

    #[test]
    fn default_config_is_valid() {
        WorkerConfig::default().validate().unwrap();
    }

    #[test]
    fn dangerous_settings_are_rejected() {
        type Break = fn(&mut WorkerConfig);
        let cases: [(&str, Break); 10] = [
            ("concurrency", |c| c.concurrency = 0),
            ("min_concurrency", |c| c.min_concurrency = 17),
            ("batch_size", |c| c.batch_size = 0),
            ("max_attempts", |c| c.max_attempts = 0),
            ("block", |c| c.block = Duration::ZERO),
            ("probe_interval", |c| c.probe_interval = Duration::ZERO),
            ("promote_interval", |c| c.promote_interval = Duration::ZERO),
            ("backoff_base", |c| c.backoff_base = Duration::from_secs(600)),
            ("lease", |c| c.probe_interval = Duration::from_secs(5)),
            ("lease", |c| c.lease = Duration::MAX),
        ];
        for (setting, break_it) in cases {
            let mut config = WorkerConfig::default();
            break_it(&mut config);
            match config.validate() {
                Err(Error::InvalidConfig(msg)) => assert!(msg.contains(setting), "{setting}: {msg}"),
                other => panic!("{setting}: expected InvalidConfig, got {other:?}"),
            }
        }
    }

    #[test]
    fn eviction_is_possible_only_with_a_memory_limit_and_an_evicting_policy() {
        assert!(!may_evict("0", "allkeys-lru"), "no memory limit");
        assert!(!may_evict("1073741824", "noeviction"));
        assert!(may_evict("1073741824", "allkeys-lru"));
        assert!(may_evict("1073741824", "volatile-ttl"), "chain state has a TTL");
        assert!(!may_evict("", ""), "unknown settings don't warn");
    }
}
