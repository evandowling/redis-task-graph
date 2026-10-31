use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use redis::aio::ConnectionManager;
use redis::streams::{StreamAutoClaimReply, StreamId, StreamPendingCountReply, StreamReadOptions, StreamReadReply};
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::client::TaskQueue;
use crate::error::{Error, Result};
use crate::heartbeat::{Heartbeat, Liveness};
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

impl WorkerConfig {
    /// The effective scheduling-lag budget.
    pub fn lag_budget(&self) -> Duration {
        self.max_scheduling_lag.unwrap_or_else(|| {
            (self.lease / 10).clamp(Duration::from_millis(5), Duration::from_millis(100))
        })
    }
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
pub struct Worker {
    queue: TaskQueue,
    registry: Registry,
    config: WorkerConfig,
}

impl Worker {
    pub fn builder(queue: TaskQueue, registry: Registry) -> Self {
        Self {
            queue,
            registry,
            config: WorkerConfig::default(),
        }
    }

    pub fn config(mut self, config: WorkerConfig) -> Self {
        self.config = config;
        self
    }

    pub fn concurrency(mut self, n: usize) -> Self {
        self.config.concurrency = n.max(1);
        self
    }

    pub fn min_concurrency(mut self, n: usize) -> Self {
        self.config.min_concurrency = n.max(1);
        self
    }

    /// Enables or disables pushback (adaptive concurrency). Enabled by default.
    pub fn adaptive_concurrency(mut self, enabled: bool) -> Self {
        self.config.adaptive_concurrency = enabled;
        self
    }

    pub fn max_scheduling_lag(mut self, d: Duration) -> Self {
        self.config.max_scheduling_lag = Some(d);
        self
    }

    pub fn batch_size(mut self, n: usize) -> Self {
        self.config.batch_size = n.max(1);
        self
    }

    pub fn block(mut self, d: Duration) -> Self {
        self.config.block = d;
        self
    }

    pub fn lease(mut self, d: Duration) -> Self {
        self.config.lease = d;
        self
    }

    pub fn max_attempts(mut self, n: u32) -> Self {
        self.config.max_attempts = n.max(1);
        self
    }

    pub fn backoff(mut self, base: Duration, max: Duration) -> Self {
        self.config.backoff_base = base;
        self.config.backoff_max = max;
        self
    }

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

    pub fn consumer_name(mut self, name: impl Into<String>) -> Self {
        self.config.consumer_name = name.into();
        self
    }

    /// Creates the consumer groups and starts the background loops.
    ///
    /// Dropping the returned handle signals shutdown without waiting; call
    /// [`WorkerHandle::shutdown`] to wait for in-flight tasks to finish.
    pub async fn spawn(self) -> Result<WorkerHandle> {
        if self.registry.is_empty() {
            return Err(Error::NoProcessors);
        }
        let mut streams: Vec<String> = self
            .registry
            .processor_types()
            .map(|t| self.queue.stream_key(t))
            .collect();
        streams.sort();
        // Each stream's lease: its processor type's, or the worker's.
        let stream_prefix = self.queue.keys.stream_prefix();
        let processor_type = |s: &str| s.strip_prefix(stream_prefix.as_str()).unwrap_or(s).to_owned();
        let per_stream: HashMap<String, StreamInfo> = streams
            .iter()
            .map(|s| {
                let processor_type = processor_type(s);
                let lease = self.registry.get(&processor_type).and_then(|p| p.lease);
                let info = StreamInfo {
                    metrics: telemetry::handle(&processor_type),
                    lease: lease.unwrap_or(self.config.lease),
                };
                (s.clone(), info)
            })
            .collect();
        let too_short = |what: &str, lease: Duration| {
            Error::InvalidConfig(format!("{what} lease is {lease:?}; the minimum is {MIN_LEASE:?}"))
        };
        if self.config.lease < MIN_LEASE {
            return Err(too_short("the worker", self.config.lease));
        }
        if let Some((stream, info)) = per_stream.iter().find(|(_, s)| s.lease < MIN_LEASE) {
            return Err(too_short(stream, info.lease));
        }

        let mut conn = self.queue.conn.clone();
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
        let monitored = Monitored::new(Limiter::new(initial));
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
        );
        let stats = Arc::downgrade(&monitored);
        telemetry::register_worker(&config.consumer_name, stats.clone());
        let inner = Arc::new(Inner {
            monitored,
            in_flight: AtomicUsize::new(0),
            idle: Notify::new(),
            group,
            streams,
            per_stream,
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
            supervisor_inner.wait_idle().await;
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

impl WorkerHandle {
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
    /// Metrics for the stream's processor type.
    metrics: telemetry::TypeHandle,
    /// Lease for the stream's tasks: its processor type's, or the worker's.
    lease: Duration,
}

struct Inner {
    queue: TaskQueue,
    registry: Registry,
    config: WorkerConfig,
    group: String,
    streams: Vec<String>,
    /// Metrics and lease for each stream, resolved once at spawn.
    per_stream: HashMap<String, StreamInfo>,
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
    /// Returns `None` if shutdown was signalled while waiting.
    async fn acquire_batch(
        &self,
        max: usize,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Option<Vec<Permit>> {
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

/// Starts processing a claimed entry. `deliveries` is how many times Redis has delivered it,
/// counting this delivery.
fn dispatch(
    inner: &Arc<Inner>,
    stream: String,
    entry: StreamId,
    permit: Option<Permit>,
    deliveries: u64,
    lease: Duration,
) {
    let guard = InFlight::new(inner);
    let dispatched_at = std::time::Instant::now();
    // Start renewing the lease now, at claim time: the task may wait in the run queue for longer
    // than the lease when the runtime is saturated.
    let lease = inner.monitored.hold_lease(&stream, &entry.id, lease);
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
                        dispatch(&inner, key.key.clone(), entry, permits.pop(), 1, info.lease);
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
            dispatch(inner, stream.to_owned(), entry, permits.pop(), deliveries, info.lease);
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
    let delayed_key = inner.queue.keys.delayed();
    loop {
        let mut pipe = redis::pipe();
        for stream in &inner.streams {
            pipe.xlen(stream).cmd("XPENDING").arg(stream).arg(&inner.group);
        }
        pipe.zcard(&delayed_key);
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
                if let Some(delayed) = values.last().and_then(|v| redis::from_redis_value_ref(v).ok()) {
                    telemetry::record_delayed(inner.queue.namespace(), delayed);
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
    let keys = &inner.queue.keys;
    while !sleep_or_stop(&mut shutdown, inner.config.promote_interval).await {
        loop {
            let moved: redis::RedisResult<usize> = scripts::PROMOTE
                .key(keys.delayed())
                .key(keys.dead())
                .arg(keys.stream_prefix())
                .arg(LIMIT)
                .arg(PAYLOAD_FIELD)
                .arg(inner.config.dead_letter_max_len)
                .invoke_async(&mut conn)
                .await;
            match moved {
                Ok(n) if n >= LIMIT => continue,
                Ok(_) => break,
                Err(e) => {
                    warn!(error = %e, "failed to promote delayed tasks");
                    break;
                }
            }
        }
    }
}

enum Outcome {
    Succeeded(serde_json::Value),
    Failed(String),
    /// Not retryable: dead-letter immediately.
    Fatal(String),
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
    }
}

async fn succeed(
    inner: &Inner,
    stream: &str,
    id: &str,
    env: TaskEnvelope,
    output: serde_json::Value,
) -> redis::RedisResult<i64> {
    let keys = &inner.queue.keys;
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
        let output = output.to_string();
        let (chain_key, output) = (&chain_key, &output);
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
    let heartbeat = Heartbeat::new(liveness.clone());
    let task = tokio::spawn((processor.handler)(envelope.context.clone(), heartbeat));
    let joined = if processor.heartbeat {
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
    let keys = &inner.queue.keys;
    let (delayed, dead) = (keys.delayed(), keys.dead());
    let chain_key = chain.map(|(link, _)| keys.chain(&link.chain_id));
    let delay_ms = delay.as_millis() as u64;
    with_retries(inner.queue.conn.clone(), |mut conn| {
        let (delayed, dead, payload, chain_key) = (&delayed, &dead, &payload, &chain_key);
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
}
