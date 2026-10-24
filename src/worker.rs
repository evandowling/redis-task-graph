use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use redis::aio::ConnectionManager;
use redis::streams::{StreamAutoClaimReply, StreamId, StreamReadOptions, StreamReadReply};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::client::TaskQueue;
use crate::error::{Error, Result};
use crate::registry::{HandlerError, Registry};
use crate::scripts;
use crate::task::{PAYLOAD_FIELD, TaskEnvelope};

/// Tuning knobs for a [`Worker`].
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Maximum number of tasks processed at once by this worker.
    pub concurrency: usize,
    /// Maximum number of entries claimed per read.
    pub batch_size: usize,
    /// How long a read blocks waiting for new entries.
    pub block: Duration,
    /// How long a claimed task may go without a heartbeat before other workers may reclaim it.
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

        let mut conn = self.queue.conn.clone();
        let group = self.queue.group_name();
        for stream in &streams {
            create_group(&mut conn, stream, &group).await?;
        }
        let blocking = self.queue.blocking_connection(self.config.block).await?;

        let inner = Arc::new(Inner {
            semaphore: Arc::new(Semaphore::new(self.config.concurrency)),
            in_flight: AtomicUsize::new(0),
            idle: Notify::new(),
            group,
            streams,
            queue: self.queue,
            registry: self.registry,
            config: self.config,
        });

        let (tx, rx) = watch::channel(false);
        let loops = [
            tokio::spawn(fetch_loop(inner.clone(), blocking, rx.clone())),
            tokio::spawn(reclaim_loop(inner.clone(), rx.clone())),
            tokio::spawn(promote_loop(inner.clone(), rx)),
        ];
        let supervisor_inner = inner.clone();
        let join = tokio::spawn(async move {
            for handle in loops {
                if let Err(e) = handle.await {
                    error!(error = %e, "worker loop panicked");
                }
            }
            supervisor_inner.wait_idle().await;
        });

        Ok(WorkerHandle {
            shutdown: tx,
            join,
            consumer_name: inner.config.consumer_name.clone(),
        })
    }
}

/// Controls a running worker.
pub struct WorkerHandle {
    shutdown: watch::Sender<bool>,
    join: JoinHandle<()>,
    consumer_name: String,
}

impl WorkerHandle {
    pub fn consumer_name(&self) -> &str {
        &self.consumer_name
    }

    /// Stops claiming new tasks and waits for in-flight tasks to finish.
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        if let Err(e) = self.join.await {
            error!(error = %e, "worker supervisor panicked");
        }
    }
}

struct Inner {
    queue: TaskQueue,
    registry: Registry,
    config: WorkerConfig,
    group: String,
    streams: Vec<String>,
    semaphore: Arc<Semaphore>,
    in_flight: AtomicUsize,
    idle: Notify,
}

impl Inner {
    fn consumer(&self) -> &str {
        &self.config.consumer_name
    }

    /// Takes one permit, waiting if necessary, then as many more as are free up to `max`.
    /// Returns `None` if shutdown was signalled while waiting.
    async fn acquire_batch(
        &self,
        max: usize,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Option<Vec<OwnedSemaphorePermit>> {
        let first = tokio::select! {
            p = self.semaphore.clone().acquire_owned() => p.expect("semaphore is never closed"),
            _ = shutdown.changed() => return None,
        };
        let mut permits = vec![first];
        while permits.len() < max {
            match self.semaphore.clone().try_acquire_owned() {
                Ok(p) => permits.push(p),
                Err(_) => break,
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

fn dispatch(
    inner: &Arc<Inner>,
    stream: String,
    entry: StreamId,
    permit: Option<OwnedSemaphorePermit>,
) {
    let guard = InFlight::new(inner);
    tokio::spawn(process(inner.clone(), stream, entry, permit, guard));
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
                for key in reply.keys {
                    for entry in key.ids {
                        // COUNT applies per stream, so a multi-stream read can return more
                        // entries than permits; the excess waits for a permit inside `process`.
                        dispatch(&inner, key.key.clone(), entry, permits.pop());
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

async fn reclaim_loop(inner: Arc<Inner>, mut shutdown: watch::Receiver<bool>) {
    let mut conn = inner.queue.conn.clone();
    let interval = (inner.config.lease / 2).max(Duration::from_millis(100));
    let min_idle = inner.config.lease.as_millis() as u64;
    while !sleep_or_stop(&mut shutdown, interval).await {
        for stream in &inner.streams {
            let Some(mut permits) = inner.acquire_batch(inner.config.batch_size, &mut shutdown).await
            else {
                return;
            };
            let reply: redis::RedisResult<StreamAutoClaimReply> = redis::cmd("XAUTOCLAIM")
                .arg(stream)
                .arg(&inner.group)
                .arg(inner.consumer())
                .arg(min_idle)
                .arg("0-0")
                .arg("COUNT")
                .arg(permits.len())
                .query_async(&mut conn)
                .await;
            match reply {
                Ok(reply) => {
                    for entry in reply.claimed {
                        debug!(stream, id = %entry.id, "reclaimed expired task");
                        dispatch(&inner, stream.clone(), entry, permits.pop());
                    }
                }
                Err(e) => warn!(error = %e, stream, "failed to reclaim tasks"),
            }
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
    Succeeded,
    Failed(String),
    /// Not retryable: dead-letter immediately.
    Fatal(String),
}

async fn process(
    inner: Arc<Inner>,
    stream: String,
    entry: StreamId,
    permit: Option<OwnedSemaphorePermit>,
    _guard: InFlight,
) {
    // Keep the lease alive from the moment the entry is claimed, including while waiting for a permit.
    let heartbeat = tokio::spawn(heartbeat(inner.clone(), stream.clone(), entry.id.clone()));
    let _permit = match permit {
        Some(p) => p,
        None => inner
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore is never closed"),
    };

    let payload: String = entry.get(PAYLOAD_FIELD).unwrap_or_default();
    let (envelope, outcome) = match serde_json::from_str::<TaskEnvelope>(&payload) {
        Ok(envelope) => {
            let outcome = run(&inner, &envelope).await;
            (Some(envelope), outcome)
        }
        Err(e) => (None, Outcome::Fatal(format!("invalid task payload: {e}"))),
    };
    heartbeat.abort();

    let id = &entry.id;
    let finalized = match (outcome, envelope) {
        (Outcome::Succeeded, _) => {
            let (inner, stream) = (&inner, &stream);
            with_retries(inner.queue.conn.clone(), |mut conn| async move {
                scripts::ACK
                    .key(stream)
                    .arg(&inner.group)
                    .arg(id)
                    .arg(inner.consumer())
                    .invoke_async::<i64>(&mut conn)
                    .await
            })
            .await
        }
        (Outcome::Failed(err), Some(mut env)) => {
            env.attempt += 1;
            env.last_error = Some(err.clone());
            if env.attempt < inner.config.max_attempts {
                let delay = backoff(
                    env.attempt,
                    inner.config.backoff_base,
                    inner.config.backoff_max,
                );
                warn!(task = %env.id, attempt = env.attempt, error = %err, "task failed; retrying");
                retry_or_dead(&inner, &stream, id, "retry", &env, delay).await
            } else {
                error!(task = %env.id, attempt = env.attempt, error = %err, "task failed; dead-lettering");
                retry_or_dead(&inner, &stream, id, "dead", &env, Duration::ZERO).await
            }
        }
        (Outcome::Fatal(err), Some(mut env)) => {
            error!(task = %env.id, error = %err, "task cannot be processed; dead-lettering");
            env.last_error = Some(err);
            retry_or_dead(&inner, &stream, id, "dead", &env, Duration::ZERO).await
        }
        (Outcome::Fatal(err) | Outcome::Failed(err), None) => {
            error!(stream, id, error = %err, "dead-lettering undecodable entry");
            let raw = serde_json::json!({ "raw": payload, "last_error": err }).to_string();
            retry_or_dead_raw(&inner, &stream, id, "dead", raw, Duration::ZERO).await
        }
    };

    match finalized {
        Ok(1) => {}
        Ok(_) => warn!(stream, id, "lease lost before task was finalized; another worker owns it"),
        Err(e) => error!(stream, id, error = %e, "failed to finalize task; it will be redelivered"),
    }
}

async fn run(inner: &Inner, envelope: &TaskEnvelope) -> Outcome {
    let Some(handler) = inner.registry.get(&envelope.processor_type).cloned() else {
        return Outcome::Fatal(format!(
            "no processor registered for type `{}`",
            envelope.processor_type
        ));
    };
    match tokio::spawn(handler(envelope.context.clone())).await {
        Ok(Ok(())) => Outcome::Succeeded,
        Ok(Err(HandlerError::Failed(e))) => Outcome::Failed(e),
        Ok(Err(HandlerError::Decode(e))) => Outcome::Fatal(format!("invalid context: {e}")),
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
    retry_or_dead_raw(inner, stream, id, mode, payload, delay).await
}

async fn retry_or_dead_raw(
    inner: &Inner,
    stream: &str,
    id: &str,
    mode: &str,
    payload: String,
    delay: Duration,
) -> redis::RedisResult<i64> {
    let keys = &inner.queue.keys;
    let (delayed, dead) = (keys.delayed(), keys.dead());
    let delay_ms = delay.as_millis() as u64;
    with_retries(inner.queue.conn.clone(), |mut conn| {
        let (delayed, dead, payload) = (&delayed, &dead, &payload);
        async move {
            scripts::RETRY_OR_DEAD
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
                .arg(PAYLOAD_FIELD)
                .invoke_async::<i64>(&mut conn)
                .await
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

async fn heartbeat(inner: Arc<Inner>, stream: String, id: String) {
    let mut conn = inner.queue.conn.clone();
    let period = (inner.config.lease / 3).max(Duration::from_millis(50));
    loop {
        tokio::time::sleep(period).await;
        let res: redis::RedisResult<i64> = scripts::HEARTBEAT
            .key(&stream)
            .arg(&inner.group)
            .arg(&id)
            .arg(inner.consumer())
            .invoke_async(&mut conn)
            .await;
        match res {
            Ok(1) => {}
            Ok(_) => {
                warn!(stream, id, "lease lost; another worker may run this task");
                return;
            }
            Err(e) => warn!(stream, id, error = %e, "heartbeat failed"),
        }
    }
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
