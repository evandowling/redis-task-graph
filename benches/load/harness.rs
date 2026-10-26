//! Shared load-test machinery: instrumented processors, worker clusters running in their own
//! Tokio runtimes (standing in for separate machines), and workload generators.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, mpsc};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::oneshot;

use futures::StreamExt;
use redis::AsyncCommands;
use redis_task_graph::{Registry, TaskQueue, TaskType, Worker, WorkerHandle};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

pub const REDIS_URL: &str = "redis://127.0.0.1/";

/// Typed chain step: does the configured work and passes the job on with `hop + 1`.
pub const STEP: TaskType<Job, Job> = TaskType::new("step");
/// Untyped "do it yourself" chain step: does the work, then submits the next hop itself.
pub const DIY: &str = "diy";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: u64,
    /// Submission time of the job's first hop, in microseconds since the Unix epoch.
    pub t0_us: u64,
    pub hop: u32,
    pub len: u32,
    #[serde(default)]
    pub pad: String,
}

impl Job {
    pub fn new(id: u64, len: u32, pad_bytes: usize) -> Self {
        Self { id, t0_us: now_us(), hop: 0, len, pad: "x".repeat(pad_bytes) }
    }
}

pub fn now_us() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64
}

/// What each processor invocation does before returning.
#[derive(Debug, Clone, Copy)]
pub enum Work {
    Noop,
    /// A well-behaved async wait (network call, database query), completed by [`io_wait`].
    AsyncWait(Duration),
    /// `std::thread::sleep` on the async runtime: blocking I/O done wrong.
    BlockingSleep(Duration),
    /// `std::thread::sleep` inside `spawn_blocking`: blocking I/O done right.
    SpawnBlockingSleep(Duration),
    /// CPU work on the async runtime thread.
    CpuInline(u64),
    /// CPU work inside `spawn_blocking`.
    CpuSpawnBlocking(u64),
}

pub async fn do_work(work: Work) {
    match work {
        Work::Noop => {}
        Work::AsyncWait(d) => io_wait(d).await,
        Work::BlockingSleep(d) => std::thread::sleep(d),
        Work::SpawnBlockingSleep(d) => tokio::task::spawn_blocking(move || std::thread::sleep(d)).await.unwrap(),
        Work::CpuInline(iters) => {
            burn(iters);
        }
        Work::CpuSpawnBlocking(iters) => {
            tokio::task::spawn_blocking(move || burn(iters)).await.unwrap();
        }
    }
}

/// A pending simulated I/O completion, ordered by deadline.
struct Completion {
    deadline: Instant,
    seq: u64,
    done: oneshot::Sender<()>,
}

impl PartialEq for Completion {
    fn eq(&self, other: &Self) -> bool {
        (self.deadline, self.seq) == (other.deadline, other.seq)
    }
}
impl Eq for Completion {}
impl PartialOrd for Completion {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Completion {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.deadline, self.seq).cmp(&(other.deadline, other.seq))
    }
}

/// Simulated I/O device: a dedicated thread that completes each request after exactly its
/// latency. `tokio::time::sleep` can't stand in for I/O here because Windows timer ticks round
/// a 20 ms sleep up to ~31 ms, while `std::thread::sleep` uses a high-resolution timer.
static IO_DEVICE: LazyLock<mpsc::Sender<(Instant, oneshot::Sender<()>)>> = LazyLock::new(|| {
    let (tx, rx) = mpsc::channel::<(Instant, oneshot::Sender<()>)>();
    std::thread::spawn(move || {
        let mut pending = BinaryHeap::new();
        let mut seq = 0u64;
        loop {
            loop {
                match rx.try_recv() {
                    Ok((deadline, done)) => {
                        seq += 1;
                        pending.push(Reverse(Completion { deadline, seq, done }));
                    }
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => return,
                }
            }
            let now = Instant::now();
            while pending.peek().is_some_and(|Reverse(c)| c.deadline <= now) {
                let Reverse(c) = pending.pop().unwrap();
                let _ = c.done.send(());
            }
            match pending.peek() {
                None => match rx.recv() {
                    Ok((deadline, done)) => {
                        seq += 1;
                        pending.push(Reverse(Completion { deadline, seq, done }));
                    }
                    Err(_) => return,
                },
                Some(Reverse(next)) => {
                    std::thread::sleep((next.deadline - now).min(Duration::from_micros(250)));
                }
            }
        }
    });
    tx
});

/// Waits `d` without occupying a runtime thread, like awaiting a network response.
pub async fn io_wait(d: Duration) {
    let (tx, rx) = oneshot::channel();
    IO_DEVICE.send((Instant::now() + d, tx)).unwrap();
    rx.await.unwrap();
}

/// Spins for `iters` rounds of xorshift; the optimizer can't remove it.
pub fn burn(iters: u64) -> u64 {
    let mut x = 0x9E3779B97F4A7C15u64;
    for _ in 0..iters {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x = std::hint::black_box(x);
    }
    x
}

/// Iterations of [`burn`] that take roughly `target` on one core.
pub fn calibrate_burn(target: Duration) -> u64 {
    let mut iters = 1_000_000u64;
    loop {
        let start = Instant::now();
        burn(iters);
        let took = start.elapsed();
        if took >= Duration::from_millis(50) {
            return (iters as f64 * target.as_secs_f64() / took.as_secs_f64()) as u64;
        }
        iters *= 2;
    }
}

/// Collects execution counts, completion times and end-to-end latencies from processors.
#[derive(Default)]
pub struct Recorder {
    pub executions: AtomicU64,
    /// When the first worker began spawning; throughput is measured from here.
    start_us: AtomicU64,
    last_us: AtomicU64,
    completed: Mutex<HashSet<u64>>,
    latencies_us: Mutex<Vec<u64>>,
}

impl Recorder {
    pub fn new() -> Arc<Self> {
        let r = Self::default();
        r.start_us.store(u64::MAX, Ordering::Relaxed);
        Arc::new(r)
    }

    fn mark_start(&self) {
        self.start_us.fetch_min(now_us(), Ordering::Relaxed);
    }

    fn executed(&self) {
        self.executions.fetch_add(1, Ordering::Relaxed);
        self.last_us.fetch_max(now_us(), Ordering::Relaxed);
    }

    fn finished(&self, job: &Job) {
        let latency = now_us().saturating_sub(job.t0_us);
        self.latencies_us.lock().unwrap().push(latency);
        self.completed.lock().unwrap().insert(job.id);
    }

    pub fn completed(&self) -> usize {
        self.completed.lock().unwrap().len()
    }

    /// Seconds from worker start to the last processor execution.
    pub fn active_secs(&self) -> f64 {
        let (a, b) = (self.start_us.load(Ordering::Relaxed), self.last_us.load(Ordering::Relaxed));
        if a == u64::MAX || b <= a { 1e-9 } else { (b - a) as f64 / 1e6 }
    }

    pub fn take_latencies_ms(&self) -> Vec<f64> {
        let mut v: Vec<f64> = std::mem::take(&mut *self.latencies_us.lock().unwrap())
            .into_iter()
            .map(|us| us as f64 / 1000.0)
            .collect();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v
    }

    /// Executions per second over the active window.
    pub fn rate(&self) -> f64 {
        self.executions.load(Ordering::Relaxed) as f64 / self.active_secs()
    }
}

pub fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx]
}

/// Builds a registry with the instrumented `STEP` and `DIY` processors.
pub fn registry(queue: &TaskQueue, rec: &Arc<Recorder>, work: Work) -> Registry {
    let mut reg = Registry::new();
    let r = rec.clone();
    reg.register(STEP, move |mut job: Job| {
        let r = r.clone();
        async move {
            do_work(work).await;
            r.executed();
            job.hop += 1;
            if job.hop >= job.len {
                r.finished(&job);
            }
            Ok(job)
        }
    })
    .unwrap();
    let (r, q) = (rec.clone(), queue.clone());
    reg.register(DIY, move |mut job: Job| {
        let (r, q) = (r.clone(), q.clone());
        async move {
            do_work(work).await;
            r.executed();
            job.hop += 1;
            if job.hop >= job.len {
                r.finished(&job);
            } else {
                q.submit(DIY, &job).await?;
            }
            Ok(())
        }
    })
    .unwrap();
    reg
}

#[derive(Debug, Clone, Copy)]
pub struct ClusterSpec {
    pub workers: usize,
    /// Tokio worker threads per worker runtime.
    pub threads: usize,
    pub concurrency: usize,
    pub lease: Duration,
    pub work: Work,
}

impl ClusterSpec {
    pub fn new(concurrency: usize, work: Work) -> Self {
        Self { workers: 1, threads: 8, concurrency, lease: Duration::from_secs(30), work }
    }
}

/// A set of workers, each in its own multi-threaded runtime.
pub struct Cluster {
    workers: Vec<(Runtime, WorkerHandle)>,
}

impl Cluster {
    /// Must be called outside any runtime.
    pub fn start(spec: ClusterSpec, ns: &str, rec: &Arc<Recorder>) -> Cluster {
        let workers = (0..spec.workers)
            .map(|_| {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(spec.threads)
                    .enable_all()
                    .build()
                    .unwrap();
                let handle = rt.block_on(async {
                    let queue = TaskQueue::connect(REDIS_URL, ns).await.unwrap();
                    let reg = registry(&queue, rec, spec.work);
                    rec.mark_start();
                    Worker::builder(queue, reg)
                        .concurrency(spec.concurrency)
                        .batch_size(spec.concurrency.min(64))
                        .lease(spec.lease)
                        .spawn()
                        .await
                        .unwrap()
                });
                (rt, handle)
            })
            .collect();
        Cluster { workers }
    }

    pub fn stop(self) {
        for (rt, handle) in self.workers {
            rt.block_on(handle.shutdown());
            rt.shutdown_timeout(Duration::from_secs(5));
        }
    }
}

/// Runtime for producers and measurement, separate from the workers.
pub fn driver() -> Runtime {
    tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap()
}

pub fn fresh_ns() -> String {
    format!("bench-{}", uuid::Uuid::new_v4().simple())
}

pub async fn connect(ns: &str) -> TaskQueue {
    TaskQueue::connect(REDIS_URL, ns).await.unwrap()
}

/// Deletes every key in the namespace.
pub async fn cleanup(ns: &str) {
    let client = redis::Client::open(REDIS_URL).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let keys: Vec<String> = conn.keys(format!("{ns}:*")).await.unwrap();
    for chunk in keys.chunks(1000) {
        let _: () = conn.del(chunk).await.unwrap();
    }
}

/// What to enqueue for each job.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Shape {
    /// One regular task per job.
    Task,
    /// One chain of `len` typed steps per job.
    Chain(u32),
    /// One self-submitting DIY chain of `len` hops per job.
    Diy(u32),
}

impl Shape {
    pub fn len(self) -> u32 {
        match self {
            Shape::Task => 1,
            Shape::Chain(n) | Shape::Diy(n) => n,
        }
    }
}

pub async fn enqueue(queue: &TaskQueue, shape: Shape, job: Job) {
    match shape {
        Shape::Task => {
            queue.submit(STEP, &job).await.unwrap();
        }
        Shape::Chain(len) => {
            // STEP is Job -> Job, so appending a step keeps the builder's type.
            let mut chain = queue.chain(STEP, &job);
            for _ in 1..len {
                chain = chain.then(STEP);
            }
            chain.submit().await.unwrap();
        }
        Shape::Diy(_) => {
            queue.submit(DIY, &job).await.unwrap();
        }
    }
}

/// Enqueues `jobs` jobs with up to 64 submissions in flight.
pub async fn preload(queue: &TaskQueue, shape: Shape, jobs: usize, pad: usize) {
    futures::stream::iter(0..jobs as u64)
        .map(|id| enqueue(queue, shape, Job::new(id, shape.len(), pad)))
        .buffer_unordered(64)
        .collect::<Vec<()>>()
        .await;
}

/// Waits until `rec` has seen `jobs` distinct jobs finish, or `timeout` passes.
pub async fn wait_done(rec: &Recorder, jobs: usize, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while rec.completed() < jobs {
        if Instant::now() > deadline {
            eprintln!("  timed out: {}/{} jobs done", rec.completed(), jobs);
            return false;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    true
}

/// Preloads `jobs` jobs, then starts a cluster and measures how fast it drains them.
/// Returns processor executions per second.
pub fn drain(spec: ClusterSpec, shape: Shape, jobs: usize, pad: usize) -> DrainResult {
    let ns = fresh_ns();
    let rec = Recorder::new();
    let rt = driver();
    rt.block_on(async {
        let q = connect(&ns).await;
        preload(&q, shape, jobs, pad).await;
    });
    let cluster = Cluster::start(spec, &ns, &rec);
    let ok = rt.block_on(wait_done(&rec, jobs, Duration::from_secs(120)));
    // Give in-flight duplicates (if any) a moment to land before counting executions.
    std::thread::sleep(Duration::from_millis(50));
    cluster.stop();
    rt.block_on(cleanup(&ns));
    if !ok {
        eprintln!("  warning: drain incomplete; rate covers only finished work");
    }
    DrainResult {
        rate: rec.rate(),
        executions: rec.executions.load(Ordering::Relaxed),
        steps: jobs as u64 * shape.len() as u64,
    }
}

/// Runs [`drain`] `reps` times and returns the run with the median rate.
pub fn drain_median(spec: ClusterSpec, shape: Shape, jobs: usize, pad: usize, reps: usize) -> DrainResult {
    let mut runs: Vec<DrainResult> = (0..reps.max(1)).map(|_| drain(spec, shape, jobs, pad)).collect();
    runs.sort_by(|a, b| a.rate.partial_cmp(&b.rate).unwrap());
    runs.swap_remove(runs.len() / 2)
}

/// Median wall-clock duration of `io_wait(d)`.
pub fn measured_io_wait(d: Duration) -> Duration {
    driver().block_on(async {
        let mut v = Vec::new();
        for _ in 0..30 {
            let s = Instant::now();
            io_wait(d).await;
            v.push(s.elapsed());
        }
        v.sort();
        v[v.len() / 2]
    })
}

pub struct DrainResult {
    /// Processor executions per second.
    pub rate: f64,
    pub executions: u64,
    /// Expected executions if every step ran exactly once.
    pub steps: u64,
}

impl DrainResult {
    pub fn duplicate_pct(&self) -> f64 {
        (self.executions.saturating_sub(self.steps)) as f64 * 100.0 / self.steps as f64
    }
}
