# redis-task-graph

A Redis-backed task queue for async Rust. You submit tasks to Redis with a **processor type** and a serializable **context**. Background workers claim pending tasks in parallel and call the async function registered for each task's type with its context.

```rust
queue.submit("send_email", &SendEmail { to: "a@example.com".into() }).await?;
```

Features:
- **Typed tasks.** Contexts and outputs are serde types. `TaskType<In, Out>` constants let the compiler check that producers and processors agree.
- **Chains.** Steps run in order, and each step's output becomes the next step's input. Hand-offs are atomic, and the chain's status and final output can be queried.
- **Retries and dead letters.** Failed tasks retry with exponential backoff, then move to a dead-letter stream.
- **Normally exactly-once delivery.** Each claimed task is held under a lease. A lost worker's tasks are picked up by others.
- **Pushback.** Each worker adapts its concurrency to how loaded its async runtime is, so a misbehaving processor can't starve the worker.
- **Optional live metrics.** OpenTelemetry metrics are behind the `otel` feature, with a ready-made OpenObserve dashboard.

Related documents:
- [`performance_readme.md`](performance_readme.md): measured throughput, latency and scaling.
- [`monitoring/README.md`](monitoring/README.md): live metrics and dashboards.
- [`development_guide.md`](development_guide.md): internal design, for contributors.

## Contents

- [Requirements](#requirements)
- [Quick start](#quick-start)
- [Execution model](#execution-model)
- [Writing processors](#writing-processors)
- [Chains](#chains)
- [Failures, retries and dead letters](#failures-retries-and-dead-letters)
- [Delivery guarantees](#delivery-guarantees)
- [Configuring workers](#configuring-workers)
- [Sample applications](#sample-applications)
- [Deployment and sizing](#deployment-and-sizing)
- [Monitoring](#monitoring)
- [Limitations](#limitations)

## Requirements

- **Rust 1.89 or newer** and a **Tokio** multi-threaded runtime.
- **Redis 7.x**, or a compatible server such as Memurai on Windows. Redis Cluster is not supported (see [Limitations](#limitations)).

Add the crate from crates.io, plus serde and Tokio:

```toml
[dependencies]
redis-task-graph = "0.1"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

To record metrics, enable the `otel` feature: `redis-task-graph = { version = "0.1", features = ["otel"] }`.

## Quick start

```rust
use std::time::Duration;
use redis_task_graph::{Registry, TaskQueue, Worker};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct SendEmail {
    to: String,
    subject: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // All keys this queue uses start with "myapp:".
    let queue = TaskQueue::connect("redis://127.0.0.1/", "myapp").await?;

    // Bind each processor type to an async function taking its context.
    let mut registry = Registry::new();
    registry.register("send_email", |ctx: SendEmail| async move {
        println!("emailing {} about {}", ctx.to, ctx.subject);
        Ok(())
    })?;

    // Start a background worker that runs those processors.
    let worker = Worker::builder(queue.clone(), registry).spawn().await?;

    // Submit tasks from anywhere that has a TaskQueue, in this process or another.
    queue
        .submit("send_email", &SendEmail { to: "a@example.com".into(), subject: "hi".into() })
        .await?;

    tokio::time::sleep(Duration::from_secs(1)).await;
    worker.shutdown().await; // Stops claiming, then waits for in-flight tasks.
    Ok(())
}
```

The repository's examples run against a local Redis (set `REDIS_URL` to use another):

```sh
cargo run --example basic        # two processor types and a handful of tasks
cargo run --example ping_pong    # processors that submit tasks to each other
cargo run --release --example monitored --features otel -- 10   # 10-minute workload with live metrics
```

## Execution model

### Parts

| Part | Role |
|---|---|
| `TaskQueue` | A handle to one **namespace** in Redis. It submits tasks and chains and reads chain status and dead letters. It's cheap to clone, and all clones share one multiplexed connection. |
| `Registry` | Maps processor-type names to async functions. |
| `Worker` | Runs the processors in a `Registry`. `Worker::builder(queue, registry).spawn()` starts it in the background and returns a `WorkerHandle`. |
| `WorkerHandle` | Exposes live stats (`stats()`) and graceful shutdown (`shutdown()`). |

### Where tasks live

Each processor type has its own Redis stream, `<namespace>:stream:<processor type>`. Submitting a task appends one entry to that stream. The entry holds the task's id, its JSON-encoded context, its attempt count and its submission time. Submitting needs no worker to be running: tasks wait in Redis until a worker that handles their type starts.

All workers in a namespace join one Redis **consumer group**. Redis hands each stream entry to exactly one member of the group. That is how many workers, in one process or many, share the load without coordinating with each other.

A worker only reads the streams of the processor types in its registry. Tasks for types no running worker handles stay in Redis untouched until such a worker starts. You can use this to split processor types across separate pools of workers.

### A task's life

```text
 submit ──► stream entry ──► claimed by one worker ──► processor runs ──┬─ Ok ─────► acked and deleted
            (pending)        (lease starts)                             │            (chains: next step enqueued)
                                                                        ├─ Err ────► retry later, after backoff
                                                                        │            (attempts left)
                                                                        └─ Err ────► dead-letter stream
                                                                                     (no attempts left, or undecodable)
```

1. **Claim.** The worker's fetch loop reads new entries in batches, never more than it has free concurrency for. Claiming a task starts a **lease**: the task belongs to this worker for as long as the worker keeps renewing the lease.
2. **Run.** The worker decodes the context into the processor's input type and calls the processor on the Tokio runtime. Many tasks run concurrently, up to the worker's concurrency limit.
3. **Record the outcome.** Each outcome is applied with one atomic Redis script:
   - **Success:** the entry is acknowledged and deleted. For a chain step, the next step is enqueued in the same script.
   - **Failure with attempts left:** the task is scheduled for a retry.
   - **Failure with no attempts left:** the task is moved to the dead-letter stream.

   Every script first checks that this worker still owns the task, so a worker that lost its lease can't record an outcome over the new owner.
4. **Recover.** If a worker dies or loses contact with Redis, its leases stop being renewed. Once a lease has gone unrenewed for `lease` (30 s by default, or the processor type's own lease), another worker reclaims the task and runs it. Processors can also opt in to [heartbeats](#heartbeats-and-short-leases), so that one that hangs is aborted and retried.

### Concurrency

Each worker runs at most `concurrency` tasks at once (16 by default). With **pushback**, which is on by default, the worker treats that number as a ceiling:
- It measures **scheduling lag**: how long ready work waits for a free runtime thread.
- It starts at the runtime's thread count and raises the limit while demand exceeds it and the runtime keeps up.
- It cuts the limit when lag exceeds a budget. That happens when processors compute or block on the runtime instead of awaiting.

While a worker holds back, unclaimed tasks stay in Redis for other workers. `WorkerHandle::stats()` reports the current limit, the tasks in flight and the latest lag.

Leases are renewed by a dedicated OS thread per worker, with its own Redis connection. Renewal keeps working even when processors block every runtime thread.

### Ordering

Tasks in a stream are claimed roughly in submission order, but they run concurrently across workers and threads, so they can finish in any order. A retried task goes back to the end of its stream. If one piece of work must follow another, use a [chain](#chains).

## Writing processors

A processor is an async function or closure. It takes the deserialized context by value and returns `Result<Out, BoxError>`:

```rust
registry.register("resize_image", |ctx: ResizeImage| async move {
    let bytes = storage.get(&ctx.key).await?;   // any error type converts with `?`
    if ctx.width == 0 {
        return Err("width must be positive".into());
    }
    // ...
    Ok(())
})?;
```

- **Context type:** any `DeserializeOwned` type. A context that fails to decode is dead-lettered at once, because retrying can't fix it.
- **Output type:** any `Serialize` type. Processors used only as plain tasks usually return `()`. Output matters when the processor is a chain step.
- **Errors:** return `Err` for retryable failures. `BoxError` is `Box<dyn Error + Send + Sync>`, so `?` works with most error types, and `"message".into()` builds one from a string.
- **Panics:** a panic counts as a failure and is retried like an `Err`. The worker keeps running.
- **Registration:** `register` returns an error if the type is already registered. It returns `&mut Registry`, so calls can be chained with `?`.

### Typed handles

Plain strings work as processor types, but a `TaskType<In, Out>` constant lets the compiler check that producers pass the context type the processor expects:

```rust
use redis_task_graph::TaskType;

pub const RESIZE: TaskType<ResizeImage, Thumbnail> = TaskType::new("resize_image");

registry.register(RESIZE, |ctx| async move { /* ctx: ResizeImage */ Ok(Thumbnail { .. }) })?;
queue.submit(RESIZE, &ResizeImage { key, width: 256 }).await?;   // must be a &ResizeImage
```

Put these constants in a module shared by your producer and worker code. The type checking is compile-time only: Redis just stores the name and the JSON context.

### Sharing state with processors

The registered function is called once per task and must be `Send + Sync + 'static`. To give processors a database pool, an HTTP client or a `TaskQueue`, capture an `Arc` (or another cheap-to-clone handle), and clone it into each task's future:

```rust
let db = Arc::new(Database::connect(..).await?);
registry.register("save", {
    let db = db.clone();
    move |row: Row| {
        let db = db.clone();
        async move {
            db.insert(row).await?;
            Ok(())
        }
    }
})?;
```

### Keep the runtime free

Processors run on the Tokio runtime alongside the worker's own Redis I/O, so:
- **CPU work over about a millisecond** belongs in `tokio::task::spawn_blocking` (or a rayon pool), with the result awaited.
- **Blocking calls** (synchronous HTTP clients, `std::fs`, `std::thread::sleep`) belong there too.

Pushback limits the damage when a processor blocks anyway, but blocking still costs throughput. On a 20 ms wait, blocking caps a worker at runtime threads ÷ 20 ms, about 400 tasks/s with 8 threads. An async wait reached 44,000 tasks/s. See [section 4 of the performance notes](performance_readme.md#4-scaling-cpu-bound-tasks).

### Heartbeats and short leases

By default, a task's lease is renewed for as long as its processor runs. That keeps long tasks safe, but a processor that hangs (a deadlock, or a network call with no timeout) is never noticed.

Processors that should be watched for hangs take a `Heartbeat` and call `beat()` while they make progress:

```rust
use redis_task_graph::Heartbeat;

registry
    .register_with_heartbeat("import_file", |job: Import, hb: Heartbeat| async move {
        for chunk in job.chunks() {
            import(chunk).await?;
            hb.beat();                 // "still making progress"
        }
        Ok(())
    })?
    .set_lease("import_file", Duration::from_secs(5))?;
```

How it works:
- **Opt-in.** Until a processor's first `beat()`, its lease is renewed as usual, so it can do slow setup first. Processors registered with `register` never beat and keep the default behavior.
- **Silence means hung.** After its first beat, the lease is renewed only while beats keep coming: at least one per lease. If a processor goes silent for a whole lease, the worker aborts it and records a retryable failure ("processor sent no heartbeat …"). The task then retries with backoff and counts toward `max_attempts`.
- **Detection time.** A hung processor is aborted between one lease and about 1⅓ leases after its last beat. If the whole worker is stuck, its lease expires anyway, and another worker reclaims the task.
- **Cheap beats.** `beat()` stores a timestamp. Call it as often as you like, including from a `spawn_blocking` closure: `Heartbeat` is `Clone + Send`.
- **Abort limits.** Aborting drops the processor's future at its next `.await`. Blocking code inside the processor can't be interrupted; it keeps running until it returns, and its result is discarded.

`Registry::set_lease` gives one processor type its own lease, overriding the worker's. Short leases detect hung processors and crashed workers sooner, but they leave less margin for a pause in reaching Redis. The minimum is `MIN_LEASE` (100 ms). Every worker that runs a type should set the same lease for it.

## Chains

A chain runs steps in order and passes each step's output to the next step as its input:

```rust
const PARSE: TaskType<String, Vec<u32>> = TaskType::new("parse");
const SUM: TaskType<Vec<u32>, u32> = TaskType::new("sum");

registry.register(PARSE, |s: String| async move {
    Ok(s.split(',').map(|n| n.trim().parse()).collect::<Result<_, _>>()?)
})?;
registry.register(SUM, |v: Vec<u32>| async move { Ok(v.iter().sum()) })?;

let chain = queue.chain(PARSE, &"1,2,3".to_string())
    .then(SUM)
    .submit()
    .await?;                                     // chain: ChainId<u32>

match queue.wait_for_chain(&chain, Duration::from_secs(10)).await? {
    Some(ChainStatus::Completed(total)) => println!("sum = {total}"),     // total: u32
    Some(ChainStatus::Failed { step, error }) => eprintln!("step {step} failed: {error}"),
    Some(ChainStatus::Running { step, total }) => println!("still at step {step} of {total}"),
    None => println!("unknown chain, or its result expired"),
}
```

How chains work:
- **Typed steps.** With `TaskType` handles, `.then(step)` only compiles if the step's input type equals the previous step's output type. Plain string steps are untyped: their output is `()` as far as the compiler knows. Read an untyped chain's result as JSON with `chain.cast::<serde_json::Value>()`.
- **Fixed at submission.** All steps are listed when the chain is submitted. Each step runs as an ordinary task of its processor type, on any worker that handles that type. The same processor can serve plain tasks and chain steps.
- **Atomic hand-off.** One Redis script acknowledges a step and enqueues the next one, so a crash can neither drop the rest of the chain nor fork it into two copies.
- **Retries.** A failed step retries like any task and keeps its place in the chain.
- **Failure.** If a step is dead-lettered, the chain becomes `Failed { step, error }` and later steps never run.
- **Status.** `chain_status` reads the status once and `wait_for_chain` polls it until the chain finishes. The status and final output are kept for 24 hours after the chain finishes. Set `.result_ttl(Duration)` on the builder to change that.

**When to use one.** A chain's latency is the *sum* of its steps, because they run one after another. Use a chain when a step really needs the previous step's output, and submit independent tasks when work can run in parallel. Each hop carries the step's output through Redis, so pass references (object-store keys, row ids) rather than large payloads. Above about 64 KB, chains move data 2–4× more slowly than plain tasks.

## Failures, retries and dead letters

| What happened | Result |
|---|---|
| Processor returned `Err` or panicked, with attempts left | Retried after a backoff delay |
| Processor failed on its last attempt (`max_attempts`, 5 by default) | Moved to the dead-letter stream |
| Context couldn't be decoded, or the output couldn't be encoded | Dead-lettered at once, without retrying |
| A heartbeat processor went silent for a whole lease | Aborted, then retried like a failure |
| Earlier deliveries never recorded an outcome, because their workers crashed or stalled | Each one counts as a failed attempt. Once they reach `max_attempts`, the task is dead-lettered without running again, so a task that crashes workers can't crash them forever. |

- **Backoff:** the delay before retry *n* is `backoff_base × 2^(n-1)`, capped at `backoff_max` (1 s and 5 min by default). Jitter puts the actual delay at random in the upper half of that value.
- **Where retries wait:** waiting retries are held in a sorted set. Each worker moves due retries back onto their streams every `promote_interval` (500 ms), so delays are accurate to about that interval.
- **Retried envelope:** a retried task keeps its task id. Its `attempt` count goes up, and `last_error` holds the most recent failure message.

To inspect failures:

```rust
for task in queue.dead_letters(100).await? {
    println!("{} ({}): attempt {} failed: {:?}",
        task.id, task.processor_type, task.attempt, task.last_error);
}
```

The dead-letter stream (`<namespace>:dead`) is capped at about `dead_letter_max_len` entries (100,000 by default), dropping the oldest. To replay a dead letter, submit its `context` again.

## Delivery guarantees

Delivery is **at-least-once and normally exactly-once**. The design trades a small chance of a duplicate run for not having to coordinate between workers on every task.

A task normally runs exactly once. It can run **more than once** when its first run outlives its lease:
- **The worker crashes or is killed** after the processor did its work but before the outcome was recorded. Another worker reclaims the task after the lease expires and runs it again.
- **The worker's monitor thread can't reach Redis** for longer than about two-thirds of a lease, for example during a network partition or a long process pause. The lease expires, another worker takes the task, and both run it. Only the new owner's outcome is recorded; the stale worker's write is rejected.
- **Recording the outcome fails** after three attempts, for example because Redis is unreachable. The task stays pending and is reclaimed once its lease expires.

A task is **never silently lost** once `submit` has returned. Every state change happens in Redis, inside an atomic script.

What this means for your processors:
- **Make processors with side effects idempotent.** Key external writes by the task's id or by a business key, so that a second run is harmless.
- **Keep the lease well above the longest pause you expect** in reaching Redis. The default of 30 s is generous. A shorter lease recovers faster from a crashed worker, but tolerates shorter stalls. For long-running processors, prefer heartbeats with a short per-type lease over one long lease for everything.
- **Don't build your own chains.** A processor that submits follow-up work and then succeeds (as `ping_pong` does) can lose the follow-up if it crashes between the two, or run it twice. Chains don't have this problem.

## Configuring workers

Set options with the builder methods, or pass a whole `WorkerConfig` with `.config(..)`:

```rust
let worker = Worker::builder(queue.clone(), registry)
    .concurrency(64)                     // most tasks this worker runs at once
    .lease(Duration::from_secs(30))
    .max_attempts(5)
    .backoff(Duration::from_secs(1), Duration::from_secs(300))
    .spawn()
    .await?;
```

| Setting | Default | Meaning |
|---|---|---|
| `concurrency` | 16 | Most tasks running at once. With pushback, this is the ceiling. |
| `min_concurrency` | 1 | Lowest limit pushback may set |
| `adaptive_concurrency` | `true` | Pushback on or off |
| `max_scheduling_lag` | lease ÷ 10, within 5–100 ms | Lag budget: the limit is cut when lag exceeds this |
| `probe_interval` | 10 ms | How often the monitor thread samples lag and checks leases |
| `batch_size` | 16 | Most entries claimed per read |
| `block` | 1 s | How long a read waits for new entries |
| `lease` | 30 s | How long a task may go unrenewed before other workers may reclaim it. Override it per type with `Registry::set_lease`; at least `MIN_LEASE` (100 ms). |
| `max_attempts` | 5 | Total runs before a task is dead-lettered |
| `backoff_base`, `backoff_max` | 1 s, 300 s | Retry delay curve (set both with `.backoff(base, max)`) |
| `promote_interval` | 500 ms | How often due retries are moved back to their streams |
| `dead_letter_max_len` | 100,000 | Approximate cap on the dead-letter stream |
| `consumer_name` | `worker-<uuid>` | This worker's name in the consumer group. Must be unique among running workers. |

`WorkerConfig` is `#[non_exhaustive]`, so it can't be built with a struct literal outside the crate. To build a configuration once and reuse it, start from `WorkerConfig::default()`, assign the fields you need, and pass it with `.config(..)`.

**Shutdown:**
- `shutdown().await` stops claiming new tasks and waits for the ones in flight to finish.
- Dropping the `WorkerHandle` signals shutdown without waiting.
- If the process exits without either, its in-flight tasks are reclaimed by other workers after their leases expire.

## Sample applications

### 1. A background job service: producer and worker processes

The usual layout is a shared crate with the task definitions, then one or more producer processes (such as a web API) and one or more worker processes. They share nothing but the Redis URL and the namespace.

```rust
// tasks.rs, shared by producers and workers
use redis_task_graph::TaskType;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Welcome { pub user_id: u64, pub email: String }

pub const WELCOME_EMAIL: TaskType<Welcome, ()> = TaskType::new("welcome_email");
pub const NAMESPACE: &str = "myapp";
```

```rust
// producer: for example, inside a sign-up request handler
let queue = TaskQueue::connect(&redis_url, tasks::NAMESPACE).await?;   // once, at startup
queue.submit(tasks::WELCOME_EMAIL, &Welcome { user_id, email }).await?;
```

```rust
// worker binary
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let queue = TaskQueue::connect(&redis_url, tasks::NAMESPACE).await?;
    let mailer = Arc::new(Mailer::new());

    let mut registry = Registry::new();
    registry.register(tasks::WELCOME_EMAIL, move |w: Welcome| {
        let mailer = mailer.clone();
        async move {
            // Idempotent: the mail provider de-duplicates on this key.
            mailer.send(&w.email, "Welcome!", format!("welcome-{}", w.user_id)).await?;
            Ok(())
        }
    })?;

    let worker = Worker::builder(queue, registry).concurrency(64).spawn().await?;
    tokio::signal::ctrl_c().await?;
    worker.shutdown().await;
    Ok(())
}
```

Run as many worker processes as you need. They split the work through the consumer group. Give each process a unique `consumer_name`, or keep the random default.

### 2. A processing pipeline with chains

A document-ingestion pipeline where each step needs the previous one's result. Each step passes a small reference along rather than the document itself:

```rust
const EXTRACT: TaskType<DocRef, TextRef> = TaskType::new("extract_text");
const EMBED:   TaskType<TextRef, EmbeddingRef> = TaskType::new("embed");
const INDEX:   TaskType<EmbeddingRef, IndexReceipt> = TaskType::new("index");

let id = queue.chain(EXTRACT, &doc)
    .then(EMBED)
    .then(INDEX)
    .result_ttl(Duration::from_secs(3600))   // callers check back within the hour
    .submit()
    .await?;

// Hand id.uuid() to the client. Later, possibly in another process:
let id: ChainId<IndexReceipt> = ChainId::from_uuid(uuid);
let status = queue.chain_status(&id).await?;
```

Each step's processor can run in a different worker pool. For example, `embed` can run on GPU machines while `extract_text` runs on general-purpose ones: each pool registers only its own processor types.

### 3. Separate pools for CPU-heavy and I/O-heavy work

Register processor types in different workers to give each class of work its own concurrency limit:

```rust
// I/O pool: many concurrent waits are cheap.
let mut io = Registry::new();
io.register("fetch_feed", fetch_feed)?.register("notify", notify)?;
let io_worker = Worker::builder(queue.clone(), io).concurrency(256).spawn().await?;

// CPU pool: concurrency about the number of physical cores, with compute off the runtime.
let mut cpu = Registry::new();
cpu.register("transcode", |job: Transcode| async move {
    Ok(tokio::task::spawn_blocking(move || transcode(job)).await??)
})?;
let cpu_worker = Worker::builder(queue.clone(), cpu).concurrency(8).spawn().await?;
```

The two workers can run in the same process or in different ones.

### 4. Processors that schedule follow-up work

A processor can capture a `TaskQueue` and submit more tasks. [`examples/ping_pong.rs`](examples/ping_pong.rs) shows this pattern. Two printers, `ping` and `pong`, each print their context. Then, with equal odds, each one either prints "I lost" or sends a task to the other with its own name. The rally starts with a `pong` task whose context is `"ping"`:

```rust
fn register_printer(registry: &mut Registry, queue: &TaskQueue, game_over: &Arc<Notify>,
                    name: &'static str, opponent: &'static str) -> redis_task_graph::Result<()> {
    let (queue, game_over) = (queue.clone(), game_over.clone());
    registry.register(name, move |context: String| {
        let (queue, game_over) = (queue.clone(), game_over.clone());
        async move {
            println!("[{name}] {context}");
            if rand::random_bool(0.5) {
                println!("[{name}] I lost");
                game_over.notify_one();
            } else {
                queue.submit(opponent, &name).await?;
            }
            Ok(())
        }
    })?;
    Ok(())
}
```

This suits open-ended or data-dependent follow-ups, like crawling a page and then queueing its links. For a fixed sequence, prefer a chain: a follow-up submitted this way isn't atomic with the current task's completion (see [Delivery guarantees](#delivery-guarantees)).

### 5. Tests against a real Redis

Use a unique namespace per test so tests can run in parallel against one Redis server, and delete the namespace's keys afterwards:

```rust
let ns = format!("test-{}", uuid::Uuid::new_v4());
let queue = TaskQueue::connect("redis://127.0.0.1/", ns.clone()).await?;
// ... submit, spawn a worker with short leases and backoff, assert, shut down ...
// cleanup: delete keys matching "{ns}:*"
```

Short timings keep tests fast: `.lease(Duration::from_millis(500))`, `.backoff(Duration::from_millis(10), Duration::from_millis(50))` and `.block(Duration::from_millis(100))`. [`tests/integration.rs`](tests/integration.rs) has many examples, including multi-worker and failure cases.

## Deployment and sizing

From the measurements in [`performance_readme.md`](performance_readme.md), taken on one laptop with Redis on localhost:

- **Redis is the shared ceiling.** One Redis server topped out at about 75–95k no-op task completions per second here. Beyond that, split the work across namespaces on separate Redis servers.
- **For I/O-bound tasks, throughput ≈ concurrency ÷ task time** across all workers. For example, tasks that wait 20 ms need a total concurrency of about 200 to reach 10k tasks/s.
- **For CPU-bound tasks,** use `spawn_blocking` and set concurrency to about the number of physical cores per worker.
- **Plan for headroom.** Keep peak load under about 80% of measured drain capacity. Past that, queueing latency rises sharply.
- **Size chain result TTLs to the volume.** Every finished chain's status stays in Redis for its `result_ttl`. At high chain volumes, shorten it.
- **Leases set the recovery time.** A crashed worker's tasks are picked up within about 1.5 leases.

## Monitoring

Build with `--features otel` to record OpenTelemetry metrics through the global meter provider. Your application installs the provider and exporter; the library never does. **Install the provider before the first `submit` or `Worker::spawn`:** the library creates its instruments on first use, and instruments created earlier stay bound to the no-op provider.

The metrics cover:
- throughput by outcome;
- queue, run and finalize times;
- pushback state;
- queue depth, pending and delayed counts;
- lost leases.

[`monitoring/`](monitoring/README.md) runs OpenObserve locally as a native binary and imports a dashboard. Measured overhead is within noise. Without the feature, or without a provider, recording is a no-op.

For lighter needs, `WorkerHandle::stats()` gives the live concurrency limit, tasks in flight and scheduling lag, and the library logs through `tracing`.

## Limitations

- **No Redis Cluster.** The scripts derive stream keys from task payloads, so all of a namespace's keys must live on one Redis node.
- **No fan-out or fan-in yet.** Chains are linear: each step has one input and one output.
- **No priorities or per-type rate limits.** A worker reads all its streams with equal weight. Use separate pools to isolate types.
- **Chain status is polled.** `wait_for_chain` polls every 25 ms; there's no push notification.
- **Clock-based timing.** Retry due times use the Redis server's clock. The queue-time metric uses the producer's and worker's clocks, so skew between machines shows up in it.
- **Consumer entries accumulate.** Each worker adds a consumer to the group under its `consumer_name`, and the library doesn't remove them. With random names, the group's consumer list grows over restarts. Fixed names avoid that, but two live workers must never share a name.
