# Development guide

This guide covers how `redis-task-graph` works inside, for contributors and coding agents. It explains:
- the Redis data model;
- each component's job and the invariants it keeps;
- why the design looks the way it does;
- where the performance limits are and how to measure changes against them.

For the public API and usage, read [`README.md`](README.md) first. Measured numbers are in [`performance_readme.md`](performance_readme.md).

## Contents

1. [Repository map](#1-repository-map)
2. [Design goals and tradeoffs](#2-design-goals-and-tradeoffs)
3. [Redis data model](#3-redis-data-model)
4. [Task envelope](#4-task-envelope)
5. [Producer path](#5-producer-path)
6. [Worker internals](#6-worker-internals)
7. [Atomic state transitions (Lua scripts)](#7-atomic-state-transitions-lua-scripts)
8. [Leases and the monitor thread](#8-leases-and-the-monitor-thread)
9. [The limiter](#9-the-limiter)
10. [Pushback controller](#10-pushback-controller)
11. [Chains](#11-chains)
12. [Retries, promotion and dead letters](#12-retries-promotion-and-dead-letters)
13. [Delivery semantics in detail](#13-delivery-semantics-in-detail)
14. [Telemetry](#14-telemetry)
15. [Performance model](#15-performance-model)
16. [Testing and benchmarking](#16-testing-and-benchmarking)
17. [Contributor rules of thumb](#17-contributor-rules-of-thumb)
18. [Known gaps and next steps](#18-known-gaps-and-next-steps)
19. [Releasing](#19-releasing)

## 1. Repository map

| Path | Contents |
|---|---|
| `src/lib.rs` | Crate docs and public re-exports |
| `src/client.rs` | `TaskQueue`: connection, `submit`, chain status, dead letters, and the blocking-read connection |
| `src/chain.rs` | `ChainBuilder`, `ChainId<Out>`, `ChainStatus<Out>`, status-hash parsing |
| `src/task.rs` | `TaskType<In,Out>`, `IntoTaskType`, `TaskEnvelope`, `ChainLink`, `TaskId` |
| `src/registry.rs` | `Registry`: type-erases typed processors into JSON-in, JSON-out handlers |
| `src/worker.rs` | `Worker`, `WorkerConfig`, `WorkerHandle`, and the worker's async loops (fetch, reclaim, promote, queue sampling) |
| `src/limiter.rs` | Fair, resizable concurrency limiter on top of `tokio::sync::Semaphore` |
| `src/monitor.rs` | Per-worker OS thread: lag measurement, pushback `Controller`, lease renewal |
| `src/heartbeat.rs` | `Heartbeat` (processor beats) and `Liveness`, the per-task state the monitor checks before renewing |
| `src/scripts.rs` | Every Lua script (`ACK`, `ADVANCE`, `COMPLETE`, `RETRY_OR_DEAD`, `HEARTBEAT`, `PROMOTE`) |
| `src/keys.rs` | Redis key naming |
| `src/telemetry.rs` | OpenTelemetry instruments behind the `otel` feature, with no-op stubs otherwise |
| `src/error.rs` | `Error`, `Result`, `BoxError` |
| `tests/integration.rs` | End-to-end tests against a live Redis (basic, `chains`, `pushback` modules) |
| `tests/telemetry.rs` | Metric assertions with an in-memory exporter (`--features otel`) |
| `benches/load/` | Custom load-test harness (`harness = false`): scenarios, A/B comparison, SVG plots |
| `perf/` | Benchmark results, graphs, the A/B comparison report and the `ab.sh` interleaved runner |
| `monitoring/` | OpenObserve start and stop scripts, dashboard JSON, monitoring docs |
| `examples/` | `basic`, `ping_pong`, `monitored` (needs `otel`) |

## 2. Design goals and tradeoffs

These decisions shaped everything else. Keep them in mind before changing the core.

| Goal | Decision | Cost accepted |
|---|---|---|
| Normally exactly-once, cheap per task | A lease per claimed entry, using the Redis Streams pending-entries list (PEL), plus an ownership check in every finalizing script | Duplicates are possible when a lease expires while the task is still running (section 13) |
| No coordination between workers | One consumer group per namespace; Redis assigns each entry to one consumer | Needs a single Redis node (no Cluster) |
| Atomic state changes | Each outcome is one Lua script | Scripts run on Redis's single thread, so script work is the throughput ceiling |
| Typed API over an untyped store | `TaskType<In,Out>` is a zero-sized phantom wrapper around a name; contexts are JSON | Type safety holds only if every process agrees on the name-to-type mapping |
| Workers survive bad processors | Leases are renewed off the runtime; pushback limits admission | One OS thread per worker; scheduling lag is sampled every 10 ms |
| Telemetry costs nothing when unused | A cargo feature plus no-op stubs; atomics and sampling on the hot path | Duration histograms are sampled above 500 tasks/s per type |

## 3. Redis data model

All keys share the namespace prefix `ns` from `TaskQueue::connect(url, ns)`. See `src/keys.rs`.

| Key | Type | Contents |
|---|---|---|
| `{ns}:stream:{type}` | Stream | One entry per pending task, with field `task` holding the envelope JSON. Entries are deleted (`XDEL`) once their outcome is recorded, so stream length ≈ backlog plus in flight. |
| `{ns}:workers` | Consumer group (on every stream) | Created with `XGROUP CREATE … 0 MKSTREAM` when a worker starts. Starting at `0` delivers tasks submitted before any worker existed. `BUSYGROUP` errors are ignored. |
| `{ns}:delayed` | Sorted set | Envelope JSON of waiting retries, scored by due time in milliseconds of Redis server time |
| `{ns}:chain:{uuid}` | Hash | Chain status: `state` (`running`, `completed` or `failed`), `step`, `total`, plus `output` or `error`. Gets a TTL when the chain finishes. |
| `{ns}:dead` | Stream | Dead-lettered envelopes (field `task`), capped with `XADD MAXLEN ~` |

**Ownership state lives in the PEL.** The library keeps no "claimed by" field of its own. An entry is owned by the consumer that the stream's PEL lists for it. Its lease age is the PEL's idle time:
- `XREADGROUP` and `XAUTOCLAIM` set the owner.
- `XCLAIM … 0` (the heartbeat) resets the idle time.
- `XACK` removes the entry from the PEL.

Every script checks ownership with `XPENDING stream group id id 1 consumer`.

## 4. Task envelope

`TaskEnvelope` (`src/task.rs`) is the only thing stored per task:

```rust
pub struct TaskEnvelope {
    pub id: TaskId,               // UUID v4, stable across retries
    pub processor_type: String,   // also determines the stream; PROMOTE reads it
    pub context: serde_json::Value,
    pub attempt: u32,             // failed attempts so far
    pub enqueued_at: u64,         // ms since epoch, producer clock
    pub last_error: Option<String>,
    pub chain: Option<ChainLink>, // chain_id, step, remaining types, result_ttl_secs
}
```

- **Compatibility.** Optional fields use `#[serde(default)]`, so older envelopes still decode. Add new fields the same way. A field that fails to decode makes the whole entry undecodable, and it is dead-lettered without retries.
- **Plan travels with the task.** `ChainLink::remaining` carries the rest of the chain's step types inside each step's envelope. A chain's plan therefore needs no separate storage and no lookup per step.
- **Stream entry id ≠ task id.** A retry gets a new stream entry id (it's re-added) and keeps its `TaskId`. Code that talks to Redis uses the entry id. Logs and dead letters use the task id.

## 5. Producer path

- **`TaskQueue::submit`** builds an envelope, serializes it and runs a single `XADD`. `TaskQueue` holds a `ConnectionManager`, a multiplexed and auto-reconnecting connection. Concurrent submits from clones of one `TaskQueue` are pipelined on it. That is why submissions scale to more than 500k/s with many producers but sit near 28k/s for one sequential producer: they're bound by round trips.
- **`ChainBuilder::submit`** runs `MULTI · HSET status · XADD first step · EXEC`. The status hash exists before the first step can run, so a worker's `ADVANCE` never races a missing hash.
- **The typed layer is compile-time only.** `IntoTaskType<In,Out>` is implemented for:
  - `TaskType<In,Out>`;
  - `&str` and `String`, with `Out = ()`.

  `ChainBuilder<'q, Out>::then<Next>(impl IntoTaskType<Out, Next>)` is what enforces type adjacency. A `compile_fail` doctest on `TaskQueue::chain` guards it.

## 6. Worker internals

### Threads and tasks per worker

```text
 Tokio runtime (the caller's)                         OS thread "rtg-monitor-<consumer>"
 ├── fetch_loop     XREADGROUP on a dedicated conn ◄──┐  every probe_interval (10 ms):
 ├── reclaim_loop   XAUTOCLAIM every lease/2          │   1. sample scheduling lag
 ├── promote_loop   PROMOTE every promote_interval    │   2. Controller → Limiter::set_limit
 ├── sample_queues  (otel only) every 5 s             │   3. HEARTBEAT due leases (sync conn)
 ├── supervisor     joins loops, waits idle, stops ───┘
 └── per task: process() ─► tokio::spawn(handler) ─► finalize script
```

`Worker::spawn` (`src/worker.rs`) does the following:
1. Rejects an empty registry, then validates the settings: `WorkerConfig::validate`, plus `check_lease` for every stream's lease. Both return `Error::InvalidConfig`.
2. Builds the sorted list of stream keys from the registry's processor types. Reads `maxmemory` and `maxmemory-policy` with `CONFIG GET` and logs a warning if Redis may evict keys (best-effort; managed services often disable `CONFIG`). Creates the consumer group on each stream.
3. Opens a **second** connection for blocking reads. Its response timeout is `block + 5 s`. A blocking `XREADGROUP` on the shared multiplexed connection would stall every other command behind it.
4. Creates the `Limiter`. With pushback, the initial limit is the runtime's thread count, clamped to `[min_concurrency, concurrency]`. Without it, the limit is `concurrency`.
5. Wraps the limiter in `Monitored`, spawns the `Monitor` thread, and registers the worker's gauges with telemetry.
6. Pre-builds a `HashMap<stream, TypeHandle>`, so recording metrics never allocates or takes a global lock.
7. Spawns the loops and a supervisor. On shutdown, the supervisor:
   1. joins the loops;
   2. waits for `in_flight` to reach 0;
   3. stops the monitor thread inside `spawn_blocking`, because thread join is blocking.

### The fetch path, step by step

1. **`acquire_batch(batch_size)`** waits for one permit, then takes as many more as are free without waiting. A worker therefore never claims more than it can run.
2. **`XREADGROUP GROUP g consumer COUNT permits BLOCK block STREAMS s1 s2 … > > …`** claims entries from all of this worker's streams at once.
3. **Contention signal.** If the reply filled every permit and the batch was smaller than `batch_size`, demand exceeded the limit, and the loop calls `mark_contended()`. This flag, not a sampled in-flight count, is what lets pushback raise the limit. See section 10 for why.
4. **`dispatch`** runs for each entry. It:
   - starts an `InFlight` guard (used for shutdown);
   - records `dispatched_at`;
   - **registers the lease with the monitor right away** (`hold_lease`);
   - spawns `process`.

   `COUNT` applies *per stream*, so a multi-stream read can return more entries than permits. Extra entries are dispatched with no permit and wait for one inside `process`, still under a renewed lease.
5. **`process`**:
   1. records the start delay (`dispatched_at.elapsed()`), which is an input to scheduling lag;
   2. takes a permit if it has none;
   3. decodes the envelope;
   4. calls `run`;
   5. classifies the outcome for metrics;
   6. calls `finalize`;
   7. records metrics only once `finalize` returns 1.

   The permit, the `InFlight` guard and the `LeaseGuard` are all dropped when `process` returns. The lease therefore covers queueing, waiting for a permit, running *and* recording the outcome.
6. **`run`** spawns the handler as its own Tokio task, so a panic becomes a `JoinError`, which is turned into a retryable failure. This costs one extra task spawn per task.
7. **`finalize`** picks the script:
   - success: `ACK`, or `ADVANCE` or `COMPLETE` for chain steps;
   - retryable failure: `RETRY_OR_DEAD` in `retry` mode;
   - out of attempts, or a fatal error: `RETRY_OR_DEAD` in `dead` mode.

   `with_retries` runs the call up to 3 times (sleeping 100 and 200 ms in between), because a lost finalize means a duplicate delivery.

### Two in-flight counters

They measure different things. Don't merge them.

| Counter | Counts | Used for |
|---|---|---|
| `Inner::in_flight` (`InFlight` guard) | Dispatched and not yet finalized, including tasks waiting for a permit | Graceful shutdown (`wait_idle`) |
| `Limiter::in_flight` | Tasks holding a permit | Pushback (the controller's "settled" test), `WorkerStats` |

### The reclaim loop

Each stream is scanned every `max(lease/2, 50 ms)`, using that stream's own lease (section 8). A scan pass makes up to `MAX_RECLAIM_CALLS` (16) calls. Each call:
1. takes a batch of permits;
2. runs `XAUTOCLAIM stream group consumer <lease ms> <cursor> COUNT n`;
3. reads the claimed entries' delivery counts with one pipeline of `XPENDING stream group id id 1` (section 12);
4. dispatches what it got, with those counts.

**Why the cursor.** `XAUTOCLAIM` examines at most `10 × COUNT` PEL entries per call, which is 160 at the default batch size. The loop keeps the cursor each call returns, per stream, and stops a pass when the cursor wraps to `0-0`. Earlier versions always started at `0-0`. With more than 160 healthy in-flight entries ahead of an expired one, the expired one was never reached. `expired_task_behind_many_live_ones_is_reclaimed` covers this.

Because the limiter is FIFO-fair, the reclaim loop can't be starved by the fetch loop. An earlier unfair limiter did starve it, and the `expired_lease_is_reclaimed_by_another_worker` test caught that.

## 7. Atomic state transitions (Lua scripts)

`src/scripts.rs`. Every script that changes a claimed entry starts with the `owned()` check:

```lua
local function owned(stream, group, id, consumer)
  return #redis.call('XPENDING', stream, group, id, id, 1, consumer) > 0
end
```

| Script | Keys | Effect | Returns |
|---|---|---|---|
| `ACK` | stream | `XACK`, `XDEL` | 1, or 0 if not owned |
| `ADVANCE` | stream, next_stream, chain | `XACK`, `XDEL`, `XADD` the next step's envelope, `HSET state=running step=n` | 1 / 0 |
| `COMPLETE` | stream, chain | `XACK`, `XDEL`, `HSET state=completed output=…`, `EXPIRE ttl` | 1 / 0 |
| `RETRY_OR_DEAD` | stream, delayed, dead, chain (optional) | `XACK`, `XDEL`, then `ZADD` at `TIME + delay` (retry), or `XADD MAXLEN ~` to dead plus marking the chain failed with a TTL (dead) | 1 / 0 |
| `HEARTBEAT` | stream | If the entry is pending and owned by us, `XCLAIM … 0 JUSTID` to reset idle time | 1 renewed, 0 owned by someone else, -1 no longer pending |
| `PROMOTE` | delayed, dead | Moves up to `limit` due members from the sorted set onto `prefix .. processor_type` streams; payloads that won't decode go to dead | Number moved |

**Invariant: a worker that lost ownership can never change state.**
- The finalize scripts return 0 and do nothing. The worker counts that as `lease_lost` and logs it.
- The new owner's outcome is the only one recorded.

This is what turns "at-least-once execution" into "effectively-once state transitions".

**Cluster incompatibility.** `PROMOTE` builds stream keys from payload contents, and `RETRY_OR_DEAD` and `ADVANCE` touch keys under several names. Supporting Redis Cluster would need hash tags (`{ns}`) on every key *and* would need `PROMOTE` to receive its keys rather than derive them.

Scripts are `LazyLock<redis::Script>` and are invoked with `EVALSHA`. The redis crate loads a script on `NOSCRIPT` and retries.

## 8. Leases and the monitor thread

`src/monitor.rs`.

**Why a separate OS thread.** Heartbeats originally ran as async tasks on the worker's runtime. Processors that computed or blocked on every runtime thread starved the heartbeats. Leases then expired mid-task, and 22–39% of tasks ran twice in the `lease_starvation` scenario. The monitor thread uses its own **synchronous** `redis::Connection`, with 1 s connect, read and write timeouts. It needs no runtime thread, so leases survive a fully blocked runtime. The `leases_survive_processors_that_block_every_runtime_thread` test proves it: it uses a separate worker runtime plus a thief that runs `XAUTOCLAIM`.

**Two kinds of heartbeat.** *Renewals* are the monitor's `HEARTBEAT` script calls, which reset an entry's PEL idle time in Redis. *Processor beats* are `Heartbeat::beat()` calls from user code (`src/heartbeat.rs`), which only store a timestamp in memory. The monitor decides from the beats whether to keep renewing.

**Lease length.** Each stream has a lease: its processor type's `Registry::set_lease`, or the worker's `lease`. `Worker::spawn` resolves the per-stream leases once into `Inner::per_stream`, and rejects any shorter than `MIN_LEASE` (100 ms) or 10 probe intervals, or longer than 30 days, with `Error::InvalidConfig`. Ten probe intervals leave room for three renewal attempts per lease, since renewals are due every third of a lease and checked once per probe. The reclaim loop uses the stream's lease as its `XAUTOCLAIM` min-idle time. Every lease-table entry carries its own lease and renewal period.

**Lease table.**
- **Structure:** 16 shards (`SHARDS`), each a `Mutex<HashMap<(stream index, entry id), Lease>>`. The stream index is the stream's position in the worker's sorted stream list (`StreamInfo::index`), which `Monitored` keeps for building renewals, so a key costs one allocation (the `Box<str>` id). `hold_lease` assigns shards round-robin with an atomic counter, and the `LeaseGuard` remembers its shard. A `Lease` holds `next_due`, `period = max(lease/3, 10 ms)`, `lease`, and an `Arc<Liveness>`.
- **Liveness:** shared by the entry, the worker's `process`, and the processor's `Heartbeat`. It holds:
  - a `running` flag, set only while the processor runs;
  - `last_beat_us`, where 0 means the processor has never beaten;
  - the processor task's `AbortHandle`, set when it starts.
- **Insertion:** `hold_lease` inserts the entry at claim time and returns a `LeaseGuard`, which removes it on drop and exposes the `Liveness`.
- **Scan:** each tick, `renew_leases` locks one shard at a time, takes its due entries and bumps their `next_due`. Tasks on the other shards can start and finish meanwhile. For each one:
  - **Hung:** if `silence() >= lease`, meaning the processor is running, has beaten before, and has been silent for a whole lease, the entry is *not* renewed, and the monitor aborts the processor through its `AbortHandle`.
  - **Otherwise:** the key is queued for renewal.

  The queued keys are then renewed in batches of up to `RENEW_BATCH` (128): one `HEARTBEAT` script call per batch, with each entry's stream in `KEYS` and its id in `ARGV`, returning one status per entry.
- **Status 1:** the lease was renewed, so nothing to do.
- **Status -1:** the entry was acked between scheduling and sending. The lease is removed quietly.
- **Status 0:** someone else owns the entry. The worker logs a warning, increments `rtg.leases.lost`, and removes the lease.
- **Error:** the monitor drops the connection, which is reopened on the next use. It marks the failed batch and all later ones due again, and ends the round, so an unreachable Redis costs one connection timeout per tick rather than one per batch.

**Aborting hung processors.** `run` spawns the processor, stores its `AbortHandle` in the `Liveness`, and awaits it. When the monitor finds a task stalled:
1. it aborts the processor from the monitor thread, which works even when the worker's own tasks are starved;
2. `run` sees a cancelled `JoinError` and records `Outcome::Failed("processor sent no heartbeat …")`, which retries with backoff through the normal finalize path;
3. the monitor keeps not renewing, so the lease still expires if the worker never gets to finalize, and another worker reclaims the task.

`run` treats every cancellation as a stall, because nothing else aborts processor tasks. Processors registered without heartbeats skip the abort handle and the running flag entirely; their only extra cost is the `Arc<Liveness>`.

A first version raced the `JoinHandle` against a `Notify` with `tokio::select!`. In the interleaved A/B it cost 6% of no-op throughput and 33–39% of p90/p99 latency near capacity (38–42k tasks/s). With the abort handle, the same comparison measured −2.3% throughput (A/A noise 2.2%) and +2.9% latency. Merging the per-stream lease lookup into the existing metrics lookup, and skipping the abort handle for processors without heartbeats, brought no-op throughput to +0.0% median over three interleaved rounds, with every point within ±5.6%. The per-task path now has no locks, wakers or timers for heartbeats. The cost is detection granularity: a stall is noticed at the next renewal check, between `lease` and `lease + lease/3` after the last beat.

The finalize normally runs before the Redis lease expires: the last renewal was less than `lease` after the last beat, and the stall is seen within a period after that. So the ownership check passes, and the retry is recorded with the task's attempt count.

Processors that never beat are renewed unconditionally, which is the pre-heartbeat behavior. `abort()` takes effect at the processor's next `.await`. A processor waiting on `spawn_blocking` is cancelled at once, but the blocking closure runs to completion and its result is dropped. A processor that blocks inside its async body is only cancelled once it yields; until then, the expired lease lets another worker take the task over.

**Cost.**
- **Short tasks:** a task shorter than `lease/3` never sends a renewal. Its lease costs two allocations (the `Arc<Liveness>` and the key's id), an atomic increment to pick a shard, and one insert and one remove under that shard's lock.
- **Long tasks:** each is renewed once per `lease/3`, batched with the other due leases: one script call per 128 due leases, so a tick with 300 due leases costs three round trips. The script runs `XPENDING` and `XCLAIM` per entry, so a full batch holds Redis's thread for a few hundred microseconds; `RENEW_BATCH` bounds that.
- **Measured:** sharding and batching cost nothing measurable on the per-task path: no-op throughput -0.5% median (A/A noise 6.9%), and latency at pinned loads +1.2% median, over interleaved rounds against the single-mutex version.
**Timing.**
- **Expiry:** a lease expires once its entry's PEL idle time exceeds the stream's lease.
- **Tolerated stall:** renewals run every `lease/3`, so the monitor tolerates a stall reaching Redis of at least `2/3 × lease`.
- **Recovery:** the reclaim loop scans each stream every `lease/2`. A dead worker's tasks are reclaimed within about `1.5 × lease`.

## 9. The limiter

`src/limiter.rs`. It needs three properties at once:
1. **Fairness.** The fetch and reclaim loops and permit-less dispatched tasks all wait on the same limiter. Tokio's `Semaphore` is FIFO, so a released permit goes to the longest waiter.
2. **Resizable while permits are held.** Pushback lowers the limit without preempting running work.
3. **Contention signal.** The controller needs to know whether demand exceeded supply since its last sample.

How resizing works:
- **Lowering by `k`:** `forget_permits(k)` removes the free permits at once. The remainder becomes **debt**. Each `Permit` drop first tries to pay a unit of debt, by forgetting the permit instead of returning it.
- **Raising by `k`:** cancels outstanding debt first, then `add_permits` adds whatever is left.
- **Serialization:** `set_limit` calls are serialized by a mutex. Acquire and release are lock-free apart from the semaphore itself.
- **Contention flag:** `acquire` sets `contended` when it has to wait, and the fetch loop sets it explicitly (section 6). `take_contended` swaps it back to false.

There are six unit tests, including `released_permit_goes_to_the_waiter_first`.

## 10. Pushback controller

### Measuring lag

Scheduling lag is how long ready work waits for a runtime thread. The monitor takes the **max** of two measurements:
- **Probe lag:** each tick, if the previous probe has run, the monitor spawns a tiny task and records how long it took to be polled. If the probe is still pending, the lag is the time since it was sent. A runtime stuck for 500 ms therefore reads 500 ms even though the probe never ran.
- **Start delay:** `process` records `dispatched_at.elapsed()` on its first poll into `start_delay_us` with `fetch_max`, and the monitor reads and resets it with `swap(0)`.

The probe alone isn't enough. Tasks spawned from outside the runtime land in the injection queue, and Tokio polls that queue ahead of a backed-up local run queue. The probe then reads low while admitted tasks wait a long time.

The latest lag is published in `Monitored::lag_us` for `WorkerStats` and the gauge.

### Control law

`Controller::next_limit(limit, in_flight, saturated, lag, now)` follows TCP-style AIMD with slow start:

| Condition | Action |
|---|---|
| `lag > budget`, `in_flight ≤ limit`, cooldown elapsed, `limit > min` | `limit ← max(min, min(limit−1, ⌊0.7·limit⌋))`; leave slow start |
| `lag ≤ budget/2`, `saturated`, `limit < max` | Slow start: `limit ← min(max, 2·limit)`; afterwards: `limit + 1` |
| Otherwise | Unchanged |

- **Budget:** `max_scheduling_lag`, defaulting to `clamp(lease/10, 5 ms, 100 ms)`. The default ties the budget to the lease, so lag stays far below what would threaten leases.
- **Cooldown:** `max(budget, probe_interval)`.
- **"Settled" gate (`in_flight ≤ limit`):** after a cut, work admitted before the cut keeps the runtime busy until it drains. Cutting again during that time overshoots toward `min`. Without this gate the controller oscillated.
- **"Saturated" input:** the fetch loop's contention flag, *not* a sampled `in_flight == limit`. With cheap tasks, permits are released and re-taken between 10 ms samples, so a sampled check rarely saw saturation and the limit never grew. That bug cost 10–30% of no-op throughput in the first version.
- **Hysteresis:** growth needs `lag ≤ budget/2`, which avoids flapping around the budget.

Controller unit tests are at the bottom of `monitor.rs`. The integration test `blocked_runtime_lowers_the_limit_and_recovery_raises_it` checks the closed loop: slow start from 2 up to 8, a drop under blocking load, then recovery.

## 11. Chains

- **Submission:** an atomic `HSET` of the status plus an `XADD` of step 0 (section 5). The envelope's `ChainLink` carries `chain_id`, `step`, `remaining` and `result_ttl_secs`.
- **Intermediate step success (`ADVANCE`):** in one script, ack and delete the step, `XADD` the next step's envelope with this step's output as its context, and `HSET step`. The next envelope gets a new `TaskId` and `attempt = 0`. Because the ack and the enqueue are atomic, the chain can't be lost or forked at a hand-off.
- **Final step success (`COMPLETE`):** ack, then store `output` and set the TTL.
- **Step dead-lettered:** `RETRY_OR_DEAD` in `dead` mode also sets `state=failed`, `step` and `error` on the hash, and sets the TTL. Retries leave the hash alone; the step's place is kept in its envelope.
- **Reading:** `chain_status` runs `HGETALL` and `parse_status`. A missing hash returns `None`, meaning unknown or expired. `wait_for_chain` polls every 25 ms until the chain finishes or the timeout passes. `ChainId<Out>` is a phantom-typed UUID; `cast::<T>()` reinterprets the output type.

**Costs.**
- **Redis work:** an intermediate step does about 2 extra commands inside its script compared with a plain ack (`XADD` and `HSET`). That is the source of the roughly 18% throughput gap when Redis is the bottleneck.
- **Payload copies:** each step's output is sent to Redis as a script argument and copied into the next entry. That is why large payloads hurt chains 2–4× more than plain tasks.
- **Memory:** about 170 B per chain status record, kept for `result_ttl`.

## 12. Retries, promotion and dead letters

- **Backoff:** `backoff(attempt, base, max)` computes `base·2^(attempt−1)` capped at `max`, then jitters uniformly within `[d/2, d]`. Unit test: `backoff_grows_and_caps`.
- **Scheduling a retry:** `RETRY_OR_DEAD` in `retry` mode computes the due time from Redis `TIME`, so worker clock skew doesn't matter. It then `ZADD`s the updated envelope JSON, with `attempt` incremented and `last_error` set, as the member. The JSON is unique per task and attempt because it includes the task id and the attempt.
- **Promotion:** `promote_loop` runs `PROMOTE` every `promote_interval`, moving up to 100 due members per call. It loops while a call moves all 100. Every worker in a namespace runs this loop. Running it more than once is harmless because the script is atomic: a member is moved once.
- **Dead letters:** `XADD {ns}:dead MAXLEN ~ dead_letter_max_len`.
  - Undecodable entries are stored as `{"raw": <payload>, "last_error": …}` rather than as a `TaskEnvelope`.
  - Retries whose payload `PROMOTE` can't route are dead-lettered verbatim.
  - `dead_letters()` returns `DeadLetter::Task` for valid envelopes and `DeadLetter::Undecodable { payload, error }` for both kinds of bad entry (unwrapping `{raw, last_error}`), and for entries with no payload field. One bad entry never fails the call.
- **Error classes** (`registry.rs`, `HandlerError`):

  | Class | Causes | Handling |
  |---|---|---|
  | `Fatal` | Context failed to decode, output failed to encode | Dead-lettered at once |
  | `Failed` | Processor `Err` | Retryable |
  | Panic | Processor panicked (`JoinError`) | Mapped to `Failed`, so retryable |
  | Missing processor | No processor registered for the type | `Fatal`; can only happen if stream and type disagree |
  | Stalled | A heartbeat processor went silent for a lease (section 8) | Aborted, then `Failed`, so retryable |
  | Crash loop | Earlier deliveries never recorded an outcome | Counted as attempts; `Fatal` once they reach `max_attempts` |

- **Crash loops (poison tasks):** a task that kills its worker (out of memory, `abort`, a stack overflow) never records a failure, so `attempt` alone would let it crash workers forever. Redis counts deliveries per entry: `XREADGROUP` delivers once, and each `XAUTOCLAIM` adds one. The monitor's renewal uses `XCLAIM … JUSTID`, which doesn't add to the count.
  - **Counting:** the reclaim loop reads the counts, and `process` adds `deliveries − 1` to the envelope's `attempt` before running.
  - **Dead-lettering:** if that reaches `max_attempts`, the task is dead-lettered as `Fatal` ("delivered N times without an outcome being recorded") without running.
  - **Later attempts:** the bumped `attempt` carries into later retries, so crashes and failures share one budget.
  - **Caveat:** a delivery that was reclaimed only because its worker stalled reaching Redis also counts, even if it eventually succeeded locally.
  - **Test:** `task_that_keeps_crashing_workers_is_dead_lettered_without_running`.

## 13. Delivery semantics in detail

| Scenario | What happens | Duplicate run? |
|---|---|---|
| Normal success | Owned check passes; ack, delete | No |
| Worker crashes before running | Lease expires; another worker reclaims and runs | No (first never ran) |
| Worker crashes after side effects, before finalize | Lease expires; reclaimed and rerun | **Yes** |
| Monitor can't reach Redis for ≥ 2/3 lease while task runs | Lease expires; reclaimed; both run; stale finalize returns 0 | **Yes** (only one outcome recorded) |
| Finalize fails 3 times (Redis down) | Entry stays pending; once the `LeaseGuard` drops, no more heartbeats; reclaimed after `lease` | **Yes** |
| Heartbeat races ack | `HEARTBEAT` returns -1; lease dropped quietly | No |
| Processor blocks every runtime thread | Monitor thread keeps renewing; pushback lowers admission | No (verified 0% duplicates) |
| Heartbeat processor hangs | Aborted after one lease of silence and retried; if the worker is also wedged, the lease expires and another worker retries | No, unless the hung code later resumes side effects that were blocked |
| Processor without heartbeats hangs | Lease renewed forever; the task holds its permit until the worker restarts | No, but the task never finishes. Use heartbeats to detect this. |
| Task crashes every worker that runs it | Each reclaim counts as an attempt; dead-lettered at `max_attempts` without running | Yes, up to `max_attempts` runs |
| Producer crashes after `XADD` returns | Task is durable in Redis (subject to Redis persistence settings) | N/A |
| DIY chain (processor submits next task, then returns) | Crash between submit and ack runs the next step twice; crash before submit loses it | Possible, and also possible loss |

Durability is whatever Redis gives you. With default RDB snapshots, a Redis crash can lose recent writes, including acks, which would cause redelivery, and submits, which would lose tasks. Use AOF with `appendfsync everysec` or `always` for stronger guarantees.

## 14. Telemetry

`src/telemetry.rs`. Every call site in the library uses `telemetry::*` functions. Without the `otel` feature, they are empty `#[inline]`-able stubs with the same signatures, so call sites need no `cfg`. The one exception is the `sample_queues` loop, which is `cfg`-gated.

**Hot-path design.** The first version recorded about 6 SDK measurements per task. At 47k tasks/s, that raised p50 latency near capacity from 11 to 75 ms, through contention on the SDK's shared aggregation state. The current design:
- **Counters:** `TypeMetrics` holds per-processor-type `AtomicU64`s, in a global `RwLock<HashMap<String, Arc<TypeMetrics>>>`. Workers resolve a `TypeHandle(Arc<TypeMetrics>)` once per stream at spawn, so recording is a relaxed atomic add. Observable counters read the atomics at export time.
- **Histograms:** queue, run and finalize times go to SDK histograms only for sampled tasks. `Sampler` uses a 1 s window per type. When the last window's rate exceeded `HISTOGRAM_BUDGET` (500/s), it keeps each task with probability `500/rate`. Percentiles stay unbiased, but `_count` is the number of samples.
- **Gauges:** observable gauges read `Monitored::stats()` through `Weak` references (dead workers drop out), and read queue samples that `sample_queues` writes every 5 s using pipelined `XLEN`, `XPENDING` summary and `ZCARD`. Samples older than 30 s are no longer reported.
- **Meter binding:** instruments are created lazily from `opentelemetry::global::meter("redis-task-graph")` on first use. **The application must install its meter provider before the first submit or worker spawn**, or the instruments bind to the no-op provider. `tests/telemetry.rs` runs in its own binary for this reason.

The metric list is in [`monitoring/README.md`](monitoring/README.md#metrics).

## 15. Performance model

The full results are in [`performance_readme.md`](performance_readme.md). This section is the mental model behind them.

### Redis work per operation

| Operation | Redis work |
|---|---|
| `submit` | 1 `XADD` |
| Chain submit | `MULTI` · `HSET` · `XADD` · `EXEC` |
| Claim | 1 `XREADGROUP` per batch (up to `batch_size`) |
| Success | 1 script: `XPENDING`, `XACK`, `XDEL` |
| Chain step | 1 script: `XPENDING`, `XACK`, `XDEL`, `XADD`, `HSET` |
| Chain final step | 1 script: `XPENDING`, `XACK`, `XDEL`, `HSET`, `EXPIRE` |
| Failure | 1 script: `XPENDING`, `XACK`, `XDEL`, then `ZADD` or `XADD` (plus `HSET`/`EXPIRE` for a dead chain step) |
| Heartbeat | 1 script per 128 due leases per tick; each task is due every lease/3, only if it runs longer than that |
| Background | `XAUTOCLAIM` per stream per lease/2; `PROMOTE` per 500 ms; (otel) 1 pipeline per 5 s |

### Where the limits are

| Regime | Limit | Measured (one laptop, localhost) |
|---|---|---|
| Sequential (concurrency 1) | Round trips per task (claim, run, ack ≈ 135 µs) | ~7.4k tasks/s |
| No-op, high concurrency | Redis's single thread running scripts | 64k at c=64; 75–95k at c=256 |
| I/O-bound | Little's law: concurrency ÷ task time | 44k/s at c=1024, 20 ms (88% of ideal) |
| Blocking on runtime | Runtime threads ÷ wait | 391/s with 8 threads, 20 ms |
| `spawn_blocking` I/O | Blocking pool (512 threads) ÷ wait | ~25k/s at 20 ms |
| CPU-bound | Physical cores (with about 1.4× from SMT) | ~2.2k/s for 5 ms tasks, 8C/16T |
| Latency | Queueing near capacity | p99 < 15 ms up to 85% of capacity; 9× at 95% |
| Chains, Redis-bound | Extra script work per step | −18% steps/s; flat in length |
| Chains, latency | Steps are sequential | Sum of steps; per-step overhead about 0.14 ms |
| Large payloads | Bandwidth (copied per hop for chains) | Plain ~540–600 MB/s; chains 2–4× lower from 64 KB |
| Submissions | Pipelining on one connection | 28k/s with 1 producer, 575k/s with 256 |

### Performance-sensitive code

These paths run for every task. Changes to them need an A/B measurement (section 16).
- `fetch_loop`, `dispatch` and `process` in `worker.rs`, including the per-task allocations (`String` clones for the stream and entry id) and the extra `tokio::spawn` in `run`.
- `Limiter::acquire`/`try_acquire` and `Permit::drop`.
- `Monitored::hold_lease` and the `LeaseGuard` drop. Each locks one of the 16 lease-table shards.
- `TypeHandle::{claimed, finished, chain_step, times}` and `Sampler::keep`.
- The finalize scripts. Every extra Redis command inside them lowers the Redis-bound ceiling directly.

Off the hot path:
- The monitor tick runs every 10 ms and is O(leases) because of the scan. In-flight tasks are bounded by `concurrency`, so the scan stays small; a due-time heap would avoid it but, with lazy deletion, would hold a stale entry for every task finished within the last `lease/3`.
- The reclaim, promote and sample loops.

## 16. Testing and benchmarking

### Tests

```sh
cargo test                       # unit tests + integration tests (needs Redis)
cargo test --features otel       # adds telemetry unit tests and tests/telemetry.rs
cargo clippy --all-targets
cargo clippy --all-targets --features otel
```

- **Where they run:** integration tests use `REDIS_URL` (default `redis://127.0.0.1/`) and are **skipped**, with a message, if Redis is unreachable within 2 s.
- **Isolation:** each test uses a fresh `rtg-test-<uuid>` namespace and deletes its keys on drop, so tests run in parallel against one server.
- **Timings:** tests use short leases, backoffs and block times. Make new tests wait on conditions with the `eventually(timeout, cond)` helper, not fixed sleeps.
- **Writing new tests:**
  - For lease or pushback behavior, run the worker in a **separate runtime** from the test's driver. Otherwise a blocked worker runtime also blocks the assertions.
  - To simulate another worker taking a lease, run `XAUTOCLAIM` directly as a "thief" consumer.

The current count, with `--features otel`, is 55: 27 integration tests, 24 unit tests, 1 telemetry test and 3 doctests.

### Benchmarks

`benches/load/main.rs` is a custom harness.

```sh
cargo bench --bench load                       # all 14 scenarios (~13 min), then graphs
cargo bench --bench load -- <scenario>...      # specific scenarios
cargo bench --bench load -- --quick            # smoke run
cargo bench --bench load -- --plot             # re-render graphs from perf/results
cargo bench --bench load -- --no-pushback ...  # run with adaptive concurrency off
cargo bench --bench load --features otel -- --otel ...   # stream metrics to OTLP while running
```

**Scenarios:**
- `consume_throughput`
- `submit_throughput`
- `latency_under_load`
- `chain_latency`
- `chain_latency_io`
- `chain_throughput`
- `chain_throughput_io`
- `io_bound`
- `cpu_bound`
- `lease_starvation`
- `payload_size`
- `worker_scaling`
- `pushback_limit`
- `pushback_lag`

**Harness design:**
- **Isolated workers:** each simulated worker gets its own Tokio runtime with a fixed thread count, standing in for a separate machine.
- **Exact I/O waits:** simulated I/O completes on a dedicated device thread at exact deadlines. On Windows, `tokio::time::sleep` rounds 20 ms up to about 31 ms.
- **Drain throughput:** measured from worker start to the last execution, with the queue pre-filled.
- **Latency:** measured from a timestamp carried in the job.
- **Outputs:**
  - JSON goes to `perf/results/`.
  - Plotters renders SVGs to `perf/graphs/`, with light and `-dark` variants.
  - `perf/results/summary.md` holds the numbers.

### Comparing two versions: always interleave

This machine's speed drifted by 30–45% between back-to-back sessions, which produced fake 10–35% "regressions", even in code paths that hadn't changed. Use the interleaved runner:

```sh
git worktree add ../rtg-baseline <baseline-commit>
perf/ab.sh ../rtg-baseline /tmp/ab consume_throughput latency_under_load chain_throughput
# ROUNDS=2 for more samples. For feature builds:
# CAND_CARGO_ARGS="--features otel" CAND_RUN_ARGS="--otel" perf/ab.sh ...
git worktree remove ../rtg-baseline
```

- **Order:** it runs each scenario in A B B A order (`ROUNDS` repeats the cycle).
- **Merge:** it merges each side's runs by median (`--merge-median`).
- **Report:** it writes `perf/comparison.md` and overlay graphs.
- **Threshold:** a change is flagged only if it exceeds twice the baseline's own run-to-run spread, and at least 10%.
- **Control:** include `submit_throughput` as a control when your change doesn't touch submission. If the control moves, the environment drifted.
- **Pin latency loads:** `latency_under_load` offers fractions of a capacity it measures in each run, so a side that measures higher capacity is driven harder, and at 95% of capacity that alone can swing latency several-fold. For A/B runs, set `RTG_LATENCY_CAPACITY=<tasks/s>` so both sides get the same absolute loads. Without it, a lease-table change once showed a false 2–3× p50 regression at 95% load; pinned, it measured +1.2% median.

## 17. Contributor rules of thumb

1. **Every state transition on a claimed entry goes through a script with the `owned()` check.** Never `XACK`, `XDEL` or `XADD` from Rust for a claimed entry outside a script.
2. **Keep work on the hot path proportional to one task.** No new Redis round trips per task, no global locks, no SDK calls, no allocation inside loops over tasks. Do per-type setup once in `Worker::spawn`.
3. **The monitor thread must never depend on the async runtime.** It uses a sync connection and atomics only. If you add work to it, keep each tick bounded: the 10 ms tick drives both pushback and leases.
4. **Leases cover claim to finalize.** If you add a stage (for example, a pre-processing step), make sure the `LeaseGuard` outlives it.
5. **Envelope changes must stay backward compatible:** `#[serde(default)]` and `skip_serializing_if`. Workers and producers on different versions share streams.
6. **Library telemetry goes through `telemetry.rs`.** Add a stub to the non-`otel` module too, and an assertion to `tests/telemetry.rs`.
7. **Measure with `perf/ab.sh`,** and update `performance_readme.md` when a number it quotes changes. The readme notes which commit its text numbers come from.
8. **Run tests and clippy with and without `--features otel`.**
9. **Keep the public API semver-friendly.** New public structs and enums that may gain fields or variants get `#[non_exhaustive]`. A type from `redis`, `serde_json` or `uuid` in the public API means a major upgrade of that dependency is a breaking release; the crate re-exports all three so users can match versions.

## 18. Known gaps and next steps

- **Fan-out and fan-in.** Chains are linear. A DAG layer would extend `ChainLink`, or replace it with a graph id plus a join counter in the status hash, decremented atomically by a script.
- **Consumer cleanup.** Consumers are never removed from the group (`XGROUP DELCONSUMER`). Random default names make the list grow across restarts.
- **Abort on lease loss.** When a renewal finds another consumer owns the entry, the original processor keeps running until it finishes, and its outcome is then rejected. Signalling the task's `Liveness` would let the worker abort it right away, reducing duplicate side effects.
- **Redis Cluster.** Needs hash-tagged keys and a `PROMOTE` that receives its keys rather than deriving them.
- **Push-based chain completion.** `wait_for_chain` polls; keyspace notifications or a per-chain stream would remove that.

## 19. Releasing

The crate is distributed through crates.io under the MIT license.

1. Move the `[Unreleased]` entries in `CHANGELOG.md` under a new version heading.
2. Bump `version` in `Cargo.toml`. Before 1.0, a breaking change bumps the minor version (0.1 → 0.2).
3. Run `cargo publish --dry-run`. It builds the packaged crate in isolation, the same way a user would get it. `cargo package --list` shows what ships: `perf/`, `monitoring/` and `.github/` are excluded in the manifest.
4. Commit, tag `vX.Y.Z`, then `cargo publish`. Published versions are permanent; `cargo yank` only stops new projects from picking a version up.
5. Once 0.1.0 is published, add `cargo semver-checks` to CI to catch accidental breaking changes against the last release.

`rust-version` (1.89) is the highest minimum among the library's dependencies (`uuid` sets it). The CI `msrv` job checks the library on that version. Raise it when a dependency upgrade requires a newer Rust.
