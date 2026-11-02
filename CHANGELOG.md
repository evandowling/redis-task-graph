# Changelog

All notable changes to this crate are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the crate follows
[Semantic Versioning](https://semver.org/). Before 1.0, a minor version bump (0.1 → 0.2) may
include breaking changes.

## [Unreleased]

### Breaking

- **`TaskEnvelope::context` is a `Box<RawValue>`** instead of a `serde_json::Value`. The context stays as JSON text until it is decoded straight into the processor's input type, so it is no longer parsed into a `Value` tree, then cloned, then converted. Decode it yourself with `serde_json::from_str(envelope.context.get())`. `TaskEnvelope` compares contexts by their JSON text.
- **Stream entries store the context in its own field.** `submit`, chain submissions and chain hand-offs write the envelope without its context to the `task` field and the context to a new `context` field, so a worker hands the context to the processor without first scanning it to find its end. 0.3 workers read entries in either layout, but 0.2 workers can't read the new one; see "Upgrading from 0.2" in the README. Retries and dead letters keep the old layout.

Together with the output change below, these make no-op tasks with 64–512 KB contexts 3–3.5x faster for byte arrays and 1.4–1.7x faster for `u32` arrays; chain steps get 1.6–2.3x and up to 1.6x. Text contexts and small contexts are unchanged, except for chain steps with 256 KB and larger text, which are 14–75% faster.

### Changed

- **A plain task's output is no longer serialized.** Only a chain step's output is used, so a processor's output for a plain task is dropped as it is, and an output that can't be serialized no longer fails a plain task.
- **Idle workers' monitors sleep.** While a worker holds no leases, its monitor thread wakes once a second instead of every probe interval (10 ms by default). Claiming a task wakes it at once, so the first lease renewal is never late. Benchmarks are unchanged.
- **Every public item is documented**, and every fallible public function has an "Errors" section. The crate now forbids `unsafe` code and warns on `missing_docs` and `clippy::missing_errors_doc`, which CI treats as errors.
- **Test cleanup is synchronous and uses `SCAN`.** Tests no longer leave their namespaces behind in Redis, and neither tests nor benchmarks use `KEYS`.

## [0.2.0] - 2026-11-01

### Breaking

- **New key layout.** Every key now starts with the namespace in a hash tag (`{app}:stream:email` instead of `app:stream:email`), and each processor type has its own retry set (`{app}:delayed:email`). Every script now receives all the keys it touches, so the layout is ready for Redis Cluster. 0.2 workers drain 0.1 streams and retries, and `dead_letters()` and `chain_status` read 0.1 data too; see "Upgrading from 0.1" in the README.
- **Namespaces can't be empty or contain `{` or `}`.** `TaskQueue::connect` returns the new `Error::InvalidNamespace`.
- **`rtg.queue.delayed` is labelled by `set`** (the retry set's key) instead of `namespace`, since each worker now samples only its own types' sets.
- **Per-type metrics carry a `namespace` attribute** alongside `processor_type`, so namespaces that share type names are no longer counted together. Stale queue samples are now removed rather than skipped.
- **`Heartbeat` is now `TaskControl`, and `register_with_heartbeat` is now `register_with_control`.** The old names remain as deprecated aliases. `TaskControl` adds cancellation: `is_cancelled()`, `cancelled().await` and `cancel_reason()` report `CancelReason::LeaseLost` when another worker takes the task over, and `CancelReason::Shutdown` when the worker shuts down.
- **Shutdown cancels in-flight tasks.** `WorkerHandle::shutdown` now signals running processors before waiting for them. A controlled processor that returns an error after the signal has its task released: requeued at once without using an attempt. Tasks claimed but not yet started at shutdown are released without running.
- **`TaskQueue::dead_letters` returns `Vec<DeadLetter>`.** `DeadLetter::Task` holds the task; `DeadLetter::Undecodable { payload, error }` holds an entry that was never a valid task. Previously one such entry made the whole call fail, and entries with no payload field were silently skipped. Use `DeadLetter::task()` or `into_task()` to get the envelope.

### Added

- **Processor heartbeats:** processors registered with `Registry::register_with_control` receive a
  `TaskControl`. Once a processor beats, its lease is renewed only while beats keep arriving, and a
  processor that goes silent for a whole lease is aborted and retried. Processors that never beat
  keep being renewed as before.
- **Per-type leases:** `Registry::set_lease` overrides the worker's lease for one processor type, so
  short leases can be used where hangs or crashes should be detected quickly.
- **Lease validation:** `MIN_LEASE` (100 ms) is the shortest lease a worker accepts.
  `Worker::spawn` rejects shorter leases with the new `Error::InvalidConfig`. `Error::UnknownProcessor`
  is returned by `set_lease` for a type that isn't registered.
- **Configuration validation:** `WorkerConfig::validate`, called by `Worker::spawn`, rejects unsafe settings with `Error::InvalidConfig`: zero counts or `block`, a `promote_interval` under 10 ms, `min_concurrency` above `concurrency`, `backoff_base` above `backoff_max`, leases shorter than 10 probe intervals, and durations over 30 days.
- **Eviction-policy warning:** workers log a warning at startup if Redis has a memory limit and a `maxmemory-policy` other than `noeviction`, since eviction silently deletes queued tasks.
- **`Debug` for every public type:** `Registry`, `TaskQueue` (shows the namespace), `ChainBuilder` (its steps), `Worker` and `WorkerHandle`. The crate now warns on `missing_debug_implementations`.
- **`Error::Thread`:** `Worker::spawn` returns it when the worker's monitor thread can't be started, instead of panicking.

### Changed

- **Graceful shutdown removes the worker's consumer** from each stream's group, unless it still owns pending entries. Restarts with random consumer names no longer grow the consumer list.
- **Lease renewal is batched:** the monitor renews up to 128 due leases per script call instead of one call per lease, so workers holding many long-running tasks with short leases need far fewer round trips.
- **Sharded lease table:** the table of held leases is split into 16 independently locked shards, and each key takes one allocation instead of two. Tasks starting and finishing contend less, and a renewal scan never locks every task at once.
- **Stored errors include their causes.** A processor error's `source()` chain is now recorded in `last_error` and the dead letter as `top: cause: root`. A cause whose message already appears in the text is skipped.
- **`wait_for_chain` backs off.** It polls 10 ms apart at first, doubling to 500 ms, instead of every 25 ms, and no longer sleeps past its timeout.
- **CI actions are pinned to commit SHAs.**
- **Performance matches 0.1.** In interleaved A/B runs against 0.1 with Redis snapshots paused, every benchmark was within noise: no-op throughput +0.9%, chain throughput −0.0%, p99 latency near capacity −0.1% to +0.6%, submissions +5.8%. The published results in `perf/` still apply.

### Fixed

- **Expired tasks could go unreclaimed.** The reclaim loop always restarted its `XAUTOCLAIM` scan at
  the start of the pending list, and each call examines at most 10 × COUNT entries. With more than
  160 healthy in-flight tasks ahead of an expired one, the expired task was never reclaimed. The
  scan now resumes from the returned cursor.
- **Crash loops ran forever.** A task that crashed or killed every worker that ran it was
  redelivered with no limit, because no failure was ever recorded. Deliveries that never recorded
  an outcome now count as attempts, and such a task is dead-lettered once it reaches
  `max_attempts`.
- **Unreachable Redis stalled lease renewal.** While Redis was unreachable, lease renewal waited
  out a connection timeout for every due lease in turn. It now stops at the first error and
  retries on the next tick.
- **A monitor panic stopped lease renewal silently.** If the monitor thread panicked, leases and pushback stopped while the worker kept claiming tasks, which then expired and ran twice. The panic is now caught and logged, the worker claims nothing new until a monitor tick completes, and the monitor carries on.
- **Finalize retries were miscounted as lost leases.** When a finalize call succeeded but its reply was lost, the retry found the entry gone and counted the task as `lease_lost`. The finalize scripts now return 0 when another consumer owns the entry and -1 when it is no longer pending; a retry that sees -1 counts as success.

## [0.1.0] - 2026-10-31

First release.

- **Tasks:** submit tasks with a processor type and a serde context. Workers claim them from
  Redis Streams through a consumer group and run the registered async processor.
- **Typed handles:** `TaskType<In, Out>` constants check producer and processor types at
  compile time.
- **Chains:** linear chains whose steps hand off their output atomically, with queryable status
  and a result TTL.
- **Failures:** retries with exponential backoff and jitter, then a capped dead-letter stream.
- **Leases:** leases are renewed from a dedicated thread per worker, and an expired lease lets
  another worker reclaim the task. Delivery is at-least-once and normally exactly-once.
- **Pushback:** adaptive concurrency driven by runtime scheduling lag.
- **Metrics:** optional OpenTelemetry metrics behind the `otel` feature.
