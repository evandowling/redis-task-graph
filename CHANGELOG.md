# Changelog

All notable changes to this crate are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the crate follows
[Semantic Versioning](https://semver.org/). Before 1.0, a minor version bump (0.1 → 0.2) may
include breaking changes.

## [Unreleased]

### Added

- **Processor heartbeats:** processors registered with `Registry::register_with_heartbeat` receive a
  `Heartbeat`. Once a processor beats, its lease is renewed only while beats keep arriving, and a
  processor that goes silent for a whole lease is aborted and retried. Processors that never beat
  keep being renewed as before.
- **Per-type leases:** `Registry::set_lease` overrides the worker's lease for one processor type, so
  short leases can be used where hangs or crashes should be detected quickly.
- **Lease validation:** `MIN_LEASE` (100 ms) is the shortest lease a worker accepts.
  `Worker::spawn` rejects shorter leases with the new `Error::InvalidConfig`. `Error::UnknownProcessor`
  is returned by `set_lease` for a type that isn't registered.
- **Configuration validation:** `WorkerConfig::validate`, called by `Worker::spawn`, rejects unsafe settings with `Error::InvalidConfig`: zero counts or `block`, a `promote_interval` under 10 ms, `min_concurrency` above `concurrency`, `backoff_base` above `backoff_max`, leases shorter than 10 probe intervals, and durations over 30 days.
- **Eviction-policy warning:** workers log a warning at startup if Redis has a memory limit and a `maxmemory-policy` other than `noeviction`, since eviction silently deletes queued tasks.
- **`Debug` for `Registry`.**

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

## [0.1.0]

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
