# Changelog

All notable changes to this crate are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the crate follows
[Semantic Versioning](https://semver.org/). Before 1.0, a minor version bump (0.1 → 0.2) may
include breaking changes.

## [Unreleased]

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
