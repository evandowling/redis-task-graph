//! A Redis-backed task queue with typed, async task processors.
//!
//! Tasks are submitted with a processor type and a serializable context. A background
//! [`Worker`] claims pending tasks from Redis Streams and calls the processor registered for
//! each task's type with its deserialized context.
//!
//! Tasks can also be linked into chains with [`TaskQueue::chain`], where each step's output
//! becomes the next step's input. Declaring processors as [`TaskType`] constants makes the
//! compiler check that adjacent steps' types line up.
//!
//! With the `otel` cargo feature, workers and producers record OpenTelemetry metrics
//! (throughput, queue and run times, pushback state, queue depth) through the global meter
//! provider. See `monitoring/README.md` for the metric list and a ready-made OpenObserve setup.
//!
//! Delivery is at-least-once and normally exactly-once: each task is claimed by a single worker
//! under a lease kept alive by heartbeats. If a worker crashes or stalls past its lease, another
//! worker reclaims the task, so processors with side effects should be idempotent.

mod chain;
mod client;
mod error;
mod keys;
mod limiter;
mod monitor;
mod registry;
mod scripts;
mod task;
mod telemetry;
mod worker;

pub use chain::{ChainBuilder, ChainId, ChainStatus, DEFAULT_RESULT_TTL};
pub use client::TaskQueue;
pub use error::{BoxError, Error, Result};
pub use registry::Registry;
pub use task::{ChainLink, IntoTaskType, TaskEnvelope, TaskId, TaskType};
pub use worker::{Worker, WorkerConfig, WorkerHandle, WorkerStats};

// Dependencies whose types appear in this crate's API, so users can name them at matching versions.
pub use redis;
pub use serde_json;
pub use uuid;
