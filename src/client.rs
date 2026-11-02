use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use redis::AsyncCommands;
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::streams::StreamRangeReply;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::chain::{ChainBuilder, ChainId, ChainStatus, parse_status};
use crate::error::{Error, Result};
use crate::keys::{self, Keys};
use crate::scripts;
use crate::task::{
    CONTEXT_FIELD, IntoTaskType, PAYLOAD_FIELD, Task, TaskEnvelope, TaskHeader, TaskId,
};

/// When a scheduled task becomes due.
enum Due {
    /// This long after the script runs, on Redis's clock.
    After(Duration),
    /// At this time; one before the epoch is treated as already due.
    At(SystemTime),
}

impl Due {
    /// Delays and times beyond this many milliseconds (about 285,000 years) are capped, so the
    /// due time stays exact as a Lua number and a sorted-set score.
    const MAX_MS: u64 = 1 << 53;

    /// The due time as [`scripts::SCHEDULE`] takes it.
    fn spec(&self) -> String {
        let ms = |d: Duration| {
            u64::try_from(d.as_millis())
                .unwrap_or(u64::MAX)
                .min(Self::MAX_MS)
        };
        match self {
            Self::After(delay) => format!("+{}", ms(*delay)),
            Self::At(at) => ms(at.duration_since(UNIX_EPOCH).unwrap_or_default()).to_string(),
        }
    }
}

/// An entry in the dead-letter stream, from [`TaskQueue::dead_letters`].
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum DeadLetter {
    /// A task that failed fatally or ran out of attempts.
    Task(TaskEnvelope),
    /// An entry whose payload wasn't a valid task, so it was never run.
    Undecodable {
        /// The payload as stored, or empty if the entry had none. For an entry that stored the
        /// context in its own field, the envelope and the context joined by a newline.
        payload: String,
        /// Why it couldn't be decoded.
        error: String,
    },
}

impl DeadLetter {
    /// The task, unless the entry couldn't be decoded.
    pub fn task(&self) -> Option<&TaskEnvelope> {
        match self {
            Self::Task(task) => Some(task),
            Self::Undecodable { .. } => None,
        }
    }

    /// Consumes the dead letter and returns its task, unless the entry couldn't be decoded.
    pub fn into_task(self) -> Option<TaskEnvelope> {
        match self {
            Self::Task(task) => Some(task),
            Self::Undecodable { .. } => None,
        }
    }

    /// Workers store an undecodable entry as `{"raw": <payload>, "last_error": <why>}`; retries
    /// whose payload can't be routed are stored as they were.
    fn decode(payload: Option<String>) -> Self {
        let Some(payload) = payload else {
            return Self::Undecodable {
                payload: String::new(),
                error: format!("entry has no `{PAYLOAD_FIELD}` field"),
            };
        };
        match serde_json::from_str::<TaskEnvelope>(&payload) {
            Ok(task) => Self::Task(task),
            Err(e) => {
                #[derive(serde::Deserialize)]
                struct Raw {
                    raw: String,
                    last_error: String,
                }
                match serde_json::from_str::<Raw>(&payload) {
                    Ok(raw) => Self::Undecodable {
                        payload: raw.raw,
                        error: raw.last_error,
                    },
                    Err(_) => Self::Undecodable {
                        payload,
                        error: format!("invalid task payload: {e}"),
                    },
                }
            }
        }
    }
}

/// Handle to a namespaced task queue in Redis. Cheap to clone.
#[derive(Clone)]
pub struct TaskQueue {
    pub(crate) client: redis::Client,
    pub(crate) conn: ConnectionManager,
    pub(crate) keys: Arc<Keys>,
}

impl std::fmt::Debug for TaskQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskQueue")
            .field("namespace", &self.keys.ns())
            .finish_non_exhaustive()
    }
}

impl TaskQueue {
    /// Connects to Redis at `url`; all keys are prefixed with `namespace`.
    ///
    /// # Errors
    ///
    /// [`Error::Redis`] if `url` is invalid or Redis can't be reached, and
    /// [`Error::InvalidNamespace`] if `namespace` is empty or contains `{` or `}`.
    pub async fn connect(url: &str, namespace: impl Into<String>) -> Result<Self> {
        Self::from_client(redis::Client::open(url)?, namespace).await
    }

    /// Like [`TaskQueue::connect`], with a client you have configured yourself.
    ///
    /// # Errors
    ///
    /// [`Error::Redis`] if Redis can't be reached, and [`Error::InvalidNamespace`] if
    /// `namespace` is empty or contains `{` or `}`.
    pub async fn from_client(client: redis::Client, namespace: impl Into<String>) -> Result<Self> {
        let namespace = namespace.into();
        if !keys::valid_namespace(&namespace) {
            return Err(Error::InvalidNamespace(namespace));
        }
        let conn = client.get_connection_manager().await?;
        Ok(Self {
            client,
            conn,
            keys: Arc::new(Keys::new(namespace)),
        })
    }

    /// The namespace that prefixes this queue's keys.
    pub fn namespace(&self) -> &str {
        self.keys.ns()
    }

    /// Redis stream key holding pending tasks for `processor_type`.
    pub fn stream_key(&self, processor_type: &str) -> String {
        self.keys.stream(processor_type)
    }

    /// Name of the consumer group that workers in this namespace join.
    pub fn group_name(&self) -> String {
        self.keys.group()
    }

    /// Enqueues a task to be handled by the processor registered for `task`, which is either a
    /// [`TaskType`](crate::TaskType) or a plain string. The processor's output is discarded.
    ///
    /// # Errors
    ///
    /// [`Error::Serialization`] if `context` can't be serialized, or [`Error::Redis`] if Redis can't be reached or the command fails.
    pub async fn submit<In: Serialize, Out>(
        &self,
        task: impl IntoTaskType<In, Out>,
        context: &In,
    ) -> Result<TaskId> {
        let header = TaskHeader::new(task.into_name(), None);
        let context = serde_json::to_string(context)?;
        let head = serde_json::to_string(&header)?;
        let mut conn = self.conn.clone();
        let _: String = conn
            .xadd(
                self.keys.stream(&header.processor_type),
                "*",
                &[
                    (PAYLOAD_FIELD, head.as_str()),
                    (CONTEXT_FIELD, context.as_str()),
                ],
            )
            .await?;
        crate::telemetry::task_submitted(self.namespace(), &header.processor_type);
        Ok(header.id)
    }

    /// Like [`TaskQueue::submit`], but the task becomes available to workers once `delay` has
    /// passed on Redis's clock. A zero delay queues it at once.
    ///
    /// Workers move due tasks onto the stream every
    /// [`promote_interval`](crate::WorkerConfig::promote_interval), so a task runs up to that
    /// long after it is due, and never before. Until then it waits in the processor type's
    /// delayed set alongside retries, and is stored as one JSON envelope, as retries are.
    ///
    /// # Errors
    ///
    /// [`Error::Serialization`] if `context` can't be serialized, or [`Error::Redis`] if Redis can't be reached or the command fails.
    pub async fn submit_after<In: Serialize, Out>(
        &self,
        task: impl IntoTaskType<In, Out>,
        context: &In,
        delay: Duration,
    ) -> Result<TaskId> {
        let ids = self
            .schedule(task.into_name(), [(context, Due::After(delay))])
            .await?;
        Ok(ids[0])
    }

    /// Like [`TaskQueue::submit_after`], but the task becomes available at `at`. A time that has
    /// already passed queues it at once.
    ///
    /// `at` comes from this process's clock and is compared with Redis's, so clock skew between
    /// the two shifts when the task runs. Prefer [`TaskQueue::submit_after`] for delays measured
    /// from now.
    ///
    /// # Errors
    ///
    /// [`Error::Serialization`] if `context` can't be serialized, or [`Error::Redis`] if Redis can't be reached or the command fails.
    pub async fn submit_at<In: Serialize, Out>(
        &self,
        task: impl IntoTaskType<In, Out>,
        context: &In,
        at: SystemTime,
    ) -> Result<TaskId> {
        let ids = self
            .schedule(task.into_name(), [(context, Due::At(at))])
            .await?;
        Ok(ids[0])
    }

    /// Schedules many tasks of one processor type, each with its own delay, as for
    /// [`TaskQueue::submit_after`]. Returns their ids in order.
    ///
    /// Tasks are sent in batches of up to 1,000, one round trip and one atomic script call per
    /// batch.
    ///
    /// # Errors
    ///
    /// [`Error::Serialization`] if a context can't be serialized, in which case nothing is
    /// scheduled. [`Error::Redis`] if Redis can't be reached or a command fails, in which case
    /// the batches before the failing one have been scheduled.
    pub async fn submit_many_after<'a, In: Serialize + 'a, Out>(
        &self,
        task: impl IntoTaskType<In, Out>,
        tasks: impl IntoIterator<Item = (&'a In, Duration)>,
    ) -> Result<Vec<TaskId>> {
        let tasks = tasks
            .into_iter()
            .map(|(context, delay)| (context, Due::After(delay)));
        self.schedule(task.into_name(), tasks).await
    }

    /// Serializes every task, then runs [`scripts::SCHEDULE`] once per batch.
    async fn schedule<'a, In: Serialize + 'a>(
        &self,
        processor_type: String,
        tasks: impl IntoIterator<Item = (&'a In, Due)>,
    ) -> Result<Vec<TaskId>> {
        const BATCH: usize = 1000;
        let mut ids = Vec::new();
        let mut args = Vec::new();
        for (context, due) in tasks {
            let task = Task {
                header: TaskHeader::new(processor_type.clone(), None),
                context: serde_json::to_string(context)?.into_boxed_str(),
            };
            ids.push(task.header.id);
            args.push((due.spec(), task.to_envelope_json()));
        }
        let (delayed, stream) = (
            self.keys.delayed(&processor_type),
            self.keys.stream(&processor_type),
        );
        let mut conn = self.conn.clone();
        for batch in args.chunks(BATCH) {
            let mut invocation = scripts::SCHEDULE.prepare_invoke();
            invocation.key(&delayed).key(&stream).arg(PAYLOAD_FIELD);
            for (spec, payload) in batch {
                invocation.arg(spec).arg(payload);
            }
            let _: usize = invocation.invoke_async(&mut conn).await?;
            for _ in batch {
                crate::telemetry::task_submitted(self.namespace(), &processor_type);
            }
        }
        Ok(ids)
    }

    /// Starts building a chain whose first step runs `first` with `input`. Add steps with
    /// [`ChainBuilder::then`] and enqueue the chain with [`ChainBuilder::submit`].
    ///
    /// ```no_run
    /// # use redis_task_graph::{TaskQueue, TaskType, ChainStatus};
    /// # use std::time::Duration;
    /// const PARSE: TaskType<String, Vec<u32>> = TaskType::new("parse");
    /// const SUM: TaskType<Vec<u32>, u32> = TaskType::new("sum");
    ///
    /// # async fn run(queue: TaskQueue) -> redis_task_graph::Result<()> {
    /// let chain = queue.chain(PARSE, &"1,2,3".to_string()).then(SUM).submit().await?;
    /// let status = queue.wait_for_chain(&chain, Duration::from_secs(10)).await?;
    /// assert_eq!(status, Some(ChainStatus::Completed(6)));
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Steps whose types don't line up are rejected at compile time:
    ///
    /// ```compile_fail
    /// # use redis_task_graph::{TaskQueue, TaskType};
    /// const PARSE: TaskType<String, Vec<u32>> = TaskType::new("parse");
    /// const SQUARE: TaskType<u32, u32> = TaskType::new("square");
    /// # async fn run(queue: TaskQueue) {
    /// queue.chain(PARSE, &"1,2,3".to_string()).then(SQUARE); // SQUARE needs u32, not Vec<u32>
    /// # }
    /// ```
    pub fn chain<In: Serialize, Out>(
        &self,
        first: impl IntoTaskType<In, Out>,
        input: &In,
    ) -> ChainBuilder<'_, Out> {
        ChainBuilder::new(self, first.into_name(), input)
    }

    /// Returns a chain's status, or `None` if it is unknown or its result has expired.
    ///
    /// # Errors
    ///
    /// [`Error::Redis`] if Redis can't be reached or the command fails. [`Error::Serialization`] if the chain completed with
    /// an output that isn't an `Out`.
    pub async fn chain_status<Out: DeserializeOwned>(
        &self,
        chain: &ChainId<Out>,
    ) -> Result<Option<ChainStatus<Out>>> {
        let mut conn = self.conn.clone();
        let mut fields: HashMap<String, String> =
            conn.hgetall(self.keys.chain(&chain.uuid())).await?;
        if fields.is_empty() {
            // A chain submitted by 0.1 keeps its status under the 0.1 key.
            fields = conn
                .hgetall(self.keys.legacy().chain(&chain.uuid()))
                .await?;
        }
        parse_status(fields)
    }

    /// Polls a chain's status until it finishes or `timeout` elapses, returning the last status.
    /// Polls start 10 ms apart and back off to 500 ms, so a long wait costs Redis little.
    ///
    /// # Errors
    ///
    /// As for [`TaskQueue::chain_status`].
    pub async fn wait_for_chain<Out: DeserializeOwned>(
        &self,
        chain: &ChainId<Out>,
        timeout: Duration,
    ) -> Result<Option<ChainStatus<Out>>> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut interval = Duration::from_millis(10);
        loop {
            let status = self.chain_status(chain).await?;
            let finished = status.as_ref().is_none_or(ChainStatus::is_finished);
            let now = tokio::time::Instant::now();
            if finished || now >= deadline {
                return Ok(status);
            }
            tokio::time::sleep(interval.min(deadline - now)).await;
            interval = (interval * 2).min(Duration::from_millis(500));
        }
    }

    /// Returns up to `count` dead letters, oldest first. Entries that can't be decoded as tasks
    /// are returned as [`DeadLetter::Undecodable`] rather than failing the whole call.
    ///
    /// # Errors
    ///
    /// [`Error::Redis`] if Redis can't be reached or the command fails.
    pub async fn dead_letters(&self, count: usize) -> Result<Vec<DeadLetter>> {
        let mut conn = self.conn.clone();
        // Dead letters written by 0.1 come first: they are older than any written since.
        let mut entries = Vec::with_capacity(count);
        for key in [self.keys.legacy().dead(), self.keys.dead()] {
            let remaining = count - entries.len();
            if remaining == 0 {
                break;
            }
            let reply: StreamRangeReply = conn.xrange_count(key, "-", "+", remaining).await?;
            entries.extend(reply.ids);
        }
        Ok(entries
            .iter()
            .map(|entry| DeadLetter::decode(entry.get::<String>(PAYLOAD_FIELD)))
            .collect())
    }

    /// Opens a dedicated connection for blocking reads, so they don't stall the shared one.
    pub(crate) async fn blocking_connection(
        &self,
        block: std::time::Duration,
    ) -> Result<ConnectionManager> {
        let config = ConnectionManagerConfig::new()
            .set_response_timeout(Some(block + std::time::Duration::from_secs(5)));
        Ok(self
            .client
            .get_connection_manager_with_config(config)
            .await?)
    }
}
