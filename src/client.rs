use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use redis::AsyncCommands;
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::streams::StreamRangeReply;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::chain::{ChainBuilder, ChainId, ChainStatus, parse_status};
use crate::error::Result;
use crate::keys::Keys;
use crate::task::{IntoTaskType, PAYLOAD_FIELD, TaskEnvelope, TaskId, now_millis};

/// Handle to a namespaced task queue in Redis. Cheap to clone.
#[derive(Clone)]
pub struct TaskQueue {
    pub(crate) client: redis::Client,
    pub(crate) conn: ConnectionManager,
    pub(crate) keys: Arc<Keys>,
}

impl TaskQueue {
    /// Connects to Redis at `url`; all keys are prefixed with `namespace`.
    pub async fn connect(url: &str, namespace: impl Into<String>) -> Result<Self> {
        Self::from_client(redis::Client::open(url)?, namespace).await
    }

    pub async fn from_client(client: redis::Client, namespace: impl Into<String>) -> Result<Self> {
        let conn = client.get_connection_manager().await?;
        Ok(Self {
            client,
            conn,
            keys: Arc::new(Keys::new(namespace)),
        })
    }

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
    pub async fn submit<In: Serialize, Out>(
        &self,
        task: impl IntoTaskType<In, Out>,
        context: &In,
    ) -> Result<TaskId> {
        let envelope = TaskEnvelope {
            id: TaskId::new(),
            processor_type: task.into_name(),
            context: serde_json::to_value(context)?,
            attempt: 0,
            enqueued_at: now_millis(),
            last_error: None,
            chain: None,
        };
        let payload = serde_json::to_string(&envelope)?;
        let mut conn = self.conn.clone();
        let _: String = conn
            .xadd(
                self.keys.stream(&envelope.processor_type),
                "*",
                &[(PAYLOAD_FIELD, payload)],
            )
            .await?;
        Ok(envelope.id)
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
    pub async fn chain_status<Out: DeserializeOwned>(
        &self,
        chain: &ChainId<Out>,
    ) -> Result<Option<ChainStatus<Out>>> {
        let mut conn = self.conn.clone();
        let fields: HashMap<String, String> = conn.hgetall(self.keys.chain(&chain.uuid())).await?;
        parse_status(fields)
    }

    /// Polls a chain's status until it finishes or `timeout` elapses, returning the last status.
    pub async fn wait_for_chain<Out: DeserializeOwned>(
        &self,
        chain: &ChainId<Out>,
        timeout: Duration,
    ) -> Result<Option<ChainStatus<Out>>> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let status = self.chain_status(chain).await?;
            let finished = status.as_ref().is_none_or(ChainStatus::is_finished);
            if finished || tokio::time::Instant::now() >= deadline {
                return Ok(status);
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Returns up to `count` dead-lettered tasks, oldest first.
    pub async fn dead_letters(&self, count: usize) -> Result<Vec<TaskEnvelope>> {
        let mut conn = self.conn.clone();
        let reply: StreamRangeReply = conn.xrange_count(self.keys.dead(), "-", "+", count).await?;
        reply
            .ids
            .iter()
            .filter_map(|entry| entry.get::<String>(PAYLOAD_FIELD))
            .map(|payload| serde_json::from_str(&payload).map_err(Into::into))
            .collect()
    }

    /// Opens a dedicated connection for blocking reads, so they don't stall the shared one.
    pub(crate) async fn blocking_connection(
        &self,
        block: std::time::Duration,
    ) -> Result<ConnectionManager> {
        let config = ConnectionManagerConfig::new()
            .set_response_timeout(Some(block + std::time::Duration::from_secs(5)));
        Ok(self.client.get_connection_manager_with_config(config).await?)
    }
}
