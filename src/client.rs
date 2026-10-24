use std::sync::Arc;

use redis::AsyncCommands;
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::streams::StreamRangeReply;
use serde::Serialize;

use crate::error::Result;
use crate::keys::Keys;
use crate::task::{PAYLOAD_FIELD, TaskEnvelope, TaskId, now_millis};

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

    /// Enqueues a task to be handled by the processor registered for `processor_type`.
    pub async fn submit<C: Serialize>(&self, processor_type: &str, context: &C) -> Result<TaskId> {
        let envelope = TaskEnvelope {
            id: TaskId::new(),
            processor_type: processor_type.to_owned(),
            context: serde_json::to_value(context)?,
            attempt: 0,
            enqueued_at: now_millis(),
            last_error: None,
        };
        let payload = serde_json::to_string(&envelope)?;
        let mut conn = self.conn.clone();
        let _: String = conn
            .xadd(self.keys.stream(processor_type), "*", &[(PAYLOAD_FIELD, payload)])
            .await?;
        Ok(envelope.id)
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
