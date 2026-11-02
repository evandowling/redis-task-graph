use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::client::TaskQueue;
use crate::error::Result;
use crate::task::{CONTEXT_FIELD, ChainLink, IntoTaskType, PAYLOAD_FIELD, TaskHeader};

/// Default time a finished chain's status and output are kept in Redis.
pub const DEFAULT_RESULT_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// Identifies a submitted chain whose final step produces `Out`.
pub struct ChainId<Out> {
    id: Uuid,
    _out: PhantomData<fn() -> Out>,
}

impl<Out> ChainId<Out> {
    /// Rebuilds a chain ID from a stored UUID. `Out` must match the chain's final output type.
    /// Rebuilds a handle from a chain's UUID, for example one stored by another process.
    /// Nothing checks that the chain's final output is really an `Out`.
    pub fn from_uuid(id: Uuid) -> Self {
        Self {
            id,
            _out: PhantomData,
        }
    }

    /// The chain's UUID, which [`ChainId::from_uuid`] turns back into a handle.
    pub fn uuid(&self) -> Uuid {
        self.id
    }

    /// Reinterprets the chain's final output as another type, e.g. `serde_json::Value`.
    pub fn cast<T>(self) -> ChainId<T> {
        ChainId::from_uuid(self.id)
    }
}

impl<Out> Clone for ChainId<Out> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Out> Copy for ChainId<Out> {}

impl<Out> PartialEq for ChainId<Out> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<Out> Eq for ChainId<Out> {}

impl<Out> fmt::Debug for ChainId<Out> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ChainId").field(&self.id).finish()
    }
}

impl<Out> fmt::Display for ChainId<Out> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.id.fmt(f)
    }
}

/// Progress of a chain. Step indexes are zero-based.
#[derive(Debug, Clone, PartialEq)]
pub enum ChainStatus<Out> {
    /// Step `step` of `total` is queued, running or waiting for a retry.
    Running {
        /// The step now queued or running.
        step: usize,
        /// Number of steps in the chain.
        total: usize,
    },
    /// The final step succeeded with this output.
    Completed(Out),
    /// Step `step` was dead-lettered with this error; later steps never ran.
    Failed {
        /// The step that was dead-lettered.
        step: usize,
        /// The error from its last attempt, with its causes.
        error: String,
    },
}

impl<Out> ChainStatus<Out> {
    /// Whether the chain has completed or failed, so its status won't change again.
    pub fn is_finished(&self) -> bool {
        !matches!(self, Self::Running { .. })
    }
}

/// Builds a chain of tasks in which each step's output becomes the next step's input.
/// Created by [`TaskQueue::chain`].
#[must_use = "a chain does nothing until it is submitted"]
pub struct ChainBuilder<'q, Out> {
    queue: &'q TaskQueue,
    first: String,
    input: serde_json::Result<String>,
    rest: Vec<String>,
    result_ttl: Duration,
    _out: PhantomData<fn() -> Out>,
}

impl<Out> fmt::Debug for ChainBuilder<'_, Out> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let steps: Vec<&str> = std::iter::once(&self.first)
            .chain(&self.rest)
            .map(String::as_str)
            .collect();
        f.debug_struct("ChainBuilder")
            .field("queue", self.queue)
            .field("steps", &steps)
            .field("result_ttl", &self.result_ttl)
            .finish_non_exhaustive()
    }
}

impl<'q, Out> ChainBuilder<'q, Out> {
    pub(crate) fn new<In: Serialize>(queue: &'q TaskQueue, first: String, input: &In) -> Self {
        Self {
            queue,
            first,
            input: serde_json::to_string(input),
            rest: Vec::new(),
            result_ttl: DEFAULT_RESULT_TTL,
            _out: PhantomData,
        }
    }

    /// Appends a step whose input is the previous step's output.
    pub fn then<Next>(self, step: impl IntoTaskType<Out, Next>) -> ChainBuilder<'q, Next> {
        let mut rest = self.rest;
        rest.push(step.into_name());
        ChainBuilder {
            queue: self.queue,
            first: self.first,
            input: self.input,
            rest,
            result_ttl: self.result_ttl,
            _out: PhantomData,
        }
    }

    /// How long the chain's status and output are kept after it finishes.
    pub fn result_ttl(mut self, ttl: Duration) -> Self {
        self.result_ttl = ttl;
        self
    }

    /// Records the chain as running and enqueues its first step.
    ///
    /// # Errors
    ///
    /// [`Error::Serialization`](crate::Error::Serialization) if the input passed to
    /// [`TaskQueue::chain`] couldn't be serialized, or [`Error::Redis`](crate::Error::Redis) if Redis can't be reached or the command fails.
    pub async fn submit(self) -> Result<ChainId<Out>> {
        let chain_id = Uuid::new_v4();
        let link = ChainLink {
            chain_id,
            step: 0,
            remaining: self.rest,
            result_ttl_secs: self.result_ttl.as_secs().max(1),
        };
        let total = link.total_steps();
        let context = self.input?;
        let header = TaskHeader::new(self.first, Some(link));
        let head = serde_json::to_string(&header)?;
        let keys = &self.queue.keys;
        let mut conn = self.queue.conn.clone();
        let _: () = redis::pipe()
            .atomic()
            .hset_multiple(
                keys.chain(&chain_id),
                &[
                    ("state", "running".to_owned()),
                    ("step", "0".to_owned()),
                    ("total", total.to_string()),
                ],
            )
            .ignore()
            .xadd(
                keys.stream(&header.processor_type),
                "*",
                &[
                    (PAYLOAD_FIELD, head.as_str()),
                    (CONTEXT_FIELD, context.as_str()),
                ],
            )
            .ignore()
            .query_async(&mut conn)
            .await?;
        crate::telemetry::task_submitted(self.queue.namespace(), &header.processor_type);
        crate::telemetry::chain_submitted(self.queue.namespace(), &header.processor_type);
        Ok(ChainId::from_uuid(chain_id))
    }
}

/// Parses the fields of a chain's status hash. Returns `None` for an empty (missing) hash.
pub(crate) fn parse_status<Out: DeserializeOwned>(
    fields: HashMap<String, String>,
) -> Result<Option<ChainStatus<Out>>> {
    let num = |name: &str| fields.get(name).and_then(|v| v.parse().ok()).unwrap_or(0);
    Ok(match fields.get("state").map(String::as_str) {
        None => None,
        Some("completed") => {
            let output = fields.get("output").map(String::as_str).unwrap_or("null");
            Some(ChainStatus::Completed(serde_json::from_str(output)?))
        }
        Some("failed") => Some(ChainStatus::Failed {
            step: num("step"),
            error: fields.get("error").cloned().unwrap_or_default(),
        }),
        Some(_) => Some(ChainStatus::Running {
            step: num("step"),
            total: num("total"),
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fields(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn parses_each_state() {
        assert_eq!(parse_status::<u32>(HashMap::new()).unwrap(), None);
        assert_eq!(
            parse_status::<u32>(fields(&[
                ("state", "running"),
                ("step", "1"),
                ("total", "3")
            ]))
            .unwrap(),
            Some(ChainStatus::Running { step: 1, total: 3 })
        );
        assert_eq!(
            parse_status::<u32>(fields(&[("state", "completed"), ("output", "42")])).unwrap(),
            Some(ChainStatus::Completed(42))
        );
        assert_eq!(
            parse_status::<u32>(fields(&[
                ("state", "failed"),
                ("step", "2"),
                ("error", "boom")
            ]))
            .unwrap(),
            Some(ChainStatus::Failed {
                step: 2,
                error: "boom".into()
            })
        );
    }

    #[test]
    fn completed_output_with_wrong_type_is_an_error() {
        assert!(
            parse_status::<u32>(fields(&[("state", "completed"), ("output", "\"x\"")])).is_err()
        );
    }
}
