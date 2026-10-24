use serde::{Deserialize, Serialize};
use std::fmt;
use std::marker::PhantomData;
use uuid::Uuid;

/// A typed handle naming a processor that takes `In` as its context and produces `Out`.
///
/// Declare one per processor type and share it between producers and workers, so chains can
/// only connect steps whose output and input types match:
///
/// ```
/// use redis_task_graph::TaskType;
/// const PARSE: TaskType<String, Vec<String>> = TaskType::new("parse");
/// ```
pub struct TaskType<In, Out> {
    name: &'static str,
    _types: PhantomData<fn(In) -> Out>,
}

impl<In, Out> TaskType<In, Out> {
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            _types: PhantomData,
        }
    }

    pub const fn name(&self) -> &'static str {
        self.name
    }
}

impl<In, Out> Clone for TaskType<In, Out> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<In, Out> Copy for TaskType<In, Out> {}

impl<In, Out> fmt::Debug for TaskType<In, Out> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("TaskType").field(&self.name).finish()
    }
}

/// Anything that names a processor taking `In` and producing `Out`: a [`TaskType`], or a plain
/// string for an untyped processor whose output is treated as `()`.
pub trait IntoTaskType<In, Out> {
    fn into_name(self) -> String;
}

impl<In, Out> IntoTaskType<In, Out> for TaskType<In, Out> {
    fn into_name(self) -> String {
        self.name.to_owned()
    }
}

impl<In> IntoTaskType<In, ()> for &str {
    fn into_name(self) -> String {
        self.to_owned()
    }
}

impl<In> IntoTaskType<In, ()> for String {
    fn into_name(self) -> String {
        self
    }
}

/// Unique identifier assigned to a task at submission time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TaskId(pub Uuid);

impl TaskId {
    pub(crate) fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// The serialized form of a task as stored in Redis.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskEnvelope {
    pub id: TaskId,
    pub processor_type: String,
    pub context: serde_json::Value,
    /// Number of previous failed attempts.
    #[serde(default)]
    pub attempt: u32,
    /// Submission time in milliseconds since the Unix epoch.
    pub enqueued_at: u64,
    /// Error message from the most recent failed attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Set when this task is a step of a chain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain: Option<ChainLink>,
}

/// Position of a task within a chain, and the steps that follow it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChainLink {
    pub chain_id: Uuid,
    /// Zero-based index of this step.
    pub step: usize,
    /// Processor types of the steps after this one, in order.
    pub remaining: Vec<String>,
    /// How long the chain's final status is kept after it completes or fails.
    pub result_ttl_secs: u64,
}

impl ChainLink {
    pub fn total_steps(&self) -> usize {
        self.step + 1 + self.remaining.len()
    }
}

/// Field name under which the envelope JSON is stored in each stream entry.
pub(crate) const PAYLOAD_FIELD: &str = "task";

pub(crate) fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_round_trips_through_json() {
        let env = TaskEnvelope {
            id: TaskId::new(),
            processor_type: "email".into(),
            context: serde_json::json!({ "to": "a@b.c" }),
            attempt: 2,
            enqueued_at: 42,
            last_error: Some("boom".into()),
            chain: Some(ChainLink {
                chain_id: Uuid::new_v4(),
                step: 1,
                remaining: vec!["next".into()],
                result_ttl_secs: 60,
            }),
        };
        let json = serde_json::to_string(&env).unwrap();
        assert_eq!(serde_json::from_str::<TaskEnvelope>(&json).unwrap(), env);
    }

    #[test]
    fn envelope_defaults_optional_fields() {
        let json = format!(
            r#"{{"id":"{}","processor_type":"t","context":null,"enqueued_at":1}}"#,
            Uuid::nil()
        );
        let env: TaskEnvelope = serde_json::from_str(&json).unwrap();
        assert_eq!(env.attempt, 0);
        assert_eq!(env.last_error, None);
        assert_eq!(env.chain, None);
    }
}
