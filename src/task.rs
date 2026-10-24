use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

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
    }
}
