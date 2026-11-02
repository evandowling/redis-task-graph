use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
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
    /// Names a processor type. Producers and workers must use the same name.
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            _types: PhantomData,
        }
    }

    /// The processor type's name.
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
    /// The processor type's name.
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
///
/// Envelopes compare equal when their contexts have the same JSON text, so two contexts that
/// differ only in formatting or key order are not equal.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct TaskEnvelope {
    /// Assigned at submission; kept across retries.
    pub id: TaskId,
    /// Which processor runs this task.
    pub processor_type: String,
    /// The processor's input as JSON text, kept as-is rather than parsed, so a task's context is
    /// decoded once, straight into the processor's type. Decode it yourself with
    /// `serde_json::from_str(context.get())`.
    pub context: Box<RawValue>,
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

impl PartialEq for TaskEnvelope {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.processor_type == other.processor_type
            && self.context.get() == other.context.get()
            && self.attempt == other.attempt
            && self.enqueued_at == other.enqueued_at
            && self.last_error == other.last_error
            && self.chain == other.chain
    }
}

/// Position of a task within a chain, and the steps that follow it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ChainLink {
    /// The chain this step belongs to; see [`ChainId::uuid`](crate::ChainId::uuid).
    pub chain_id: Uuid,
    /// Zero-based index of this step.
    pub step: usize,
    /// Processor types of the steps after this one, in order.
    pub remaining: Vec<String>,
    /// How long the chain's final status is kept after it completes or fails.
    pub result_ttl_secs: u64,
}

impl ChainLink {
    /// Number of steps in the whole chain.
    pub fn total_steps(&self) -> usize {
        self.step + 1 + self.remaining.len()
    }
}

/// Field name under which the envelope JSON is stored in each stream entry.
pub(crate) const PAYLOAD_FIELD: &str = "task";

/// Field name under which a stream entry stores the task's context, when the envelope in
/// [`PAYLOAD_FIELD`] leaves it out.
pub(crate) const CONTEXT_FIELD: &str = "context";

/// A [`TaskEnvelope`] without its context. Submitting a task, or advancing a chain, writes the
/// header and the context to separate fields of the stream entry, so a worker can hand the
/// context to the processor without first scanning it to find where it ends.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct TaskHeader {
    pub id: TaskId,
    pub processor_type: String,
    #[serde(default)]
    pub attempt: u32,
    pub enqueued_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain: Option<ChainLink>,
}

impl TaskHeader {
    /// The header of a task that has just been submitted.
    pub fn new(processor_type: String, chain: Option<ChainLink>) -> Self {
        Self {
            id: TaskId::new(),
            processor_type,
            attempt: 0,
            enqueued_at: now_millis(),
            last_error: None,
            chain,
        }
    }
}

/// A claimed task. Its context is the JSON text as stored, which the processor decodes.
#[derive(Debug)]
pub(crate) struct Task {
    pub header: TaskHeader,
    pub context: Box<str>,
}

impl Task {
    /// Decodes a stream entry's fields. If the entry has no `context` field, it was written by
    /// 0.2 or earlier, or promoted from the retry set, and `task` holds the whole envelope.
    ///
    /// On failure, returns the error and the entry's text, for the dead-letter stream: an entry
    /// with both fields gives the two joined by a newline.
    pub fn decode(task: String, context: Option<String>) -> Result<Self, (serde_json::Error, String)> {
        let Some(context) = context else {
            return match serde_json::from_str::<TaskEnvelope>(&task) {
                Ok(env) => Ok(Self {
                    header: TaskHeader {
                        id: env.id,
                        processor_type: env.processor_type,
                        attempt: env.attempt,
                        enqueued_at: env.enqueued_at,
                        last_error: env.last_error,
                        chain: env.chain,
                    },
                    context: env.context.into(),
                }),
                Err(e) => Err((e, task)),
            };
        };
        match serde_json::from_str::<TaskHeader>(&task) {
            Ok(header) => Ok(Self { header, context: context.into_boxed_str() }),
            Err(e) => Err((e, format!("{task}\n{context}"))),
        }
    }

    /// The whole envelope as one JSON object, as the retry set and dead-letter stream store it.
    /// The context is copied as-is, so a context that isn't valid JSON makes an invalid
    /// envelope, which `dead_letters()` reports as undecodable.
    pub fn to_envelope_json(&self) -> String {
        let header = serde_json::to_string(&self.header).expect("header serializes");
        // The header is an object with at least an id, so it ends with `}` and needs a comma.
        let open = &header[..header.len() - 1];
        let mut json = String::with_capacity(header.len() + self.context.len() + 12);
        json.push_str(open);
        json.push_str(",\"context\":");
        json.push_str(&self.context);
        json.push('}');
        json
    }
}

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
            context: serde_json::value::to_raw_value(&serde_json::json!({ "to": "a@b.c" })).unwrap(),
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
    fn task_decodes_both_layouts_and_reassembles_the_envelope() {
        let header = TaskHeader::new("t".into(), None);
        let context = r#"{"to":"a@b.c"}"#;
        let split = Task::decode(serde_json::to_string(&header).unwrap(), Some(context.into())).unwrap();
        assert_eq!((&split.header, &*split.context), (&header, context));

        let combined = split.to_envelope_json();
        let env: TaskEnvelope = serde_json::from_str(&combined).unwrap();
        assert_eq!((env.id, env.context.get()), (header.id, context));
        let task = Task::decode(combined, None).unwrap();
        assert_eq!((&task.header, &*task.context), (&header, context));

        let (_, raw) = Task::decode("{".into(), Some("[1]".into())).unwrap_err();
        assert_eq!(raw, "{\n[1]");
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
