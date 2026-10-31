use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{BoxError, Error, Result};
use crate::heartbeat::Heartbeat;
use crate::task::IntoTaskType;

/// Why an erased handler did not succeed.
#[derive(Debug)]
pub(crate) enum HandlerError {
    /// The context could not be decoded or the output encoded. Retrying cannot help.
    Fatal(String),
    /// The processor returned an error. Retryable.
    Failed(String),
}

pub(crate) type ErasedHandler = Arc<
    dyn Fn(serde_json::Value, Heartbeat) -> BoxFuture<'static, std::result::Result<serde_json::Value, HandlerError>>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub(crate) struct Processor {
    pub(crate) handler: ErasedHandler,
    /// Lease for this processor type's tasks; the worker's lease when `None`.
    pub(crate) lease: Option<Duration>,
    /// Whether the processor was registered with heartbeats. Only those can stall.
    pub(crate) heartbeat: bool,
}

/// Maps processor types to the async functions that handle them.
#[derive(Default, Clone)]
pub struct Registry {
    processors: HashMap<String, Processor>,
}

impl std::fmt::Debug for Registry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut types: Vec<_> = self.processors.keys().collect();
        types.sort();
        f.debug_struct("Registry").field("processor_types", &types).finish()
    }
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `processor` for `task`, which is either a [`TaskType`](crate::TaskType) or a
    /// plain string. The task context is deserialized into `In` before the processor is called,
    /// and its `Out` becomes the input of the next step when the task is part of a chain.
    ///
    /// The task's lease is renewed for as long as the processor runs. To have hung processors
    /// detected and retried, use [`Registry::register_with_heartbeat`] instead.
    pub fn register<In, Out, F, Fut>(
        &mut self,
        task: impl IntoTaskType<In, Out>,
        processor: F,
    ) -> Result<&mut Self>
    where
        In: DeserializeOwned + Send + 'static,
        Out: Serialize + Send + 'static,
        F: Fn(In) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<Out, BoxError>> + Send + 'static,
    {
        self.insert(task, false, move |context, _heartbeat| processor(context))
    }

    /// Registers a processor that also receives a [`Heartbeat`]. Once the processor calls
    /// [`Heartbeat::beat`], it must keep beating at least once per lease: a processor that goes
    /// silent for a whole lease is aborted, and its task is retried. Combine with
    /// [`Registry::set_lease`] to detect hung processors quickly.
    pub fn register_with_heartbeat<In, Out, F, Fut>(
        &mut self,
        task: impl IntoTaskType<In, Out>,
        processor: F,
    ) -> Result<&mut Self>
    where
        In: DeserializeOwned + Send + 'static,
        Out: Serialize + Send + 'static,
        F: Fn(In, Heartbeat) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<Out, BoxError>> + Send + 'static,
    {
        self.insert(task, true, processor)
    }

    fn insert<In, Out, F, Fut>(
        &mut self,
        task: impl IntoTaskType<In, Out>,
        heartbeat: bool,
        processor: F,
    ) -> Result<&mut Self>
    where
        In: DeserializeOwned + Send + 'static,
        Out: Serialize + Send + 'static,
        F: Fn(In, Heartbeat) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<Out, BoxError>> + Send + 'static,
    {
        let processor_type = task.into_name();
        if self.processors.contains_key(&processor_type) {
            return Err(Error::DuplicateProcessor(processor_type));
        }
        let processor = Arc::new(processor);
        let handler: ErasedHandler = Arc::new(move |value, heartbeat| {
            let processor = processor.clone();
            Box::pin(async move {
                let context: In = serde_json::from_value(value)
                    .map_err(|e| HandlerError::Fatal(format!("invalid context: {e}")))?;
                let output = processor(context, heartbeat)
                    .await
                    .map_err(|e| HandlerError::Failed(e.to_string()))?;
                serde_json::to_value(output)
                    .map_err(|e| HandlerError::Fatal(format!("invalid output: {e}")))
            })
        });
        self.processors.insert(processor_type, Processor { handler, lease: None, heartbeat });
        Ok(self)
    }

    /// Sets the lease for one registered processor type, overriding the worker's lease. A short
    /// lease means a crashed worker's tasks, or a silent heartbeat processor, are retried sooner.
    /// Every worker that runs this processor type should use the same lease for it.
    pub fn set_lease(&mut self, processor_type: &str, lease: Duration) -> Result<&mut Self> {
        let processor = self
            .processors
            .get_mut(processor_type)
            .ok_or_else(|| Error::UnknownProcessor(processor_type.to_owned()))?;
        processor.lease = Some(lease);
        Ok(self)
    }

    pub fn processor_types(&self) -> impl Iterator<Item = &str> {
        self.processors.keys().map(String::as_str)
    }

    pub fn is_empty(&self) -> bool {
        self.processors.is_empty()
    }

    pub(crate) fn get(&self, processor_type: &str) -> Option<&Processor> {
        self.processors.get(processor_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TaskType;
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct Ctx {
        n: u32,
    }

    #[tokio::test]
    async fn dispatches_typed_context() {
        let mut reg = Registry::new();
        reg.register("t", |ctx: Ctx| async move {
            if ctx.n == 1 { Ok(()) } else { Err("bad n".into()) }
        })
        .unwrap();
        let h = |v| (reg.get("t").unwrap().handler)(v, Heartbeat::detached());
        assert_eq!(h(serde_json::json!({ "n": 1 })).await.unwrap(), serde_json::Value::Null);
        assert!(matches!(h(serde_json::json!({ "n": 2 })).await, Err(HandlerError::Failed(_))));
        assert!(matches!(h(serde_json::json!({ "x": 1 })).await, Err(HandlerError::Fatal(_))));
    }

    #[tokio::test]
    async fn returns_serialized_output_for_typed_tasks() {
        const DOUBLE: TaskType<u32, u32> = TaskType::new("double");
        let mut reg = Registry::new();
        reg.register(DOUBLE, |n| async move { Ok(n * 2) }).unwrap();
        let h = |v| (reg.get("double").unwrap().handler)(v, Heartbeat::detached());
        assert_eq!(h(serde_json::json!(21)).await.unwrap(), serde_json::json!(42));
    }

    #[test]
    fn lease_can_be_set_only_for_registered_types() {
        let mut reg = Registry::new();
        reg.register("t", |_: Ctx| async { Ok(()) }).unwrap();
        reg.set_lease("t", Duration::from_secs(2)).unwrap();
        assert_eq!(reg.get("t").unwrap().lease, Some(Duration::from_secs(2)));
        assert!(matches!(reg.set_lease("missing", Duration::from_secs(2)), Err(Error::UnknownProcessor(_))));
    }

    #[test]
    fn rejects_duplicate_registration() {
        let mut reg = Registry::new();
        reg.register("t", |_: Ctx| async { Ok(()) }).unwrap();
        assert!(matches!(
            reg.register("t", |_: Ctx| async { Ok(()) }),
            Err(Error::DuplicateProcessor(_))
        ));
    }
}
