use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::value::RawValue;

use crate::error::{BoxError, Error, Result};
use crate::control::TaskControl;
use crate::task::IntoTaskType;

/// Why an erased handler did not succeed.
#[derive(Debug)]
pub(crate) enum HandlerError {
    /// The context could not be decoded or the output encoded. Retrying cannot help.
    Fatal(String),
    /// The processor returned an error. Retryable.
    Failed(String),
}

/// Formats `error` followed by each of its causes, as `top: cause: root`. A cause whose message
/// already appears in the text so far is skipped, since many errors include their source in
/// their own message.
pub(crate) fn error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        let text = cause.to_string();
        if !message.contains(&text) {
            message.push_str(": ");
            message.push_str(&text);
        }
        source = cause.source();
    }
    message
}

pub(crate) type ErasedHandler = Arc<
    dyn Fn(&RawValue, TaskControl) -> BoxFuture<'static, std::result::Result<Box<RawValue>, HandlerError>>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub(crate) struct Processor {
    pub(crate) handler: ErasedHandler,
    /// Lease for this processor type's tasks; the worker's lease when `None`.
    pub(crate) lease: Option<Duration>,
    /// Whether the processor was registered with a [`TaskControl`], so it may beat. Only those
    /// can stall.
    pub(crate) controlled: bool,
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
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `processor` for `task`, which is either a [`TaskType`](crate::TaskType) or a
    /// plain string. The task context is deserialized into `In` before the processor is called,
    /// and its `Out` becomes the input of the next step when the task is part of a chain.
    ///
    /// The task's lease is renewed for as long as the processor runs. To have hung processors
    /// detected and retried, or to stop when the lease is lost or the worker shuts down, use
    /// [`Registry::register_with_control`] instead.
    ///
    /// # Errors
    ///
    /// [`Error::DuplicateProcessor`] if a processor is already registered for `task`.
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
        self.insert(task, false, move |context, _control| processor(context))
    }

    /// Registers a processor that also receives a [`TaskControl`], for heartbeats and
    /// cancellation. Once the processor calls [`TaskControl::beat`], it must keep beating at
    /// least once per lease: a processor that goes silent for a whole lease is aborted, and its
    /// task is retried. Combine with [`Registry::set_lease`] to detect hung processors quickly.
    ///
    /// # Errors
    ///
    /// [`Error::DuplicateProcessor`] if a processor is already registered for `task`.
    pub fn register_with_control<In, Out, F, Fut>(
        &mut self,
        task: impl IntoTaskType<In, Out>,
        processor: F,
    ) -> Result<&mut Self>
    where
        In: DeserializeOwned + Send + 'static,
        Out: Serialize + Send + 'static,
        F: Fn(In, TaskControl) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<Out, BoxError>> + Send + 'static,
    {
        self.insert(task, true, processor)
    }

    /// The 0.1 name for [`Registry::register_with_control`].
    ///
    /// # Errors
    ///
    /// As for [`Registry::register_with_control`].
    #[deprecated(since = "0.2.0", note = "renamed to `register_with_control`")]
    pub fn register_with_heartbeat<In, Out, F, Fut>(
        &mut self,
        task: impl IntoTaskType<In, Out>,
        processor: F,
    ) -> Result<&mut Self>
    where
        In: DeserializeOwned + Send + 'static,
        Out: Serialize + Send + 'static,
        F: Fn(In, TaskControl) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<Out, BoxError>> + Send + 'static,
    {
        self.register_with_control(task, processor)
    }

    fn insert<In, Out, F, Fut>(
        &mut self,
        task: impl IntoTaskType<In, Out>,
        controlled: bool,
        processor: F,
    ) -> Result<&mut Self>
    where
        In: DeserializeOwned + Send + 'static,
        Out: Serialize + Send + 'static,
        F: Fn(In, TaskControl) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<Out, BoxError>> + Send + 'static,
    {
        let processor_type = task.into_name();
        if self.processors.contains_key(&processor_type) {
            return Err(Error::DuplicateProcessor(processor_type));
        }
        let processor = Arc::new(processor);
        let handler: ErasedHandler = Arc::new(move |raw: &RawValue, control| {
            // Decoded here, straight from the stored JSON text, so the future doesn't need its
            // own copy of the context.
            let context = serde_json::from_str::<In>(raw.get());
            let processor = processor.clone();
            Box::pin(async move {
                let context = context.map_err(|e| HandlerError::Fatal(format!("invalid context: {e}")))?;
                let output = processor(context, control)
                    .await
                    .map_err(|e| HandlerError::Failed(error_chain(&*e)))?;
                serde_json::value::to_raw_value(&output)
                    .map_err(|e| HandlerError::Fatal(format!("invalid output: {e}")))
            })
        });
        self.processors.insert(processor_type, Processor { handler, lease: None, controlled });
        Ok(self)
    }

    /// Sets the lease for one registered processor type, overriding the worker's lease. A short
    /// lease means a crashed worker's tasks, or a processor that stops beating, are retried sooner.
    /// Every worker that runs this processor type should use the same lease for it.
    ///
    /// # Errors
    ///
    /// [`Error::UnknownProcessor`] if no processor is registered for `processor_type`. A lease
    /// that is too short is rejected later, by [`Worker::spawn`](crate::Worker::spawn).
    pub fn set_lease(&mut self, processor_type: &str, lease: Duration) -> Result<&mut Self> {
        let processor = self
            .processors
            .get_mut(processor_type)
            .ok_or_else(|| Error::UnknownProcessor(processor_type.to_owned()))?;
        processor.lease = Some(lease);
        Ok(self)
    }

    /// The registered processor types, in no particular order.
    pub fn processor_types(&self) -> impl Iterator<Item = &str> {
        self.processors.keys().map(String::as_str)
    }

    /// Whether no processors are registered.
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
        let h = |v: serde_json::Value| {
            let raw = serde_json::value::to_raw_value(&v).unwrap();
            (reg.get("t").unwrap().handler)(&raw, TaskControl::detached())
        };
        assert_eq!(h(serde_json::json!({ "n": 1 })).await.unwrap().get(), "null");
        assert!(matches!(h(serde_json::json!({ "n": 2 })).await, Err(HandlerError::Failed(_))));
        assert!(matches!(h(serde_json::json!({ "x": 1 })).await, Err(HandlerError::Fatal(_))));
    }

    #[tokio::test]
    async fn returns_serialized_output_for_typed_tasks() {
        const DOUBLE: TaskType<u32, u32> = TaskType::new("double");
        let mut reg = Registry::new();
        reg.register(DOUBLE, |n| async move { Ok(n * 2) }).unwrap();
        let raw = serde_json::value::to_raw_value(&21).unwrap();
        let output = (reg.get("double").unwrap().handler)(&raw, TaskControl::detached()).await.unwrap();
        assert_eq!(output.get(), "42");
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

    #[derive(Debug)]
    struct Wrapped(&'static str, Option<Box<dyn std::error::Error + Send + Sync>>);
    impl std::fmt::Display for Wrapped {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }
    impl std::error::Error for Wrapped {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.1.as_deref().map(|e| e as _)
        }
    }

    #[test]
    fn error_chain_joins_causes_and_skips_repeats() {
        let root = Wrapped("connection refused", None);
        let mid = Wrapped("fetch failed: connection refused", Some(Box::new(root)));
        let top = Wrapped("sync user", Some(Box::new(mid)));
        assert_eq!(error_chain(&top), "sync user: fetch failed: connection refused");
    }
}
