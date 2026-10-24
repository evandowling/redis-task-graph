use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{BoxError, Error, Result};
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
    dyn Fn(serde_json::Value) -> BoxFuture<'static, std::result::Result<serde_json::Value, HandlerError>>
        + Send
        + Sync,
>;

/// Maps processor types to the async functions that handle them.
#[derive(Default, Clone)]
pub struct Registry {
    handlers: HashMap<String, ErasedHandler>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `processor` for `task`, which is either a [`TaskType`](crate::TaskType) or a
    /// plain string. The task context is deserialized into `In` before the processor is called,
    /// and its `Out` becomes the input of the next step when the task is part of a chain.
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
        let processor_type = task.into_name();
        if self.handlers.contains_key(&processor_type) {
            return Err(Error::DuplicateProcessor(processor_type));
        }
        let processor = Arc::new(processor);
        let handler: ErasedHandler = Arc::new(move |value| {
            let processor = processor.clone();
            Box::pin(async move {
                let context: In = serde_json::from_value(value)
                    .map_err(|e| HandlerError::Fatal(format!("invalid context: {e}")))?;
                let output = processor(context)
                    .await
                    .map_err(|e| HandlerError::Failed(e.to_string()))?;
                serde_json::to_value(output)
                    .map_err(|e| HandlerError::Fatal(format!("invalid output: {e}")))
            })
        });
        self.handlers.insert(processor_type, handler);
        Ok(self)
    }

    pub fn processor_types(&self) -> impl Iterator<Item = &str> {
        self.handlers.keys().map(String::as_str)
    }

    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }

    pub(crate) fn get(&self, processor_type: &str) -> Option<&ErasedHandler> {
        self.handlers.get(processor_type)
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
        let h = reg.get("t").unwrap();
        assert_eq!(h(serde_json::json!({ "n": 1 })).await.unwrap(), serde_json::Value::Null);
        assert!(matches!(h(serde_json::json!({ "n": 2 })).await, Err(HandlerError::Failed(_))));
        assert!(matches!(h(serde_json::json!({ "x": 1 })).await, Err(HandlerError::Fatal(_))));
    }

    #[tokio::test]
    async fn returns_serialized_output_for_typed_tasks() {
        const DOUBLE: TaskType<u32, u32> = TaskType::new("double");
        let mut reg = Registry::new();
        reg.register(DOUBLE, |n| async move { Ok(n * 2) }).unwrap();
        let h = reg.get("double").unwrap();
        assert_eq!(h(serde_json::json!(21)).await.unwrap(), serde_json::json!(42));
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
