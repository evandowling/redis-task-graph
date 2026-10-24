use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use futures::future::BoxFuture;
use serde::de::DeserializeOwned;

use crate::error::{BoxError, Error, Result};

/// Why an erased handler did not succeed.
#[derive(Debug)]
pub(crate) enum HandlerError {
    /// The stored context could not be decoded into the processor's type. Not retryable.
    Decode(String),
    /// The processor returned an error. Retryable.
    Failed(String),
}

pub(crate) type ErasedHandler =
    Arc<dyn Fn(serde_json::Value) -> BoxFuture<'static, std::result::Result<(), HandlerError>> + Send + Sync>;

/// Maps processor types to the async functions that handle them.
#[derive(Default, Clone)]
pub struct Registry {
    handlers: HashMap<String, ErasedHandler>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `processor` for `processor_type`. The task context is deserialized into `C`
    /// before the processor is called.
    pub fn register<C, F, Fut>(&mut self, processor_type: impl Into<String>, processor: F) -> Result<&mut Self>
    where
        C: DeserializeOwned + Send + 'static,
        F: Fn(C) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<(), BoxError>> + Send + 'static,
    {
        let processor_type = processor_type.into();
        if self.handlers.contains_key(&processor_type) {
            return Err(Error::DuplicateProcessor(processor_type));
        }
        let processor = Arc::new(processor);
        let handler: ErasedHandler = Arc::new(move |value| {
            let processor = processor.clone();
            Box::pin(async move {
                let context: C =
                    serde_json::from_value(value).map_err(|e| HandlerError::Decode(e.to_string()))?;
                processor(context)
                    .await
                    .map_err(|e| HandlerError::Failed(e.to_string()))
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
        assert!(h(serde_json::json!({ "n": 1 })).await.is_ok());
        assert!(matches!(h(serde_json::json!({ "n": 2 })).await, Err(HandlerError::Failed(_))));
        assert!(matches!(h(serde_json::json!({ "x": 1 })).await, Err(HandlerError::Decode(_))));
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
