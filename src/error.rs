use thiserror::Error;

/// Boxed error type returned by task processors.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Errors produced by the library itself (not by task processors).
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("a processor is already registered for type `{0}`")]
    DuplicateProcessor(String),

    #[error("worker has no registered processors")]
    NoProcessors,
}

pub type Result<T> = std::result::Result<T, Error>;
