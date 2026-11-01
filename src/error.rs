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

    #[error("no processor is registered for type `{0}`")]
    UnknownProcessor(String),

    #[error("invalid worker configuration: {0}")]
    InvalidConfig(String),

    #[error("invalid namespace `{0}`: it must be non-empty and contain no `{{` or `}}`")]
    InvalidNamespace(String),
}

pub type Result<T> = std::result::Result<T, Error>;
