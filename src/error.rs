use thiserror::Error;

/// Boxed error type returned by task processors.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Errors produced by the library itself (not by task processors).
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// A Redis command or connection failed.
    #[error("redis error: {0}")]
    Redis(#[from] redis::RedisError),

    /// A context, output or stored record couldn't be serialized or deserialized.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// [`Registry::register`](crate::Registry::register) was called twice for one type.
    #[error("a processor is already registered for type `{0}`")]
    DuplicateProcessor(String),

    /// [`Worker::spawn`](crate::Worker::spawn) was given an empty registry.
    #[error("worker has no registered processors")]
    NoProcessors,

    /// A setting named a processor type that isn't registered.
    #[error("no processor is registered for type `{0}`")]
    UnknownProcessor(String),

    /// A worker setting is out of range; the message names it.
    #[error("invalid worker configuration: {0}")]
    InvalidConfig(String),

    /// The operating system couldn't start the worker's monitor thread.
    #[error("failed to start the worker's monitor thread: {0}")]
    Thread(#[source] std::io::Error),

    /// A namespace was empty or contained `{` or `}`, which would break its hash tag.
    #[error("invalid namespace `{0}`: it must be non-empty and contain no `{{` or `}}`")]
    InvalidNamespace(String),
}

/// Result type for the library's own operations.
pub type Result<T> = std::result::Result<T, Error>;
