//! Redis key and name builders. Every key is prefixed with the queue namespace.

#[derive(Debug, Clone)]
pub(crate) struct Keys {
    ns: String,
}

impl Keys {
    pub(crate) fn new(ns: impl Into<String>) -> Self {
        Self { ns: ns.into() }
    }

    pub(crate) fn ns(&self) -> &str {
        &self.ns
    }

    /// Stream holding pending tasks for one processor type.
    pub(crate) fn stream(&self, processor_type: &str) -> String {
        format!("{}:stream:{}", self.ns, processor_type)
    }

    /// Prefix that, followed by a processor type, forms a stream key. Used by Lua scripts.
    pub(crate) fn stream_prefix(&self) -> String {
        format!("{}:stream:", self.ns)
    }

    /// Consumer group shared by all workers in this namespace.
    pub(crate) fn group(&self) -> String {
        format!("{}:workers", self.ns)
    }

    /// Sorted set of tasks waiting for a retry, scored by due time in milliseconds.
    pub(crate) fn delayed(&self) -> String {
        format!("{}:delayed", self.ns)
    }

    /// Hash holding a chain's status and, once it finishes, its output or error.
    pub(crate) fn chain(&self, chain_id: &uuid::Uuid) -> String {
        format!("{}:chain:{}", self.ns, chain_id)
    }

    /// Stream of tasks that exhausted their attempts or could not be decoded.
    pub(crate) fn dead(&self) -> String {
        format!("{}:dead", self.ns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_namespaced_keys() {
        let k = Keys::new("app");
        assert_eq!(k.stream("email"), "app:stream:email");
        assert_eq!(k.stream_prefix(), "app:stream:");
        assert_eq!(k.group(), "app:workers");
        assert_eq!(k.delayed(), "app:delayed");
        assert_eq!(k.dead(), "app:dead");
        let id = uuid::Uuid::nil();
        assert_eq!(k.chain(&id), format!("app:chain:{id}"));
    }
}
