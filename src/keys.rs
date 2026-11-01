//! Redis key and name builders. Every key starts with the namespace in a hash tag, `{ns}`, so a
//! namespace's keys share one Redis Cluster slot and scripts can touch several of them.
//!
//! [`Keys::legacy`] builds the 0.1 layout, which had no hash tag and one retry set per
//! namespace. Workers still read it so that tasks written by 0.1 are drained after an upgrade.

#[derive(Debug, Clone)]
pub(crate) struct Keys {
    ns: String,
    /// What every key starts with: `{ns}`, or `ns` in the legacy layout.
    prefix: String,
    legacy: bool,
}

impl Keys {
    pub(crate) fn new(ns: impl Into<String>) -> Self {
        let ns = ns.into();
        Self { prefix: format!("{{{ns}}}"), ns, legacy: false }
    }

    /// The 0.1 layout for the same namespace.
    pub(crate) fn legacy(&self) -> Self {
        Self { ns: self.ns.clone(), prefix: self.ns.clone(), legacy: true }
    }

    pub(crate) fn is_legacy(&self) -> bool {
        self.legacy
    }

    pub(crate) fn ns(&self) -> &str {
        &self.ns
    }

    /// Stream holding pending tasks for one processor type.
    pub(crate) fn stream(&self, processor_type: &str) -> String {
        format!("{}:stream:{}", self.prefix, processor_type)
    }

    /// Prefix that, followed by a processor type, forms a stream key.
    pub(crate) fn stream_prefix(&self) -> String {
        format!("{}:stream:", self.prefix)
    }

    /// Consumer group shared by all workers in this namespace. A group name, not a key, so it
    /// is the same in both layouts.
    pub(crate) fn group(&self) -> String {
        format!("{}:workers", self.ns)
    }

    /// Sorted set of one processor type's tasks waiting for a retry, scored by due time in
    /// milliseconds. The legacy layout has one set for every type.
    pub(crate) fn delayed(&self, processor_type: &str) -> String {
        if self.legacy {
            format!("{}:delayed", self.prefix)
        } else {
            format!("{}:delayed:{}", self.prefix, processor_type)
        }
    }

    /// Hash holding a chain's status and, once it finishes, its output or error.
    pub(crate) fn chain(&self, chain_id: &uuid::Uuid) -> String {
        format!("{}:chain:{}", self.prefix, chain_id)
    }

    /// Stream of tasks that exhausted their attempts or could not be decoded.
    pub(crate) fn dead(&self) -> String {
        format!("{}:dead", self.prefix)
    }
}

/// Whether `ns` can be used as a namespace: braces would break the hash tag.
pub(crate) fn valid_namespace(ns: &str) -> bool {
    !ns.is_empty() && !ns.contains(['{', '}'])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_hash_tagged_keys() {
        let k = Keys::new("app");
        assert_eq!(k.stream("email"), "{app}:stream:email");
        assert_eq!(k.stream_prefix(), "{app}:stream:");
        assert_eq!(k.group(), "app:workers");
        assert_eq!(k.delayed("email"), "{app}:delayed:email");
        assert_eq!(k.dead(), "{app}:dead");
        let id = uuid::Uuid::nil();
        assert_eq!(k.chain(&id), format!("{{app}}:chain:{id}"));
    }

    #[test]
    fn legacy_layout_matches_0_1() {
        let k = Keys::new("app").legacy();
        assert_eq!(k.stream("email"), "app:stream:email");
        assert_eq!(k.group(), "app:workers");
        assert_eq!(k.delayed("email"), "app:delayed");
        assert_eq!(k.dead(), "app:dead");
        let id = uuid::Uuid::nil();
        assert_eq!(k.chain(&id), format!("app:chain:{id}"));
    }

    #[test]
    fn namespaces_with_braces_are_invalid() {
        assert!(valid_namespace("app"));
        assert!(!valid_namespace("a{b"));
        assert!(!valid_namespace("a}b"));
        assert!(!valid_namespace(""));
    }
}
