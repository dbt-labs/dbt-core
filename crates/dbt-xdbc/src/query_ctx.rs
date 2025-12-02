/// Context carrying metadata associated with a query.
#[derive(Clone, Debug, Default)]
pub struct QueryCtx {
    // Model executing this query
    node_unique_id: Option<String>,
    // Execution Phase
    phase: Option<&'static str>,
    // Description (abribrary string) associated with the query
    desc: Option<String>,
}

impl QueryCtx {
    /// Create a new Query Context with a description.
    pub fn new(description: impl Into<String>) -> Self {
        QueryCtx {
            node_unique_id: None,
            phase: None,
            desc: Some(description.into()),
        }
    }

    /// Set the unique node id associated with this context.
    ///
    /// Re-assigning the node id will panic in debug builds.
    pub fn with_node_id(mut self, node_unique_id: impl Into<String>) -> Self {
        debug_assert!(
            self.node_unique_id.is_none(),
            "unexpected reassignment of node_unique_id"
        );
        self.node_unique_id = Some(node_unique_id.into());
        self
    }

    pub fn with_desc(mut self, desc: impl Into<String>) -> Self {
        self.set_desc(desc.into());
        self
    }

    pub fn set_desc(&mut self, desc: impl Into<String>) {
        debug_assert!(
            self.desc.is_none(),
            "unexpected reassignment of description"
        );
        self.desc = Some(desc.into());
    }

    pub fn with_phase(mut self, phase: &'static str) -> Self {
        self.phase = Some(phase);
        self
    }

    /// Return unique node id associated with this context
    pub fn node_id(&self) -> Option<&String> {
        self.node_unique_id.as_ref()
    }

    /// Returns a clone of the description associated with the
    /// context.
    pub fn desc(&self) -> Option<&String> {
        self.desc.as_ref()
    }

    /// Returns the Execution Phase
    pub fn phase(&self) -> Option<&'static str> {
        self.phase
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_desc() {
        let query_ctx = QueryCtx::default().with_desc("this is a really good query");
        assert_eq!(query_ctx.desc().unwrap(), "this is a really good query");
    }

    #[test]
    #[should_panic]
    fn test_desc_twice() {
        QueryCtx::default().with_desc("abc").with_desc("123");
    }

    #[test]
    fn test_unique_id() {
        let query_ctx = QueryCtx::default().with_node_id("123");
        assert_eq!(query_ctx.node_id().unwrap(), "123");
    }

    #[test]
    #[should_panic]
    fn test_unique_id_twice() {
        QueryCtx::default().with_node_id("123").with_node_id("abc");
    }
}
