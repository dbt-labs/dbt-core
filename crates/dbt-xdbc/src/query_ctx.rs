use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionPhase {
    Unspecified,
    Render,
    Analyze,
    Run,
}

impl FromStr for ExecutionPhase {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "render" => Ok(ExecutionPhase::Render),
            "analyze" => Ok(ExecutionPhase::Analyze),
            "run" => Ok(ExecutionPhase::Run),
            _ => Err("Invalid execution phase".to_string()),
        }
    }
}

impl fmt::Display for ExecutionPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ExecutionPhase::Unspecified => "unspecified",
            ExecutionPhase::Render => "render",
            ExecutionPhase::Analyze => "analyze",
            ExecutionPhase::Run => "run",
        };
        s.fmt(f)
    }
}

/// Context carrying metadata associated with a query.
#[derive(Clone, Debug)]
pub struct QueryCtx {
    // Model executing this query
    node_unique_id: Option<String>,
    // Execution Phase
    phase: Option<ExecutionPhase>,
    // Time this instance was created
    created_at: DateTime<Utc>,
    // Description (abribrary string) associated with the query
    desc: Option<String>,
}

impl Default for QueryCtx {
    fn default() -> Self {
        QueryCtx::create(None, None, None)
    }
}

impl QueryCtx {
    fn create(
        node_unique_id: Option<String>,
        phase: Option<ExecutionPhase>,
        desc: Option<String>,
    ) -> Self {
        Self {
            node_unique_id,
            phase,
            created_at: Utc::now(),
            desc,
        }
    }

    /// Creates a new context by keeping other fields same but
    /// updating unique node id.
    pub fn with_node_id(self, node_unique_id: impl Into<String>) -> Self {
        // We never allow unique id to be reassigned
        assert!(self.node_unique_id.is_none());
        Self::create(Some(node_unique_id.into()), self.phase, self.desc)
    }

    /// Create a new context by keeping other fields same and using
    /// the given description.
    pub fn with_desc(self, desc: impl Into<String>) -> Self {
        let mut ctx = self;
        ctx.set_desc(desc.into());
        ctx
    }

    pub fn set_desc(&mut self, desc: impl Into<String>) {
        // We never allow one to reassign description
        assert!(self.desc.is_none());
        self.desc = Some(desc.into());
    }

    /// Creates a new context by keeping other fields same and setting
    /// the given execution phase.
    pub fn with_phase(self, phase: ExecutionPhase) -> Self {
        Self::create(self.node_unique_id, Some(phase), self.desc)
    }

    /// Return unique node id associated with this context
    pub fn node_id(&self) -> Option<&String> {
        self.node_unique_id.as_ref()
    }

    /// Returns time this instance was created.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Returns time this instance was created as a string.
    pub fn created_at_as_str(&self) -> String {
        self.created_at.to_rfc3339()
    }

    /// Returns a clone of the description associated with the
    /// context.
    pub fn desc(&self) -> Option<&String> {
        self.desc.as_ref()
    }

    /// Returns the Execution Phase
    pub fn phase(&self) -> Option<ExecutionPhase> {
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
