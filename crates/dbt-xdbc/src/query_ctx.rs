//! Query abstraction used to carry the query sources and associated
//! metadata around adapter code.

use chrono::{DateTime, Utc};
use dbt_common::constants::EXECUTING;
use log;
use serde_json::json;
use std::fmt::Write;

/// Query source plus metadata.
#[derive(Clone, Debug)]
pub struct QueryCtx {
    // Adapter type executing this query
    adapter_type: String,
    // Model executing this query
    node_unique_id: Option<String>,
    // Actual query content
    sql: Option<String>,
    // Time this instance was created
    created_at: DateTime<Utc>,
    // Description (abribrary string) associated with the query
    desc: Option<String>,
}

impl QueryCtx {
    fn create(
        adapter_type: impl Into<String>,
        node_unique_id: Option<String>,
        sql: Option<String>,
        desc: Option<String>,
    ) -> Self {
        Self {
            adapter_type: adapter_type.into(),
            node_unique_id,
            sql,
            created_at: Utc::now(),
            desc,
        }
    }

    /// Create a new query with the given adapter type.
    pub fn new(adapter_type: impl Into<String>) -> Self {
        Self::create(adapter_type, None, None, None)
    }

    /// Creates a new context by keeping other fields same but
    /// updating unique node id.
    pub fn with_node_id(&self, node_unique_id: impl Into<String>) -> Self {
        // We never allow unique id to be reassigned
        assert!(self.node_unique_id.is_none());
        Self::create(
            self.adapter_type.clone(),
            Some(node_unique_id.into()),
            self.sql.clone(),
            self.desc.clone(),
        )
    }

    /// Creates a new context by keeping other fields same and setting
    /// the given sql query.
    pub fn with_sql(&self, sql: impl Into<String>) -> Self {
        // We allow creating new queries by replacing sql
        Self::create(
            self.adapter_type.clone(),
            self.node_unique_id.clone(),
            Some(sql.into()),
            self.desc.clone(),
        )
    }

    /// Create a new context by keeping other fields same and using
    /// the given description.
    pub fn with_desc(&self, desc: impl Into<String>) -> Self {
        // We never allow one to reassign description
        assert!(self.desc.is_none());
        Self::create(
            self.adapter_type.clone(),
            self.node_unique_id.clone(),
            self.sql.clone(),
            Some(desc.into()),
        )
    }

    /// Return unique node id associated with this context
    pub fn node_id(&self) -> Option<String> {
        self.node_unique_id.clone()
    }

    /// Returns adapter type in this context.
    pub fn adapter_type(&self) -> String {
        self.adapter_type.clone()
    }

    /// Returns time this instance was created.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Returns time this instance was created as a string.
    pub fn created_at_as_str(&self) -> String {
        self.created_at.to_rfc3339()
    }

    /// Returns a clone of the actual sql code carried by this
    /// instance.
    pub fn sql(&self) -> Option<String> {
        self.sql.clone()
    }

    /// Returns a clone of the description associated with the
    /// context.
    pub fn desc(&self) -> Option<String> {
        self.desc.clone()
    }

    /// Format query context as we want to see it in a log file and log it in query_log
    pub fn log_for_execution(&self) {
        let mut buf = String::new();

        writeln!(&mut buf, "-- created_at: {}", self.created_at_as_str()).unwrap();
        writeln!(&mut buf, "-- dialect: {}", self.adapter_type()).unwrap();

        let node_id = match self.node_id() {
            Some(id) => id,
            None => "not available".to_string(),
        };
        writeln!(&mut buf, "-- node_id: {node_id}").unwrap();

        match self.desc() {
            Some(desc) => writeln!(&mut buf, "-- desc: {desc}").unwrap(),
            None => writeln!(&mut buf, "-- desc: not provided").unwrap(),
        }

        if let Some(sql) = self.sql() {
            write!(&mut buf, "{sql}").unwrap();
            if !sql.ends_with(";") {
                write!(&mut buf, ";").unwrap();
            }
        }

        if node_id != "not available" {
            log::debug!(target: EXECUTING, name = "SQLQuery", data:serde = json!({ "node_info": { "unique_id": node_id } }); "{buf}");
        } else {
            log::debug!(target: EXECUTING, name = "SQLQuery"; "{buf}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_desc() {
        let query_ctx = QueryCtx::new("fake").with_desc("this is a really good query");
        assert_eq!(query_ctx.desc().unwrap(), "this is a really good query");
    }

    #[test]
    #[should_panic]
    fn test_desc_twice() {
        QueryCtx::new("fake").with_desc("abc").with_desc("123");
    }

    #[test]
    fn test_unique_id() {
        let query_ctx = QueryCtx::new("fake").with_node_id("123");
        assert_eq!(query_ctx.node_id().unwrap(), "123");
    }

    #[test]
    #[should_panic]
    fn test_unique_id_twice() {
        QueryCtx::new("fake")
            .with_node_id("123")
            .with_node_id("abc");
    }

    #[test]
    fn test_sql() {
        let query_ctx = QueryCtx::new("fake").with_sql("select 1");
        assert_eq!(query_ctx.sql().unwrap(), "select 1");
    }

    #[test]
    fn test_log_for_execution() {
        let query_ctx = QueryCtx::new("test_adapter")
            .with_node_id("test_node_123")
            .with_sql("SELECT * FROM test_table")
            .with_desc("Test query for logging");

        // Should not panic
        query_ctx.log_for_execution();
    }
}
