use dbt_frontend_common::dialect::Dialect;
use std::fmt::Debug;

/// Trait for SQL statement splitting functionality
pub trait StmtSplitter: Send + Sync + Debug {
    /// Split a SQL string into individual statements
    ///
    /// The implementation should:
    /// - Split the SQL into individual statements based on delimiters
    /// - Filter out empty or comment-only statements
    /// - Handle dialect-specific syntax correctly
    fn split(&self, sql: &str, dialect: Dialect) -> Vec<String>;
}

/// Naive implementation of StmtSplitter
///
/// Used as a placeholder until a more robust solution is made available
/// to this crate.
#[derive(Debug)]
pub struct NaiveStmtSplitter;

impl StmtSplitter for NaiveStmtSplitter {
    fn split(&self, sql: &str, _dialect: Dialect) -> Vec<String> {
        sql.split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect()
    }
}
