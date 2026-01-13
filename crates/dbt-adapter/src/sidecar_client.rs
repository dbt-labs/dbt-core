//! SidecarClient trait for subprocess-based adapter execution.
//!
//! This module defines the interface for adapters that delegate execution
//! to a subprocess (sidecar) or external service. The actual implementation
//! is provided by closed-source crates that wrap dbt-db-runner.
//!
//! Design: SA crates expose only the trait interface. Implementation details
//! (RunnerManager, subprocess spawning, message protocol) remain in proprietary code.

use std::fmt::Debug;

use arrow::record_batch::RecordBatch;
use minijinja::State;

use crate::errors::AdapterResult;

// Re-export types needed by trait implementations
pub use dbt_xdbc::connection::Connection;
pub use dbt_xdbc::query_ctx::QueryCtx;

/// Trait for adapters that execute via subprocess (sidecar) or HTTP service.
///
/// This trait defines the interface for delegation to dbt-db-runner or similar
/// execution backends. It allows Snowflake (and other) adapters to route
/// execution to DuckDB or other engines without exposing implementation details
/// in SA crates.
///
/// # Design Principles
///
/// - **Interface Only**: SA crates define the trait, closed-source implements it
/// - **Stateless**: Each method is self-contained, no hidden state dependencies
/// - **Session Management**: Clients handle session lifecycle (init/shutdown)
/// - **Connection Pooling**: Clients may maintain connection pools internally
///
/// # Implementation Notes
///
/// Implementations typically:
/// - Wrap `RunnerManager` or HTTP client
/// - Translate adapter calls into `TaskMessage` protocol
/// - Handle subprocess lifecycle and error recovery
/// - Manage session isolation and state directories
pub trait SidecarClient: Debug + Send + Sync {
    /// Execute SQL and optionally fetch results.
    ///
    /// # Arguments
    ///
    /// * `ctx` - Query context (warehouse config, logging, tracing)
    /// * `sql` - Fully compiled SQL to execute
    /// * `fetch` - Whether to fetch and return result rows
    ///
    /// # Returns
    ///
    /// * `Some(RecordBatch)` if `fetch = true` and query returns rows
    /// * `None` if `fetch = false` or query returns no rows (DDL, DML)
    ///
    /// # Errors
    ///
    /// Returns adapter error if:
    /// - SQL execution fails
    /// - Subprocess communication fails
    /// - Results cannot be parsed
    fn execute(&self, ctx: &QueryCtx, sql: &str, fetch: bool)
    -> AdapterResult<Option<RecordBatch>>;

    /// Create a new connection within this session.
    ///
    /// Connections may share session state but have independent transaction scope.
    /// Implementations may use connection pooling internally.
    ///
    /// # Arguments
    ///
    /// * `state` - Optional minijinja state for context
    /// * `node_id` - Optional dbt node identifier for logging/tracing
    ///
    /// # Returns
    ///
    /// A new connection handle that can execute queries independently.
    fn new_connection(
        &self,
        state: Option<&State>,
        node_id: Option<String>,
    ) -> AdapterResult<Box<dyn Connection>>;

    /// Gracefully shutdown the session.
    ///
    /// Closes connections, flushes state, and terminates subprocess if applicable.
    /// Should be called when adapter is dropped or dbt run completes.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Subprocess fails to terminate cleanly
    /// - State cannot be flushed
    fn shutdown(&self) -> AdapterResult<()>;
}
