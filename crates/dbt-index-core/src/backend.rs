//! Read-only abstraction over the parquet artifact set.
//!
//! `Backend` is the gated "untyped SQL access" capability the docs server
//! uses to power its non-feature endpoints (node listing, project info,
//! catalog stats, etc.). The trait surface lives in this OSS crate; the
//! real DuckDB-backed implementation lives in proprietary `dbt-index`
//! and is injected by `dbt-cli` at startup. Without an injected impl,
//! [`UnavailableBackend`] is the default and every method reports the
//! feature is unavailable so callers can render a PLG upsell rather than
//! crashing.
//!
//! Methods are synchronous; HTTP handlers should call them from inside
//! `tokio::task::spawn_blocking` so they don't stall the async runtime.
//!
//! **Streaming:** today this trait collects all batches into a `Vec` before
//! returning. When endpoints whose result sets can grow large land
//! (column-lineage graph, full edge dump), add a sibling
//! `query_arrow_stream` returning a `RecordBatchReader` so handlers can
//! pipe batches directly into the HTTP response body without buffering.

use arrow_array::RecordBatch;

#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    #[error(
        "index backend is not available; rerun `dbt --use-index <run|build|compile|parse>` and ensure the proprietary distribution is installed"
    )]
    NotAvailable,
    #[error("query failed: {0}")]
    Query(String),
    #[error("invalid result shape: {0}")]
    Shape(String),
}

/// SQL access over the dbt parquet index.
///
/// Default impls report "not available" so an empty impl is a valid
/// no-op stub; see [`UnavailableBackend`]. The proprietary distribution
/// overrides every method.
pub trait Backend: Send + Sync {
    /// Whether this backend is wired to a real data source. Hosts use
    /// this for PLG gating before calling [`query_arrow`] etc.
    fn is_available(&self) -> bool {
        false
    }

    /// Whether the named fully-qualified parquet table exists and has rows.
    /// Used for capability detection at server startup.
    fn table_has_rows(&self, _table: &str) -> bool {
        false
    }

    /// Execute a query that returns a single scalar in column 0 of row 0.
    /// Returns `None` if the query produces no rows, fails, or the
    /// backend is unavailable.
    fn query_scalar(&self, _sql: &str) -> Option<String> {
        None
    }

    /// Execute a query and return all result batches as Arrow `RecordBatch`es.
    /// Bounded results only — see streaming note in the module docs.
    fn query_arrow(&self, _sql: &str) -> Result<Vec<RecordBatch>, BackendError> {
        Err(BackendError::NotAvailable)
    }
}

/// No-op default. Inherits the trait's defaults (everything reports
/// unavailable). Use behind `Arc<dyn Backend>` in an injection bundle
/// when the proprietary impl isn't wired in.
pub struct UnavailableBackend;

impl Backend for UnavailableBackend {}
