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
//!
//! Opens an in-memory DuckDB and registers a `CREATE VIEW` per
//! `<schema>.<table>.parquet` in `index_dir`. Read-only — no DDL bootstrap,
//! no inserts. Used by the in-binary docs server for untyped SQL access
//! (node listings, project info, etc.) and reused by the typed feature
//! providers in `crate::providers::*` so a single DuckDB connection
//! backs every gated capability.
//!
//! Empty/unreadable parquet files are skipped rather than failing
//! startup — capability detection is the job of [`Backend::table_has_rows`].

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

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::db::Db;

pub struct DuckDbViewsBackend {
    inner: Mutex<Db>,
    index_dir: PathBuf,
}

impl DuckDbViewsBackend {
    pub fn open(index_dir: &Path) -> Result<Self, BackendError> {
        if !index_dir.exists() {
            return Err(BackendError::Query(format!(
                "index directory does not exist: {}\n\n\
                 Run `dbt --write-index <run|build|compile>` to generate parquet artifacts, \
                 or pass --target-path <DIR> pointing at a directory whose `index/` subdirectory contains them.",
                index_dir.display()
            )));
        }
        let mut db = Db::open_memory().map_err(|e| BackendError::Query(e.to_string()))?;
        register_views(&mut db, index_dir)?;
        Ok(Self {
            inner: Mutex::new(db),
            index_dir: index_dir.to_path_buf(),
        })
    }

    pub fn index_dir(&self) -> &Path {
        &self.index_dir
    }
}

impl Backend for DuckDbViewsBackend {
    fn is_available(&self) -> bool {
        true
    }

    fn table_has_rows(&self, table: &str) -> bool {
        let Ok(mut db) = self.inner.lock() else {
            return false;
        };
        db.query_count(&format!("SELECT COUNT(*) FROM {table}")) != "0"
    }

    fn query_scalar(&self, sql: &str) -> Option<String> {
        let mut db = self.inner.lock().ok()?;
        db.query_scalar(sql, 0)
    }

    fn query_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>, BackendError> {
        let mut db = self
            .inner
            .lock()
            .map_err(|e| BackendError::Query(format!("db lock poisoned: {e}")))?;
        db.execute_query(sql)
            .map_err(|e| BackendError::Query(e.to_string()))
    }
}

fn register_views(db: &mut Db, index_dir: &Path) -> Result<(), BackendError> {
    db.execute_update("CREATE SCHEMA IF NOT EXISTS dbt")
        .map_err(|e| BackendError::Query(e.to_string()))?;
    db.execute_update("CREATE SCHEMA IF NOT EXISTS dbt_rt")
        .map_err(|e| BackendError::Query(e.to_string()))?;

    let entries = std::fs::read_dir(index_dir).map_err(|e| BackendError::Query(e.to_string()))?;
    for entry in entries {
        let entry = entry.map_err(|e| BackendError::Query(e.to_string()))?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("parquet") {
            continue;
        }
        let Some(file_stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        let (schema, table) = match file_stem.split_once('.') {
            Some(("dbt", t)) => ("dbt", t),
            Some(("dbt_rt", t)) => ("dbt_rt", t),
            _ => continue,
        };
        let path_str = path.to_string_lossy().replace('\'', "''");
        let sql = format!(
            "CREATE OR REPLACE VIEW {schema}.{table} AS SELECT * FROM read_parquet('{path_str}')"
        );
        // Tolerate per-file failures: schema may not align with a stale parquet etc.
        let _ = db.execute_update(&sql);
    }
    Ok(())
}
