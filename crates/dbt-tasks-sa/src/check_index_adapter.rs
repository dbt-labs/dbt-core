//! Query the metadata index through a real DuckDB **adapter**, rather than a raw ADBC connection.
//!
//! Checks have historically read the index via `dbt-index-core`'s `DuckDbViewsBackend`, which opens
//! an ADBC connection directly and so bypasses the adapter layer entirely. Two consequences: check
//! SQL cannot use `adapter.*` or any macro that dispatches on the adapter, and the driver it loads is
//! `Backend::DuckDBExtended` — the bespoke dbt-built DuckDB carrying internal extensions, the same
//! driver sidecar mode selects — rather than vanilla `Backend::DuckDB`.
//!
//! This module is the adapter-backed replacement. It builds a DuckDB adapter over an in-memory
//! database, registers the index parquet as views, and executes check SQL through it.
//!
//! Three properties were verified before writing this (see `adapter_index_spike` in `check_task`):
//! the adapter constructs with no profile or credentials; views registered by one call are still
//! visible to the next, despite DuckDB connections deliberately not being cached
//! (`guard.persist = adapter_type != AdapterType::DuckDB`); and vanilla DuckDB reads parquet, which
//! matters because the whole point of the extended driver is its extra extensions.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use dbt_adapter::adapter::Adapter;
use dbt_adapter::adapter::adapter_factory::{AdapterFactory, DefaultAdapterFactory};
use dbt_adapter::sql_types::DefaultTypeOpsFactory;
use dbt_adapter_core::AdapterType;
use dbt_common::cancellation::CancellationToken;
use dbt_index_core::db::{COLUMNS_VIEW_DDL, GRAPH_NODES_VIEWS_DDL, split_ddl_statements};
use dbt_jinja_utils::info_schema::PARSE_SAFE_VIEWS;
use dbt_schemas::schemas::relations::DEFAULT_RESOLVED_QUOTING;

/// Build a DuckDB adapter over an in-memory database.
///
/// The config is deliberately minimal and hand-rolled rather than inherited from the project:
/// `AdapterFactoryImpl` silently returns a *mock* adapter when the config carries
/// `execute: sidecar|service` or `user: mock_test_user`, so reusing a project's mapping would hand
/// back something that never executes.
///
/// `AdapterType::DuckDB` maps to vanilla `Backend::DuckDB` in the factory, which is the point.
///
/// Public because rendering needs an adapter too, for a different reason: a check's Render task wants
/// one purely so macros and utilities *dispatch* on duckdb and emit duckdb SQL. That use needs no
/// views registered, so it takes this rather than [`open_index_adapter`].
pub fn in_memory_duckdb_adapter(token: CancellationToken) -> Result<Arc<Adapter>, String> {
    let mut config = dbt_yaml::Mapping::new();
    config.insert("type".into(), "duckdb".into());
    config.insert("path".into(), ":memory:".into());

    DefaultAdapterFactory
        .create_adapter(
            AdapterType::DuckDB,
            config,
            Arc::new(DefaultTypeOpsFactory),
            None,
            BTreeMap::new(),
            None,
            DEFAULT_RESOLVED_QUOTING,
            None,
            token,
            None,
            None,
        )
        .map_err(|e| format!("could not open an in-memory duckdb adapter: {e}"))
}

/// Base tables the parse-safe views in [`PARSE_SAFE_VIEWS_DDL`] are defined in terms of.
/// Registered from parquet like any other view, but deliberately not in [`PARSE_SAFE_VIEWS`]:
/// they mix parse-safe columns with compile-filled or never-populated ones, which is exactly what
/// the derived views project away. A check can only reach them through `dbt.graph_nodes` /
/// `dbt.columns`, never directly.
const PARSE_SAFE_VIEW_BASE_TABLES: &[&str] = &["nodes", "node_columns"];

/// Register every `dbt.*.parquet` / `dbt_rt.*.parquet` in `index_dir` as a view.
///
/// Paths are made **absolute** here rather than executing the index's own `views.sql`, which emits
/// relative paths and would therefore depend on the process working directory.
///
/// Per-file failures are tolerated: a stale parquet whose schema no longer matches should not make
/// every check in the project unrunnable. A check that needed that table fails on its own when it
/// queries a view that isn't there, which is the more useful error.
fn register_index_views(adapter: &Adapter, index_dir: &Path) -> Result<(), String> {
    adapter
        .execute_without_state(None, "create schema if not exists dbt", false, None)
        .map_err(|e| format!("could not create schema dbt: {e}"))?;

    // Only the views a parse-time check may read, plus the base tables the derived ones among them
    // are defined over. Registering the whole index directory would expose tables that are empty at
    // parse — and an empty table does not error, it returns zero rows, which a check reports as a
    // pass. The allowlist is therefore a correctness boundary, not tidiness.
    //
    // Tracks which base tables actually registered: `node_columns.parquet` in particular does not
    // exist until something has run static analysis, so a project that never has does not get one,
    // and the derived view built on it (`dbt.columns`) must be skipped rather than fail the whole
    // batch — a check that never reads `dbt.columns` should not go down because of it.
    let mut registered = std::collections::HashSet::new();
    for view in PARSE_SAFE_VIEWS.iter().chain(PARSE_SAFE_VIEW_BASE_TABLES) {
        let path = index_dir.join(format!("dbt.{view}.parquet"));
        if !path.exists() {
            // A missing view is left unregistered on purpose: a check that needs it then fails with
            // "table does not exist", which is loud. Creating an empty stand-in would make the same
            // check pass while having read nothing. If the view is a base table for one of the
            // derived views below, that view's own `CREATE VIEW` is skipped rather than attempted,
            // for the same reason: attempting it would fail loudly for every check, not just the one
            // that reads it.
            continue;
        }
        // Absolute path, not the index's own `views.sql`, which emits relative paths and so would
        // depend on the process working directory.
        let quoted = path.to_string_lossy().replace('\'', "''");
        let sql =
            format!("create or replace view dbt.{view} as select * from read_parquet('{quoted}')");
        adapter
            .execute_without_state(None, &sql, false, None)
            .map_err(|e| format!("could not register view dbt.{view}: {e}"))?;
        registered.insert(*view);
    }

    // The parse-safe views themselves (`dbt.graph_nodes`, `dbt.models`, …) are defined as SQL over
    // the base tables just registered, not as their own parquet files, so they are created here
    // rather than picked up by the loop above. Each half only runs if its base table registered;
    // `dbt.columns`'s absence must not take down `dbt.graph_nodes` and vice versa.
    if registered.contains("nodes") {
        for stmt in split_ddl_statements(GRAPH_NODES_VIEWS_DDL) {
            adapter
                .execute_without_state(None, &stmt, false, None)
                .map_err(|e| format!("could not register parse-safe view ({e}): {stmt}"))?;
        }
    }
    if registered.contains("node_columns") {
        for stmt in split_ddl_statements(COLUMNS_VIEW_DDL) {
            adapter
                .execute_without_state(None, &stmt, false, None)
                .map_err(|e| format!("could not register parse-safe view ({e}): {stmt}"))?;
        }
    }
    Ok(())
}

/// Open the index through a DuckDB adapter with its views registered.
pub fn open_index_adapter(
    index_dir: &Path,
    token: CancellationToken,
) -> Result<Arc<Adapter>, String> {
    let adapter = in_memory_duckdb_adapter(token)?;
    register_index_views(&adapter, index_dir)?;
    Ok(adapter)
}

/// Run a check's SQL and return its rows as Arrow batches.
///
/// `AgateTable` is Arrow-backed, so this hands back the same shape `evaluate_batches` already
/// consumes — the scoping and preview logic is unchanged by moving onto the adapter.
pub fn query_index(adapter: &Adapter, sql: &str) -> Result<Vec<RecordBatch>, String> {
    let (_response, table) = adapter
        .execute_without_state(None, sql, true, None)
        .map_err(|e| e.to_string())?;
    let batch = table.to_record_batch();
    Ok(vec![RecordBatch::clone(&batch)])
}

/// The properties this module depends on, pinned as tests.
///
/// The open question is view lifetime. `connection.rs` sets `guard.persist = adapter_type !=
/// AdapterType::DuckDB`, so DuckDB connections are deliberately *not* cached — each adapter call
/// drops its connection. Views registered by one call therefore survive only if the cached `Database`
/// handle keeps the catalog alive across connections (DuckDB MVCC). Nothing asserts that today, and
/// the whole "checks execute through the adapter" direction depends on it.
///
/// So: register a view through the adapter, then read it back through a *separate* call.
#[cfg(test)]
mod tests {
    use dbt_adapter::adapter::adapter_factory::{AdapterFactory, DefaultAdapterFactory};
    use dbt_adapter::sql_types::DefaultTypeOpsFactory;
    use dbt_adapter_core::AdapterType;
    use dbt_common::cancellation::never_cancels;
    use dbt_schemas::schemas::relations::DEFAULT_RESOLVED_QUOTING;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    /// A real (not mock) in-memory DuckDB adapter, built from a hand-rolled config mapping rather
    /// than a project profile.
    ///
    /// The mapping is deliberately minimal: `AdapterFactoryImpl` short-circuits to a *mock* adapter
    /// when the config carries `execute: sidecar|service` or `user: mock_test_user`, so inheriting a
    /// project's config would silently give us something that never executes.
    fn index_adapter() -> Arc<dbt_adapter::adapter::Adapter> {
        let mut config = dbt_yaml::Mapping::new();
        config.insert("type".into(), "duckdb".into());
        config.insert("path".into(), ":memory:".into());

        DefaultAdapterFactory
            .create_adapter(
                AdapterType::DuckDB,
                config,
                Arc::new(DefaultTypeOpsFactory),
                None,
                BTreeMap::new(),
                None,
                DEFAULT_RESOLVED_QUOTING,
                None,
                never_cancels(),
                None,
                None,
            )
            .expect("in-memory duckdb adapter should construct without credentials")
    }

    #[test]
    fn adapter_constructs_without_a_profile() {
        let adapter = index_adapter();
        assert_eq!(adapter.adapter_type(), AdapterType::DuckDB);
    }

    /// Answers Alexander's question directly: what happens if we load `Backend::DuckDB` instead of
    /// `DuckDBExtended`?
    ///
    /// The factory maps `AdapterType::DuckDB` to the vanilla `Backend::DuckDB`, so this adapter is
    /// already on it. The index is Parquet, and the extended driver is described as carrying "internal
    /// extensions" — so the question that matters is whether *vanilla* DuckDB can read Parquet at all.
    /// Writing then reading a Parquet file through the adapter settles it.
    #[test]
    fn vanilla_duckdb_reads_parquet() {
        let tmp = tempfile::TempDir::new().unwrap();
        let parquet = tmp.path().join("t.parquet");
        let adapter = index_adapter();

        adapter
            .execute_without_state(
                None,
                &format!(
                    "copy (select 7 as answer) to '{}' (format parquet)",
                    parquet.display()
                ),
                false,
                None,
            )
            .expect("vanilla duckdb should write parquet");
        assert!(parquet.exists(), "parquet file should have been created");

        // Absolute path on purpose: the index's own `views.sql` uses *relative* paths, so running it
        // verbatim through an adapter would depend on the process CWD.
        adapter
            .execute_without_state(
                None,
                &format!(
                    "create view idx as select * from read_parquet('{}')",
                    parquet.display()
                ),
                false,
                None,
            )
            .expect("vanilla duckdb should register a view over parquet");

        let (_resp, table) = adapter
            .execute_without_state(None, "select answer from idx", true, None)
            .expect("querying the parquet-backed view should succeed");
        assert_eq!(
            table.num_rows(),
            1,
            "expected one row back through the view"
        );
    }

    /// The load-bearing one: does a view registered in one call survive into the next?
    #[test]
    fn views_survive_across_adapter_calls() {
        let adapter = index_adapter();

        adapter
            .execute_without_state(
                None,
                "create view spike as select 42 as answer",
                false,
                None,
            )
            .expect("create view should succeed");

        // Separate call — a fresh connection, since DuckDB connections are not persisted.
        let (_resp, table) = adapter
            .execute_without_state(None, "select answer from spike", true, None)
            .expect(
                "view must still exist on a later call, or checks cannot execute through the adapter",
            );

        assert_eq!(
            table.num_rows(),
            1,
            "expected the single row back from the view"
        );
    }
}
