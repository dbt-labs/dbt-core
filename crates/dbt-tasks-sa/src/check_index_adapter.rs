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
use dbt_index_core::info_schema::parse_safe::{self, BASE_SCHEMA};
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

/// Register the index as the views a parse-time check reads.
///
/// Two layers, both from `index_dir`'s parquet. The index's own tables go into
/// `dbt_internal`, and the parse-safe views over them into `dbt` — so `info_schema('models')`
/// resolves, and `dbt.nodes` does not exist to be read by accident. See
/// `dbt_index_core::info_schema::parse_safe` for why the split is a correctness boundary and
/// not tidiness.
///
/// Paths are made **absolute** here rather than executing the index's own `views.sql`, which
/// emits relative paths and would therefore depend on the process working directory.
///
/// A *missing* parquet is tolerated — it is skipped, and a check that needed that table then
/// fails on its own with "table does not exist", which is the more useful error. One that is
/// present but fails to register still aborts the batch. That is the opposite of
/// `register_info_schema_views`, which serves a single user-named view and so has no stake in
/// whether the rest of the directory reads.
fn register_index_views(adapter: &Adapter, index_dir: &Path) -> Result<(), String> {
    for schema in [parse_safe::VIEW_SCHEMA, BASE_SCHEMA] {
        adapter
            .execute_without_state(
                None,
                &format!("create schema if not exists {schema}"),
                false,
                None,
            )
            .map_err(|e| format!("could not create schema {schema}: {e}"))?;
    }

    // Only the tables the parse-safe views are built from. Registering the whole index
    // directory would expose tables that are empty at parse — and an empty table does not
    // error, it returns zero rows, which a check reports as a pass.
    //
    // Tracks which ones actually registered: `node_columns.parquet` in particular does not
    // exist until something has written columns, so a project without them still gets every
    // other view.
    let mut registered = std::collections::HashSet::new();
    for table in parse_safe::base_tables() {
        let path = index_dir.join(format!("dbt.{table}.parquet"));
        if !path.exists() {
            // Left unregistered on purpose: a check that needs it then fails with "table does
            // not exist", which is loud. An empty stand-in would make the same check pass
            // having read nothing.
            continue;
        }
        let quoted = path.to_string_lossy().replace('\'', "''");
        let sql = format!(
            "create or replace view {BASE_SCHEMA}.{table} as select * from read_parquet('{quoted}')"
        );
        adapter
            .execute_without_state(None, &sql, false, None)
            .map_err(|e| format!("could not register {BASE_SCHEMA}.{table}: {e}"))?;
        registered.insert(table);
    }

    // A view whose tables are all present is created; one missing a table is skipped rather
    // than attempted, so `dbt.node_columns`'s absence cannot take down `dbt.models`.
    for view in parse_safe::VIEWS {
        if !view.base_tables().iter().all(|t| registered.contains(t)) {
            continue;
        }
        let sql = view.create_view_sql().map_err(|e| e.to_string())?;
        adapter
            .execute_without_state(None, &sql, false, None)
            .map_err(|e| format!("could not register parse-safe view ({e}): {sql}"))?;
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

/// Register every `dbt*.*.parquet` in `dir` as a view.
///
/// Used for `target/info_schema/v<n>/`, whose files are already the public
/// shape (`dbt.models.parquet`, `dbt_rt.run_results.parquet`, …). Do not layer
/// the index's parse-safe SQL views on top: those project `dbt.nodes`, which
/// the information schema does not write.
///
/// `dbt_internal` is deliberately left unregistered. `show_info` already filters `Ns::DbtInternal`
/// out of `--info` and out of the `info_schema()` Jinja function, but that only constrains what the
/// function *returns* — the SQL it renders into still executes here, so any `dbt_internal.*` view
/// this registered would stay reachable by naming it directly. Not registering it is what actually
/// makes it unreachable, and keeps the gate and the registered surface in agreement.
fn register_info_schema_views(adapter: &Adapter, dir: &Path) -> Result<(), String> {
    for schema in ["dbt", "dbt_rt"] {
        adapter
            .execute_without_state(
                None,
                &format!("create schema if not exists {schema}"),
                false,
                None,
            )
            .map_err(|e| format!("could not create schema {schema}: {e}"))?;
    }

    let entries = std::fs::read_dir(dir).map_err(|e| {
        format!(
            "could not read information schema at {}: {e}",
            dir.display()
        )
    })?;
    for entry in entries {
        let entry = entry.map_err(|e| format!("could not read information schema entry: {e}"))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some((schema, table)) = parse_index_parquet_name(&name) else {
            continue;
        };
        let path = entry.path();
        let quoted = path.to_string_lossy().replace('\'', "''");
        let sql = format!(
            "create or replace view {schema}.{table} as select * from read_parquet('{quoted}')"
        );
        if let Err(e) = adapter.execute_without_state(None, &sql, false, None) {
            // Tolerated on purpose: one unreadable or schema-mismatched file — a truncated
            // `dbt_rt.run_results.parquet` left by an interrupted run, say — must not fail
            // `dbt show --info models`, which never reads it. The view is simply left unregistered,
            // so a query that does want it fails with "table does not exist", naming the problem.
            tracing::warn!("could not register information schema view {schema}.{table}: {e}");
        }
    }
    Ok(())
}

fn parse_index_parquet_name(name: &str) -> Option<(&str, &str)> {
    let stem = name.strip_suffix(".parquet")?;
    let (schema, table) = stem.split_once('.')?;
    // `dbt_internal` is absent on purpose; see `register_info_schema_views`.
    if schema != "dbt" && schema != "dbt_rt" {
        return None;
    }
    if table.is_empty() || !table.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return None;
    }
    Some((schema, table))
}

/// Open `target/info_schema/v<n>/` through a DuckDB adapter.
pub fn open_info_schema_adapter(
    info_schema_dir: &Path,
    token: CancellationToken,
) -> Result<Arc<Adapter>, String> {
    let adapter = in_memory_duckdb_adapter(token)?;
    register_info_schema_views(&adapter, info_schema_dir)?;
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

    /// The render-time allowlist and the views actually registered are two lists in two
    /// crates: `dbt-jinja-utils` cannot depend on the index, so it names the views rather
    /// than deriving them. Nothing but this makes them agree — an allowlisted name with no
    /// view expands to a missing relation, and a registered view nobody allowlists is
    /// unreachable.
    #[test]
    fn the_render_allowlist_names_exactly_the_registered_views() {
        let registered: Vec<&str> = dbt_index_core::info_schema::parse_safe::VIEWS
            .iter()
            .map(|v| v.name)
            .collect();
        let mut allowed = dbt_jinja_utils::info_schema::PARSE_SAFE_VIEWS.to_vec();
        let mut expected = registered.clone();
        allowed.sort_unstable();
        expected.sort_unstable();
        assert_eq!(allowed, expected);
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

    /// Write `select 1 as n` to `<dir>/<name>` as parquet, via a throwaway adapter.
    fn write_parquet(dir: &std::path::Path, name: &str) {
        let path = dir.join(name);
        index_adapter()
            .execute_without_state(
                None,
                &format!(
                    "copy (select 1 as n) to '{}' (format parquet)",
                    path.display()
                ),
                false,
                None,
            )
            .unwrap_or_else(|e| panic!("should write {name}: {e}"));
    }

    /// `show_info` filters `Ns::DbtInternal` out of `--info` and out of the `info_schema()`
    /// Jinja function, but that only governs what the function *returns*; the SQL it renders
    /// into runs against this adapter. So `dbt_internal` has to be absent from the adapter
    /// itself, or `--inline "... from dbt_internal.<t> ..."` walks straight past the gate.
    #[test]
    fn dbt_internal_is_not_reachable_from_the_information_schema_adapter() {
        let tmp = tempfile::TempDir::new().unwrap();
        write_parquet(tmp.path(), "dbt.models.parquet");
        write_parquet(tmp.path(), "dbt_internal.secrets.parquet");

        let adapter = super::open_info_schema_adapter(tmp.path(), never_cancels())
            .expect("the information schema adapter should open");

        adapter
            .execute_without_state(None, "select n from dbt.models", true, None)
            .expect("a public view should be queryable");
        assert!(
            adapter
                .execute_without_state(None, "select n from dbt_internal.secrets", true, None)
                .is_err(),
            "dbt_internal must not be reachable, gate or no gate"
        );
    }

    /// One unreadable file must not take down the view the user actually asked for: an
    /// interrupted run can leave a truncated `dbt_rt.run_results.parquet` behind, and
    /// `dbt show --info models` does not read it.
    #[test]
    fn an_unreadable_parquet_does_not_take_down_the_other_views() {
        let tmp = tempfile::TempDir::new().unwrap();
        write_parquet(tmp.path(), "dbt.models.parquet");
        std::fs::write(
            tmp.path().join("dbt_rt.run_results.parquet"),
            b"not parquet",
        )
        .unwrap();

        let adapter = super::open_info_schema_adapter(tmp.path(), never_cancels())
            .expect("one bad file should not fail the whole registration");

        adapter
            .execute_without_state(None, "select n from dbt.models", true, None)
            .expect("the readable view should still be queryable");
        assert!(
            adapter
                .execute_without_state(None, "select * from dbt_rt.run_results", true, None)
                .is_err(),
            "the unreadable file should be left unregistered, not stubbed out empty"
        );
    }
}
