//! The `info_schema()` Jinja helper available to project quality checks.
//!
//! A check is SQL over the metadata index, and it runs **at parse time** — before anything is
//! compiled or materialized. So the helper deliberately exposes only views whose every column has
//! its final value by the end of parse. Anything later is not merely unavailable, it is *quietly*
//! unavailable: an index column that gets filled during compile reads as NULL at parse, and a check
//! filtering on it finds "violations" that are artefacts of timing rather than of the project.

use minijinja::Value;

/// Views a parse-time check may read.
///
/// `dbt.nodes` and `dbt.node_columns` are **not** on this list: they mix parse-safe columns with
/// ones that are compile-filled (`compiled_code`, `grain`, `classifiers`, …) or that no writer
/// populates at all (`unique_key`, `pre_hook`, … — the real values live in the `config` JSON
/// blob). `dbt.graph_nodes` and `dbt.columns` are the parse-safe projections of those two tables:
/// every listed column enumerated explicitly, never `SELECT *`, so a column that cannot hold data
/// at parse simply is not there — referencing it is a DuckDB binder error, not a silent NULL.
/// `dbt.models` / `dbt.seeds` / … are `dbt.graph_nodes` filtered by `resource_type`, defined in
/// terms of `dbt.graph_nodes` rather than `dbt.nodes` directly, so they inherit the same guarantee.
///
/// Defined once, in `dbt_index_core::db::PARSE_SAFE_VIEWS_DDL`, and registered from there by the
/// check adapter — this list only has to name them, not redefine their columns.
///
/// Also deliberately excluded, unrelated to the nodes/columns split above:
///
/// - the `dbt_rt` schema — written from run artifacts, so it does not exist at parse at all.
/// - `generation` — index bookkeeping rather than project metadata; parse-stable but not useful to a
///   check, and exposing it invites checks that depend on ingest internals.
pub const PARSE_SAFE_VIEWS: &[&str] = &[
    "graph_nodes",
    "models",
    "seeds",
    "tests",
    "snapshots",
    "sources",
    "analyses",
    "operations",
    "functions",
    "checks",
    "columns",
    "docs",
    "edges",
    "macros",
    "project",
    "project_vars",
    "test_metadata",
];

/// The `info_schema('<view>')` helper. Expands to `dbt.<view>` so a check can read the index
/// without hard-coding the schema: `SELECT … FROM {{ info_schema('models') }}`.
///
/// Only [`PARSE_SAFE_VIEWS`] are accepted. An unknown or later-phase name is an error at render
/// time, naming what is available — far better than a check that runs and silently reports nothing,
/// which is what happens when a check reads a table that is empty at parse.
pub fn make_info_schema_fn() -> Value {
    use minijinja::value::Kwargs;
    use minijinja::{Error as MjError, ErrorKind};
    Value::from_function(
        |args: &[Value], _kwargs: Kwargs| -> Result<Value, MjError> {
            let view = args.first().and_then(|v| v.as_str()).ok_or_else(|| {
                MjError::new(
                    ErrorKind::InvalidOperation,
                    "info_schema(view): first argument must be a view name string",
                )
            })?;
            if !PARSE_SAFE_VIEWS.contains(&view) {
                return Err(MjError::new(
                    ErrorKind::InvalidOperation,
                    format!(
                        "info_schema: '{view}' is not available to a parse-time check. \
                         Available views: {}",
                        PARSE_SAFE_VIEWS.join(", ")
                    ),
                ));
            }
            Ok(Value::from(format!("dbt.{view}")))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(view: &str) -> Result<String, String> {
        let env = minijinja::Environment::new();
        make_info_schema_fn()
            .call(&env.empty_state(), &[Value::from(view)], &[])
            .map(|v| v.to_string())
            .map_err(|e| e.to_string())
    }

    #[test]
    fn expands_every_parse_safe_view_to_the_dbt_schema() {
        for view in PARSE_SAFE_VIEWS {
            assert_eq!(
                call(view).unwrap_or_else(|e| panic!("{view} must expand, got {e}")),
                format!("dbt.{view}"),
            );
        }
    }

    #[test]
    fn rejects_the_raw_tables_the_parse_safe_views_are_projected_from() {
        // `dbt.nodes` / `dbt.node_columns` mix parse-safe columns with compile-filled or
        // never-populated ones. `dbt.graph_nodes` / `dbt.columns` are the safe projections;
        // the raw tables underneath must stay unreachable through this helper.
        for raw in ["nodes", "node_columns"] {
            let err = call(raw).expect_err("the raw table must be refused");
            assert!(
                err.contains("not available to a parse-time check"),
                "unexpected message for {raw}: {err}"
            );
        }
    }

    #[test]
    fn rejects_views_that_are_not_final_at_parse() {
        // `dbt_rt.*` does not exist until run artifacts are written; `generation` is ingest
        // bookkeeping, not project metadata. Both must fail loudly rather than read as empty.
        for later in [
            "column_lineage",
            "catalog_tables",
            "run_results",
            "generation",
        ] {
            let err = call(later).expect_err("a later-phase view must be refused");
            assert!(
                err.contains("not available to a parse-time check"),
                "unexpected message for {later}: {err}"
            );
        }
    }

    #[test]
    fn rejects_names_that_could_inject_sql() {
        // The result is spliced into the check's SQL, and the allowlist is what makes that safe: no
        // separate escaping is needed because nothing outside the list is ever expanded.
        for bad in [
            "",
            "models; DROP TABLE x",
            "dbt.models",
            "a-b",
            "a b",
            "a'b",
            "MODELS",
        ] {
            assert!(call(bad).is_err(), "expected {bad:?} to be rejected");
        }
    }

    #[test]
    fn error_names_the_available_views() {
        let err = call("nope").expect_err("unknown view");
        assert!(
            err.contains("graph_nodes"),
            "should list what is available: {err}"
        );
        assert!(
            err.contains("models"),
            "should list what is available: {err}"
        );
    }
}
