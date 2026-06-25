use std::collections::BTreeMap;

use super::*;
use crate::adapter::Adapter;
use crate::adapter::adapter_impl::AdapterImpl;
use crate::sql_types::DefaultTypeOps;
use crate::stmt_splitter::DefaultStmtSplitter;
use dbt_adapter_core::AdapterType;

use dbt_common::cancellation::never_cancels;
use dbt_schemas::schemas::relations::{DEFAULT_DBT_QUOTING, DEFAULT_RESOLVED_QUOTING};
use indexmap::IndexMap;

/// Helper to call [Adapter::call_method_impl] with jinja-valued arguments.
fn dispatch_test(
    adapter: &Arc<Adapter>,
    name: &str,
    args: &[Value],
) -> Result<Value, minijinja::Error> {
    let env = minijinja::Environment::new();
    let state = State::new_for_env(&env);
    adapter.call_method_impl(&state, name, args, &[])
}

/// Create a Typed-phase DuckDB adapter backed by MockEngine.
fn make_duckdb_adapter() -> Arc<Adapter> {
    make_mock_adapter(AdapterType::DuckDB)
}

/// Create a parse-phase DuckDB adapter (returns defaults, no real execution).
fn make_duckdb_parse_adapter() -> Arc<Adapter> {
    let adapter = Adapter::new_parse_phase_adapter(
        AdapterType::DuckDB,
        dbt_yaml::Mapping::new(),
        DEFAULT_DBT_QUOTING,
        Arc::new(DefaultTypeOps::new(AdapterType::DuckDB)),
        None,
        None,
    );
    Arc::new(adapter)
}

/// Helper to build a minijinja dict Value from key-value pairs.
fn dict(pairs: &[(&str, &str)]) -> Value {
    let map: IndexMap<String, Value> = pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), Value::from(*v)))
        .collect();
    Value::from(map)
}

// -- external_root tests --------------------------------------------------

#[test]
fn test_external_root_default() {
    let adapter = make_duckdb_adapter();
    let result = dispatch_test(&adapter, "external_root", &[]).unwrap();
    assert_eq!(result.as_str().unwrap(), ".");
}

// TODO: test external_root with custom config once MockAdapter supports custom AdapterConfig

// -- external_write_options tests (ported from dbt-duckdb test_external_utils.py) --

#[test]
fn test_external_write_options_csv_inferred() {
    let adapter = make_duckdb_adapter();
    let result = dispatch_test(
        &adapter,
        "external_write_options",
        &[Value::from("/tmp/test.csv"), dict(&[])],
    )
    .unwrap();
    assert_eq!(result.as_str().unwrap(), "format csv, header 1");
}

#[test]
fn test_external_write_options_parquet_with_codec() {
    let adapter = make_duckdb_adapter();
    let result = dispatch_test(
        &adapter,
        "external_write_options",
        &[Value::from("./foo.parquet"), dict(&[("codec", "zstd")])],
    )
    .unwrap();
    assert_eq!(result.as_str().unwrap(), "codec zstd, format parquet");
}

#[test]
fn test_external_write_options_delimiter_infers_csv() {
    let adapter = make_duckdb_adapter();
    let result = dispatch_test(
        &adapter,
        "external_write_options",
        &[
            Value::from("bar"),
            dict(&[("delimiter", "|"), ("header", "0")]),
        ],
    )
    .unwrap();
    assert_eq!(
        result.as_str().unwrap(),
        "delimiter '|', header 0, format csv"
    );
}

#[test]
fn test_external_write_options_partition_by_single() {
    let adapter = make_duckdb_adapter();
    let result = dispatch_test(
        &adapter,
        "external_write_options",
        &[Value::from("a.parquet"), dict(&[("partition_by", "ds")])],
    )
    .unwrap();
    assert_eq!(result.as_str().unwrap(), "partition_by ds, format parquet");
}

#[test]
fn test_external_write_options_partition_by_multi_adds_parens() {
    let adapter = make_duckdb_adapter();
    let result = dispatch_test(
        &adapter,
        "external_write_options",
        &[
            Value::from("b.csv"),
            dict(&[("partition_by", "ds,category")]),
        ],
    )
    .unwrap();
    assert_eq!(
        result.as_str().unwrap(),
        "partition_by (ds,category), format csv, header 1"
    );
}

#[test]
fn test_external_write_options_null_quoted() {
    let adapter = make_duckdb_adapter();
    let result = dispatch_test(
        &adapter,
        "external_write_options",
        &[Value::from("/path/to/c.csv"), dict(&[("null", "\\N")])],
    )
    .unwrap();
    assert_eq!(result.as_str().unwrap(), "null '\\N', format csv, header 1");
}

// -- external_read_location tests (ported from dbt-duckdb test_external_utils.py) --

#[test]
fn test_external_read_location_no_partition() {
    let adapter = make_duckdb_adapter();
    let result = dispatch_test(
        &adapter,
        "external_read_location",
        &[
            Value::from("bar"),
            dict(&[("format", "csv"), ("delimiter", "|"), ("header", "0")]),
        ],
    )
    .unwrap();
    assert_eq!(result.as_str().unwrap(), "bar");
}

#[test]
fn test_external_read_location_single_partition() {
    let adapter = make_duckdb_adapter();
    let result = dispatch_test(
        &adapter,
        "external_read_location",
        &[
            Value::from("/tmp/a"),
            dict(&[("partition_by", "ds"), ("format", "parquet")]),
        ],
    )
    .unwrap();
    assert_eq!(result.as_str().unwrap(), "/tmp/a/*/*.parquet");
}

#[test]
fn test_external_read_location_multi_partition() {
    let adapter = make_duckdb_adapter();
    let result = dispatch_test(
        &adapter,
        "external_read_location",
        &[Value::from("b"), dict(&[("partition_by", "ds,category")])],
    )
    .unwrap();
    assert_eq!(result.as_str().unwrap(), "b/*/*/*.parquet");
}

fn make_adapter_with_truthy_nulls(adapter_type: AdapterType) -> Arc<Adapter> {
    let concrete = AdapterImpl::new_mock(
        adapter_type,
        BTreeMap::from([(
            "enable_truthy_nulls_equals_macro".to_string(),
            Value::from(true),
        )]),
        DEFAULT_RESOLVED_QUOTING,
        Arc::new(DefaultTypeOps::new(adapter_type)),
        Arc::new(DefaultStmtSplitter),
    );
    Arc::new(Adapter::new(Arc::new(concrete), None, never_cancels()))
}

/// Create a Typed-phase mock adapter for the given adapter type (no behavior flags).
fn make_mock_adapter(adapter_type: AdapterType) -> Arc<Adapter> {
    let concrete = AdapterImpl::new_mock(
        adapter_type,
        BTreeMap::new(),
        DEFAULT_RESOLVED_QUOTING,
        Arc::new(DefaultTypeOps::new(adapter_type)),
        Arc::new(DefaultStmtSplitter),
    );
    Arc::new(Adapter::new(Arc::new(concrete), None, never_cancels()))
}

#[test]
fn test_render_equals_flag_off_returns_simple_eq() {
    let adapter = make_duckdb_adapter();
    let result = dispatch_test(
        &adapter,
        "render_equals",
        &[Value::from("a"), Value::from("b")],
    )
    .unwrap();
    assert_eq!(result.as_str().unwrap(), "(a = b)");
}

#[test]
fn test_render_equals_parse_mode_returns_simple_eq() {
    let adapter = make_duckdb_parse_adapter();
    let result = dispatch_test(
        &adapter,
        "render_equals",
        &[Value::from("a"), Value::from("b")],
    )
    .unwrap();
    assert_eq!(result.as_str().unwrap(), "(a = b)");
}

#[test]
fn test_render_equals_flag_on_snowflake_is_not_distinct_from() {
    let adapter = make_adapter_with_truthy_nulls(AdapterType::Snowflake);
    let result = dispatch_test(
        &adapter,
        "render_equals",
        &[Value::from("a"), Value::from("b")],
    )
    .unwrap();
    assert_eq!(result.as_str().unwrap(), "(a IS NOT DISTINCT FROM b)");
}

#[test]
fn test_render_equals_flag_on_bigquery_is_not_distinct_from() {
    let adapter = make_adapter_with_truthy_nulls(AdapterType::Bigquery);
    let result = dispatch_test(
        &adapter,
        "render_equals",
        &[Value::from("a"), Value::from("b")],
    )
    .unwrap();
    assert_eq!(result.as_str().unwrap(), "(a IS NOT DISTINCT FROM b)");
}

#[test]
fn test_render_equals_flag_on_postgres_is_not_distinct_from() {
    let adapter = make_adapter_with_truthy_nulls(AdapterType::Postgres);
    let result = dispatch_test(
        &adapter,
        "render_equals",
        &[Value::from("a"), Value::from("b")],
    )
    .unwrap();
    assert_eq!(result.as_str().unwrap(), "(a IS NOT DISTINCT FROM b)");
}

#[test]
fn test_render_equals_flag_on_redshift_is_not_distinct_from() {
    let adapter = make_adapter_with_truthy_nulls(AdapterType::Redshift);
    let result = dispatch_test(
        &adapter,
        "render_equals",
        &[Value::from("a"), Value::from("b")],
    )
    .unwrap();
    assert_eq!(result.as_str().unwrap(), "(a IS NOT DISTINCT FROM b)");
}

#[test]
fn test_render_equals_flag_on_duckdb_is_not_distinct_from() {
    let adapter = make_adapter_with_truthy_nulls(AdapterType::DuckDB);
    let result = dispatch_test(
        &adapter,
        "render_equals",
        &[Value::from("a"), Value::from("b")],
    )
    .unwrap();
    assert_eq!(result.as_str().unwrap(), "(a IS NOT DISTINCT FROM b)");
}

#[test]
fn test_render_equals_flag_on_databricks_is_not_distinct_from() {
    let adapter = make_adapter_with_truthy_nulls(AdapterType::Databricks);
    let result = dispatch_test(
        &adapter,
        "render_equals",
        &[Value::from("a"), Value::from("b")],
    )
    .unwrap();
    assert_eq!(result.as_str().unwrap(), "(a IS NOT DISTINCT FROM b)");
}

// -- location_exists tests ------------------------------------------------

#[test]
fn test_location_exists_parse_mode_returns_false() {
    let adapter = make_duckdb_parse_adapter();
    let result = dispatch_test(
        &adapter,
        "location_exists",
        &[Value::from("/nonexistent/path")],
    )
    .unwrap();
    // Parse-mode adapter always returns false
    assert_eq!(result, Value::from(false));
}

// -- parse-mode arg permissiveness ----------------------------------------
//
// Python `@available.parse_*` decorators short-circuit at parse time without
// inspecting argument types; macros that pass the "wrong" thing should still
// receive the canned value. These tests pin that invariant: at parse time,
// mistyped args do not raise — the Parse arm returns the canned response.

#[test]
fn test_parse_mode_accepts_mistyped_args_drop_relation() {
    let adapter = make_duckdb_parse_adapter();
    // drop_relation expects a BaseRelation; passing an integer would error at
    // dispatch time pre-refactor. Parse mode must now ignore arg types.
    let result = dispatch_test(&adapter, "drop_relation", &[Value::from(42)]).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_parse_mode_accepts_mistyped_args_check_schema_exists() {
    let adapter = make_duckdb_parse_adapter();
    // check_schema_exists expects two strings; passing an int + list should not
    // error at parse time — Parse arm returns the canned `true`.
    let result = dispatch_test(
        &adapter,
        "check_schema_exists",
        &[Value::from(42), Value::from(vec![Value::from("oops")])],
    )
    .unwrap();
    assert_eq!(result, Value::from(true));
}

#[test]
fn test_parse_mode_accepts_mistyped_args_list_relations_without_caching() {
    let adapter = make_duckdb_parse_adapter();
    // list_relations_without_caching expects a BaseRelation; pass a string instead.
    let result = dispatch_test(
        &adapter,
        "list_relations_without_caching",
        &[Value::from("oops")],
    )
    .unwrap();
    // Parse-mode returns an empty list
    assert!(result.try_iter().unwrap().next().is_none());
}

#[test]
fn test_get_relation_dispatch_spark_absent_database() {
    // Exercises the full `"get_relation"` arm of `call_method_impl` (arg parsing + per-adapter
    // database resolution + handoff to `get_relation`) for the absent-database (`none`) case,
    // covering every branch of the inline resolution. Spark relations carry no catalog, so the
    // database argument is `none`.
    let args = [
        Value::from(()), // database: none
        Value::from("my_schema"),
        Value::from("my_table"),
    ];

    // Spark: no catalog -> resolves to the empty default and a relation is still returned.
    let result = dispatch_test(
        &make_mock_adapter(AdapterType::Spark),
        "get_relation",
        &args,
    )
    .unwrap();
    assert!(!result.is_none() && !result.is_undefined());

    // Databricks: substitutes its default catalog -> a relation is returned.
    let result = dispatch_test(
        &make_mock_adapter(AdapterType::Databricks),
        "get_relation",
        &args,
    )
    .unwrap();
    assert!(!result.is_none() && !result.is_undefined());

    // Every other adapter requires a database -> an absent value is an explicit error.
    let err = dispatch_test(
        &make_mock_adapter(AdapterType::DuckDB),
        "get_relation",
        &args,
    )
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("argument 'database' to get_relation() is required"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_databricks_get_incremental_strategy_macro_accepts_delete_insert() {
    dispatch_test(
        &make_mock_adapter(AdapterType::Databricks),
        "get_incremental_strategy_macro",
        &[dict(&[]), Value::from("delete+insert")],
    )
    .unwrap();
}
