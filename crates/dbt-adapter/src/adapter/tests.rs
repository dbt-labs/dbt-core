use std::collections::BTreeMap;

use super::*;
use crate::adapter::Adapter;
use crate::adapter::adapter_impl::AdapterImpl;
use crate::relation::config_v2::{ComponentConfigChange, RelationConfig};
use crate::relation::databricks::config::components::RelationTagsLoader;
use crate::relation::{Relation, RelationObject};
use crate::sql_types::DefaultTypeOps;
use crate::stmt_splitter::DefaultStmtSplitter;
use dbt_adapter_core::AdapterType;

use dbt_common::cancellation::never_cancels;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::relations::{DEFAULT_DBT_QUOTING, DEFAULT_RESOLVED_QUOTING};
use indexmap::IndexMap;
use minijinja_contrib::testing::jinja_assert;

fn never_full_refresh(_: &IndexMap<&'static str, ComponentConfigChange>) -> bool {
    false
}

fn model_config_value(tags: IndexMap<String, String>) -> Value {
    Value::from_object(RelationConfig::new(
        AdapterType::Databricks,
        [RelationTagsLoader::new_component_type_erased(tags)],
        never_full_refresh,
    ))
}

#[test]
fn test_relation_tag_metadata_planning_at_adapter_boundary() {
    let tagged = model_config_value(IndexMap::from_iter([(
        "deployment".to_string(),
        "DBT".to_string(),
    )]));
    let empty = model_config_value(IndexMap::new());

    assert!(should_fetch_relation_tags(Some(&tagged)));
    assert!(!should_fetch_relation_tags(Some(&empty)));
    assert!(should_fetch_relation_tags(None));
    assert!(should_fetch_relation_tags(Some(&Value::from(42))));
}

#[derive(Debug)]
struct GetRelationConfigFixture {
    adapter: Arc<Adapter>,
    relation: Value,
    tagged: Value,
    empty: Value,
}

impl Object for GetRelationConfigFixture {
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str()? {
            "adapter" => Some(Value::from_object(self.adapter.as_ref().clone())),
            "relation" => Some(self.relation.clone()),
            "tagged" => Some(self.tagged.clone()),
            "empty" => Some(self.empty.clone()),
            _ => None,
        }
    }
}

fn get_relation_config_fixture() -> GetRelationConfigFixture {
    let relation = Relation::new(
        AdapterType::Databricks,
        "main".to_string(),
        "test_schema".to_string(),
        "test_mv".to_string(),
    )
    .with_relation_type(RelationType::MaterializedView)
    .with_quoting(DEFAULT_RESOLVED_QUOTING);
    GetRelationConfigFixture {
        adapter: make_mock_adapter(AdapterType::Databricks),
        relation: Value::from_object(RelationObject::new(Arc::new(relation))),
        tagged: model_config_value(IndexMap::from_iter([(
            "deployment".to_string(),
            "DBT".to_string(),
        )])),
        empty: model_config_value(IndexMap::new()),
    }
}

#[test]
fn test_get_relation_config_typed_adapter_jinja_value_contract() {
    let template = r#"
        {% set tagged = obj.adapter.get_relation_config(obj.relation, obj.tagged) %}
        {% set empty = obj.adapter.get_relation_config(obj.relation, obj.empty) %}
        {% set missing = obj.adapter.get_relation_config(obj.relation) %}
        {% set wrong_type = obj.adapter.get_relation_config(obj.relation, 42) %}
        {{ tagged is not none and empty is not none and missing is not none and wrong_type is not none }}
    "#;

    jinja_assert(get_relation_config_fixture(), template, "True");
}

#[test]
fn test_get_relation_config_typed_adapter_rejects_excess_jinja_arguments() {
    let mut env = minijinja::Environment::new();
    env.add_global("obj", Value::from_object(get_relation_config_fixture()));
    let error = env
        .render_str(
            "{{ obj.adapter.get_relation_config(obj.relation, obj.tagged, 42) }}",
            BTreeMap::<String, String>::new(),
            &[],
        )
        .unwrap_err();

    assert!(
        error.to_string().contains("too many arguments"),
        "unexpected error: {error}"
    );
}

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
    // covering every branch of the inline resolution. An absent database is tolerated for every
    // adapter, matching dbt-core: `adapter.get_relation(database=none, ...)` returns a relation
    // rather than raising (verified against dbt-core / dbt-duckdb, which returns `None`).
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

    // Every other adapter tolerates an absent database (defaults to `""`) rather than erroring,
    // matching dbt-core's duck-typed `get_relation`.
    let result = dispatch_test(
        &make_mock_adapter(AdapterType::DuckDB),
        "get_relation",
        &args,
    )
    .unwrap();
    assert!(!result.is_none() && !result.is_undefined());
}
