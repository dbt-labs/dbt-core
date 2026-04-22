use std::collections::BTreeMap;
use std::sync::Arc;

use dbt_adapter::funcs::none_value;
use dbt_adapter::relation::RelationObject;
use dbt_adapter_core::AdapterType;
use dbt_jinja_utils::mock_object::MockJinjaObject;
use dbt_schemas::dbt_types::RelationType;
use minijinja::Value;

use crate::macro_test_harness::{
    MacroTestHarness, assert_executed_contains, default_mock_config, executed_sql,
};

fn incremental_macro_name(adapter_type: AdapterType) -> &'static str {
    match adapter_type {
        AdapterType::Databricks => "materialization_incremental_databricks",
        other => panic!("unsupported adapter for incremental materialization test: {other:?}"),
    }
}

fn render_incremental(
    harness: &MacroTestHarness,
    adapter_type: AdapterType,
    ctx: BTreeMap<String, Value>,
) -> dbt_common::FsResult<String> {
    let call = format!("{{{{ {}() }}}}", incremental_macro_name(adapter_type));
    harness.render(&call, ctx)
}

fn incremental_model(alias: &str, sql: &str) -> Value {
    Value::from_serialize(BTreeMap::from([
        ("alias", Value::from(alias)),
        (
            "unique_id",
            Value::from(format!("model.test_project.{alias}")),
        ),
        ("columns", Value::from(BTreeMap::<String, Value>::new())),
        ("language", Value::from("sql")),
        ("compiled_code", Value::from(sql)),
    ]))
}

fn incremental_config() -> Arc<MockJinjaObject> {
    let mock = default_mock_config();
    mock.set_attr("materialized", Value::from("incremental"));
    mock.on("get", |args| {
        let key = args.first().and_then(|v| v.as_str());
        let default = args.get(1).cloned().unwrap_or(Value::UNDEFINED);
        match key {
            Some("contract") => Ok(Value::from_serialize(BTreeMap::from([(
                "enforced".to_string(),
                Value::from(false),
            )]))),
            Some("full_refresh") => Ok(Value::from(false)),
            Some("on_schema_change") => Ok(Value::from("ignore")),
            _ => Ok(default),
        }
    });
    mock
}

fn incremental_ctx(harness: &MacroTestHarness) -> BTreeMap<String, Value> {
    harness
        .materialization_context("my_incr", "SELECT id, name FROM source")
        .relation_type(RelationType::Table)
        .config(Value::from_dyn_object(incremental_config()))
        .with(
            "model",
            incremental_model("my_incr", "SELECT id, name FROM source"),
        )
        .build()
}

mod databricks {
    use super::*;
    const ADAPTER: AdapterType = AdapterType::Databricks;

    fn build_harness() -> MacroTestHarness {
        let mut harness = MacroTestHarness::for_adapter(ADAPTER)
            .load_all_macros()
            .with_stub_functions()
            .with_behavior_flag("use_materialization_v2", false)
            .with_behavior_flag("use_catalogs_v2", false)
            .with_behavior_flag("use_managed_iceberg", false)
            .build()
            .expect("harness should build");

        harness
            .env_mut()
            .env
            .add_function("var", |_name: Value, default: Option<Value>| {
                Ok(default.unwrap_or(Value::UNDEFINED))
            });

        let mock = harness.mock();
        mock.on("clean_sql", |args| {
            Ok(args.first().cloned().unwrap_or(Value::UNDEFINED))
        });
        mock.on("get_column_tags_from_model", |_| Ok(Value::UNDEFINED));
        mock.on("drop_relation", |_| Ok(Value::UNDEFINED));
        mock.on("commit", |_| Ok(Value::UNDEFINED));
        mock.on("resolve_file_format", |_| Ok(Value::from("delta")));
        mock.on("is_uniform", |_| Ok(Value::from(false)));
        mock.on("has_dbr_capability", |_| Ok(Value::from(false)));
        mock.on("is_cluster", |_| Ok(Value::from(false)));
        mock.on("optimize", |_| Ok(Value::UNDEFINED));
        mock.on("valid_incremental_strategies", |_| {
            Ok(Value::from(vec![
                Value::from("append"),
                Value::from("merge"),
                Value::from("insert_overwrite"),
                Value::from("replace_where"),
                Value::from("delete+insert"),
            ]))
        });

        let catalog_val = Value::from_serialize(BTreeMap::from([
            ("file_format".to_string(), Value::from("delta")),
            ("table_format".to_string(), Value::from("delta")),
        ]));
        mock.on("build_catalog_relation", move |_| Ok(catalog_val.clone()));

        harness
    }

    #[test]
    fn no_existing_relation_creates_table() {
        let harness = build_harness();
        harness.mock().on("get_relation", |_| Ok(none_value()));

        let ctx = incremental_ctx(&harness);
        render_incremental(&harness, ADAPTER, ctx)
            .unwrap_or_else(|e| panic!("incremental materialization failed: {e:?}"));

        harness
            .mock()
            .observed_calls()
            .assert_not_called("drop_relation");

        assert_executed_contains(harness.mock(), "create");
    }

    #[test]
    fn existing_view_dropped_and_recreated() {
        let harness = build_harness();

        let existing = harness.relation(
            "TEST_DB",
            "TEST_SCHEMA",
            "my_incr",
            Some(RelationType::View),
        );
        harness.mock().on("get_relation", move |_| {
            Ok(RelationObject::new(Arc::clone(&existing)).into_value())
        });

        let ctx = incremental_ctx(&harness);
        render_incremental(&harness, ADAPTER, ctx)
            .unwrap_or_else(|e| panic!("incremental with existing view failed: {e:?}"));

        harness
            .mock()
            .observed_calls()
            .assert_called("drop_relation");
        assert_executed_contains(harness.mock(), "create");
    }

    #[test]
    fn existing_table_incremental_merge() {
        let harness = build_harness();

        let existing = harness.relation(
            "TEST_DB",
            "TEST_SCHEMA",
            "my_incr",
            Some(RelationType::Table),
        );
        harness.mock().on("get_relation", move |_| {
            Ok(RelationObject::new(Arc::clone(&existing)).into_value())
        });
        harness
            .mock()
            .on("get_relation_config", |_| Ok(Value::UNDEFINED));

        let model_config = Arc::new(MockJinjaObject::new());
        model_config.on("get_changeset", |_| Ok(none_value()));
        let model_config_val = Value::from_dyn_object(model_config);
        harness.mock().on("get_config_from_model", move |_| {
            Ok(model_config_val.clone())
        });

        harness.mock().on("get_incremental_strategy_macro", |_| {
            Ok(Value::from_function(
                |_args: &[Value]| -> Result<Value, minijinja::Error> {
                    Ok(Value::from("SELECT 1 /* incremental merge */"))
                },
            ))
        });

        let ctx = incremental_ctx(&harness);
        render_incremental(&harness, ADAPTER, ctx)
            .unwrap_or_else(|e| panic!("incremental merge failed: {e:?}"));

        let sqls = executed_sql(harness.mock());
        assert!(
            sqls.len() >= 2,
            "Expected at least 2 SQL statements (temp table + merge), got: {sqls:?}",
        );
    }
}
