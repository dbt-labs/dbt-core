use std::collections::BTreeMap;

use dbt_adapter::relation::RelationObject;
use dbt_adapter_core::AdapterType;
use dbt_schemas::dbt_types::RelationType;
use minijinja::Value;

use crate::macro_test_harness::{MacroTestHarness, assert_executed_contains, executed_sql};

fn relation_database(arg: Option<&Value>) -> Option<String> {
    arg.and_then(|v| v.get_attr("database").ok())
        .and_then(|v| v.as_str().map(str::to_string))
}

fn build_harness(
    iceberg_database: &'static str,
    ducklake_database: &'static str,
) -> MacroTestHarness {
    let mut harness = MacroTestHarness::for_adapter(AdapterType::DuckDB)
        .load_all_macros()
        .with_stub_functions()
        .build()
        .expect("harness should build");

    let mock = harness.mock().clone();
    mock.on("table_format", move |args| {
        let database = relation_database(args.first());
        let table_format = match database.as_deref() {
            Some(database) if database == iceberg_database => "iceberg",
            Some(database) if database == ducklake_database => "ducklake",
            _ => "default",
        };
        Ok(Value::from(table_format))
    });
    mock.on("commit", |_| Ok(Value::UNDEFINED));
    mock.on("quote_as_configured", |args| {
        Ok(args.first().cloned().unwrap_or(Value::UNDEFINED))
    });
    harness
        .env_mut()
        .env
        .add_function("load_result", |_name: Value| {
            Ok(Value::from_serialize(BTreeMap::from([(
                "table",
                Vec::<Vec<Value>>::new(),
            )])))
        });
    harness
        .env_mut()
        .env
        .add_global("execute", Value::from(true));
    harness
        .env_mut()
        .env
        .add_global("adapter", Value::from_dyn_object(mock));

    harness
}

#[test]
fn get_columns_in_relation_uses_describe_for_iceberg_relations() {
    let harness = build_harness("iceberg_demo", "ducklake_demo");
    let relation = harness.relation("iceberg_demo", "main", "orders", Some(RelationType::Table));
    let ctx = BTreeMap::from([(
        "relation".to_string(),
        RelationObject::new(relation).into_value(),
    )]);

    harness
        .render("{{ duckdb__get_columns_in_relation(relation) }}", ctx)
        .expect("render should succeed");

    let executed = executed_sql(harness.mock()).join("\n");
    assert!(
        executed.contains("from (describe"),
        "Iceberg relations should use DESCRIBE, got: {executed}"
    );
    assert!(
        !executed.contains("information_schema.columns"),
        "Iceberg relations should not use information_schema.columns, got: {executed}"
    );
}

#[test]
fn drop_relation_omits_cascade_for_iceberg_relations() {
    let harness = build_harness("iceberg_demo", "ducklake_demo");
    let relation = harness.relation("iceberg_demo", "main", "orders", Some(RelationType::Table));
    let ctx = BTreeMap::from([(
        "relation".to_string(),
        RelationObject::new(relation).into_value(),
    )]);

    harness
        .render("{{ duckdb__drop_relation(relation) }}", ctx)
        .expect("render should succeed");

    assert_executed_contains(harness.mock(), "drop table if exists");
    let executed = executed_sql(harness.mock()).join("\n");
    assert!(
        !executed.contains("cascade"),
        "Iceberg drop should omit CASCADE, got: {executed}"
    );
}

#[test]
fn rename_relation_commits_before_iceberg_rename() {
    let harness = build_harness("iceberg_demo", "ducklake_demo");
    let from_relation = harness.relation(
        "iceberg_demo",
        "main",
        "orders__dbt_tmp",
        Some(RelationType::Table),
    );
    let to_relation = harness.relation("regular_demo", "main", "orders", Some(RelationType::Table));
    let ctx = BTreeMap::from([
        (
            "from_relation".to_string(),
            RelationObject::new(from_relation).into_value(),
        ),
        (
            "to_relation".to_string(),
            RelationObject::new(to_relation).into_value(),
        ),
    ]);

    harness
        .render(
            "{{ duckdb__rename_relation(from_relation, to_relation) }}",
            ctx,
        )
        .expect("render should succeed");

    let calls = harness.mock().observed_calls();
    let commit_idx = calls
        .iter()
        .position(|call| call.method == "commit")
        .expect("Iceberg rename should commit before ALTER TABLE RENAME");
    let execute_idx = calls
        .iter()
        .position(|call| call.method == "execute")
        .expect("rename should execute ALTER TABLE RENAME");
    assert!(
        commit_idx < execute_idx,
        "commit should be rendered before rename SQL: {calls:?}"
    );
    drop(calls);

    let executed = executed_sql(harness.mock()).join("\n");
    assert!(
        executed.contains("alter table"),
        "rename should use ALTER TABLE RENAME, got: {executed}"
    );
    assert!(
        !executed.contains("create table") && !executed.contains("drop table"),
        "rename should not use DROP/CREATE fallback, got: {executed}"
    );
}
