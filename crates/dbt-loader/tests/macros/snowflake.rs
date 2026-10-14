use std::collections::BTreeMap;
use std::sync::Arc;

use dbt_adapter::relation::{Relation, RelationObject};
use dbt_adapter_core::AdapterType;
use dbt_jinja_utils::mock_object::MockJinjaObject;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::relations::base::{BaseRelation, TableFormat};
use minijinja::Value;

use crate::macro_test_harness::{
    MacroTestHarness, assert_executed_contains, default_mock_config, executed_sql,
    resolved_quoting_for,
};

#[test]
fn snapshot_time_uses_iceberg_compatible_precision() {
    let harness = MacroTestHarness::for_adapter(AdapterType::Snowflake)
        .load_all_macros()
        .build()
        .expect("harness should build");

    // Iceberg-ness comes from the catalog relation (so catalogs.yml-defined Iceberg counts too),
    // not from a `table_format` key on the node config.
    harness.mock().on("build_catalog_relation", |_| {
        Ok(Value::from_serialize(BTreeMap::from([(
            "table_format",
            "iceberg",
        )])))
    });
    let ctx = BTreeMap::from([(
        "config".to_string(),
        Value::from_dyn_object(default_mock_config()),
    )]);

    let rendered = harness
        .render("{{ snapshot_get_time() }}", ctx)
        .expect("snapshot_get_time should render");
    let normalized = rendered.split_whitespace().collect::<Vec<_>>().join(" ");

    assert_eq!(
        normalized,
        "to_timestamp_ntz(convert_timezone('UTC', current_timestamp()))::timestamp_ntz(6)"
    );
}

#[test]
fn snapshot_time_uses_default_precision_for_non_iceberg() {
    let harness = MacroTestHarness::for_adapter(AdapterType::Snowflake)
        .load_all_macros()
        .build()
        .expect("harness should build");

    harness.mock().on("build_catalog_relation", |_| {
        Ok(Value::from_serialize(BTreeMap::from([(
            "table_format",
            "default",
        )])))
    });
    let ctx = BTreeMap::from([(
        "config".to_string(),
        Value::from_dyn_object(default_mock_config()),
    )]);

    let rendered = harness
        .render("{{ snapshot_get_time() }}", ctx)
        .expect("snapshot_get_time should render");
    let normalized = rendered.split_whitespace().collect::<Vec<_>>().join(" ");

    assert_eq!(
        normalized,
        "to_timestamp_ntz(convert_timezone('UTC', current_timestamp()))"
    );
}

#[test]
fn snapshot_schema_evolution_uses_iceberg_alter() {
    let harness = MacroTestHarness::for_adapter(AdapterType::Snowflake)
        .load_all_macros()
        .with_stub_functions()
        .build()
        .expect("harness should build");
    harness.mock().on("quote", |args| {
        let identifier = args
            .first()
            .and_then(Value::as_str)
            .expect("quote should receive an identifier");
        Ok(Value::from(format!("\"{identifier}\"")))
    });

    let relation: Arc<dyn BaseRelation> = Arc::new(
        Relation::new(
            AdapterType::Snowflake,
            "TEST_DB".to_string(),
            "TEST_SCHEMA".to_string(),
            "SNAPSHOT".to_string(),
        )
        .with_relation_type(RelationType::Table)
        .with_quoting(resolved_quoting_for(AdapterType::Snowflake))
        .with_table_format(TableFormat::Iceberg),
    );
    let column = Arc::new(MockJinjaObject::new());
    column.set_attr("name", Value::from("NEW_COLUMN"));
    column.set_attr("data_type", Value::from("NUMBER"));
    column.on("is_string", |_| Ok(Value::from(false)));
    let context = BTreeMap::from([
        (
            "relation".to_string(),
            RelationObject::new(relation).into_value(),
        ),
        (
            "columns".to_string(),
            Value::from(vec![Value::from_dyn_object(column)]),
        ),
        ("execute".to_string(), Value::from(true)),
    ]);

    harness
        .render("{{ create_columns(relation, columns) }}", context)
        .expect("snapshot column creation should render");

    assert_executed_contains(
        harness.mock(),
        "alter iceberg table TEST_DB.TEST_SCHEMA.SNAPSHOT add column",
    );
    assert_executed_contains(harness.mock(), "\"NEW_COLUMN\"");
}

#[test]
fn snapshot_schema_evolution_matches_v1_default_for_non_iceberg() {
    let harness = MacroTestHarness::for_adapter(AdapterType::Snowflake)
        .load_all_macros()
        .with_stub_functions()
        .build()
        .expect("harness should build");
    harness.mock().on("quote", |args| {
        let identifier = args
            .first()
            .and_then(Value::as_str)
            .expect("quote should receive an identifier");
        Ok(Value::from(format!("\"{identifier}\"")))
    });

    let relation: Arc<dyn BaseRelation> = Arc::new(
        Relation::new(
            AdapterType::Snowflake,
            "TEST_DB".to_string(),
            "TEST_SCHEMA".to_string(),
            "SNAPSHOT".to_string(),
        )
        .with_relation_type(RelationType::Table)
        .with_quoting(resolved_quoting_for(AdapterType::Snowflake))
        .with_table_format(TableFormat::Default),
    );
    // Same SQL as dbt-snowflake v1 (no override there): one `ALTER TABLE ... ADD COLUMN` per
    // column, typed with `expanded_data_type` (which carries the string size).
    let new_column = |name: &str, expanded: &str| {
        let column = Arc::new(MockJinjaObject::new());
        column.set_attr("name", Value::from(name));
        column.set_attr("data_type", Value::from("VARCHAR"));
        column.set_attr("expanded_data_type", Value::from(expanded));
        column.on("is_string", |_| Ok(Value::from(true)));
        Value::from_dyn_object(column)
    };
    let context = BTreeMap::from([
        (
            "relation".to_string(),
            RelationObject::new(relation).into_value(),
        ),
        (
            "columns".to_string(),
            Value::from(vec![
                new_column("COL_A", "VARCHAR(100)"),
                new_column("COL_B", "NUMBER(38,0)"),
            ]),
        ),
        ("execute".to_string(), Value::from(true)),
    ]);

    harness
        .render("{{ create_columns(relation, columns) }}", context)
        .expect("snapshot column creation should render");

    let sqls = executed_sql(harness.mock());
    let normalized: Vec<String> = sqls
        .iter()
        .map(|s| s.split_whitespace().collect::<Vec<_>>().join(" "))
        .collect();
    let add_column_statements: Vec<&String> = normalized
        .iter()
        .filter(|s| {
            s.to_lowercase()
                .contains("alter table test_db.test_schema.snapshot add column")
        })
        .collect();
    assert_eq!(
        add_column_statements.len(),
        2,
        "expected one ALTER TABLE per added column, got: {sqls:?}"
    );
    assert!(
        add_column_statements
            .iter()
            .any(|s| s.contains("\"COL_A\" VARCHAR(100)")),
        "string size should come from expanded_data_type, got: {sqls:?}"
    );
    assert!(
        add_column_statements
            .iter()
            .any(|s| s.contains("\"COL_B\" NUMBER(38,0)")),
        "got: {sqls:?}"
    );
    assert!(
        !normalized
            .iter()
            .any(|s| s.to_lowercase().contains("iceberg")),
        "non-Iceberg snapshot should not use ALTER ICEBERG TABLE, got: {sqls:?}"
    );
}

#[test]
fn python_table_tmp_relation_type_is_allowed() {
    let harness = MacroTestHarness::for_adapter(AdapterType::Snowflake)
        .load_all_macros()
        .build()
        .expect("harness should build");

    let config = default_mock_config();
    config.on("get", |args| {
        let key = args.first().and_then(|v| v.as_str());
        let default = args.get(1).cloned().unwrap_or(Value::UNDEFINED);
        match key {
            Some("tmp_relation_type") => Ok(Value::from("table")),
            _ => Ok(default),
        }
    });

    let ctx = BTreeMap::from([("config".to_string(), Value::from_dyn_object(config))]);

    let rendered = harness
        .render(
            "{{ dbt_snowflake_get_tmp_relation_type('default', none, 'python') }}",
            ctx,
        )
        .expect("table is valid for Python tmp_relation_type");

    assert_eq!(rendered.trim(), "table");
}

fn render_python_table(temporary: bool, is_transient: bool) -> String {
    let harness = MacroTestHarness::for_adapter(AdapterType::Snowflake)
        .load_all_macros()
        .build()
        .expect("harness should build");

    let catalog_relation = Arc::new(MockJinjaObject::new());
    catalog_relation.set_attr("catalog_type", Value::from("INFO_SCHEMA"));
    catalog_relation.set_attr("is_transient", Value::from(is_transient));
    harness.mock().on("build_catalog_relation", move |_| {
        Ok(Value::from_dyn_object(catalog_relation.clone()))
    });

    let ctx = harness
        .materialization_context(
            "orders",
            "def model(dbt, session):\n    return session.table('orders')",
        )
        .with("temporary", Value::from(temporary))
        .build();

    harness
        .render(
            "{{ snowflake__create_table_as(temporary, this, compiled_code, 'python') }}",
            ctx,
        )
        .expect("Python table macro should render")
}

#[test]
fn python_incremental_staging_table_is_temporary() {
    let rendered = render_python_table(true, true);

    assert!(
        rendered.contains("table_type='temporary'"),
        "Python incremental staging tables should be temporary, got:\n{rendered}"
    );
    assert!(!rendered.contains("table_type='transient'"));
}

#[test]
fn python_table_preserves_transient_config() {
    let rendered = render_python_table(false, true);

    assert!(
        rendered.contains("table_type='transient'"),
        "Python table models should preserve transient configuration, got:\n{rendered}"
    );
    assert!(!rendered.contains("table_type='temporary'"));
}

#[test]
fn alter_relation_comment_uses_iceberg_syntax_after_incorporate() {
    use dbt_adapter::relation::{Relation, RelationObject};
    use dbt_schemas::dbt_types::RelationType;
    use dbt_schemas::schemas::relations::base::{BaseRelation, TableFormat};

    let harness = MacroTestHarness::for_adapter(AdapterType::Snowflake)
        .load_all_macros()
        .build()
        .expect("harness should build");

    let iceberg: Arc<dyn BaseRelation> = Arc::new(
        Relation::new(
            AdapterType::Snowflake,
            "TEST_DB".to_string(),
            "TEST_SCHEMA".to_string(),
            "my_iceberg_table".to_string(),
        )
        .with_relation_type(Some(RelationType::Table))
        .with_table_format(TableFormat::Iceberg),
    );

    // Mirrors `target_relation.incorporate(type='table')` in the incremental
    // materialization, called right before `persist_docs` (fs#14268).
    let incorporated = iceberg
        .incorporate(None, Some(RelationType::Table), None)
        .expect("incorporate should succeed");

    let ctx = BTreeMap::from([
        (
            "relation".to_string(),
            RelationObject::new(incorporated).into_value(),
        ),
        ("relation_comment".to_string(), Value::from("a comment")),
    ]);

    let rendered = harness
        .render(
            "{{ snowflake__alter_relation_comment(relation, relation_comment) }}",
            ctx,
        )
        .expect("alter_relation_comment should render");

    assert!(
        rendered.contains("alter iceberg table"),
        "expected iceberg ALTER syntax after incorporate, got:\n{rendered}"
    );
    assert!(
        !rendered.contains("comment on table"),
        "must not fall back to plain COMMENT ON TABLE for an Iceberg relation, got:\n{rendered}"
    );
}
