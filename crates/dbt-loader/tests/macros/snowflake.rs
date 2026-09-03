use std::collections::BTreeMap;
use std::sync::Arc;

use dbt_adapter::relation::{Relation, RelationObject, do_create_relation};
use dbt_adapter_core::AdapterType;
use dbt_jinja_utils::mock_object::MockJinjaObject;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::relations::base::TableFormat;
use minijinja::Value;

use crate::macro_test_harness::{MacroTestHarness, default_mock_config};

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
fn incremental_catalog_linked_target_adopts_and_quotes_existing_identity() {
    let incremental = include_str!(
        "../../src/dbt_macro_assets/dbt-snowflake/macros/materializations/incremental.sql"
    );
    assert!(incremental.contains("is_catalog_linked_db and existing_relation is not none"));
    assert!(incremental.contains("\"schema\": existing_relation.schema"));
    assert!(incremental.contains("\"identifier\": existing_relation.identifier"));
    assert!(incremental.contains(".quote(schema=true, identifier=true)"));

    let harness = MacroTestHarness::for_adapter(AdapterType::Snowflake)
        .load_all_macros()
        .build()
        .expect("incremental macro should parse");
    let target = RelationObject::new(Arc::new(
        Relation::new(
            AdapterType::Snowflake,
            "cld".to_string(),
            "SCHEMA_A".to_string(),
            "T_ORDERS".to_string(),
        )
        .with_quoting(ResolvedQuoting::falses())
        .with_table_format(TableFormat::Iceberg),
    ));
    let existing = RelationObject::new(Arc::from(
        do_create_relation(
            AdapterType::Snowflake,
            "CLD".to_string(),
            "schema_a".to_string(),
            Some("t_orders".to_string()),
            None,
            ResolvedQuoting::trues(),
        )
        .unwrap(),
    ));
    let rendered = harness
        .render(
            r#"{% set adopted = target.incorporate(path={"schema": existing.schema, "identifier": existing.identifier}).quote(schema=true, identifier=true) %}{{ adopted }}|{{ adopted.is_iceberg_format }}"#,
            BTreeMap::from([
                ("target".to_string(), target.into_value()),
                ("existing".to_string(), existing.into_value()),
            ]),
        )
        .unwrap();

    assert_eq!(rendered, "cld.\"schema_a\".\"t_orders\"|True");
}

#[test]
fn load_cached_relation_passes_node_specific_quote_policy() {
    let harness = MacroTestHarness::for_adapter(AdapterType::Snowflake)
        .load_all_macros()
        .build()
        .expect("relation macros should parse");
    harness.mock().on("get_relation", |args| {
        let kwargs = args.last().and_then(Value::as_object).expect("kwargs");
        let relation_value = kwargs
            .get_value(&Value::from("relation"))
            .expect("relation-aware lookup argument");
        let relation = relation_value
            .downcast_object_ref::<RelationObject>()
            .expect("RelationObject");
        assert!(relation.inner().quote_policy().identifier);
        Ok(relation_value)
    });
    let requested = RelationObject::new(Arc::from(
        do_create_relation(
            AdapterType::Snowflake,
            "CLD".to_string(),
            "SCHEMA_A".to_string(),
            Some("t_orders".to_string()),
            None,
            ResolvedQuoting {
                database: false,
                schema: false,
                identifier: true,
            },
        )
        .unwrap(),
    ));

    let rendered = harness
        .render(
            "{{ load_cached_relation(requested) }}",
            BTreeMap::from([("requested".to_string(), requested.into_value())]),
        )
        .expect("load_cached_relation should preserve the relation object");

    assert_eq!(rendered, "CLD.SCHEMA_A.\"t_orders\"");
}
