use std::collections::BTreeMap;
use std::sync::Arc;

use dbt_adapter::relation::RelationObject;
use dbt_adapter_core::AdapterType;
use dbt_jinja_utils::mock_object::MockJinjaObject;
use dbt_schemas::dbt_types::RelationType;
use minijinja::Value;

use crate::macro_test_harness::{MacroTestHarness, executed_sql};

fn normalize(rendered: &str) -> String {
    rendered.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// A documented column the relation lacks is warned about once and skipped, and
/// every comment is dollar-quoted so `;` survives inside the single ALTER.
#[test]
fn persist_docs_warns_about_missing_columns_and_dollar_quotes_comments() {
    let exceptions = Arc::new(MockJinjaObject::new());
    exceptions.on("warn", |_| Ok(Value::UNDEFINED));
    let harness = MacroTestHarness::for_adapter(AdapterType::ClickHouse)
        .load_all_macros()
        .with_stub_functions()
        .with_global("exceptions", Value::from_dyn_object(exceptions.clone()))
        .build()
        .expect("harness should build");
    harness
        .mock()
        .on("get_clickhouse_cluster_name", |_| Ok(Value::from(())));
    harness.mock().on("get_columns_in_relation", |_| {
        Ok(Value::from(vec![Value::from_serialize(BTreeMap::from([
            ("name", "id"),
        ]))]))
    });
    let config = Arc::new(MockJinjaObject::new());
    config.on("persist_relation_docs", |_| Ok(Value::from(true)));
    config.on("persist_column_docs", |_| Ok(Value::from(true)));

    let relation = harness.relation("my_db", "my_schema", "my_model", Some(RelationType::Table));
    let model = Value::from_serialize(BTreeMap::from([
        (
            "description",
            Value::from("Table description; with 'quotes' and \"doubles\""),
        ),
        (
            "columns",
            Value::from_serialize(BTreeMap::from([
                (
                    "id",
                    BTreeMap::from([("description", "id description; semicolon")]),
                ),
                (
                    "missing",
                    BTreeMap::from([("description", "documented but absent")]),
                ),
            ])),
        ),
    ]));
    let ctx = BTreeMap::from([
        (
            "relation".to_string(),
            RelationObject::new(relation).into_value(),
        ),
        ("model".to_string(), model),
        ("config".to_string(), Value::from_dyn_object(config)),
        ("execute".to_string(), Value::from(true)),
    ]);
    let rendered_relation = harness
        .render("{{ relation }}", ctx.clone())
        .expect("render should succeed");

    harness
        .render(
            "{{ clickhouse__persist_docs(relation, model, true, true) }}",
            ctx,
        )
        .expect("render should succeed");

    let sql = executed_sql(harness.mock());
    assert_eq!(sql.len(), 1, "{sql:?}");
    assert_eq!(
        normalize(&sql[0]),
        format!(
            "alter table {rendered_relation} modify comment \
             $dbt_comment_literal_block$Table description; with 'quotes' and \"doubles\"$dbt_comment_literal_block$, \
             comment column `id` $dbt_comment_literal_block$id description; semicolon$dbt_comment_literal_block$"
        )
    );
    let calls = exceptions.observed_calls();
    let warnings: Vec<_> = calls.to("warn").collect();
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    let message = warnings[0].args[0].to_string();
    assert!(
        message.contains("not present in the database: missing"),
        "{message}"
    );
}
