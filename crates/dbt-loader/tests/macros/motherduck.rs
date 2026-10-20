use std::collections::BTreeMap;
use std::sync::Arc;

use dbt_adapter::relation::RelationObject;
use dbt_adapter_core::AdapterType;
use dbt_jinja_utils::mock_object::MockJinjaObject;
use dbt_schemas::dbt_types::RelationType;
use minijinja::Value;

use crate::macro_test_harness::{MacroTestHarness, default_mock_config, executed_sql};

fn harness() -> MacroTestHarness {
    MacroTestHarness::for_adapter(AdapterType::MotherDuck)
        .load_all_macros()
        .with_stub_functions()
        .build()
        .expect("MotherDuck macro harness should build")
}

#[test]
fn dispatch_uses_motherduck_override_then_duckdb_parent() {
    let harness = harness();
    let batch_size = harness
        .render(
            "{{ adapter.dispatch('get_batch_size', 'dbt')() }}",
            BTreeMap::<String, Value>::new(),
        )
        .expect("MotherDuck seed override should resolve");
    assert_eq!(batch_size.trim(), "500");

    let timestamp = harness
        .render(
            "{{ adapter.dispatch('current_timestamp', 'dbt')() }}",
            BTreeMap::<String, Value>::new(),
        )
        .expect("DuckDB parent macro should resolve");
    assert_eq!(timestamp.trim(), "now()");
}

#[test]
fn create_table_as_rejects_python_models() {
    let err = harness()
        .render(
            "{{ motherduck__create_table_as(false, none, 'pass', 'python') }}",
            BTreeMap::<String, Value>::new(),
        )
        .expect_err("Python models must be rejected");
    assert!(err.to_string().contains("Python models are not supported"));
}

#[test]
fn non_empty_seed_uses_percent_s_bindings() {
    let harness = harness();
    harness
        .mock()
        .on("get_binding_char", |_| Ok(Value::from("$1")));
    harness.mock().on("quote_seed_column", |args| {
        Ok(args.first().cloned().unwrap_or(Value::UNDEFINED))
    });
    harness.mock().on("add_query", |_| Ok(Value::UNDEFINED));
    let config = default_mock_config();
    config.on("get", |args| {
        if args.first().and_then(Value::as_str) == Some("fast") {
            Ok(Value::from(false))
        } else {
            Ok(args.get(1).cloned().unwrap_or(Value::UNDEFINED))
        }
    });

    let model = Value::from_serialize(BTreeMap::from([
        ("alias", Value::from("raw_orders")),
        (
            "config",
            Value::from_serialize(BTreeMap::from([("quote_columns", Value::from(false))])),
        ),
    ]));
    let agate_table = Value::from_serialize(BTreeMap::from([
        ("column_names", Value::from_serialize(["id", "description"])),
        (
            "rows",
            Value::from_serialize([[Value::from(1), Value::from("MotherDuck seed row")]]),
        ),
    ]));
    let ctx = harness
        .materialization_context("raw_orders", "")
        .relation_type(RelationType::Table)
        .config(Value::from_dyn_object(config))
        .with("model", model)
        .with("agate_table", agate_table)
        .build();

    let rendered = harness
        .render("{{ load_csv_rows(model, agate_table) }}", ctx)
        .expect("non-empty MotherDuck seed should render and execute");
    let calls = harness.mock().observed_calls();
    let add_query = calls
        .to("add_query")
        .next()
        .expect("non-empty seed should execute an INSERT");
    let executed = add_query.args[0].as_str().expect("add_query SQL");

    assert_eq!(executed.matches("%s").count(), 2, "{executed}");
    assert!(!executed.contains("$1"), "{executed}");
    assert_eq!(rendered.matches("%s").count(), 2, "{rendered}");
}

fn incremental_config() -> Arc<MockJinjaObject> {
    let config = default_mock_config();
    config.on("get", |args| {
        let key = args.first().and_then(Value::as_str);
        let default = args.get(1).cloned().unwrap_or(Value::UNDEFINED);
        match key {
            Some("contract") => Ok(Value::from_serialize(BTreeMap::from([("enforced", false)]))),
            Some("full_refresh") => Ok(Value::from(false)),
            Some("incremental_strategy") => Ok(Value::from("test")),
            Some("on_schema_change") => Ok(Value::from("ignore")),
            Some("unique_key") => Ok(Value::from("id")),
            _ => Ok(default),
        }
    });
    config
}

fn second_incremental_run(adapter_type: AdapterType, dml: &'static str) -> MacroTestHarness {
    let harness = MacroTestHarness::for_adapter(adapter_type)
        .load_all_macros()
        .with_stub_functions()
        .build()
        .expect("incremental macro harness should build");
    let existing = harness.relation(
        "TEST_DB",
        "TEST_SCHEMA",
        "my_incremental",
        Some(RelationType::Table),
    );
    harness.mock().on("get_relation", move |_| {
        Ok(RelationObject::new(Arc::clone(&existing)).into_value())
    });
    harness.mock().on("has_feature", move |args| {
        Ok(Value::from(match args.first().and_then(Value::as_str) {
            Some("motherduck") => adapter_type == AdapterType::MotherDuck,
            Some("transactions") => true,
            _ => false,
        }))
    });
    harness.mock().on("get_temp_relation_path", |_| {
        Ok(Value::from_serialize(BTreeMap::from([
            ("database", "TEST_DB"),
            ("schema", "dbt_temp"),
            ("identifier", "my_incremental__dbt_tmp"),
        ])))
    });
    harness
        .mock()
        .on("generate_unique_temporary_table_suffix", |args| {
            Ok(args.first().cloned().unwrap_or(Value::from("__dbt_tmp")))
        });
    harness.mock().on("drop_relation", |_| Ok(Value::UNDEFINED));
    harness.mock().on("commit", |_| Ok(Value::UNDEFINED));
    harness.mock().on("get_columns_in_relation", |_| {
        Ok(Value::from_serialize([BTreeMap::from([
            ("name", "id"),
            ("quoted", "\"id\""),
        ])]))
    });
    harness.mock().on("build_catalog_relation", |_| {
        Ok(Value::from_serialize(BTreeMap::from([(
            "duckdb_write_strategy",
            "create_as_select",
        )])))
    });
    harness
        .mock()
        .on("get_incremental_strategy_macro", move |_| {
            Ok(Value::from_function(
                move |_args: &[Value]| -> Result<Value, minijinja::Error> { Ok(Value::from(dml)) },
            ))
        });

    let model = Value::from_serialize(BTreeMap::from([
        ("alias", Value::from("my_incremental")),
        (
            "unique_id",
            Value::from("model.test_project.my_incremental"),
        ),
        ("columns", Value::from(BTreeMap::<String, Value>::new())),
        ("language", Value::from("sql")),
        ("compiled_code", Value::from("select 2 as id")),
    ]));
    let ctx = harness
        .materialization_context("my_incremental", "select 2 as id")
        .relation_type(RelationType::Table)
        .config(Value::from_dyn_object(incremental_config()))
        .with("model", model)
        .with("dbt_version", Value::from("2.0.0"))
        .with("context", Value::from(BTreeMap::<String, Value>::new()))
        .build();
    harness
        .render("{{ materialization_incremental_duckdb() }}", ctx)
        .expect("second incremental run should render");
    harness
}

fn assert_motherduck_staging_precedes(dml: &'static str) {
    let harness = second_incremental_run(AdapterType::MotherDuck, dml);
    let sqls = executed_sql(harness.mock());
    let create = sqls
        .iter()
        .position(|sql| sql.contains("select 2 as id"))
        .expect("staging CREATE statement");
    let dml_index = sqls
        .iter()
        .position(|sql| sql.contains(dml))
        .expect("incremental DML statement");

    assert!(
        create < dml_index,
        "staging CREATE must precede DML: {sqls:?}"
    );
    assert!(
        !sqls[create].contains(dml),
        "CREATE and DML must be separate"
    );
    assert!(
        !sqls[dml_index].contains("select 2 as id"),
        "DML must not prepare the dependent CREATE"
    );
}

#[test]
fn second_incremental_insert_executes_staging_separately() {
    assert_motherduck_staging_precedes("INSERT INTO incremental_target SELECT * FROM staging");
}

#[test]
fn second_incremental_merge_executes_staging_separately() {
    assert_motherduck_staging_precedes(
        "MERGE INTO incremental_target USING staging ON FALSE WHEN NOT MATCHED THEN INSERT BY NAME",
    );
}

#[test]
fn duckdb_second_incremental_run_keeps_staging_and_dml_together() {
    const DML: &str = "INSERT INTO incremental_target SELECT * FROM staging";
    let harness = second_incremental_run(AdapterType::DuckDB, DML);
    let sqls = executed_sql(harness.mock());
    assert!(
        sqls.iter()
            .any(|sql| sql.contains("select 2 as id") && sql.contains(DML)),
        "DuckDB should keep staging CREATE and DML in one execute call: {sqls:?}"
    );
}

#[test]
fn inherited_metadata_macros_escape_string_literals() {
    const MALICIOUS: &str = "x' OR 1=1 --";
    const ESCAPED: &str = "x'' OR 1=1 --";

    let harness = harness();
    let information_schema = harness.relation(
        MALICIOUS,
        "information_schema",
        "",
        Some(RelationType::View),
    );
    let catalog_sql = harness
        .render(
            "{{ duckdb__get_catalog_tables_sql(information_schema) }}",
            BTreeMap::from([(
                "information_schema".to_string(),
                RelationObject::new(information_schema).into_value(),
            )]),
        )
        .expect("catalog database filter should render");
    assert_eq!(catalog_sql.matches(ESCAPED).count(), 2, "{catalog_sql}");

    let schema_sql = harness
        .render(
            "{{ duckdb__get_catalog_schemas_where_clause_sql(schemas) }}",
            BTreeMap::from([(
                "schemas".to_string(),
                Value::from(vec![Value::from(MALICIOUS)]),
            )]),
        )
        .expect("catalog schema filter should render");
    assert!(schema_sql.contains(ESCAPED), "{schema_sql}");

    let relation = harness.relation("TEST_DB", MALICIOUS, MALICIOUS, Some(RelationType::Table));
    let relation_value = RelationObject::new(Arc::clone(&relation)).into_value();
    let relation_sql = harness
        .render(
            "{{ duckdb__get_catalog_relations_where_clause_sql(relations) }}",
            BTreeMap::from([("relations".to_string(), Value::from(vec![relation_value]))]),
        )
        .expect("catalog relation filter should render");
    assert_eq!(relation_sql.matches(ESCAPED).count(), 2, "{relation_sql}");

    harness
        .render(
            "{{ drop_indexes_on_relation(relation) }}",
            BTreeMap::from([
                (
                    "relation".to_string(),
                    RelationObject::new(relation).into_value(),
                ),
                ("execute".to_string(), Value::from(true)),
            ]),
        )
        .expect("index metadata query should execute");
    let sqls = executed_sql(harness.mock());
    let index_queries = sqls
        .iter()
        .filter(|sql| sql.contains("duckdb_indexes()"))
        .collect::<Vec<_>>();
    assert!(
        !index_queries.is_empty()
            && index_queries
                .iter()
                .all(|sql| sql.matches(ESCAPED).count() == 2),
        "schema/model literals must be escaped: {sqls:?}"
    );
}

#[test]
fn inherited_create_schema_escapes_database_literal() {
    const MALICIOUS: &str = "x' OR 1=1 --";
    const ESCAPED: &str = "x'' or 1=1 --";

    let harness = harness();
    let relation = harness.relation(MALICIOUS, "safe_schema", "", None);
    harness
        .render(
            "{{ duckdb__create_schema(relation) }}",
            BTreeMap::from([
                (
                    "relation".to_string(),
                    RelationObject::new(relation).into_value(),
                ),
                ("execute".to_string(), Value::from(true)),
            ]),
        )
        .expect("create_schema should execute");
    let sqls = executed_sql(harness.mock());
    assert!(
        sqls.iter().any(|sql| sql.contains(ESCAPED)),
        "database literal must be escaped: {sqls:?}"
    );
}
