use std::collections::BTreeMap;

use dbt_adapter::catalog_relation::CatalogRelation;
use dbt_adapter::relation::RelationObject;
use dbt_adapter_core::AdapterType;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::dbt_catalogs_v2::CatalogType;
use dbt_schemas::schemas::relations::base::TableFormat;
use minijinja::Value;

use crate::macro_test_harness::{MacroTestHarness, default_mock_config};

mod databricks {
    use super::*;

    fn render_primary_key_constraint(expression: Option<&str>) -> String {
        let harness = MacroTestHarness::for_adapter(AdapterType::Databricks)
            .load_all_macros()
            .with_stub_functions()
            .build()
            .expect("constraint harness should build");
        harness.mock().on("quote", |args| {
            let identifier = args
                .first()
                .and_then(Value::as_str)
                .expect("quote expects a string identifier");
            Ok(Value::from(format!("`{identifier}`")))
        });

        let relation = harness.relation(
            "TEST_DB",
            "TEST_SCHEMA",
            "constrained_model",
            Some(RelationType::Table),
        );
        let mut constraint = BTreeMap::from([
            ("type".to_string(), Value::from("primary_key")),
            ("columns".to_string(), Value::from(vec![Value::from("id")])),
        ]);
        if let Some(expression) = expression {
            constraint.insert("expression".to_string(), Value::from(expression));
        }

        let columns = BTreeMap::from([(
            "id".to_string(),
            BTreeMap::from([("name".to_string(), "id".to_string())]),
        )]);
        let ctx = BTreeMap::from([
            (
                "relation".to_string(),
                RelationObject::new(relation).into_value(),
            ),
            ("constraint".to_string(), Value::from_serialize(constraint)),
            (
                "model".to_string(),
                Value::from_serialize(BTreeMap::from([("columns", columns)])),
            ),
        ]);

        harness
            .render(
                "{{ get_constraint_sql(relation, constraint, model) | join('\\n') }}",
                ctx,
            )
            .expect("primary-key constraint should render")
    }

    fn generated_constraint_name(sql: &str) -> &str {
        sql.split_once("add constraint ")
            .and_then(|(_, suffix)| suffix.split_once(" primary key"))
            .map(|(name, _)| name)
            .expect("rendered SQL should contain a primary-key constraint name")
    }

    #[test]
    fn primary_key_expression_is_rendered_and_changes_generated_name() {
        let without_expression = render_primary_key_constraint(None);
        let with_expression = render_primary_key_constraint(Some("timeseries"));

        assert!(
            with_expression.contains("primary key(`id`) timeseries;"),
            "primary-key expression should be appended to the DDL: {with_expression}"
        );
        assert_eq!(
            generated_constraint_name(&without_expression),
            "f82f3cec0489fd1683773f6573fdf556"
        );
        assert_eq!(
            generated_constraint_name(&with_expression),
            "c5a2df3443aff7d5b8c474aea39c6265",
            "the generated name must match the current-v1 hash input"
        );
    }

    fn constraint_config(persist_constraints: bool) -> Value {
        let config = default_mock_config();
        config.on("get", move |args| {
            let key = args.first().and_then(Value::as_str);
            let default = args.get(1).cloned().unwrap_or(Value::UNDEFINED);
            match key {
                Some("persist_constraints") => Ok(Value::from(persist_constraints)),
                Some("contract") => Ok(Value::from_serialize(BTreeMap::from([(
                    "enforced".to_string(),
                    Value::from(false),
                )]))),
                _ => Ok(default),
            }
        });
        Value::from_dyn_object(config)
    }

    fn render_legacy_constraint_macro(
        expression: &str,
        context: BTreeMap<String, Value>,
    ) -> dbt_common::FsResult<String> {
        MacroTestHarness::for_adapter(AdapterType::Databricks)
            .load_all_macros()
            .with_stub_functions()
            .build()
            .expect("constraint harness should build")
            .render(expression, context)
    }

    #[test]
    fn legacy_constraint_metadata_respects_persist_constraints_and_validates_shape() {
        let model = Value::from_serialize(BTreeMap::from([(
            "meta".to_string(),
            BTreeMap::from([(
                "constraints".to_string(),
                vec![BTreeMap::from([
                    ("name".to_string(), "positive_id".to_string()),
                    ("condition".to_string(), "id > 0".to_string()),
                ])],
            )]),
        )]));

        let enabled = render_legacy_constraint_macro(
            "{{ get_model_constraints(model) | tojson }}",
            BTreeMap::from([
                ("config".to_string(), constraint_config(true)),
                ("model".to_string(), model.clone()),
            ]),
        )
        .expect("valid legacy model constraint should render");
        let disabled = render_legacy_constraint_macro(
            "{{ get_model_constraints(model) | tojson }}",
            BTreeMap::from([
                ("config".to_string(), constraint_config(false)),
                ("model".to_string(), model),
            ]),
        )
        .expect("disabled legacy constraints should render an empty list");

        assert!(
            enabled.contains("positive_id") && enabled.contains("expression"),
            "enabled legacy constraints should be translated: {enabled:?}"
        );
        assert_eq!(disabled.trim(), "[]");

        let invalid_model = Value::from_serialize(BTreeMap::from([(
            "meta".to_string(),
            BTreeMap::from([(
                "constraints".to_string(),
                vec![BTreeMap::from([(
                    "condition".to_string(),
                    "id > 0".to_string(),
                )])],
            )]),
        )]));
        let error = render_legacy_constraint_macro(
            "{{ get_model_constraints(model) | tojson }}",
            BTreeMap::from([
                ("config".to_string(), constraint_config(true)),
                ("model".to_string(), invalid_model),
            ]),
        )
        .expect_err("legacy check constraints without a name must fail");
        assert!(error.to_string().contains("Invalid check constraint name"));
    }

    fn build_comment_clause_harness() -> MacroTestHarness {
        let databricks_comment_sql =
            include_str!("../../src/dbt_macro_assets/dbt-databricks/macros/relations/comment.sql");

        let dispatching_comment_clause = r#"
{% macro comment_clause() -%}
  {{ adapter.dispatch('comment_clause', 'dbt')() }}
{%- endmacro %}
"#;

        MacroTestHarness::for_adapter(AdapterType::Databricks)
            .with_macro("dbt", "comment_clause", dispatching_comment_clause)
            .with_macro_at_path(
                "dbt_databricks",
                "databricks__comment_clause",
                databricks_comment_sql,
                "dbt_macro_assets/dbt-databricks/macros/relations/comment.sql",
            )
            .build()
            .expect("harness should build")
    }

    // `databricks__create_table_as` calls these clause helpers (defined in other asset files)
    // unconditionally. Each must resolve, but the only thing under test here is the
    // create/replace branch, so we register them as no-ops.
    const CLAUSE_STUBS: [&str; 7] = [
        "file_format_clause",
        "partition_cols",
        "liquid_clustered_cols",
        "clustered_cols",
        "location_clause",
        "comment_clause",
        "tblproperties_clause",
    ];

    fn build_create_table_harness() -> MacroTestHarness {
        let databricks_create_table_sql = include_str!(
            "../../src/dbt_macro_assets/dbt-databricks/macros/relations/table/create.sql"
        );

        let mut builder = MacroTestHarness::for_adapter(AdapterType::Databricks)
            .with_macro_at_path(
                "dbt_databricks",
                "databricks__create_table_as",
                databricks_create_table_sql,
                "dbt-databricks/macros/relations/table/create.sql",
            );
        for name in CLAUSE_STUBS {
            builder = builder.with_macro(
                "dbt_databricks",
                name,
                &format!("{{% macro {name}() %}}{{% endmacro %}}"),
            );
        }
        builder.build().expect("create table harness should build")
    }

    fn ctx_for(description: &str) -> BTreeMap<String, Value> {
        BTreeMap::from([
            (
                "config".to_string(),
                Value::from_serialize(BTreeMap::from([(
                    "persist_docs".to_string(),
                    BTreeMap::from([("relation".to_string(), true)]),
                )])),
            ),
            (
                "model".to_string(),
                Value::from_serialize(BTreeMap::from([(
                    "description".to_string(),
                    description.to_string(),
                )])),
            ),
        ])
    }

    #[test]
    fn comment_clause_does_not_render_empty_comment() {
        let harness = build_comment_clause_harness();
        let rendered = harness
            .render("{{ comment_clause() }}", ctx_for(""))
            .expect("render should succeed");

        assert_eq!(rendered.trim(), "");
        assert!(
            !rendered.contains("comment ''"),
            "Should never render an empty comment clause, got: {rendered:?}"
        );
    }

    #[test]
    fn comment_clause_renders_non_empty_comment() {
        let harness = build_comment_clause_harness();
        let rendered = harness
            .render("{{ comment_clause() }}", ctx_for("hello"))
            .expect("render should succeed");

        assert!(
            rendered.contains("comment 'hello'"),
            "Expected non-empty comment clause, got: {rendered:?}"
        );
    }

    /// Render `databricks__create_table_as` for a Databricks relation with the given catalog
    /// shape (the #10647 area), toggling the `use_catalogs_v2` behavior flag.
    fn render_databricks_create_table(
        use_catalogs_v2: bool,
        catalog_type: &str,
        table_format: &str,
        file_format: Option<&str>,
    ) -> String {
        let harness = build_create_table_harness();

        if use_catalogs_v2 {
            enable_catalogs_v2();
        }

        harness.mock().set_attr(
            "behavior",
            Value::from_serialize(BTreeMap::from([(
                "use_catalogs_v2",
                BTreeMap::from([("no_warn", use_catalogs_v2)]),
            )])),
        );

        let relation = Value::from_object(CatalogRelation {
            catalog_type: if catalog_type.eq_ignore_ascii_case("hive_metastore") {
                CatalogType::HiveMetastore
            } else {
                CatalogType::Unity
            },
            table_format: if table_format.eq_ignore_ascii_case("iceberg") {
                TableFormat::Iceberg
            } else {
                TableFormat::Default
            },
            file_format: file_format.map(str::to_string),
            ..CatalogRelation::default_catalog_relation_databricks()
        });
        harness
            .mock()
            .on("build_catalog_relation", move |_| Ok(relation.clone()));

        let ctx = harness
            .materialization_context("customers", "select 1")
            .relation_type(RelationType::Table)
            .with("dbt_version", Value::from("2.0.0"))
            .build();

        harness
            .render(
                "{{ databricks__create_table_as(false, this, 'select 1') }}",
                ctx,
            )
            .expect("render should succeed")
    }

    fn enable_catalogs_v2() {
        let catalogs = dbt_yaml::from_str("catalogs: []\n").expect("valid catalogs.yml v2");
        let project_flags =
            dbt_yaml::from_str("use_catalogs_v2: true\n").expect("valid project flags");
        dbt_adapter::load_catalogs::do_load_catalogs(
            catalogs,
            std::path::Path::new("catalogs.yml"),
            Some(&project_flags),
        )
        .expect("catalogs.yml v2 should load");
    }

    #[test]
    fn managed_iceberg_uses_create_or_replace_under_catalogs_v2() {
        let rendered = render_databricks_create_table(true, "unity", "iceberg", Some("parquet"));
        assert!(
            rendered.to_lowercase().contains("create or replace table"),
            "managed iceberg under catalogs v2 must use `create or replace table`, got:\n{rendered}"
        );
    }

    #[test]
    fn non_replaceable_relation_keeps_plain_create_under_catalogs_v2() {
        let rendered = render_databricks_create_table(true, "unity", "default", Some("parquet"));
        let lower = rendered.to_lowercase();
        assert!(
            !lower.contains("create or replace table") && lower.contains("create table"),
            "non-replaceable relation under catalogs v2 must keep `create table`, got:\n{rendered}"
        );
    }

    #[test]
    fn managed_iceberg_keeps_v1_behavior_without_catalogs_v2() {
        let rendered = render_databricks_create_table(false, "unity", "iceberg", Some("parquet"));
        let lower = rendered.to_lowercase();
        assert!(
            !lower.contains("create or replace table") && lower.contains("create table"),
            "managed iceberg without catalogs v2 must keep `create table`, got:\n{rendered}"
        );
    }
}
