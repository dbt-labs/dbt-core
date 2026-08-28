use std::collections::BTreeMap;

use dbt_adapter::catalog_relation::CatalogRelation;
use dbt_adapter_core::AdapterType;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::dbt_catalogs_v2::CatalogType;
use dbt_schemas::schemas::relations::base::TableFormat;
use minijinja::Value;

use crate::macro_test_harness::MacroTestHarness;

mod databricks {
    use super::*;

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
    const CLAUSE_STUBS: [&str; 8] = [
        "file_format_clause",
        "partition_cols",
        "get_create_row_filter_clause",
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

    fn build_location_clause_harness() -> MacroTestHarness {
        let databricks_location_sql =
            include_str!("../../src/dbt_macro_assets/dbt-databricks/macros/relations/location.sql");

        MacroTestHarness::for_adapter(AdapterType::Databricks)
            .with_macro(
                "dbt",
                "is_incremental",
                "{% macro is_incremental() %}{{ return(false) }}{% endmacro %}",
            )
            .with_macro_at_path(
                "dbt_databricks",
                "location_clause",
                databricks_location_sql,
                "dbt_macro_assets/dbt-databricks/macros/relations/location.sql",
            )
            .build()
            .expect("location clause harness should build")
    }

    fn render_location_clause(external_volume: Option<&str>) -> String {
        let harness = build_location_clause_harness();
        let relation = Value::from_object(CatalogRelation {
            external_volume: external_volume.map(str::to_string),
            ..CatalogRelation::default_catalog_relation_databricks()
        });

        harness
            .render(
                "{{ location_clause(relation) }}",
                BTreeMap::from([("relation".to_string(), relation)]),
            )
            .expect("render should succeed")
    }

    #[test]
    fn location_clause_renders_location_from_relation() {
        let rendered = render_location_clause(Some("s3://bucket/root/a"));
        assert!(
            rendered.contains("location 's3://bucket/root/a'"),
            "expected a location clause, got: {rendered:?}"
        );
    }

    #[test]
    fn location_clause_renders_nothing_without_location() {
        let rendered = render_location_clause(None);
        assert!(
            !rendered.to_lowercase().contains("location"),
            "expected no location clause, got: {rendered:?}"
        );
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

    fn build_file_format_harness() -> MacroTestHarness {
        let file_format_sql = include_str!(
            "../../src/dbt_macro_assets/dbt-databricks/macros/relations/file_format.sql"
        );

        MacroTestHarness::for_adapter(AdapterType::Databricks)
            .with_macro_at_path(
                "dbt_databricks",
                "file_format_clause",
                file_format_sql,
                "dbt-databricks/macros/relations/file_format.sql",
            )
            .build()
            .expect("file format harness should build")
    }

    fn render_file_format_clause(
        use_catalogs_v2: bool,
        use_managed_iceberg: bool,
        relation: CatalogRelation,
    ) -> String {
        let harness = build_file_format_harness();

        if use_catalogs_v2 {
            enable_catalogs_v2();
        }

        harness.mock().set_attr(
            "behavior",
            Value::from_serialize(serde_json::json!({
                "use_catalogs_v2": { "no_warn": use_catalogs_v2 },
                "use_managed_iceberg": use_managed_iceberg,
            })),
        );

        let ctx = BTreeMap::from([
            ("dbt_version".to_string(), Value::from("2.0.0")),
            ("relation".to_string(), Value::from_object(relation)),
        ]);

        harness
            .render("{{ file_format_clause(relation) }}", ctx)
            .expect("render should succeed")
    }

    #[test]
    fn iceberg_without_catalogs_yml_renders_uniform_delta_v1_by_default() {
        let relation = CatalogRelation::default_catalog_relation_databricks()
            .with_table_format(TableFormat::Iceberg)
            .with_adapter_property("use_uniform", "false");
        let rendered = render_file_format_clause(false, false, relation);
        let lower = rendered.to_lowercase();
        assert!(
            lower.contains("using delta") && !lower.contains("using iceberg"),
            "table_format=iceberg without catalogs.yml must default to UniForm (`using delta`) in v1, unchanged from today, got:\n{rendered}"
        );
    }

    #[test]
    fn iceberg_renders_managed_iceberg_when_use_managed_iceberg_enabled_v1() {
        let relation = CatalogRelation::default_catalog_relation_databricks()
            .with_table_format(TableFormat::Iceberg)
            .with_adapter_property("use_uniform", "false");
        let rendered = render_file_format_clause(false, true, relation);
        let lower = rendered.to_lowercase();
        assert!(
            lower.contains("using iceberg") && !lower.contains("using delta"),
            "table_format=iceberg with use_managed_iceberg: true must opt into managed Iceberg (`using iceberg`), got:\n{rendered}"
        );
    }

    #[test]
    fn iceberg_without_catalog_name_renders_uniform_delta_explicit_opt_in_v2() {
        let relation = CatalogRelation::default_catalog_relation_databricks()
            .with_table_format(TableFormat::Iceberg)
            .with_adapter_property("use_uniform", "true");
        let rendered = render_file_format_clause(true, true, relation);
        let lower = rendered.to_lowercase();
        assert!(
            lower.contains("using delta") && !lower.contains("using iceberg"),
            "table_format=iceberg with explicit use_uniform:true (v2) must render UniForm (`using delta`), got:\n{rendered}"
        );
    }

    #[test]
    fn iceberg_without_catalogs_yml_renders_managed_iceberg_even_with_use_catalogs_v2_flag() {
        let relation = CatalogRelation::default_catalog_relation_databricks()
            .with_table_format(TableFormat::Iceberg)
            .with_adapter_property("use_uniform", "false");
        let rendered = render_file_format_clause(true, true, relation);
        let lower = rendered.to_lowercase();
        assert!(
            lower.contains("using iceberg") && !lower.contains("using delta"),
            "table_format=iceberg with use_catalogs_v2=true but no catalogs.yml must still default to managed Iceberg (`using iceberg`), got:\n{rendered}"
        );
    }
}
