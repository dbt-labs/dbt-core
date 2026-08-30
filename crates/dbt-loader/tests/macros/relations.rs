use std::collections::BTreeMap;

use dbt_adapter::catalog_relation::CatalogRelation;
use dbt_adapter::relation::RelationObject;
use dbt_adapter_core::AdapterType;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::dbt_catalogs_v2::CatalogType;
use dbt_schemas::schemas::project::{ModelConfig, ProjectModelConfig};
use dbt_schemas::schemas::relations::base::TableFormat;
use indexmap::IndexMap;
use minijinja::Value;

use crate::macro_test_harness::{MacroTestHarness, default_mock_config};

mod databricks {
    use super::*;

    const STATEMENT_STUB: &str = r#"
{% macro statement(name=none, fetch_result=False, auto_begin=True, language='sql') -%}
  [statement:{{ name }}]{{ caller() }}[/statement]
{%- endmacro %}
"#;

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

    #[test]
    fn tblproperties_clause_preserves_project_declaration_order() {
        let databricks_tblproperties_sql = include_str!(
            "../../src/dbt_macro_assets/dbt-databricks/macros/relations/tblproperties.sql"
        );
        let harness = MacroTestHarness::for_adapter(AdapterType::Databricks)
            .with_macro_at_path(
                "dbt_databricks",
                "databricks__tblproperties_clause",
                databricks_tblproperties_sql,
                "dbt-databricks/macros/relations/tblproperties.sql",
            )
            .build()
            .expect("tblproperties harness should build");
        harness.mock().on("is_uniform", |_| Ok(Value::from(false)));

        let project_config: ProjectModelConfig = dbt_yaml::from_str(
            r#"
+tblproperties:
  zeta: last
  alpha: first
  middle: center
__additional_properties__: {}
"#,
        )
        .expect("project config should parse");
        let model_config: ModelConfig = project_config.into();
        let tblproperties = model_config
            .__warehouse_specific_config__
            .tblproperties
            .expect("tblproperties should be present");
        let config = IndexMap::from([(
            "tblproperties".to_string(),
            Value::from_serialize(tblproperties),
        )]);

        let rendered = harness
            .render(
                "{{ databricks__tblproperties_clause() }}",
                BTreeMap::from([("config".to_string(), Value::from_serialize(config))]),
            )
            .expect("tblproperties clause should render");

        let zeta = rendered.find("'zeta' = 'last'").expect("zeta property");
        let alpha = rendered.find("'alpha' = 'first'").expect("alpha property");
        let middle = rendered
            .find("'middle' = 'center'")
            .expect("middle property");
        assert!(
            zeta < alpha && alpha < middle,
            "rendered clause: {rendered}"
        );
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

    fn build_optimize_harness() -> MacroTestHarness {
        let harness = MacroTestHarness::for_adapter(AdapterType::Databricks)
            .load_all_macros()
            .with_macro("dbt", "statement", STATEMENT_STUB)
            .build()
            .expect("optimize harness should build");

        harness
            .mock()
            .on("resolve_file_format", |_| Ok(Value::from("delta")));

        harness
    }

    fn render_optimize(
        config_values: BTreeMap<String, Value>,
        vars: BTreeMap<String, bool>,
    ) -> String {
        let mut harness = build_optimize_harness();
        let config = default_mock_config();
        config.on("get", move |args| {
            let key = args.first().and_then(Value::as_str);
            let default = args.get(1).cloned().unwrap_or(Value::UNDEFINED);
            Ok(key
                .and_then(|key| config_values.get(key).cloned())
                .unwrap_or(default))
        });

        harness
            .env_mut()
            .env
            .add_function("var", move |name: Value, default: Option<Value>| {
                let value = name
                    .as_str()
                    .and_then(|name| vars.get(name))
                    .copied()
                    .map(Value::from);
                Ok(value.unwrap_or_else(|| default.unwrap_or(Value::UNDEFINED)))
            });

        let relation = RelationObject::new(harness.relation(
            "TEST_DB",
            "TEST_SCHEMA",
            "target",
            Some(RelationType::Table),
        ))
        .into_value();
        harness
            .render(
                "{{ optimize(relation) }}",
                BTreeMap::from([
                    ("config".to_string(), Value::from_dyn_object(config)),
                    ("relation".to_string(), relation),
                ]),
            )
            .expect("optimize should render")
    }

    #[test]
    fn optimize_macro_skips_configured_clustering() {
        for (cluster_key, cluster_value) in [
            ("zorder", Value::from("id")),
            (
                "liquid_clustered_by",
                Value::from_serialize(vec!["id".to_string()]),
            ),
            ("auto_liquid_cluster", Value::from(true)),
        ] {
            let rendered = render_optimize(
                BTreeMap::from([
                    (cluster_key.to_string(), cluster_value),
                    ("skip_optimize".to_string(), Value::from(true)),
                ]),
                BTreeMap::new(),
            );
            assert!(
                !rendered.to_lowercase().contains("optimize"),
                "skip_optimize must suppress post-materialization OPTIMIZE for {cluster_key}, got: {rendered:?}"
            );
        }
    }

    #[test]
    fn optimize_macro_preserves_existing_guards() {
        let cluster = BTreeMap::from([(String::from("zorder"), Value::from("id"))]);

        let rendered = render_optimize(cluster.clone(), BTreeMap::new());
        assert!(
            rendered.to_lowercase().contains("optimize"),
            "absent skip_optimize must preserve optimize, got: {rendered:?}"
        );

        let rendered = render_optimize(
            BTreeMap::from([
                (String::from("zorder"), Value::from("id")),
                (String::from("skip_optimize"), Value::from(false)),
            ]),
            BTreeMap::new(),
        );
        assert!(
            rendered.to_lowercase().contains("optimize"),
            "false skip_optimize must preserve optimize, got: {rendered:?}"
        );

        for variable in ["DATABRICKS_SKIP_OPTIMIZE", "databricks_skip_optimize"] {
            let rendered = render_optimize(
                cluster.clone(),
                BTreeMap::from([(variable.to_string(), true)]),
            );
            assert!(
                !rendered.to_lowercase().contains("optimize"),
                "{variable} must still suppress optimize, got: {rendered:?}"
            );
        }
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

    fn render_constraint_changeset(contract_enforced: bool) -> String {
        let alter_sql = include_str!(
            "../../src/dbt_macro_assets/dbt-databricks/macros/relations/table/alter.sql"
        );
        let harness = MacroTestHarness::for_adapter(AdapterType::Databricks)
            .with_macro_at_path(
                "dbt_databricks",
                "apply_config_changeset",
                alter_sql,
                "dbt-databricks/macros/relations/table/alter.sql",
            )
            .with_macro(
                "test_project",
                "apply_constraints",
                "{% macro apply_constraints(relation, constraints) %}APPLY_CONSTRAINTS_SENTINEL{% endmacro %}",
            )
            .with_stub_functions()
            .build()
            .expect("constraint changeset harness should build");
        let config = default_mock_config();
        config.on("get", move |args| {
            let key = args.first().and_then(Value::as_str);
            let default = args.get(1).cloned().unwrap_or(Value::UNDEFINED);
            match key {
                Some("contract") => Ok(Value::from_serialize(BTreeMap::from([(
                    "enforced",
                    contract_enforced,
                )]))),
                _ => Ok(default),
            }
        });
        let changes = Value::from_serialize(BTreeMap::from([(
            "changes",
            BTreeMap::from([("constraints", Value::from(true))]),
        )]));
        let ctx = harness
            .materialization_context("constraint_table", "select 1")
            .relation_type(RelationType::Table)
            .config(Value::from_dyn_object(config))
            .with("configuration_changes", changes)
            .build();

        harness
            .render(
                "{{ apply_config_changeset(this, model, configuration_changes) }}",
                ctx,
            )
            .expect("constraint changeset should render")
    }

    #[test]
    fn table_alter_skips_constraint_reconciliation_when_contract_is_unenforced() {
        let rendered = render_constraint_changeset(false);
        assert!(
            !rendered.contains("APPLY_CONSTRAINTS_SENTINEL"),
            "unenforced contract must not apply constraints, got: {rendered}"
        );
    }

    #[test]
    fn table_alter_reconciles_constraints_when_contract_is_enforced() {
        let rendered = render_constraint_changeset(true);
        assert!(
            rendered.contains("APPLY_CONSTRAINTS_SENTINEL"),
            "enforced contract must apply constraints, got: {rendered}"
        );
    }
}
