use std::collections::BTreeMap;
use std::sync::Arc;

use dbt_adapter::relation::RelationObject;
use dbt_adapter_core::AdapterType;
use dbt_jinja_utils::flags::Flags;
use dbt_jinja_utils::mock_object::MockJinjaObject;
use dbt_schemas::dbt_types::RelationType;
use minijinja::Value;

use crate::macro_test_harness::MacroTestHarness;

fn build_harness() -> MacroTestHarness {
    MacroTestHarness::for_adapter(AdapterType::Bigquery)
        .load_all_macros()
        .with_stub_functions()
        .build()
        .expect("harness should build")
}

#[test]
fn snapshot_check_cols_matches_existing_column_case_insensitively() {
    let harness = MacroTestHarness::for_adapter(AdapterType::Bigquery)
        .load_all_macros()
        .with_macro(
            "dbt_bigquery",
            "bigquery__get_columns_in_query",
            "{% macro bigquery__get_columns_in_query(select_sql) %}{{ return(['dice_change_hash']) }}{% endmacro %}",
        )
        .build()
        .expect("harness should build");

    harness
        .mock()
        .on("get_relation", |_| Ok(Value::from("snapshot_target")));
    harness.mock().on("get_columns_in_relation", |_| {
        Ok(Value::from_serialize(vec![BTreeMap::from([(
            "name",
            "DICE_CHANGE_HASH",
        )])]))
    });
    harness.mock().on("quote", |args| {
        Ok(Value::from(format!(
            "`{}`",
            args[0].as_str().expect("column name")
        )))
    });

    let node = BTreeMap::from([
        ("database", "test-db"),
        ("schema", "test_schema"),
        ("alias", "member_snapshot"),
        ("compiled_code", "select DICE_CHANGE_HASH from source"),
    ]);
    let rendered = harness
        .render(
            "{% set added, cols = snapshot_check_all_get_existing_columns(node, true, ['dice_change_hash']) %}{{ 'added' if added else 'existing' }}|{{ cols | join(',') }}",
            BTreeMap::from([("node", Value::from_serialize(node))]),
        )
        .expect("snapshot check should render");

    assert_eq!(rendered.trim(), "existing|`dice_change_hash`");
}

fn base_relation_ctx(harness: &MacroTestHarness) -> BTreeMap<String, Value> {
    let relation = harness.relation(
        "test-db",
        "test_schema",
        "member_snapshot",
        Some(RelationType::Table),
    );
    BTreeMap::from([
        (
            "base_relation".to_string(),
            RelationObject::new(relation).into_value(),
        ),
        // `make_temp_relation` (dbt-adapters/macros/adapters/relation.sql) reads
        // `model.batch` before dispatching; a real dbt `model` always has this key
        // (None for non-microbatch models), so mirror that shape here.
        (
            "model".to_string(),
            Value::from_serialize(BTreeMap::from([("batch", Value::from(()))])),
        ),
    ])
}

#[test]
fn make_temp_relation_appends_unique_suffix() {
    // dbt Core's dbt-bigquery adapter appends a `strftime("%H%M%S%f")` suffix to
    // `__dbt_tmp` temp relations (via `bigquery__make_relation_with_suffix`) to avoid
    // collisions across concurrent runs. Fusion previously fell back to the generic
    // `default__make_temp_relation`, which reused the bare `__dbt_tmp` identifier and
    // caused a conformance SQL mismatch against dbt Core (#8247).
    let harness = build_harness();
    let ctx = base_relation_ctx(&harness);

    let rendered = harness
        .render("{{ make_temp_relation(base_relation).identifier }}", ctx)
        .expect("render should succeed");
    let rendered = rendered.trim();

    let suffix = rendered
        .strip_prefix("member_snapshot__dbt_tmp")
        .unwrap_or_else(|| panic!("expected __dbt_tmp-prefixed identifier, got: {rendered:?}"));
    assert!(
        !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit()),
        "BigQuery temp relation should append an all-digit timestamp suffix, got: {rendered:?}"
    );
}

#[test]
fn make_intermediate_relation_does_not_append_suffix() {
    // Unlike `make_temp_relation`, the intermediate/backup relation paths
    // (`dstring=False`) must keep the bare suffix untouched.
    let harness = build_harness();
    let ctx = base_relation_ctx(&harness);

    let rendered = harness
        .render(
            "{{ make_intermediate_relation(base_relation).identifier }}",
            ctx,
        )
        .expect("render should succeed");

    assert_eq!(rendered.trim(), "member_snapshot__dbt_tmp");
}

// ---------------------------------------------------------------------------
// bigquery__generate_schema_name (LRC 4-part namespace)
// ---------------------------------------------------------------------------
const BQ_GENERATE_SCHEMA_NAME: &str = "{{ bigquery__generate_schema_name('staging', node) }}";
const DEFAULT__GENERATE_SCHEMA_NAME: &str = r#"
{% macro default__generate_schema_name(custom_schema_name, node) -%}
    custom_{{ custom_schema_name | trim }}
{%- endmacro %}
"#;

fn schema_name_harness(use_catalogs_v2: bool, override_default: bool) -> MacroTestHarness {
    let mut project_flags = BTreeMap::new();
    if use_catalogs_v2 {
        project_flags.insert("use_catalogs_v2".to_string(), Value::from(true));
    }

    let mut builder = MacroTestHarness::for_adapter(AdapterType::Bigquery)
        .load_all_macros()
        .with_stub_functions()
        .with_global(
            "target",
            Value::from_serialize(BTreeMap::from([("schema", "jaffle_shop")])),
        )
        .with_global(
            "flags",
            Value::from_object(Flags::from_project_flags(project_flags)),
        );

    if override_default {
        builder = builder.with_macro(
            "test_project",
            "default__generate_schema_name",
            DEFAULT__GENERATE_SCHEMA_NAME,
        );
    }

    builder.build().expect("harness should build")
}

fn schema_name_ctx(catalog_name: Option<&'static str>) -> BTreeMap<String, Value> {
    let config = Arc::new(MockJinjaObject::new());
    config.on("get", move |args| {
        Ok(match args.first().and_then(|v| v.as_str()) {
            Some("catalog_name") => catalog_name.map(Value::from).unwrap_or(Value::UNDEFINED),
            _ => Value::UNDEFINED,
        })
    });

    let node = Arc::new(MockJinjaObject::new());
    node.set_attr("config", Value::from_dyn_object(config));

    BTreeMap::from([
        (
            "TARGET_PACKAGE_NAME".to_string(),
            Value::from("test_project"),
        ),
        ("node".to_string(), Value::from_dyn_object(node)),
    ])
}

fn with_lakehouse_catalog(harness: &MacroTestHarness, lakehouse_catalog: &'static str) {
    harness.mock().on("build_catalog_relation", move |_| {
        Ok(Value::from_serialize(BTreeMap::from([(
            "lakehouse_catalog",
            lakehouse_catalog,
        )])))
    });
}

#[test]
fn generate_schema_name_without_catalogs_v2_matches_default() {
    let harness = schema_name_harness(false, false);
    let bigquery = harness
        .render(BQ_GENERATE_SCHEMA_NAME, schema_name_ctx(None))
        .expect("render should succeed");
    let default = harness
        .render(
            "{{ default__generate_schema_name('staging', node) }}",
            schema_name_ctx(None),
        )
        .expect("render should succeed");

    assert_eq!(bigquery.trim(), default.trim());
    assert_eq!(bigquery.trim(), "jaffle_shop_staging");
}

#[test]
fn generate_schema_name_with_catalogs_v2_prefixes_lakehouse_catalog() {
    let harness = schema_name_harness(true, false);
    with_lakehouse_catalog(&harness, "sales_catalog");

    let rendered = harness
        .render(BQ_GENERATE_SCHEMA_NAME, schema_name_ctx(Some("BQ")))
        .expect("render should succeed");

    assert_eq!(rendered.trim(), "sales_catalog.jaffle_shop_staging");
}

#[test]
fn generate_schema_name_with_catalogs_v2_but_no_lrc_matches_default() {
    let harness = schema_name_harness(true, false);

    let rendered = harness
        .render(BQ_GENERATE_SCHEMA_NAME, schema_name_ctx(None))
        .expect("render should succeed");

    assert_eq!(rendered.trim(), "jaffle_shop_staging");
}

#[test]
fn generate_schema_name_composes_on_a_projects_own_default() {
    let off = schema_name_harness(false, true);
    assert_eq!(
        off.render(BQ_GENERATE_SCHEMA_NAME, schema_name_ctx(None))
            .expect("render should succeed")
            .trim(),
        "custom_staging"
    );

    let on = schema_name_harness(true, true);
    with_lakehouse_catalog(&on, "sales_catalog");
    assert_eq!(
        on.render(BQ_GENERATE_SCHEMA_NAME, schema_name_ctx(Some("BQ")))
            .expect("render should succeed")
            .trim(),
        "sales_catalog.custom_staging"
    );
}

fn render_governance_tags(
    relation_type: RelationType,
    column_dict: Value,
) -> dbt_common::FsResult<String> {
    let harness = build_harness();
    harness.mock().on("quote", |args| {
        Ok(Value::from(format!(
            "`{}`",
            args[0].as_str().expect("column name")
        )))
    });
    let relation = harness.relation("test-db", "test_schema", "customers", Some(relation_type));
    let ctx = BTreeMap::from([
        (
            "relation".to_string(),
            RelationObject::new(relation).into_value(),
        ),
        ("column_dict".to_string(), column_dict),
    ]);
    harness.render(
        "{{ bigquery__get_alter_column_data_governance_tags_sql(relation, column_dict) }}",
        ctx,
    )
}

fn column_dict(columns: BTreeMap<&str, Value>) -> Value {
    Value::from_serialize(columns)
}

fn governance_tag(key: &str, value: &str) -> Value {
    Value::from_serialize(BTreeMap::from([("key", key), ("value", value)]))
}

#[test]
fn data_governance_tags_alter_sets_one_tag() {
    let mut email = BTreeMap::new();
    email.insert(
        "data_governance_tags".to_string(),
        governance_tag("my-project/data_class", "email"),
    );
    let mut plain = BTreeMap::new();
    plain.insert("description".to_string(), Value::from("untagged column"));

    let rendered = render_governance_tags(
        RelationType::Table,
        column_dict(BTreeMap::from([
            ("email", Value::from_serialize(email)),
            ("name", Value::from_serialize(plain)),
        ])),
    )
    .expect("governance tag SQL should render");

    let sql = rendered.trim();
    assert!(
        sql.starts_with("alter table "),
        "expected one ALTER TABLE, got: {sql}"
    );
    assert!(
        sql.contains("alter column `email` set options (data_governance_tags=[(\"my-project/data_class\", \"email\")])"),
        "missing set clause, got: {sql}"
    );
    assert!(
        !sql.contains("`name`"),
        "a column without data_governance_tags must be left alone, got: {sql}"
    );
}

#[test]
fn data_governance_tags_alter_escapes_literals_and_rejects_nested_path() {
    let mut email = BTreeMap::new();
    email.insert("data_type".to_string(), Value::from("STRING"));
    email.insert(
        "data_governance_tags".to_string(),
        governance_tag("my-project/data_class", "say \"hi\""),
    );
    let rendered = render_governance_tags(
        RelationType::Table,
        column_dict(BTreeMap::from([("email", Value::from_serialize(email))])),
    )
    .expect("quoted value should render");
    let sql = rendered.trim();
    assert!(
        sql.contains("alter column `email` set options (data_governance_tags=[(\"my-project/data_class\", \"say \\\"hi\\\"\")])"),
        "unexpected escaped SQL: {sql}"
    );

    let mut nested = BTreeMap::new();
    nested.insert("data_type".to_string(), Value::from("STRING"));
    nested.insert(
        "data_governance_tags".to_string(),
        governance_tag("my-project/data_class", "email"),
    );
    let err = render_governance_tags(
        RelationType::Table,
        column_dict(BTreeMap::from([(
            "address.email",
            Value::from_serialize(nested),
        )])),
    )
    .expect_err("a dotted field path must fail");
    assert!(
        err.to_string().contains("does not accept a dotted path"),
        "unexpected error: {err}"
    );
}

#[test]
fn data_governance_tags_alter_rejects_struct_parent_and_bad_key() {
    let mut parent = BTreeMap::new();
    parent.insert(
        "data_type".to_string(),
        Value::from("ARRAY<STRUCT<email STRING>>"),
    );
    parent.insert(
        "data_governance_tags".to_string(),
        governance_tag("my-project/data_class", "email"),
    );
    let err = render_governance_tags(
        RelationType::Table,
        column_dict(BTreeMap::from([("address", Value::from_serialize(parent))])),
    )
    .expect_err("STRUCT parent must fail");
    assert!(
        err.to_string().contains("RECORD or STRUCT parent"),
        "unexpected error: {err}"
    );

    let mut bad_key = BTreeMap::new();
    bad_key.insert(
        "data_governance_tags".to_string(),
        governance_tag("data_class", "email"),
    );
    let err = render_governance_tags(
        RelationType::Table,
        column_dict(BTreeMap::from([("email", Value::from_serialize(bad_key))])),
    )
    .expect_err("a key without a namespace must fail");
    assert!(
        err.to_string().contains("{parent_id}/{short_name}"),
        "unexpected error: {err}"
    );

    let mut view_email = BTreeMap::new();
    view_email.insert(
        "data_governance_tags".to_string(),
        governance_tag("my-project/data_class", "email"),
    );
    let err = render_governance_tags(
        RelationType::View,
        column_dict(BTreeMap::from([(
            "email",
            Value::from_serialize(view_email),
        )])),
    )
    .expect_err("a view must fail");
    assert!(
        err.to_string().contains("only supported on tables"),
        "unexpected error: {err}"
    );
}
