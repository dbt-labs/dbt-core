use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use dbt_adapter::{BaseAdapter, BridgeAdapter, sql_types::NaiveTypeOpsImpl};
use dbt_common::{adapter::AdapterType, cancellation::never_cancels};
use dbt_jinja_utils::{JinjaEnvBuilder, MacroUnitsWrapper};
use dbt_schemas::schemas::relations::DEFAULT_DBT_QUOTING;
use minijinja::{
    Value, dispatch_object::THREAD_LOCAL_DEPENDENCIES, machinery::Span, macro_unit::MacroInfo,
    macro_unit::MacroUnit,
};

// THREAD_LOCAL_DEPENDENCIES is global and tests run in parallel by default. Serialize mutations to
// avoid cross-test flakes.
static TEST_DEPS_LOCK: Mutex<()> = Mutex::new(());

fn set_thread_local_dependencies(pkgs: impl IntoIterator<Item = &'static str>) {
    let _guard = TEST_DEPS_LOCK.lock().unwrap();
    let deps = THREAD_LOCAL_DEPENDENCIES.get_or_init(|| Mutex::new(BTreeSet::new()));
    let mut deps = deps.lock().unwrap();
    deps.clear();
    deps.extend(pkgs.into_iter().map(str::to_string));
}

fn create_macro_unit(name: &str, sql: &str, path: &str) -> MacroUnit {
    MacroUnit {
        info: MacroInfo {
            name: name.to_string(),
            path: PathBuf::from(path),
            span: Span {
                start_line: 0,
                start_col: 0,
                start_offset: 0,
                end_line: 0,
                end_col: 0,
                end_offset: 0,
            },
            funcsign: None,
            args: vec![],
            unique_id: "test".to_string(),
            name_span: Span::default(),
        },
        sql: sql.to_string(),
    }
}

fn build_env() -> dbt_common::FsResult<dbt_jinja_utils::jinja_environment::JinjaEnv> {
    // Ensure adapter.dispatch(..., macro_namespace="dbt") behaves like production: `dbt` is always
    // a dependency namespace, so namespaced dispatch includes it in the search packages.
    set_thread_local_dependencies(["dbt"]);

    let adapter = BridgeAdapter::new_parse_phase_adapter(
        AdapterType::Databricks,
        dbt_serde_yaml::Mapping::default(),
        DEFAULT_DBT_QUOTING,
        Box::new(NaiveTypeOpsImpl::new(AdapterType::Databricks)),
        never_cancels(),
        None,
    );

    // Use the real macro-asset file content so this test is a true regression check.
    let databricks_comment_sql =
        include_str!("../src/dbt_macro_assets/dbt-databricks/macros/relations/comment.sql");

    // `comment_clause()` is defined in dbt-spark in production, but for this regression we only
    // need the dispatch wrapper that calls the Databricks override.
    let dispatching_comment_clause = r#"
{% macro comment_clause() -%}
  {{ adapter.dispatch('comment_clause', 'dbt')() }}
{%- endmacro %}
"#;

    let mut macro_units = MacroUnitsWrapper::new(BTreeMap::new());
    macro_units.macros.insert(
        "dbt".to_string(),
        vec![create_macro_unit(
            "comment_clause",
            dispatching_comment_clause,
            "inline://dbt/comment_clause.sql",
        )],
    );
    macro_units.macros.insert(
        "dbt_databricks".to_string(),
        vec![create_macro_unit(
            "databricks__comment_clause",
            databricks_comment_sql,
            "dbt_macro_assets/dbt-databricks/macros/relations/comment.sql",
        )],
    );

    let builder = JinjaEnvBuilder::new()
        .with_adapter(Arc::new(adapter) as Arc<dyn BaseAdapter>)
        .with_root_package("test_package".to_string())
        .try_with_macros(macro_units)?;

    Ok(builder.build())
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
fn databricks_comment_clause_does_not_render_empty_comment() {
    let env = build_env().expect("env should build");
    let rendered = env
        .render_str("{{ comment_clause() }}", ctx_for(""), &[])
        .expect("render should succeed");

    assert_eq!(rendered.trim(), "");
    assert!(
        !rendered.contains("comment ''"),
        "Should never render an empty comment clause, got: {rendered:?}"
    );
}

#[test]
fn databricks_comment_clause_renders_non_empty_comment() {
    let env = build_env().expect("env should build");
    let rendered = env
        .render_str("{{ comment_clause() }}", ctx_for("hello"), &[])
        .expect("render should succeed");

    assert!(
        rendered.contains("comment 'hello'"),
        "Expected non-empty comment clause, got: {rendered:?}"
    );
}
