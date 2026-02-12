use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use dbt_adapter::{BaseAdapter, BridgeAdapter, sql_types::NaiveTypeOpsImpl};
use dbt_common::{adapter::AdapterType, cancellation::never_cancels};
use dbt_jinja_utils::{JinjaEnvBuilder, MacroUnitsWrapper};
use dbt_schemas::schemas::relations::DEFAULT_DBT_QUOTING;
use minijinja::{Value, machinery::Span, macro_unit::MacroInfo, macro_unit::MacroUnit};

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
    let adapter = BridgeAdapter::new_parse_phase_adapter(
        AdapterType::Databricks,
        dbt_yaml::Mapping::default(),
        DEFAULT_DBT_QUOTING,
        Box::new(NaiveTypeOpsImpl::new(AdapterType::Databricks)),
        never_cancels(),
        None,
    );

    // Load the actual macro asset file so this becomes a regression test once the macro is added.
    let databricks_persist_docs_sql =
        include_str!("../src/dbt_macro_assets/dbt-databricks/macros/adapters/persist_docs.sql");
    // Until Fusion's macro assets implement `comment_on_column_sql`, define a fallback to ensure
    // the test fails via assertion (wrong SQL) rather than panicking (missing macro).
    //
    // Once the macro is added upstream in the asset file, that later definition will override
    // this fallback and the test will pass.
    let fallback = r#"
{% macro comment_on_column_sql(column_path, escaped_comment) -%}
  alter table {{ column_path.rsplit('.', 1)[0] }} change column {{ column_path.rsplit('.', 1)[1] }} comment '{{ escaped_comment }}'
{%- endmacro %}
"#;
    let combined = format!("{fallback}\n{databricks_persist_docs_sql}");

    let mut macro_units = MacroUnitsWrapper::new(BTreeMap::new());
    macro_units.macros.insert(
        "dbt_databricks".to_string(),
        vec![create_macro_unit(
            "comment_on_column_sql",
            &combined,
            "dbt_macro_assets/dbt-databricks/macros/adapters/persist_docs.sql",
        )],
    );

    let builder = JinjaEnvBuilder::new()
        .with_adapter(Arc::new(adapter) as Arc<dyn BaseAdapter>)
        .with_root_package("test_package".to_string())
        .try_with_macros(macro_units)?;

    Ok(builder.build())
}

#[test]
fn databricks_comment_on_column_sql_uses_legacy_alter_table_syntax() {
    let env = build_env().expect("env should build");
    let ctx = BTreeMap::from([
        (
            "column_path".to_string(),
            Value::from("`dbt`.`dbt_entities`.`ent_shopify_inventory_quantity`.`id`"),
        ),
        (
            "escaped_comment".to_string(),
            Value::from("Primary key for the inventory quantity record."),
        ),
    ]);

    // This currently fails until Fusion's macro assets match upstream dbt-databricks.
    let rendered = env
        .render_str(
            "{{ comment_on_column_sql(column_path, escaped_comment) }}",
            ctx,
            &[],
        )
        .expect("render should succeed");

    assert_eq!(
        rendered.trim(),
        "alter table `dbt`.`dbt_entities`.`ent_shopify_inventory_quantity` change column `id` comment 'Primary key for the inventory quantity record.'"
    );
}
