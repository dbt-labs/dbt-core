use std::collections::BTreeMap;

use dbt_common::adapter::AdapterType;
use minijinja::Value;

use crate::macro_test_harness::MacroTestHarness;

mod databricks {
    use super::*;

    fn build_comment_on_column_harness() -> MacroTestHarness {
        let databricks_persist_docs_sql = include_str!(
            "../../src/dbt_macro_assets/dbt-databricks/macros/adapters/persist_docs.sql"
        );

        // Until Fusion's macro assets implement `comment_on_column_sql`, define a fallback to ensure
        // the test fails via assertion (wrong SQL) rather than panicking (missing macro).
        let fallback = r#"
{% macro comment_on_column_sql(column_path, escaped_comment) -%}
  alter table {{ column_path.rsplit('.', 1)[0] }} change column {{ column_path.rsplit('.', 1)[1] }} comment '{{ escaped_comment }}'
{%- endmacro %}
"#;
        let combined = format!("{fallback}\n{databricks_persist_docs_sql}");

        let harness = MacroTestHarness::for_adapter(AdapterType::Databricks)
            .with_root_package("test_package".to_string())
            .with_macro_at_path(
                "dbt_databricks",
                "comment_on_column_sql",
                &combined,
                "dbt_macro_assets/dbt-databricks/macros/adapters/persist_docs.sql",
            )
            .build()
            .expect("harness should build");

        harness
            .mock()
            .on("has_dbr_capability", |_| Ok(Value::from(false)));

        harness
    }

    #[test]
    fn comment_on_column_sql_uses_legacy_alter_table_syntax() {
        let harness = build_comment_on_column_harness();
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

        let rendered = harness
            .render(
                "{{ comment_on_column_sql(column_path, escaped_comment) }}",
                ctx,
            )
            .expect("render should succeed");

        assert_eq!(
            rendered.trim(),
            "ALTER TABLE `dbt`.`dbt_entities`.`ent_shopify_inventory_quantity` ALTER COLUMN `id` COMMENT 'Primary key for the inventory quantity record.'"
        );
    }
}
