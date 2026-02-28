use std::collections::BTreeMap;

use dbt_common::adapter::AdapterType;
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
}
