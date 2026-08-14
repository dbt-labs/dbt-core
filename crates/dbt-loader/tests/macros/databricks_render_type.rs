use dbt_adapter_core::AdapterType;

use crate::macro_test_harness::MacroTestHarness;

#[test]
fn databricks_render_type_uses_valid_alter_keywords() {
    let harness = MacroTestHarness::for_adapter(AdapterType::Databricks)
        .load_all_macros()
        .with_stub_functions()
        .build()
        .expect("harness should build");

    for (relation_type, expected) in [
        ("table", "TABLE"),
        ("view", "VIEW"),
        ("materialized_view", "MATERIALIZED VIEW"),
        ("streaming_table", "STREAMING TABLE"),
        ("metric_view", "VIEW"),
    ] {
        let rendered = harness
            .render(
                &format!("{{{{ render_type('{relation_type}') }}}}"),
                minijinja::context! {},
            )
            .expect("render_type should render");
        assert_eq!(rendered.trim(), expected);
    }
}
