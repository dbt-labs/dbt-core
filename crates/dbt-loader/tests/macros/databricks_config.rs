use std::sync::Arc;

use dbt_adapter::relation::RelationObject;
use dbt_adapter_core::AdapterType;
use dbt_jinja_utils::mock_object::MockJinjaObject;
use dbt_schemas::dbt_types::RelationType;
use minijinja::Value;

use crate::macro_test_harness::{MacroTestHarness, default_mock_config};

fn assert_configuration_helper_passes_desired_config(relation_type: RelationType) {
    let harness = MacroTestHarness::for_adapter(AdapterType::Databricks)
        .load_all_macros()
        .with_stub_functions()
        .build()
        .expect("harness should build");

    let model_config = Arc::new(MockJinjaObject::new());
    model_config.set_attr("marker", Value::from("desired"));
    model_config.on("get_changeset", |args| {
        assert_eq!(args.len(), 1);
        assert_eq!(
            args[0].get_attr("marker").unwrap().as_str(),
            Some("existing")
        );
        Ok(Value::from("changes"))
    });
    let model_config_value = Value::from_dyn_object(Arc::clone(&model_config));
    harness.mock().on("get_config_from_model", move |_| {
        Ok(model_config_value.clone())
    });

    let existing_config = Arc::new(MockJinjaObject::new());
    existing_config.set_attr("marker", Value::from("existing"));
    let existing_config_value = Value::from_dyn_object(existing_config);
    harness.mock().on("get_relation_config", move |args| {
        assert_eq!(args.len(), 2);
        assert_eq!(
            args[1].get_attr("marker").unwrap().as_str(),
            Some("desired")
        );
        Ok(existing_config_value.clone())
    });

    let config = default_mock_config();
    config.set_attr("model", Value::from("model"));
    let existing = harness.relation(
        "TEST_DB",
        "TEST_SCHEMA",
        "configured_relation",
        Some(relation_type),
    );
    let ctx = harness
        .materialization_context("configured_relation", "select 1")
        .config(Value::from_dyn_object(config))
        .with("existing", RelationObject::new(existing).into_value())
        .build();

    let rendered = harness
        .render("{{ get_configuration_changes(existing) }}", ctx)
        .expect("configuration helper should render");
    assert_eq!(rendered, "changes");

    let calls = harness.mock().observed_calls();
    let model_index = calls
        .iter()
        .position(|call| call.method == "get_config_from_model")
        .unwrap();
    let relation_index = calls
        .iter()
        .position(|call| call.method == "get_relation_config")
        .unwrap();
    assert!(model_index < relation_index);
}

#[test]
fn view_configuration_helper_passes_desired_config() {
    assert_configuration_helper_passes_desired_config(RelationType::View);
}

#[test]
fn materialized_view_configuration_helper_passes_desired_config() {
    assert_configuration_helper_passes_desired_config(RelationType::MaterializedView);
}

#[test]
fn streaming_table_configuration_helper_passes_desired_config() {
    assert_configuration_helper_passes_desired_config(RelationType::StreamingTable);
}
