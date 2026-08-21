use dbt_schemas::schemas::project::{ProjectModelConfig, TypedRecursiveConfig};

#[test]
fn notebook_scoped_libraries_config_is_preserved() {
    let config: ProjectModelConfig = dbt_yaml::from_str(
        r#"
+notebook_scoped_libraries: true
__additional_properties__: {}
"#,
    )
    .unwrap();

    assert!(config.has_set_fields());

    let serialized = serde_json::to_value(config).unwrap();
    assert_eq!(
        serialized.get("+notebook_scoped_libraries"),
        Some(&serde_json::Value::Bool(true))
    );
}
