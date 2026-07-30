use super::{
    build_workflow_spec, extract_notebook_scoped_libraries, extract_packages,
    prepare_code_with_notebook_scoped_packages,
};
use minijinja::Value;
use serde_json::json;

#[test]
fn packages_require_a_list_of_strings_and_preserve_order_and_duplicates() {
    let valid = Value::from_serialize(json!({
        "packages": ["pandas", "numpy", "pandas"],
    }));
    assert_eq!(
        extract_packages(&valid).unwrap(),
        vec!["pandas", "numpy", "pandas"]
    );

    let scalar = Value::from_serialize(json!({"packages": "pandas"}));
    assert!(extract_packages(&scalar).is_err());

    let non_string = Value::from_serialize(json!({"packages": ["pandas", 1]}));
    assert!(extract_packages(&non_string).is_err());
}

#[test]
fn notebook_scoped_libraries_matches_pydantic_boolean_coercion() {
    for value in [json!(true), json!(1), json!("yes"), json!("ON")] {
        let config = Value::from_serialize(json!({"notebook_scoped_libraries": value}));
        assert!(extract_notebook_scoped_libraries(&config).unwrap());
    }
    for value in [json!(false), json!(0), json!("no"), json!("off")] {
        let config = Value::from_serialize(json!({"notebook_scoped_libraries": value}));
        assert!(!extract_notebook_scoped_libraries(&config).unwrap());
    }
    for value in [json!("sometimes"), json!(2), json!([]), json!({})] {
        let config = Value::from_serialize(json!({"notebook_scoped_libraries": value}));
        assert!(extract_notebook_scoped_libraries(&config).is_err());
    }
}

#[test]
fn empty_index_url_is_omitted_from_notebook_install() {
    let actual = prepare_code_with_notebook_scoped_packages(
        "compiled_code",
        &["pandas".to_string()],
        true,
        Some(""),
    );

    assert!(actual.starts_with("%pip install -q pandas"));
    assert!(!actual.contains("--index-url"));
}

#[test]
fn workflow_spec_excludes_adapter_control_fields_but_keeps_job_settings() {
    let python_job_config = Value::from_serialize(json!({
        "name": "configured-name",
        "existing_job_id": "42",
        "grants": {"view": [{"user_name": "user@example.com"}]},
        "post_hook_tasks": [{"task_key": "post-hook"}],
        "additional_task_settings": {"timeout_seconds": 60},
        "max_concurrent_runs": 2,
    }));

    let (workflow_spec, existing_job_id) = build_workflow_spec(
        &python_job_config,
        "catalog",
        "schema",
        "model",
        "/Workspace/model",
        json!({}),
    )
    .unwrap();

    assert_eq!(existing_job_id, Some(42));
    assert_eq!(workflow_spec["name"], "configured-name");
    assert_eq!(workflow_spec["max_concurrent_runs"], 2);
    assert_eq!(workflow_spec["tasks"].as_array().unwrap().len(), 2);
    for control_field in [
        "existing_job_id",
        "grants",
        "post_hook_tasks",
        "additional_task_settings",
    ] {
        assert!(workflow_spec.get(control_field).is_none());
    }
    assert_eq!(workflow_spec["tasks"][0]["timeout_seconds"], 60);
}
