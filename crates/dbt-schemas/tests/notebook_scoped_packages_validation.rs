use dbt_schemas::schemas::project::ProjectModelConfig;

fn parse(value: &str) -> Result<ProjectModelConfig, dbt_yaml::Error> {
    dbt_yaml::from_str(&format!(
        "+notebook_scoped_libraries: {value}\n__additional_properties__: {{}}\n"
    ))
}

#[test]
fn notebook_scoped_libraries_matches_pydantic_boolean_coercion() {
    for value in ["true", "1", "1.0", "yes", "ON", "'t'"] {
        assert_eq!(parse(value).unwrap().notebook_scoped_libraries, Some(true));
    }
    for value in ["false", "0", "0.0", "no", "off", "'F'"] {
        assert_eq!(parse(value).unwrap().notebook_scoped_libraries, Some(false));
    }
}

#[test]
fn notebook_scoped_libraries_rejects_invalid_values() {
    for value in ["sometimes", "2", "0.5", "null", "[]", "{}"] {
        assert!(parse(value).is_err(), "{value} should be rejected");
    }
}
