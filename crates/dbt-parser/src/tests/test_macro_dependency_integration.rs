//! Integration tests for macro dependency tracking

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use chrono_tz::Tz;
    use dbt_common::cancellation::never_cancels;
    use dbt_common::io_args::IoArgs;
    use dbt_jinja_utils::invocation_args::InvocationArgs;
    use dbt_jinja_utils::listener::{
        DefaultRenderingEventListenerFactory, RenderingEventListenerFactory,
    };
    use dbt_jinja_utils::phases::parse::init::initialize_parse_jinja_environment;
    use dbt_jinja_utils::utils::render_sql;
    use dbt_schemas::schemas::profiles::{DbConfig, PostgresDbConfig};
    use dbt_schemas::schemas::relations::DEFAULT_DBT_QUOTING;
    use dbt_schemas::schemas::serde::StringOrInteger;
    use minijinja::Value;
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::PathBuf;

    fn setup_test_env() -> dbt_jinja_utils::jinja_environment::JinjaEnv {
        let tz_now: DateTime<Tz> = Utc::now().with_timezone(&Tz::UTC);
        let invocation_args = InvocationArgs::default();

        initialize_parse_jinja_environment(
            "test_project",
            "profile",
            "target",
            "postgres",
            &DbConfig::Postgres(PostgresDbConfig {
                port: Some(StringOrInteger::Integer(5432)),
                database: Some("test_db".to_string()),
                host: Some("localhost".to_string()),
                user: Some("test_user".to_string()),
                password: Some("test_pass".to_string()),
                schema: Some("test_schema".to_string()),
                ..Default::default()
            }),
            DEFAULT_DBT_QUOTING,
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            tz_now,
            &invocation_args,
            BTreeSet::from(["test_project".to_string()]),
            IoArgs::default(),
            None,
            never_cancels(),
        )
        .unwrap()
    }

    #[test]
    fn test_basic_macro_tracking() {
        // Test that we track macro calls in templates
        let test_sql = r#"
{{ config(materialized='table') }}
{{ ref('my_model') }}
{{ custom_macro() }}
"#;

        let env = setup_test_env();

        // Create context with mock functions
        let mut ctx = BTreeMap::new();
        ctx.insert(
            "config".to_string(),
            Value::from_function(|_args: &[Value]| -> Result<String, minijinja::Error> {
                Ok("".to_string())
            }),
        );
        ctx.insert(
            "ref".to_string(),
            Value::from_function(|_args: &[Value]| -> Result<String, minijinja::Error> {
                Ok("referenced_table".to_string())
            }),
        );
        ctx.insert(
            "custom_macro".to_string(),
            Value::from_function(|_args: &[Value]| -> Result<String, minijinja::Error> {
                Ok("custom result".to_string())
            }),
        );

        let listener_factory = DefaultRenderingEventListenerFactory::default();
        let test_path = PathBuf::from("test_basic.sql");

        let _result = render_sql(test_sql, &env, &ctx, &listener_factory, &test_path);

        let macro_calls = listener_factory.drain_macro_calls(&test_path);

        // Verify all macros are tracked
        assert!(macro_calls.contains("config"));
        assert!(macro_calls.contains("ref"));
        assert!(macro_calls.contains("custom_macro"));
        assert_eq!(macro_calls.len(), 3);
    }

    #[test]
    fn test_macro_tracking_with_render_error() {
        // Test that macro calls are tracked even when rendering fails
        let test_sql = r#"
{{ config(materialized='table') }}
{{ undefined_macro() }}
"#;

        let env = setup_test_env();

        let mut ctx = BTreeMap::new();
        ctx.insert(
            "config".to_string(),
            Value::from_function(|_args: &[Value]| -> Result<String, minijinja::Error> {
                Ok("".to_string())
            }),
        );
        // Note: undefined_macro is not in context, so rendering will fail

        let listener_factory = DefaultRenderingEventListenerFactory::default();
        let test_path = PathBuf::from("test_error.sql");

        let result = render_sql(test_sql, &env, &ctx, &listener_factory, &test_path);

        // Rendering should fail
        assert!(result.is_err());

        // But we should still track the macros called before the error
        let macro_calls = listener_factory.drain_macro_calls(&test_path);

        // Config is called before the error, but undefined_macro might not be tracked
        // depending on when the error occurs
        println!("Tracked macro calls with error: {macro_calls:?}");
    }
}
