//! Tests for namespaced macro tracking

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use chrono_tz::Tz;
    use dbt_common::cancellation::never_cancels;
    use dbt_common::io_args::IoArgs;
    use dbt_jinja_utils::invocation_args::InvocationArgs;
    use dbt_jinja_utils::listener::{DefaultListenerFactory, ListenerFactory};
    use dbt_jinja_utils::phases::parse::init::initialize_parse_jinja_environment;
    use dbt_jinja_utils::utils::render_sql;
    use dbt_schemas::schemas::profiles::{DbConfig, PostgresDbConfig};
    use dbt_schemas::schemas::relations::DEFAULT_DBT_QUOTING;
    use dbt_schemas::schemas::serde::StringOrInteger;
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::PathBuf;
    use std::sync::Arc;

    #[test]
    fn test_namespaced_macro_tracking() {
        // Test that namespaced macro calls are tracked (without namespace prefix in current implementation)
        let test_sql = r#"
{{ dbt_utils.get_url_host('https://example.com') }}
{{ dbt.replace('hello', 'h', 'H') }}
"#;

        let tz_now: DateTime<Tz> = Utc::now().with_timezone(&Tz::UTC);
        let invocation_args = InvocationArgs::default();

        let mut env = initialize_parse_jinja_environment(
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
        .unwrap();

        // Add templates for namespaced macros
        env.env
            .add_template_owned(
                "dbt_utils.get_url_host",
                r#"{% macro get_url_host(url) %}host{% endmacro %}"#,
                None,
                &[],
            )
            .unwrap();

        env.env
            .add_template_owned(
                "dbt.replace",
                r#"{% macro replace(text, old, new) %}{{ text | replace(old, new) }}{% endmacro %}"#,
                None,
                &[],
            )
            .unwrap();

        let listener_factory = DefaultListenerFactory::default();
        let test_path = PathBuf::from("test_namespaced.sql");
        let env = Arc::new(env);

        let _result = render_sql(
            test_sql,
            &env,
            &BTreeMap::new(),
            &listener_factory,
            &test_path,
        );

        let macro_calls = listener_factory.drain_macro_calls(&test_path);

        // In current implementation, namespaced calls are tracked without namespace prefix
        assert!(macro_calls.contains("get_url_host"));
        assert!(macro_calls.contains("replace"));
        assert_eq!(macro_calls.len(), 2);
    }
}
