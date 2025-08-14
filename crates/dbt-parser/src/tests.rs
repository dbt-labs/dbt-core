//! Render tests for the dbt-parser crate

#[cfg(test)]
mod test_macro_dependency_integration;

#[cfg(test)]
mod test_namespaced_macro_tracking;

#[cfg(test)]
#[allow(clippy::module_inception)]
mod tests {
    use dbt_common::cancellation::never_cancels;
    use dbt_common::{FsResult, io_args::IoArgs};
    use dbt_frontend_common::error::CodeLocation;
    use dbt_fusion_adapter::parse::adapter::create_parse_adapter;
    use dbt_jinja_utils::invocation_args::InvocationArgs;
    use dbt_jinja_utils::jinja_environment::JinjaEnv;
    use dbt_jinja_utils::listener::DefaultRenderingEventListenerFactory;
    use dbt_jinja_utils::phases::parse::build_resolve_model_context;
    use dbt_jinja_utils::phases::parse::init::initialize_parse_jinja_environment;
    use dbt_jinja_utils::phases::parse::sql_resource::SqlResource;
    use dbt_jinja_utils::utils::render_sql;
    use dbt_schemas::schemas::profiles::{DbConfig, PostgresDbConfig};
    use dbt_schemas::schemas::project::{DefaultTo, ModelConfig};
    use dbt_schemas::schemas::relations::DEFAULT_DBT_QUOTING;
    use dbt_schemas::schemas::serde::StringOrInteger;
    use dbt_schemas::state::DbtRuntimeConfig;
    use minijinja::constants::TARGET_PACKAGE_NAME;
    use minijinja::machinery::Span;
    use minijinja::{AutoEscape, Error};
    use minijinja::{Environment, Value};

    use crate::utils::{get_node_fqn, parse_macro_statements};

    use chrono::{DateTime, Utc};
    use chrono_tz::Tz;
    use std::collections::BTreeSet;
    use std::path::Path;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};
    use std::{collections::BTreeMap, path::PathBuf};

    fn create_resolve_model_context<T: DefaultTo<T> + 'static>(
        init_config: &T,
        sql_resources: &Arc<Mutex<Vec<SqlResource<T>>>>,
    ) -> BTreeMap<String, Value> {
        let mut context = build_resolve_model_context(
            init_config,
            "postgres",
            "db",
            "schema",
            "my_model",
            get_node_fqn(
                "common",
                PathBuf::from("test"),
                vec!["my_model".to_string()],
            ),
            "common",
            "test",
            DEFAULT_DBT_QUOTING,
            Arc::new(DbtRuntimeConfig::default()),
            sql_resources.clone(),
            Arc::new(AtomicBool::new(false)),
            &PathBuf::from("test"),
            &IoArgs::default(),
        );
        context.insert(TARGET_PACKAGE_NAME.to_string(), Value::from("common"));
        context
    }

    fn setup_test_env() -> (
        JinjaEnv,
        Arc<Mutex<Vec<SqlResource<ModelConfig>>>>,
        ModelConfig,
    ) {
        let init_config = ModelConfig {
            alias: Some("alias".to_string()),
            ..Default::default()
        };
        let invocation_args = InvocationArgs {
            ..Default::default()
        };
        let tz_now: DateTime<Tz> = Utc::now().with_timezone(&Tz::UTC);

        let env = initialize_parse_jinja_environment(
            "common",
            "profile",
            "target",
            "postgres",
            &DbConfig::Postgres(PostgresDbConfig {
                port: Some(StringOrInteger::Integer(5432)),
                database: Some("postgres".to_string()),
                host: Some("localhost".to_string()),
                user: Some("postgres".to_string()),
                password: Some("postgres".to_string()),
                schema: Some("schema".to_string()),
                ..Default::default()
            }),
            DEFAULT_DBT_QUOTING,
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            tz_now,
            &invocation_args,
            BTreeSet::from(["common".to_string()]),
            IoArgs::default(),
            None,
            never_cancels(),
        )
        .unwrap();

        let sql_resources = Arc::new(Mutex::new(Vec::new()));

        (env, sql_resources, init_config)
    }

    #[tokio::test]
    async fn test_render_sql_with_ref_macro() {
        let (env, sql_resources, init_config) = setup_test_env();
        // Set the package name for the current context
        {
            let resolve_model_context = create_resolve_model_context(&init_config, &sql_resources);
            let sql = "SELECT * FROM {{ ref('my_table') }};";

            let rendered = render_sql(
                sql,
                &env,
                &resolve_model_context,
                &DefaultRenderingEventListenerFactory::default(),
                &PathBuf::from("test"),
            )
            .unwrap();

            let sql_resources_locked = sql_resources.lock().unwrap().clone();

            assert_eq!(
                rendered.trim(),
                "SELECT * FROM \"db\".\"schema\".\"my_table\";"
            );
            assert_eq!(
                sql_resources_locked,
                vec![
                    SqlResource::Config(Box::new(init_config)),
                    SqlResource::Ref((
                        "my_table".to_string(),
                        None,
                        None,
                        CodeLocation::new(1, 15, 14)
                    ))
                ]
            );
        }
    }

    #[tokio::test]
    async fn test_render_sql_with_source_macro() {
        let (env, sql_resources, init_config) = setup_test_env();
        // Set the package name for the current context
        {
            let resolve_model_scope = create_resolve_model_context(&init_config, &sql_resources);
            let sql = "SELECT * FROM {{ source('my_schema', 'my_table') }};";

            let rendered = render_sql(
                sql,
                &env,
                &resolve_model_scope,
                &DefaultRenderingEventListenerFactory::default(),
                &PathBuf::from("test"),
            )
            .unwrap();

            let sql_resources_locked = sql_resources.lock().unwrap().clone();

            assert_eq!(
                rendered.trim(),
                "SELECT * FROM \"db\".\"schema\".\"my_table\";"
            );
            assert_eq!(
                sql_resources_locked,
                vec![
                    SqlResource::Config(Box::new(init_config)),
                    SqlResource::Source((
                        "my_schema".to_string(),
                        "my_table".to_string(),
                        CodeLocation::new(1, 15, 14)
                    ))
                ]
            );
        }
    }

    #[tokio::test]
    async fn test_render_sql_with_metric_macro() {
        let (env, sql_resources, init_config) = setup_test_env();
        // Set the package name for the current context
        {
            let resolve_model_scope = create_resolve_model_context(&init_config, &sql_resources);
            let sql = "{{ metric('metric') }} {{ metric('metric_package', 'metric_two') }}";

            let rendered = render_sql(
                sql,
                &env,
                &resolve_model_scope,
                &DefaultRenderingEventListenerFactory::default(),
                &PathBuf::from("test"),
            )
            .unwrap();

            let sql_resources_locked = sql_resources.lock().unwrap().clone();

            assert_eq!(rendered.trim(), "metric metric_two");
            assert_eq!(
                sql_resources_locked,
                vec![
                    SqlResource::Config(Box::new(init_config)),
                    SqlResource::Metric(("metric".to_string(), None)),
                    SqlResource::Metric((
                        "metric_two".to_string(),
                        Some("metric_package".to_string())
                    )),
                ]
            );
        }
    }

    #[tokio::test]
    async fn test_render_sql_with_config_macro() {
        let (env, sql_resources, init_config) = setup_test_env();
        // Set the package name for the current context
        {
            let resolve_model_scope = create_resolve_model_context(&init_config, &sql_resources);
            let sql = r#"
        {{
            config(
                schema = 'my_schema',
                alias = 'my_alias'~'suffix',
                materialized = 'view'
            )
        }}
        "#;
            let rendered = render_sql(
                sql,
                &env,
                &resolve_model_scope,
                &DefaultRenderingEventListenerFactory::default(),
                &PathBuf::from("test"),
            )
            .unwrap();

            assert_eq!(rendered.trim(), "");

            let expected_config = {
                let mut map = BTreeMap::new();
                map.insert("schema".to_string(), Value::from("my_schema"));
                map.insert("alias".to_string(), Value::from("my_aliassuffix"));
                map.insert("materialized".to_string(), Value::from("view"));
                map.insert("enabled".to_string(), Value::from(true)); // this gets inhertied from the global config which is true if not specified (important that this is not overridden)
                let config: ModelConfig =
                    dbt_serde_yaml::from_value(dbt_serde_yaml::to_value(map).unwrap()).unwrap();
                SqlResource::Config(Box::new(config))
            };

            let sql_resources_locked = sql_resources.lock().unwrap().clone();
            assert_eq!(
                sql_resources_locked,
                vec![SqlResource::Config(Box::new(init_config)), expected_config]
            );
        }
    }

    #[test]
    #[ignore = "This test does not work due to dispatch not getting context of macros defined below"]
    fn test_adapter_dispatch() {
        #[allow(unused_imports)] // required to compile code with various feature flags
        use minijinja::compiler::parser::Parser;
        #[allow(unused_imports)] // required to compile code with various feature flags
        use minijinja::machinery::WhitespaceConfig;
        #[allow(unused_imports)] // required to compile code with various feature flags
        use minijinja::machinery::{CodeGenerator, Instructions, Vm};
        #[allow(unused_imports)] // required to compile code with various feature flags
        use minijinja::syntax::SyntaxConfig;
        #[allow(dead_code)]
        fn simple_eval<S: serde::Serialize>(
            instructions: &Instructions<'_>,
            ctx: S,
        ) -> Result<String, Error> {
            let mut env = Environment::new();
            let adapter =
                create_parse_adapter("postgres", DEFAULT_DBT_QUOTING, never_cancels()).unwrap();
            env.add_global("adapter", adapter.as_value());
            let empty_blocks = BTreeMap::new();
            let vm = Vm::new(&env);
            let root = Value::from_serialize(&ctx);

            Ok(vm
                .eval(instructions, root, &empty_blocks, AutoEscape::None, &[])?
                .0
                .as_str()
                .unwrap()
                .to_string())
        }
        panic!("test code disabled below");
    }

    #[tokio::test]
    async fn test_fromjson() {
        let (env, _, _) = setup_test_env();
        let env = Arc::new(env);
        let sql = r#"
        {% set json_str = '{"abc": 123}' %}
        {% set parsed = fromjson(json_str) %}
        {{ parsed['abc'] }}
        "#;

        let rendered = render_sql(
            sql,
            &env,
            &BTreeMap::new(),
            &DefaultRenderingEventListenerFactory::default(),
            &PathBuf::from("test"),
        )
        .unwrap();

        assert_eq!(rendered.trim(), "123");
    }

    #[tokio::test]
    async fn test_tojson() {
        let (env, _, _) = setup_test_env();
        let env = Arc::new(env);
        let sql = r#"
        {% set my_dict = {"abc": 123, "def": 456} %}
        {% set json_str = tojson(my_dict) %}
        {{ json_str }}
        "#;

        let rendered = render_sql(
            sql,
            &env,
            &BTreeMap::new(),
            &DefaultRenderingEventListenerFactory::default(),
            &PathBuf::from("test"),
        )
        .unwrap();

        let rendered = rendered.trim().replace(" ", "").replace("\n", "");
        assert_eq!(rendered, r#"{"abc":123,"def":456}"#);
    }

    #[tokio::test]
    async fn test_tojson_with_sort_keys() {
        let (env, _, _) = setup_test_env();
        let env = Arc::new(env);
        let sql = r#"
        {% set my_dict = {"def": 456, "abc": 123} %}
        {% set json_str = tojson(my_dict, sort_keys=true) %}
        {{ json_str }}
        "#;

        let rendered = render_sql(
            sql,
            &env,
            &BTreeMap::new(),
            &DefaultRenderingEventListenerFactory::default(),
            &PathBuf::from("test"),
        )
        .unwrap();

        let rendered = rendered.trim().replace(" ", "").replace("\n", "");
        assert_eq!(rendered, r#"{"abc":123,"def":456}"#);
    }

    #[tokio::test]
    async fn test_tojson_with_default() {
        let (env, _, _) = setup_test_env();
        let env = Arc::new(env);
        let sql = r#"
        {% set invalid_json = undefined %}
        {% set json_str = tojson(invalid_json, '{"default": true}') %}
        {{ json_str }}
        "#;

        let rendered = render_sql(
            sql,
            &env,
            &BTreeMap::new(),
            &DefaultRenderingEventListenerFactory::default(),
            &PathBuf::from("test"),
        )
        .unwrap();

        assert_eq!(rendered.trim(), r#"{"default": true}"#);
    }

    #[tokio::test]
    async fn test_fromyaml() {
        let (env, _, _) = setup_test_env();
        let env = Arc::new(env);
        let sql = r#"
        {% set my_yml_str -%}
        dogs:
         - good
         - bad
        {%- endset %}
        {% set my_dict = fromyaml(my_yml_str) %}
        {{ my_dict['dogs'] | join(", ") }}
        "#;

        let rendered = render_sql(
            sql,
            &env,
            &BTreeMap::new(),
            &DefaultRenderingEventListenerFactory::default(),
            &PathBuf::from("test"),
        )
        .unwrap();

        assert_eq!(rendered.trim(), "good, bad");
    }

    #[tokio::test]
    async fn test_toyaml_basic() {
        let (env, _, _) = setup_test_env();
        let env = Arc::new(env);
        let sql = r#"
        {% set my_dict = {"abc": 123, "def": 456} %}
        {% set yaml_str = toyaml(my_dict) %}
        {{ yaml_str }}
        "#;

        // Render the snippet
        let rendered = render_sql(
            sql,
            &env,
            &BTreeMap::new(),
            &DefaultRenderingEventListenerFactory::default(),
            &PathBuf::from("test"),
        )
        .unwrap();

        let trimmed = rendered.trim().replace('\n', " ").replace('\r', "");
        assert!(trimmed.contains("abc: 123"));
        assert!(trimmed.contains("def: 456"));
    }

    #[tokio::test]
    async fn test_set_strict_function() {
        let (env, _, _) = setup_test_env();
        let env = Arc::new(env);
        let sql = r#"
        {% set my_list = [1, 2, 2, 3] %}
        {% set my_set = set_strict(my_list) %}
        {{ my_set | join(", ") }}
        "#;

        let rendered = render_sql(
            sql,
            &env,
            &BTreeMap::new(),
            &DefaultRenderingEventListenerFactory::default(),
            &PathBuf::from("test"),
        )
        .unwrap();

        let trimmed = rendered.trim();
        assert!(
            trimmed == "1, 2, 3"
                || trimmed == "1, 3, 2"
                || trimmed == "2, 1, 3"
                || trimmed == "2, 3, 1"
                || trimmed == "3, 1, 2"
                || trimmed == "3, 2, 1"
        );

        // Test error case with non-iterable
        let sql_error = r#"
        {% set my_set = set_strict(42) %}
        {{ my_set }}
        "#;

        let result = render_sql(
            sql_error,
            &env,
            &BTreeMap::new(),
            &DefaultRenderingEventListenerFactory::default(),
            &PathBuf::from("test"),
        );

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_local_md5() {
        let (env, _, _) = setup_test_env();
        let env = Arc::new(env);
        let sql = r#"
        {% set value = "hello world" %}
        {{ local_md5(value) }}
        "#;

        let rendered = render_sql(
            sql,
            &env,
            &BTreeMap::new(),
            &DefaultRenderingEventListenerFactory::default(),
            &PathBuf::from("test"),
        )
        .unwrap();

        assert_eq!(rendered.trim(), "5eb63bbbe01eeed093cb22bb8f5acdc3");
    }

    #[test]
    fn test_parse_regular_macro() -> FsResult<()> {
        let sql = r#"
            {% macro my_macro() %}
                select 1 as col
            {% endmacro %}
        "#;

        let resources = parse_macro_statements(sql, &PathBuf::from("test.sql"), &["macro"])?;
        assert_eq!(
            resources,
            vec![SqlResource::Macro(
                "my_macro".to_string(),
                Span {
                    start_line: 2,
                    start_col: 13,
                    start_offset: 13,
                    end_line: 4,
                    end_col: 27,
                    end_offset: 94
                },
                None,
                vec![]
            )]
        );
        Ok(())
    }

    #[test]
    fn test_parse_test_macro() -> FsResult<()> {
        let sql = r#"
            {% test positive_value(model, column_name) %}
                select *
                from {{ model }}
                where {{ column_name }} < 0
            {% endtest %}
        "#;

        let resources = parse_macro_statements(sql, &PathBuf::from("test.sql"), &["test"])?;
        assert_eq!(
            resources,
            vec![SqlResource::Test(
                "test_positive_value".to_string(),
                Span {
                    start_line: 2,
                    start_col: 13,
                    start_offset: 13,
                    end_line: 6,
                    end_col: 26,
                    end_offset: 186
                }
            )]
        );
        Ok(())
    }

    #[test]
    fn test_parse_multiple_macros() -> FsResult<()> {
        let sql = r#"
            {% macro first() %}
                select 1
            {% endmacro %}

            {% test second(model) %}
                select * from {{ model }}
            {% endtest %}

            {% macro third() %}
                select 3
            {% endmacro %}
        "#;

        let resources =
            parse_macro_statements(sql, &PathBuf::from("test.sql"), &["macro", "test"])?;
        assert_eq!(
            resources,
            vec![
                SqlResource::Macro(
                    "first".to_string(),
                    Span {
                        start_line: 2,
                        start_col: 13,
                        start_offset: 13,
                        end_line: 4,
                        end_col: 27,
                        end_offset: 84
                    },
                    None,
                    vec![]
                ),
                SqlResource::Test(
                    "test_second".to_string(),
                    Span {
                        start_line: 6,
                        start_col: 13,
                        start_offset: 98,
                        end_line: 8,
                        end_col: 26,
                        end_offset: 190
                    }
                ),
                SqlResource::Macro(
                    "third".to_string(),
                    Span {
                        start_line: 10,
                        start_col: 13,
                        start_offset: 204,
                        end_line: 12,
                        end_col: 27,
                        end_offset: 275
                    },
                    None,
                    vec![]
                ),
            ]
        );
        Ok(())
    }

    #[test]
    fn test_parse_nested_macros() -> FsResult<()> {
        let sql = r#"
            {% macro outer() %}
                {% macro inner() %}
                    select 1
                {% endmacro %}
            {% endmacro %}
        "#;

        let resources = parse_macro_statements(sql, &PathBuf::from("test.sql"), &["macro"])?;
        assert_eq!(
            resources,
            vec![
                SqlResource::Macro(
                    "outer".to_string(),
                    Span {
                        start_line: 2,
                        start_col: 13,
                        start_offset: 13,
                        end_line: 6,
                        end_col: 27,
                        end_offset: 155
                    },
                    None,
                    vec![]
                ),
                SqlResource::Macro(
                    "inner".to_string(),
                    Span {
                        start_line: 3,
                        start_col: 17,
                        start_offset: 49,
                        end_line: 5,
                        end_col: 31,
                        end_offset: 128
                    },
                    None,
                    vec![]
                ),
            ]
        );
        Ok(())
    }

    #[test]
    fn test_parse_invalid_sql() {
        let sql = r#"
            {% macro unclosed() %}
                select 1
            {# Missing endmacro #}
        "#;

        let result = parse_macro_statements(sql, &PathBuf::from("test.sql"), &["macro"]);
        println!("result: {result:?}");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_materialization_macro() -> FsResult<()> {
        let sql_default = r#"
            {% materialization name, default %}

            {% endmaterialization %}
        "#;

        let resources = parse_macro_statements(
            sql_default,
            &PathBuf::from("test.sql"),
            &["materialization"],
        )?;
        assert_eq!(
            resources,
            vec![SqlResource::Materialization(
                "materialization_name_default".to_string(),
                "default".to_string(),
                Span {
                    start_line: 2,
                    start_col: 13,
                    end_line: 4,
                    end_col: 37,
                    start_offset: 13,
                    end_offset: 86
                }
            )]
        );

        let sql_custom = r#"
        {% materialization name, adapter='redshift', supported_languages=['sql', 'python'] %}

        {% endmaterialization %}
    "#;

        let resources =
            parse_macro_statements(sql_custom, &PathBuf::from("test.sql"), &["materialization"])?;
        assert_eq!(
            resources,
            vec![SqlResource::Materialization(
                "materialization_name_redshift".to_string(),
                "redshift".to_string(),
                Span {
                    start_line: 2,
                    start_col: 9,
                    end_line: 4,
                    end_col: 33,
                    start_offset: 9,
                    end_offset: 128
                }
            )]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_dict_update() {
        let (env, _, _) = setup_test_env();
        let env = Arc::new(env);
        let sql = r#"
        {% set my_dict = dict(
            a=1,
            b=2,
            c=3
        ) %}
        {% do my_dict.update({"d": 4, "c": 5}) %}
        {{ tojson(my_dict, sort_keys=true) }}
        "#;

        let rendered = render_sql(
            sql,
            &env,
            &BTreeMap::new(),
            &DefaultRenderingEventListenerFactory::default(),
            &PathBuf::from("test"),
        )
        .unwrap();

        let rendered = rendered.trim().replace(" ", "").replace("\n", "");
        assert_eq!(rendered, r#"{"a":1,"b":2,"c":5,"d":4}"#);
    }

    #[test]
    fn test_process_markdown_single_doc() {
        let sql = r#"
        {% docs cloud_plan_tier %}
        An identifier to group specific plans by targeted user groups.
        {% enddocs %}
        "#;

        let docs = parse_macro_statements(sql, Path::new("test.sql"), &["docs"]).unwrap();
        let doc_names: Vec<String> = docs
            .iter()
            .filter_map(|x| {
                if let SqlResource::Doc(name, _) = x {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect();

        assert_eq!(doc_names, vec!["cloud_plan_tier".to_string()]);
    }

    #[test]
    fn test_process_markdown_multiple_docs() {
        let sql = r#"


        {% docs cloud_plan %}
        The plan name representing the pricing and features for a given Cloud account.
        {% enddocs %}

        {% docs database_source %}
        The source Postgres database the Cloud account information comes from.
        {% enddocs %}
        "#;

        let docs = parse_macro_statements(sql, Path::new("test.sql"), &["docs"]).unwrap();
        let doc_names: Vec<String> = docs
            .iter()
            .filter_map(|x| {
                if let SqlResource::Doc(name, _) = x {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect();

        assert_eq!(
            doc_names,
            vec!["cloud_plan".to_string(), "database_source".to_string()]
        );
    }

    #[test]
    fn test_process_markdown_with_md_suffix() {
        let sql = r#"
        {% docs cloud_plan_tier.md %}
        An identifier to group specific plans by targeted user groups.
        {% enddocs %}
        "#;

        let docs = parse_macro_statements(sql, Path::new("test.sql"), &["docs"]).unwrap();
        let doc_names: Vec<String> = docs
            .iter()
            .filter_map(|x| {
                if let SqlResource::Doc(name, _) = x {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect();

        assert_eq!(doc_names, vec!["cloud_plan_tier".to_string()]);
    }

    #[test]
    fn test_process_markdown_no_docs() {
        let sql = r#"
        This is a readme.md file with {{ invalid-ish jinja }} in it
        "#;

        let docs = parse_macro_statements(sql, Path::new("test.sql"), &["docs"]).unwrap();
        assert!(docs.is_empty());
    }
    #[test]
    fn test_process_markdown_unclosed_docs() {
        let sql = r#"
    {% docs cloud_plan_tier %}
    An identifier to group specific plans by targeted user groups.
    "#;

        let res = parse_macro_statements(sql, Path::new("test.sql"), &["docs"]);
        println!("res: {res:?}");
        assert!(res.is_err());
    }
}
