//! Tests for FQN project-config construction.
//!
//! The code under test lives in `dbt-schemas` (re-exported as
//! `crate::dbt_project_config`), but these tests need `dbt-jinja-utils` to
//! deserialize the YAML fixtures, and that crate sits above `dbt-schemas` — a
//! dev-dependency back would be a cycle. So they live here, in the crate that
//! already has everything they need.

#[cfg(test)]
mod init_project_config_tests {
    use dbt_common::tracing::fs_error_log::get_log_message;
    use dbt_common::{ErrorCode, FsResult};
    use dbt_schemas::schemas::project::{
        DbtProjectConfig, ModelConfig, ProjectModelConfig, ProjectSnapshotConfig, ResolvableConfig,
        SnapshotConfig, TypedRecursiveConfig, init_project_config,
    };
    use dbt_tracing::{
        SeverityNumber, TelemetryOutputFlags,
        emit::create_root_info_span,
        init::create_tracing_subcriber_with_layer,
        layer::ConsumerLayer,
        test_support::mocks::{MockDynSpanEvent, TestLayer, test_data_layer},
    };

    #[allow(clippy::type_complexity)]
    fn init_project_config_from_yaml<T, S>(
        yaml: &str,
        disallow_plus_prefix: bool,
    ) -> (
        FsResult<DbtProjectConfig<T>>,
        Vec<(ErrorCode, String)>,
        Vec<(ErrorCode, String)>,
    )
    where
        T: ResolvableConfig<T> + PartialEq,
        S: TypedRecursiveConfig + Into<T> + for<'de> serde::Deserialize<'de>,
        T::PackageDefaults: Default,
    {
        // Diagnostics are emitted through the tracing layer, so capture them with a
        // test consumer rather than a status reporter.
        let (test_layer, _, _, log_records) = TestLayer::new();
        let subscriber = create_tracing_subcriber_with_layer(
            tracing::level_filters::LevelFilter::TRACE,
            test_data_layer(
                1,
                None,
                false,
                std::iter::empty(),
                std::iter::once(Box::new(test_layer) as ConsumerLayer),
            ),
            &[],
        )
        .expect("test tracing subscriber should be valid");

        let configs: S = dbt_jinja_utils::serde::from_yaml_raw(yaml, None, false, None)
            .expect("yaml deserializes into config type");

        // The data layer only records events emitted within an active span, so run
        // `init_project_config` inside a root span.
        let result = tracing::subscriber::with_default(subscriber, || {
            let _root = create_root_info_span(MockDynSpanEvent {
                name: "root".to_string(),
                flags: TelemetryOutputFlags::ALL,
                ..Default::default()
            })
            .entered();

            init_project_config::<T, S>(
                &Some(configs),
                Default::default(),
                None,
                disallow_plus_prefix,
            )
        });

        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        for record in log_records.lock().unwrap().iter() {
            // Diagnostics may arrive as either an `FsErrorLog` or a plain `LogMessage`
            // attribute; `get_log_message` handles both. The tests only inspect the
            // message string, so a placeholder `ErrorCode` is acceptable.
            let code = get_log_message(&record.attributes)
                .and_then(|lm| lm.code)
                .and_then(|c| u16::try_from(c).ok())
                .and_then(|c| ErrorCode::try_from(c).ok())
                .unwrap_or(ErrorCode::Generic);
            let entry = (code, record.body.clone());
            match record.severity_number {
                SeverityNumber::Error => errors.push(entry),
                SeverityNumber::Warn => warnings.push(entry),
                _ => {}
            }
        }

        (result, errors, warnings)
    }

    #[test]
    fn valid_config_emits_no_diagnostics() {
        let yml = r#"
path_1:
  +enabled: true
path_2:
  +enabled: true
  path_3:
    +enabled: false
"#;
        let (result, errors, warnings) =
            init_project_config_from_yaml::<ModelConfig, ProjectModelConfig>(yml, false);
        assert!(result.is_ok());
        assert!(errors.is_empty());
        assert!(warnings.is_empty());
    }

    #[test]
    fn plus_prefixed_resource_path_emits_diagnostics() {
        let yml = r#"
+path_1:
  +enabled: false
  +quoting:
    database: true
"#;
        // Behavior flag off
        let (result, errors, warnings) =
            init_project_config_from_yaml::<ModelConfig, ProjectModelConfig>(yml, false);
        assert!(result.is_ok());
        assert!(errors.is_empty());
        assert_eq!(warnings.len(), 1);
        let (_, msg) = &warnings[0];
        assert!(msg.contains("Resource path `+path_1` in dbt_project.yml starts with `+`"));

        // Behavior flag on
        let (result, errors, warnings) =
            init_project_config_from_yaml::<ModelConfig, ProjectModelConfig>(yml, true);
        assert!(result.is_ok());
        assert!(warnings.is_empty());
        assert_eq!(errors.len(), 1);
        let (_, msg) = &errors[0];
        assert!(msg.contains("Unrecognized key `+path_1`"));
    }

    #[test]
    fn nested_plus_resource_path_emits_diagnostics() {
        let yml = r#"
path_1:
  +path_2:
    path_3:
      path_4:
        +enabled: true
"#;
        // Behavior flag off
        let (result, errors, warnings) =
            init_project_config_from_yaml::<ModelConfig, ProjectModelConfig>(yml, false);
        assert!(result.is_ok());
        assert!(errors.is_empty());
        assert_eq!(warnings.len(), 1);
        let (_, msg) = &warnings[0];
        assert!(msg.contains("Resource path `path_1.+path_2` in dbt_project.yml starts with `+`"));

        // Behavior flag on
        let (result, errors, warnings) =
            init_project_config_from_yaml::<ModelConfig, ProjectModelConfig>(yml, true);
        assert!(result.is_ok());
        assert!(warnings.is_empty());
        assert_eq!(errors.len(), 1);
        let (_, msg) = &errors[0];
        assert!(msg.contains("Unrecognized key `path_1.+path_2`"));
    }

    #[test]
    fn ambiguous_resource_path_only_errors() {
        let yml = r#"
path_1:
  path_2:
    +enabled: true
    path_3:
      +not_provably_a_resource_path: {}
"#;
        // Behavior flag off
        let (result, errors, warnings) =
            init_project_config_from_yaml::<ModelConfig, ProjectModelConfig>(yml, false);
        assert!(result.is_ok());
        assert!(errors.is_empty());
        assert!(warnings.is_empty());

        // Behavior flag on
        let (result, errors, warnings) =
            init_project_config_from_yaml::<ModelConfig, ProjectModelConfig>(yml, true);
        assert!(result.is_ok());
        assert!(warnings.is_empty());
        assert_eq!(errors.len(), 1);
        let (_, msg) = &errors[0];
        assert!(
            msg.contains("Unrecognized key `path_1.path_2.path_3.+not_provably_a_resource_path`")
        );
    }

    #[test]
    fn meta_accepts_plus_prefixes() {
        let yml = r#"
path_1:
  path_2:
    +enabled: true
    +meta:
      +key_1: true
      +key_2: 15451
"#;
        // Behavior flag off
        let (result, errors, warnings) =
            init_project_config_from_yaml::<ModelConfig, ProjectModelConfig>(yml, false);
        assert!(result.is_ok());
        assert!(errors.is_empty());
        assert!(warnings.is_empty());

        // Behavior flag on
        let (result, errors, warnings) =
            init_project_config_from_yaml::<ModelConfig, ProjectModelConfig>(yml, true);
        assert!(result.is_ok());
        assert!(errors.is_empty());
        assert!(warnings.is_empty());
    }

    #[test]
    fn exactly_one_warning_per_plus_prefixed_path() {
        let yml = r#"
path_1:
  +path_2:
    +meta:
      +cool_key: 15451
    path_3:
      +quoting:
        database: true
        schema: false
      path_4:
        +enabled: true
  +path_3:
    +alias: cool_alias
    path_5:
      +description: cool_description
  +not_provably_a_resource_path: {}
"#;
        // Behavior flag off
        let (result, errors, warnings) =
            init_project_config_from_yaml::<ModelConfig, ProjectModelConfig>(yml, false);
        assert!(result.is_ok());
        assert!(errors.is_empty());
        assert_eq!(warnings.len(), 2);
        let (_, msg) = &warnings[0];
        assert!(msg.contains("Resource path `path_1.+path_2` in dbt_project.yml starts with `+`"));
        let (_, msg) = &warnings[1];
        assert!(msg.contains("Resource path `path_1.+path_3` in dbt_project.yml starts with `+`"));

        // Behavior flag on
        let (result, errors, warnings) =
            init_project_config_from_yaml::<ModelConfig, ProjectModelConfig>(yml, true);
        assert!(result.is_ok());
        assert!(warnings.is_empty());
        assert_eq!(errors.len(), 3);
        let (_, msg) = &errors[0];
        assert!(msg.contains("Unrecognized key `path_1.+not_provably_a_resource_path`"));
        let (_, msg) = &errors[1];
        assert!(msg.contains("Unrecognized key `path_1.+path_2`"));
        let (_, msg) = &errors[2];
        assert!(msg.contains("Unrecognized key `path_1.+path_3`"));
    }

    #[test]
    fn unrecognized_key_always_emits_error() {
        let yml = r#"
path_1:
  +bogus_key: scalar_value
"#;
        // Behavior flag off
        let (result, errors, warnings) =
            init_project_config_from_yaml::<ModelConfig, ProjectModelConfig>(yml, false);
        assert!(result.is_ok());
        assert!(warnings.is_empty());
        assert_eq!(errors.len(), 1);
        let (_, msg) = &errors[0];
        assert!(msg.contains("Unrecognized key `path_1.+bogus_key`"));

        // Behavior flag on
        let (result, errors, warnings) =
            init_project_config_from_yaml::<ModelConfig, ProjectModelConfig>(yml, true);
        assert!(result.is_ok());
        assert!(warnings.is_empty());
        assert_eq!(errors.len(), 1);
        let (_, msg) = &errors[0];
        assert!(msg.contains("Unrecognized key `path_1.+bogus_key`"));
    }

    #[test]
    fn repro_emits_error_at_key() {
        // https://github.com/dbt-labs/dbt-core/issues/14433
        let yml = r#"
my_project:
  staging:
    +contract:
      enforced: true
"#;
        // Behavior flag off
        // With the behavior flag off, the behavior is the same as in the issue.
        // We don't recognize that +contract is the source of the issue.

        // Behavior flag on
        // With the behavior flag on, we correctly see the unrecognized key
        // error surface at +contract.
        let (result, errors, warnings) =
            init_project_config_from_yaml::<SnapshotConfig, ProjectSnapshotConfig>(yml, true);
        assert!(result.is_ok());
        assert!(warnings.is_empty());
        assert_eq!(errors.len(), 1);
        let (_, msg) = &errors[0];
        assert!(msg.contains("Unrecognized key `my_project.staging.+contract`"));
    }
}
