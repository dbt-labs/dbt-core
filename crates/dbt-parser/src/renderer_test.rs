#[cfg(test)]
mod tests {
    use crate::args::ResolveArgs;
    use crate::dbt_project_config::DbtProjectConfig;
    use crate::renderer::{RenderCtx, RenderCtxInner, render_unresolved_sql_files};
    use dbt_common::adapter::AdapterType;
    use dbt_common::io_args::{FsCommand, IoArgs};
    use dbt_common::serde_utils::Omissible;
    use dbt_jinja_utils::jinja_environment::JinjaEnv;
    use dbt_jinja_utils::listener::DefaultJinjaTypeCheckEventListenerFactory;
    use dbt_schemas::filter::RunFilter;
    use dbt_schemas::schemas::common::{DbtChecksum, DbtQuoting, normalize_sql};
    use dbt_schemas::schemas::project::ModelConfig;
    use dbt_schemas::schemas::properties::ModelProperties;
    use dbt_schemas::state::{DbtAsset, DbtRuntimeConfig};
    use minijinja::Environment;
    use std::collections::{BTreeMap, HashMap};
    use std::path::PathBuf;
    use std::sync::Arc;

    /// Test that verifies root project config overrides work correctly in both
    /// sequential and parallel rendering modes by actually calling render_unresolved_sql_files
    #[tokio::test]
    async fn test_render_unresolved_sql_files_config_override() {
        // Set up a temporary directory and create a test SQL file
        let temp_dir = tempfile::TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();
        let models_dir = base_path.join("models");
        std::fs::create_dir_all(&models_dir).unwrap();

        let sql_file = models_dir.join("test_model.sql");
        std::fs::write(&sql_file, "SELECT 1 as test").unwrap();

        let path = PathBuf::from("models/test_model.sql");
        // Create the DbtAsset
        let test_asset = DbtAsset {
            base_path: base_path.clone(),
            original_path: path.clone(),
            path: path.clone(),
            package_name: "test_package".to_string(),
        };

        // Create configs - simulating a package with its own config and root project override
        let package_cfg = ModelConfig {
            enabled: Some(true),
            schema: Omissible::Present(Some("package_schema".to_string())),
            ..Default::default()
        };
        let package_config = DbtProjectConfig::<ModelConfig> {
            config: package_cfg,
            children: HashMap::new(),
        };

        let root_cfg = ModelConfig {
            enabled: Some(true),
            schema: Omissible::Present(Some("root_override_schema".to_string())),
            ..Default::default()
        };
        let root_config = DbtProjectConfig::<ModelConfig> {
            config: root_cfg,
            children: HashMap::new(),
        };

        // Set up Jinja environment
        let env = Environment::new();
        let jinja_env = Arc::new(JinjaEnv::new(env));

        // Create the arguments
        let args = ResolveArgs {
            io: IoArgs {
                in_dir: base_path.clone(),
                out_dir: base_path.clone(),
                ..Default::default()
            },
            num_threads: Some(1), // Will test both sequential (1) and parallel (>1)
            command: FsCommand::Test,
            vars: BTreeMap::new(),
            from_main: false,
            selector: None,
            select: None,
            indirect_selection: None,
            exclude: None,
            replay: None,
            sample_config: RunFilter::default(),
            sample_renaming: BTreeMap::new(),
        };

        // Create base context with minimal required values
        let mut base_ctx = BTreeMap::new();
        base_ctx.insert(
            "project_name".to_string(),
            minijinja::Value::from("test_package"),
        );

        // Create the render context
        let render_ctx = RenderCtx {
            inner: Arc::new(RenderCtxInner {
                args: args.clone(),
                base_ctx,
                root_project_name: "root_project".to_string(),
                package_name: "test_package".to_string(), // Different from root - this triggers the override logic
                adapter_type: AdapterType::Postgres,
                database: "test_db".to_string(),
                schema: "default_schema".to_string(),
                local_project_config: package_config,
                root_project_config: root_config,
                resource_paths: vec!["models".to_string()],
                package_quoting: DbtQuoting {
                    database: Some(true),
                    schema: Some(true),
                    identifier: Some(true),
                    snowflake_ignore_case: Some(false),
                },
            }),
            jinja_env: jinja_env.clone(),
            runtime_config: Arc::new(DbtRuntimeConfig::default()),
        };

        // Create a cancellation token
        use dbt_common::cancellation::CancellationToken;
        let token = CancellationToken::never_cancels();

        // Test 1: Sequential rendering (num_threads = 1)
        let mut node_properties = BTreeMap::new();
        let seq_results = render_unresolved_sql_files::<ModelConfig, ModelProperties>(
            &render_ctx,
            &[test_asset],
            &mut node_properties,
            &token,
            Arc::new(DefaultJinjaTypeCheckEventListenerFactory::default()),
        )
        .await
        .unwrap();

        assert_eq!(seq_results.len(), 1, "Should have one result");
        let seq_schema = match &seq_results[0].sql_file_info.config.schema {
            Omissible::Present(Some(s)) => s.clone(),
            _ => panic!("Expected schema to be present in sequential result"),
        };

        // Test 2: Parallel rendering (num_threads > 1, and enough files to trigger parallel)
        let mut parallel_ctx = render_ctx.clone();
        Arc::make_mut(&mut parallel_ctx.inner).args.num_threads = Some(4);

        // Create 60 files to ensure we exceed the 50-file threshold for parallel processing
        let mut many_assets = vec![];
        for i in 0..60 {
            let sql_file = models_dir.join(format!("model_{i}.sql"));
            std::fs::write(&sql_file, format!("SELECT {i} as id")).unwrap();
            let path = PathBuf::from(format!("models/model_{i}.sql"));
            many_assets.push(DbtAsset {
                base_path: base_path.clone(),
                original_path: path.clone(),
                path: path.clone(),
                package_name: "test_package".to_string(),
            });
        }

        node_properties.clear();
        let par_results = render_unresolved_sql_files::<ModelConfig, ModelProperties>(
            &parallel_ctx,
            &many_assets,
            &mut node_properties,
            &token,
            Arc::new(DefaultJinjaTypeCheckEventListenerFactory::default()),
        )
        .await
        .unwrap();

        assert!(par_results.len() >= 60, "Should have results for all files");

        // Check the first result's schema
        let par_schema = match &par_results[0].sql_file_info.config.schema {
            Omissible::Present(Some(s)) => s.clone(),
            _ => panic!("Expected schema to be present in parallel result"),
        };

        // The key assertion: both sequential and parallel should resolve to root override
        assert_eq!(
            seq_schema, "root_override_schema",
            "Sequential rendering should use root project override"
        );
        assert_eq!(
            par_schema, "root_override_schema",
            "Parallel rendering should use root project override"
        );
        assert_eq!(
            seq_schema, par_schema,
            "Sequential and parallel should produce the same schema"
        );
    }

    /// Simple unit test to verify config override ordering behavior
    #[test]
    fn test_config_override_order() {
        use crate::sql_file_info::SqlFileInfo;
        use dbt_common::serde_utils::Omissible;
        use dbt_jinja_utils::phases::parse::sql_resource::SqlResource;
        use dbt_schemas::schemas::common::DbtChecksum;
        use dbt_schemas::schemas::project::ModelConfig;

        // Create two configs - package and root
        let package_config = ModelConfig {
            schema: Omissible::Present(Some("package_schema".to_string())),
            ..Default::default()
        };

        let root_config = ModelConfig {
            schema: Omissible::Present(Some("root_override_schema".to_string())),
            ..Default::default()
        };

        // Test the correct order: package config first, then root config
        let resources_correct_order = vec![
            SqlResource::Config(Box::new(package_config.clone())),
            SqlResource::Config(Box::new(root_config.clone())),
        ];

        let sql_file_info_correct = SqlFileInfo::<ModelConfig>::from_sql_resources(
            resources_correct_order,
            DbtChecksum::hash(b"test"),
            false,
        );

        // The schema should be from root config (override)
        match &sql_file_info_correct.config.schema {
            Omissible::Present(Some(schema)) => {
                assert_eq!(
                    schema, "root_override_schema",
                    "Root config should override package config"
                );
            }
            _ => panic!("Expected schema to be present"),
        }

        // Test the wrong order (what was happening with insert(0)): root config first, then package config
        let resources_wrong_order = vec![
            SqlResource::Config(Box::new(root_config)),
            SqlResource::Config(Box::new(package_config)),
        ];

        let sql_file_info_wrong = SqlFileInfo::<ModelConfig>::from_sql_resources(
            resources_wrong_order,
            DbtChecksum::hash(b"test"),
            false,
        );

        // With wrong order, the schema would incorrectly be from package config
        match &sql_file_info_wrong.config.schema {
            Omissible::Present(Some(schema)) => {
                assert_eq!(
                    schema, "package_schema",
                    "With wrong order, package config incorrectly overrides root"
                );
            }
            _ => panic!("Expected schema to be present"),
        }
    }

    /// Test that verifies the checksum calculation in render_sql_file matches expected values.
    /// The checksum is computed by:
    /// 1. Normalizing SQL (removing whitespace and lowercasing)
    /// 2. SHA256 hashing the normalized content
    #[tokio::test]
    async fn test_render_sql_file_checksum_calculation() {
        use dbt_common::cancellation::CancellationToken;

        // Set up a temporary directory and create a test SQL file
        let temp_dir = tempfile::TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();
        let models_dir = base_path.join("models");
        std::fs::create_dir_all(&models_dir).unwrap();

        // Test case 1: Simple SQL content
        let sql_content = "SELECT 1 as id";
        let sql_file = models_dir.join("test_model.sql");
        std::fs::write(&sql_file, sql_content).unwrap();

        // Compute the expected checksum manually using the same algorithm
        let normalized_sql = normalize_sql(sql_content);
        let expected_checksum = DbtChecksum::hash(normalized_sql.as_bytes());

        // Verify our normalization understanding is correct
        assert_eq!(
            normalized_sql, "select1asid",
            "Normalized SQL should have no whitespace and be lowercase"
        );

        let path = PathBuf::from("models/test_model.sql");
        let test_asset = DbtAsset {
            base_path: base_path.clone(),
            original_path: path.clone(),
            path: path.clone(),
            package_name: "test_package".to_string(),
        };

        // Minimal config setup
        let package_config = DbtProjectConfig::<ModelConfig> {
            config: ModelConfig {
                enabled: Some(true),
                ..Default::default()
            },
            children: HashMap::new(),
        };

        let env = Environment::new();
        let jinja_env = Arc::new(JinjaEnv::new(env));

        let args = ResolveArgs {
            io: IoArgs {
                in_dir: base_path.clone(),
                out_dir: base_path.clone(),
                ..Default::default()
            },
            num_threads: Some(1),
            command: FsCommand::Parse,
            vars: BTreeMap::new(),
            from_main: false,
            selector: None,
            select: None,
            indirect_selection: None,
            exclude: None,
            replay: None,
            sample_config: RunFilter::default(),
            sample_renaming: BTreeMap::new(),
        };

        let mut base_ctx = BTreeMap::new();
        base_ctx.insert(
            "project_name".to_string(),
            minijinja::Value::from("test_package"),
        );

        let render_ctx = RenderCtx {
            inner: Arc::new(RenderCtxInner {
                args,
                base_ctx,
                root_project_name: "test_package".to_string(),
                package_name: "test_package".to_string(),
                adapter_type: AdapterType::Postgres,
                database: "test_db".to_string(),
                schema: "test_schema".to_string(),
                local_project_config: package_config.clone(),
                root_project_config: package_config,
                resource_paths: vec!["models".to_string()],
                package_quoting: DbtQuoting {
                    database: Some(true),
                    schema: Some(true),
                    identifier: Some(true),
                    snowflake_ignore_case: Some(false),
                },
            }),
            jinja_env,
            runtime_config: Arc::new(DbtRuntimeConfig::default()),
        };

        let token = CancellationToken::never_cancels();
        let mut node_properties = BTreeMap::new();

        let results = render_unresolved_sql_files::<ModelConfig, ModelProperties>(
            &render_ctx,
            &[test_asset],
            &mut node_properties,
            &token,
            Arc::new(DefaultJinjaTypeCheckEventListenerFactory::default()),
        )
        .await
        .unwrap();

        assert_eq!(results.len(), 1, "Should have exactly one result");

        let actual_checksum = &results[0].sql_file_info.checksum;
        assert_eq!(
            actual_checksum, &expected_checksum,
            "Checksum in SqlFileRenderResult should match expected checksum.\n\
             Expected: {:?}\n\
             Actual: {:?}\n\
             SQL content: '{}'\n\
             Normalized: '{}'",
            expected_checksum, actual_checksum, sql_content, normalized_sql
        );
    }

    /// Test checksum calculation with SQL containing various whitespace patterns
    #[tokio::test]
    async fn test_render_sql_file_checksum_whitespace_invariant() {
        use dbt_common::cancellation::CancellationToken;

        let temp_dir = tempfile::TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();
        let models_dir = base_path.join("models");
        std::fs::create_dir_all(&models_dir).unwrap();

        // Two SQL files with different whitespace but same semantic content
        // should produce the same checksum
        let sql_compact = "SELECT 1 as id";
        let sql_spaced = "SELECT   1   as   id";
        let sql_multiline = "SELECT\n    1\n    as\n    id";

        // All should normalize to the same string
        let normalized_compact = normalize_sql(sql_compact);
        let normalized_spaced = normalize_sql(sql_spaced);
        let normalized_multiline = normalize_sql(sql_multiline);

        assert_eq!(normalized_compact, normalized_spaced);
        assert_eq!(normalized_spaced, normalized_multiline);
        assert_eq!(normalized_compact, "select1asid");

        // All should produce the same checksum
        let checksum_compact = DbtChecksum::hash(normalized_compact.as_bytes());
        let checksum_spaced = DbtChecksum::hash(normalized_spaced.as_bytes());
        let checksum_multiline = DbtChecksum::hash(normalized_multiline.as_bytes());

        assert_eq!(checksum_compact, checksum_spaced);
        assert_eq!(checksum_spaced, checksum_multiline);

        // Now verify through the rendering pipeline
        std::fs::write(models_dir.join("model_compact.sql"), sql_compact).unwrap();
        std::fs::write(models_dir.join("model_spaced.sql"), sql_spaced).unwrap();
        std::fs::write(models_dir.join("model_multiline.sql"), sql_multiline).unwrap();

        let assets: Vec<DbtAsset> = ["model_compact", "model_spaced", "model_multiline"]
            .iter()
            .map(|name| {
                let path = PathBuf::from(format!("models/{}.sql", name));
                DbtAsset {
                    base_path: base_path.clone(),
                    original_path: path.clone(),
                    path,
                    package_name: "test_package".to_string(),
                }
            })
            .collect();

        let package_config = DbtProjectConfig::<ModelConfig> {
            config: ModelConfig {
                enabled: Some(true),
                ..Default::default()
            },
            children: HashMap::new(),
        };

        let env = Environment::new();
        let jinja_env = Arc::new(JinjaEnv::new(env));

        let args = ResolveArgs {
            io: IoArgs {
                in_dir: base_path.clone(),
                out_dir: base_path.clone(),
                ..Default::default()
            },
            num_threads: Some(1),
            command: FsCommand::Parse,
            vars: BTreeMap::new(),
            from_main: false,
            selector: None,
            select: None,
            indirect_selection: None,
            exclude: None,
            replay: None,
            sample_config: RunFilter::default(),
            sample_renaming: BTreeMap::new(),
        };

        let mut base_ctx = BTreeMap::new();
        base_ctx.insert(
            "project_name".to_string(),
            minijinja::Value::from("test_package"),
        );

        let render_ctx = RenderCtx {
            inner: Arc::new(RenderCtxInner {
                args,
                base_ctx,
                root_project_name: "test_package".to_string(),
                package_name: "test_package".to_string(),
                adapter_type: AdapterType::Postgres,
                database: "test_db".to_string(),
                schema: "test_schema".to_string(),
                local_project_config: package_config.clone(),
                root_project_config: package_config,
                resource_paths: vec!["models".to_string()],
                package_quoting: DbtQuoting {
                    database: Some(true),
                    schema: Some(true),
                    identifier: Some(true),
                    snowflake_ignore_case: Some(false),
                },
            }),
            jinja_env,
            runtime_config: Arc::new(DbtRuntimeConfig::default()),
        };

        let token = CancellationToken::never_cancels();
        let mut node_properties = BTreeMap::new();

        let results = render_unresolved_sql_files::<ModelConfig, ModelProperties>(
            &render_ctx,
            &assets,
            &mut node_properties,
            &token,
            Arc::new(DefaultJinjaTypeCheckEventListenerFactory::default()),
        )
        .await
        .unwrap();

        assert_eq!(results.len(), 3, "Should have three results");

        // All three files should have the same checksum despite different whitespace
        let checksums: Vec<_> = results.iter().map(|r| &r.sql_file_info.checksum).collect();
        assert_eq!(
            checksums[0], checksums[1],
            "Compact and spaced SQL should have same checksum"
        );
        assert_eq!(
            checksums[1], checksums[2],
            "Spaced and multiline SQL should have same checksum"
        );
        assert_eq!(
            checksums[0], &checksum_compact,
            "Rendered checksum should match manually computed checksum"
        );
    }
}
