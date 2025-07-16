//! Module defines the global project configuration, which is used to
//! load and propagate configuration properties from the root `dbt_project.yml`
//! to the individual model directories.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use dbt_common::{
    ErrorCode, FsResult, fs_err, io_args::IoArgs, show_error, show_warning_soon_to_be_error,
};
use dbt_schemas::schemas::project::{
    DataTestConfig, DefaultTo, IterChildren, ModelConfig, SeedConfig, SnapshotConfig, SourceConfig,
    UnitTestConfig,
};
use dbt_schemas::schemas::{common::DbtQuoting, project::DbtProject};
use dbt_serde_yaml::ShouldBe;

/// Used to deserialize the top-level `dbt_project.yml` configuration
/// for `models`, `data_tests`, `seeds` etc..
///
/// ```yaml
/// models:
///   dbt_jinja(project_name):
///     adapter(folder_name in project):
///       +schema: 'dbt_jinja'
///       get_relation_cache:
///       +alias: 'dbt_jinja'
/// ```
///
/// This configuration is path based, meaning each key that is not a
/// property of it's configuration <T> is the name of a directory, which may have
/// source files or apply additional configuration. Configuration precedence
/// is given to the most specific path configuration. All unspecified
/// configuration is inherited from the parent.
///
#[derive(Debug, Clone)]
pub struct DbtProjectConfig<T: DefaultTo<T>> {
    /// The root configuration (i.e. at the `dbt_project.yml` level or inherited from `profiles.yml`)
    pub config: T,
    /// Child configuration applied by path part
    pub children: HashMap<String, DbtProjectConfig<T>>,
}

impl<T: DefaultTo<T>> DbtProjectConfig<T> {
    /// Create a new [GlobalProjectConfig] from a default configuration and the root dbt_project.yml [DbtProjectConfigs]
    pub fn try_new<S: Into<T> + IterChildren<S> + Clone>(
        io: &IoArgs,
        dbt_config: &T,
        configs: &S,
    ) -> FsResult<Self> {
        recur_build_dbt_project_config(io, dbt_config, configs, "")
    }

    /// Get the configuration for a ref path
    pub fn get_config_for_path(
        &self,
        ref_path: &Path,
        project_name: &str,
        resource_paths: &[String],
    ) -> &T {
        let stripped_path = strip_resource_paths_from_ref_path(ref_path, resource_paths);
        let mut components = stripped_path.components().rev().collect::<Vec<_>>();

        let mut current_config = self;
        // If the project name is not in the children, return the root config
        if let Some(project_config) = current_config.children.get(project_name) {
            current_config = project_config;
            while let Some(component) = components.pop() {
                // look for filestem specific configs on the last component
                let component_str: String = if components.is_empty() {
                    stripped_path
                        .file_stem()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_string()
                } else {
                    component.as_os_str().to_str().unwrap().to_string()
                };
                if let Some(child) = current_config.children.get(&component_str) {
                    current_config = child;
                } else {
                    break;
                }
            }
        }
        &current_config.config
    }

    /// Set the configuration for the root [GlobalProjectConfig]
    pub fn with_config(&mut self, config: T) {
        self.config = config;
    }
}

/// Recursively build the [DbtProjectConfig] from a parent and child configuration
pub fn recur_build_dbt_project_config<T: DefaultTo<T>, S: Into<T> + IterChildren<S> + Clone>(
    io: &IoArgs,
    parent_config: &T,
    child: &S,
    key_path: &str,
) -> FsResult<DbtProjectConfig<T>> {
    let mut child_config: T = child.clone().into();
    child_config.default_to(parent_config);
    let mut children = HashMap::new();

    // Handle additional properties generically - each child inherits from current config
    for (key, maybe_child_config_variant) in child.iter_children() {
        let key_path = if key_path.is_empty() {
            key.clone()
        } else {
            format!("{key_path}.{key}")
        };
        let child_config_variant = match maybe_child_config_variant {
            ShouldBe::AndIs(config) => config,
            ShouldBe::ButIsnt { raw, .. } => {
                let err = fs_err!(
                    code => ErrorCode::UnusedConfigKey,
                    loc => raw.as_ref().map(|r| r.span()).unwrap_or_default(),
                    "Ignored unexpected key `{:?}`. YAML path: `{}`.", key.trim(), key_path
                );
                if std::env::var("_DBT_FUSION_STRICT_MODE").is_ok() {
                    show_error!(io, err);
                } else {
                    show_warning_soon_to_be_error!(io, err);
                }
                continue;
            }
        };

        children.insert(
            key.clone(),
            recur_build_dbt_project_config(io, &child_config, child_config_variant, &key_path)?,
        );
    }

    Ok(DbtProjectConfig {
        config: child_config,
        children,
    })
}

/// Config wrapping propagated configs for the root project
pub struct RootProjectConfigs {
    /// Model configs
    pub models: DbtProjectConfig<ModelConfig>,
    /// Source configs
    pub sources: DbtProjectConfig<SourceConfig>,
    /// Snapshot configs
    pub snapshots: DbtProjectConfig<SnapshotConfig>,
    /// Seed configs
    pub seeds: DbtProjectConfig<SeedConfig>,
    /// Test configs
    pub tests: DbtProjectConfig<DataTestConfig>,
    /// Unit test configs
    pub unit_tests: DbtProjectConfig<UnitTestConfig>,
}

/// Build the [RootProjectConfigs] from a [DbtProject]
pub fn build_root_project_configs(
    io_args: &IoArgs,
    root_project: &DbtProject,
    root_project_quoting: DbtQuoting,
) -> FsResult<RootProjectConfigs> {
    let maybe_root_project_config =
        match (root_project.tests.clone(), root_project.data_tests.clone()) {
            (Some(_), Some(_)) => {
                unimplemented!("Merge logic for tests and data tests is unimplemented")
            }
            (Some(tests), None) => Some(tests),
            (None, Some(data_tests)) => Some(data_tests),
            (None, None) => None,
        };
    Ok(RootProjectConfigs {
        models: init_project_config(
            io_args,
            &root_project.models,
            ModelConfig {
                enabled: Some(true),
                quoting: Some(root_project_quoting),
                ..Default::default()
            },
        )?,
        sources: init_project_config(
            io_args,
            &root_project.sources,
            SourceConfig {
                enabled: Some(true),
                quoting: Some(root_project_quoting),
                ..Default::default()
            },
        )?,
        snapshots: init_project_config(
            io_args,
            &root_project.snapshots,
            SnapshotConfig {
                enabled: Some(true),
                quoting: Some(root_project_quoting),
                ..Default::default()
            },
        )?,
        seeds: init_project_config(
            io_args,
            &root_project.seeds,
            SeedConfig {
                enabled: Some(true),
                quoting: Some(root_project_quoting),
                ..Default::default()
            },
        )?,
        tests: init_project_config(
            io_args,
            &maybe_root_project_config,
            DataTestConfig {
                enabled: Some(true),
                quoting: Some(root_project_quoting),
                ..Default::default()
            },
        )?,
        unit_tests: init_project_config(
            io_args,
            &root_project.unit_tests,
            UnitTestConfig {
                enabled: Some(true),
                ..Default::default()
            },
        )?,
    })
}

/// generate the project config that will be inherited throughout the project
pub fn init_project_config<T: DefaultTo<T>, S: Into<T> + IterChildren<S> + Clone>(
    io_args: &IoArgs,
    dbt_project_configs: &Option<S>,
    default_config: T,
) -> FsResult<DbtProjectConfig<T>> {
    let project_config = if let Some(configs) = dbt_project_configs {
        DbtProjectConfig::try_new(io_args, &default_config, configs)?
    } else {
        DbtProjectConfig {
            config: default_config,
            children: HashMap::new(),
        }
    };
    Ok(project_config)
}

/// Strip resource paths from the beginning of a reference path
/// This function tries to find which resource path is a prefix of the ref_path
/// and returns the path with that prefix stripped
fn strip_resource_paths_from_ref_path(ref_path: &Path, resource_paths: &[String]) -> PathBuf {
    // Try to find a resource path that is a prefix of the ref_path
    for resource_path in resource_paths {
        let resource_pathbuf = PathBuf::from(resource_path);

        // Use Path::starts_with which properly handles path components
        if ref_path.starts_with(&resource_pathbuf) {
            // Use Path::strip_prefix which is designed for this exact purpose
            if let Ok(stripped) = ref_path.strip_prefix(&resource_pathbuf) {
                // Only return the stripped path if it's not empty
                // (i.e., ref_path was not exactly equal to resource_path)
                if stripped.as_os_str().is_empty() {
                    return ref_path.to_path_buf();
                } else {
                    return stripped.to_path_buf();
                }
            }
        }
    }

    // If no resource path matches, return the original path
    ref_path.to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_resource_paths_single_level() {
        let ref_path = Path::new("models/my_model.sql");
        let resource_paths = vec!["models".to_string()];
        let result = strip_resource_paths_from_ref_path(ref_path, &resource_paths);
        assert_eq!(result, PathBuf::from("my_model.sql"));
    }

    #[test]
    fn test_strip_resource_paths_nested_structure() {
        let ref_path = Path::new("dbt/models/example/my_first_model.sql");
        let resource_paths = vec!["dbt/models".to_string()];
        let result = strip_resource_paths_from_ref_path(ref_path, &resource_paths);
        assert_eq!(result, PathBuf::from("example/my_first_model.sql"));
    }

    #[test]
    fn test_strip_resource_paths_deep_nesting() {
        let ref_path = Path::new("warehouse/staging/models/marts/finance/revenue.sql");
        let resource_paths = vec!["warehouse/staging/models".to_string()];
        let result = strip_resource_paths_from_ref_path(ref_path, &resource_paths);
        assert_eq!(result, PathBuf::from("marts/finance/revenue.sql"));
    }

    #[test]
    fn test_strip_resource_paths_multiple_paths() {
        let ref_path = Path::new("src/models/staging/customers.sql");
        let resource_paths = vec![
            "models".to_string(),
            "src/models".to_string(),
            "dbt/models".to_string(),
        ];
        let result = strip_resource_paths_from_ref_path(ref_path, &resource_paths);
        assert_eq!(result, PathBuf::from("staging/customers.sql"));
    }

    #[test]
    fn test_strip_resource_paths_no_match() {
        let ref_path = Path::new("analysis/my_analysis.sql");
        let resource_paths = vec!["models".to_string(), "seeds".to_string()];
        let result = strip_resource_paths_from_ref_path(ref_path, &resource_paths);
        assert_eq!(result, PathBuf::from("analysis/my_analysis.sql"));
    }

    #[test]
    fn test_strip_resource_paths_empty_resource_paths() {
        let ref_path = Path::new("models/example/my_model.sql");
        let resource_paths: Vec<String> = vec![];
        let result = strip_resource_paths_from_ref_path(ref_path, &resource_paths);
        assert_eq!(result, PathBuf::from("models/example/my_model.sql"));
    }

    #[test]
    fn test_strip_resource_paths_exact_match() {
        let ref_path = Path::new("models");
        let resource_paths = vec!["models".to_string()];
        let result = strip_resource_paths_from_ref_path(ref_path, &resource_paths);
        // Should return original path since stripping would result in empty string
        assert_eq!(result, PathBuf::from("models"));
    }

    #[test]
    fn test_get_config_for_path_basic() {
        let mut config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: HashMap::new(),
        };
        config.config.enabled = Some(true);

        // Add a child config for project "test_project"
        let mut project_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: HashMap::new(),
        };
        project_config.config.enabled = Some(false);
        config
            .children
            .insert("test_project".to_string(), project_config);

        let result = config.get_config_for_path(
            Path::new("models/my_model.sql"),
            "test_project",
            &["models".to_string()],
        );

        assert_eq!(result.enabled, Some(false));
    }

    #[test]
    fn test_get_config_for_path_nested_directory() {
        let mut config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: HashMap::new(),
        };
        config.config.enabled = Some(true);

        // Add project config
        let mut project_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: HashMap::new(),
        };
        project_config.config.enabled = Some(false);

        // Add example subdirectory config
        let mut example_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: HashMap::new(),
        };
        example_config.config.enabled = Some(true);
        example_config.config.materialized =
            Some(dbt_schemas::schemas::common::DbtMaterialization::Table);

        project_config
            .children
            .insert("example".to_string(), example_config);
        config
            .children
            .insert("test_project".to_string(), project_config);

        let result = config.get_config_for_path(
            Path::new("dbt/models/example/my_model.sql"),
            "test_project",
            &["dbt/models".to_string()],
        );

        assert_eq!(result.enabled, Some(true));
        assert_eq!(
            result.materialized,
            Some(dbt_schemas::schemas::common::DbtMaterialization::Table)
        );
    }

    #[test]
    fn test_get_config_for_path_file_specific_config() {
        let mut config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: HashMap::new(),
        };
        config.config.enabled = Some(true);

        // Add project config
        let mut project_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: HashMap::new(),
        };
        project_config.config.enabled = Some(false);

        // Add example subdirectory config
        let mut example_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: HashMap::new(),
        };
        example_config.config.enabled = Some(true);

        // Add file-specific config
        let mut file_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: HashMap::new(),
        };
        file_config.config.enabled = Some(false);
        file_config.config.materialized =
            Some(dbt_schemas::schemas::common::DbtMaterialization::View);

        example_config
            .children
            .insert("my_model".to_string(), file_config);
        project_config
            .children
            .insert("example".to_string(), example_config);
        config
            .children
            .insert("test_project".to_string(), project_config);

        let result = config.get_config_for_path(
            Path::new("dbt/models/example/my_model.sql"),
            "test_project",
            &["dbt/models".to_string()],
        );

        assert_eq!(result.enabled, Some(false));
        assert_eq!(
            result.materialized,
            Some(dbt_schemas::schemas::common::DbtMaterialization::View)
        );
    }

    #[test]
    fn test_get_config_for_path_nonexistent_project() {
        let mut config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: HashMap::new(),
        };
        config.config.enabled = Some(true);
        config.config.materialized = Some(dbt_schemas::schemas::common::DbtMaterialization::Table);

        let result = config.get_config_for_path(
            Path::new("models/my_model.sql"),
            "nonexistent_project",
            &["models".to_string()],
        );

        // Should return root config
        assert_eq!(result.enabled, Some(true));
        assert_eq!(
            result.materialized,
            Some(dbt_schemas::schemas::common::DbtMaterialization::Table)
        );
    }

    #[test]
    fn test_get_config_for_path_partial_match() {
        let mut config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: HashMap::new(),
        };
        config.config.enabled = Some(true);

        // Add project config
        let mut project_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: HashMap::new(),
        };
        project_config.config.enabled = Some(false);

        // Add staging subdirectory config - now with enabled field set
        let mut staging_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: HashMap::new(),
        };
        staging_config.config.enabled = Some(false);
        staging_config.config.materialized =
            Some(dbt_schemas::schemas::common::DbtMaterialization::View);

        project_config
            .children
            .insert("staging".to_string(), staging_config);
        config
            .children
            .insert("test_project".to_string(), project_config);

        // Path has staging/finance but only staging config exists
        let result = config.get_config_for_path(
            Path::new("models/staging/finance/customers.sql"),
            "test_project",
            &["models".to_string()],
        );

        // Should get staging config since finance doesn't exist
        assert_eq!(result.enabled, Some(false));
        assert_eq!(
            result.materialized,
            Some(dbt_schemas::schemas::common::DbtMaterialization::View)
        );
    }

    #[test]
    fn test_get_config_for_path_empty_resource_paths() {
        let mut config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: HashMap::new(),
        };
        config.config.enabled = Some(true);

        // Add project config with subdirectory
        let mut project_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: HashMap::new(),
        };
        let mut models_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: HashMap::new(),
        };
        let mut example_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: HashMap::new(),
        };
        example_config.config.materialized =
            Some(dbt_schemas::schemas::common::DbtMaterialization::Table);

        models_config
            .children
            .insert("example".to_string(), example_config);
        project_config
            .children
            .insert("models".to_string(), models_config);
        config
            .children
            .insert("test_project".to_string(), project_config);

        let result = config.get_config_for_path(
            Path::new("models/example/my_model.sql"),
            "test_project",
            &[], // Empty resource paths
        );

        // Should traverse full path since no stripping occurs
        assert_eq!(
            result.materialized,
            Some(dbt_schemas::schemas::common::DbtMaterialization::Table)
        );
    }

    #[test]
    fn test_strip_resource_paths_first_match_wins() {
        // Test that the function uses the first matching path in the array
        let ref_path = Path::new("models/staging/customers.sql");
        let resource_paths = vec![
            "models".to_string(),         // This should match first
            "models/staging".to_string(), // This is more specific but comes later
        ];
        let result = strip_resource_paths_from_ref_path(ref_path, &resource_paths);
        // Should strip "models" (first match), not "models/staging"
        assert_eq!(result, PathBuf::from("staging/customers.sql"));
    }

    #[test]
    fn test_integration_real_dbt_project_structure() {
        // Integration test: Test a realistic DBT project scenario end-to-end
        let mut config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: HashMap::new(),
        };
        config.config.enabled = Some(true);
        config.config.materialized = Some(dbt_schemas::schemas::common::DbtMaterialization::View);

        // Set up project structure like: my_project -> staging -> +materialized: table
        let mut project_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: HashMap::new(),
        };
        project_config.config.enabled = Some(true);

        let mut staging_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: HashMap::new(),
        };
        staging_config.config.materialized =
            Some(dbt_schemas::schemas::common::DbtMaterialization::Table);
        staging_config.config.enabled = Some(true);

        // Add specific model config
        let mut customers_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: HashMap::new(),
        };
        customers_config.config.materialized =
            Some(dbt_schemas::schemas::common::DbtMaterialization::Incremental);
        customers_config.config.enabled = Some(false);

        staging_config
            .children
            .insert("stg_customers".to_string(), customers_config);
        project_config
            .children
            .insert("staging".to_string(), staging_config);
        config
            .children
            .insert("my_project".to_string(), project_config);

        // Test path: warehouse/dbt/models/staging/stg_customers.sql
        // With resource_paths: ["warehouse/dbt/models"]
        // Should strip to: staging/stg_customers.sql
        // Should find config: my_project -> staging -> stg_customers
        let result = config.get_config_for_path(
            Path::new("warehouse/dbt/models/staging/stg_customers.sql"),
            "my_project",
            &["warehouse/dbt/models".to_string()],
        );

        // Should get the most specific config (file-level)
        assert_eq!(result.enabled, Some(false));
        assert_eq!(
            result.materialized,
            Some(dbt_schemas::schemas::common::DbtMaterialization::Incremental)
        );
    }

    #[test]
    fn test_resource_path_edge_cases() {
        // Test various edge cases that could occur in real projects

        // Case 1: Resource path with trailing slash
        let result1 = strip_resource_paths_from_ref_path(
            Path::new("models/my_model.sql"),
            &["models/".to_string()],
        );
        assert_eq!(result1, PathBuf::from("my_model.sql"));

        // Case 2: Very deep nesting
        let result2 = strip_resource_paths_from_ref_path(
            Path::new("data/warehouse/dbt/models/marts/finance/reporting/revenue_monthly.sql"),
            &["data/warehouse/dbt/models".to_string()],
        );
        assert_eq!(
            result2,
            PathBuf::from("marts/finance/reporting/revenue_monthly.sql")
        );

        // Case 3: Path that has similar prefix but different directory
        // This should NOT be stripped because "models_backup" is not the "models" directory
        let result3 = strip_resource_paths_from_ref_path(
            Path::new("models_backup/my_model.sql"),
            &["models".to_string()],
        );
        // Fixed behavior: no stripping since "models_backup" != "models" directory
        assert_eq!(result3, PathBuf::from("models_backup/my_model.sql"));
    }

    #[test]
    fn test_path_component_boundary_matching() {
        // Test that we correctly distinguish between path components vs string prefixes

        // Should strip: exact directory match
        let result1 = strip_resource_paths_from_ref_path(
            Path::new("models/staging/customers.sql"),
            &["models".to_string()],
        );
        assert_eq!(result1, PathBuf::from("staging/customers.sql"));

        // Should NOT strip: different directory with similar name
        let result2 = strip_resource_paths_from_ref_path(
            Path::new("models_v2/customers.sql"),
            &["models".to_string()],
        );
        assert_eq!(result2, PathBuf::from("models_v2/customers.sql"));

        // Should NOT strip: file that starts with resource path name
        let result3 =
            strip_resource_paths_from_ref_path(Path::new("models.sql"), &["models".to_string()]);
        assert_eq!(result3, PathBuf::from("models.sql"));

        // Should strip: nested path with exact component match
        let result4 = strip_resource_paths_from_ref_path(
            Path::new("src/models/staging/customers.sql"),
            &["src/models".to_string()],
        );
        assert_eq!(result4, PathBuf::from("staging/customers.sql"));

        // Should NOT strip: similar but different nested path
        let result5 = strip_resource_paths_from_ref_path(
            Path::new("src/models_new/customers.sql"),
            &["src/models".to_string()],
        );
        assert_eq!(result5, PathBuf::from("src/models_new/customers.sql"));
    }
}
