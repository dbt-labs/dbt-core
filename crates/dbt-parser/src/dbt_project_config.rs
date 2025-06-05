//! Module defines the global project configuration, which is used to
//! load and propagate configuration properties from the root `dbt_project.yml`
//! to the individual model directories.

use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
};

use dbt_common::{
    fs_err, io_args::IoArgs, once_cell_vars::DISPATCH_CONFIG, show_warning, ErrorCode, FsResult,
};
use dbt_jinja_utils::{
    jinja_environment::JinjaEnvironment, phases::parse::build_resolve_context,
    serde::into_typed_with_jinja,
};
use dbt_schemas::{
    project_configs::ProjectConfigs,
    schemas::{
        common::DbtQuoting,
        manifest::DbtConfig,
        project::{
            DbtProject, ProjectDataTestConfig, ProjectModelConfig, ProjectSeedConfig,
            ProjectSnapshotConfig, ProjectSourceConfig, ProjectUnitTestConfig,
        },
    },
};
use dbt_serde_yaml::Value;

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
/// property of [DbtConfig] is the name of a directory, which may have
/// source files or apply additional configuration. Configuration precedence
/// is given to the most specific path configuration. All unspecified
/// configuration is inherited from the parent.
///
#[derive(Debug, Clone, Default)]
pub struct DbtProjectConfig {
    /// The root configuration (i.e. at the `dbt_project.yml` level or inherited from `profiles.yml`)
    pub config: DbtConfig,
    /// Child configuration applied by path part
    pub children: HashMap<String, DbtProjectConfig>,
}

impl DbtProjectConfig {
    /// Create a new [GlobalProjectConfig] from a default [DbtConfig] and the root dbt_project.yml [DbtProjectConfigs]
    pub fn try_new(
        io_args: &IoArgs,
        dbt_config: &DbtConfig,
        configs: &ProjectConfigs,
        env: &JinjaEnvironment<'static>,
        context: &BTreeMap<String, minijinja::Value>,
    ) -> FsResult<Self> {
        recur_build_dbt_project_config(io_args, dbt_config, configs, env, context)
    }

    /// Get the configuration for a ref path
    pub fn get_config_for_path(
        &self,
        ref_path: &Path,
        project_name: &str,
        resource_paths: &[String],
    ) -> &DbtConfig {
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
    pub fn with_config(&mut self, config: DbtConfig) {
        self.config = config;
    }
}

/// Recursively build the [GlobalProjectConfig] from a parent [DbtConfig] and a [DbtProjectConfigs]
pub fn recur_build_dbt_project_config(
    io_args: &IoArgs,
    parent_config: &DbtConfig,
    child: &ProjectConfigs,
    env: &JinjaEnvironment<'static>,
    context: &BTreeMap<String, minijinja::Value>,
) -> FsResult<DbtProjectConfig> {
    let mut child_config: DbtConfig = child.try_into()?;
    child_config.default_to(parent_config);
    let mut children = HashMap::new();
    for (key, childs_child) in child.additional_properties() {
        // do NOT recurse if childs_child is null or the key is a config, i.e. starts with a '+'
        if childs_child.is_null() || key.starts_with('+') {
            continue;
        }

        match childs_child {
            Value::String(_, span) | Value::Number(_, span) | Value::Bool(_, span) => {
                show_warning!(
                    io_args,
                    fs_err!(
                        code=>ErrorCode::InvalidArgument,
                        loc=>span.clone(),
                        "Unexpected config keys encountered in 'dbt_project.yml` for '{}'",
                        key
                    )
                );
                continue;
            }
            Value::Mapping(mapping, _span) => {
                let child: ProjectModelConfig = into_typed_with_jinja(
                    Some(io_args),
                    mapping.clone().into(),
                    true,
                    env,
                    context,
                    None,
                )?;
                children.insert(
                    key.clone(),
                    recur_build_dbt_project_config(
                        io_args,
                        &child_config,
                        &ProjectConfigs::ModelConfigs(&child),
                        env,
                        context,
                    )?,
                );
            }
            _ => {}
        }
        match child {
            ProjectConfigs::ModelConfigs(_) => {
                let child: ProjectModelConfig = into_typed_with_jinja(
                    Some(io_args),
                    childs_child.clone(),
                    true,
                    env,
                    &context,
                    None,
                )?;

                children.insert(
                    key.clone(),
                    recur_build_dbt_project_config(
                        io_args,
                        &child_config,
                        &ProjectConfigs::ModelConfigs(&child),
                        env,
                        context,
                    )?,
                );
            }
            ProjectConfigs::DataTestConfigs(_) => {
                let child: ProjectDataTestConfig = into_typed_with_jinja(
                    Some(io_args),
                    childs_child.clone(),
                    true,
                    env,
                    &context,
                    None,
                )?;

                // "Unexpected config keys encountered in 'dbt_project.yml` for 'data_tests' under path '{}': {:?}",
                children.insert(
                    key.clone(),
                    recur_build_dbt_project_config(
                        io_args,
                        &child_config,
                        &ProjectConfigs::DataTestConfigs(&child),
                        env,
                        context,
                    )?,
                );
            }
            ProjectConfigs::SeedConfigs(_) => {
                let child: ProjectSeedConfig = into_typed_with_jinja(
                    Some(io_args),
                    childs_child.clone(),
                    true,
                    env,
                    context,
                    None,
                )?;

                children.insert(
                    key.clone(),
                    recur_build_dbt_project_config(
                        io_args,
                        &child_config,
                        &ProjectConfigs::SeedConfigs(&child),
                        env,
                        context,
                    )?,
                );
            }
            ProjectConfigs::SnapshotConfigs(_) => {
                let child: ProjectSnapshotConfig = into_typed_with_jinja(
                    Some(io_args),
                    childs_child.clone(),
                    true,
                    env,
                    context,
                    None,
                )?;

                children.insert(
                    key.clone(),
                    recur_build_dbt_project_config(
                        io_args,
                        &child_config,
                        &ProjectConfigs::SnapshotConfigs(&child),
                        env,
                        context,
                    )?,
                );
            }
            ProjectConfigs::SourceConfigs(_) => {
                let child: ProjectSourceConfig = into_typed_with_jinja(
                    Some(io_args),
                    childs_child.clone(),
                    true,
                    env,
                    context,
                    None,
                )?;

                children.insert(
                    key.clone(),
                    recur_build_dbt_project_config(
                        io_args,
                        &child_config,
                        &ProjectConfigs::SourceConfigs(&child),
                        env,
                        context,
                    )?,
                );
            }

            ProjectConfigs::UnitTestConfigs(_) => {
                let child: ProjectUnitTestConfig = into_typed_with_jinja(
                    Some(io_args),
                    childs_child.clone(),
                    true,
                    env,
                    context,
                    None,
                )?;

                children.insert(
                    key.clone(),
                    recur_build_dbt_project_config(
                        io_args,
                        &child_config,
                        &ProjectConfigs::UnitTestConfigs(&child),
                        env,
                        context,
                    )?,
                );
            }
        };
    }
    Ok(DbtProjectConfig {
        config: child_config,
        children,
    })
}

/// Config wrapping propagated configs for the root project
pub struct RootProjectConfigs {
    /// Model configs
    pub models: DbtProjectConfig,
    /// Source configs
    pub sources: DbtProjectConfig,
    /// Snapshot configs
    pub snapshots: DbtProjectConfig,
    /// Seed configs
    pub seeds: DbtProjectConfig,
    /// Test configs
    pub tests: DbtProjectConfig,
    /// Unit test configs
    pub unit_tests: DbtProjectConfig,
}

/// Build the [RootProjectConfigs] from a [DbtProject]
pub fn build_root_project_configs(
    io_args: &IoArgs,
    root_project: &DbtProject,
    root_project_quoting: DbtQuoting,
    env: &JinjaEnvironment<'static>,
) -> FsResult<RootProjectConfigs> {
    let context = build_resolve_context(
        &root_project.name,
        &root_project.name,
        &BTreeMap::new(),
        DISPATCH_CONFIG.get().unwrap().read().unwrap().clone(),
    );
    let maybe_root_project_config = match (&root_project.tests, &root_project.data_tests) {
        (Some(_), Some(_)) => {
            unimplemented!("Merge logic for tests and data tests is unimplemented")
        }
        (Some(tests), None) => Some(ProjectConfigs::DataTestConfigs(tests)),
        (None, Some(data_tests)) => Some(ProjectConfigs::DataTestConfigs(data_tests)),
        (None, None) => None,
    };
    Ok(RootProjectConfigs {
        models: init_project_config(
            io_args,
            root_project_quoting,
            &root_project
                .models
                .as_ref()
                .map(ProjectConfigs::ModelConfigs),
            env,
            &context,
        )?,
        sources: init_project_config(
            io_args,
            root_project_quoting,
            &root_project
                .sources
                .as_ref()
                .map(ProjectConfigs::SourceConfigs),
            env,
            &context,
        )?,
        snapshots: init_project_config(
            io_args,
            root_project_quoting,
            &root_project
                .snapshots
                .as_ref()
                .map(ProjectConfigs::SnapshotConfigs),
            env,
            &context,
        )?,
        seeds: init_project_config(
            io_args,
            root_project_quoting,
            &root_project.seeds.as_ref().map(ProjectConfigs::SeedConfigs),
            env,
            &context,
        )?,
        tests: init_project_config(
            io_args,
            root_project_quoting,
            &maybe_root_project_config,
            env,
            &context,
        )?,
        unit_tests: init_project_config(
            io_args,
            root_project_quoting,
            &root_project
                .unit_tests
                .as_ref()
                .map(ProjectConfigs::UnitTestConfigs),
            env,
            &context,
        )?,
    })
}

/// generate the project config that will be inherited throughout the project
pub fn init_project_config(
    io_args: &IoArgs,
    package_quoting: DbtQuoting,
    dbt_project_configs: &Option<ProjectConfigs<'_>>,
    env: &JinjaEnvironment<'static>,
    context: &BTreeMap<String, minijinja::Value>,
) -> FsResult<DbtProjectConfig> {
    let global_config = DbtConfig {
        enabled: Some(true),
        // Language specific quoting
        quoting: Some(package_quoting),
        ..Default::default()
    };
    let project_config = if let Some(configs) = dbt_project_configs {
        DbtProjectConfig::try_new(io_args, &global_config, configs, env, context)?
    } else {
        let mut config = DbtProjectConfig::default();
        config.with_config(global_config);
        config
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
        let mut config = DbtProjectConfig::default();
        config.config.enabled = Some(true);

        // Add a child config for project "test_project"
        let mut project_config = DbtProjectConfig::default();
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
        let mut config = DbtProjectConfig::default();
        config.config.enabled = Some(true);

        // Add project config
        let mut project_config = DbtProjectConfig::default();
        project_config.config.enabled = Some(false);

        // Add example subdirectory config
        let mut example_config = DbtProjectConfig::default();
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
        let mut config = DbtProjectConfig::default();
        config.config.enabled = Some(true);

        // Add project config
        let mut project_config = DbtProjectConfig::default();
        project_config.config.enabled = Some(false);

        // Add example subdirectory config
        let mut example_config = DbtProjectConfig::default();
        example_config.config.enabled = Some(true);

        // Add file-specific config
        let mut file_config = DbtProjectConfig::default();
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
        let mut config = DbtProjectConfig::default();
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
        let mut config = DbtProjectConfig::default();
        config.config.enabled = Some(true);

        // Add project config
        let mut project_config = DbtProjectConfig::default();
        project_config.config.enabled = Some(false);

        // Add staging subdirectory config - now with enabled field set
        let mut staging_config = DbtProjectConfig::default();
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
        let mut config = DbtProjectConfig::default();
        config.config.enabled = Some(true);

        // Add project config with subdirectory
        let mut project_config = DbtProjectConfig::default();
        let mut models_config = DbtProjectConfig::default();
        let mut example_config = DbtProjectConfig::default();
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
        let mut config = DbtProjectConfig::default();
        config.config.enabled = Some(true);
        config.config.materialized = Some(dbt_schemas::schemas::common::DbtMaterialization::View);

        // Set up project structure like: my_project -> staging -> +materialized: table
        let mut project_config = DbtProjectConfig::default();
        project_config.config.enabled = Some(true);

        let mut staging_config = DbtProjectConfig::default();
        staging_config.config.materialized =
            Some(dbt_schemas::schemas::common::DbtMaterialization::Table);
        staging_config.config.enabled = Some(true);

        // Add specific model config
        let mut customers_config = DbtProjectConfig::default();
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
