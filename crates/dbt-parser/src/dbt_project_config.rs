//! Module defines the global project configuration, which is used to
//! load and propagate configuration properties from the root `dbt_project.yml`
//! to the individual model directories.

use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
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
    pub fn get_config_for_path(&self, ref_path: &Path, project_name: &str) -> &DbtConfig {
        let mut components = ref_path.components().rev().collect::<Vec<_>>();
        // Remove the first component, which is the root directory
        components.pop();
        let mut current_config = self;
        // If the project name is not in the children, return the root config
        if let Some(project_config) = current_config.children.get(project_name) {
            current_config = project_config;
            while let Some(component) = components.pop() {
                // look for filestem specific configs on the last component
                let component_str: String = if components.is_empty() {
                    ref_path.file_stem().unwrap().to_str().unwrap().to_string()
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
