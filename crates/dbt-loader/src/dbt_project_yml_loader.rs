use dbt_common::io_args::IoArgs;
use dbt_common::tracing::emit::emit_warn_log_from_fs_error;
use dbt_common::{ErrorCode, fs_err};
use dbt_common::{FsResult, unexpected_fs_err};
use dbt_jinja_utils::serde::{into_typed_with_jinja, value_from_file};
use dbt_jinja_utils::var_fn;
use dbt_jinja_utils::{jinja_environment::JinjaEnv, phases::parse::build_resolve_context};
use dbt_schemas::schemas::project::DbtProject;
use dbt_schemas::schemas::project::{
    ProjectAnalysisConfig, ProjectDataTestConfig, ProjectExposureConfig, ProjectFunctionConfig,
    ProjectModelConfig, ProjectSeedConfig, ProjectSemanticModelConfig, ProjectSnapshotConfig,
    ProjectSourceConfig, ProjectUnitTestConfig,
};
use dbt_serde_yaml::{ShouldBe, Value as YmlValue};
use minijinja::Value;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

macro_rules! prune_section {
    ($proj:expr, $io:expr, $field:ident, $name:expr, $ty:ty) => {
        if let Some(cfg) = $proj.$field.as_mut() {
            prune_unexpected_nulls_in_section($io, $name, cfg, |c: &mut $ty| {
                &mut c.__additional_properties__
            });
        }
    };
}

fn prune_sections(io_args: &IoArgs, dbt_project: &mut DbtProject) {
    prune_section!(dbt_project, io_args, models, "models", ProjectModelConfig);
    prune_section!(dbt_project, io_args, seeds, "seeds", ProjectSeedConfig);
    prune_section!(
        dbt_project,
        io_args,
        snapshots,
        "snapshots",
        ProjectSnapshotConfig
    );
    prune_section!(
        dbt_project,
        io_args,
        sources,
        "sources",
        ProjectSourceConfig
    );
    prune_section!(dbt_project, io_args, tests, "tests", ProjectDataTestConfig);
    prune_section!(
        dbt_project,
        io_args,
        unit_tests,
        "unit_tests",
        ProjectUnitTestConfig
    );
    prune_section!(
        dbt_project,
        io_args,
        exposures,
        "exposures",
        ProjectExposureConfig
    );
    prune_section!(
        dbt_project,
        io_args,
        analyses,
        "analyses",
        ProjectAnalysisConfig
    );
    prune_section!(
        dbt_project,
        io_args,
        functions,
        "functions",
        ProjectFunctionConfig
    );
    prune_section!(
        dbt_project,
        io_args,
        semantic_models,
        "semantic-models",
        ProjectSemanticModelConfig
    );
}

fn prune_unexpected_nulls_in_children<T>(
    io_args: &IoArgs,
    section_name: &str,
    current_path: &str,
    cfg: &mut T,
    get_children_map: fn(&mut T) -> &mut BTreeMap<String, ShouldBe<T>>,
) {
    let children = get_children_map(cfg);

    // Collect keys to remove to avoid mutable iteration issues
    let mut keys_to_remove: Vec<String> = Vec::new();

    for (child_key, child_val) in children.iter_mut() {
        match child_val {
            ShouldBe::AndIs(child_cfg) => {
                let next_path = if current_path.is_empty() {
                    child_key.clone()
                } else {
                    format!("{}.{}", current_path, child_key)
                };
                prune_unexpected_nulls_in_children::<T>(
                    io_args,
                    section_name,
                    &next_path,
                    child_cfg,
                    get_children_map,
                );
            }
            ShouldBe::ButIsnt(..) => {
                // FIXME: We should always emit the original error from the
                // ShouldBe::ButIsnt, instead of making up a new one here
                if let Some(YmlValue::Null(span)) = child_val.as_ref_raw() {
                    let trimmed_key = child_key.trim();
                    let yaml_path = if current_path.is_empty() {
                        format!("{}.{}", section_name, trimmed_key)
                    } else {
                        format!("{}.{}.{}", section_name, current_path, trimmed_key)
                    };
                    let suggestion = if !trimmed_key.starts_with('+') {
                        format!(" Try '+{}' instead.", trimmed_key)
                    } else {
                        String::new()
                    };
                    let err = fs_err!(
                        code => ErrorCode::UnusedConfigKey,
                        loc => span.clone(),
                        "Ignored unexpected key '{}'.{} YAML path: '{}'.",
                        trimmed_key,
                        suggestion,
                        yaml_path
                    );
                    emit_warn_log_from_fs_error(&err, io_args.status_reporter.as_ref());
                    keys_to_remove.push(child_key.clone());
                }
            }
        }
    }

    for key in keys_to_remove {
        children.remove(&key);
    }
}

fn prune_unexpected_nulls_in_section<T>(
    io_args: &IoArgs,
    section_name: &str,
    section_cfg: &mut T,
    get_children_map: fn(&mut T) -> &mut BTreeMap<String, ShouldBe<T>>,
) {
    prune_unexpected_nulls_in_children(io_args, section_name, "", section_cfg, get_children_map);
}

pub fn load_project_yml(
    io_args: &IoArgs,
    env: &JinjaEnv,
    dbt_project_path: &Path,
    dependency_package_name: Option<&str>,
    cli_vars: BTreeMap<String, dbt_serde_yaml::Value>,
) -> FsResult<DbtProject> {
    let mut context = build_resolve_context(
        "dbt_project.yml",
        "dbt_project.yml",
        &BTreeMap::new(),
        BTreeMap::new(),
    );

    context.insert("var".to_string(), Value::from_function(var_fn(cli_vars)));

    // Parse the template without vars using Jinja
    let mut dbt_project: DbtProject = into_typed_with_jinja(
        io_args,
        value_from_file(io_args, dbt_project_path, true, dependency_package_name)?,
        false,
        env,
        &context,
        &[],
        dependency_package_name,
        true,
    )?;

    // Prune unexpected null keys (e.g. empty keys) early and emit warnings
    prune_sections(io_args, &mut dbt_project);

    // Set default model paths if not specified
    fill_default(&mut dbt_project.analysis_paths, &["analysis", "analyses"]);
    fill_default(&mut dbt_project.asset_paths, &["assets"]);
    fill_default(&mut dbt_project.function_paths, &["functions"]);
    fill_default(&mut dbt_project.macro_paths, &["macros"]);
    fill_default(&mut dbt_project.model_paths, &["models"]);
    fill_default(&mut dbt_project.seed_paths, &["seeds"]);
    fill_default(&mut dbt_project.snapshot_paths, &["snapshots"]);
    fill_default(&mut dbt_project.test_paths, &["tests"]);

    // We need to add the generic test paths for each test path defined in the project
    for test_path in dbt_project.test_paths.as_deref().unwrap_or_default() {
        let path = PathBuf::from(test_path);
        dbt_project
            .macro_paths
            .as_mut()
            .ok_or_else(|| unexpected_fs_err!("Macro paths should exist"))?
            .push(path.join("generic").to_string_lossy().to_string());
    }

    if dbt_project.clean_targets.is_none() {
        dbt_project.clean_targets = Some(vec![])
    }

    Ok(dbt_project)
}

fn fill_default(paths: &mut Option<Vec<String>>, defaults: &[&str]) {
    if paths.as_ref().is_none_or(|v| v.is_empty()) {
        *paths = Some(defaults.iter().map(|value| (*value).to_string()).collect());
    }
}

pub fn collect_protected_paths(dbt_project: &DbtProject) -> Vec<String> {
    let mut result: Vec<String> = vec![];

    result.extend_from_slice(dbt_project.analysis_paths.as_deref().unwrap_or_default());
    result.extend_from_slice(dbt_project.asset_paths.as_deref().unwrap_or_default());
    result.extend_from_slice(dbt_project.macro_paths.as_deref().unwrap_or_default());
    result.extend_from_slice(dbt_project.model_paths.as_deref().unwrap_or_default());
    result.extend_from_slice(dbt_project.seed_paths.as_deref().unwrap_or_default());
    result.extend_from_slice(dbt_project.snapshot_paths.as_deref().unwrap_or_default());
    result.extend_from_slice(dbt_project.test_paths.as_deref().unwrap_or_default());

    result
}
