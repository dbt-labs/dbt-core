use dbt_common::io_args::IoArgs;
use dbt_common::{FsResult, io_utils::try_read_yml_to_str, unexpected_fs_err};
use dbt_jinja_utils::{
    jinja_environment::JinjaEnvironment,
    phases::{load::RenderProjectScope, parse::build_resolve_context},
    serde::from_yaml_jinja,
};
use dbt_schemas::schemas::project::DbtProject;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

pub fn load_project_yml(
    io_args: &IoArgs,
    env: &mut JinjaEnvironment<'static>,
    dbt_project_path: &Path,
    cli_vars: BTreeMap<String, dbt_serde_yaml::Value>,
) -> FsResult<DbtProject> {
    let dbt_project_render_scope = RenderProjectScope::new(env, cli_vars);
    let template = try_read_yml_to_str(dbt_project_path)?;

    let context = build_resolve_context(
        "dbt_project.yml",
        "dbt_project.yml",
        &BTreeMap::new(),
        BTreeMap::new(),
    );

    // Parse the template without vars using Jinja
    let mut dbt_project: DbtProject = from_yaml_jinja(
        Some(io_args),
        &template,
        false,
        dbt_project_render_scope.jinja_env,
        &context,
        &[],
        Some(dbt_project_path),
    )?;

    // Set default model paths if not specified
    fill_default(&mut dbt_project.analysis_paths, "analyses");
    fill_default(&mut dbt_project.asset_paths, "assets");
    fill_default(&mut dbt_project.macro_paths, "macros");
    fill_default(&mut dbt_project.model_paths, "models");
    fill_default(&mut dbt_project.seed_paths, "seeds");
    fill_default(&mut dbt_project.snapshot_paths, "snapshots");
    fill_default(&mut dbt_project.test_paths, "tests");

    // We need to add the generic test paths for each test path defined in the project
    for test_path in dbt_project.test_paths.as_deref().unwrap_or_default() {
        let path = PathBuf::from(test_path);
        dbt_project
            .macro_paths
            .as_mut()
            .ok_or(unexpected_fs_err!("Macro paths should exist"))?
            .push(path.join("generic").to_string_lossy().to_string());
    }

    if dbt_project.clean_targets.is_none() {
        dbt_project.clean_targets = Some(vec![])
    }

    Ok(dbt_project)
}

fn fill_default(paths: &mut Option<Vec<String>>, default: &str) {
    if paths.as_ref().is_none_or(|v| v.is_empty()) {
        *paths = Some(vec![default.to_string()]);
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
