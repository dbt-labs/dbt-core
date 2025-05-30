use crate::{
    args::LoadArgs,
    dbt_project_yml_loader::{collect_protected_paths, load_project_yml},
    load,
};
use std::{collections::BTreeMap, path::Path, time::SystemTime};

use dbt_common::{
    constants::{DBT_PROJECT_YML, REMOVING},
    err, fs_err, fsinfo,
    io_args::{EvalArgs, IoArgs},
    show_error, show_progress, show_progress_exit, stdfs, ErrorCode, FsResult,
};
use dbt_jinja_utils::{
    invocation_args::InvocationArgs, phases::load::init::initialize_load_jinja_environment,
};

pub async fn execute_clean_command(arg: &EvalArgs, files: &[String]) -> FsResult<i32> {
    let start = SystemTime::now();

    let load_args = LoadArgs::from_eval_args(arg);
    let invocation_args = InvocationArgs::from_eval_args(arg);
    let (dbt_state, num_threads) = load(&load_args, &invocation_args).await?;
    let flags: BTreeMap<String, minijinja::Value> = invocation_args.to_dict();

    let arg = arg.with_threads(num_threads);

    let mut env = initialize_load_jinja_environment(
        &dbt_state.dbt_profile.profile,
        &dbt_state.dbt_profile.target,
        &dbt_state.dbt_profile.db_config.adapter_type(),
        &dbt_state.dbt_profile.db_config,
        dbt_state.run_started_at,
        &flags,
    )?;

    let dbt_project_path = arg.io.in_dir.join(DBT_PROJECT_YML);
    let dbt_project = load_project_yml(&arg.io, &mut env, &dbt_project_path, arg.vars.clone())?;

    let protected_paths = collect_protected_paths(&dbt_project)
        .iter()
        .map(|p| std::path::absolute(arg.io.in_dir.join(p)))
        .collect::<Result<Vec<_>, _>>()?;

    let mut paths_to_delete = dbt_project
        .clean_targets
        .as_ref()
        .unwrap()
        .iter()
        .chain(files.iter())
        .map(|path| {
            let path = Path::new(path);
            if path.is_absolute() {
                err!(
                    ErrorCode::InvalidPath,
                    "Absolute paths are not allowed: {}",
                    path.display()
                )
            } else {
                std::path::absolute(arg.io.in_dir.join(path)).map_err(Into::into)
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    paths_to_delete.push(arg.io.out_dir.clone());

    let all_safe = paths_to_delete.iter().all(|path_to_delete| {
        // The clean command does not delete anything outside of the project directory
        unrelated_paths(&arg.io, &arg.io.in_dir, path_to_delete)
            // The clean command does not delete protected directories ("models", "macros", etc.)
            && protected_paths
                .iter()
                .all(|protected_path| unrelated_paths(&arg.io, protected_path, path_to_delete))
    });

    if all_safe {
        paths_to_delete.iter().try_for_each(|path| {
            if path.exists() {
                let info = fsinfo!(REMOVING.into(), arg.io.format_display_path(path));
                show_progress!(&arg.io, info);
                stdfs::remove_dir_all(path)
            } else {
                log::trace!("The target directory does not exist: {}", path.display());
                Ok(())
            }
        })?;
    }

    show_progress_exit!(arg, start)
}

fn unrelated_paths<P: AsRef<Path>, Q: AsRef<Path>>(io: &IoArgs, to: P, from: Q) -> bool {
    match stdfs::diff_paths(&to, &from) {
        Ok(diff) => {
            // It is safe to delete a directory if the only way to get to a protected directory is to navigate to the parent.
            if diff.components().next() == Some(std::path::Component::ParentDir) {
                Ok(true)
            } else {
                let e = fs_err!(
                    ErrorCode::InvalidPath,
                    "The target directory is protected: {}",
                    from.as_ref().display()
                );
                show_error!(io, &e);
                Err(e)
            }
        }
        Err(e) => {
            show_error!(io, &e);
            Err(e)
        }
    }
    .is_ok()
}
