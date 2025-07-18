use dbt_jinja_utils::jinja_environment::JinjaEnv;

use dbt_common::constants::{DBT_PROFILES_YML, LOADING};
use dbt_common::stdfs::canonicalize;
use dbt_common::{ErrorCode, FsResult, err, fs_err, fsinfo, show_progress, show_warning};

use pathdiff::diff_paths;
use std::path::PathBuf;

use dbt_schemas::schemas::project::DbtProjectSimplified;
use dbt_schemas::state::DbtProfile;

use dirs::home_dir;

use crate::args::LoadArgs;
use crate::utils::{coalesce, read_profiles_and_extract_db_config};
use serde::Serialize;

pub fn load_profiles<S: Serialize>(
    arg: &LoadArgs,
    raw_dbt_project: &DbtProjectSimplified,
    jinja_env: &JinjaEnv,
    ctx: &S,
) -> FsResult<DbtProfile> {
    // The profile name comes either from dbt_project.yml or the --profile arg.
    // If the profile is not specified in dbt_project, it's a warning, if --profile
    // is specified, if it's not specified, it's an error.
    let profile_str =
        get_profile_string(arg, arg.profile.as_ref(), raw_dbt_project.profile.as_ref())?;

    // TODO: Add Secret Renderer logic to profile renderer

    // Load Profiles From ~/.dbt/profiles.yml and the dbt_project_dir
    let has_dbt_cloud_config_defined = if let Some(dbt_cloud) = raw_dbt_project.dbt_cloud.as_ref() {
        dbt_cloud.project_id.is_some()
    } else {
        false
    };
    let profile_path = get_profile_path(arg, &arg.profiles_dir, has_dbt_cloud_config_defined)?;

    let abs_profile_path = canonicalize(&profile_path)?;
    let abs_in_dir = canonicalize(&arg.io.in_dir)?;
    let relative_profile_path = diff_paths(&abs_profile_path, &abs_in_dir).ok_or_else(|| {
        fs_err!(
            ErrorCode::IoError,
            "Failed to get relative path from profiles.yml to project directory"
        )
    })?;

    let show_path = if let Some(home_dir) = home_dir() {
        let home_dir = home_dir.join(".dbt");
        if abs_profile_path.starts_with(home_dir) {
            PathBuf::from("~/.dbt/profiles.yml")
        } else {
            relative_profile_path.clone()
        }
    } else {
        relative_profile_path.clone()
    };

    show_progress!(
        arg.io,
        fsinfo!(LOADING.into(), show_path.display().to_string())
    );

    // Load just the keys -> values from the profiles.yml file
    let dbt_target_override = &arg.target;
    let (target, db_config) = read_profiles_and_extract_db_config(
        &arg.io,
        dbt_target_override,
        jinja_env,
        ctx,
        &profile_str,
        profile_path,
    )?;

    // TODO: Certain databases enforce that database and schema are specified
    let database = coalesce(&[&db_config.get_database(), &Some("dbt".to_string())]).unwrap();
    let schema = coalesce(&[&db_config.get_schema(), &Some("pub".to_string())]).unwrap();

    Ok(DbtProfile {
        database,
        schema,
        profile: profile_str,
        target,
        db_config,
        relative_profile_path,
        threads: arg.threads,
    })
}

fn get_profile_string(
    arg: &LoadArgs,
    arg_profile_str: Option<&String>,
    proj_profile_str: Option<&String>,
) -> FsResult<String> {
    match (proj_profile_str, arg_profile_str) {
        (None, None) => {
            err!(
                ErrorCode::InvalidConfig,
                "No profile specified in dbt_project.yml"
            )
        }
        (None, Some(prof)) => {
            show_warning!(
                &arg.io,
                fs_err!(
                    ErrorCode::InvalidConfig,
                    "No profile specified in dbt_project.yml"
                )
            );
            Ok(prof.to_string())
        }
        (Some(proj_prof), None) => Ok(proj_prof.to_string()),
        (Some(_), Some(prof)) => Ok(prof.to_string()),
    }
}

fn get_profile_path(
    arg: &LoadArgs,
    dbt_profile_dir_override: &Option<PathBuf>,
    has_dbt_cloud_config_defined: bool,
) -> FsResult<PathBuf> {
    let dbt_cloud_not_supported_yet_message = if has_dbt_cloud_config_defined {
        "Cloud CLI credentials from `dbt_cloud.yml` are not yet supported."
    } else {
        ""
    };

    match dbt_profile_dir_override {
        Some(dbt_profile_dir_override) => {
            let maybe_profile_path = dbt_profile_dir_override.join(DBT_PROFILES_YML);
            if maybe_profile_path.exists() {
                Ok(maybe_profile_path)
            } else {
                err!(
                    ErrorCode::InvalidConfig,
                    "No profiles.yml found at `{}`. \n\n{} Try running without the --profiles-dir flag to check the default locations.",
                    maybe_profile_path.display(),
                    dbt_common::pretty_string::BLUE.apply_to("suggestion: ")
                )
            }
        }
        None => {
            let maybe_profile_path = arg.io.in_dir.join(DBT_PROFILES_YML);
            if maybe_profile_path.exists() {
                Ok(maybe_profile_path)
            } else if let Some(home_path) = home_dir() {
                let dbt_home_profile_path = home_path.join(".dbt").join(DBT_PROFILES_YML);
                if dbt_home_profile_path.exists() {
                    Ok(dbt_home_profile_path)
                } else {
                    err!(
                        ErrorCode::InvalidConfig,
                        "No profiles.yml found at `{}` or `{}`. \n\n{}Run `dbt init` to create a profiles.yml file and connect to your database. {}",
                        dbt_home_profile_path.display(),
                        maybe_profile_path.display(),
                        dbt_common::pretty_string::BLUE.apply_to("suggestion: "),
                        dbt_cloud_not_supported_yet_message
                    )
                }
            } else {
                err!(
                    ErrorCode::InvalidConfig,
                    "No profiles.yml found in ~/.dbt, in project directory, or specified via --profiles-dir. \n\n{} Run `dbt init` to create a profiles.yml file and connect to your database. {}",
                    dbt_common::pretty_string::BLUE.apply_to("suggestion: "),
                    dbt_cloud_not_supported_yet_message
                )
            }
        }
    }
}
