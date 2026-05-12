use dbt_common::tracing::emit::{emit_info_progress_message, emit_warn_log_message};
use dbt_telemetry::ProgressMessage;

use dbt_common::stdfs::canonicalize;
use dbt_common::warn_error_options::WarnErrorOptions;
use dbt_common::{ErrorCode, FsResult, err, fs_err};

use dbt_yaml::{Span, Spanned};

use dbt_jinja_utils::register_base_functions;
use dbt_profile::{
    ProfileEnvironment, ProfileError, ResolvedProfile, find_profiles_path, resolve_with_env,
};
use dbt_schemas::schemas::profiles::DbConfig;
use dbt_schemas::schemas::serde::yaml_to_fs_error;

use pathdiff::diff_paths;
use std::path::PathBuf;

use dbt_schemas::schemas::project::DbtProjectSimplified;
use dbt_schemas::state::DbtProfile;

use dirs::home_dir;

use crate::args::LoadArgs;

const ALLOW_EXPERIMENTAL_ADAPTERS_ENV: &str = "DBT_ALLOW_EXPERIMENTAL_ADAPTERS";

pub fn load_profiles(
    arg: &LoadArgs,
    raw_dbt_project: &DbtProjectSimplified,
) -> FsResult<DbtProfile> {
    let profile = get_profile_with_span(arg.profile.as_ref(), raw_dbt_project.profile.clone())?;

    // Locate profiles.yml via dbt-profile's standard search order:
    // --profiles-dir (exclusive) > CWD > ~/.dbt/
    let profile_path = find_profiles_path(arg.profiles_dir.as_deref())
        .map_err(|e| fs_err!(ErrorCode::InvalidConfig, "{}", e))?;

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

    emit_info_progress_message(
        ProgressMessage::new_from_action_and_target(
            "Loading".to_string(),
            show_path.display().to_string(),
        ),
        arg.io.status_reporter.as_ref(),
    );

    let profile_name = profile.clone().into_inner();

    // Resolve the profile using dbt-profile's Jinja environment, plus the same base
    // functions as full dbt Jinja (`tojson`, `fromjson`, etc.) so profiles.yml matches dbt-core.
    let mut penv = ProfileEnvironment::new(arg.vars.clone());
    register_base_functions(&mut penv.env, arg.io.clone(), WarnErrorOptions::default());
    let resolved: ResolvedProfile =
        resolve_with_env(&penv, &profile_path, &profile_name, arg.target.as_deref()).map_err(
            |e| match e {
                ProfileError::Yaml { source, path } => yaml_to_fs_error(source, Some(&path)),
                ProfileError::ProfileMissing { .. } => fs_err!(
                    code => ErrorCode::IoError,
                    loc => profile.span().clone(),
                    "Profile '{}' not found in profiles.yml",
                    profile_name
                ),
                _ => fs_err!(ErrorCode::InvalidConfig, "{}", e),
            },
        )?;

    // Convert the rendered credentials mapping into a typed DbConfig
    let credentials_value = dbt_yaml::Value::Mapping(resolved.credentials, Span::default());
    let db_config: DbConfig = dbt_yaml::from_value(credentials_value).map_err(|e| {
        fs_err!(
            ErrorCode::InvalidConfig,
            "Failed to parse profiles.yml: {}",
            e
        )
    })?;

    let adapter = db_config.adapter_type();
    if !experimental_adapters_allowed()
        && !dbt_adapter_core::NON_EXPERIMENTAL_ADAPTERS.contains(&adapter)
    {
        return Err(fs_err!(
            ErrorCode::InvalidConfig,
            "The '{}' adapter is not yet supported by dbt Fusion. \
             Supported adapters: snowflake, bigquery, databricks, redshift",
            adapter
        ));
    }

    if db_config.has_removed_execute_field() {
        emit_warn_log_message(
            ErrorCode::DeprecatedOption,
            "The `execute:` field in profiles.yml is no longer supported and will be ignored. \
             Use the `--compute inline|sidecar|service|remote` CLI flag instead. \
             Please remove `execute:` from your profile.",
            arg.io.status_reporter.as_ref(),
        );
    }

    let database = db_config.get_database_or_default();
    let schema = db_config
        .get_schema()
        .map(String::as_str)
        .unwrap_or("public")
        .to_string();

    Ok(DbtProfile {
        database,
        schema,
        profile: profile.into_inner(),
        target: resolved.target_name,
        db_config,
        relative_profile_path,
        threads: arg.threads,
    })
}

fn experimental_adapters_allowed() -> bool {
    if cfg!(debug_assertions) && std::env::var_os(ALLOW_EXPERIMENTAL_ADAPTERS_ENV).is_none() {
        true
    } else {
        !dbt_env::env_var_is_disabled(ALLOW_EXPERIMENTAL_ADAPTERS_ENV)
    }
}

/// Resolve the profile name to use.
fn get_profile_with_span(
    arg_profile: Option<&String>,
    proj_profile: Spanned<Option<String>>,
) -> FsResult<Spanned<String>> {
    match (proj_profile.as_ref(), arg_profile) {
        (None, None) => {
            err!(
                ErrorCode::InvalidConfig,
                "No profile specified in dbt_project.yml"
            )
        }
        (None, Some(prof)) | (Some(_), Some(prof)) => Ok(Spanned::new(prof.to_string())
            .map_span(|_| Span::default().with_filename(PathBuf::from("<cmdline>")))),
        (Some(_), None) => Ok(proj_profile.map(|x| x.unwrap())),
    }
}

#[cfg(test)]
mod tests {
    use dbt_common::io_args::IoArgs;
    use dbt_common::warn_error_options::WarnErrorOptions;
    use dbt_jinja_utils::register_base_functions;
    use dbt_profile::ProfileEnvironment;

    #[test]
    fn loader_registers_tojson_function_on_profile_env() {
        let mut penv = ProfileEnvironment::new(Default::default());
        register_base_functions(
            &mut penv.env,
            IoArgs::default(),
            WarnErrorOptions::default(),
        );
        let out = penv
            .env
            .render_str("{{ tojson({'a': 1}) }}", &penv.ctx, &[])
            .expect("tojson should be registered for loader profile resolution");
        assert!(out.contains("\"a\""), "unexpected tojson output: {out}");
    }
}
