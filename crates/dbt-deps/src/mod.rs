mod steps;

mod add_package;
mod github_client;
mod hub_client;
pub mod package_listing;
pub mod private_package;
pub mod semver;
mod tarball;
mod tarball_client;
pub mod types;
pub mod utils;

use dbt_common::cancellation::CancellationToken;
use dbt_common::constants::DBT_PROJECT_YML;
use dbt_common::create_info_span;
use dbt_common::io_args::{IoArgs, ReplayMode, TimeMachineMode};
use dbt_common::tracing::emit::emit_info_progress_message;
use dbt_common::tracing::span_info::{
    SpanStatusRecorder as _, record_span_status, record_span_status_with_attrs,
};
use dbt_common::{ErrorCode, FsResult, err, stdfs};
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_schemas::schemas::packages::{DbtPackagesLock, UpstreamProject};
use dbt_telemetry::{DepsAllPackagesInstalled, GenericOpExecuted};
use hub_client::{DBT_HUB_URL, HubClient};
use std::{collections::BTreeMap, path::Path};
use steps::{
    compute_package_lock, install_packages, load_dbt_packages,
    load_dbt_packages_lock_without_validation, try_load_valid_dbt_packages_lock,
};
use tracing::Instrument as _;

/// Loads and installs packages, and returns the packages lock and the dependencies map
#[allow(clippy::cognitive_complexity, clippy::too_many_arguments)]
pub async fn get_or_install_packages(
    io: &IoArgs,
    env: &JinjaEnv,
    packages_install_path: &Path,
    install_deps: bool,
    add_package: Option<String>,
    upgrade: bool,
    lock: bool,
    vars: BTreeMap<String, dbt_yaml::Value>,
    version_check: bool,
    skip_private_deps: bool,
    replay_mode: Option<&ReplayMode>,
    token: &CancellationToken,
) -> FsResult<(DbtPackagesLock, Vec<UpstreamProject>)> {
    // In Time Machine replay mode, skip all package fetching/installation
    // We should only load the existing package-lock.yml from disk
    let is_time_machine_replay = matches!(
        replay_mode,
        Some(ReplayMode::TimeMachine(TimeMachineMode::Replay(_)))
    );

    if is_time_machine_replay {
        // Just load the existing package-lock.yml without any validation or fetching
        let dbt_packages_lock =
            load_dbt_packages_lock_without_validation(io, packages_install_path, env, &vars)?
                .unwrap_or_default();

        emit_info_progress_message(
            dbt_telemetry::ProgressMessage::new_from_action_and_target(
                "Loading".to_string(),
                "package-lock.yml".to_string(),
            ),
            io.status_reporter.as_ref(),
        );

        // Return empty upstream projects since we're not fetching anything
        return Ok((dbt_packages_lock, vec![]));
    }

    let hub_url_from_env = std::env::var("DBT_PACKAGE_HUB_URL");
    let hub_url = hub_url_from_env
        .as_deref()
        .map(|s| {
            if s.ends_with('/') {
                // dbt-core required a trailing slash - here we support but do not require it.
                &s[0..s.len() - 1]
            } else {
                s
            }
        })
        .unwrap_or(DBT_HUB_URL);
    let mut hub_registry = HubClient::new(hub_url);

    // Add package first if specified, then load the package definition
    if let Some(add_package) = add_package {
        add_package::add_package(&add_package, &io.in_dir)?;
    }

    // This gets the package entries from packages.yml or dependencies.yml
    let (package_def, package_yml_name) = load_dbt_packages(io, &io.in_dir)?;

    let dbt_packages_lock = if let Some(ref dbt_packages) = package_def {
        if !upgrade
            && !lock
            && let Some(dbt_packages_lock) = try_load_valid_dbt_packages_lock(
                io,
                packages_install_path,
                dbt_packages,
                env,
                &vars,
            )?
        {
            emit_info_progress_message(
                dbt_telemetry::ProgressMessage::new_from_action_and_target(
                    "Loading".to_string(),
                    package_yml_name.to_string(),
                ),
                io.status_reporter.as_ref(),
            );
            dbt_packages_lock
        } else {
            let fetch_span = create_info_span(GenericOpExecuted::new(
                "deps-compute-package-lock".to_string(),
                format!("resolving packages from {}", package_yml_name),
                None,
            ));
            let lock_result = compute_package_lock(
                io,
                &vars,
                env,
                &mut hub_registry,
                dbt_packages,
                version_check,
                skip_private_deps,
                token,
            )
            .instrument(fetch_span.clone())
            .await;

            match lock_result {
                Ok(lock) => {
                    record_span_status_with_attrs(
                        &fetch_span,
                        |attrs| {
                            if let Some(g) = attrs.downcast_mut::<GenericOpExecuted>() {
                                g.item_count_total = Some(lock.packages.len() as u64);
                            }
                        },
                        None,
                    );
                    lock
                }
                Err(e) => {
                    record_span_status(&fetch_span, Some(&e.to_string()));
                    return Err(e);
                }
            }
        }
    } else {
        // No packages.yml defined - try to load from package-lock.yml if it exists
        // This matches dbt-core behavior where the lock file can be used independently

        // If upgrade flag is set but no packages.yml exists, we can't upgrade
        if upgrade {
            use dbt_common::tracing::emit::emit_warn_log_message;
            emit_warn_log_message(
                ErrorCode::InvalidConfig,
                "Cannot upgrade packages without packages.yml or dependencies.yml. Using existing package-lock.yml.",
                io.status_reporter.as_ref(),
            );
        }

        if let Some(dbt_packages_lock) =
            load_dbt_packages_lock_without_validation(io, packages_install_path, env, &vars)?
        {
            emit_info_progress_message(
                dbt_telemetry::ProgressMessage::new_from_action_and_target(
                    "Loading".to_string(),
                    "package-lock.yml".to_string(),
                ),
                io.status_reporter.as_ref(),
            );
            dbt_packages_lock
        } else {
            // No packages.yml and no valid package-lock.yml - return empty
            DbtPackagesLock::default()
        }
    };

    if install_deps && !lock && !dbt_packages_lock.packages.is_empty() {
        // Start install span. Note that actual package count may end up being less
        // then in the package lock due to package incorporation logic
        let install_span = create_info_span(DepsAllPackagesInstalled::start(
            dbt_packages_lock.packages.len() as u64,
        ));

        install_span.in_scope(|| {
            // check if the packages install path exists
            if !packages_install_path.exists() {
                // Create the directory
                stdfs::create_dir_all(packages_install_path).unwrap();
            }
        });

        install_packages(
            io,
            &vars,
            &mut hub_registry,
            env,
            &dbt_packages_lock,
            packages_install_path,
            skip_private_deps,
        )
        .instrument(install_span.clone())
        .await
        .record_status(&install_span)?;
    }

    // A package is considered "missing" if the 'dbt_project.yml' file for that
    // package does not exist.
    let mut missing_packages = Vec::new();
    if !lock {
        for package in dbt_packages_lock.packages.iter() {
            if !packages_install_path
                .join(package.package_name())
                .join(DBT_PROJECT_YML)
                .exists()
            {
                missing_packages.push(package.package_name());
            }
        }
    }
    let mut missing_packages_after_auto_install = Vec::new();

    // Auto install missing packages if not installing deps
    if !lock && !missing_packages.is_empty() {
        if !install_deps {
            // Start install span. Note that actual package count may end up being less
            // then in the package lock due to package incorporation logic
            let install_span = create_info_span(DepsAllPackagesInstalled::start(
                dbt_packages_lock.packages.len() as u64,
            ));

            install_span.in_scope(|| {
                // check if the packages install path exists
                if !packages_install_path.exists() {
                    // Create the directory
                    stdfs::create_dir_all(packages_install_path).unwrap();
                }
            });

            install_packages(
                io,
                &vars,
                &mut hub_registry,
                env,
                &dbt_packages_lock,
                packages_install_path,
                skip_private_deps,
            )
            .instrument(install_span.clone())
            .await
            .record_status(&install_span)?;

            for package in dbt_packages_lock.packages.iter() {
                if !packages_install_path.join(package.package_name()).exists() {
                    missing_packages_after_auto_install.push(package.package_name());
                }
            }
            if !missing_packages_after_auto_install.is_empty() {
                return err!(
                    ErrorCode::InvalidConfig,
                    "The following packages are missing from the packages install path: {:?}. Check you package definition and run 'fs deps' to install the missing packages.",
                    missing_packages_after_auto_install.join(", ")
                );
            }
        } else {
            return err!(
                ErrorCode::InvalidConfig,
                "The following packages are missing from the packages install path: {:?}. Check you package definition and run 'fs deps' to install the missing packages.",
                missing_packages.join(", ")
            );
        }
    }

    Ok((
        dbt_packages_lock,
        package_def.map(|p| p.projects).unwrap_or_default(),
    ))
}
