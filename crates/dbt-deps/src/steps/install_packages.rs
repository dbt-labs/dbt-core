use dbt_common::io_args::IoArgs;
use dbt_common::io_utils::StatusReporter;
use dbt_common::pretty_string::{GREEN, RED};
use dbt_common::tracing::emit::{emit_info_log_message, emit_warn_log_message};
use dbt_common::tracing::formatters::deps::get_package_display_name;
use dbt_common::tracing::span_info::{
    SpanStatusRecorder as _, find_and_update_span_attrs, update_span_attrs,
};
use dbt_common::{
    ErrorCode, FsResult,
    constants::{DBT_PACKAGES_LOCK_FILE, INSTALLING},
    create_info_span, fs_err,
    pretty_string::BLUE,
    stdfs,
};
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_schemas::schemas::packages::DbtPackagesLock;
use dbt_telemetry::{DepsAllPackagesInstalled, DepsPackageInstalled, PackageType};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use tracing::{Instrument as _, Span};
use vortex_events::package_install_event;

use crate::package_listing::UnpinnedPackage;

use crate::{
    github_client::download_git_like_package,
    hub_client::HubClient,
    package_listing::PackageListing,
    tarball_client::TarballClient,
    utils::{move_dir, read_and_validate_dbt_project, sanitize_git_url},
};

/// Create a package installation span and report to status reporter
fn create_package_installed_span(
    package: &UnpinnedPackage,
    status_reporter: Option<&Arc<dyn StatusReporter + 'static>>,
) -> Span {
    let (package_name, package_type, package_url_or_path) = match package {
        UnpinnedPackage::Hub(hub_unpinned_package) => (
            Some(hub_unpinned_package.package.as_str()),
            PackageType::Hub,
            None,
        ),
        UnpinnedPackage::Git(git_unpinned_package) => (
            None,
            PackageType::Git,
            Some(sanitize_git_url(git_unpinned_package.git.as_str())),
        ),
        UnpinnedPackage::Local(local_unpinned_package) => (
            local_unpinned_package.name.as_deref(),
            PackageType::Local,
            Some(sanitize_git_url(
                &local_unpinned_package.local.to_string_lossy(),
            )),
        ),
        UnpinnedPackage::Private(private_unpinned_package) => (
            private_unpinned_package.name.as_deref(),
            PackageType::Private,
            Some(sanitize_git_url(&private_unpinned_package.private)),
        ),
        UnpinnedPackage::Tarball(tarball_unpinned_package) => (
            None,
            PackageType::Tarball,
            Some(sanitize_git_url(&tarball_unpinned_package.tarball)),
        ),
    };

    // Create the span attributes
    let attrs = DepsPackageInstalled::start(
        package_name.map(str::to_string),
        package_type,
        None,
        package_url_or_path,
    );

    // Format the message for the status reporter based on package type
    let message_detail = get_package_display_name(&attrs).unwrap_or("unknown");

    // Report to status reporter if available
    if let Some(reporter) = status_reporter {
        reporter.show_progress(INSTALLING, message_detail, None);
    }

    // Create and return the span
    create_info_span(attrs)
}

#[allow(clippy::cognitive_complexity)]
pub async fn install_packages(
    io_args: &IoArgs,
    vars: &BTreeMap<String, dbt_yaml::Value>,
    hub_registry: &HubClient,
    jinja_env: &JinjaEnv,
    dbt_packages_lock: &DbtPackagesLock,
    packages_install_path: &Path,
    skip_private_deps: bool,
) -> FsResult<()> {
    // Cleanup package-lock.yml
    let package_lock_str = dbt_yaml::to_string(&dbt_packages_lock).unwrap();
    // Create tmp dir for tarball
    let packages_lock_path = &io_args.in_dir.join(DBT_PACKAGES_LOCK_FILE);
    std::fs::write(packages_lock_path, &package_lock_str).map_err(|e| {
        fs_err!(
            ErrorCode::IoError,
            "Failed to write package-lock.yml file: {}",
            e,
        )
    })?;
    if packages_install_path.exists() {
        std::fs::remove_dir_all(packages_install_path).map_err(|e| {
            fs_err!(
                ErrorCode::IoError,
                "Failed to remove existing packages install dir: {}",
                e,
            )
        })?;
    }
    std::fs::create_dir_all(packages_install_path).map_err(|e| {
        fs_err!(
            ErrorCode::IoError,
            "Failed to create packages install dir: {}",
            e,
        )
    })?;
    if dbt_packages_lock.packages.is_empty() {
        return Ok(());
    }
    let mut package_listing = PackageListing::new(io_args.clone(), vars.clone())
        .with_skip_private_deps(skip_private_deps);

    // Collect fusion-schema-compat upgrade suggestions
    let mut fusion_compat_suggestions: Vec<(String, String, String)> = Vec::new();
    package_listing.hydrate_dbt_packages_lock(dbt_packages_lock, jinja_env)?;

    // Update telemetry with resolved package count
    find_and_update_span_attrs(|ev: &mut DepsAllPackagesInstalled| {
        ev.package_count = package_listing.packages.len() as u64
    });

    for package in package_listing.packages.values() {
        // Create span for overall package installation
        let pspan = create_package_installed_span(package, io_args.status_reporter.as_ref());

        if skip_private_deps && let UnpinnedPackage::Private(private_unpinned_package) = package {
            emit_info_log_message(format!(
                "Skipping private package {} due to --skip-private-deps flag",
                private_unpinned_package
                    .name
                    .as_ref()
                    .unwrap_or(&private_unpinned_package.private)
            ));
            continue;
        }

        // Install package within that span and capture result
        install_package(
            io_args,
            vars,
            hub_registry,
            jinja_env,
            packages_install_path,
            package,
            &mut fusion_compat_suggestions,
            &pspan,
        )
        .instrument(pspan.clone())
        .await
        .record_status(&pspan)?;

        // Finally update set the closing legacy dbt core code if we managed to install
        update_span_attrs(&pspan, |ev: &mut DepsPackageInstalled| {
            ev.dbt_core_event_code = "M016".to_string();
        });
    }

    // Display fusion-schema-compat upgrade suggestions at the end
    if !fusion_compat_suggestions.is_empty() {
        // TODO: we should not apply color at the site where we emit event. The "proper"
        // way would be to introduce a dedicated event type or error code and apply formatting
        // in tracing formatters.
        let suggestions: Vec<String> = fusion_compat_suggestions
            .iter()
            .map(|(name, current_version, latest_version)| {
                format!(
                    "   {} -> {}",
                    RED.apply_to(format!("{name}@{current_version}")),
                    GREEN.apply_to(latest_version)
                )
            })
            .collect();

        let msg = format!(
            "\n{} The following packages have fusion schema compatible versions available.\n{}\n",
            BLUE.apply_to("suggestion:"),
            suggestions.join("\n"),
        );

        emit_info_log_message(msg);
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn install_package(
    io_args: &IoArgs,
    vars: &BTreeMap<String, dbt_yaml::Value>,
    hub_registry: &HubClient,
    jinja_env: &JinjaEnv,
    packages_install_path: &Path,
    package: &UnpinnedPackage,
    fusion_compat_suggestions: &mut Vec<(String, String, String)>,
    pspan: &Span,
) -> FsResult<()> {
    match package {
        UnpinnedPackage::Hub(hub_unpinned_package) => {
            let pinned_package = hub_unpinned_package.resolved(hub_registry).await?;

            if pinned_package.version != pinned_package.version_latest
                && (std::env::var("NEXTEST").is_err()
                    || (std::env::var("NEXTEST").is_ok()
                        && std::env::var("TEST_DEPS_LATEST_VERSION").is_ok()))
            {
                emit_warn_log_message(
                    ErrorCode::DependencyWarning,
                    format!(
                        "Updated version available for {}@{}: {}",
                        pinned_package.name, pinned_package.version, pinned_package.version_latest,
                    ),
                    io_args.status_reporter.as_ref(),
                );
            }

            // Store resolved package version
            update_span_attrs(pspan, |ev: &mut DepsPackageInstalled| {
                ev.package_version = Some(pinned_package.version.clone());
            });

            // Check fusion-schema-compat and suggest upgrade if needed
            let hub_package = hub_registry
                .get_hub_package(&pinned_package.package)
                .await?;

            let metadata = hub_package
                .versions
                .get(&pinned_package.version)
                .expect("Version should exist in package metadata");

            // Collect fusion-schema-compat upgrade suggestions
            if metadata.fusion_schema_compat != Some(true)
                && hub_package.latest_fusion_schema_compat == Some(true)
                && (std::env::var("NEXTEST").is_err()
                    || (std::env::var("NEXTEST").is_ok()
                        && std::env::var("TEST_DEPS_LATEST_VERSION").is_ok()))
            {
                fusion_compat_suggestions.push((
                    pinned_package.name.clone(),
                    pinned_package.version.clone(),
                    pinned_package.version_latest.clone(),
                ));
            }

            let tarball_url = metadata.downloads.tarball.clone();
            let project_name = metadata.name.clone();
            let final_path = packages_install_path.join(&project_name);

            let tarball_client = TarballClient::new();
            tarball_client
                .download_and_extract_tarball(&tarball_url, &final_path, true, None)
                .await?;

            if io_args.send_anonymous_usage_stats {
                package_install_event(
                    io_args.invocation_id.to_string(),
                    pinned_package.name.clone(),
                    pinned_package.version.clone(),
                    "hub".to_string(),
                );
            }
        }
        UnpinnedPackage::Git(git_unpinned_package) => {
            let tmp_dir = tempfile::tempdir_in(packages_install_path)
                .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to create temp dir: {}", e))?;
            let download_dir = tmp_dir.path().join("git_pkg");
            let (checkout_path, commit_sha) = download_git_like_package(
                &git_unpinned_package.git,
                &git_unpinned_package.revisions,
                &git_unpinned_package.subdirectory,
                git_unpinned_package.warn_unpinned.unwrap_or_default(),
                &download_dir,
            )
            .await?;

            let dbt_project = read_and_validate_dbt_project(
                io_args,
                &checkout_path,
                // do not report warnings here, since it would have alerady been reported
                // during package resolution phase
                false,
                jinja_env,
                vars,
            )?;
            let project_name = dbt_project.name;

            // Update span with resolved package info
            update_span_attrs(pspan, |ev: &mut DepsPackageInstalled| {
                ev.package_name = Some(project_name.clone());
                ev.package_version = Some(commit_sha.clone());
            });

            move_dir(&checkout_path, &packages_install_path.join(&project_name)).await?;
            // Keep tmp_dir alive until we're done with checkout_path
            drop(tmp_dir);

            if io_args.send_anonymous_usage_stats {
                package_install_event(
                    io_args.invocation_id.to_string(),
                    project_name,
                    commit_sha,
                    "git".to_string(),
                );
            }
        }
        UnpinnedPackage::Local(local_unpinned_package) => {
            let package_path = &io_args.in_dir.join(&local_unpinned_package.local);
            let install_path =
                packages_install_path.join(local_unpinned_package.name.as_ref().unwrap());
            let relative_package_path = stdfs::diff_paths(package_path, packages_install_path)?;
            stdfs::symlink(&relative_package_path, &install_path)?;
            let package_name = local_unpinned_package
                .name
                .clone()
                .unwrap_or_else(|| package_path.display().to_string());

            if io_args.send_anonymous_usage_stats {
                package_install_event(
                    io_args.invocation_id.to_string(),
                    package_name,
                    "".to_string(),
                    "local".to_string(),
                );
            }
        }
        UnpinnedPackage::Private(private_unpinned_package) => {
            let tmp_dir = tempfile::tempdir_in(packages_install_path)
                .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to create temp dir: {}", e))?;
            let download_dir = tmp_dir.path().join("private_pkg");
            let (checkout_path, commit_sha) = download_git_like_package(
                &private_unpinned_package.private,
                &private_unpinned_package.revisions,
                &private_unpinned_package.subdirectory,
                private_unpinned_package.warn_unpinned.unwrap_or_default(),
                &download_dir,
            )
            .await?;
            let dbt_project = read_and_validate_dbt_project(
                io_args,
                &checkout_path,
                // do not report warnings here, since it would have alerady been reported
                // during package resolution phase
                false,
                jinja_env,
                vars,
            )?;
            let project_name = dbt_project.name;

            // Update span with resolved package info
            update_span_attrs(pspan, |ev: &mut DepsPackageInstalled| {
                ev.package_name = Some(project_name.clone());
                ev.package_version = Some(commit_sha.clone());
            });

            move_dir(&checkout_path, &packages_install_path.join(&project_name)).await?;
            // Keep tmp_dir alive until we're done with checkout_path
            drop(tmp_dir);
            let package_name = private_unpinned_package
                .name
                .clone()
                .unwrap_or_else(|| private_unpinned_package.private.clone());

            if io_args.send_anonymous_usage_stats {
                package_install_event(
                    io_args.invocation_id.to_string(),
                    package_name,
                    commit_sha,
                    "private".to_string(),
                );
            }
        }
        UnpinnedPackage::Tarball(tarball_unpinned_package) => {
            // Download and extract the tarball to a temp dir on the same filesystem as
            // packages_install_path so that the final rename is atomic (no cross-device move).
            let tmp_extract = tempfile::tempdir_in(packages_install_path)
                .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to create temp dir: {}", e))?;
            let extract_path = tmp_extract.path().join("package");

            let tarball_client = TarballClient::new();
            tarball_client
                .download_and_extract_tarball(
                    &tarball_unpinned_package.tarball,
                    &extract_path,
                    true,
                    None,
                )
                .await?;

            let dbt_project = read_and_validate_dbt_project(
                io_args,
                &extract_path,
                // do not report warnings here, since it would have alerady been reported
                // during package resolution phase
                false,
                jinja_env,
                vars,
            )?;
            let project_name = dbt_project.name;

            // Update span with resolved package info
            update_span_attrs(pspan, |ev: &mut DepsPackageInstalled| {
                ev.package_name = Some(project_name.clone());
                ev.package_version = Some("tarball".to_string());
            });

            move_dir(&extract_path, &packages_install_path.join(&project_name)).await?;

            if io_args.send_anonymous_usage_stats {
                package_install_event(
                    io_args.invocation_id.to_string(),
                    project_name,
                    "tarball".to_string(),
                    "tarball".to_string(),
                );
            }
        }
    };

    Ok(())
}
