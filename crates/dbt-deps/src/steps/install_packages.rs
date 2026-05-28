use dbt_common::io_utils::StatusReporter;
use dbt_common::pretty_string::{GREEN, RED};
use dbt_common::tracing::emit::emit_info_log_message;
use dbt_common::tracing::formatters::deps::get_package_display_name;
use dbt_common::tracing::span_info::{
    SpanStatusRecorder as _, find_and_update_span_attrs, update_span_attrs,
};
use dbt_common::{
    ErrorCode, FsResult,
    constants::{DBT_PACKAGES_LOCK_FILE, INSTALLING},
    create_info_span, fs_err,
    pretty_string::BLUE,
    stdfs, tokiofs,
};
use dbt_schemas::schemas::packages::{DbtPackageLock, DbtPackagesLock};
use dbt_telemetry::{DepsAllPackagesInstalled, DepsPackageInstalled, PackageType};
use dbt_yaml::Verbatim;
use std::path::Path;
use std::sync::Arc;
use tracing::{Instrument as _, Span};
use vortex_events::package_install_event;

use crate::package_listing::UnpinnedPackage;

use crate::context::DepsOperationContext;
use crate::{
    git_client::install_git_like_package,
    package_listing::PackageListing,
    utils::{
        ensure_dir, make_tempdir, move_dir, read_and_validate_dbt_project, sanitize_git_url,
        scrub_package_name_secret_env_vars,
    },
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

fn package_lock_needs_scrub(package: &DbtPackageLock) -> bool {
    match package {
        DbtPackageLock::Git(git_package_lock) => {
            scrub_package_name_secret_env_vars(git_package_lock.git.as_str()).is_some()
        }
        DbtPackageLock::Tarball(tarball_package_lock) => {
            scrub_package_name_secret_env_vars(tarball_package_lock.tarball.as_str()).is_some()
        }
        _ => false,
    }
}

fn scrub_package_lock_for_file(dbt_packages_lock: &mut DbtPackagesLock) {
    for package in dbt_packages_lock.packages.iter_mut() {
        match package {
            DbtPackageLock::Git(git_package_lock) => {
                if let Some(scrubbed) =
                    scrub_package_name_secret_env_vars(git_package_lock.git.as_str())
                {
                    git_package_lock.git = Verbatim::from(scrubbed.into_owned());
                }
            }
            DbtPackageLock::Tarball(tarball_package_lock) => {
                if let Some(scrubbed) =
                    scrub_package_name_secret_env_vars(tarball_package_lock.tarball.as_str())
                {
                    tarball_package_lock.tarball = Verbatim::from(scrubbed.into_owned());
                }
            }
            _ => {}
        }
    }
}

pub async fn install_packages(
    ctx: &DepsOperationContext<'_>,
    dbt_packages_lock: &DbtPackagesLock,
    packages_install_path: &Path,
) -> FsResult<()> {
    // Cleanup package-lock.yml
    let package_lock_str = if dbt_packages_lock
        .packages
        .iter()
        .any(package_lock_needs_scrub)
    {
        let mut scrubbed_dbt_packages_lock = DbtPackagesLock {
            packages: dbt_packages_lock.packages.clone(),
            sha1_hash: dbt_packages_lock.sha1_hash.clone(),
        };
        scrub_package_lock_for_file(&mut scrubbed_dbt_packages_lock);
        dbt_yaml::to_string(&scrubbed_dbt_packages_lock).unwrap()
    } else {
        dbt_yaml::to_string(dbt_packages_lock).unwrap()
    };
    // Create tmp dir for tarball
    let packages_lock_path = &ctx.io.in_dir.join(DBT_PACKAGES_LOCK_FILE);
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
    let mut package_listing = PackageListing::new(ctx.io.clone(), ctx.vars.clone())
        .with_skip_private_deps(ctx.skip_private_deps);

    // Collect fusion-schema-compat upgrade suggestions
    let mut fusion_compat_suggestions: Vec<(String, String, String)> = Vec::new();
    package_listing.hydrate_dbt_packages_lock(dbt_packages_lock, ctx.jinja_env)?;

    // Update telemetry with resolved package count
    find_and_update_span_attrs(|ev: &mut DepsAllPackagesInstalled| {
        ev.package_count = package_listing.packages.len() as u64
    });

    for package in package_listing.packages.values() {
        // Create span for overall package installation
        let pspan = create_package_installed_span(package, ctx.io.status_reporter.as_ref());

        if ctx.skip_private_deps
            && let UnpinnedPackage::Private(private_unpinned_package) = package
        {
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
            ctx,
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

async fn install_package(
    ctx: &DepsOperationContext<'_>,
    packages_install_path: &Path,
    package: &UnpinnedPackage,
    fusion_compat_suggestions: &mut Vec<(String, String, String)>,
    pspan: &Span,
) -> FsResult<()> {
    match package {
        UnpinnedPackage::Hub(hub_unpinned_package) => {
            let pinned_package = hub_unpinned_package.resolved(&ctx.hub_registry).await?;

            if pinned_package.version != pinned_package.version_latest
                && (std::env::var("NEXTEST").is_err()
                    || (std::env::var("NEXTEST").is_ok()
                        && std::env::var("TEST_DEPS_LATEST_VERSION").is_ok()))
            {
                emit_info_log_message(format!(
                    "Updated version available for {}@{}: {}",
                    pinned_package.name, pinned_package.version, pinned_package.version_latest,
                ));
            }

            // Store resolved package version
            update_span_attrs(pspan, |ev: &mut DepsPackageInstalled| {
                ev.package_version = Some(pinned_package.version.clone());
            });

            // Check fusion-schema-compat and suggest upgrade if needed
            let hub_package = ctx
                .hub_registry
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

            // try to substitute fusion compatible version if requested
            let tarball_url = if ctx.use_v2_compatible_package_downloads
                && let Some(fusion_compatibility) = &metadata.fusion_compatibility
                && let Some(hub_fusion_compatible_download) =
                    &fusion_compatibility.fusion_compatible_download
                && let Some(fusion_compatible_download_url) =
                    &hub_fusion_compatible_download.tarball
            {
                emit_info_log_message(format!(
                    "Installing the v2-compatible download from Package Hub for {}@{}",
                    pinned_package.name, pinned_package.version,
                ));
                fusion_compatible_download_url.clone()
            } else {
                metadata.downloads.tarball.clone()
            };

            let project_name = metadata.name.clone();
            let final_path = packages_install_path.join(&project_name);
            ensure_dir(&final_path).await?;

            if let Err(e) = ctx
                .tarball_client
                .download_and_extract_tarball(&tarball_url, &final_path, true, None, &[])
                .await
            {
                let _ = tokiofs::remove_dir_all(&final_path).await;
                return Err(e);
            }

            if ctx.io.send_anonymous_usage_stats {
                package_install_event(
                    ctx.io.invocation_id.to_string(),
                    pinned_package.name.clone(),
                    pinned_package.version.clone(),
                    "hub".to_string(),
                );
            }
        }
        UnpinnedPackage::Git(git_unpinned_package) => {
            let tmp_dir = make_tempdir(Some(packages_install_path))?;
            let download_dir = tmp_dir.path().join("git_pkg");
            ensure_dir(&download_dir).await?;
            let sha = git_unpinned_package
                .revisions
                .last()
                .cloned()
                .unwrap_or_default();
            let (checkout_path, commit_sha) = install_git_like_package(
                ctx,
                &git_unpinned_package.git,
                &sha,
                &git_unpinned_package.subdirectory,
                &download_dir,
            )
            .await?;

            let dbt_project = read_and_validate_dbt_project(
                ctx.io,
                &checkout_path,
                // do not report warnings here, since it would have alerady been reported
                // during package resolution phase
                false,
                ctx.jinja_env,
                ctx.vars,
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

            if ctx.io.send_anonymous_usage_stats {
                package_install_event(
                    ctx.io.invocation_id.to_string(),
                    project_name,
                    commit_sha,
                    "git".to_string(),
                );
            }
        }
        UnpinnedPackage::Local(local_unpinned_package) => {
            let package_path = &ctx.io.in_dir.join(&local_unpinned_package.local);
            let install_path =
                packages_install_path.join(local_unpinned_package.name.as_ref().unwrap());
            let relative_package_path = stdfs::diff_paths(package_path, packages_install_path)?;
            stdfs::symlink(&relative_package_path, &install_path)?;
            let package_name = local_unpinned_package
                .name
                .clone()
                .unwrap_or_else(|| package_path.display().to_string());

            if ctx.io.send_anonymous_usage_stats {
                package_install_event(
                    ctx.io.invocation_id.to_string(),
                    package_name,
                    "".to_string(),
                    "local".to_string(),
                );
            }
        }
        UnpinnedPackage::Private(private_unpinned_package) => {
            let tmp_dir = make_tempdir(Some(packages_install_path))?;
            let download_dir = tmp_dir.path().join("private_pkg");
            ensure_dir(&download_dir).await?;
            let sha = private_unpinned_package
                .revisions
                .last()
                .cloned()
                .unwrap_or_default();
            let (checkout_path, commit_sha) = install_git_like_package(
                ctx,
                &private_unpinned_package.private,
                &sha,
                &private_unpinned_package.subdirectory,
                &download_dir,
            )
            .await?;
            let dbt_project = read_and_validate_dbt_project(
                ctx.io,
                &checkout_path,
                // do not report warnings here, since it would have alerady been reported
                // during package resolution phase
                false,
                ctx.jinja_env,
                ctx.vars,
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

            if ctx.io.send_anonymous_usage_stats {
                package_install_event(
                    ctx.io.invocation_id.to_string(),
                    package_name,
                    commit_sha,
                    "private".to_string(),
                );
            }
        }
        UnpinnedPackage::Tarball(tarball_unpinned_package) => {
            // Download and extract the tarball to a temp dir on the same filesystem as
            // packages_install_path so that the final rename is atomic (no cross-device move).
            let tmp_extract = make_tempdir(Some(packages_install_path))?;
            let extract_path = tmp_extract.path().join("package");
            ensure_dir(&extract_path).await?;

            ctx.tarball_client
                .download_and_extract_tarball(
                    &tarball_unpinned_package.tarball,
                    &extract_path,
                    true,
                    None,
                    &[],
                )
                .await?;

            let dbt_project = read_and_validate_dbt_project(
                ctx.io,
                &extract_path,
                // do not report warnings here, since it would have alerady been reported
                // during package resolution phase
                false,
                ctx.jinja_env,
                ctx.vars,
            )?;
            let project_name = dbt_project.name;

            // Update span with resolved package info
            update_span_attrs(pspan, |ev: &mut DepsPackageInstalled| {
                ev.package_name = Some(project_name.clone());
                ev.package_version = Some("tarball".to_string());
            });

            move_dir(&extract_path, &packages_install_path.join(&project_name)).await?;

            if ctx.io.send_anonymous_usage_stats {
                package_install_event(
                    ctx.io.invocation_id.to_string(),
                    project_name,
                    "tarball".to_string(),
                    "tarball".to_string(),
                );
            }
        }
    };

    Ok(())
}
