use dbt_common::tracing::emit::emit_warn_log_message;
use dbt_common::{ErrorCode, FsResult, stdfs};
use dbt_schemas::schemas::packages::{
    DbtPackageLock, DbtPackages, DbtPackagesLock, GitPackageLock, HubPackageLock, LocalPackageLock,
    PackageVersion, PrivatePackageLock, TarballPackageLock,
};
use std::collections::HashSet;

use crate::{
    git_client::download_git_like_package,
    package_listing::UnpinnedPackage,
    tarball_client::download_tarball_package,
    types::{GitPinnedPackage, LocalPinnedPackage, PrivatePinnedPackage, TarballPinnedPackage},
    utils::{ensure_dir, fusion_sha1_hash_packages, make_tempdir, read_and_validate_dbt_project},
};

use crate::package_listing::PackageListing;

use crate::context::DepsOperationContext;

use super::load_dbt_packages;

pub async fn compute_package_lock(
    ctx: &DepsOperationContext<'_>,
    dbt_packages: &DbtPackages,
) -> FsResult<DbtPackagesLock> {
    let sha1_hash = fusion_sha1_hash_packages(
        &dbt_packages.packages,
        ctx.use_v2_compatible_package_downloads,
    );
    // First step, is to flatten into a single list of packages
    let mut dbt_packages_lock = DbtPackagesLock::default();
    let mut package_listing = PackageListing::new(ctx.io.clone(), ctx.vars.clone())
        .with_skip_private_deps(ctx.skip_private_deps);
    package_listing.hydrate_dbt_packages(dbt_packages, ctx.jinja_env)?;
    let mut final_listing = PackageListing::new(ctx.io.clone(), ctx.vars.clone())
        .with_skip_private_deps(ctx.skip_private_deps);
    resolve_packages(ctx, &mut final_listing, &mut package_listing).await?;
    for package in final_listing.packages.values() {
        match package {
            UnpinnedPackage::Hub(hub_unpinned_package) => {
                let pinned_package = hub_unpinned_package.resolved(&ctx.hub_registry).await?;
                dbt_packages_lock
                    .packages
                    .push(DbtPackageLock::Hub(HubPackageLock {
                        package: pinned_package.package,
                        name: pinned_package.name,
                        version: PackageVersion::String(pinned_package.version),
                    }));
            }
            UnpinnedPackage::Git(git_unpinned_package) => {
                let pinned_package: GitPinnedPackage = git_unpinned_package.clone().try_into()?;
                dbt_packages_lock
                    .packages
                    .push(DbtPackageLock::Git(GitPackageLock {
                        // Using the original entry to ensure that we preserve the original git url
                        // to be stored in the `package-lock.yml` file (despite this being horrible practice)
                        git: git_unpinned_package.original_entry.git.clone(),
                        name: pinned_package.name,
                        revision: pinned_package.revision,
                        warn_unpinned: pinned_package.warn_unpinned,
                        subdirectory: pinned_package.subdirectory,
                        __unrendered__: pinned_package.unrendered,
                    }));
            }
            UnpinnedPackage::Local(local_package) => {
                let pinned_package: LocalPinnedPackage = local_package.clone().try_into()?;
                dbt_packages_lock
                    .packages
                    .push(DbtPackageLock::Local(LocalPackageLock {
                        name: pinned_package.name,
                        local: stdfs::diff_paths(&local_package.local, &ctx.io.in_dir)?,
                    }));
            }
            UnpinnedPackage::Private(private_unpinned_package) => {
                let pinned_package: PrivatePinnedPackage =
                    private_unpinned_package.clone().try_into()?;
                dbt_packages_lock
                    .packages
                    .push(DbtPackageLock::Private(PrivatePackageLock {
                        // Using the original entry to ensure that we preserve the original git url
                        // to be stored in the `package-lock.yml` file (despite this being horrible practice)
                        private: private_unpinned_package.original_entry.private.clone(),
                        name: pinned_package.name,
                        provider: pinned_package.provider,
                        revision: pinned_package.revision,
                        warn_unpinned: pinned_package.warn_unpinned,
                        subdirectory: pinned_package.subdirectory,
                        __unrendered__: pinned_package.unrendered,
                    }));
            }
            UnpinnedPackage::Tarball(tarball_unpinned_package) => {
                let pinned_package: TarballPinnedPackage =
                    tarball_unpinned_package.clone().try_into()?;
                let mut unrendered = pinned_package.unrendered;
                // We remove the 'name' from unrendered so that we don't
                // end up with two 'name' fields in the package lock.
                unrendered.remove("name");
                dbt_packages_lock
                    .packages
                    .push(DbtPackageLock::Tarball(TarballPackageLock {
                        tarball: tarball_unpinned_package.original_entry.tarball.clone(),
                        name: pinned_package.name,
                        __unrendered__: unrendered,
                    }));
            }
        }
    }
    dbt_packages_lock.sha1_hash = sha1_hash;
    // Note: This is currently sorting by package name, but there's more to do here
    dbt_packages_lock.packages.sort_by_key(|a| a.package_name());
    // Deduplicate packages with the same project name, keeping only the first occurrence.
    // This handles cases where packages from different sources (e.g., calogica/dbt_date and
    // godatadriven/dbt_date) resolve to the same project name. dbt-core allows this and
    // deduplicates during resolution. We do the same here by removing duplicates.
    // Note: We intentionally do NOT error on duplicate package names here.
    // dbt-core handles duplicate package names by merging them during resolution
    // via the incorporate() method in the PackageListing. It only checks for
    // duplicate *project* names after fetching metadata, not duplicate package names.
    let mut seen = HashSet::new();
    dbt_packages_lock.packages.retain(|package| {
        let lookup_name = package.package_name();
        if seen.contains(&lookup_name) {
            // Warn about duplicate package - keeping the first occurrence
            emit_warn_log_message(
                ErrorCode::DepsFoundDuplicatePackage,
                format!(
                    "Duplicate package name '{}' found in dependencies. Keeping the first occurrence. \
                     This will be an error in a future version of Fusion.",
                    lookup_name
                ),
                ctx.io.status_reporter.as_ref(),
            );
            false
        } else {
            seen.insert(lookup_name);
            true
        }
    });
    Ok(dbt_packages_lock)
}

async fn resolve_packages(
    ctx: &DepsOperationContext<'_>,
    final_listing: &mut PackageListing,
    package_listing: &mut PackageListing,
) -> FsResult<()> {
    let mut next_listing = PackageListing::new(ctx.io.clone(), ctx.vars.clone())
        .with_skip_private_deps(package_listing.skip_private_deps);
    for unpinned_package in package_listing.packages.values_mut() {
        ctx.cancellation.check_cancellation()?;
        match unpinned_package {
            UnpinnedPackage::Hub(hub_unpinned_package) => {
                let pinned_package = hub_unpinned_package.resolved(&ctx.hub_registry).await?;
                let hub_package = ctx
                    .hub_registry
                    .get_hub_package(&pinned_package.package)
                    .await?;
                ctx.hub_registry
                    .check_package_deprecation(&package_listing.io_args, &hub_package);
                let metadata = hub_package
                    .versions
                    .get(&pinned_package.version)
                    .expect("Version should exist in package metadata");

                if std::env::var("NEXTEST").is_err() && ctx.version_check {
                    ctx.hub_registry.check_require_dbt_version(
                        &package_listing.io_args,
                        &hub_package.name,
                        metadata,
                    );
                }
                next_listing.update_from(&metadata.packages, ctx.jinja_env)?;
            }
            UnpinnedPackage::Git(git_unpinned_package) => {
                let tmp_dir = make_tempdir(None)?;
                let download_dir = tmp_dir.path().join("git_pkg");
                ensure_dir(&download_dir).await?;
                let (checkout_path, commit_sha) = download_git_like_package(
                    ctx,
                    &git_unpinned_package.git,
                    &git_unpinned_package.revisions,
                    &git_unpinned_package.subdirectory,
                    git_unpinned_package.warn_unpinned.unwrap_or_default(),
                    &download_dir,
                )
                .await?;
                git_unpinned_package.revisions = vec![commit_sha];
                let dbt_project = read_and_validate_dbt_project(
                    ctx.io,
                    &checkout_path,
                    true,
                    ctx.jinja_env,
                    ctx.vars,
                )?;
                git_unpinned_package.name = Some(dbt_project.name);
                if let Some(dbt_packages) = load_dbt_packages(ctx.io, &checkout_path)?.0 {
                    next_listing.update_from(&dbt_packages.packages, ctx.jinja_env)?;
                }
                // Keep tmp_dir alive until we're done with checkout_path
                drop(tmp_dir);
            }
            UnpinnedPackage::Local(local_unpinned_package) => {
                let (dbt_packages, _) = load_dbt_packages(ctx.io, &local_unpinned_package.local)?;
                if let Some(dbt_packages) = dbt_packages {
                    next_listing.update_from(&dbt_packages.packages, ctx.jinja_env)?;
                }
            }
            UnpinnedPackage::Private(private_unpinned_package) => {
                let tmp_dir = make_tempdir(None)?;
                let download_dir = tmp_dir.path().join("git_pkg");
                ensure_dir(&download_dir).await?;
                let (checkout_path, commit_sha) = download_git_like_package(
                    ctx,
                    &private_unpinned_package.private,
                    &private_unpinned_package.revisions,
                    &private_unpinned_package.subdirectory,
                    private_unpinned_package.warn_unpinned.unwrap_or_default(),
                    &download_dir,
                )
                .await?;
                private_unpinned_package.revisions = vec![commit_sha];
                let dbt_project = read_and_validate_dbt_project(
                    ctx.io,
                    &checkout_path,
                    true,
                    ctx.jinja_env,
                    ctx.vars,
                )?;
                private_unpinned_package.name = Some(dbt_project.name);
                if let Some(dbt_packages) = load_dbt_packages(ctx.io, &checkout_path)?.0 {
                    next_listing.update_from(&dbt_packages.packages, ctx.jinja_env)?;
                }
                // Keep tmp_dir alive until we're done with checkout_path
                drop(tmp_dir);
            }
            UnpinnedPackage::Tarball(tarball_unpinned_package) => {
                let tmp_dir = make_tempdir(None)?;
                let download_dir = tmp_dir.path().join("package");
                ensure_dir(&download_dir).await?;

                let checkout_path =
                    download_tarball_package(ctx, &tarball_unpinned_package.tarball, &download_dir)
                        .await?;
                let dbt_project = read_and_validate_dbt_project(
                    ctx.io,
                    &checkout_path,
                    true,
                    ctx.jinja_env,
                    ctx.vars,
                )?;
                tarball_unpinned_package.name = Some(dbt_project.name);
                if let Some(dbt_packages) = load_dbt_packages(ctx.io, &checkout_path)?.0 {
                    next_listing.update_from(&dbt_packages.packages, ctx.jinja_env)?;
                }
            }
        }
        final_listing.incorporate_unpinned_package(unpinned_package)?;
    }
    if !next_listing.packages.is_empty() {
        Box::pin(resolve_packages(ctx, final_listing, &mut next_listing)).await?;
    }
    Ok(())
}
