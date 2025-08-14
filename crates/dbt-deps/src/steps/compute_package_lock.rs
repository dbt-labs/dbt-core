use dbt_common::cancellation::CancellationToken;
use dbt_common::io_args::IoArgs;
use dbt_common::{ErrorCode, FsResult, err, fs_err, stdfs};
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_schemas::schemas::packages::{
    DbtPackageLock, DbtPackages, DbtPackagesLock, GitPackageLock, HubPackageLock, LocalPackageLock,
    PackageVersion, PrivatePackageLock, TarballPackageLock,
};
use std::collections::{BTreeMap, HashSet};

use crate::{
    package_listing::UnpinnedPackage,
    tarball_client::TarballClient,
    types::{GitPinnedPackage, LocalPinnedPackage, PrivatePinnedPackage, TarballPinnedPackage},
    utils::{handle_git_like_package, read_and_validate_dbt_project, sha1_hash_packages},
};

use crate::{hub_client::HubClient, package_listing::PackageListing};

use super::load_dbt_packages;

pub async fn compute_package_lock(
    io: &IoArgs,
    vars: &BTreeMap<String, dbt_serde_yaml::Value>,
    jinja_env: &JinjaEnv,
    hub_registry: &mut HubClient,
    dbt_packages: &DbtPackages,
    token: &CancellationToken,
) -> FsResult<DbtPackagesLock> {
    let sha1_hash = sha1_hash_packages(&dbt_packages.packages);
    // First step, is to flatten into a single list of packages
    let mut dbt_packages_lock = DbtPackagesLock::default();
    let mut package_listing = PackageListing::new(io.clone(), vars.clone());
    package_listing.hydrate_dbt_packages(dbt_packages, jinja_env)?;
    let mut final_listing = PackageListing::new(io.clone(), vars.clone());
    hub_registry.hydrate_index().await?;
    resolve_packages(
        io,
        vars,
        hub_registry,
        &mut final_listing,
        &mut package_listing,
        jinja_env,
        token,
    )
    .await?;
    for package in final_listing.packages.values() {
        match package {
            UnpinnedPackage::Hub(hub_unpinned_package) => {
                let pinned_package = hub_unpinned_package.resolved(hub_registry).await?;
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
                        local: stdfs::diff_paths(&local_package.local, &io.in_dir)?,
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
    // Sanity check for duplicate package names
    let mut seen = HashSet::new();
    for package in dbt_packages_lock.packages.iter() {
        let lookup_name = package.package_name();
        if seen.contains(&lookup_name) {
            let conflict_package = dbt_packages_lock.get_by_name(&lookup_name).unwrap();
            return err!(
                ErrorCode::InvalidConfig,
                "Duplicate packages originating from conflicting package sources. Package '{}' has sources in '{}' and '{}'.",
                lookup_name,
                conflict_package.entry_type(),
                package.entry_type(),
            );
        }
        seen.insert(lookup_name);
    }
    Ok(dbt_packages_lock)
}

async fn resolve_packages(
    io: &IoArgs,
    vars: &BTreeMap<String, dbt_serde_yaml::Value>,
    hub_registry: &mut HubClient,
    final_listing: &mut PackageListing,
    package_listing: &mut PackageListing,
    jinja_env: &JinjaEnv,
    token: &CancellationToken,
) -> FsResult<()> {
    let mut next_listing = PackageListing::new(io.clone(), vars.clone());
    for unpinned_package in package_listing.packages.values_mut() {
        token.check_cancellation()?;
        match unpinned_package {
            UnpinnedPackage::Hub(hub_unpinned_package) => {
                let pinned_package = hub_unpinned_package.resolved(hub_registry).await?;
                let hub_package = hub_registry
                    .get_hub_package(&pinned_package.package)
                    .await?;
                let metadata = hub_package
                    .versions
                    .get(&pinned_package.version)
                    .expect("Version should exist in package metadata");
                next_listing.update_from(&metadata.packages, jinja_env)?;
            }
            UnpinnedPackage::Git(git_unpinned_package) => {
                let (tmp_dir, checkout_path, commit_sha) = handle_git_like_package(
                    &git_unpinned_package.git,
                    &git_unpinned_package.revisions,
                    &git_unpinned_package.subdirectory,
                    git_unpinned_package.warn_unpinned.unwrap_or_default(),
                    None,
                )?;
                git_unpinned_package.revisions = vec![commit_sha];
                let dbt_project = read_and_validate_dbt_project(io, &checkout_path, true)?;
                git_unpinned_package.name = Some(dbt_project.name);
                if let Some(dbt_packages) = load_dbt_packages(io, &checkout_path)?.0 {
                    next_listing.update_from(&dbt_packages.packages, jinja_env)?;
                }
                // Keep tmp_dir alive until we're done with checkout_path
                drop(tmp_dir);
            }
            UnpinnedPackage::Local(local_unpinned_package) => {
                let (dbt_packages, _) = load_dbt_packages(io, &local_unpinned_package.local)?;
                if let Some(dbt_packages) = dbt_packages {
                    next_listing.update_from(&dbt_packages.packages, jinja_env)?;
                }
            }
            UnpinnedPackage::Private(private_unpinned_package) => {
                let (tmp_dir, checkout_path, commit_sha) = handle_git_like_package(
                    &private_unpinned_package.private,
                    &private_unpinned_package.revisions,
                    &private_unpinned_package.subdirectory,
                    private_unpinned_package.warn_unpinned.unwrap_or_default(),
                    None,
                )?;
                private_unpinned_package.revisions = vec![commit_sha];
                let dbt_project = read_and_validate_dbt_project(io, &checkout_path, true)?;
                private_unpinned_package.name = Some(dbt_project.name);
                if let Some(dbt_packages) = load_dbt_packages(io, &checkout_path)?.0 {
                    next_listing.update_from(&dbt_packages.packages, jinja_env)?;
                }
                // Keep tmp_dir alive until we're done with checkout_path
                drop(tmp_dir);
            }
            UnpinnedPackage::Tarball(tarball_unpinned_package) => {
                // Download and extract the tarball
                let tarball_dir = tempfile::tempdir().map_err(|e| {
                    fs_err!(ErrorCode::IoError, "Failed to create temp dir: {}", e,)
                })?;
                let tar_path = tarball_dir
                    .path()
                    .join(tarball_unpinned_package.tarball.replace('/', "_"));
                let untar_path = tempfile::TempDir::new().map_err(|e| {
                    fs_err!(ErrorCode::IoError, "Failed to create untar dir: {}", e)
                })?;

                let mut tarball_client = TarballClient::new();
                tarball_client
                    .download_and_extract_tarball(
                        &tarball_unpinned_package.tarball,
                        &tar_path,
                        &untar_path,
                        "tarball_package",
                    )
                    .await?;

                // Find the extracted package directory
                let tar_contents = std::fs::read_dir(&untar_path).map_err(|e| {
                    fs_err!(
                        ErrorCode::IoError,
                        "Failed to read untarred directory: {}",
                        e
                    )
                })?;
                let tar_contents: Vec<_> = tar_contents
                    .filter_map(|entry| entry.ok())
                    .filter(|entry| entry.path().is_dir())
                    .collect();

                if tar_contents.len() != 1 {
                    return err!(
                        ErrorCode::InvalidConfig,
                        "Incorrect structure for package extracted from {}. The extracted package needs to follow the structure <package_name>/<package_contents>.",
                        tarball_unpinned_package.tarball
                    );
                }

                let checkout_path = tar_contents[0].path();
                let dbt_project = read_and_validate_dbt_project(io, &checkout_path, true)?;
                tarball_unpinned_package.name = Some(dbt_project.name);
                if let Some(dbt_packages) = load_dbt_packages(io, &checkout_path)?.0 {
                    next_listing.update_from(&dbt_packages.packages, jinja_env)?;
                }
            }
        }
        final_listing.incorporate_unpinned_package(unpinned_package)?;
    }
    if !next_listing.packages.is_empty() {
        Box::pin(resolve_packages(
            io,
            vars,
            hub_registry,
            final_listing,
            &mut next_listing,
            jinja_env,
            token,
        ))
        .await?;
    }
    Ok(())
}
