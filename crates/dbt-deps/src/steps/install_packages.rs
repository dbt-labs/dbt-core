use dbt_common::io_args::IoArgs;
use dbt_common::stdfs::File;
use dbt_common::{constants::DBT_PACKAGES_LOCK_FILE, err, fs_err, stdfs, ErrorCode, FsResult};
use dbt_jinja_utils::jinja_environment::JinjaEnvironment;
use dbt_schemas::schemas::packages::DbtPackagesLock;
use flate2::read::GzDecoder;
use std::path::{Path, PathBuf};
use vortex_events::package_install_event;

use crate::package_listing::UnpinnedPackage;

use crate::{
    hub_client::HubClient,
    package_listing::PackageListing,
    utils::{handle_git_like_package, read_and_validate_dbt_project},
};

pub async fn install_packages(
    io_args: &IoArgs,
    hub_registry: &mut HubClient,
    jinja_env: &JinjaEnvironment<'static>,
    dbt_packages_lock: &DbtPackagesLock,
    packages_install_path: &Path,
) -> FsResult<()> {
    // Cleanup package-lock.yml
    let package_lock_str = dbt_serde_yaml::to_string(&dbt_packages_lock).unwrap();
    // Create tmp dir for tarball
    let packages_lock_path = &io_args.in_dir.join(DBT_PACKAGES_LOCK_FILE);
    std::fs::write(packages_lock_path, &package_lock_str).map_err(|e| {
        fs_err!(
            ErrorCode::IoError,
            "Failed to write package-lock.yml file: {}",
            e,
        )
    })?;
    let tarball_dir = tempfile::tempdir()
        .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to create temp dir: {}", e,))?;
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
    let mut package_listing = PackageListing::new(io_args.clone());
    package_listing.hydrate_dbt_packages_lock(dbt_packages_lock, jinja_env)?;

    for package in package_listing.packages.values() {
        match package {
            UnpinnedPackage::Hub(hub_unpinned_package) => {
                let pinned_package = hub_unpinned_package.resolved(hub_registry).await?;
                let version = pinned_package.get_version();
                let tar_name = format!("{}.{}.tar.gz", pinned_package.package, version);
                let tar_path = tarball_dir.path().join(tar_name);
                std::fs::create_dir_all(tar_path.parent().unwrap()).map_err(|e| {
                    fs_err!(ErrorCode::IoError, "Failed to create tarball dir: {}", e,)
                })?;
                let hub_package = hub_registry
                    .get_hub_package(&pinned_package.package)
                    .await?;
                let metadata = hub_package
                    .versions
                    .get(&pinned_package.version)
                    .expect("Version should exist in package metadata");
                let tarball_url = metadata.downloads.tarball.clone();
                let project_name = metadata.name.clone();
                // Download the tarball
                hub_registry
                    .download_tarball(&tarball_url, &tar_path)
                    .await?;
                // Extract the tarball
                let untar_path = tempfile::TempDir::new_in(packages_install_path).map_err(|e| {
                    fs_err!(ErrorCode::IoError, "Failed to create untar dir: {}", e)
                })?;
                // Reopen the tar file for extraction
                let tar = File::open(&tar_path)
                    .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to open tar file: {}", e))?;
                let gz = GzDecoder::new(tar);
                let mut tar = tar::Archive::new(gz);
                tar.unpack(&untar_path)
                    .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to unpack tar file: {}", e))?;
                if let Some(common_prefix) = get_common_prefix(&tar_path)? {
                    let rename_path = packages_install_path.join(project_name);
                    stdfs::rename(untar_path.path().join(&common_prefix), &rename_path)?;
                } else {
                    return err!(ErrorCode::IoError, "No common prefix for package found");
                }
                if io_args.send_anonymous_usage_stats {
                    package_install_event(
                        io_args.invocation_id.to_string(),
                        pinned_package.name.clone(),
                        pinned_package.version.clone(),
                        "hub".to_string(),
                    )
                    .await;
                }
            }
            UnpinnedPackage::Git(git_unpinned_package) => {
                let (tmp_dir, checkout_path, commit_sha) = handle_git_like_package(
                    &git_unpinned_package.git,
                    &git_unpinned_package.revisions,
                    &git_unpinned_package.subdirectory,
                    git_unpinned_package.warn_unpinned.unwrap_or_default(),
                )?;
                let dbt_project = read_and_validate_dbt_project(&checkout_path)?;
                let project_name = dbt_project.name;
                stdfs::rename(&checkout_path, packages_install_path.join(project_name))?;
                // Keep tmp_dir alive until we're done with checkout_path
                drop(tmp_dir);
                package_install_event(
                    io_args.invocation_id.to_string(),
                    git_unpinned_package
                        .name
                        .clone()
                        .unwrap_or("none".to_string()),
                    commit_sha,
                    "git".to_string(),
                )
                .await;
            }
            UnpinnedPackage::Local(local_unpinned_package) => {
                let package_path = &io_args.in_dir.join(&local_unpinned_package.local);
                let install_path =
                    packages_install_path.join(local_unpinned_package.name.as_ref().unwrap());
                let relative_package_path = stdfs::diff_paths(package_path, packages_install_path)?;
                stdfs::symlink(&relative_package_path, &install_path)?;
                package_install_event(
                    io_args.invocation_id.to_string(),
                    local_unpinned_package
                        .name
                        .clone()
                        .unwrap_or("none".to_string()),
                    "".to_string(),
                    "local".to_string(),
                )
                .await;
            }
            UnpinnedPackage::Private(private_unpinned_package) => {
                let (tmp_dir, checkout_path, commit_sha) = handle_git_like_package(
                    &private_unpinned_package.private,
                    &private_unpinned_package.revisions,
                    &private_unpinned_package.subdirectory,
                    private_unpinned_package.warn_unpinned.unwrap_or_default(),
                )?;
                let dbt_project = read_and_validate_dbt_project(&checkout_path)?;
                let project_name = dbt_project.name;
                stdfs::rename(&checkout_path, packages_install_path.join(project_name))?;
                // Keep tmp_dir alive until we're done with checkout_path
                drop(tmp_dir);
                package_install_event(
                    io_args.invocation_id.to_string(),
                    private_unpinned_package
                        .name
                        .clone()
                        .unwrap_or("none".to_string()),
                    commit_sha,
                    "private".to_string(),
                )
                .await;
            }
        }
    }
    Ok(())
}

fn get_common_prefix(tar_path: &Path) -> FsResult<Option<PathBuf>> {
    // Open the tarball file
    let tar = File::open(tar_path)
        .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to open tar file: {}", e,))?;
    let gz = GzDecoder::new(tar);
    let mut tar_archive = tar::Archive::new(gz);

    // Collect all paths in the tarball
    let mut paths = Vec::new();
    for entry in tar_archive
        .entries()
        .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get tarball entries: {}", e))?
    {
        let entry =
            entry.map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get tarball entry: {}", e))?;
        let path = entry.path().expect("Path should exist");
        if path == Path::new("pax_global_header") {
            continue;
        }
        paths.push(path.to_path_buf());
    }

    if paths.is_empty() {
        return Ok(None);
    }
    // Sort paths to prepare for finding the common prefix
    paths.sort();

    // Find the common prefix
    let first = &paths[0];
    let last = &paths[paths.len() - 1];
    let mut prefix = PathBuf::new();

    for (a, b) in first.components().zip(last.components()) {
        if a == b {
            prefix.push(a.as_os_str());
        } else {
            break;
        }
    }

    Ok(Some(prefix))
}
