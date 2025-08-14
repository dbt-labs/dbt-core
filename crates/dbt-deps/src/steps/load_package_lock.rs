use dbt_common::io_args::IoArgs;
use dbt_common::{
    ErrorCode, FsResult,
    constants::{DBT_PACKAGES_LOCK_FILE, DBT_PROJECT_YML},
    err, fs_err,
    io_utils::try_read_yml_to_str,
    show_warning, stdfs,
};
use dbt_jinja_utils::serde::from_yaml_raw;
use dbt_schemas::schemas::project::DbtProjectNameOnly;
use dbt_schemas::schemas::{
    packages::{
        DbtPackageLock, DbtPackages, DbtPackagesLock, DeprecatedDbtPackageLock,
        DeprecatedDbtPackagesLock, GitPackageLock, HubPackageLock, LocalPackageLock,
    },
    project::DbtProject,
};
use std::{collections::HashSet, path::Path};

use crate::utils::sha1_hash_packages;

pub fn try_load_valid_dbt_packages_lock(
    io: &IoArgs,
    dbt_packages_dir: &Path,
    dbt_packages: &DbtPackages,
) -> FsResult<Option<DbtPackagesLock>> {
    let packages_lock_path = io.in_dir.join(DBT_PACKAGES_LOCK_FILE);
    let sha1_hash = sha1_hash_packages(&dbt_packages.packages);
    if packages_lock_path.exists() {
        let yml_str = try_read_yml_to_str(&packages_lock_path)?;
        let rendered_yml: DbtPackagesLock =
            match from_yaml_raw(io, &yml_str, Some(&packages_lock_path), true, None) {
                Ok(rendered_yml) => rendered_yml,
                Err(e) => {
                    if e.to_string()
                        .contains("not match any variant of untagged enum DbtPackageLock")
                    {
                        return try_load_from_deprecated_dbt_packages_lock(
                            io,
                            dbt_packages_dir,
                            &yml_str,
                        );
                    }
                    return err!(
                        ErrorCode::IoError,
                        "Failed to parse package-lock.yml file: {}",
                        e
                    );
                }
            };
        if rendered_yml.sha1_hash == sha1_hash {
            return Ok(Some(rendered_yml));
        }
    }
    Ok(None)
}

// This is a hack to support the old dbt_packages_lock.yml file format
// In the future, we should not support just checking for directory names
fn try_load_from_deprecated_dbt_packages_lock(
    io: &IoArgs,
    dbt_packages_dir: &Path,
    yml_str: &str,
) -> FsResult<Option<DbtPackagesLock>> {
    match from_yaml_raw::<DeprecatedDbtPackagesLock>(io, yml_str, None, true, None) {
        // Here, we need to do a fuzzy lookup on the old dbt_packages_lock.yml file
        Ok(DeprecatedDbtPackagesLock {
            packages: deprecated_packages,
            sha1_hash,
        }) => {
            show_warning!(
                io,
                fs_err!(
                    ErrorCode::FmtError,
                    "Found package-lock.yml file with out of date formatting, ignoring..."
                )
            );
            if !dbt_packages_dir.exists() {
                show_warning!(
                    io,
                    fs_err!(
                        ErrorCode::FmtError,
                        "Attempted to infer package name from package-lock.yml, but no packages directory found, skipping...",
                    )
                );
                return Ok(None);
            }

            // List directories in dbt_packages_dir
            let all_packages = match dbt_packages_dir.read_dir() {
                Ok(dir_entries) => dir_entries.collect::<Result<Vec<_>, _>>().map_err(|e| {
                    fs_err!(
                        ErrorCode::IoError,
                        "Failed to read package directory entries: {}",
                        e
                    )
                })?,
                Err(e) => {
                    show_warning!(
                        io,
                        fs_err!(
                            ErrorCode::IoError,
                            "Failed to read packages directory at {}: {}",
                            dbt_packages_dir.display(),
                            e
                        )
                    );
                    return Ok(None);
                }
            };

            let mut avail_packages = HashSet::new();
            for package in all_packages {
                let package_path = package.path();
                let package_name = package_path.file_name().unwrap().to_str().unwrap();
                avail_packages.insert(package_name.to_lowercase());
            }
            let mut packages = Vec::new();
            for package in deprecated_packages {
                match package {
                    DeprecatedDbtPackageLock::Hub(hub) => {
                        let package = hub.package;
                        let version = hub.version;
                        // Split package name and version (if "/" exists)
                        let parts: Vec<&str> = package.split('/').collect();
                        let package_name = parts.last().expect("Package name should exist");
                        if avail_packages.contains(package_name.to_lowercase().as_str()) {
                            packages.push(DbtPackageLock::Hub(HubPackageLock {
                                package: package.to_string(),
                                name: package_name.to_string(),
                                version,
                            }));
                        } else {
                            show_warning!(
                                io,
                                fs_err!(
                                    ErrorCode::FmtError,
                                    "Attempted to infer package name from package-lock.yml, but package {} not found in '{}', skipping...",
                                    package,
                                    dbt_packages_dir.display()
                                )
                            );
                            return Ok(None);
                        }
                    }
                    DeprecatedDbtPackageLock::Git(package) => {
                        let git = package.git;
                        let revision = package.revision;
                        let warn_unpinned = package.warn_unpinned;
                        let subdirectory = package.subdirectory;
                        let unrendered = package.__unrendered__;

                        let parts: Vec<&str> = git.split('/').collect();
                        let package_name = parts.last().expect("Package name should exist");
                        if avail_packages.contains(package_name.to_lowercase().as_str()) {
                            packages.push(DbtPackageLock::Git(GitPackageLock {
                                git: git.to_owned().into(),
                                name: package_name.to_string(),
                                revision,
                                warn_unpinned,
                                subdirectory,
                                __unrendered__: unrendered,
                            }));
                        } else {
                            show_warning!(
                                io,
                                fs_err!(
                                    ErrorCode::FmtError,
                                    "Attempted to infer package name from package-lock.yml, but package {} not found in '{}', skipping...",
                                    git,
                                    dbt_packages_dir.display()
                                )
                            );
                            return Ok(None);
                        }
                    }
                    DeprecatedDbtPackageLock::Local(local) => {
                        let local = local.local;
                        // Find the package name from the `dbt_project.yml` file located in the local package
                        let dbt_project_path =
                            if let Ok(dbt_project_path) = stdfs::diff_paths(&local, &io.in_dir) {
                                dbt_project_path
                            } else {
                                io.in_dir.join(&local)
                            };

                        let project_yml_file = dbt_project_path.join(DBT_PROJECT_YML);
                        let dbt_project_str = try_read_yml_to_str(&project_yml_file)?;

                        // Try to deserialize only the package name for error reporting,
                        // falling back to the path if deserialization fails
                        let dependency_package_name = from_yaml_raw::<DbtProjectNameOnly>(
                            io,
                            &dbt_project_str,
                            Some(&project_yml_file),
                            // Do not report errors twice. This
                            // parse is only an attempt to get the package name. All actual errors
                            // will be reported when we parse the full `DbtProject` below.
                            false,
                            None,
                        )
                        .map(|p| p.name)
                        .ok()
                        .unwrap_or(project_yml_file.to_string_lossy().to_string());

                        let dbt_project: DbtProject = from_yaml_raw(
                            io,
                            &dbt_project_str,
                            Some(&project_yml_file),
                            true,
                            // TODO: do we really want to hide errors from local packages?
                            // maybe we want to let these ones to show up as project errors?
                            Some(dependency_package_name.as_str()),
                        )?;
                        let package_name = dbt_project.name;
                        packages.push(DbtPackageLock::Local(LocalPackageLock {
                            name: package_name,
                            local,
                        }));
                    }
                }
            }
            Ok(Some(DbtPackagesLock {
                packages,
                sha1_hash,
            }))
        }
        Err(e) => {
            err!(
                ErrorCode::IoError,
                "Failed to parse package-lock.yml file: {}",
                e
            )
        }
    }
}
