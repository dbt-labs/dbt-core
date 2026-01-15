use dbt_common::adapter::AdapterType;
use dbt_common::cancellation::CancellationToken;
use dbt_common::tracing::emit::emit_warn_log_message;
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use indexmap::IndexMap;
use sha2::{Digest, Sha256};
use walkdir::WalkDir;

use std::collections::{BTreeMap, HashSet};
use std::io::Read;
use std::path::{Path, PathBuf};

use dbt_common::constants::DBT_PROJECT_YML;

use dbt_common::stdfs;

use dbt_common::err;
use dbt_common::{ErrorCode, FsResult, fs_err};
use dbt_schemas::state::{DbtPackage, DbtProfile, DbtVars};

use crate::args::LoadArgs;
use crate::loader::load_inner;

mod assets {
    #![allow(clippy::disallowed_methods)] // RustEmbed generates calls to std::path::Path::canonicalize

    use rust_embed::RustEmbed;

    #[derive(RustEmbed)]
    #[folder = "src/dbt_macro_assets/"]
    pub struct MacroAssets;
}

pub async fn load_packages(
    arg: &LoadArgs,
    env: &JinjaEnv,
    dbt_profile: &DbtProfile,
    collected_vars: &mut Vec<(String, IndexMap<String, DbtVars>)>,
    lookup_map: &BTreeMap<String, String>,
    packages_install_path: &Path,
    token: &CancellationToken,
) -> FsResult<Vec<DbtPackage>> {
    // Collect dependency package paths with a flag set to `true`
    // indicating that they are indeed dependencies. This is necessary
    // to differentiate between root project and dependencies later on.
    let mut dirs = if packages_install_path.exists() {
        stdfs::read_dir(packages_install_path)?
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_type()
                    .map(|ft| ft.is_dir() || ft.is_symlink())
                    .unwrap_or(false)
            })
            .map(|e| (e.path(), true))
            .collect()
    } else {
        vec![]
    };
    // Sort packages to make the output deterministic
    dirs.sort();
    // Add root package to the front of the list
    // `false` indicates that this is a root project
    dirs.insert(0, (arg.io.in_dir.clone(), false));

    collect_packages(
        arg,
        env,
        dbt_profile,
        collected_vars,
        dirs,
        lookup_map,
        token,
    )
    .await
}

pub async fn load_internal_packages(
    arg: &LoadArgs,
    env: &JinjaEnv,
    dbt_profile: &DbtProfile,
    collected_vars: &mut Vec<(String, IndexMap<String, DbtVars>)>,
    internal_packages_install_path: &Path,
    token: &CancellationToken,
) -> FsResult<Vec<DbtPackage>> {
    let mut dbt_internal_packages_dirs: Vec<(PathBuf, bool)> =
        stdfs::read_dir(internal_packages_install_path)?
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|ft| ft.is_dir()).unwrap_or(false)) // `true` indicates that this package path is a "dependency", not a root project
            .map(|e| (e.path(), true))
            .collect();
    dbt_internal_packages_dirs.sort();
    collect_packages(
        arg,
        env,
        dbt_profile,
        collected_vars,
        dbt_internal_packages_dirs,
        &BTreeMap::new(),
        token,
    )
    .await
}

/// Sync internal packages to disk using hash comparison.
/// Only writes files that have changed and removes stale files.
///
/// This function ensures that the internal dbt packages required for
/// the specified adapter type are present in the given installation path.
///
/// It will also ensure that all the files are exactly as stored in the embedded assets,
/// writing only those that differ based on SHA-256 hash comparison.
pub fn persist_internal_packages(
    internal_packages_install_path: &Path,
    adapter_type: AdapterType,
    #[allow(unused)] enable_persist_compare_package: bool,
) -> FsResult<()> {
    // Copy the dbt-adapters and dbt-{adapter_type} to the packages_install_path
    let adapter_package = format!("dbt-{adapter_type}");
    let mut internal_packages = vec!["dbt-adapters", &adapter_package];
    // Some adapters have extra dependencies
    match adapter_type {
        AdapterType::Redshift => internal_packages.push("dbt-postgres"),
        AdapterType::Databricks => internal_packages.push("dbt-spark"),
        _ => {}
    }

    // Track expected file paths for cleanup
    let mut expected_files: HashSet<PathBuf> = HashSet::new();

    for package in internal_packages {
        let mut found = false;
        for asset in assets::MacroAssets::iter() {
            let asset_path = asset.as_ref();
            if !asset_path.starts_with(package) {
                continue;
            }
            found = true;

            let install_path = internal_packages_install_path.join(asset_path);
            expected_files.insert(install_path.clone());

            let asset_contents = assets::MacroAssets::get(asset_path).expect("Asset must exist");
            let embedded_data = asset_contents.data.as_ref();

            // Check if file needs to be written
            let needs_write = if install_path.exists() {
                // Compare hashes
                let mut existing_data = Vec::new();
                std::fs::File::open(&install_path)
                    .and_then(|mut f| f.read_to_end(&mut existing_data))
                    .map(|_| {
                        let embedded_hash = Sha256::digest(embedded_data);
                        let existing_hash = Sha256::digest(&existing_data);
                        embedded_hash != existing_hash
                    })
                    .unwrap_or(true) // If read fails, write it
            } else {
                true // File doesn't exist, needs write
            };

            if needs_write {
                if let Some(parent) = install_path.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        fs_err!(
                            ErrorCode::IoError,
                            "Failed to create directory for dbt adapter package {}: {}",
                            parent.display(),
                            e
                        )
                    })?;
                }
                std::fs::write(&install_path, embedded_data).map_err(|e| {
                    fs_err!(
                        ErrorCode::IoError,
                        "Failed to write file for dbt adapter package {}: {}",
                        install_path.display(),
                        e
                    )
                })?;
            }
        }

        if !found {
            return err!(
                ErrorCode::InvalidConfig,
                "Missing default macro package '{}' for adapter type '{}'",
                package,
                adapter_type
            );
        }
    }

    // Remove extra files not in expected set
    if internal_packages_install_path.exists() {
        for entry in WalkDir::new(internal_packages_install_path)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_file() {
                let path = entry.path().to_path_buf();
                if !expected_files.contains(&path) {
                    std::fs::remove_file(&path).map_err(|e| {
                        fs_err!(
                            ErrorCode::IoError,
                            "Failed to remove stale adapter package file {}: {}",
                            path.display(),
                            e
                        )
                    })?;
                }
            }
        }
    }

    Ok(())
}

async fn collect_packages(
    arg: &LoadArgs,
    env: &JinjaEnv,
    dbt_profile: &DbtProfile,
    collected_vars: &mut Vec<(String, IndexMap<String, DbtVars>)>,
    package_paths: Vec<(PathBuf, bool)>,
    lookup_map: &BTreeMap<String, String>,
    token: &CancellationToken,
) -> FsResult<Vec<DbtPackage>> {
    let mut packages = vec![];
    // `is_dependency` Indicates if we are loading a dependency or a root project
    for (package_path, is_dependency) in package_paths {
        token.check_cancellation()?;
        if package_path.is_dir() {
            if package_path.join(DBT_PROJECT_YML).exists() {
                let package = load_inner(
                    arg,
                    env,
                    &package_path,
                    dbt_profile,
                    is_dependency,
                    lookup_map,
                    false,
                    collected_vars,
                )
                .await?;
                packages.push(package);
            } else {
                emit_warn_log_message(
                    ErrorCode::InvalidConfig,
                    format!(
                        "Package {} does not contain a dbt_project.yml file",
                        package_path.file_name().unwrap().to_str().unwrap()
                    ),
                    arg.io.status_reporter.as_ref(),
                );
            }
        }
    }
    Ok(packages)
}
