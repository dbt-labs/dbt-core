use std::path::{Path, PathBuf};

use dbt_schemas::schemas::packages::{DbtPackageEntry, LocalPackage};
use sha1::Digest;

use dbt_common::{
    ErrorCode, FsResult, constants::DBT_PROJECT_YML, err, fs_err, io_args::IoArgs,
    io_utils::try_read_yml_to_str,
};
use dbt_jinja_utils::serde::from_yaml_raw;
use dbt_schemas::schemas::project::DbtProject;

use crate::github_client::clone_and_checkout;

pub fn get_local_package_full_path(in_dir: &Path, local_package: &LocalPackage) -> PathBuf {
    if local_package.local.is_absolute() {
        local_package.local.clone()
    } else {
        in_dir.join(&local_package.local)
    }
}

pub fn sha1_hash_packages(packages: &[DbtPackageEntry]) -> String {
    let mut package_strs = packages
        .iter()
        .map(|p| serde_json::to_string(p).unwrap())
        .collect::<Vec<String>>();
    package_strs.sort();
    format!(
        "{:x}",
        sha1::Sha1::digest(package_strs.join("\n").as_bytes())
    )
}

pub fn handle_git_like_package(
    repo_url: &str,
    revisions: &[String],
    subdirectory: &Option<String>,
    warn_unpinned: bool,
    packages_install_path: Option<&Path>,
) -> FsResult<(tempfile::TempDir, PathBuf, String)> {
    let tmp_dir = packages_install_path
        .map_or_else(tempfile::tempdir, tempfile::tempdir_in)
        .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to create temp dir: {}", e))?;
    let revision = revisions.last().unwrap_or(&"HEAD".to_string()).clone();
    let (checkout_path, commit_sha) = clone_and_checkout(
        repo_url,
        &tmp_dir
            .path()
            .to_path_buf()
            .join(repo_url.split('/').next_back().unwrap()),
        &Some(revision.clone()),
        subdirectory,
        false,
    )?;
    if ["HEAD", "main", "master"].contains(&revision.as_str()) && warn_unpinned {
        println!(
            "\nWARNING: The package {repo_url} is pinned to the default branch, which is not recommended. Consider pinning to a specific commit SHA instead."
        );
    }
    Ok((tmp_dir, checkout_path, commit_sha))
}

pub fn read_and_validate_dbt_project(io: &IoArgs, checkout_path: &Path) -> FsResult<DbtProject> {
    let path_to_dbt_project = checkout_path.join(DBT_PROJECT_YML);
    if !path_to_dbt_project.exists() {
        return err!(
            ErrorCode::IoError,
            "Package does not contain a dbt_project.yml file: {}",
            checkout_path.display()
        );
    }
    from_yaml_raw(
        io,
        &try_read_yml_to_str(&path_to_dbt_project)?,
        Some(&path_to_dbt_project),
        false,
    )
}
