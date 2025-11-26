use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use dbt_schemas::schemas::{
    packages::{DbtPackageEntry, LocalPackage},
    project::DbtProjectNameOnly,
};
use sha1::Digest;

use dbt_common::{ErrorCode, FsResult, constants::DBT_PROJECT_YML, err, fs_err, io_args::IoArgs};
use dbt_jinja_utils::{
    jinja_environment::JinjaEnv,
    phases::load::LoadContext,
    serde::{into_typed_with_jinja, value_from_file},
};
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
    let revision = revisions
        .last()
        .cloned()
        .unwrap_or_else(|| "HEAD".to_string());
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
            "\nWARNING: The package {} is pinned to the default branch, which is not recommended. Consider pinning to a specific commit SHA instead.",
            sanitize_git_url(repo_url)
        );
    }
    Ok((tmp_dir, checkout_path, commit_sha))
}

pub fn read_and_validate_dbt_project(
    io: &IoArgs,
    checkout_path: &Path,
    show_errors_or_warnings: bool,
    jinja_env: &JinjaEnv,
    vars: &BTreeMap<String, dbt_serde_yaml::Value>,
) -> FsResult<DbtProject> {
    let path_to_dbt_project = checkout_path.join(DBT_PROJECT_YML);
    if !path_to_dbt_project.exists() {
        return err!(
            ErrorCode::IoError,
            "Package does not contain a dbt_project.yml file: {}",
            checkout_path.display()
        );
    }

    // Try to deserialize only the package name for error reporting,
    // falling back to the path if deserialization fails
    let dependency_package_name = value_from_file(io, &path_to_dbt_project, false, None)
        .ok()
        .and_then(|value| {
            let deps_context = LoadContext::new(vars.clone());
            into_typed_with_jinja::<DbtProjectNameOnly, _>(
                io,
                value,
                false,
                jinja_env,
                &deps_context,
                &[],
                None,
                false,
            )
            .ok()
        })
        .map(|p| p.name)
        .unwrap_or_else(|| path_to_dbt_project.to_string_lossy().to_string());

    let deps_context = LoadContext::new(vars.clone());
    into_typed_with_jinja(
        io,
        value_from_file(
            io,
            &path_to_dbt_project,
            show_errors_or_warnings,
            Some(&dependency_package_name),
        )?,
        false,
        jinja_env,
        &deps_context,
        &[],
        Some(&dependency_package_name),
        show_errors_or_warnings,
    )
}

/// Sanitizes username/password segments from git urls
/// e.g. https://username:password@github.com/dbt-labs/secret-project
///   becomes https://github.com/dbt-labs/secret-project
pub fn sanitize_git_url(url: &str) -> String {
    if let Ok(mut parsed) = url::Url::parse(url) {
        let _ = parsed.set_username("");
        let _ = parsed.set_password(None);
        parsed.to_string()
    } else {
        // Fallback if Url can't parse - use regex
        let patterns_to_remove = [
            // https://token@host
            r"https://[^@/]+@([^/]+)",
            // https://user:pass@host
            r"https://[^:]+:[^@/]+@([^/]+)",
            // git@host:path
            r"git@([^:]+):",
        ];

        let mut sanitized = url.to_string();
        for pattern in &patterns_to_remove {
            if let Ok(re) = regex::Regex::new(pattern) {
                if *pattern == r"git@([^:]+):" {
                    // Special handling for SSH format: git@host:path -> https://host/path
                    sanitized = re.replace(&sanitized, "https://$1/").to_string();
                } else {
                    sanitized = re.replace(&sanitized, "https://$1").to_string();
                }
            }
        }
        sanitized
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_git_url_basic_credentials() {
        let url = "https://username:password@github.com/dbt-labs/secret-project";
        let sanitized = sanitize_git_url(url);
        assert_eq!(sanitized, "https://github.com/dbt-labs/secret-project");
    }

    #[test]
    fn test_sanitize_git_url_token_only() {
        let url = "https://ghp_1234567890abcdef@github.com/dbt-labs/secret-project";
        let sanitized = sanitize_git_url(url);
        assert_eq!(sanitized, "https://github.com/dbt-labs/secret-project");
    }

    #[test]
    fn test_sanitize_git_url_github_token() {
        let url = "https://ghp_abcdef1234567890@github.com/dbt-labs/dbt-core.git";
        let sanitized = sanitize_git_url(url);
        assert_eq!(sanitized, "https://github.com/dbt-labs/dbt-core.git");
    }

    #[test]
    fn test_sanitize_git_url_ssh_format() {
        let url = "git@github.com:dbt-labs/secret-project.git";
        let sanitized = sanitize_git_url(url);
        assert_eq!(sanitized, "https://github.com/dbt-labs/secret-project.git");
    }

    #[test]
    fn test_sanitize_git_url_with_path_and_query() {
        let url = "https://user:pass@github.com/dbt-labs/secret-project.git?ref=main";
        let sanitized = sanitize_git_url(url);
        assert_eq!(
            sanitized,
            "https://github.com/dbt-labs/secret-project.git?ref=main"
        );
    }

    #[test]
    fn test_sanitize_git_url_already_clean() {
        let url = "https://github.com/dbt-labs/dbt-core.git";
        let sanitized = sanitize_git_url(url);
        assert_eq!(sanitized, "https://github.com/dbt-labs/dbt-core.git");
    }

    #[test]
    fn test_sanitize_git_url_http_instead_of_https() {
        let url = "http://user:pass@github.com/dbt-labs/secret-project.git";
        let sanitized = sanitize_git_url(url);
        assert_eq!(sanitized, "http://github.com/dbt-labs/secret-project.git");
    }

    #[test]
    fn test_sanitize_git_url_username_only() {
        let url = "https://username@github.com/dbt-labs/secret-project.git";
        let sanitized = sanitize_git_url(url);
        assert_eq!(sanitized, "https://github.com/dbt-labs/secret-project.git");
    }

    #[test]
    fn test_sanitize_git_url_special_characters_in_credentials() {
        let url = "https://user%40domain.com:pass%21word@github.com/dbt-labs/secret-project.git";
        let sanitized = sanitize_git_url(url);
        assert_eq!(sanitized, "https://github.com/dbt-labs/secret-project.git");
    }

    #[test]
    fn test_sanitize_git_url_gitlab() {
        let url = "https://oauth2:glpat-1234567890@gitlab.com/dbt-labs/secret-project.git";
        let sanitized = sanitize_git_url(url);
        assert_eq!(sanitized, "https://gitlab.com/dbt-labs/secret-project.git");
    }

    #[test]
    fn test_sanitize_git_url_azure_devops() {
        let url = "https://user:pat@dev.azure.com/organization/project/_git/repo";
        let sanitized = sanitize_git_url(url);
        assert_eq!(
            sanitized,
            "https://dev.azure.com/organization/project/_git/repo"
        );
    }

    #[test]
    fn test_sanitize_git_url_bitbucket() {
        let url = "https://user:app_password@bitbucket.org/dbt-labs/secret-project.git";
        let sanitized = sanitize_git_url(url);
        assert_eq!(
            sanitized,
            "https://bitbucket.org/dbt-labs/secret-project.git"
        );
    }

    #[test]
    fn test_sanitize_git_url_invalid_url() {
        let url = "not-a-valid-url";
        let sanitized = sanitize_git_url(url);
        // Should return the original string since it's not a valid URL
        assert_eq!(sanitized, "not-a-valid-url");
    }

    #[test]
    fn test_sanitize_git_url_empty_string() {
        let url = "";
        let sanitized = sanitize_git_url(url);
        assert_eq!(sanitized, "");
    }

    #[test]
    fn test_sanitize_git_url_multiple_credentials() {
        let url = "https://user1:pass1@github.com/dbt-labs/secret-project.git";
        let sanitized = sanitize_git_url(url);
        assert_eq!(sanitized, "https://github.com/dbt-labs/secret-project.git");
    }

    #[test]
    fn test_sanitize_git_url_with_fragment() {
        let url = "https://user:pass@github.com/dbt-labs/secret-project.git#v1.0.0";
        let sanitized = sanitize_git_url(url);
        assert_eq!(
            sanitized,
            "https://github.com/dbt-labs/secret-project.git#v1.0.0"
        );
    }

    #[test]
    fn test_sanitize_git_url_long_token() {
        let url = "https://ghp_1234567890abcdef1234567890abcdef12345678@github.com/dbt-labs/secret-project.git";
        let sanitized = sanitize_git_url(url);
        assert_eq!(sanitized, "https://github.com/dbt-labs/secret-project.git");
    }

    #[test]
    fn test_sanitize_git_url_x_access_token() {
        let url =
            "https://x-access-token:ghp_1234567890abcdef@github.com/dbt-labs/secret-project.git";
        let sanitized = sanitize_git_url(url);
        assert_eq!(sanitized, "https://github.com/dbt-labs/secret-project.git");
    }
}
