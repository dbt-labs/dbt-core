use std::{
    borrow::Cow,
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use dbt_schemas::schemas::{
    packages::{DbtPackageEntry, DbtPackages, LocalPackage},
    project::DbtProjectNameOnly,
};
use sha1::Digest;

use dbt_common::{
    ErrorCode, FsResult, constants::DBT_PROJECT_YML, err, fs_err, io_args::IoArgs, tokiofs,
    tracing::emit::emit_warn_log_message,
};
use dbt_jinja_utils::{
    jinja_environment::JinjaEnv,
    phases::load::LoadContext,
    serde::{into_typed_with_jinja, value_from_file},
    utils::SECRET_ENV_VAR_PREFIX,
};
use dbt_schemas::schemas::project::DbtProject;

/// Create `path` (and missing parents), mapping any I/O error into an
/// `FsResult` that includes the path for context.
pub async fn ensure_dir(path: &Path) -> FsResult<()> {
    tokiofs::create_dir_all(path).await.map_err(|e| {
        fs_err!(
            ErrorCode::IoError,
            "Failed to create directory '{}': {}",
            path.display(),
            e,
        )
    })
}

/// Create a new `TempDir`, optionally inside `parent`, mapping any I/O error
/// into an `FsResult`. When `parent` is `None` the system temp dir is used.
pub fn make_tempdir(parent: Option<&Path>) -> FsResult<tempfile::TempDir> {
    let result = match parent {
        Some(dir) => tempfile::tempdir_in(dir),
        None => tempfile::tempdir(),
    };
    result.map_err(|e| fs_err!(ErrorCode::IoError, "Failed to create temp dir: {}", e))
}

/// Move a directory from `src` to `dst`.
///
/// Attempts an atomic `rename` first. If that fails due to a cross-device error
/// (src and dst on different filesystems), falls back to a recursive copy followed
/// by deletion of the source.
pub async fn move_dir(src: &Path, dst: &Path) -> FsResult<()> {
    if tokiofs::rename(src, dst).await.is_ok() {
        return Ok(());
    }
    // rename failed (e.g. cross-device) — fall back to recursive copy + delete
    copy_dir_recursive(src, dst).await?;
    tokiofs::remove_dir_all(src).await
}

async fn copy_dir_recursive(src: &Path, dst: &Path) -> FsResult<()> {
    tokiofs::create_dir_all(dst).await?;
    let mut entries = tokiofs::read_dir(src).await?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to read directory entry: {}", e))?
    {
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        let file_type = entry.file_type().await.map_err(|e| {
            fs_err!(
                ErrorCode::IoError,
                "Failed to get file type for '{}': {}",
                src_path.display(),
                e
            )
        })?;
        if file_type.is_dir() {
            Box::pin(copy_dir_recursive(&src_path, &dst_path)).await?;
        } else {
            tokiofs::copy(&src_path, &dst_path).await?;
        }
    }
    Ok(())
}

pub fn get_local_package_full_path(in_dir: &Path, local_package: &LocalPackage) -> PathBuf {
    if local_package.local.is_absolute() {
        local_package.local.clone()
    } else {
        in_dir.join(&local_package.local)
    }
}

pub fn fusion_sha1_hash_packages(
    packages: &[DbtPackageEntry],
    use_v2_compatible_package_downloads: bool,
) -> String {
    let mut package_strs = packages
        .iter()
        .map(|p| serde_json::to_string(p).unwrap())
        .collect::<Vec<String>>();
    package_strs.sort();
    // Add flag for installing v2-compatible downloads from Package Hub to hash
    // so changing the flag will trigger a fresh deps install
    // Only use true so existing package lock files don't need updates
    if use_v2_compatible_package_downloads {
        package_strs.push(format!(
            "use_v2_compatible_package_downloads: {}",
            use_v2_compatible_package_downloads
        ));
    }
    format!(
        "{:x}",
        sha1::Sha1::digest(package_strs.join("\n").as_bytes())
    )
}

// TODO: Implement the proper core sha1 hash
#[allow(dead_code)]
pub fn core_sha1_hash_packages(_packages: &[DbtPackageEntry]) -> String {
    unimplemented!()
}

pub fn scrub_package_name_secret_env_vars(package_name: &str) -> Option<Cow<'_, str>> {
    let mut scrubbed = Cow::Borrowed(package_name);
    for (_, secret) in std::env::vars()
        .filter(|(key, value)| key.starts_with(SECRET_ENV_VAR_PREFIX) && !value.trim().is_empty())
    {
        if scrubbed.contains(secret.as_str()) {
            scrubbed = Cow::Owned(scrubbed.replace(secret.as_str(), "*****"));
        }
    }

    match scrubbed {
        Cow::Borrowed(_) => None,
        owned @ Cow::Owned(_) => Some(owned),
    }
}

pub fn scrubbed_package_names_from_package_def(dbt_packages: &DbtPackages) -> Vec<String> {
    let mut scrubbed_package_names = Vec::new();

    // https://github.com/dbt-labs/dbt-core/blob/c02340d4c14df1459c00cf91b9ab738e1c4c9507/core/dbt/deps/git.py#L56-L69
    for package in &dbt_packages.packages {
        match package {
            DbtPackageEntry::Git(git_package) => {
                if let Some(scrubbed) = scrub_package_name_secret_env_vars(git_package.git.as_str())
                {
                    scrubbed_package_names.push(scrubbed.into_owned());
                }
            }
            DbtPackageEntry::Tarball(tarball_package) => {
                if let Some(scrubbed) =
                    scrub_package_name_secret_env_vars(tarball_package.tarball.as_str())
                {
                    scrubbed_package_names.push(scrubbed.into_owned());
                }
            }
            _ => {}
        }
    }

    scrubbed_package_names
}

pub fn emit_scrubbed_package_name_warnings(io_args: &IoArgs, scrubbed_package_names: &[String]) {
    for scrubbed in scrubbed_package_names {
        emit_warn_log_message(
            ErrorCode::DepsScrubbedPackageName,
            format!(
                "Detected secret env var in {scrubbed}. dbt will write a scrubbed representation to the lock file. This will cause issues with subsequent 'dbt deps' using the lock file, requiring 'dbt deps --upgrade'"
            ),
            io_args.status_reporter.as_ref(),
        );
    }
}

pub fn read_and_validate_dbt_project(
    io: &IoArgs,
    checkout_path: &Path,
    show_errors_or_warnings: bool,
    jinja_env: &JinjaEnv,
    vars: &BTreeMap<String, dbt_yaml::Value>,
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
