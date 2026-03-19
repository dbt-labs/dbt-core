//! GitHub repository cloning and archive download.
//!
//! This module handles downloading repositories using two methods:
//! 1. **Archive download** (preferred, GitHub only): Downloads tarball from GitHub archive endpoint, faster than git clone
//! 2. **Git clone** (fallback): Uses git commands when archive download fails or for non-GitHub URLs
//!
//! For GitHub URLs, the archive download path resolves tags/branches to commit SHAs using GitHub API,
//! then downloads and extracts the archive. If this fails or the URL is not GitHub, it falls back to git clone.

use std::path::{Path, PathBuf};

use dbt_common::tracing::emit::emit_warn_log_message;
use dbt_common::{ErrorCode, FsResult, err, fs_err, tokiofs};
use reqwest::Client;
use tokio::process::Command;

use crate::tarball_client::TarballClient;
use crate::utils::sanitize_git_url;

/// Validate that `subdirectory` is a single normal path component (no `..`,
/// no nested paths like `a/b`, no absolute paths). dbt only supports a single
/// directory level; anything else is either a misconfiguration or a path
/// traversal attempt.
fn validate_subdirectory(subdir: &str) -> Result<&str, String> {
    let mut components = Path::new(subdir).components();
    match (components.next(), components.next()) {
        (Some(std::path::Component::Normal(_)), None) => Ok(subdir),
        _ => Err(format!(
            "Invalid subdirectory '{}': must be a single directory name with no path separators or '..' components",
            subdir
        )),
    }
}

/// Analyzes git stderr output and returns a user-friendly error message.
/// This helps distinguish between different failure modes like authentication errors,
/// missing refs, and permission issues.
fn parse_git_clone_error(stderr: &str, repo: &str, revision: Option<&str>) -> (ErrorCode, String) {
    let sanitized_repo = sanitize_git_url(repo);
    let ref_name = revision.unwrap_or("HEAD");

    // Check for missing remote ref (branch/tag doesn't exist)
    if stderr.contains("couldn't find remote ref") || stderr.contains("did not match any") {
        return (
            ErrorCode::PackageDownloadFailed,
            format!(
                "Git ref '{}' not found in repository '{}'. Please verify the tag/branch/commit exists.",
                ref_name, sanitized_repo
            ),
        );
    }

    // Check for authentication failures
    if stderr.contains("Authentication failed")
        || stderr.contains("could not read Username")
        || stderr.contains("Invalid username or password")
        || stderr.contains("terminal prompts disabled")
    {
        return (
            ErrorCode::AuthFailed,
            format!(
                "Authentication failed for repository '{}'. Please check your credentials or access token.",
                sanitized_repo
            ),
        );
    }

    // Check for repository not found
    if stderr.contains("Repository not found")
        || stderr.contains("does not appear to be a git repository")
        || stderr.contains("not found")
    {
        return (
            ErrorCode::PackageDownloadFailed,
            format!(
                "Repository '{}' not found. Please verify the repository URL and your access permissions.",
                sanitized_repo
            ),
        );
    }

    // Check for permission denied
    if stderr.contains("403")
        || stderr.contains("Permission denied")
        || stderr.contains("Access denied")
    {
        return (
            ErrorCode::PermissionDenied,
            format!(
                "Permission denied for repository '{}'. Please verify you have access to this repository.",
                sanitized_repo
            ),
        );
    }

    // Handle HTTP 500 error (common with GitLab for both auth and missing refs)
    if stderr.contains("returned error: 500")
        || stderr.contains("The requested URL returned error: 500")
    {
        return (
            ErrorCode::PackageDownloadFailed,
            format!(
                "Failed to access repository '{}' (HTTP 500). This could be due to: \
                (1) invalid credentials, (2) missing permissions, or (3) non-existent git ref '{}'. \
                Please verify your access and that the specified version exists.",
                sanitized_repo, ref_name
            ),
        );
    }

    // Handle HTTP 401/unauthorized
    if stderr.contains("401") || stderr.contains("Unauthorized") {
        return (
            ErrorCode::AuthFailed,
            format!(
                "Unauthorized access to repository '{}'. Please check your credentials.",
                sanitized_repo
            ),
        );
    }

    // Fallback: include the sanitized URL for context
    (
        ErrorCode::ExecutorError,
        format!(
            "Git clone failed for '{}': {}",
            sanitized_repo,
            stderr.trim()
        ),
    )
}

/// Check if a revision string is a 40-character commit SHA.
pub fn is_commit(revision: &str) -> bool {
    let revision = revision.trim();
    revision.len() == 40 && revision.chars().all(|c| c.is_ascii_hexdigit())
}

/// Parse GitHub URL and return (owner, repo) if valid GitHub URL.
fn parse_github_url(url: &str) -> Option<(String, String)> {
    let url = url.trim_end_matches(".git");
    let path = if url.starts_with("https://github.com/") {
        url.strip_prefix("https://github.com/")?
    } else if url.starts_with("git@github.com:") {
        url.strip_prefix("git@github.com:")?
    } else {
        return None;
    };

    let mut parts = path.split('/');
    let owner = parts.next()?;
    let repo = parts.next()?;
    if parts.next().is_some() {
        return None; // More than 2 path components
    }
    Some((owner.to_string(), repo.to_string()))
}

/// Resolve a tag or branch to its commit SHA using GitHub API.
///
/// Returns commit SHAs directly without network call. Uses GitHub API commits endpoint
/// with minimal response size for fast resolution.
async fn resolve_ref_to_sha(repo_url: &str, revision: &str) -> Result<String, String> {
    if is_commit(revision) {
        return Ok(revision.to_string());
    }

    let (owner, repo) =
        parse_github_url(repo_url).ok_or_else(|| format!("Not a GitHub URL: {}", repo_url))?;

    // Use GitHub API commits endpoint - works for tags, branches, and commit SHAs
    // Using application/vnd.github.sha header returns just the SHA (fastest, ~40 bytes response)
    let api_url = format!(
        "https://api.github.com/repos/{}/{}/commits/{}",
        owner, repo, revision
    );
    // TODO: use a shared reqwest::Client across packages to avoid per-call overhead (same approach as tarball/hub clients)
    let client = Client::new();
    let resp = client
        .get(&api_url)
        .header("Accept", "application/vnd.github.sha")
        // TODO: the /commits/{ref} endpoint also accepts heads/BRANCH and tags/TAG prefixes per API docs,
        // but plain names work for all formats as confirmed by testing. If tag/branch name ambiguity
        // ever becomes an issue, add prefix detection here.
        .header(
            "User-Agent",
            concat!("dbt-fusion/", env!("CARGO_PKG_VERSION")),
        )
        .send()
        .await
        .map_err(|e| format!("Failed to fetch SHA from GitHub API: {}", e))?;

    if resp.status().is_success() {
        let sha = resp
            .text()
            .await
            .map_err(|e| format!("Failed to read SHA: {}", e))?;
        let sha = sha.trim();
        if sha.len() == 40 && sha.chars().all(|c| c.is_ascii_hexdigit()) {
            return Ok(sha.to_string());
        }
    }

    Err(format!("Could not resolve {} to a commit SHA", revision))
}

/// Download GitHub archive tarball and extract to target directory.
///
/// Uses the centralized tarball client with automatic root directory stripping.
/// Supports optional subdirectory filtering for sparse extraction.
async fn download_github_archive(
    owner: &str,
    repo: &str,
    revision: &str,
    clone_dir: &Path,
    subdirectory: Option<&str>,
) -> Result<(), String> {
    let archive_url = format!(
        "https://github.com/{}/{}/archive/{}.tar.gz",
        owner, repo, revision
    );

    let tarball_client = TarballClient::new();

    // If subdirectory is specified, extract to clone_dir/{subdir} to match git sparse-checkout behavior.
    // Otherwise extract directly to clone_dir.
    let extract_target = if let Some(subdir) = subdirectory {
        clone_dir.join(subdir)
    } else {
        clone_dir.to_path_buf()
    };

    tarball_client
        .download_and_extract_tarball(
            &archive_url,
            &extract_target,
            true, // strip_root=true for GitHub archives
            subdirectory,
        )
        .await
        .map_err(|e| format!("Failed to download/extract archive: {}", e))?;

    Ok(())
}

/// Try to download repository as GitHub archive.
///
/// Downloads archive and resolves tag/branch to SHA in parallel. Returns commit SHA on success.
async fn try_github_archive(
    repo_url: &str,
    owner: &str,
    repo: &str,
    revision: &str,
    clone_dir: &Path,
    subdirectory: Option<&str>,
) -> Result<String, String> {
    // Download archive and resolve SHA in parallel
    let (archive_result, sha_result) = tokio::join!(
        download_github_archive(owner, repo, revision, clone_dir, subdirectory),
        resolve_ref_to_sha(repo_url, revision)
    );

    archive_result?;
    sha_result
}

/// Fetch a git repository and checkout the specified revision.
///
/// For GitHub URLs, tries archive download first (faster), falls back to git clone if archive fails.
/// For non-GitHub URLs, uses git clone directly.
///
/// Returns the path to the checked-out directory (with subdirectory if specified) and the commit SHA.
pub async fn fetch_git_repository(
    repo: &str,
    clone_dir: &Path,
    revision: Option<&str>,
    maybe_checkout_subdir: Option<&str>,
    remove_git_dir: bool,
) -> FsResult<(PathBuf, String)> {
    if let Some(subdir) = maybe_checkout_subdir {
        validate_subdirectory(subdir).map_err(|e| fs_err!(ErrorCode::InvalidConfig, "{}", e))?;
    }
    if let Some((owner, repo_name)) = parse_github_url(repo) {
        if let Some(rev) = revision {
            match try_github_archive(
                repo,
                &owner,
                &repo_name,
                rev,
                clone_dir,
                maybe_checkout_subdir,
            )
            .await
            {
                Ok(sha) => {
                    let final_path = if let Some(subdir) = maybe_checkout_subdir {
                        clone_dir.join(subdir)
                    } else {
                        clone_dir.to_path_buf()
                    };
                    return Ok((final_path, sha));
                }
                Err(_) => {
                    // Fall back to git clone — clean up any partial archive extraction first
                    let _ = tokiofs::remove_dir_all(clone_dir).await;
                    tokiofs::create_dir_all(clone_dir).await?;
                }
            }
        }
    }

    clone(
        repo,
        clone_dir,
        revision,
        maybe_checkout_subdir,
        remove_git_dir,
    )
    .await?;

    let commit_sha = checkout(clone_dir, revision.unwrap_or("HEAD")).await?;
    Ok((
        clone_dir.join(maybe_checkout_subdir.unwrap_or_default()),
        commit_sha,
    ))
}

/// Download a git-like package (git or private) to a directory.
///
/// Creates the download directory, downloads the package, and cleans up on failure.
/// Returns (checkout_path, commit_sha).
pub async fn download_git_like_package(
    repo_url: &str,
    revisions: &[String],
    subdirectory: &Option<String>,
    warn_unpinned: bool,
    download_dir: &Path,
) -> FsResult<(PathBuf, String)> {
    if let Some(subdir) = subdirectory {
        validate_subdirectory(subdir).map_err(|e| fs_err!(ErrorCode::InvalidConfig, "{}", e))?;
    }
    tokiofs::create_dir_all(download_dir).await?;

    let revision = revisions
        .last()
        .cloned()
        .unwrap_or_else(|| "HEAD".to_string());

    let (checkout_path, commit_sha) = match fetch_git_repository(
        repo_url,
        download_dir,
        Some(&revision),
        subdirectory.as_deref(),
        false,
    )
    .await
    {
        Ok(result) => result,
        Err(e) => {
            let _ = tokiofs::remove_dir_all(download_dir).await;
            return Err(e);
        }
    };

    if ["HEAD", "main", "master"].contains(&revision.as_str()) && warn_unpinned {
        emit_warn_log_message(
            ErrorCode::UnpinnedPackageWarning,
            format!(
                "The package {} is pinned to the default branch, which is not recommended. Consider pinning to a specific commit SHA instead.",
                sanitize_git_url(repo_url)
            ),
            None,
        );
    }

    Ok((checkout_path, commit_sha))
}

/// Check if a revision is a tag (faster than listing all tags).
async fn is_tag(clone_dir: &Path, revision: &str) -> FsResult<bool> {
    let output = Command::new("git")
        .current_dir(clone_dir)
        .env("LC_ALL", "C")
        .args(["rev-parse", "--verify", &format!("refs/tags/{}", revision)])
        .output()
        .await
        .map_err(|e| fs_err!(ErrorCode::ExecutorError, "Error checking tag: {e}"))?;
    Ok(output.status.success())
}

async fn checkout(clone_dir: &Path, revision: &str) -> FsResult<String> {
    let is_commit_revision = is_commit(revision);

    if is_commit_revision {
        Command::new("git")
            .current_dir(clone_dir)
            .args(["fetch", "origin", "--depth=1", revision])
            .output()
            .await
            .map_err(|e| fs_err!(ErrorCode::ExecutorError, "Error fetching: {e}"))?;
    } else {
        Command::new("git")
            .current_dir(clone_dir)
            .args(["remote", "set-branches", "origin", revision])
            .output()
            .await
            .map_err(|e| fs_err!(ErrorCode::ExecutorError, "Error setting branches: {e}"))?;
        Command::new("git")
            .current_dir(clone_dir)
            .args(["fetch", "origin", "--depth=1", "--tags", revision])
            .output()
            .await
            .map_err(|e| fs_err!(ErrorCode::ExecutorError, "Error fetching: {e}"))?;
    }

    let spec = if is_commit_revision {
        revision.to_string()
    } else if is_tag(clone_dir, revision).await? {
        format!("tags/{revision}")
    } else {
        format!("origin/{revision}")
    };

    Command::new("git")
        .current_dir(clone_dir)
        .env("LC_ALL", "C")
        .args(["reset", "--hard", &spec])
        .output()
        .await
        .map_err(|e| fs_err!(ErrorCode::ExecutorError, "Error checking out: {e}"))?;

    let commit_sha = Command::new("git")
        .current_dir(clone_dir)
        .args(["rev-parse", &spec])
        .output()
        .await
        .map_err(|e| fs_err!(ErrorCode::ExecutorError, "Error getting revision: {e}"))?;

    Ok(String::from_utf8(commit_sha.stdout)
        .expect("Git output should be UTF-8")
        .trim()
        .to_string())
}

async fn clone(
    repo: &str,
    clone_dir: &Path,
    revision: Option<&str>,
    maybe_checkout_subdir: Option<&str>,
    remove_git_dir: bool,
) -> FsResult<()> {
    let mut clone_cmd = Command::new("git");
    clone_cmd.args(["clone", "--depth=1"]);

    if maybe_checkout_subdir.is_some() {
        // TODO: Check the git version whether --filter is supported for subdirectory checkout
        clone_cmd.args(["--filter=blob:none", "--sparse"]);
    }

    // Only add branch if we have a non-commit revision
    if let Some(rev) = revision
        && !is_commit(rev)
    {
        clone_cmd.args(["--branch", rev]);
    }

    clone_cmd.arg(repo).arg(clone_dir).env("LC_ALL", "C");
    let output = clone_cmd
        .output()
        .await
        .map_err(|e| fs_err!(ErrorCode::ExecutorError, "Error cloning repo: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8(output.stderr).expect("Git output should be UTF-8");
        if stderr.contains("Remote branch HEAD not found") {
            // For commit hashes, we don't pass any revision during clone
            let basic_output = Command::new("git")
                .args(["clone", repo])
                .arg(clone_dir)
                .output()
                .await
                .map_err(|e| fs_err!(ErrorCode::ExecutorError, "Error cloning repo: {e}"))?;
            if !basic_output.status.success() {
                let basic_stderr =
                    String::from_utf8(basic_output.stderr).expect("Git output should be UTF-8");
                let (error_code, message) = parse_git_clone_error(&basic_stderr, repo, revision);
                return err!(error_code, "{}", message);
            }
            return Ok(());
        }
        let (error_code, message) = parse_git_clone_error(&stderr, repo, revision);
        return err!(error_code, "{}", message);
    }

    if let Some(subdir) = maybe_checkout_subdir {
        let sparse_output = Command::new("git")
            .current_dir(clone_dir)
            .args(["sparse-checkout", "set", subdir])
            .output()
            .await
            .map_err(|e| {
                fs_err!(
                    ErrorCode::ExecutorError,
                    "Error setting sparse checkout: {e}"
                )
            })?;
        if !sparse_output.status.success() {
            return err!(
                ErrorCode::ExecutorError,
                "Git sparse checkout of {subdir} failed with exit status: {}",
                String::from_utf8(sparse_output.stderr).expect("Git output should be UTF-8")
            );
        }
    }

    if remove_git_dir {
        tokiofs::remove_dir_all(clone_dir.join(".git"))
            .await
            .map_err(|e| {
                fs_err!(
                    ErrorCode::ExecutorError,
                    "Error removing .git directory: {e}"
                )
            })?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_commit() {
        assert!(is_commit("1234567890abcdef1234567890abcdef12345678"));
        assert!(!is_commit("1234567890abcdef1234567890abcdef1234567890abc"));
    }

    #[test]
    fn test_parse_github_url_https() {
        // Standard HTTPS URL
        let result = parse_github_url("https://github.com/dbt-labs/dbt-utils.git");
        assert_eq!(
            result,
            Some(("dbt-labs".to_string(), "dbt-utils".to_string()))
        );

        // Without .git suffix
        let result = parse_github_url("https://github.com/dbt-labs/dbt-utils");
        assert_eq!(
            result,
            Some(("dbt-labs".to_string(), "dbt-utils".to_string()))
        );
    }

    #[test]
    fn test_parse_github_url_ssh() {
        // SSH URL format
        let result = parse_github_url("git@github.com:dbt-labs/dbt-utils.git");
        assert_eq!(
            result,
            Some(("dbt-labs".to_string(), "dbt-utils".to_string()))
        );

        // Without .git suffix
        let result = parse_github_url("git@github.com:dbt-labs/dbt-utils");
        assert_eq!(
            result,
            Some(("dbt-labs".to_string(), "dbt-utils".to_string()))
        );
    }

    #[test]
    fn test_parse_github_url_non_github() {
        // Non-GitHub URLs should return None
        assert_eq!(parse_github_url("https://gitlab.com/owner/repo.git"), None);
        assert_eq!(
            parse_github_url("https://bitbucket.org/owner/repo.git"),
            None
        );
        assert_eq!(parse_github_url("git@gitlab.com:owner/repo.git"), None);
    }

    #[test]
    fn test_parse_git_clone_error_missing_ref() {
        let stderr = "fatal: couldn't find remote ref v1.1500.3";
        let (code, message) =
            parse_git_clone_error(stderr, "https://github.com/org/repo.git", Some("v1.1500.3"));
        assert_eq!(code, ErrorCode::PackageDownloadFailed);
        assert!(message.contains("v1.1500.3"));
        assert!(message.contains("not found"));
    }

    #[test]
    fn test_parse_git_clone_error_auth_failed() {
        let stderr = "fatal: Authentication failed for 'https://github.com/org/repo.git/'";
        let (code, message) =
            parse_git_clone_error(stderr, "https://user:pass@github.com/org/repo.git", None);
        assert_eq!(code, ErrorCode::AuthFailed);
        assert!(message.contains("Authentication failed"));
        // Verify credentials are sanitized in the message
        assert!(!message.contains("user:pass"));
    }

    #[test]
    fn test_parse_git_clone_error_permission_denied() {
        let stderr = "fatal: unable to access 'https://github.com/org/repo.git/': The requested URL returned error: 403";
        let (code, message) =
            parse_git_clone_error(stderr, "https://github.com/org/repo.git", None);
        assert_eq!(code, ErrorCode::PermissionDenied);
        assert!(message.contains("Permission denied"));
    }

    #[test]
    fn test_parse_git_clone_error_http_500_gitlab() {
        let stderr = "Cloning into '/tmp/.tmpRyO2PJ/private_package.git'...\nfatal: unable to access 'https://gitlab.host.org/private_package.git/': The requested URL returned error: 500";
        let (code, message) = parse_git_clone_error(
            stderr,
            "https://gitlab-ci-token:xxx@gitlab.host.org/private_package.git",
            Some("v1.0.0"),
        );
        assert_eq!(code, ErrorCode::PackageDownloadFailed);
        assert!(message.contains("HTTP 500"));
        assert!(message.contains("v1.0.0"));
        assert!(message.contains("credentials"));
        assert!(message.contains("permissions"));
        assert!(!message.contains("gitlab-ci-token"));
    }

    #[test]
    fn test_parse_git_clone_error_unauthorized() {
        let stderr = "fatal: unable to access 'https://github.com/org/repo.git/': The requested URL returned error: 401";
        let (code, message) =
            parse_git_clone_error(stderr, "https://github.com/org/repo.git", None);
        assert_eq!(code, ErrorCode::AuthFailed);
        assert!(message.contains("Unauthorized"));
    }

    #[test]
    fn test_parse_git_clone_error_repo_not_found() {
        let stderr = "fatal: repository 'https://github.com/org/nonexistent.git/' not found";
        let (code, message) =
            parse_git_clone_error(stderr, "https://github.com/org/nonexistent.git", None);
        assert_eq!(code, ErrorCode::PackageDownloadFailed);
        assert!(message.contains("not found"));
    }

    #[test]
    fn test_parse_git_clone_error_unknown_error() {
        let stderr = "fatal: some unexpected git error occurred";
        let (code, message) =
            parse_git_clone_error(stderr, "https://github.com/org/repo.git", None);
        assert_eq!(code, ErrorCode::ExecutorError);
        assert!(message.contains("some unexpected git error"));
    }

    #[test]
    fn test_parse_git_clone_error_sanitizes_credentials() {
        let stderr = "fatal: couldn't find remote ref main";
        let repo_with_creds = "https://ghp_1234567890abcdef@github.com/org/repo.git";
        let (_, message) = parse_git_clone_error(stderr, repo_with_creds, Some("main"));
        assert!(!message.contains("ghp_1234567890abcdef"));
        assert!(message.contains("github.com"));
    }
}
