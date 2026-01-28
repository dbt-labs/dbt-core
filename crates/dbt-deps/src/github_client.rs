use dbt_common::{ErrorCode, FsResult, err, fs_err};
use std::{path::PathBuf, process::Command};

use crate::utils::sanitize_git_url;

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

pub fn is_commit(revision: &str) -> bool {
    revision.len() == 40 && revision.chars().all(|c| c.is_ascii_hexdigit())
}

pub fn clone_and_checkout(
    repo: &str,
    clone_dir: &PathBuf,
    revision: &Option<String>,
    maybe_checkout_subdir: &Option<String>,
    remove_git_dir: bool,
) -> FsResult<(PathBuf, String)> {
    let _exit_msg = clone(
        repo,
        clone_dir,
        revision,
        maybe_checkout_subdir,
        remove_git_dir,
    )?;
    let commit_sha = checkout(clone_dir, revision.as_deref().unwrap_or("HEAD"))?;
    Ok((
        clone_dir.join(maybe_checkout_subdir.clone().unwrap_or_default()),
        commit_sha,
    ))
}

pub fn list_tags(clone_dir: &PathBuf) -> FsResult<Vec<String>> {
    let output = Command::new("git")
        .current_dir(clone_dir)
        .env("LC_ALL", "C")
        .arg("tag")
        .arg("--list")
        .output()
        .map_err(|e| fs_err!(ErrorCode::ExecutorError, "Error listing tags: {e}"))?;
    let tags = String::from_utf8(output.stdout).expect("Git output should be UTF-8");
    Ok(tags.split('\n').map(|s| s.to_string()).collect())
}

pub fn checkout(clone_dir: &PathBuf, revision: &str) -> FsResult<String> {
    // Fetch command
    let mut fetch_cmd = Command::new("git");
    fetch_cmd.arg("fetch").arg("origin").arg("--depth=1");
    let is_commit_revision = is_commit(revision);

    if is_commit_revision {
        fetch_cmd.arg(revision);
        fetch_cmd
            .current_dir(clone_dir)
            .output()
            .map_err(|e| fs_err!(ErrorCode::ExecutorError, "Error fetching: {e}"))?;
    } else {
        let mut set_branch_cmd = Command::new("git");
        set_branch_cmd
            .current_dir(clone_dir)
            .arg("remote")
            .arg("set-branches")
            .arg("origin")
            .arg(revision)
            .output()
            .map_err(|e| fs_err!(ErrorCode::ExecutorError, "Error setting branches: {e}"))?;
        fetch_cmd
            .current_dir(clone_dir)
            .arg("--tags")
            .arg(revision)
            .output()
            .map_err(|e| fs_err!(ErrorCode::ExecutorError, "Error fetching: {e}"))?;
    }
    let spec = if is_commit_revision {
        revision.to_string()
    } else if list_tags(clone_dir)?.contains(&revision.to_string()) {
        format!("tags/{revision}")
    } else {
        format!("origin/{revision}")
    };
    let mut checkout_cmd = Command::new("git");
    let _ = checkout_cmd
        .current_dir(clone_dir)
        .env("LC_ALL", "C")
        .arg("reset")
        .arg("--hard")
        .arg(&spec)
        .output()
        .map_err(|e| fs_err!(ErrorCode::ExecutorError, "Error checking out: {e}"))?;
    let mut get_commit_sha_cmd = Command::new("git");
    let commit_sha = get_commit_sha_cmd
        .current_dir(clone_dir)
        .arg("rev-parse")
        .arg(spec)
        .output()
        .map_err(|e| fs_err!(ErrorCode::ExecutorError, "Error getting revision: {e}"))?;
    Ok(String::from_utf8(commit_sha.stdout)
        .expect("Git output should be UTF-8")
        .trim()
        .to_string())
}

pub fn clone(
    repo: &str,
    clone_dir: &PathBuf,
    revision: &Option<String>,
    maybe_checkout_subdir: &Option<String>,
    remove_git_dir: bool,
) -> FsResult<String> {
    let mut clone_cmd = Command::new("git");
    clone_cmd.arg("clone").arg("--depth=1");

    if maybe_checkout_subdir.is_some() {
        // TODO: Check the git version whether --filter is supported for subdirectory checkout
        clone_cmd.arg("--filter=blob:none").arg("--sparse");
    }

    // Only add branch if we have a non-commit revision
    if let Some(revision) = revision
        && !is_commit(revision)
    {
        clone_cmd.arg("--branch").arg(revision);
    }

    clone_cmd.arg(repo);
    clone_cmd.arg(clone_dir);
    clone_cmd.env("LC_ALL", "C");
    let output = clone_cmd
        .output()
        .map_err(|e| fs_err!(ErrorCode::ExecutorError, "Error cloning repo: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8(output.stderr).expect("Git output should be UTF-8");
        if stderr.contains("Remote branch HEAD not found") {
            // For commit hashes, we don't pass any revision during clone
            let mut basic_clone = Command::new("git");
            basic_clone.arg("clone").arg(repo).arg(clone_dir);
            let basic_output = basic_clone
                .output()
                .map_err(|e| fs_err!(ErrorCode::ExecutorError, "Error cloning repo: {e}"))?;
            if !basic_output.status.success() {
                let basic_stderr =
                    String::from_utf8(basic_output.stderr).expect("Git output should be UTF-8");
                let (error_code, message) =
                    parse_git_clone_error(&basic_stderr, repo, revision.as_deref());
                return err!(error_code, "{}", message);
            }
            return Ok(String::from_utf8(basic_output.stdout).expect("Git output should be UTF-8"));
        }
        let (error_code, message) = parse_git_clone_error(&stderr, repo, revision.as_deref());
        return err!(error_code, "{}", message);
    }

    if let Some(subdir) = maybe_checkout_subdir {
        let mut sparse_checkout_cmd = Command::new("git");
        sparse_checkout_cmd
            .arg("sparse-checkout")
            .arg("set")
            .arg(subdir);
        let sparse_output = sparse_checkout_cmd
            .current_dir(clone_dir)
            .output()
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
        std::fs::remove_dir_all(clone_dir.join(".git")).map_err(|e| {
            fs_err!(
                ErrorCode::ExecutorError,
                "Error removing .git directory: {e}",
            )
        })?;
    }
    Ok(String::from_utf8(output.stdout).expect("Git output should be UTF-8"))
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
        // This is the exact error from GitLab that the bug report mentions
        let stderr = "Cloning into '/tmp/.tmpRyO2PJ/private_package.git'...\nfatal: unable to access 'https://gitlab.host.org/private_package.git/': The requested URL returned error: 500";
        let (code, message) = parse_git_clone_error(
            stderr,
            "https://gitlab-ci-token:xxx@gitlab.host.org/private_package.git",
            Some("v1.0.0"),
        );
        assert_eq!(code, ErrorCode::PackageDownloadFailed);
        assert!(message.contains("HTTP 500"));
        assert!(message.contains("v1.0.0"));
        // Verify it mentions possible causes
        assert!(message.contains("credentials"));
        assert!(message.contains("permissions"));
        // Verify credentials are sanitized
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
        // Verify credentials are not in the message
        assert!(!message.contains("ghp_1234567890abcdef"));
        assert!(message.contains("github.com"));
    }
}
