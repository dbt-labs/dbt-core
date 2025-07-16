use dbt_common::{ErrorCode, FsResult, err, fs_err};
use std::{path::PathBuf, process::Command};
// Use the local git client!

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
    let commit_sha = checkout(
        clone_dir,
        revision.clone().unwrap_or("HEAD".to_string()).as_str(),
    )?;
    Ok((
        clone_dir.join(maybe_checkout_subdir.clone().unwrap_or("".to_string())),
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
        .map_err(|e| fs_err!(ErrorCode::RuntimeError, "Error listing tags: {e}"))?;
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
            .map_err(|e| fs_err!(ErrorCode::RuntimeError, "Error fetching: {e}"))?;
    } else {
        let mut set_branch_cmd = Command::new("git");
        set_branch_cmd
            .current_dir(clone_dir)
            .arg("remote")
            .arg("set-branches")
            .arg("origin")
            .arg(revision)
            .output()
            .map_err(|e| fs_err!(ErrorCode::RuntimeError, "Error setting branches: {e}"))?;
        fetch_cmd
            .current_dir(clone_dir)
            .arg("--tags")
            .arg(revision)
            .output()
            .map_err(|e| fs_err!(ErrorCode::RuntimeError, "Error fetching: {e}"))?;
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
        .map_err(|e| fs_err!(ErrorCode::RuntimeError, "Error checking out: {e}"))?;
    let mut get_commit_sha_cmd = Command::new("git");
    let commit_sha = get_commit_sha_cmd
        .current_dir(clone_dir)
        .arg("rev-parse")
        .arg(spec)
        .output()
        .map_err(|e| fs_err!(ErrorCode::RuntimeError, "Error getting revision: {e}"))?;
    Ok(String::from_utf8(commit_sha.stdout).expect("Git output should be UTF-8"))
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
    if let Some(revision) = revision {
        if !is_commit(revision) {
            clone_cmd.arg("--branch").arg(revision);
        }
    }

    clone_cmd.arg(repo);
    clone_cmd.arg(clone_dir);
    clone_cmd.env("LC_ALL", "C");
    let output = clone_cmd
        .output()
        .map_err(|e| fs_err!(ErrorCode::RuntimeError, "Error cloning repo: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8(output.stderr).expect("Git output should be UTF-8");
        if stderr.contains("Remote branch HEAD not found") {
            // For commit hashes, we don't pass any revision during clone
            let mut basic_clone = Command::new("git");
            basic_clone.arg("clone").arg(repo).arg(clone_dir);
            let basic_output = basic_clone
                .output()
                .map_err(|e| fs_err!(ErrorCode::RuntimeError, "Error cloning repo: {e}"))?;
            if !basic_output.status.success() {
                return err!(
                    ErrorCode::RuntimeError,
                    "Git clone failed with exit status: {}",
                    String::from_utf8(basic_output.stderr).expect("Git output should be UTF-8")
                );
            }
            return Ok(String::from_utf8(basic_output.stdout).expect("Git output should be UTF-8"));
        }
        return err!(
            ErrorCode::RuntimeError,
            "Git clone failed with exit status: {}",
            stderr
        );
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
                    ErrorCode::RuntimeError,
                    "Error setting sparse checkout: {e}"
                )
            })?;
        if !sparse_output.status.success() {
            return err!(
                ErrorCode::RuntimeError,
                "Git sparse checkout of {subdir} failed with exit status: {}",
                String::from_utf8(sparse_output.stderr).expect("Git output should be UTF-8")
            );
        }
    }
    if remove_git_dir {
        std::fs::remove_dir_all(clone_dir.join(".git")).map_err(|e| {
            fs_err!(
                ErrorCode::RuntimeError,
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
}
