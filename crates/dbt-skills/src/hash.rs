//! Hashing a skill directory's contents.
//!
//! Used to tell whether an installed skill is still identical to the source it
//! was copied from, which is what makes re-running `dbt deps` a no-op instead of
//! a rewrite.

use std::path::{Path, PathBuf};

use dbt_common::{FsResult, stdfs};
use sha2::{Digest, Sha256};
use walkdir::WalkDir;

/// Hash the contents of a skill directory.
///
/// Both relative paths and file bytes feed the hash, so a rename is a change.
/// Symlinks are not followed: only regular files contribute, matching what
/// `install::copy_skill` actually copies.
pub fn hash_skill_dir(skill_dir: &Path) -> FsResult<String> {
    let mut entries: Vec<PathBuf> = WalkDir::new(skill_dir)
        .follow_links(false)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .map(|entry| entry.path().to_path_buf())
        .collect();
    entries.sort();

    let mut hasher = Sha256::new();
    for path in entries {
        let relative = stdfs::diff_paths(&path, skill_dir)?;
        // Normalize separators so a hash taken on Windows matches one on Unix.
        let relative = relative.to_string_lossy().replace('\\', "/");
        hasher.update(relative.as_bytes());
        hasher.update([0u8]);
        hasher.update(stdfs::read(&path)?);
        hasher.update([0u8]);
    }

    Ok(format!("sha256:{:x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn hashing_notices_content_and_renames() {
        let tmp = TempDir::new().unwrap();
        fs::write(tmp.path().join("SKILL.md"), "hello").unwrap();
        let base = hash_skill_dir(tmp.path()).unwrap();

        fs::write(tmp.path().join("SKILL.md"), "goodbye").unwrap();
        assert_ne!(hash_skill_dir(tmp.path()).unwrap(), base);

        fs::write(tmp.path().join("SKILL.md"), "hello").unwrap();
        fs::write(tmp.path().join("extra.md"), "").unwrap();
        assert_ne!(hash_skill_dir(tmp.path()).unwrap(), base);
    }

    #[test]
    fn hashing_covers_nested_files() {
        let tmp = TempDir::new().unwrap();
        fs::write(tmp.path().join("SKILL.md"), "hello").unwrap();
        let before = hash_skill_dir(tmp.path()).unwrap();

        fs::create_dir(tmp.path().join("scripts")).unwrap();
        fs::write(tmp.path().join("scripts/run.sh"), "echo hi").unwrap();
        assert_ne!(hash_skill_dir(tmp.path()).unwrap(), before);
    }
}
