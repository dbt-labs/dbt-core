//! The `.provenance` sidecar dbt writes into every skill directory it installs.
//!
//! Skill harnesses only read `SKILL.md`, so the sidecar is inert to them. It is
//! dbt's only record of which installed skills are its own and whether the user
//! has edited them since — there is no central state file.

use std::path::{Path, PathBuf};

use dbt_common::{ErrorCode, FsResult, fs_err, stdfs};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use walkdir::WalkDir;

/// Sidecar filename. Generic on purpose; `managed_by` is what identifies it as
/// dbt's, so dbt ignores any `.provenance` it didn't write.
pub const PROVENANCE_FILE: &str = ".provenance";
/// Value of `managed_by` in sidecars dbt owns.
pub const MANAGED_BY_DBT: &str = "dbt";

/// A skill that lost a name collision to the skill this sidecar describes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShadowedSkill {
    /// `None` when the shadowed skill came from the project itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub package: Option<String>,
    pub source_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Provenance {
    pub managed_by: String,
    /// `package` or `project`.
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub package: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    pub source_path: String,
    pub install_mode: String,
    /// `sha256:…` over the source directory at install time.
    pub content_hash: String,
    pub installed_at: String,
    /// Same-named skills that were not installed because this one won.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub shadowed: Vec<ShadowedSkill>,
}

impl Provenance {
    pub fn is_dbt_managed(&self) -> bool {
        self.managed_by == MANAGED_BY_DBT
    }
}

/// Read the sidecar from an installed skill directory.
///
/// Returns `Ok(None)` when there is no sidecar (a user-authored skill), or when
/// the sidecar exists but isn't dbt's — in both cases the directory is not ours
/// to touch. An unreadable or unparseable sidecar is likewise treated as
/// "not ours" rather than an error.
pub fn read_provenance(skill_dir: &Path) -> Option<Provenance> {
    let path = skill_dir.join(PROVENANCE_FILE);
    let contents = stdfs::read_to_string(&path).ok()?;
    let provenance: Provenance = dbt_yaml::from_str(&contents).ok()?;
    provenance.is_dbt_managed().then_some(provenance)
}

/// Write the sidecar into an installed skill directory.
pub fn write_provenance(skill_dir: &Path, provenance: &Provenance) -> FsResult<()> {
    let serialized = dbt_yaml::to_string(provenance).map_err(|e| {
        fs_err!(
            ErrorCode::IoError,
            "Failed to serialize skill provenance for {}: {}",
            skill_dir.display(),
            e
        )
    })?;
    let header = "# Written by dbt. Identifies a dbt-installed skill so `dbt deps`\n\
                  # and `dbt clean` can update or remove it. Skill harnesses ignore this file.\n";
    stdfs::write(
        skill_dir.join(PROVENANCE_FILE),
        format!("{header}{serialized}"),
    )
}

/// Hash the contents of a skill directory.
///
/// The `.provenance` sidecar is excluded so that the hash of an installed copy
/// is directly comparable to the hash of its source. Both relative paths and
/// file bytes feed the hash, so a rename is a change.
pub fn hash_skill_dir(skill_dir: &Path) -> FsResult<String> {
    let mut entries: Vec<PathBuf> = WalkDir::new(skill_dir)
        .follow_links(false)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .map(|entry| entry.path().to_path_buf())
        .filter(|path| path.file_name().and_then(|n| n.to_str()) != Some(PROVENANCE_FILE))
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

    fn sample() -> Provenance {
        Provenance {
            managed_by: MANAGED_BY_DBT.to_string(),
            source: "package".to_string(),
            package: Some("dbt_project_evaluator".to_string()),
            version: Some("1.5.0".to_string()),
            source_path: "skills/how-to".to_string(),
            install_mode: "copy".to_string(),
            content_hash: "sha256:abc".to_string(),
            installed_at: "2026-06-30T00:00:00Z".to_string(),
            shadowed: vec![],
        }
    }

    #[test]
    fn round_trips_through_yaml() {
        let tmp = TempDir::new().unwrap();
        write_provenance(tmp.path(), &sample()).unwrap();
        assert_eq!(read_provenance(tmp.path()), Some(sample()));
    }

    #[test]
    fn a_directory_without_a_sidecar_is_not_ours() {
        let tmp = TempDir::new().unwrap();
        assert_eq!(read_provenance(tmp.path()), None);
    }

    #[test]
    fn a_sidecar_dbt_did_not_write_is_not_ours() {
        let tmp = TempDir::new().unwrap();
        fs::write(
            tmp.path().join(PROVENANCE_FILE),
            "managed_by: some-other-tool\nsource: project\nsource_path: x\n\
             install_mode: copy\ncontent_hash: sha256:x\ninstalled_at: now\n",
        )
        .unwrap();
        assert_eq!(read_provenance(tmp.path()), None);
    }

    #[test]
    fn hashing_ignores_the_sidecar_but_notices_content_and_renames() {
        let tmp = TempDir::new().unwrap();
        fs::write(tmp.path().join("SKILL.md"), "hello").unwrap();
        let base = hash_skill_dir(tmp.path()).unwrap();

        write_provenance(tmp.path(), &sample()).unwrap();
        assert_eq!(hash_skill_dir(tmp.path()).unwrap(), base);

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
