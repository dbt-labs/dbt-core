//! Copying selected skills into each provider directory, and pruning the ones
//! dbt previously installed that are no longer wanted.
//!
//! Every install is a plain copy — no symlinks, uniform across providers — plus
//! a `.provenance` sidecar. Re-running is a safe no-op: each destination is
//! reconciled against what dbt installed last time.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use chrono::Utc;
use dbt_common::tracing::dbt_emit::emit_warn_log_message;
use dbt_common::{ErrorCode, FsResult, stdfs};
use walkdir::WalkDir;

use crate::config::SelectedSkill;
use crate::discover::SkillOrigin;
use crate::provenance::{
    MANAGED_BY_DBT, PROVENANCE_FILE, Provenance, hash_skill_dir, read_provenance, write_provenance,
};

/// What happened to one skill in one destination directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstallOutcome {
    /// Freshly copied in.
    Installed,
    /// Already present and identical to its source; left alone.
    Unchanged,
    /// Already present but its source changed; overwritten.
    Updated,
    /// The source resolves to the destination, so there is nothing to copy.
    SourceIsDestination,
    /// A dbt-installed copy the user has since edited; left untouched.
    SkippedUserModified,
    /// A user-authored skill already owns this directory; left untouched.
    SkippedNotOurs,
    /// A dbt-installed copy that is no longer wanted; removed.
    Pruned,
}

#[derive(Debug, Clone)]
pub struct InstallReport {
    pub destination: PathBuf,
    pub skill_name: String,
    pub outcome: InstallOutcome,
}

/// Install `selected` into every directory in `destinations`, then prune.
///
/// `project_root` anchors the (relative) destination directories. Returns one
/// report per skill per destination, plus one per pruned directory.
pub fn install_skills(
    project_root: &Path,
    destinations: &[PathBuf],
    selected: &[SelectedSkill],
) -> FsResult<Vec<InstallReport>> {
    let mut reports = Vec::new();

    for destination in destinations {
        let destination = project_root.join(destination);
        let mut wanted = BTreeSet::new();

        for selection in selected {
            wanted.insert(selection.skill.name.clone());
            let outcome = install_one(&destination, selection)?;
            reports.push(InstallReport {
                destination: destination.clone(),
                skill_name: selection.skill.name.clone(),
                outcome,
            });
        }

        reports.extend(prune_destination(&destination, &wanted)?);
    }

    Ok(reports)
}

fn install_one(destination: &Path, selection: &SelectedSkill) -> FsResult<InstallOutcome> {
    let skill = &selection.skill;
    let target = destination.join(&skill.name);

    // A project may author its skills directly inside a provider directory. In
    // that case there is nothing to copy, and copying would be self-destructive.
    if same_path(&skill.dir, &target) {
        return Ok(InstallOutcome::SourceIsDestination);
    }

    let source_hash = hash_skill_dir(&skill.dir)?;

    if target.exists() {
        let Some(existing) = read_provenance(&target) else {
            emit_warn_log_message(
                ErrorCode::SkillDestinationOccupied,
                format!(
                    "Not installing skill '{}' into {}: a skill dbt does not manage is already there.",
                    skill.name,
                    target.display()
                ),
            );
            return Ok(InstallOutcome::SkippedNotOurs);
        };

        let current_hash = hash_skill_dir(&target)?;
        if current_hash != existing.content_hash {
            emit_warn_log_message(
                ErrorCode::SkillModifiedByUser,
                format!(
                    "Leaving skill '{}' in {} alone: it has been edited since dbt installed it.",
                    skill.name,
                    target.display()
                ),
            );
            return Ok(InstallOutcome::SkippedUserModified);
        }

        if existing.content_hash == source_hash && existing.shadowed == selection.shadowed {
            return Ok(InstallOutcome::Unchanged);
        }

        stdfs::remove_dir_all(&target)?;
        copy_skill(&skill.dir, &target)?;
        write_provenance(&target, &build_provenance(selection, &source_hash))?;
        return Ok(InstallOutcome::Updated);
    }

    copy_skill(&skill.dir, &target)?;
    write_provenance(&target, &build_provenance(selection, &source_hash))?;
    Ok(InstallOutcome::Installed)
}

/// Remove dbt-installed skills in `destination` that are no longer wanted.
///
/// Skills without a dbt `.provenance` are the user's and are never touched, and
/// dbt-installed copies the user has edited are left in place too.
fn prune_destination(
    destination: &Path,
    wanted: &BTreeSet<String>,
) -> FsResult<Vec<InstallReport>> {
    let mut reports = Vec::new();
    if !destination.is_dir() {
        return Ok(reports);
    }

    for entry in stdfs::read_dir(destination)?.filter_map(Result::ok) {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if wanted.contains(name) {
            continue;
        }
        let Some(existing) = read_provenance(&path) else {
            continue;
        };

        let outcome = if hash_skill_dir(&path)? == existing.content_hash {
            stdfs::remove_dir_all(&path)?;
            InstallOutcome::Pruned
        } else {
            emit_warn_log_message(
                ErrorCode::SkillModifiedByUser,
                format!(
                    "Not removing skill '{}' from {}: it has been edited since dbt installed it.",
                    name,
                    destination.display()
                ),
            );
            InstallOutcome::SkippedUserModified
        };

        reports.push(InstallReport {
            destination: destination.to_path_buf(),
            skill_name: name.to_string(),
            outcome,
        });
    }

    Ok(reports)
}

/// Remove every dbt-installed skill under `destinations`, regardless of whether
/// it is still wanted. Used by `dbt clean`.
pub fn prune_all(project_root: &Path, destinations: &[PathBuf]) -> FsResult<Vec<InstallReport>> {
    let mut reports = Vec::new();
    for destination in destinations {
        reports.extend(prune_destination(
            &project_root.join(destination),
            &BTreeSet::new(),
        )?);
    }
    Ok(reports)
}

fn build_provenance(selection: &SelectedSkill, content_hash: &str) -> Provenance {
    let (source, package, version) = match &selection.skill.origin {
        SkillOrigin::Project => ("project", None, None),
        SkillOrigin::Package { name, version } => ("package", Some(name.clone()), version.clone()),
    };

    Provenance {
        managed_by: MANAGED_BY_DBT.to_string(),
        source: source.to_string(),
        package,
        version,
        source_path: selection
            .skill
            .source_path
            .to_string_lossy()
            .replace('\\', "/"),
        install_mode: "copy".to_string(),
        content_hash: content_hash.to_string(),
        installed_at: Utc::now().to_rfc3339(),
        shadowed: selection.shadowed.clone(),
    }
}

/// Recursively copy a skill directory. Any `.provenance` in the source is
/// dropped so a re-published package can't smuggle in a fake one.
fn copy_skill(source: &Path, target: &Path) -> FsResult<()> {
    stdfs::create_dir_all(target)?;

    for entry in WalkDir::new(source)
        .follow_links(false)
        .into_iter()
        .filter_map(Result::ok)
    {
        let relative = stdfs::diff_paths(entry.path(), source)?;
        if relative.as_os_str().is_empty() {
            continue;
        }
        if entry.file_name() == PROVENANCE_FILE {
            continue;
        }

        let destination = target.join(&relative);
        if entry.file_type().is_dir() {
            stdfs::create_dir_all(&destination)?;
        } else if entry.file_type().is_file() {
            if let Some(parent) = destination.parent() {
                stdfs::create_dir_all(parent)?;
            }
            stdfs::copy(entry.path(), &destination)?;
        }
    }

    Ok(())
}

/// Compare two paths by their canonical form, falling back to a literal
/// comparison when either does not exist yet.
fn same_path(left: &Path, right: &Path) -> bool {
    match (stdfs::canonicalize(left), stdfs::canonicalize(right)) {
        (Ok(left), Ok(right)) => left == right,
        _ => left == right,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discover::DiscoveredSkill;
    use crate::validate::{SKILL_FILE, SkillFrontmatter};
    use std::fs;
    use tempfile::TempDir;

    const DEST: &str = ".agents/skills";

    fn make_source(root: &Path, name: &str, body: &str) -> DiscoveredSkill {
        let dir = root.join("skills").join(name);
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join(SKILL_FILE),
            format!("---\nname: {name}\ndescription: A skill.\n---\n{body}"),
        )
        .unwrap();

        DiscoveredSkill {
            name: name.to_string(),
            dir,
            source_path: PathBuf::from(format!("skills/{name}")),
            origin: SkillOrigin::Package {
                name: "some_pkg".to_string(),
                version: Some("1.0.0".to_string()),
            },
            fqn: vec!["some_pkg".to_string(), name.to_string()],
            frontmatter: SkillFrontmatter {
                name: name.to_string(),
                description: "A skill.".to_string(),
            },
            precedence: 1,
        }
    }

    fn selection(skill: DiscoveredSkill) -> SelectedSkill {
        SelectedSkill {
            skill,
            shadowed: vec![],
        }
    }

    fn outcomes(reports: &[InstallReport]) -> Vec<InstallOutcome> {
        reports.iter().map(|r| r.outcome).collect()
    }

    #[test]
    fn installs_flat_with_a_provenance_sidecar() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let selected = vec![selection(make_source(root, "alpha", "body"))];

        let reports = install_skills(root, &[PathBuf::from(DEST)], &selected).unwrap();
        assert_eq!(outcomes(&reports), vec![InstallOutcome::Installed]);

        let installed = root.join(DEST).join("alpha");
        assert!(installed.join(SKILL_FILE).is_file());
        let provenance = read_provenance(&installed).unwrap();
        assert_eq!(provenance.source, "package");
        assert_eq!(provenance.package.as_deref(), Some("some_pkg"));
        assert_eq!(provenance.install_mode, "copy");
        assert_eq!(provenance.source_path, "skills/alpha");
    }

    #[test]
    fn bundled_files_travel_with_the_skill() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let skill = make_source(root, "alpha", "body");
        fs::create_dir_all(skill.dir.join("scripts")).unwrap();
        fs::write(skill.dir.join("scripts/run.sh"), "echo hi").unwrap();

        install_skills(root, &[PathBuf::from(DEST)], &[selection(skill)]).unwrap();
        assert!(root.join(DEST).join("alpha/scripts/run.sh").is_file());
    }

    #[test]
    fn re_running_is_a_no_op() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let selected = vec![selection(make_source(root, "alpha", "body"))];

        install_skills(root, &[PathBuf::from(DEST)], &selected).unwrap();
        let reports = install_skills(root, &[PathBuf::from(DEST)], &selected).unwrap();
        assert_eq!(outcomes(&reports), vec![InstallOutcome::Unchanged]);
    }

    #[test]
    fn a_changed_source_overwrites_the_installed_copy() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let skill = make_source(root, "alpha", "first");
        install_skills(root, &[PathBuf::from(DEST)], &[selection(skill.clone())]).unwrap();

        fs::write(
            skill.dir.join(SKILL_FILE),
            "---\nname: alpha\ndescription: A skill.\n---\nsecond",
        )
        .unwrap();
        let reports = install_skills(root, &[PathBuf::from(DEST)], &[selection(skill)]).unwrap();

        assert_eq!(outcomes(&reports), vec![InstallOutcome::Updated]);
        let installed = fs::read_to_string(root.join(DEST).join("alpha").join(SKILL_FILE)).unwrap();
        assert!(installed.ends_with("second"));
    }

    #[test]
    fn a_user_edited_copy_is_never_stomped() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let skill = make_source(root, "alpha", "body");
        install_skills(root, &[PathBuf::from(DEST)], &[selection(skill.clone())]).unwrap();

        let installed = root.join(DEST).join("alpha");
        fs::write(installed.join(SKILL_FILE), "the user rewrote this").unwrap();

        let reports = install_skills(root, &[PathBuf::from(DEST)], &[selection(skill)]).unwrap();
        assert_eq!(
            outcomes(&reports),
            vec![InstallOutcome::SkippedUserModified]
        );
        assert_eq!(
            fs::read_to_string(installed.join(SKILL_FILE)).unwrap(),
            "the user rewrote this"
        );
    }

    #[test]
    fn a_user_authored_skill_of_the_same_name_is_never_touched() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let skill = make_source(root, "alpha", "body");

        let occupied = root.join(DEST).join("alpha");
        fs::create_dir_all(&occupied).unwrap();
        fs::write(occupied.join(SKILL_FILE), "hand written").unwrap();

        let reports = install_skills(root, &[PathBuf::from(DEST)], &[selection(skill)]).unwrap();
        assert_eq!(outcomes(&reports), vec![InstallOutcome::SkippedNotOurs]);
        assert_eq!(
            fs::read_to_string(occupied.join(SKILL_FILE)).unwrap(),
            "hand written"
        );
    }

    #[test]
    fn a_skill_that_is_no_longer_wanted_is_pruned() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let alpha = selection(make_source(root, "alpha", "body"));
        let beta = selection(make_source(root, "beta", "body"));

        install_skills(root, &[PathBuf::from(DEST)], &[alpha.clone(), beta]).unwrap();
        assert!(root.join(DEST).join("beta").is_dir());

        let reports = install_skills(root, &[PathBuf::from(DEST)], &[alpha]).unwrap();
        assert!(
            reports
                .iter()
                .any(|r| r.skill_name == "beta" && r.outcome == InstallOutcome::Pruned)
        );
        assert!(!root.join(DEST).join("beta").exists());
    }

    #[test]
    fn pruning_leaves_user_authored_skills_alone() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let mine = root.join(DEST).join("hand-written");
        fs::create_dir_all(&mine).unwrap();
        fs::write(mine.join(SKILL_FILE), "mine").unwrap();

        install_skills(root, &[PathBuf::from(DEST)], &[]).unwrap();
        assert!(mine.join(SKILL_FILE).is_file());
    }

    #[test]
    fn a_skill_authored_in_the_provider_dir_is_not_copied_onto_itself() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();

        let dir = root.join(DEST).join("alpha");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join(SKILL_FILE),
            "---\nname: alpha\ndescription: A skill.\n---\n",
        )
        .unwrap();

        let skill = DiscoveredSkill {
            name: "alpha".to_string(),
            dir: dir.clone(),
            source_path: PathBuf::from(".agents/skills/alpha"),
            origin: SkillOrigin::Project,
            fqn: vec!["root_project".to_string(), "alpha".to_string()],
            frontmatter: SkillFrontmatter {
                name: "alpha".to_string(),
                description: "A skill.".to_string(),
            },
            precedence: 0,
        };

        let reports = install_skills(root, &[PathBuf::from(DEST)], &[selection(skill)]).unwrap();
        assert_eq!(
            outcomes(&reports),
            vec![InstallOutcome::SourceIsDestination]
        );
        assert!(!dir.join(PROVENANCE_FILE).exists());
    }

    #[test]
    fn writes_into_every_destination() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let selected = vec![selection(make_source(root, "alpha", "body"))];
        let destinations = [PathBuf::from(DEST), PathBuf::from(".claude/skills")];

        install_skills(root, &destinations, &selected).unwrap();
        assert!(root.join(DEST).join("alpha").join(SKILL_FILE).is_file());
        assert!(
            root.join(".claude/skills")
                .join("alpha")
                .join(SKILL_FILE)
                .is_file()
        );
    }

    #[test]
    fn prune_all_removes_dbt_skills_but_not_user_skills() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        install_skills(
            root,
            &[PathBuf::from(DEST)],
            &[selection(make_source(root, "alpha", "body"))],
        )
        .unwrap();

        let mine = root.join(DEST).join("hand-written");
        fs::create_dir_all(&mine).unwrap();
        fs::write(mine.join(SKILL_FILE), "mine").unwrap();

        prune_all(root, &[PathBuf::from(DEST)]).unwrap();
        assert!(!root.join(DEST).join("alpha").exists());
        assert!(mine.join(SKILL_FILE).is_file());
    }
}
