//! Copying selected skills into each provider directory.
//!
//! Every install is a plain copy — no symlinks, uniform across providers. dbt
//! writes a skill only into a destination it can create fresh: an occupied
//! directory is left exactly as it is, because dbt has no way to tell its own
//! earlier copy from a skill the user wrote by hand. Re-running is a safe no-op.

use std::path::{Path, PathBuf};

use dbt_common::tracing::dbt_emit::{emit_info_progress_message, emit_warn_log_message};
use dbt_common::{ErrorCode, FsResult, stdfs};
use dbt_telemetry::ProgressMessage;
use walkdir::WalkDir;

use crate::discover::{DiscoveredSkill, SkillOrigin};
use crate::hash::hash_skill_dir;

/// What happened to one skill in one destination directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstallOutcome {
    /// Freshly copied in.
    Installed,
    /// Already present and identical to its source; left alone.
    Unchanged,
    /// The source resolves to the destination, so there is nothing to copy.
    SourceIsDestination,
    /// Something else already occupies the directory; left untouched.
    SkippedDestinationOccupied,
}

impl InstallOutcome {
    /// The action word dbt prints for this outcome, or `None` when it is a
    /// no-op worth staying quiet about.
    ///
    /// Matches how package installation reports itself: one right-aligned
    /// action per item, and nothing at all for items that did not change.
    const fn action(self) -> Option<&'static str> {
        match self {
            InstallOutcome::Installed => Some("Installing"),
            // Nothing happened, or the skill already warned for itself.
            InstallOutcome::Unchanged
            | InstallOutcome::SourceIsDestination
            | InstallOutcome::SkippedDestinationOccupied => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct InstallReport {
    /// Destination directory, relative to the project root.
    pub destination: PathBuf,
    pub skill_name: String,
    pub outcome: InstallOutcome,
}

/// Announce what happened to one skill, in the same shape as the rest of dbt:
/// `Installing  <skill> (<package>) -> <destination>`.
fn report_progress(report: &InstallReport, origin: Option<&SkillOrigin>) {
    let Some(action) = report.outcome.action() else {
        return;
    };

    let package = match origin {
        Some(SkillOrigin::Package { name, version }) => match version {
            Some(version) => format!(" ({name} {version})"),
            None => format!(" ({name})"),
        },
        Some(SkillOrigin::Project) => " (this project)".to_string(),
        None => String::new(),
    };

    emit_info_progress_message(ProgressMessage::new_from_action_and_target(
        action.to_string(),
        format!(
            "{}{package} -> {}",
            report.skill_name,
            report.destination.display()
        ),
    ));
}

/// Install `selected` into every directory in `destinations`.
///
/// `project_root` anchors the (relative) destination directories. Returns one
/// report per skill per destination.
pub fn install_skills(
    project_root: &Path,
    destinations: &[PathBuf],
    selected: &[DiscoveredSkill],
) -> FsResult<Vec<InstallReport>> {
    let mut reports = Vec::new();

    for destination in destinations {
        let absolute = project_root.join(destination);

        for skill in selected {
            let report = InstallReport {
                destination: destination.clone(),
                skill_name: skill.name.clone(),
                outcome: install_one(&absolute, skill)?,
            };
            report_progress(&report, Some(&skill.origin));
            reports.push(report);
        }
    }

    Ok(reports)
}

fn install_one(destination: &Path, skill: &DiscoveredSkill) -> FsResult<InstallOutcome> {
    let target = destination.join(&skill.name);

    // A project may author its skills directly inside a provider directory. In
    // that case there is nothing to copy, and copying would be self-destructive.
    if same_path(&skill.dir, &target) {
        return Ok(InstallOutcome::SourceIsDestination);
    }

    if target.exists() {
        // Identical contents mean this is dbt's own copy from an earlier run (or
        // a second destination that symlinks to the first), so re-running stays
        // a no-op. Anything else is someone else's, and dbt will not guess:
        // overwriting a hand-written skill is unrecoverable, a stale copy is not.
        if hash_skill_dir(&target)? == hash_skill_dir(&skill.dir)? {
            return Ok(InstallOutcome::Unchanged);
        }

        emit_warn_log_message(
            ErrorCode::SkillDestinationOccupied,
            format!(
                "Not installing skill '{}' into {}: a directory is already there and does not \
                 match the version dbt would install. Delete it to let dbt install this skill.",
                skill.name,
                target.display()
            ),
        );
        return Ok(InstallOutcome::SkippedDestinationOccupied);
    }

    copy_skill(&skill.dir, &target)?;
    Ok(InstallOutcome::Installed)
}

/// Recursively copy a skill directory.
fn copy_skill(source: &Path, target: &Path) -> FsResult<()> {
    stdfs::create_dir_all(target)?;

    let entries = WalkDir::new(source)
        .follow_links(false)
        .into_iter()
        .filter_map(Result::ok);

    for entry in entries {
        let relative = stdfs::diff_paths(entry.path(), source)?;
        if relative.as_os_str().is_empty() {
            continue;
        }
        copy_entry(entry.path(), &target.join(&relative), entry.file_type())?;
    }

    Ok(())
}

/// Recreate one walked entry at `destination`. Anything that is neither a file
/// nor a directory (a symlink inside the source tree, say) is skipped.
fn copy_entry(source: &Path, destination: &Path, file_type: std::fs::FileType) -> FsResult<()> {
    if file_type.is_dir() {
        return stdfs::create_dir_all(destination);
    }
    if !file_type.is_file() {
        return Ok(());
    }

    if let Some(parent) = destination.parent() {
        stdfs::create_dir_all(parent)?;
    }
    stdfs::copy(source, destination).map(|_| ())
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

    fn outcomes(reports: &[InstallReport]) -> Vec<InstallOutcome> {
        reports.iter().map(|r| r.outcome).collect()
    }

    #[test]
    fn installs_flat_into_the_destination() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let selected = vec![make_source(root, "alpha", "body")];

        let reports = install_skills(root, &[PathBuf::from(DEST)], &selected).unwrap();
        assert_eq!(outcomes(&reports), vec![InstallOutcome::Installed]);
        assert!(root.join(DEST).join("alpha").join(SKILL_FILE).is_file());
    }

    #[test]
    fn bundled_files_travel_with_the_skill() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let skill = make_source(root, "alpha", "body");
        fs::create_dir_all(skill.dir.join("scripts")).unwrap();
        fs::write(skill.dir.join("scripts/run.sh"), "echo hi").unwrap();

        install_skills(root, &[PathBuf::from(DEST)], &[skill]).unwrap();
        assert!(root.join(DEST).join("alpha/scripts/run.sh").is_file());
    }

    #[test]
    fn re_running_is_a_no_op() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let selected = vec![make_source(root, "alpha", "body")];

        install_skills(root, &[PathBuf::from(DEST)], &selected).unwrap();
        let reports = install_skills(root, &[PathBuf::from(DEST)], &selected).unwrap();
        assert_eq!(outcomes(&reports), vec![InstallOutcome::Unchanged]);
    }

    #[test]
    fn an_occupied_destination_is_left_alone_whoever_owns_it() {
        // dbt keeps no record of what it installed, so it cannot tell its own
        // stale copy from a skill the user wrote by hand, and refuses to touch
        // either. Losing an update is recoverable; losing the user's work is not.
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let skill = make_source(root, "alpha", "body");

        let occupied = root.join(DEST).join("alpha");
        fs::create_dir_all(&occupied).unwrap();
        fs::write(occupied.join(SKILL_FILE), "hand written").unwrap();

        let reports = install_skills(root, &[PathBuf::from(DEST)], &[skill]).unwrap();
        assert_eq!(
            outcomes(&reports),
            vec![InstallOutcome::SkippedDestinationOccupied]
        );
        assert_eq!(
            fs::read_to_string(occupied.join(SKILL_FILE)).unwrap(),
            "hand written"
        );
    }

    #[test]
    fn a_changed_source_does_not_overwrite_the_installed_copy() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let skill = make_source(root, "alpha", "first");
        install_skills(root, &[PathBuf::from(DEST)], &[skill.clone()]).unwrap();

        fs::write(
            skill.dir.join(SKILL_FILE),
            "---\nname: alpha\ndescription: A skill.\n---\nsecond",
        )
        .unwrap();
        let reports = install_skills(root, &[PathBuf::from(DEST)], &[skill]).unwrap();

        assert_eq!(
            outcomes(&reports),
            vec![InstallOutcome::SkippedDestinationOccupied]
        );
        let installed = fs::read_to_string(root.join(DEST).join("alpha").join(SKILL_FILE)).unwrap();
        assert!(installed.ends_with("first"), "{installed}");
    }

    #[test]
    fn nothing_is_removed_from_a_destination() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let stale = root.join(DEST).join("no-longer-shipped");
        fs::create_dir_all(&stale).unwrap();
        fs::write(stale.join(SKILL_FILE), "still here").unwrap();

        let reports = install_skills(root, &[PathBuf::from(DEST)], &[]).unwrap();
        assert!(reports.is_empty());
        assert!(stale.join(SKILL_FILE).is_file());
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

        let reports = install_skills(root, &[PathBuf::from(DEST)], &[skill]).unwrap();
        assert_eq!(
            outcomes(&reports),
            vec![InstallOutcome::SourceIsDestination]
        );
    }

    #[test]
    fn writes_into_every_destination() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let selected = vec![make_source(root, "alpha", "body")];
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
    #[cfg(unix)]
    fn two_destinations_that_symlink_to_one_directory_install_once() {
        // dbt-core's own repo symlinks .claude/skills -> ../.agents/skills, so a
        // user with `ai_provider: [wizard, claude]` has two destination paths
        // naming one directory. The second pass must recognize the copy the first
        // pass just wrote as identical, not report the destination as occupied.
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let selected = vec![make_source(root, "alpha", "body")];

        fs::create_dir_all(root.join(DEST)).unwrap();
        fs::create_dir_all(root.join(".claude")).unwrap();
        std::os::unix::fs::symlink("../.agents/skills", root.join(".claude/skills")).unwrap();

        let destinations = [PathBuf::from(DEST), PathBuf::from(".claude/skills")];
        let reports = install_skills(root, &destinations, &selected).unwrap();

        assert_eq!(
            outcomes(&reports),
            vec![InstallOutcome::Installed, InstallOutcome::Unchanged]
        );
        let installed: Vec<_> = fs::read_dir(root.join(DEST))
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(installed, vec!["alpha".to_string()]);
    }

    #[test]
    #[cfg(unix)]
    fn symlinks_inside_a_skill_are_not_followed_out_of_the_tree() {
        // A package can ship whatever it likes inside its own skill directory,
        // including a symlink to somewhere outside the project. Copying such a
        // link — or worse, its target — would leak files into the install.
        // `copy_entry` only handles regular files and directories; this pins
        // that, since it is a security boundary and not just tidiness.
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        fs::write(root.join("secret.txt"), "TOP SECRET").unwrap();

        let skill = make_source(root, "leaky", "body");
        std::os::unix::fs::symlink(root.join("secret.txt"), skill.dir.join("stolen.txt")).unwrap();
        std::os::unix::fs::symlink(root, skill.dir.join("everything")).unwrap();

        install_skills(root, &[PathBuf::from(DEST)], &[skill]).unwrap();

        let installed = root.join(DEST).join("leaky");
        assert!(installed.join(SKILL_FILE).is_file());
        assert!(!installed.join("stolen.txt").exists());
        assert!(!installed.join("everything").exists());
    }
}
