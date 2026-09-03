//! Copying selected skills into dbt's namespace in each provider directory.
//!
//! Every install is a plain copy into `dbt-<name>/`, with the copy's frontmatter
//! `name` rewritten to match. Any `dbt-*` directory is dbt's, which is the whole
//! ownership mechanism — nothing is recorded and nothing is read back. dbt
//! overwrites freely inside its own namespace and never touches anything outside
//! it. Re-running is a safe no-op.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use dbt_common::tracing::dbt_emit::emit_info_progress_message;
use dbt_common::{FsResult, stdfs};
use dbt_telemetry::ProgressMessage;
use walkdir::WalkDir;

use crate::discover::{DiscoveredSkill, SkillOrigin};
use crate::hash::hash_skill_dir_excluding_skill_md;
use crate::namespace;
use crate::validate::SKILL_FILE;

/// What happened to one skill in one destination directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstallOutcome {
    /// Freshly copied in.
    Installed,
    /// Already present and identical to what dbt would write; left alone.
    Unchanged,
    /// Already present in dbt's namespace but different; overwritten.
    Updated,
    /// The source resolves to the destination, so there is nothing to copy.
    SourceIsDestination,
    /// A directory in dbt's namespace that is no longer wanted; removed.
    Pruned,
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
            InstallOutcome::Updated => Some("Updating"),
            InstallOutcome::Pruned => Some("Removing"),
            InstallOutcome::Unchanged | InstallOutcome::SourceIsDestination => None,
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

/// Install `selected` into every directory in `destinations`, then prune.
///
/// `InstallReport::skill_name` carries the **namespaced** directory name, since
/// that is what appears on disk and in `dbt clean` output.
pub fn install_skills(
    project_root: &Path,
    destinations: &[PathBuf],
    selected: &[DiscoveredSkill],
) -> FsResult<Vec<InstallReport>> {
    let mut reports = Vec::new();

    for destination in destinations {
        let absolute = project_root.join(destination);
        let mut wanted = BTreeSet::new();

        for skill in selected {
            let dir_name = namespace::namespaced(&skill.name);
            wanted.insert(dir_name.clone());
            let report = InstallReport {
                destination: destination.clone(),
                skill_name: dir_name,
                outcome: install_one(&absolute, skill)?,
            };
            report_progress(&report, Some(&skill.origin));
            reports.push(report);
        }

        reports.extend(prune_destination(&absolute, destination, &wanted)?);
    }

    Ok(reports)
}

fn install_one(destination: &Path, skill: &DiscoveredSkill) -> FsResult<InstallOutcome> {
    let target = destination.join(namespace::namespaced(&skill.name));

    // A project may author its skills directly inside a provider directory.
    // Compare against both the namespaced target and the skill's own un-prefixed
    // location: prefixing would otherwise copy an in-place skill beside itself
    // and ship it twice.
    if same_path(&skill.dir, &target) || same_path(&skill.dir, &destination.join(&skill.name)) {
        return Ok(InstallOutcome::SourceIsDestination);
    }

    let rendered = render(skill)?;

    if target.exists() {
        if installed_matches(skill, &target, &rendered)? {
            return Ok(InstallOutcome::Unchanged);
        }
        // Inside dbt's namespace, so dbt replaces it without asking. There is no
        // recorded state to tell a stale copy from a user edit, and the visible
        // `dbt-` prefix is what tells users this directory is not theirs.
        stdfs::remove_dir_all(&target)?;
        write_skill(skill, &target, &rendered)?;
        return Ok(InstallOutcome::Updated);
    }

    write_skill(skill, &target, &rendered)?;
    Ok(InstallOutcome::Installed)
}

/// The exact `SKILL.md` bytes dbt writes for this skill.
fn render(skill: &DiscoveredSkill) -> FsResult<String> {
    let source_md = stdfs::read_to_string(skill.dir.join(SKILL_FILE))?;
    namespace::rewrite_name(&source_md, &namespace::namespaced(&skill.name))
}

/// Whether the installed copy is already exactly what dbt would write.
///
/// `SKILL.md` is compared against the regenerated bytes because the installed
/// copy carries a rewritten `name`; every other file must match the source.
fn installed_matches(skill: &DiscoveredSkill, target: &Path, rendered: &str) -> FsResult<bool> {
    if stdfs::read_to_string(target.join(SKILL_FILE)).unwrap_or_default() != rendered {
        return Ok(false);
    }
    Ok(
        hash_skill_dir_excluding_skill_md(&skill.dir)?
            == hash_skill_dir_excluding_skill_md(target)?,
    )
}

/// Copy the skill in, then overwrite its `SKILL.md` with the namespaced version.
/// The source tree is never modified.
fn write_skill(skill: &DiscoveredSkill, target: &Path, rendered: &str) -> FsResult<()> {
    copy_skill(&skill.dir, target)?;
    stdfs::write(target.join(SKILL_FILE), rendered)
}

/// Remove directories in dbt's namespace that are no longer wanted.
///
/// Anything without the `dbt-` prefix is the user's and is never a candidate.
fn prune_destination(
    destination: &Path,
    relative_destination: &Path,
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
        if wanted.contains(name) || !namespace::is_dbt_owned(name) {
            continue;
        }

        stdfs::remove_dir_all(&path)?;
        let report = InstallReport {
            destination: relative_destination.to_path_buf(),
            skill_name: name.to_string(),
            outcome: InstallOutcome::Pruned,
        };
        report_progress(&report, None);
        reports.push(report);
    }

    Ok(reports)
}

/// Remove dbt's entire namespace under `destinations`. Used by `dbt clean`.
pub fn prune_all(project_root: &Path, destinations: &[PathBuf]) -> FsResult<Vec<InstallReport>> {
    let mut reports = Vec::new();
    for destination in destinations {
        reports.extend(prune_destination(
            &project_root.join(destination),
            destination,
            &BTreeSet::new(),
        )?);
    }
    Ok(reports)
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
    use crate::validate::SkillFrontmatter;
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

    fn installed_md(root: &Path, dir: &str) -> String {
        fs::read_to_string(root.join(DEST).join(dir).join(SKILL_FILE)).unwrap()
    }

    #[test]
    fn installs_into_the_dbt_namespace_and_rewrites_the_name() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let selected = vec![make_source(root, "alpha", "body")];

        let reports = install_skills(root, &[PathBuf::from(DEST)], &selected).unwrap();
        assert_eq!(outcomes(&reports), vec![InstallOutcome::Installed]);

        // Namespaced directory, and the declared name agrees with it.
        assert!(root.join(DEST).join("dbt-alpha").join(SKILL_FILE).is_file());
        assert!(!root.join(DEST).join("alpha").exists());
        assert!(installed_md(root, "dbt-alpha").contains("name: dbt-alpha"));
        // The source is untouched.
        assert!(
            fs::read_to_string(root.join("skills/alpha").join(SKILL_FILE))
                .unwrap()
                .contains("name: alpha")
        );
    }

    #[test]
    fn bundled_files_travel_with_the_skill() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let skill = make_source(root, "alpha", "body");
        fs::create_dir_all(skill.dir.join("scripts")).unwrap();
        fs::write(skill.dir.join("scripts/run.sh"), "echo hi").unwrap();

        install_skills(root, &[PathBuf::from(DEST)], &[skill]).unwrap();
        assert!(root.join(DEST).join("dbt-alpha/scripts/run.sh").is_file());
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
    fn a_changed_source_updates_the_installed_copy() {
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

        assert_eq!(outcomes(&reports), vec![InstallOutcome::Updated]);
        assert!(installed_md(root, "dbt-alpha").ends_with("second"));
    }

    #[test]
    fn a_changed_bundled_file_also_updates_the_installed_copy() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let skill = make_source(root, "alpha", "body");
        fs::create_dir_all(skill.dir.join("scripts")).unwrap();
        fs::write(skill.dir.join("scripts/run.sh"), "echo one").unwrap();
        install_skills(root, &[PathBuf::from(DEST)], &[skill.clone()]).unwrap();

        fs::write(skill.dir.join("scripts/run.sh"), "echo two").unwrap();
        let reports = install_skills(root, &[PathBuf::from(DEST)], &[skill]).unwrap();

        assert_eq!(outcomes(&reports), vec![InstallOutcome::Updated]);
        assert_eq!(
            fs::read_to_string(root.join(DEST).join("dbt-alpha/scripts/run.sh")).unwrap(),
            "echo two"
        );
    }

    #[test]
    fn dbt_overwrites_edits_inside_its_own_namespace() {
        // Deliberate: `dbt-*` is dbt's territory, like dbt_packages/. Without
        // recorded state dbt cannot distinguish a user edit from a stale copy,
        // and the visible namespace is the warning.
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let skill = make_source(root, "alpha", "body");
        install_skills(root, &[PathBuf::from(DEST)], &[skill.clone()]).unwrap();

        let installed = root.join(DEST).join("dbt-alpha");
        fs::write(installed.join(SKILL_FILE), "the user rewrote this").unwrap();

        let reports = install_skills(root, &[PathBuf::from(DEST)], &[skill]).unwrap();
        assert_eq!(outcomes(&reports), vec![InstallOutcome::Updated]);
        assert!(installed_md(root, "dbt-alpha").contains("name: dbt-alpha"));
    }

    #[test]
    fn a_user_authored_skill_of_the_same_name_cannot_collide() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let skill = make_source(root, "alpha", "body");

        // The user's own `alpha` lives beside, not under, dbt's namespace.
        let mine = root.join(DEST).join("alpha");
        fs::create_dir_all(&mine).unwrap();
        fs::write(mine.join(SKILL_FILE), "hand written").unwrap();

        let reports = install_skills(root, &[PathBuf::from(DEST)], &[skill]).unwrap();
        assert_eq!(outcomes(&reports), vec![InstallOutcome::Installed]);
        assert_eq!(
            fs::read_to_string(mine.join(SKILL_FILE)).unwrap(),
            "hand written"
        );
        assert!(root.join(DEST).join("dbt-alpha").is_dir());
    }

    #[test]
    fn a_skill_that_is_no_longer_wanted_is_pruned() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let alpha = make_source(root, "alpha", "body");
        let beta = make_source(root, "beta", "body");

        install_skills(root, &[PathBuf::from(DEST)], &[alpha.clone(), beta]).unwrap();
        assert!(root.join(DEST).join("dbt-beta").is_dir());

        let reports = install_skills(root, &[PathBuf::from(DEST)], &[alpha]).unwrap();
        assert!(
            reports
                .iter()
                .any(|r| r.skill_name == "dbt-beta" && r.outcome == InstallOutcome::Pruned)
        );
        assert!(!root.join(DEST).join("dbt-beta").exists());
    }

    #[test]
    fn pruning_leaves_everything_outside_the_namespace_alone() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let mine = root.join(DEST).join("hand-written");
        fs::create_dir_all(&mine).unwrap();
        fs::write(mine.join(SKILL_FILE), "mine").unwrap();

        install_skills(root, &[PathBuf::from(DEST)], &[]).unwrap();
        assert!(mine.join(SKILL_FILE).is_file());
    }

    #[test]
    fn prune_all_removes_the_namespace_but_not_user_skills() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        install_skills(
            root,
            &[PathBuf::from(DEST)],
            &[make_source(root, "alpha", "body")],
        )
        .unwrap();

        let mine = root.join(DEST).join("hand-written");
        fs::create_dir_all(&mine).unwrap();
        fs::write(mine.join(SKILL_FILE), "mine").unwrap();

        prune_all(root, &[PathBuf::from(DEST)]).unwrap();
        assert!(!root.join(DEST).join("dbt-alpha").exists());
        assert!(mine.join(SKILL_FILE).is_file());
    }

    #[test]
    fn a_skill_authored_in_the_provider_dir_is_not_duplicated_into_the_namespace() {
        // R5 regression guard. Branch A compared source against the plain target
        // path; with a prefix the paths differ, so a naive check would copy
        // `.agents/skills/alpha` to `.agents/skills/dbt-alpha` and ship the same
        // skill twice.
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
        assert!(
            !root.join(DEST).join("dbt-alpha").exists(),
            "must not duplicate"
        );
    }

    #[test]
    fn writes_into_every_destination() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let selected = vec![make_source(root, "alpha", "body")];
        let destinations = [PathBuf::from(DEST), PathBuf::from(".claude/skills")];

        install_skills(root, &destinations, &selected).unwrap();
        assert!(root.join(DEST).join("dbt-alpha").join(SKILL_FILE).is_file());
        assert!(
            root.join(".claude/skills")
                .join("dbt-alpha")
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
        // pass just wrote as identical rather than rewriting it.
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
        assert_eq!(installed, vec!["dbt-alpha".to_string()]);
    }

    #[test]
    #[cfg(unix)]
    fn symlinks_inside_a_skill_are_not_followed_out_of_the_tree() {
        // A package can ship whatever it likes inside its own skill directory,
        // including a symlink to somewhere outside the project. Copying such a
        // link — or worse, its target — would leak files into the install.
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        fs::write(root.join("secret.txt"), "TOP SECRET").unwrap();

        let skill = make_source(root, "leaky", "body");
        std::os::unix::fs::symlink(root.join("secret.txt"), skill.dir.join("stolen.txt")).unwrap();
        std::os::unix::fs::symlink(root, skill.dir.join("everything")).unwrap();

        install_skills(root, &[PathBuf::from(DEST)], &[skill]).unwrap();

        let installed = root.join(DEST).join("dbt-leaky");
        assert!(installed.join(SKILL_FILE).is_file());
        assert!(!installed.join("stolen.txt").exists());
        assert!(!installed.join("everything").exists());
    }
}
