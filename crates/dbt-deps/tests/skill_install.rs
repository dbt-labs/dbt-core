//! End-to-end `dbt deps` tests for agent-skill installation.
//!
//! These drive the real `get_or_install_packages` entry point against `local:`
//! packages, so they exercise lock computation, package install and the skill
//! pass together without touching the network. They also stand as a regression
//! test for the constraint that `dbt deps` needs no profile and builds no
//! manifest: nothing here supplies either.

use std::fs;
use std::path::{Path, PathBuf};

use dbt_common::cancellation::CancellationToken;
use dbt_common::io_args::{FsCommand, IoArgs};
use dbt_jinja_utils::phases::load::init::initialize_load_profile_jinja_environment;
use fs_deps::get_or_install_packages;
use tempfile::TempDir;

const PROVENANCE: &str = ".provenance";
const AGENTS_DIR: &str = ".agents/skills";
const CLAUDE_DIR: &str = ".claude/skills";

/// A throwaway project tree with `local:` packages, ready for `dbt deps`.
struct TestProject {
    _tmp: TempDir,
    root: PathBuf,
}

impl TestProject {
    fn new(project_yml: &str) -> Self {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().join("project");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("dbt_project.yml"), project_yml).unwrap();
        Self { _tmp: tmp, root }
    }

    /// Create a sibling package directory and declare it in `packages.yml`.
    ///
    /// Packages are declared in call order, which is what skill name-collision
    /// precedence keys off.
    fn with_local_package(&self, name: &str, project_yml: &str) -> PathBuf {
        let package_root = self.root.parent().unwrap().join(name);
        fs::create_dir_all(&package_root).unwrap();
        fs::write(package_root.join("dbt_project.yml"), project_yml).unwrap();

        let packages_yml = self.root.join("packages.yml");
        let existing =
            fs::read_to_string(&packages_yml).unwrap_or_else(|_| "packages:\n".to_string());
        fs::write(
            &packages_yml,
            format!("{existing}  - local: \"../{name}\"\n"),
        )
        .unwrap();

        package_root
    }

    fn write_skill(&self, rel: &str, name: &str, body: &str) {
        write_skill_at(&self.root.join(rel), name, body);
    }

    fn installed(&self, dir: &str, skill: &str) -> PathBuf {
        self.root.join(dir).join(skill)
    }

    async fn deps(&self, ai_provider: Option<&[String]>) {
        let io = IoArgs {
            in_dir: self.root.clone(),
            out_dir: self.root.join("target"),
            ..Default::default()
        };
        // A profile-free Jinja environment: exactly what `dbt deps` uses.
        let env = initialize_load_profile_jinja_environment();

        get_or_install_packages(
            &io,
            FsCommand::Deps,
            &env,
            &self.root.join("dbt_packages"),
            true,  // install_deps
            None,  // add_package
            false, // upgrade
            false, // lock
            Default::default(),
            false, // version_check
            false, // skip_private_deps
            None,  // replay_mode
            &CancellationToken::never_cancels(),
            false, // use_v2_compatible_package_downloads
            ai_provider,
        )
        .await
        .expect("dbt deps should succeed");
    }
}

fn write_skill_at(dir: &Path, name: &str, body: &str) {
    fs::create_dir_all(dir).unwrap();
    fs::write(
        dir.join("SKILL.md"),
        format!("---\nname: {name}\ndescription: A test skill.\n---\n{body}"),
    )
    .unwrap();
}

fn provenance(skill_dir: &Path) -> String {
    fs::read_to_string(skill_dir.join(PROVENANCE)).unwrap()
}

#[tokio::test]
async fn installs_a_skill_shipped_by_a_package() {
    let project = TestProject::new("name: root_project\nprofile: default\n");
    let package = project.with_local_package("some_pkg", "name: some_pkg\nprofile: default\n");
    write_skill_at(&package.join("skills/from-package"), "from-package", "body");

    project.deps(Some(&["claude".to_string()])).await;

    let installed = project.installed(CLAUDE_DIR, "from-package");
    assert!(installed.join("SKILL.md").is_file());

    let provenance = provenance(&installed);
    assert!(provenance.contains("managed_by: dbt"), "{provenance}");
    assert!(provenance.contains("source: package"), "{provenance}");
    assert!(provenance.contains("some_pkg"), "{provenance}");
}

#[tokio::test]
async fn installs_the_projects_own_skills_too() {
    let project = TestProject::new("name: root_project\nprofile: default\n");
    project.write_skill("skills/mine", "mine", "body");

    project.deps(Some(&["wizard".to_string()])).await;

    assert!(
        project
            .installed(AGENTS_DIR, "mine")
            .join("SKILL.md")
            .is_file()
    );
}

#[tokio::test]
async fn writes_into_every_provider_directory_once() {
    let project = TestProject::new("name: root_project\nprofile: default\n");
    project.write_skill("skills/mine", "mine", "body");

    // wizard and codex share `.agents/skills`; claude has its own.
    project
        .deps(Some(&[
            "wizard".to_string(),
            "codex".to_string(),
            "claude".to_string(),
        ]))
        .await;

    assert!(
        project
            .installed(AGENTS_DIR, "mine")
            .join("SKILL.md")
            .is_file()
    );
    assert!(
        project
            .installed(CLAUDE_DIR, "mine")
            .join("SKILL.md")
            .is_file()
    );
}

#[tokio::test]
async fn installs_nothing_when_ai_provider_is_unset() {
    let project = TestProject::new("name: root_project\nprofile: default\n");
    project.write_skill("skills/mine", "mine", "body");

    project.deps(None).await;

    assert!(!project.root.join(AGENTS_DIR).exists());
    assert!(!project.root.join(CLAUDE_DIR).exists());
}

#[tokio::test]
async fn an_unknown_provider_installs_nothing_but_deps_still_succeeds() {
    let project = TestProject::new("name: root_project\nprofile: default\n");
    project.write_skill("skills/mine", "mine", "body");

    project.deps(Some(&["not-a-harness".to_string()])).await;

    assert!(!project.root.join(AGENTS_DIR).exists());
}

#[tokio::test]
async fn ai_provider_can_be_set_in_project_flags() {
    let project =
        TestProject::new("name: root_project\nprofile: default\nflags:\n  ai_provider: claude\n");
    project.write_skill("skills/mine", "mine", "body");

    project.deps(None).await;

    assert!(
        project
            .installed(CLAUDE_DIR, "mine")
            .join("SKILL.md")
            .is_file()
    );
}

#[tokio::test]
async fn a_disabled_skill_is_not_installed() {
    let project = TestProject::new(
        "name: root_project\nprofile: default\n\
         skills:\n  root_project:\n    mine:\n      +enabled: false\n",
    );
    project.write_skill("skills/mine", "mine", "body");
    project.write_skill("skills/yours", "yours", "body");

    project.deps(Some(&["wizard".to_string()])).await;

    assert!(!project.installed(AGENTS_DIR, "mine").exists());
    assert!(project.installed(AGENTS_DIR, "yours").is_dir());
}

#[tokio::test]
async fn a_package_skill_can_be_disabled_by_the_root_project() {
    let project = TestProject::new(
        "name: root_project\nprofile: default\n\
         skills:\n  some_pkg:\n    +enabled: false\n",
    );
    let package = project.with_local_package("some_pkg", "name: some_pkg\nprofile: default\n");
    write_skill_at(&package.join("skills/unwanted"), "unwanted", "body");

    project.deps(Some(&["wizard".to_string()])).await;

    assert!(!project.installed(AGENTS_DIR, "unwanted").exists());
}

#[tokio::test]
async fn a_package_is_read_from_its_own_skill_paths() {
    let project = TestProject::new("name: root_project\nprofile: default\n");
    let package = project.with_local_package(
        "some_pkg",
        "name: some_pkg\nprofile: default\nskill-paths: [\"agent-skills\"]\n",
    );
    write_skill_at(&package.join("agent-skills/custom"), "custom", "body");

    project.deps(Some(&["wizard".to_string()])).await;

    assert!(
        project
            .installed(AGENTS_DIR, "custom")
            .join("SKILL.md")
            .is_file()
    );
}

#[tokio::test]
async fn the_project_wins_a_name_collision_and_records_the_shadowed_skill() {
    let project = TestProject::new("name: root_project\nprofile: default\n");
    project.write_skill("skills/shared", "shared", "from the project");
    let package = project.with_local_package("some_pkg", "name: some_pkg\nprofile: default\n");
    write_skill_at(&package.join("skills/shared"), "shared", "from the package");

    project.deps(Some(&["wizard".to_string()])).await;

    let installed = project.installed(AGENTS_DIR, "shared");
    let contents = fs::read_to_string(installed.join("SKILL.md")).unwrap();
    assert!(contents.ends_with("from the project"), "{contents}");

    let provenance = provenance(&installed);
    assert!(provenance.contains("source: project"), "{provenance}");
    assert!(provenance.contains("shadowed:"), "{provenance}");
    assert!(provenance.contains("some_pkg"), "{provenance}");
}

#[tokio::test]
async fn the_first_declared_package_wins_a_collision_between_packages() {
    let project = TestProject::new("name: root_project\nprofile: default\n");
    // Declared second alphabetically but first in packages.yml, so it wins.
    let first = project.with_local_package("zeta_pkg", "name: zeta_pkg\nprofile: default\n");
    let second = project.with_local_package("alpha_pkg", "name: alpha_pkg\nprofile: default\n");
    write_skill_at(&first.join("skills/shared"), "shared", "from zeta");
    write_skill_at(&second.join("skills/shared"), "shared", "from alpha");

    project.deps(Some(&["wizard".to_string()])).await;

    let installed = project.installed(AGENTS_DIR, "shared");
    let contents = fs::read_to_string(installed.join("SKILL.md")).unwrap();
    assert!(contents.ends_with("from zeta"), "{contents}");
    assert!(provenance(&installed).contains("alpha_pkg"));
}

#[tokio::test]
async fn a_malformed_skill_is_skipped_without_failing_deps() {
    let project = TestProject::new("name: root_project\nprofile: default\n");
    project.write_skill("skills/good", "good", "body");
    let bad = project.root.join("skills/bad");
    fs::create_dir_all(&bad).unwrap();
    fs::write(bad.join("SKILL.md"), "no frontmatter at all\n").unwrap();

    project.deps(Some(&["wizard".to_string()])).await;

    assert!(project.installed(AGENTS_DIR, "good").is_dir());
    assert!(!project.installed(AGENTS_DIR, "bad").exists());
}

#[tokio::test]
async fn re_running_deps_is_a_no_op() {
    let project = TestProject::new("name: root_project\nprofile: default\n");
    project.write_skill("skills/mine", "mine", "body");
    let provider = ["wizard".to_string()];

    project.deps(Some(&provider)).await;
    let installed = project.installed(AGENTS_DIR, "mine");
    let first = provenance(&installed);

    project.deps(Some(&provider)).await;

    // Same bytes, including the install timestamp — nothing was rewritten.
    assert_eq!(provenance(&installed), first);
    assert!(installed.join("SKILL.md").is_file());
}

#[tokio::test]
async fn a_changed_package_skill_is_updated_on_the_next_deps() {
    let project = TestProject::new("name: root_project\nprofile: default\n");
    let package = project.with_local_package("some_pkg", "name: some_pkg\nprofile: default\n");
    let source = package.join("skills/evolving");
    write_skill_at(&source, "evolving", "version one");
    let provider = ["wizard".to_string()];

    project.deps(Some(&provider)).await;
    write_skill_at(&source, "evolving", "version two");
    project.deps(Some(&provider)).await;

    let installed = project.installed(AGENTS_DIR, "evolving");
    let contents = fs::read_to_string(installed.join("SKILL.md")).unwrap();
    assert!(contents.ends_with("version two"), "{contents}");
}

#[tokio::test]
async fn a_user_edited_copy_is_left_alone() {
    let project = TestProject::new("name: root_project\nprofile: default\n");
    let package = project.with_local_package("some_pkg", "name: some_pkg\nprofile: default\n");
    let source = package.join("skills/evolving");
    write_skill_at(&source, "evolving", "version one");
    let provider = ["wizard".to_string()];

    project.deps(Some(&provider)).await;

    let installed = project.installed(AGENTS_DIR, "evolving");
    fs::write(installed.join("SKILL.md"), "the user rewrote this").unwrap();

    write_skill_at(&source, "evolving", "version two");
    project.deps(Some(&provider)).await;

    assert_eq!(
        fs::read_to_string(installed.join("SKILL.md")).unwrap(),
        "the user rewrote this"
    );
}

#[tokio::test]
async fn a_removed_skill_is_pruned_and_user_skills_survive() {
    let project = TestProject::new("name: root_project\nprofile: default\n");
    let package = project.with_local_package("some_pkg", "name: some_pkg\nprofile: default\n");
    write_skill_at(&package.join("skills/temporary"), "temporary", "body");
    let provider = ["wizard".to_string()];

    project.deps(Some(&provider)).await;
    assert!(project.installed(AGENTS_DIR, "temporary").is_dir());

    // A skill the user wrote by hand, with no provenance sidecar.
    let hand_written = project.installed(AGENTS_DIR, "hand-written");
    write_skill_at(&hand_written, "hand-written", "mine");

    fs::remove_dir_all(package.join("skills/temporary")).unwrap();
    project.deps(Some(&provider)).await;

    assert!(!project.installed(AGENTS_DIR, "temporary").exists());
    assert!(hand_written.join("SKILL.md").is_file());
}

#[tokio::test]
async fn bundled_files_are_copied_alongside_the_skill() {
    let project = TestProject::new("name: root_project\nprofile: default\n");
    let package = project.with_local_package("some_pkg", "name: some_pkg\nprofile: default\n");
    let source = package.join("skills/with-scripts");
    write_skill_at(&source, "with-scripts", "body");
    fs::create_dir_all(source.join("scripts")).unwrap();
    fs::write(source.join("scripts/run.sh"), "echo hi").unwrap();

    project.deps(Some(&["wizard".to_string()])).await;

    let installed = project.installed(AGENTS_DIR, "with-scripts");
    assert_eq!(
        fs::read_to_string(installed.join("scripts/run.sh")).unwrap(),
        "echo hi"
    );
}

#[tokio::test]
async fn the_source_skill_is_never_modified() {
    let project = TestProject::new("name: root_project\nprofile: default\n");
    let package = project.with_local_package("some_pkg", "name: some_pkg\nprofile: default\n");
    let source = package.join("skills/pristine");
    write_skill_at(&source, "pristine", "body");
    let before = fs::read_to_string(source.join("SKILL.md")).unwrap();

    project.deps(Some(&["claude".to_string()])).await;

    assert_eq!(fs::read_to_string(source.join("SKILL.md")).unwrap(), before);
    assert!(
        !source.join(PROVENANCE).exists(),
        "dbt must not write a sidecar into a package's own source tree"
    );
}
