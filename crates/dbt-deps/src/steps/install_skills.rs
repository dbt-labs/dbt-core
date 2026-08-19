//! Installing agent skills once packages are on disk.
//!
//! This runs as part of package installation rather than as a separate command,
//! so skills land at the same moment the packages that ship them do. It reads
//! only files on disk — no profile, no warehouse connection, no manifest.

use std::path::Path;

use dbt_common::tracing::dbt_emit::emit_warn_log_message;
use dbt_common::{ErrorCode, stdfs};
use dbt_schemas::schemas::packages::{DbtPackages, DbtPackagesLock};
use dbt_schemas::schemas::project::DbtProject;
use dbt_skills::{InstalledPackage, install_package_skills};

/// What the skill install pass needs from the surrounding deps run.
pub struct SkillInstallInputs<'a> {
    /// Project root, which is both where `dbt_project.yml` lives and where the
    /// provider directories are written.
    pub in_dir: &'a Path,
    pub packages_install_path: &'a Path,
    pub dbt_packages_lock: &'a DbtPackagesLock,
    /// The parsed `packages.yml`/`dependencies.yml`, used only to recover
    /// declaration order — `package-lock.yml` is sorted by name, but skill name
    /// collisions are resolved by declaration order.
    pub package_def: Option<&'a DbtPackages>,
    pub ai_provider: Option<&'a [String]>,
}

/// Install skills from the root project and every installed package.
///
/// Never fails the install for skill-content reasons: bad frontmatter or a name
/// collision warns and is skipped. Any error here is surfaced as a warning too,
/// so a filesystem problem writing skills can't break `dbt deps` itself.
pub fn install_skills(inputs: SkillInstallInputs<'_>) {
    let Some(root_project) = read_root_project(inputs.in_dir) else {
        return;
    };

    let packages = ordered_packages(
        inputs.dbt_packages_lock,
        inputs.package_def,
        inputs.packages_install_path,
    );

    let installed =
        install_package_skills(inputs.in_dir, &root_project, &packages, inputs.ai_provider);
    if let Err(e) = installed {
        emit_warn_log_message(
            ErrorCode::IoError,
            format!("Could not install agent skills: {e}"),
        );
    }
}

/// Read the root `dbt_project.yml` for `skill-paths`, `skills:` and `flags:`.
///
/// Returns `None` when the file is missing or unreadable; `dbt deps` has other,
/// better error paths for that, so this stays quiet.
fn read_root_project(in_dir: &Path) -> Option<DbtProject> {
    let contents = stdfs::read_to_string(in_dir.join("dbt_project.yml")).ok()?;
    dbt_skills::yaml::from_str::<DbtProject>(&contents).ok()
}

/// Installed packages in collision-precedence order: declared packages in
/// `packages.yml` order first, then everything else.
fn ordered_packages(
    dbt_packages_lock: &DbtPackagesLock,
    package_def: Option<&DbtPackages>,
    packages_install_path: &Path,
) -> Vec<InstalledPackage> {
    let declaration_order: Vec<String> = package_def
        .map(|def| {
            def.packages
                .iter()
                .map(|entry| entry.entry_name())
                .collect()
        })
        .unwrap_or_default();

    let rank = |lock: &dbt_schemas::schemas::packages::DbtPackageLock| {
        let entry_name = lock.entry_name();
        declaration_order
            .iter()
            .position(|declared| *declared == entry_name)
            .unwrap_or(usize::MAX)
    };

    let mut locks: Vec<_> = dbt_packages_lock.packages.iter().collect();
    // Stable, so transitive packages keep the lock's own (alphabetical) order.
    locks.sort_by_key(|lock| rank(lock));

    locks
        .into_iter()
        .map(|lock| {
            let name = lock.package_name();
            InstalledPackage {
                root: packages_install_path.join(&name),
                name,
                version: lock.version_string(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_schemas::schemas::packages::{
        DbtPackageEntry, DbtPackageLock, HubPackage, HubPackageLock, PackageVersion,
    };

    fn hub_lock(package: &str, name: &str) -> DbtPackageLock {
        DbtPackageLock::Hub(HubPackageLock {
            package: package.to_string(),
            name: name.to_string(),
            version: PackageVersion::String("1.0.0".to_string()),
        })
    }

    fn hub_entry(package: &str) -> DbtPackageEntry {
        DbtPackageEntry::Hub(HubPackage {
            package: package.to_string(),
            version: None,
            install_prerelease: None,
        })
    }

    #[test]
    fn declared_packages_come_first_in_packages_yml_order() {
        // The lock is alphabetized; packages.yml declares them the other way round.
        let lock = DbtPackagesLock {
            packages: vec![
                hub_lock("dbt-labs/alpha", "alpha"),
                hub_lock("dbt-labs/beta", "beta"),
            ],
            sha1_hash: String::new(),
        };
        let def = DbtPackages {
            projects: vec![],
            packages: vec![hub_entry("dbt-labs/beta"), hub_entry("dbt-labs/alpha")],
        };

        let ordered = ordered_packages(&lock, Some(&def), Path::new("dbt_packages"));
        let names: Vec<_> = ordered.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, vec!["beta", "alpha"]);
    }

    #[test]
    fn transitive_packages_sort_after_declared_ones() {
        let lock = DbtPackagesLock {
            packages: vec![
                hub_lock("dbt-labs/a_transitive", "a_transitive"),
                hub_lock("dbt-labs/z_declared", "z_declared"),
            ],
            sha1_hash: String::new(),
        };
        let def = DbtPackages {
            projects: vec![],
            packages: vec![hub_entry("dbt-labs/z_declared")],
        };

        let ordered = ordered_packages(&lock, Some(&def), Path::new("dbt_packages"));
        let names: Vec<_> = ordered.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, vec!["z_declared", "a_transitive"]);
    }

    #[test]
    fn without_a_packages_file_the_lock_order_is_kept() {
        let lock = DbtPackagesLock {
            packages: vec![
                hub_lock("dbt-labs/alpha", "alpha"),
                hub_lock("dbt-labs/beta", "beta"),
            ],
            sha1_hash: String::new(),
        };

        let ordered = ordered_packages(&lock, None, Path::new("dbt_packages"));
        let names: Vec<_> = ordered.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, vec!["alpha", "beta"]);
    }

    #[test]
    fn package_roots_are_under_the_install_path() {
        let lock = DbtPackagesLock {
            packages: vec![hub_lock("dbt-labs/alpha", "alpha")],
            sha1_hash: String::new(),
        };

        let ordered = ordered_packages(&lock, None, Path::new("dbt_packages"));
        assert_eq!(ordered[0].root, Path::new("dbt_packages/alpha"));
    }
}
