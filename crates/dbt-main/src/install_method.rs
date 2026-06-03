//! Detect how the running `dbt` binary was installed, so we can show the right
//! upgrade command and avoid clobbering a package-manager-owned binary during
//! `dbt system update`. Detection is path-based: fast, offline, best effort.

use std::path::Path;

/// How the current `dbt` binary was most likely installed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstallMethod {
    Homebrew,
    Pip,
    Winget,
    /// Standalone `install.sh` / `install.ps1` — self-updatable.
    Direct,
    /// A manager we don't drive directly (pipx, `uv tool`, conda, scoop,
    /// chocolatey) or an install we couldn't locate. Defer to that manager.
    Other,
}

/// Path fingerprints (normalized to `/`) that uniquely identify a manager.
/// First match wins; `pipx`/`uv` must precede the pip check since their venvs
/// also look like plain pip.
const PATH_MARKERS: &[(&str, InstallMethod)] = &[
    ("/Cellar/", InstallMethod::Homebrew),
    ("/pipx/venvs/", InstallMethod::Other),
    ("/pipx/shared/", InstallMethod::Other),
    ("/uv/tools/", InstallMethod::Other),
    ("/scoop/apps/", InstallMethod::Other),
    ("/scoop/shims/", InstallMethod::Other),
    ("/chocolatey/", InstallMethod::Other),
    ("/WinGet/Packages/", InstallMethod::Winget),
    ("/WinGet/Links/", InstallMethod::Winget),
];

impl InstallMethod {
    pub fn detect() -> Self {
        match std::env::current_exe() {
            Ok(binary_path) => Self::detect_from_path(&binary_path),
            Err(_) => InstallMethod::Other,
        }
    }

    pub fn detect_from_path(binary_path: &Path) -> Self {
        // Search both the entry path and its canonical target: winget links sit
        // at the entry, Homebrew's real binary at the canonical Cellar target.
        let canonical = dbt_common::stdfs::canonicalize(binary_path)
            .unwrap_or_else(|_| binary_path.to_path_buf());
        let path = format!("{}\n{}", canonical.display(), binary_path.display()).replace('\\', "/");

        if let Some((_, method)) = PATH_MARKERS.iter().find(|(m, _)| path.contains(m)) {
            return *method;
        }
        if is_pip_installed(binary_path) {
            return InstallMethod::Pip;
        }
        InstallMethod::Direct
    }

    /// Whether `dbt system update` may update in place. Only the standalone
    /// installer is; everything else is owned by its package manager.
    pub fn is_self_updatable(self) -> bool {
        matches!(self, InstallMethod::Direct)
    }

    /// The upgrade command to surface, or `None` for [`InstallMethod::Other`]
    /// (the user should use whatever manager they installed dbt with).
    ///
    /// `target_version` pins winget, whose plain `upgrade` is unreliable for
    /// the pre-release versions dbt ships; pass `None` to upgrade to latest.
    pub fn upgrade_command(self, target_version: Option<&str>) -> Option<String> {
        let command = match self {
            InstallMethod::Homebrew => "brew upgrade dbt".to_string(),
            // `--pre`: dbt ships as PEP 440 pre-releases, which pip skips by
            // default once any stable release exists.
            InstallMethod::Pip => "pip install --pre --upgrade dbt".to_string(),
            InstallMethod::Winget => match target_version {
                Some(v) => format!("winget install --id dbtLabs.dbt --exact --version {v}"),
                None => "winget upgrade --id dbtLabs.dbt --exact".to_string(),
            },
            InstallMethod::Direct => "dbt system update".to_string(),
            InstallMethod::Other => return None,
        };
        Some(command)
    }

    pub fn label(self) -> &'static str {
        match self {
            InstallMethod::Homebrew => "Homebrew",
            InstallMethod::Pip => "pip",
            InstallMethod::Winget => "winget",
            InstallMethod::Direct => "the standalone installer",
            InstallMethod::Other => "another package manager",
        }
    }
}

/// Was dbt installed by pip into this environment? The authoritative signal is
/// a `dbt-<version>.dist-info` in `site-packages` (pip creates it, the
/// standalone installer never does), avoiding false positives for a script
/// install that merely sits beside a Python interpreter.
fn is_pip_installed(binary_path: &Path) -> bool {
    let Some(env_root) = binary_path.parent().and_then(Path::parent) else {
        return false;
    };
    // Windows: <env>/Lib/site-packages. Unix: <env>/lib{,64}/python*/site-packages.
    site_packages_has_dbt(&env_root.join("Lib").join("site-packages"))
        || ["lib", "lib64"].iter().any(|lib| {
            python_dirs(&env_root.join(lib))
                .any(|dir| site_packages_has_dbt(&dir.join("site-packages")))
        })
}

/// Iterator over `<lib>/python*` directories, empty if `lib` is unreadable.
fn python_dirs(lib: &Path) -> impl Iterator<Item = std::path::PathBuf> {
    std::fs::read_dir(lib)
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("python"))
        })
}

fn site_packages_has_dbt(site_packages: &Path) -> bool {
    let Ok(entries) = std::fs::read_dir(site_packages) else {
        return false;
    };
    entries.flatten().any(|entry| {
        entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with("dbt-") && name.ends_with(".dist-info"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn detect(path: &str) -> InstallMethod {
        InstallMethod::detect_from_path(&PathBuf::from(path))
    }

    /// Lay out `<env>/<bin_subdir>/dbt` plus an optional `dbt-*.dist-info`.
    fn make_install(env_root: &Path, bin_subdir: &str, site_packages: Option<&str>) -> PathBuf {
        let bin = env_root.join(bin_subdir);
        std::fs::create_dir_all(&bin).unwrap();
        let exe = bin.join("dbt");
        std::fs::write(&exe, b"binary").unwrap();
        if let Some(sp) = site_packages {
            std::fs::create_dir_all(env_root.join(sp).join("dbt-2.0.0rc180.dist-info")).unwrap();
        }
        exe
    }

    #[test]
    fn path_markers_classify_managers() {
        assert_eq!(
            detect("/opt/homebrew/Cellar/dbt/2.0.0/bin/dbt"),
            InstallMethod::Homebrew
        );
        assert_eq!(
            detect("/home/u/.local/pipx/venvs/dbt/bin/dbt"),
            InstallMethod::Other
        );
        assert_eq!(
            detect("/home/u/.local/share/uv/tools/dbt/bin/dbt"),
            InstallMethod::Other
        );
        assert_eq!(
            detect("C:\\Users\\u\\scoop\\shims\\dbt.exe"),
            InstallMethod::Other
        );
        assert_eq!(
            detect("C:\\ProgramData\\chocolatey\\bin\\dbt.exe"),
            InstallMethod::Other
        );
        assert_eq!(
            detect(
                "C:\\Users\\u\\AppData\\Local\\Microsoft\\WinGet\\Packages\\dbtLabs.dbt\\dbt.exe"
            ),
            InstallMethod::Winget
        );
    }

    #[test]
    fn pip_detected_via_dist_info() {
        let tmp = tempfile::tempdir().unwrap();
        let exe = make_install(tmp.path(), "bin", Some("lib/python3.11/site-packages"));
        assert_eq!(InstallMethod::detect_from_path(&exe), InstallMethod::Pip);
    }

    #[test]
    fn pip_detected_via_dist_info_windows() {
        let tmp = tempfile::tempdir().unwrap();
        let exe = make_install(tmp.path(), "Scripts", Some("Lib/site-packages"));
        assert_eq!(InstallMethod::detect_from_path(&exe), InstallMethod::Pip);
    }

    #[test]
    fn script_install_next_to_python_is_direct() {
        let tmp = tempfile::tempdir().unwrap();
        let exe = make_install(tmp.path(), "usr/local/bin", None);
        std::fs::write(exe.with_file_name("python3"), b"py").unwrap();
        assert_eq!(InstallMethod::detect_from_path(&exe), InstallMethod::Direct);
    }

    #[test]
    fn script_install_into_venv_bin_is_direct() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("pyvenv.cfg"), b"home = /usr").unwrap();
        let exe = make_install(tmp.path(), "bin", None);
        assert_eq!(InstallMethod::detect_from_path(&exe), InstallMethod::Direct);
    }

    #[test]
    fn plain_local_bin_is_direct() {
        let tmp = tempfile::tempdir().unwrap();
        let exe = make_install(&tmp.path().join(".local"), "bin", None);
        assert_eq!(InstallMethod::detect_from_path(&exe), InstallMethod::Direct);
    }

    #[test]
    fn only_direct_is_self_updatable() {
        for method in [
            InstallMethod::Homebrew,
            InstallMethod::Pip,
            InstallMethod::Winget,
            InstallMethod::Other,
        ] {
            assert!(!method.is_self_updatable());
        }
        assert!(InstallMethod::Direct.is_self_updatable());
    }

    #[test]
    fn upgrade_commands_match_method() {
        assert_eq!(
            InstallMethod::Homebrew.upgrade_command(None).as_deref(),
            Some("brew upgrade dbt")
        );
        assert_eq!(
            InstallMethod::Pip.upgrade_command(None).as_deref(),
            Some("pip install --pre --upgrade dbt")
        );
        assert_eq!(
            InstallMethod::Direct.upgrade_command(None).as_deref(),
            Some("dbt system update")
        );
        assert_eq!(InstallMethod::Other.upgrade_command(None), None);
    }

    #[test]
    fn winget_pins_version_when_known() {
        assert_eq!(
            InstallMethod::Winget
                .upgrade_command(Some("2.0.0-preview.180"))
                .as_deref(),
            Some("winget install --id dbtLabs.dbt --exact --version 2.0.0-preview.180")
        );
        assert_eq!(
            InstallMethod::Winget.upgrade_command(None).as_deref(),
            Some("winget upgrade --id dbtLabs.dbt --exact")
        );
    }
}
