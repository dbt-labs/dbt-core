use crate::python::PythonPackageManager;
use dbt_common::{ErrorCode, FsResult, fs_err};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Channel {
    Standalone,
    /// A native binary that no known package manager (Homebrew, Winget) or
    /// the standalone installer's canonical `~/.local/bin` claims — a dev
    /// build, or one placed somewhere non-standard. Treated the same as
    /// `Standalone` for self-update/uninstall purposes: nothing else owns
    /// it, so dbt may manage it directly.
    Unclaimed,
    Pypi,
    Brew,
    Winget,
    /// A native binary under a package manager we recognize by path but
    /// don't publish to (e.g. Scoop, Chocolatey) — unlike Homebrew/Winget/
    /// PyPI, we have no official package there, so there's no command we
    /// can vouch for. The `String` is the manager's display name, used only
    /// in messages (never matched on) — see `DistInfo::unsupported_channel_message`.
    Unsupported(String),
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Distribution {
    #[serde(rename = "dbt")]
    Fusion,
    #[serde(rename = "dbt-core")]
    OSS,
    #[serde(rename = "cloud-cli")]
    CloudCLI,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Generation {
    V1,
    V2,
    NotApplicable,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DistInfo {
    pub path: String,
    pub channel: Option<Channel>,
    pub distribution: Option<Distribution>,
    pub generation: Generation,
    pub py_package_manager: Option<PythonPackageManager>,
    pub py_venv_root: Option<String>,
    /// The version found at `path`, when it could be determined.
    ///
    /// `#[serde(default)]`: a `dbt` probed via `internal
    /// get-distribution-info` (see "Resolving the release channel") may be a
    /// different, older build than the one parsing its output, and older
    /// builds may predate this field.
    #[serde(default)]
    pub version: Option<String>,
    /// Whether `version` is a pre-release. `None` when `version` itself
    /// couldn't be determined — never collapsed to `false`, since command
    /// generation treats "unknown" as "not a pre-release" (the safe
    /// default) but the field itself should stay honest about that.
    #[serde(default)]
    pub is_prerelease: Option<bool>,
    pub upgrade_cmd: Option<String>,
    pub uninstall_cmd: Option<String>,
}

impl DistInfo {
    /// Discovers [`DistInfo`] for the currently running process. Thin
    /// wrapper around [`crate::DistInfoDiscovery::Current`] for callers that
    /// just want a single value rather than a one-element `Vec`.
    pub fn current(command_name: &str) -> FsResult<Self> {
        crate::DistInfoDiscovery::Current
            .discover(command_name)?
            .pop()
            .ok_or_else(|| {
                fs_err!(
                    ErrorCode::Unexpected,
                    "distribution discovery returned no results"
                )
            })
    }

    /// Whether this install is one dbt manages itself (`dbt system update` /
    /// `dbt system uninstall`) rather than one owned by an external package
    /// manager.
    pub fn is_self_managed(&self) -> bool {
        matches!(
            self.channel,
            Some(Channel::Standalone) | Some(Channel::Unclaimed)
        )
    }

    /// The upgrade command to surface, pinned to `target_version` when the
    /// channel needs it. Winget's plain `upgrade` is unreliable for dbt's
    /// pre-release versions, so a known target version is pinned explicitly;
    /// every other channel's `upgrade_cmd` is already version-agnostic.
    pub fn upgrade_command_for_version(&self, target_version: Option<&str>) -> Option<String> {
        match (&self.channel, target_version) {
            (Some(Channel::Winget), Some(v)) => Some(format!(
                "winget install --id dbtLabs.dbt --exact --version {v}"
            )),
            _ => self.upgrade_cmd.clone(),
        }
    }

    /// Human-readable name of the tool/channel that owns this install, for
    /// error messages (e.g. "dbt was installed via {label}. To uninstall,
    /// run ...").
    pub fn install_label(&self) -> &'static str {
        match self.channel {
            Some(Channel::Standalone) | Some(Channel::Unclaimed) => "the standalone installer",
            Some(Channel::Brew) => "Homebrew",
            Some(Channel::Winget) => "winget",
            Some(Channel::Pypi) => self
                .py_package_manager
                .map_or("a Python package manager", PythonPackageManager::label),
            // Not expected to be reached in practice — callers check
            // `unsupported_channel_message` first, which has the manager's
            // actual name; this is only here so the match stays exhaustive.
            Some(Channel::Unsupported(_)) => "an unsupported package manager",
            None => "another package manager",
        }
    }

    /// For a recognized-but-unsupported manager (Scoop, Chocolatey, ...),
    /// the message telling the user to go through that manager instead —
    /// `action` is the verb ("update" or "uninstall"). `None` for every
    /// other channel.
    pub fn unsupported_channel_message(&self, action: &str) -> Option<String> {
        let Some(Channel::Unsupported(manager)) = &self.channel else {
            return None;
        };
        Some(format!(
            "dbt was not installed using an officially-supported channel. \
             Please {action} with {manager} and see \
             https://docs.getdbt.com/docs/local/install-dbt?version=2 \
             for installation instructions."
        ))
    }
}

/// Generates the uninstall command for a package that isn't necessarily the
/// currently-running distribution's own package — e.g. removing `dbt-core`
/// once a cross-distribution upgrade has installed `dbt` (Fusion) alongside
/// or in place of it. Mirrors the uninstall half of
/// [`crate::PathDiscovery::command_strings`]'s `Pypi` arm, but parameterized
/// by an explicit package name instead of the hardcoded `"dbt"`.
///
/// Deliberately separate from `command_strings`: that function backs
/// `dbt system update`/`uninstall`'s self-management of the *current*
/// package and must not change behavior for this unrelated use case.
pub fn uninstall_command_for_package(
    channel: Channel,
    manager: Option<PythonPackageManager>,
    package_name: &str,
) -> Option<String> {
    match channel {
        Channel::Standalone | Channel::Unclaimed => Some("dbt system uninstall".to_string()),
        Channel::Brew => Some(format!("brew uninstall {package_name}")),
        Channel::Winget => Some(format!(
            "winget uninstall --id dbtLabs.{package_name} --exact"
        )),
        Channel::Unsupported(_) => None,
        Channel::Pypi => {
            let manager = manager?;
            let command = match manager {
                PythonPackageManager::Pip
                | PythonPackageManager::Asdf
                | PythonPackageManager::Mise
                | PythonPackageManager::Pyenv => format!("pip uninstall {package_name}"),
                PythonPackageManager::Pipx => format!("pipx uninstall {package_name}"),
                PythonPackageManager::Uv => format!("uv tool uninstall {package_name}"),
                PythonPackageManager::Poetry => format!("poetry remove {package_name}"),
                PythonPackageManager::Pdm => format!("pdm remove {package_name}"),
                PythonPackageManager::Pipenv => format!("pipenv uninstall {package_name}"),
                PythonPackageManager::Hatch => format!("hatch run pip uninstall {package_name}"),
                PythonPackageManager::Conda => format!("conda remove {package_name}"),
                PythonPackageManager::Rye => format!("rye uninstall {package_name}"),
            };
            Some(command)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distribution_serializes_to_spec_contract() {
        assert_eq!(
            serde_json::to_string(&Distribution::Fusion).unwrap(),
            "\"dbt\""
        );
        assert_eq!(
            serde_json::to_string(&Distribution::OSS).unwrap(),
            "\"dbt-core\""
        );
        assert_eq!(
            serde_json::to_string(&Distribution::CloudCLI).unwrap(),
            "\"cloud-cli\""
        );
    }

    #[test]
    fn channel_serializes_to_spec_contract() {
        assert_eq!(
            serde_json::to_string(&Channel::Standalone).unwrap(),
            "\"standalone\""
        );
        assert_eq!(serde_json::to_string(&Channel::Pypi).unwrap(), "\"pypi\"");
        assert_eq!(serde_json::to_string(&Channel::Brew).unwrap(), "\"brew\"");
        assert_eq!(
            serde_json::to_string(&Channel::Winget).unwrap(),
            "\"winget\""
        );
        assert_eq!(
            serde_json::to_string(&Channel::Unclaimed).unwrap(),
            "\"unclaimed\""
        );
    }

    #[test]
    fn generation_serializes_to_spec_contract() {
        assert_eq!(serde_json::to_string(&Generation::V1).unwrap(), "\"v1\"");
        assert_eq!(serde_json::to_string(&Generation::V2).unwrap(), "\"v2\"");
    }

    fn sample_dist_info() -> DistInfo {
        DistInfo {
            path: "/home/user/.venv/bin/dbt".to_string(),
            channel: Some(Channel::Pypi),
            distribution: Some(Distribution::Fusion),
            generation: Generation::V2,
            py_package_manager: Some(PythonPackageManager::Uv),
            py_venv_root: Some("/home/user/.venv".to_string()),
            version: Some("2.0.0-preview.203".to_string()),
            is_prerelease: Some(true),
            upgrade_cmd: Some("uv tool upgrade dbt".to_string()),
            uninstall_cmd: Some("uv tool uninstall dbt".to_string()),
        }
    }

    #[test]
    fn dist_info_serializes_full_contract_shape() {
        let info = sample_dist_info();
        let value: serde_json::Value = serde_json::to_value(&info).unwrap();
        assert_eq!(value["path"], "/home/user/.venv/bin/dbt");
        assert_eq!(value["channel"], "pypi");
        assert_eq!(value["distribution"], "dbt");
        assert_eq!(value["generation"], "v2");
        assert_eq!(value["py_package_manager"], "uv");
        assert_eq!(value["py_venv_root"], "/home/user/.venv");
        assert_eq!(value["version"], "2.0.0-preview.203");
        assert_eq!(value["is_prerelease"], true);
        assert_eq!(value["upgrade_cmd"], "uv tool upgrade dbt");
        assert_eq!(value["uninstall_cmd"], "uv tool uninstall dbt");
    }

    #[test]
    fn is_self_managed_true_for_standalone_and_unclaimed() {
        for channel in [Channel::Standalone, Channel::Unclaimed] {
            let mut info = sample_dist_info();
            info.channel = Some(channel.clone());
            assert!(info.is_self_managed(), "{channel:?} should be self-managed");
        }
    }

    #[test]
    fn is_self_managed_false_for_managed_channels() {
        for channel in [Channel::Brew, Channel::Winget, Channel::Pypi] {
            let mut info = sample_dist_info();
            info.channel = Some(channel.clone());
            assert!(
                !info.is_self_managed(),
                "{channel:?} should not be self-managed"
            );
        }
        let mut info = sample_dist_info();
        info.channel = None;
        assert!(!info.is_self_managed());
    }

    #[test]
    fn upgrade_command_for_version_pins_winget() {
        let mut info = sample_dist_info();
        info.channel = Some(Channel::Winget);
        info.upgrade_cmd = Some("winget upgrade --id dbtLabs.dbt --exact".to_string());
        assert_eq!(
            info.upgrade_command_for_version(Some("2.0.0-preview.180")),
            Some("winget install --id dbtLabs.dbt --exact --version 2.0.0-preview.180".to_string())
        );
        assert_eq!(
            info.upgrade_command_for_version(None),
            Some("winget upgrade --id dbtLabs.dbt --exact".to_string())
        );
    }

    #[test]
    fn upgrade_command_for_version_passes_through_other_channels() {
        let info = sample_dist_info();
        assert_eq!(
            info.upgrade_command_for_version(Some("2.0.0-preview.180")),
            info.upgrade_cmd
        );
    }

    #[test]
    fn install_label_covers_every_channel() {
        let cases = [
            (Some(Channel::Standalone), None, "the standalone installer"),
            (Some(Channel::Unclaimed), None, "the standalone installer"),
            (Some(Channel::Brew), None, "Homebrew"),
            (Some(Channel::Winget), None, "winget"),
            (Some(Channel::Pypi), Some(PythonPackageManager::Pip), "pip"),
            (Some(Channel::Pypi), None, "a Python package manager"),
            (None, None, "another package manager"),
        ];
        for (channel, manager, expected) in cases {
            let mut info = sample_dist_info();
            info.channel = channel.clone();
            info.py_package_manager = manager;
            assert_eq!(info.install_label(), expected, "channel={channel:?}");
        }
    }

    #[test]
    fn uninstall_command_for_package_covers_every_channel_and_manager() {
        let cases: &[(Channel, Option<PythonPackageManager>, &str)] = &[
            (Channel::Standalone, None, "dbt system uninstall"),
            (Channel::Unclaimed, None, "dbt system uninstall"),
            (Channel::Brew, None, "brew uninstall dbt-core"),
            (
                Channel::Winget,
                None,
                "winget uninstall --id dbtLabs.dbt-core --exact",
            ),
            (
                Channel::Pypi,
                Some(PythonPackageManager::Pip),
                "pip uninstall dbt-core",
            ),
            (
                Channel::Pypi,
                Some(PythonPackageManager::Asdf),
                "pip uninstall dbt-core",
            ),
            (
                Channel::Pypi,
                Some(PythonPackageManager::Mise),
                "pip uninstall dbt-core",
            ),
            (
                Channel::Pypi,
                Some(PythonPackageManager::Pyenv),
                "pip uninstall dbt-core",
            ),
            (
                Channel::Pypi,
                Some(PythonPackageManager::Pipx),
                "pipx uninstall dbt-core",
            ),
            (
                Channel::Pypi,
                Some(PythonPackageManager::Uv),
                "uv tool uninstall dbt-core",
            ),
            (
                Channel::Pypi,
                Some(PythonPackageManager::Poetry),
                "poetry remove dbt-core",
            ),
            (
                Channel::Pypi,
                Some(PythonPackageManager::Pdm),
                "pdm remove dbt-core",
            ),
            (
                Channel::Pypi,
                Some(PythonPackageManager::Pipenv),
                "pipenv uninstall dbt-core",
            ),
            (
                Channel::Pypi,
                Some(PythonPackageManager::Hatch),
                "hatch run pip uninstall dbt-core",
            ),
            (
                Channel::Pypi,
                Some(PythonPackageManager::Conda),
                "conda remove dbt-core",
            ),
            (
                Channel::Pypi,
                Some(PythonPackageManager::Rye),
                "rye uninstall dbt-core",
            ),
        ];
        for (channel, manager, expected) in cases {
            assert_eq!(
                uninstall_command_for_package(channel.clone(), *manager, "dbt-core"),
                Some(expected.to_string()),
                "channel={channel:?} manager={manager:?}"
            );
        }
    }

    #[test]
    fn uninstall_command_for_package_pypi_with_no_manager_is_none() {
        assert_eq!(
            uninstall_command_for_package(Channel::Pypi, None, "dbt-core"),
            None
        );
    }
}
