use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Channel {
    Standalone,
    Pypi,
    Brew,
    Winget,
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

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum PythonPackageManager {
    Pip,
    Pipx,
    Uv,
    Poetry,
    Pdm,
    Pipenv,
    Hatch,
    Conda,
    Asdf,
    Mise,
    Pyenv,
    Rye,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DistInfo {
    pub path: String,
    pub channel: Option<Channel>,
    pub distribution: Option<Distribution>,
    pub generation: Generation,
    pub py_package_manager: Option<PythonPackageManager>,
    pub py_venv_root: Option<String>,
    pub upgrade_cmd: Option<String>,
    pub uninstall_cmd: Option<String>,
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
    }

    #[test]
    fn generation_serializes_to_spec_contract() {
        assert_eq!(serde_json::to_string(&Generation::V1).unwrap(), "\"v1\"");
        assert_eq!(serde_json::to_string(&Generation::V2).unwrap(), "\"v2\"");
    }

    #[test]
    fn python_package_manager_serializes_to_spec_contract() {
        let cases = [
            (PythonPackageManager::Pip, "\"pip\""),
            (PythonPackageManager::Pipx, "\"pipx\""),
            (PythonPackageManager::Uv, "\"uv\""),
            (PythonPackageManager::Poetry, "\"poetry\""),
            (PythonPackageManager::Pdm, "\"pdm\""),
            (PythonPackageManager::Pipenv, "\"pipenv\""),
            (PythonPackageManager::Hatch, "\"hatch\""),
            (PythonPackageManager::Conda, "\"conda\""),
            (PythonPackageManager::Asdf, "\"asdf\""),
            (PythonPackageManager::Mise, "\"mise\""),
            (PythonPackageManager::Pyenv, "\"pyenv\""),
            (PythonPackageManager::Rye, "\"rye\""),
        ];
        for (variant, expected) in cases {
            assert_eq!(serde_json::to_string(&variant).unwrap(), expected);
        }
    }

    #[test]
    fn dist_info_serializes_full_contract_shape() {
        let info = DistInfo {
            path: "/home/user/.venv/bin/dbt".to_string(),
            channel: Some(Channel::Pypi),
            distribution: Some(Distribution::Fusion),
            generation: Generation::V2,
            py_package_manager: Some(PythonPackageManager::Uv),
            py_venv_root: Some("/home/user/.venv".to_string()),
            upgrade_cmd: Some("uv tool upgrade dbt".to_string()),
            uninstall_cmd: Some("uv tool uninstall dbt".to_string()),
        };
        let value: serde_json::Value = serde_json::to_value(&info).unwrap();
        assert_eq!(value["path"], "/home/user/.venv/bin/dbt");
        assert_eq!(value["channel"], "pypi");
        assert_eq!(value["distribution"], "dbt");
        assert_eq!(value["generation"], "v2");
        assert_eq!(value["py_package_manager"], "uv");
        assert_eq!(value["py_venv_root"], "/home/user/.venv");
        assert_eq!(value["upgrade_cmd"], "uv tool upgrade dbt");
        assert_eq!(value["uninstall_cmd"], "uv tool uninstall dbt");
    }
}
