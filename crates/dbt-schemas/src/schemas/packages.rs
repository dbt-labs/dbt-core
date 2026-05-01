use std::{
    collections::{BTreeMap, HashMap},
    fmt::Display,
    path::PathBuf,
};

use dbt_yaml::{DbtSchema, UntaggedEnumDeserialize, Verbatim};
use serde::{Deserialize, Serialize};

// Type aliases for clarity
type YmlValue = dbt_yaml::Value;

#[derive(Debug, Serialize, Deserialize, Clone, DbtSchema)]
pub struct UpstreamProject {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, Default, DbtSchema)]
pub struct DbtPackages {
    #[serde(default)]
    pub projects: Vec<UpstreamProject>,
    #[serde(default)]
    pub packages: Vec<DbtPackageEntry>,
}

#[derive(Debug, Serialize, UntaggedEnumDeserialize, Clone, DbtSchema)]
#[serde(untagged)]
pub enum DbtPackageEntry {
    Hub(HubPackage),
    Git(GitPackage),
    Local(LocalPackage),
    Private(PrivatePackage),
    Tarball(TarballPackage),
}

impl From<DbtPackageLock> for DbtPackageEntry {
    fn from(dbt_package_lock: DbtPackageLock) -> Self {
        match dbt_package_lock {
            DbtPackageLock::Hub(hub_package_lock) => {
                DbtPackageEntry::Hub(HubPackage::from(hub_package_lock))
            }
            DbtPackageLock::Git(git_package_lock) => {
                DbtPackageEntry::Git(GitPackage::from(git_package_lock))
            }
            DbtPackageLock::Local(local_package_lock) => {
                DbtPackageEntry::Local(LocalPackage::from(local_package_lock))
            }
            DbtPackageLock::Private(private_package_lock) => {
                DbtPackageEntry::Private(PrivatePackage::from(private_package_lock))
            }
            DbtPackageLock::Tarball(tarball_package_lock) => {
                DbtPackageEntry::Tarball(TarballPackage::from(tarball_package_lock))
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, DbtSchema)]
pub struct HubPackage {
    /// Package identifier on the dbt package hub, in `org/name` form (e.g. `dbt-labs/dbt_utils`).
    pub package: String,
    /// Version pin. Accepts a single version string, a list of constraints (e.g. `[">=1.0.0", "<2.0.0"]`), or a number.
    #[serde(rename = "version", skip_serializing_if = "Option::is_none")]
    pub version: Option<PackageVersion>,
    /// Allow installation of pre-release versions when resolving `version`.
    #[serde(rename = "install-prerelease", skip_serializing_if = "Option::is_none")]
    pub install_prerelease: Option<bool>,
}

impl From<HubPackageLock> for HubPackage {
    fn from(hub_package_lock: HubPackageLock) -> Self {
        HubPackage {
            package: hub_package_lock.package,
            version: Some(hub_package_lock.version),
            install_prerelease: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, DbtSchema)]
pub struct GitPackage {
    /// Git clone URL of the package repository (e.g. `https://github.com/dbt-labs/dbt_utils.git`).
    pub git: Verbatim<String>,
    /// Revision to check out: a tag, branch, or commit SHA.
    #[serde(rename = "revision", skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    /// Suppress the warning emitted when `revision` is unpinned (i.e. a branch name like `main`).
    #[serde(rename = "warn-unpinned", skip_serializing_if = "Option::is_none")]
    pub warn_unpinned: Option<bool>,
    /// Subdirectory of the repo where the dbt package is located.
    #[serde(rename = "subdirectory", skip_serializing_if = "Option::is_none")]
    pub subdirectory: Option<String>,
    #[schemars(skip)]
    #[serde(default, skip_serializing)]
    pub __unrendered__: HashMap<String, YmlValue>,
}

impl From<GitPackageLock> for GitPackage {
    fn from(git_package_lock: GitPackageLock) -> Self {
        GitPackage {
            git: git_package_lock.git,
            revision: Some(git_package_lock.revision),
            warn_unpinned: git_package_lock.warn_unpinned,
            subdirectory: git_package_lock.subdirectory,
            __unrendered__: git_package_lock.__unrendered__,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, DbtSchema)]
pub struct PrivatePackage {
    /// Private package identifier. Two-segment `org/repo` for GitHub or 2-part Azure DevOps
    /// (`azure_active_directory`); three-or-more-segment `org/group/repo` for GitLab subgroups
    /// or Azure DevOps `org/project/repo` (`ado` / `azure_devops`).
    #[schemars(regex(pattern = r"^[\w\-\.]+(/[\w\-\.]+){1,}$"))]
    pub private: Verbatim<String>,
    /// Git provider. One of `github` (default), `gitlab`, `ado`, `azure_devops`,
    /// or `azure_active_directory`. `ado` / `azure_devops` require an `org/project/repo`
    /// path; `azure_active_directory` is hosted-only and uses `org/repo`.
    #[serde(rename = "provider", skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    /// Revision to check out: a tag, branch, or commit SHA.
    #[serde(rename = "revision", skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    /// Suppress the warning emitted when `revision` is unpinned (i.e. a branch name like `main`).
    #[serde(rename = "warn-unpinned", skip_serializing_if = "Option::is_none")]
    pub warn_unpinned: Option<bool>,
    /// Subdirectory of the repo where the dbt package is located.
    #[serde(rename = "subdirectory", skip_serializing_if = "Option::is_none")]
    pub subdirectory: Option<String>,
    #[schemars(skip)]
    #[serde(default, skip_serializing)]
    pub __unrendered__: HashMap<String, YmlValue>,
}

impl From<PrivatePackageLock> for PrivatePackage {
    fn from(private_package_lock: PrivatePackageLock) -> Self {
        PrivatePackage {
            private: private_package_lock.private,
            provider: private_package_lock.provider,
            revision: Some(private_package_lock.revision),
            warn_unpinned: private_package_lock.warn_unpinned,
            subdirectory: private_package_lock.subdirectory,
            __unrendered__: private_package_lock.__unrendered__,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, DbtSchema)]
pub struct LocalPackage {
    /// Filesystem path to the local dbt package, relative to the project root.
    pub local: PathBuf,
}

impl From<LocalPackageLock> for LocalPackage {
    fn from(local_package_lock: LocalPackageLock) -> Self {
        LocalPackage {
            local: local_package_lock.local,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, DbtSchema)]
#[serde(untagged)]
pub enum PackageVersion {
    Number(f64),
    String(String),
    Array(Vec<String>),
}

impl Display for PackageVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PackageVersion::Number(number) => write!(f, "{}", number),
            PackageVersion::String(string) => write!(f, "{}", string),
            PackageVersion::Array(array) => write!(f, "[{}]", array.join(",")),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct DbtPackagesLock {
    #[serde(default)]
    pub packages: Vec<DbtPackageLock>,
    #[serde(default)]
    pub sha1_hash: String,
}

impl DbtPackagesLock {
    pub fn lookup_map(&self) -> BTreeMap<String, String> {
        self.packages
            .iter()
            .map(|p| (p.entry_name(), p.package_name()))
            .collect()
    }

    pub fn get_by_name(&self, name: &str) -> Option<&DbtPackageLock> {
        self.packages.iter().find(|p| p.package_name() == name)
    }

    pub fn has_duplicate_package_names(&self) -> bool {
        let mut seen = std::collections::HashSet::new();
        self.packages.iter().any(|p| !seen.insert(p.package_name()))
    }
}

#[derive(Debug, Serialize, UntaggedEnumDeserialize, Clone)]
#[serde(untagged)]
pub enum DbtPackageLock {
    Hub(HubPackageLock),
    Git(GitPackageLock),
    Local(LocalPackageLock),
    Private(PrivatePackageLock),
    Tarball(TarballPackageLock),
}

impl DbtPackageLock {
    pub fn package_name(&self) -> String {
        match self {
            DbtPackageLock::Hub(hub_package_lock) => hub_package_lock.name.to_string(),
            DbtPackageLock::Git(git_package_lock) => git_package_lock.name.to_string(),
            DbtPackageLock::Local(local_package_lock) => local_package_lock.name.to_string(),
            DbtPackageLock::Private(private_package_lock) => private_package_lock.name.to_string(),
            DbtPackageLock::Tarball(tarball_package_lock) => tarball_package_lock.name.to_string(),
        }
    }

    pub fn entry_name(&self) -> String {
        match self {
            DbtPackageLock::Hub(hub_package_lock) => hub_package_lock.package.to_string(),
            DbtPackageLock::Git(git_package_lock) => {
                let mut key = git_package_lock.git.to_string();
                if let Some(subdirectory) = &git_package_lock.subdirectory {
                    key.push_str(&format!("#{subdirectory}"));
                }
                key
            }
            DbtPackageLock::Local(local_package_lock) => {
                local_package_lock.local.to_string_lossy().to_string()
            }
            DbtPackageLock::Private(private_package_lock) => {
                let mut key = private_package_lock.private.to_string();
                if let Some(subdirectory) = &private_package_lock.subdirectory {
                    key.push_str(&format!("#{subdirectory}"));
                }
                key
            }
            DbtPackageLock::Tarball(tarball_package_lock) => {
                tarball_package_lock.tarball.to_string()
            }
        }
    }

    pub fn entry_type(&self) -> String {
        match self {
            DbtPackageLock::Hub(_) => "hub".to_string(),
            DbtPackageLock::Git(_) => "git".to_string(),
            DbtPackageLock::Local(_) => "local".to_string(),
            DbtPackageLock::Private(_) => "private".to_string(),
            DbtPackageLock::Tarball(_) => "tarball".to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HubPackageLock {
    pub package: String,
    pub name: String,
    #[serde(rename = "version")]
    pub version: PackageVersion,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GitPackageLock {
    pub git: Verbatim<String>,
    pub name: String,
    pub revision: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warn_unpinned: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subdirectory: Option<String>,
    #[serde(default, skip_serializing)]
    pub __unrendered__: HashMap<String, YmlValue>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LocalPackageLock {
    pub local: PathBuf,
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PrivatePackageLock {
    pub private: Verbatim<String>,
    pub name: String,
    pub revision: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warn_unpinned: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subdirectory: Option<String>,
    #[serde(default, skip_serializing)]
    pub __unrendered__: HashMap<String, YmlValue>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TarballPackageLock {
    pub tarball: Verbatim<String>,
    pub name: String,
    #[serde(default, skip_serializing)]
    pub __unrendered__: HashMap<String, YmlValue>,
}

#[derive(Debug, Serialize, Deserialize, Clone, DbtSchema)]
pub struct TarballPackage {
    /// HTTPS URL of a `.tar.gz` archive containing the dbt package.
    pub tarball: Verbatim<String>,
    #[schemars(skip)]
    #[serde(default, skip_serializing)]
    pub __unrendered__: HashMap<String, YmlValue>,
}

impl From<TarballPackageLock> for TarballPackage {
    fn from(tarball_package_lock: TarballPackageLock) -> Self {
        TarballPackage {
            tarball: tarball_package_lock.tarball,
            __unrendered__: tarball_package_lock.__unrendered__,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct DeprecatedDbtPackagesLock {
    pub packages: Vec<DeprecatedDbtPackageLock>,
    #[serde(default)]
    pub sha1_hash: String,
}

#[derive(Debug, Serialize, UntaggedEnumDeserialize, Clone)]
#[serde(untagged)]
pub enum DeprecatedDbtPackageLock {
    Hub(DeprecatedHubPackageLock),
    Git(DeprecatedGitPackageLock),
    Local(DeprecatedLocalPackageLock),
    Private(DeprecatedPrivatePackageLock),
    Tarball(DeprecatedTarballPackageLock),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeprecatedHubPackageLock {
    pub package: String,
    #[serde(rename = "version")]
    pub version: PackageVersion,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeprecatedGitPackageLock {
    pub git: String,
    pub revision: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warn_unpinned: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subdirectory: Option<String>,
    #[serde(default, skip_serializing)]
    pub __unrendered__: HashMap<String, YmlValue>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeprecatedLocalPackageLock {
    pub local: PathBuf,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeprecatedPrivatePackageLock {
    pub private: String,
    pub revision: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warn_unpinned: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subdirectory: Option<String>,
    #[serde(default, skip_serializing)]
    pub __unrendered__: HashMap<String, YmlValue>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeprecatedTarballPackageLock {
    pub tarball: String,
    #[serde(default, skip_serializing)]
    pub __unrendered__: HashMap<String, YmlValue>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_packages_lock_deserializes() {
        // Empty string (empty file)
        let result: DbtPackagesLock = dbt_yaml::from_str("").unwrap();
        assert!(result.packages.is_empty());
        assert!(result.sha1_hash.is_empty());

        // Fully commented-out content
        let commented =
            "# packages:\n#   - package: foo/bar\n#     version: 1.0.0\n# sha1_hash: abc123\n";
        let result: DbtPackagesLock = dbt_yaml::from_str(commented).unwrap();
        assert!(result.packages.is_empty());
        assert!(result.sha1_hash.is_empty());
    }
}
