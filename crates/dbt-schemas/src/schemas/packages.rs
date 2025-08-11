use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
};

use dbt_serde_yaml::{UntaggedEnumDeserialize, Verbatim};
use serde::{Deserialize, Serialize};

// Type aliases for clarity
type YmlValue = dbt_serde_yaml::Value;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UpstreamProject {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct DbtPackages {
    #[serde(default)]
    pub projects: Vec<UpstreamProject>,
    #[serde(default)]
    pub packages: Vec<DbtPackageEntry>,
}

#[derive(Debug, Serialize, UntaggedEnumDeserialize, Clone)]
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HubPackage {
    pub package: String,
    #[serde(rename = "version", skip_serializing_if = "Option::is_none")]
    pub version: Option<PackageVersion>,
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GitPackage {
    pub git: Verbatim<String>,
    #[serde(rename = "revision", skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    #[serde(rename = "warn-unpinned", skip_serializing_if = "Option::is_none")]
    pub warn_unpinned: Option<bool>,
    #[serde(rename = "subdirectory", skip_serializing_if = "Option::is_none")]
    pub subdirectory: Option<String>,
    #[serde(flatten)]
    pub unrendered: HashMap<String, YmlValue>,
}

impl From<GitPackageLock> for GitPackage {
    fn from(git_package_lock: GitPackageLock) -> Self {
        GitPackage {
            git: git_package_lock.git,
            revision: Some(git_package_lock.revision),
            warn_unpinned: git_package_lock.warn_unpinned,
            subdirectory: git_package_lock.subdirectory,
            unrendered: git_package_lock.unrendered,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PrivatePackage {
    pub private: Verbatim<String>,
    #[serde(rename = "provider", skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(rename = "revision", skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    #[serde(rename = "warn-unpinned", skip_serializing_if = "Option::is_none")]
    pub warn_unpinned: Option<bool>,
    #[serde(rename = "subdirectory", skip_serializing_if = "Option::is_none")]
    pub subdirectory: Option<String>,
    #[serde(flatten)]
    pub unrendered: HashMap<String, YmlValue>,
}

impl From<PrivatePackageLock> for PrivatePackage {
    fn from(private_package_lock: PrivatePackageLock) -> Self {
        PrivatePackage {
            private: private_package_lock.private,
            provider: private_package_lock.provider,
            revision: Some(private_package_lock.revision),
            warn_unpinned: private_package_lock.warn_unpinned,
            subdirectory: private_package_lock.subdirectory,
            unrendered: private_package_lock.unrendered,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LocalPackage {
    pub local: PathBuf,
}

impl From<LocalPackageLock> for LocalPackage {
    fn from(local_package_lock: LocalPackageLock) -> Self {
        LocalPackage {
            local: local_package_lock.local,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum PackageVersion {
    Number(f64),
    String(String),
    Array(Vec<String>),
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct DbtPackagesLock {
    pub packages: Vec<DbtPackageLock>,
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
    #[serde(flatten)]
    pub unrendered: HashMap<String, YmlValue>,
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
    #[serde(flatten)]
    pub unrendered: HashMap<String, YmlValue>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TarballPackageLock {
    pub tarball: Verbatim<String>,
    pub name: String,
    #[serde(flatten)]
    pub unrendered: HashMap<String, YmlValue>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TarballPackage {
    pub tarball: Verbatim<String>,
    #[serde(flatten)]
    pub unrendered: HashMap<String, YmlValue>,
}

impl From<TarballPackageLock> for TarballPackage {
    fn from(tarball_package_lock: TarballPackageLock) -> Self {
        TarballPackage {
            tarball: tarball_package_lock.tarball,
            unrendered: tarball_package_lock.unrendered,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct DeprecatedDbtPackagesLock {
    pub packages: Vec<DeprecatedDbtPackageLock>,
    pub sha1_hash: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum DeprecatedDbtPackageLock {
    // TODO: UntaggedEnumDeserialize does not support inlined struct variants --
    // these must be converted into named structs.
    Hub {
        package: String,
        #[serde(rename = "version")]
        version: PackageVersion,
    },
    Git {
        git: String,
        revision: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        warn_unpinned: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        subdirectory: Option<String>,
        #[serde(flatten)]
        unrendered: HashMap<String, YmlValue>,
    },
    Local {
        local: PathBuf,
    },
}
