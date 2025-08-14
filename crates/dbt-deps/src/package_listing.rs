use dbt_serde_yaml::Verbatim;
use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
};

use dbt_common::{
    ErrorCode, FsResult, constants::DBT_PROJECT_YML, err, io_args::IoArgs,
    io_utils::try_read_yml_to_str, unexpected_fs_err,
};
use dbt_jinja_utils::{
    jinja_environment::JinjaEnv,
    phases::load::LoadContext,
    serde::{from_yaml_raw, into_typed_with_jinja},
};
use dbt_schemas::schemas::{
    packages::{
        DbtPackageEntry, DbtPackages, DbtPackagesLock, GitPackage, HubPackage, LocalPackage,
        PrivatePackage, TarballPackage,
    },
    project::{DbtProject, DbtProjectNameOnly},
};

use crate::{private_package::get_resolved_url, utils::get_local_package_full_path};

use super::types::{
    GitUnpinnedPackage, HubUnpinnedPackage, LocalPinnedPackage, LocalUnpinnedPackage,
    PrivateUnpinnedPackage, TarballUnpinnedPackage,
};

trait Incorporatable {
    #[allow(dead_code)]
    fn incorporate(&mut self, other: Self);
}

impl Incorporatable for GitUnpinnedPackage {
    fn incorporate(&mut self, other: Self) {
        self.incorporate(other);
    }
}

impl Incorporatable for PrivateUnpinnedPackage {
    fn incorporate(&mut self, other: Self) {
        self.incorporate(other);
    }
}

impl Incorporatable for TarballUnpinnedPackage {
    fn incorporate(&mut self, other: Self) {
        self.incorporate(other);
    }
}

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum UnpinnedPackage {
    Hub(HubUnpinnedPackage),
    Git(GitUnpinnedPackage),
    Local(LocalUnpinnedPackage),
    Private(PrivateUnpinnedPackage),
    Tarball(TarballUnpinnedPackage),
}

impl UnpinnedPackage {
    fn type_name(&self) -> &str {
        match self {
            UnpinnedPackage::Hub(_) => "hub",
            UnpinnedPackage::Git(_) => "git",
            UnpinnedPackage::Local(_) => "local",
            UnpinnedPackage::Private(_) => "private",
            UnpinnedPackage::Tarball(_) => "tarball",
        }
    }
}

pub struct PackageListing {
    pub io_args: IoArgs,
    pub vars: BTreeMap<String, dbt_serde_yaml::Value>,
    pub packages: HashMap<String, UnpinnedPackage>,
}

impl PackageListing {
    pub fn new(io_args: IoArgs, vars: BTreeMap<String, dbt_serde_yaml::Value>) -> Self {
        Self {
            io_args,
            vars,
            packages: HashMap::new(),
        }
    }

    pub fn in_dir(&self) -> &Path {
        &self.io_args.in_dir
    }

    pub fn hydrate_dbt_packages(
        &mut self,
        packages: &DbtPackages,
        jinja_env: &JinjaEnv,
    ) -> FsResult<()> {
        for package in packages.packages.iter() {
            self.incorporate(package.clone(), jinja_env)?;
        }
        Ok(())
    }

    pub fn hydrate_dbt_packages_lock(
        &mut self,
        dbt_packages_lock: &DbtPackagesLock,
        jinja_env: &JinjaEnv,
    ) -> FsResult<()> {
        for package in dbt_packages_lock.packages.iter() {
            self.incorporate(package.clone().into(), jinja_env)?;
        }
        Ok(())
    }

    fn incorporate(&mut self, package: DbtPackageEntry, jinja_env: &JinjaEnv) -> FsResult<()> {
        let deps_context = LoadContext::new(self.vars.clone());
        match package {
            DbtPackageEntry::Hub(hub_package) => {
                let hub_package: HubPackage = {
                    let value = dbt_serde_yaml::to_value(&hub_package).map_err(|e| {
                        unexpected_fs_err!("Failed to serialize hub package spec: {e}")
                    })?;
                    into_typed_with_jinja(
                        &self.io_args,
                        value,
                        true,
                        jinja_env,
                        &deps_context,
                        &[],
                        None,
                    )
                }?;
                if let Some(unpinned_package) = self.packages.get_mut(&hub_package.package) {
                    match unpinned_package {
                        UnpinnedPackage::Hub(hub_unpinned_package) => {
                            hub_unpinned_package.incorporate(hub_package.clone().try_into()?);
                        }
                        package_type => {
                            return err!(
                                ErrorCode::InvalidConfig,
                                "Found conflicting package types for package {}: 'hub' vs '{}'",
                                hub_package.package,
                                package_type.type_name(),
                            );
                        }
                    }
                } else {
                    self.packages.insert(
                        hub_package.package.clone(),
                        UnpinnedPackage::Hub(hub_package.clone().try_into()?),
                    );
                }
            }
            DbtPackageEntry::Git(git_package) => {
                let git_package: GitPackage = {
                    let value = dbt_serde_yaml::to_value(&git_package).map_err(|e| {
                        unexpected_fs_err!("Failed to serialize git package spec: {e}")
                    })?;
                    into_typed_with_jinja(
                        &self.io_args,
                        value,
                        true,
                        jinja_env,
                        &deps_context,
                        &[],
                        None,
                    )
                }?;
                let git_package_url: String = {
                    let value = dbt_serde_yaml::to_value(&git_package.git).map_err(|e| {
                        unexpected_fs_err!("Failed to serialize git package URL: {e}")
                    })?;
                    into_typed_with_jinja(
                        &self.io_args,
                        value,
                        true,
                        jinja_env,
                        &deps_context,
                        &[],
                        None,
                    )
                }?;

                // Create key that includes subdirectory if present
                let mut package_key = git_package_url.clone();
                if let Some(subdirectory) = &git_package.subdirectory {
                    package_key.push_str(&format!("#{subdirectory}"));
                }

                self.handle_remote_package(
                    &package_key,
                    UnpinnedPackage::Git(GitUnpinnedPackage {
                        git: git_package_url,
                        name: None,
                        warn_unpinned: git_package.warn_unpinned,
                        revisions: git_package
                            .revision
                            .clone()
                            .map(|v| vec![v])
                            .unwrap_or_default(),
                        subdirectory: git_package.subdirectory.clone(),
                        unrendered: git_package.__unrendered__.clone(),
                        original_entry: git_package,
                    }),
                    "git",
                )?;
            }
            DbtPackageEntry::Local(local_package) => {
                let local_package: LocalPackage = {
                    let value = dbt_serde_yaml::to_value(&local_package).map_err(|e| {
                        unexpected_fs_err!("Failed to serialize local package spec: {e}")
                    })?;
                    into_typed_with_jinja(
                        &self.io_args,
                        value,
                        true,
                        jinja_env,
                        &deps_context,
                        &[],
                        None,
                    )
                }?;
                // Get absolute path of local package
                let full_path = get_local_package_full_path(self.in_dir(), &local_package);
                let path_to_dbt_project = full_path.join(DBT_PROJECT_YML);
                if !path_to_dbt_project.exists() {
                    return err!(
                        ErrorCode::IoError,
                        "Local package does not contain a dbt_project.yml file: {}",
                        local_package.local.display()
                    );
                };

                let yml_data = try_read_yml_to_str(&path_to_dbt_project)?;

                // Try to deserialize only the package name for error reporting,
                // falling back to the path if deserialization fails
                let dependency_package_name = from_yaml_raw::<DbtProjectNameOnly>(
                    &self.io_args,
                    &yml_data,
                    Some(&path_to_dbt_project),
                    // Do not report errors twice. This
                    // parse is only an attempt to get the package name. All actual errors
                    // will be reported when we parse the full `DbtProject` below.
                    false,
                    None,
                )
                .map(|p| p.name)
                .ok()
                .unwrap_or(path_to_dbt_project.to_string_lossy().to_string());

                let dbt_project: DbtProject = from_yaml_raw(
                    &self.io_args,
                    &yml_data,
                    Some(&path_to_dbt_project),
                    true,
                    // TODO: do we really want to hide errors from local packages?
                    // maybe we want to let these ones to show up as project errors?
                    Some(dependency_package_name.as_str()),
                )?;
                self.packages.insert(
                    full_path.to_string_lossy().to_string(),
                    UnpinnedPackage::Local(LocalUnpinnedPackage {
                        local: full_path,
                        name: Some(dbt_project.name),
                    }),
                );
            }
            DbtPackageEntry::Private(private_package) => {
                let mut private_package: PrivatePackage = {
                    let value = dbt_serde_yaml::to_value(&private_package).map_err(|e| {
                        unexpected_fs_err!("Failed to serialize private package spec: {e}")
                    })?;
                    into_typed_with_jinja(
                        &self.io_args,
                        value,
                        true,
                        jinja_env,
                        &deps_context,
                        &[],
                        None,
                    )
                }?;
                let private_package_private: String = {
                    let value =
                        dbt_serde_yaml::to_value(&private_package.private).map_err(|e| {
                            unexpected_fs_err!("Failed to serialize private package URL: {e}")
                        })?;
                    into_typed_with_jinja(
                        &self.io_args,
                        value,
                        true,
                        jinja_env,
                        &deps_context,
                        &[],
                        None,
                    )
                }?;

                private_package.private = Verbatim::from(private_package_private);

                let private_package_url = get_resolved_url(&private_package)?;

                // Create key that includes subdirectory if present
                let mut package_key = private_package_url.clone();
                if let Some(subdirectory) = &private_package.subdirectory {
                    package_key.push_str(&format!("#{subdirectory}"));
                }

                self.handle_remote_package(
                    &package_key,
                    UnpinnedPackage::Private(PrivateUnpinnedPackage {
                        private: private_package_url,
                        name: None,
                        provider: private_package.provider.clone(),
                        warn_unpinned: private_package.warn_unpinned,
                        revisions: private_package
                            .revision
                            .clone()
                            .map(|v| vec![v])
                            .unwrap_or_default(),
                        subdirectory: private_package.subdirectory.clone(),
                        unrendered: private_package.__unrendered__.clone(),
                        original_entry: private_package,
                    }),
                    "private",
                )?;
            }
            DbtPackageEntry::Tarball(tarball_package) => {
                let tarball_package: TarballPackage = {
                    let value = dbt_serde_yaml::to_value(&tarball_package).map_err(|e| {
                        unexpected_fs_err!("Failed to serialize tarball package spec: {e}")
                    })?;
                    into_typed_with_jinja(
                        &self.io_args,
                        value,
                        true,
                        jinja_env,
                        &deps_context,
                        &[],
                        None,
                    )
                }?;
                let tarball_url: String = {
                    let value =
                        dbt_serde_yaml::to_value(&tarball_package.tarball).map_err(|e| {
                            unexpected_fs_err!("Failed to serialize tarball package URL: {e}")
                        })?;
                    into_typed_with_jinja(
                        &self.io_args,
                        value,
                        true,
                        jinja_env,
                        &deps_context,
                        &[],
                        None,
                    )
                }?;

                self.handle_remote_package(
                    &tarball_url.clone(),
                    UnpinnedPackage::Tarball(TarballUnpinnedPackage {
                        tarball: tarball_url,
                        name: None,
                        unrendered: tarball_package.__unrendered__.clone(),
                        original_entry: tarball_package,
                    }),
                    "tarball",
                )?;
            }
        }
        Ok(())
    }

    fn handle_remote_package(
        &mut self,
        package_key: &str,
        new_package: UnpinnedPackage,
        package_type: &str,
    ) -> FsResult<()> {
        if let Some(existing_package) = self.packages.get_mut(package_key) {
            match existing_package {
                UnpinnedPackage::Git(existing_git_package) if package_type == "git" => {
                    if let UnpinnedPackage::Git(new_git_package) = new_package {
                        existing_git_package.incorporate(new_git_package);
                    }
                }
                UnpinnedPackage::Private(existing_private_package) if package_type == "private" => {
                    if let UnpinnedPackage::Private(new_private_package) = new_package {
                        existing_private_package.incorporate(new_private_package);
                    }
                }
                UnpinnedPackage::Tarball(existing_tarball_package) if package_type == "tarball" => {
                    if let UnpinnedPackage::Tarball(new_tarball_package) = new_package {
                        existing_tarball_package.incorporate(new_tarball_package);
                    }
                }
                _ => {
                    return err!(
                        ErrorCode::InvalidConfig,
                        "Found conflicting package types for package {}: '{}' vs '{}'",
                        package_key,
                        package_type,
                        existing_package.type_name(),
                    );
                }
            }
        } else {
            self.packages.insert(package_key.to_string(), new_package);
        }
        Ok(())
    }

    fn handle_remote_unpinned_package<T: Incorporatable + Clone>(
        &mut self,
        package_key: &str,
        new_package: &UnpinnedPackage,
        package_type: &str,
    ) -> FsResult<()> {
        if let Some(existing_package) = self.packages.get_mut(package_key) {
            match existing_package {
                UnpinnedPackage::Git(existing_git_package) if package_type == "git" => {
                    if let UnpinnedPackage::Git(new_git_package) = new_package {
                        existing_git_package.incorporate(new_git_package.clone());
                    }
                }
                UnpinnedPackage::Private(existing_private_package) if package_type == "private" => {
                    if let UnpinnedPackage::Private(new_private_package) = new_package {
                        existing_private_package.incorporate(new_private_package.clone());
                    }
                }
                UnpinnedPackage::Tarball(existing_tarball_package) if package_type == "tarball" => {
                    if let UnpinnedPackage::Tarball(new_tarball_package) = new_package {
                        existing_tarball_package.incorporate(new_tarball_package.clone());
                    }
                }
                _ => {
                    return err!(
                        ErrorCode::InvalidConfig,
                        "Found conflicting package types for package {}: '{}' vs '{}'",
                        package_key,
                        package_type,
                        existing_package.type_name(),
                    );
                }
            }
        } else {
            self.packages
                .insert(package_key.to_string(), new_package.clone());
        }
        Ok(())
    }

    pub fn incorporate_unpinned_package(&mut self, package: &UnpinnedPackage) -> FsResult<()> {
        match package {
            UnpinnedPackage::Hub(hub_unpinned_package) => {
                if let Some(existing_hub_unpinned_package) =
                    self.packages.get_mut(&hub_unpinned_package.package)
                {
                    match existing_hub_unpinned_package {
                        UnpinnedPackage::Hub(existing_hub_unpinned_package) => {
                            existing_hub_unpinned_package.incorporate(hub_unpinned_package.clone());
                        }
                        package_type => {
                            return err!(
                                ErrorCode::InvalidConfig,
                                "Found conflicting package types for package {}: 'hub' vs '{}'",
                                hub_unpinned_package.package,
                                package_type.type_name(),
                            );
                        }
                    }
                } else {
                    self.packages
                        .insert(hub_unpinned_package.package.clone(), package.clone());
                }
            }
            UnpinnedPackage::Git(git_unpinned_package) => {
                // Create key that includes subdirectory if present
                let mut package_key = git_unpinned_package.git.clone();
                if let Some(subdirectory) = &git_unpinned_package.subdirectory {
                    package_key.push_str(&format!("#{subdirectory}"));
                }
                self.handle_remote_unpinned_package::<GitUnpinnedPackage>(
                    &package_key,
                    package,
                    "git",
                )?;
            }
            UnpinnedPackage::Local(local_package) => {
                let pinned_package = LocalPinnedPackage::try_from(local_package.clone())?;
                if let Some(existing_local_unpinned_package) =
                    self.packages.get_mut(&pinned_package.name)
                {
                    match existing_local_unpinned_package {
                        UnpinnedPackage::Local(existing_local_unpinned_package) => {
                            if existing_local_unpinned_package.local != pinned_package.local {
                                return err!(
                                    ErrorCode::InvalidConfig,
                                    "Found conflicting package paths for package {}: '{}' vs '{}'",
                                    pinned_package.name,
                                    existing_local_unpinned_package.local.to_string_lossy(),
                                    pinned_package.local.to_string_lossy(),
                                );
                            }
                        }
                        _ => {
                            return err!(
                                ErrorCode::InvalidConfig,
                                "Found conflicting package types for package {}: 'local' vs '{}'",
                                pinned_package.name,
                                existing_local_unpinned_package.type_name(),
                            );
                        }
                    }
                } else {
                    self.packages.insert(
                        pinned_package.name.to_string(),
                        UnpinnedPackage::Local(LocalUnpinnedPackage {
                            local: pinned_package.local,
                            name: Some(pinned_package.name.clone()),
                        }),
                    );
                }
            }
            UnpinnedPackage::Private(private_unpinned_package) => {
                // Create key that includes subdirectory if present
                let mut package_key = private_unpinned_package.private.clone();
                if let Some(subdirectory) = &private_unpinned_package.subdirectory {
                    package_key.push_str(&format!("#{subdirectory}"));
                }
                self.handle_remote_unpinned_package::<PrivateUnpinnedPackage>(
                    &package_key,
                    package,
                    "private",
                )?;
            }
            UnpinnedPackage::Tarball(tarball_unpinned_package) => {
                self.handle_remote_unpinned_package::<TarballUnpinnedPackage>(
                    &tarball_unpinned_package.tarball,
                    package,
                    "tarball",
                )?;
            }
        }
        Ok(())
    }

    pub fn update_from(
        &mut self,
        packages: &Vec<DbtPackageEntry>,
        jinja_env: &JinjaEnv,
    ) -> FsResult<()> {
        for package in packages {
            self.incorporate(package.clone(), jinja_env)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_common::io_args::IoArgs;
    use std::collections::HashMap;

    #[test]
    fn test_handle_remote_package_with_subdirectory() {
        let io_args = IoArgs::default();
        let mut package_listing = PackageListing::new(io_args, BTreeMap::new());

        // Create two git packages with the same URL but different subdirectories
        let git_package_1 = UnpinnedPackage::Git(GitUnpinnedPackage {
            git: "https://github.com/dbt-labs/dbt-core.git".to_string(),
            name: None,
            warn_unpinned: None,
            revisions: vec!["main".to_string()],
            subdirectory: Some("core".to_string()),
            unrendered: HashMap::new(),
            original_entry: GitPackage {
                git: Verbatim::from("https://github.com/dbt-labs/dbt-core.git".to_string()),
                revision: Some("main".to_string()),
                warn_unpinned: None,
                subdirectory: Some("core".to_string()),
                __unrendered__: HashMap::new(),
            },
        });

        let git_package_2 = UnpinnedPackage::Git(GitUnpinnedPackage {
            git: "https://github.com/dbt-labs/dbt-core.git".to_string(),
            name: None,
            warn_unpinned: None,
            revisions: vec!["main".to_string()],
            subdirectory: Some("adapters".to_string()),
            unrendered: HashMap::new(),
            original_entry: GitPackage {
                git: Verbatim::from("https://github.com/dbt-labs/dbt-core.git".to_string()),
                revision: Some("main".to_string()),
                warn_unpinned: None,
                subdirectory: Some("adapters".to_string()),
                __unrendered__: HashMap::new(),
            },
        });

        // Add the first package
        package_listing
            .handle_remote_package(
                "https://github.com/dbt-labs/dbt-core.git#core",
                git_package_1,
                "git",
            )
            .unwrap();

        // Add the second package - should be treated as a separate package
        package_listing
            .handle_remote_package(
                "https://github.com/dbt-labs/dbt-core.git#adapters",
                git_package_2,
                "git",
            )
            .unwrap();

        // Verify that both packages are stored with different keys
        assert_eq!(package_listing.packages.len(), 2);
        assert!(
            package_listing
                .packages
                .contains_key("https://github.com/dbt-labs/dbt-core.git#core")
        );
        assert!(
            package_listing
                .packages
                .contains_key("https://github.com/dbt-labs/dbt-core.git#adapters")
        );
    }

    #[test]
    fn test_handle_remote_package_same_url_no_subdirectory() {
        let io_args = IoArgs::default();
        let mut package_listing = PackageListing::new(io_args, BTreeMap::new());

        // Create two git packages with the same URL and no subdirectory
        let git_package_1 = UnpinnedPackage::Git(GitUnpinnedPackage {
            git: "https://github.com/dbt-labs/dbt-core.git".to_string(),
            name: None,
            warn_unpinned: None,
            revisions: vec!["main".to_string()],
            subdirectory: None,
            unrendered: HashMap::new(),
            original_entry: GitPackage {
                git: Verbatim::from("https://github.com/dbt-labs/dbt-core.git".to_string()),
                revision: Some("main".to_string()),
                warn_unpinned: None,
                subdirectory: None,
                __unrendered__: HashMap::new(),
            },
        });

        let git_package_2 = UnpinnedPackage::Git(GitUnpinnedPackage {
            git: "https://github.com/dbt-labs/dbt-core.git".to_string(),
            name: None,
            warn_unpinned: None,
            revisions: vec!["develop".to_string()],
            subdirectory: None,
            unrendered: HashMap::new(),
            original_entry: GitPackage {
                git: Verbatim::from("https://github.com/dbt-labs/dbt-core.git".to_string()),
                revision: Some("develop".to_string()),
                warn_unpinned: None,
                subdirectory: None,
                __unrendered__: HashMap::new(),
            },
        });

        // Add the first package
        package_listing
            .handle_remote_package(
                "https://github.com/dbt-labs/dbt-core.git",
                git_package_1,
                "git",
            )
            .unwrap();

        // Add the second package - should be incorporated into the first one
        package_listing
            .handle_remote_package(
                "https://github.com/dbt-labs/dbt-core.git",
                git_package_2,
                "git",
            )
            .unwrap();

        // Verify that only one package is stored (they should be incorporated)
        assert_eq!(package_listing.packages.len(), 1);
        assert!(
            package_listing
                .packages
                .contains_key("https://github.com/dbt-labs/dbt-core.git")
        );
    }
}
