use dbt_serde_yaml::Verbatim;
use std::{collections::HashMap, path::Path};

use dbt_common::{
    constants::DBT_PROJECT_YML, err, io_args::IoArgs, io_utils::try_read_yml_to_str,
    unexpected_fs_err, ErrorCode, FsResult,
};
use dbt_jinja_utils::{
    jinja_environment::JinjaEnvironment,
    serde::{from_yaml_raw, into_typed_with_jinja},
};
use dbt_schemas::schemas::{
    packages::{
        DbtPackageEntry, DbtPackages, DbtPackagesLock, GitPackage, HubPackage, LocalPackage,
        PrivatePackage,
    },
    project::DbtProject,
};

use crate::{
    private_package::get_resolved_url, types::LocalPinnedPackage,
    utils::get_local_package_full_path,
};

use super::types::{
    GitUnpinnedPackage, HubUnpinnedPackage, LocalUnpinnedPackage, PrivateUnpinnedPackage,
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

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum UnpinnedPackage {
    Hub(HubUnpinnedPackage),
    Git(GitUnpinnedPackage),
    Local(LocalUnpinnedPackage),
    Private(PrivateUnpinnedPackage),
}

impl UnpinnedPackage {
    fn type_name(&self) -> &str {
        match self {
            UnpinnedPackage::Hub(_) => "hub",
            UnpinnedPackage::Git(_) => "git",
            UnpinnedPackage::Local(_) => "local",
            UnpinnedPackage::Private(_) => "private",
        }
    }
}

pub struct PackageListing {
    pub io_args: IoArgs,
    pub packages: HashMap<String, UnpinnedPackage>,
}

impl PackageListing {
    pub fn new(io_args: IoArgs) -> Self {
        Self {
            io_args,
            packages: HashMap::new(),
        }
    }

    pub fn in_dir(&self) -> &Path {
        &self.io_args.in_dir
    }

    pub fn hydrate_dbt_packages(
        &mut self,
        packages: &DbtPackages,
        jinja_env: &JinjaEnvironment<'static>,
    ) -> FsResult<()> {
        for package in packages.packages.iter() {
            self.incorporate(package.clone(), jinja_env)?;
        }
        Ok(())
    }

    pub fn hydrate_dbt_packages_lock(
        &mut self,
        dbt_packages_lock: &DbtPackagesLock,
        jinja_env: &JinjaEnvironment<'static>,
    ) -> FsResult<()> {
        for package in dbt_packages_lock.packages.iter() {
            self.incorporate(package.clone().into(), jinja_env)?;
        }
        Ok(())
    }

    fn incorporate(
        &mut self,
        package: DbtPackageEntry,
        jinja_env: &JinjaEnvironment<'static>,
    ) -> FsResult<()> {
        match package {
            DbtPackageEntry::Hub(hub_package) => {
                let hub_package: HubPackage = {
                    let value = dbt_serde_yaml::to_value(&hub_package).map_err(|e| {
                        unexpected_fs_err!("Failed to serialize hub package spec: {e}")
                    })?;
                    into_typed_with_jinja(Some(&self.io_args), value, true, jinja_env, &(), None)
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
                    into_typed_with_jinja(Some(&self.io_args), value, true, jinja_env, &(), None)
                }?;
                let git_package_url: String = {
                    let value = dbt_serde_yaml::to_value(&git_package.git).map_err(|e| {
                        unexpected_fs_err!("Failed to serialize git package URL: {e}")
                    })?;
                    into_typed_with_jinja(Some(&self.io_args), value, true, jinja_env, &(), None)
                }?;

                self.handle_remote_package(
                    &git_package_url.clone(),
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
                        unrendered: git_package.unrendered.clone(),
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
                    into_typed_with_jinja(Some(&self.io_args), value, true, jinja_env, &(), None)
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
                let dbt_project: DbtProject = from_yaml_raw(
                    Some(&self.io_args),
                    &try_read_yml_to_str(&path_to_dbt_project)?,
                    Some(&path_to_dbt_project),
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
                    into_typed_with_jinja(Some(&self.io_args), value, true, jinja_env, &(), None)
                }?;
                let private_package_private: String = {
                    let value =
                        dbt_serde_yaml::to_value(&private_package.private).map_err(|e| {
                            unexpected_fs_err!("Failed to serialize private package URL: {e}")
                        })?;
                    into_typed_with_jinja(Some(&self.io_args), value, true, jinja_env, &(), None)
                }?;

                private_package.private = Verbatim(private_package_private);

                let private_package_url = get_resolved_url(&private_package)?;
                self.handle_remote_package(
                    &private_package_url.clone(),
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
                        unrendered: private_package.unrendered.clone(),
                        original_entry: private_package,
                    }),
                    "private",
                )?;
            }
        }
        Ok(())
    }

    fn handle_remote_package(
        &mut self,
        package_url: &str,
        new_package: UnpinnedPackage,
        package_type: &str,
    ) -> FsResult<()> {
        if let Some(existing_package) = self.packages.get_mut(package_url) {
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
                _ => {
                    return err!(
                        ErrorCode::InvalidConfig,
                        "Found conflicting package types for package {}: '{}' vs '{}'",
                        package_url,
                        package_type,
                        existing_package.type_name(),
                    );
                }
            }
        } else {
            self.packages.insert(package_url.to_string(), new_package);
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
                self.handle_remote_unpinned_package::<GitUnpinnedPackage>(
                    &git_unpinned_package.git,
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
                self.handle_remote_unpinned_package::<PrivateUnpinnedPackage>(
                    &private_unpinned_package.private,
                    package,
                    "private",
                )?;
            }
        }
        Ok(())
    }

    pub fn update_from(
        &mut self,
        packages: &Vec<DbtPackageEntry>,
        jinja_env: &JinjaEnvironment<'static>,
    ) -> FsResult<()> {
        for package in packages {
            self.incorporate(package.clone(), jinja_env)?;
        }
        Ok(())
    }
}
