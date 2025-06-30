use dbt_common::io_args::IoArgs;
use dbt_common::{
    constants::{DBT_DEPENDENCIES_YML, DBT_PACKAGES_YML},
    err, fs_err, show_warning, stdfs, ErrorCode, FsResult,
};
use dbt_jinja_utils::serde::{from_yaml_error, value_from_file};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Read,
    path::{Path, PathBuf},
};

use dbt_jinja_utils::{
    jinja_environment::JinjaEnvironment,
    serde::{from_yaml_raw, into_typed_with_jinja},
};
use dbt_schemas::schemas::{
    packages::{DbtPackageEntry, DbtPackages},
    profiles::{DbConfig, DbTargets, DbtProfilesIntermediate},
};
use fs_deps::utils::get_local_package_full_path;
use serde::{de::DeserializeOwned, Serialize};
use std::{fs::metadata, io, time::SystemTime};

use walkdir::WalkDir;

// ------------------------------------------------------------------------------------------------
// path, directory, and file stuff

pub fn collect_file_info<P: AsRef<Path>>(
    base_path: P,
    relative_paths: &[String],
    info_paths: &mut Vec<(PathBuf, SystemTime)>,
) -> io::Result<()> {
    if !base_path.as_ref().exists() {
        return Ok(());
    }
    for relative_path in relative_paths {
        let full_path = base_path.as_ref().join(relative_path);
        if !full_path.exists() {
            continue;
        }
        for entry in WalkDir::new(full_path) {
            let entry = entry?;
            if entry.file_type().is_file() {
                let metadata = metadata(entry.path())?;
                let modified_time = metadata.modified()?;
                info_paths.push((entry.path().to_path_buf(), modified_time));
            }
        }
    }
    Ok(())
}

// ------------------------------------------------------------------------------------------------
// string stuff
pub fn indent(data: &str, spaces: usize) -> String {
    let indent = " ".repeat(spaces);
    data.lines()
        .map(|line| format!("{indent}{line}"))
        .collect::<Vec<String>>()
        .join("\n")
}

// ------------------------------------------------------------------------------------------------
// stupid other helpers:

pub fn coalesce<T: Clone>(values: &[&Option<T>]) -> Option<T> {
    for value in values {
        if value.is_some() {
            return value.to_owned().to_owned();
        }
    }
    None
}

pub fn get_db_config(
    io_args: &IoArgs,
    db_targets: DbTargets,
    maybe_target: Option<String>,
) -> FsResult<DbConfig> {
    let target_name = maybe_target.unwrap_or(db_targets.default_target.clone());
    // 6. Find the desired target
    let db_config = db_targets.outputs.get(&target_name).ok_or(fs_err!(
        ErrorCode::InvalidConfig,
        "Could not find target {} in profiles.yml",
        target_name,
    ))?;
    let db_config: DbConfig = serde_json::from_value(db_config.clone())?;

    if !db_config.ignored_properties().is_empty() {
        show_warning!(
            io_args,
            fs_err!(
                ErrorCode::InvalidConfig,
                "Unused keys in profiles.yml target '{}': {}",
                target_name,
                db_config
                    .ignored_properties()
                    .keys()
                    .map(|k| format!("'{k}'"))
                    .collect::<Vec<String>>()
                    .join(", ")
            )
        );
    }
    Ok(db_config)
}

pub fn read_profiles_and_extract_db_config<S: Serialize>(
    io_args: &IoArgs,
    dbt_target_override: &Option<String>,
    jinja_env: &JinjaEnvironment<'static>,
    ctx: &S,
    profile_str: &str,
    profile_path: PathBuf,
) -> Result<(String, DbConfig), Box<dbt_common::FsError>> {
    let prepared_profile_val = value_from_file(Some(io_args), &profile_path)?;
    let dbt_profiles = dbt_serde_yaml::from_value::<DbtProfilesIntermediate>(prepared_profile_val)
        .map_err(|e| from_yaml_error(e, Some(&profile_path)))?;
    if dbt_profiles.config.is_some() {
        return err!(
            ErrorCode::InvalidConfig,
            "Unexpected 'config' key in dbt profiles.yml"
        );
    }
    let profile_val = dbt_profiles.profiles.get(profile_str).ok_or(fs_err!(
        ErrorCode::IoError,
        "Profile '{}' not found in dbt profiles.yml",
        profile_str
    ))?;
    let db_targets: DbTargets = into_typed_with_jinja(
        Some(io_args),
        profile_val.clone(),
        true,
        jinja_env,
        &ctx,
        &[],
    )?;
    let target = match dbt_target_override {
        Some(target) => db_targets
            .outputs
            .keys()
            .any(|t| t == target)
            .then(|| target.clone())
            .ok_or(fs_err!(
                ErrorCode::IoError,
                "Target '{}' not found in dbt profiles.yml",
                target
            ))?,
        None => db_targets.default_target.clone(),
    };
    let db_config = get_db_config(io_args, db_targets, Some(target.clone()))?;
    Ok((target, db_config))
}

// TODO: this function should read to a yaml::Value so as to avoid double-io
pub fn load_raw_yml<T: DeserializeOwned>(path: &Path) -> FsResult<T> {
    let mut file = std::fs::File::open(path).map_err(|e| {
        fs_err!(
            code => ErrorCode::IoError,
            loc => path.to_path_buf(),
            "Cannot open file dbt_project.yml: {}",
            e,
        )
    })?;
    let mut data = String::new();
    file.read_to_string(&mut data).map_err(|e| {
        fs_err!(
            code => ErrorCode::IoError,
            loc => path.to_path_buf(),
            "Cannot read file dbt_project.yml: {}",
            e,
        )
    })?;

    from_yaml_raw(None, &data, Some(path))
}

fn process_package_file(
    package_file_path: &Path,
    package_lookup_map: &BTreeMap<String, String>,
    in_dir: &Path,
) -> FsResult<BTreeSet<String>> {
    let mut dependencies = BTreeSet::new();
    let dbt_packages: DbtPackages = load_raw_yml(package_file_path)?;
    for package in dbt_packages.packages {
        let entry_name = match package {
            DbtPackageEntry::Hub(hub_package) => hub_package.package,
            DbtPackageEntry::Git(git_package) => (*git_package.git).clone(),
            DbtPackageEntry::Local(local_package) => {
                let full_path = get_local_package_full_path(in_dir, &local_package);
                let relative_path = stdfs::diff_paths(&full_path, in_dir)?;
                relative_path.to_string_lossy().to_string()
            }
            DbtPackageEntry::Private(private_package) => (*private_package.private).clone(),
        };
        if let Some(entry_name) = package_lookup_map.get(&entry_name) {
            dependencies.insert(entry_name.to_string());
        } else {
            return err!(
                ErrorCode::InvalidConfig,
                "Could not find package {} in the package lookup map",
                entry_name
            );
        }
    }
    Ok(dependencies)
}

pub fn identify_package_dependencies(
    in_dir: &Path,
    package_lookup_map: &BTreeMap<String, String>,
) -> FsResult<BTreeSet<String>> {
    let mut dependencies = BTreeSet::new();

    // Process dependencies.yml if it exists
    let dependencies_yml_path = in_dir.join(DBT_DEPENDENCIES_YML);
    if dependencies_yml_path.exists() {
        dependencies.extend(process_package_file(
            &dependencies_yml_path,
            package_lookup_map,
            in_dir,
        )?);
    }

    // Process packages.yml if it exists
    let packages_yml_path = in_dir.join(DBT_PACKAGES_YML);
    if packages_yml_path.exists() {
        dependencies.extend(process_package_file(
            &packages_yml_path,
            package_lookup_map,
            in_dir,
        )?);
    }

    Ok(dependencies)
}
