use dbt_common::io_args::IoArgs;
use dbt_common::{
    ErrorCode, FsResult,
    constants::{DBT_DEPENDENCIES_YML, DBT_PACKAGES_YML},
    err, fs_err, stdfs,
};
use dbt_jinja_utils::serde::{value_from_file, yaml_to_fs_error};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Read,
    path::{Path, PathBuf},
};

use dbt_jinja_utils::{
    jinja_environment::JinjaEnv,
    serde::{from_yaml_raw, into_typed_with_jinja},
};
use dbt_schemas::schemas::{
    packages::{DbtPackageEntry, DbtPackages},
    profiles::{DbConfig, DbTargets, DbtProfilesIntermediate},
};
use fs_deps::utils::get_local_package_full_path;
use serde::{Serialize, de::DeserializeOwned};
use std::{fs::metadata, io, time::SystemTime};

use ignore::gitignore::Gitignore;
use walkdir::WalkDir;

// ------------------------------------------------------------------------------------------------
// path, directory, and file stuff

pub fn collect_file_info<P: AsRef<Path>>(
    base_path: P,
    relative_paths: &[String],
    info_paths: &mut Vec<(PathBuf, SystemTime)>,
    dbtignore: Option<&Gitignore>,
) -> io::Result<()> {
    if !base_path.as_ref().exists() {
        return Ok(());
    }
    for relative_path in relative_paths {
        let full_path = base_path.as_ref().join(relative_path);
        if !full_path.exists() {
            continue;
        }
        // Configure WalkDir to respect gitignore patterns at the directory level
        let walker = WalkDir::new(full_path);

        // Process files as normal, but use a filter function to skip directories that match gitignore
        for entry_result in walker.into_iter().filter_entry(|e| {
            // If there's no gitignore or if this is not a directory, always process it
            if dbtignore.is_none() || !e.file_type().is_dir() {
                return true;
            }

            // For directories, check if they should be included
            let rel_path = e
                .path()
                .strip_prefix(base_path.as_ref())
                .unwrap_or(e.path());
            !dbtignore.unwrap().matched(rel_path, true).is_ignore()
        }) {
            let entry = entry_result?;
            if entry.file_type().is_file() {
                // Check if this file should be ignored by .dbtignore
                if let Some(gitignore) = dbtignore {
                    let path = entry.path();
                    let relative_to_base = path.strip_prefix(base_path.as_ref()).unwrap_or(path);
                    let is_dir = entry.file_type().is_dir();
                    if gitignore.matched(relative_to_base, is_dir).is_ignore() {
                        continue; // Skip this file as it's ignored
                    }
                }
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

pub fn coalesce<T: Clone>(values: &[Option<T>]) -> Option<T> {
    for value in values {
        if value.is_some() {
            return value.to_owned();
        }
    }
    None
}

pub fn get_db_config(
    _io_args: &IoArgs,
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

    let db_config: DbConfig = dbt_serde_yaml::from_value(db_config.clone()).map_err(|e| {
        fs_err!(
            ErrorCode::InvalidConfig,
            "Failed to parse profiles.yml: {}",
            e
        )
    })?;

    // if !db_config.ignored_properties().is_empty() {
    //     show_warning!(
    //         io_args,
    //         fs_err!(
    //             ErrorCode::InvalidConfig,
    //             "Unused keys in profiles.yml target '{}': {}",
    //             target_name,
    //             db_config
    //                 .ignored_properties()
    //                 .keys()
    //                 .map(|k| format!("'{k}'"))
    //                 .collect::<Vec<String>>()
    //                 .join(", ")
    //         )
    //     );
    // }
    Ok(db_config)
}

pub fn read_profiles_and_extract_db_config<S: Serialize>(
    io_args: &IoArgs,
    target_override: &Option<String>,
    jinja_env: &JinjaEnv,
    ctx: &S,
    profile_str: &str,
    profile_path: PathBuf,
) -> Result<(String, DbConfig), Box<dbt_common::FsError>> {
    let prepared_profile_val = value_from_file(io_args, &profile_path, true, None)?;
    let dbt_profiles = dbt_serde_yaml::from_value::<DbtProfilesIntermediate>(prepared_profile_val)
        .map_err(|e| yaml_to_fs_error(e, Some(&profile_path)))?;
    if dbt_profiles.config.is_some() {
        return err!(
            ErrorCode::InvalidConfig,
            "Unexpected 'config' key in profiles.yml"
        );
    }

    // get the profile value
    let profile_val: &dbt_serde_yaml::Value =
        dbt_profiles.__profiles__.get(profile_str).ok_or(fs_err!(
            ErrorCode::IoError,
            "Profile '{}' not found in profiles.yml",
            profile_str
        ))?;

    // if dbt_target_override is None, render the target name in case the user uses an an env_var jinja expression here
    let rendered_target = if let Some(dbt_target_override) = target_override {
        dbt_target_override.clone()
    } else {
        profile_val
            .get("target")
            .and_then(|v| v.as_str())
            .map(|s| jinja_env.render_str(s, ctx, &[]))
            .transpose()?
            .unwrap_or("default".to_string())
    };
    let unrendered_outputs = profile_val.get("outputs").ok_or(fs_err!(
        ErrorCode::InvalidConfig,
        "No 'outputs' key found in dbt profiles.yml"
    ))?;

    // filter the db_targets to only include the target we want to use
    let unrendered_outputs_filtered: BTreeMap<String, dbt_serde_yaml::Value> = unrendered_outputs
        .as_mapping()
        .unwrap()
        .iter()
        .filter(|(k, _)| k.as_str().unwrap() == rendered_target)
        .map(|(k, v)| (k.as_str().unwrap().to_string(), v.clone()))
        .collect();

    if unrendered_outputs_filtered.is_empty() {
        return err!(
            ErrorCode::InvalidConfig,
            "Target '{}' not found in profiles.yml",
            rendered_target
        );
    }
    // render just the target output we want to use
    let rendered_db_target = into_typed_with_jinja(
        io_args,
        dbt_serde_yaml::to_value(BTreeMap::from([
            (
                "outputs".to_string(),
                dbt_serde_yaml::to_value(&unrendered_outputs_filtered).unwrap(),
            ),
            (
                "target".to_string(),
                dbt_serde_yaml::to_value(&rendered_target).unwrap(),
            ),
        ]))
        .map_err(|e| yaml_to_fs_error(e, Some(&profile_path)))?,
        true,
        jinja_env,
        ctx,
        &[],
        None,
    )?;
    let db_config = get_db_config(io_args, rendered_db_target, Some(rendered_target.clone()))?;

    Ok((rendered_target, db_config))
}

// TODO: this function should read to a yaml::Value so as to avoid double-io
///
/// `dependency_package_name` is used to determine if the file is part of a dependency package,
/// which affects how errors are reported.
pub fn load_raw_yml<T: DeserializeOwned>(
    io_args: &IoArgs,
    path: &Path,
    dependency_package_name: Option<&str>,
) -> FsResult<T> {
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

    from_yaml_raw(io_args, &data, Some(path), true, dependency_package_name)
}

fn process_package_file(
    io_args: &IoArgs,
    package_file_path: &Path,
    package_lookup_map: &BTreeMap<String, String>,
    in_dir: &Path,
    dependency_package_name: Option<&str>,
) -> FsResult<BTreeSet<String>> {
    let mut dependencies = BTreeSet::new();
    let dbt_packages: DbtPackages =
        load_raw_yml(io_args, package_file_path, dependency_package_name)?;
    for package in dbt_packages.packages {
        let entry_name = match package {
            DbtPackageEntry::Hub(hub_package) => hub_package.package,
            DbtPackageEntry::Git(git_package) => {
                let mut key = (*git_package.git).clone();
                if let Some(subdirectory) = &git_package.subdirectory {
                    key.push_str(&format!("#{subdirectory}"));
                }
                key
            }
            DbtPackageEntry::Local(local_package) => {
                let full_path = get_local_package_full_path(in_dir, &local_package);
                let relative_path = stdfs::diff_paths(&full_path, in_dir)?;
                relative_path.to_string_lossy().to_string()
            }
            DbtPackageEntry::Private(private_package) => {
                let mut key = (*private_package.private).clone();
                if let Some(subdirectory) = &private_package.subdirectory {
                    key.push_str(&format!("#{subdirectory}"));
                }
                key
            }
            DbtPackageEntry::Tarball(tarball_package) => (*tarball_package.tarball).clone(),
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
    io_args: &IoArgs,
    in_dir: &Path,
    package_lookup_map: &BTreeMap<String, String>,
    dependency_package_name: Option<&str>,
) -> FsResult<BTreeSet<String>> {
    let mut dependencies = BTreeSet::new();

    // Process dependencies.yml if it exists
    let dependencies_yml_path = in_dir.join(DBT_DEPENDENCIES_YML);
    if dependencies_yml_path.exists() {
        dependencies.extend(process_package_file(
            io_args,
            &dependencies_yml_path,
            package_lookup_map,
            in_dir,
            dependency_package_name,
        )?);
    }

    // Process packages.yml if it exists
    let packages_yml_path = in_dir.join(DBT_PACKAGES_YML);
    if packages_yml_path.exists() {
        dependencies.extend(process_package_file(
            io_args,
            &packages_yml_path,
            package_lookup_map,
            in_dir,
            dependency_package_name,
        )?);
    }

    Ok(dependencies)
}
