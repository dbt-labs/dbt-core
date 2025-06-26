use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use dbt_common::once_cell_vars::DISPATCH_CONFIG;
use dbt_jinja_utils::invocation_args::InvocationArgs;
use dbt_jinja_utils::jinja_environment::JinjaEnvironment;
use dbt_jinja_utils::phases::load::init::initialize_load_jinja_environment;
use dbt_jinja_utils::serde::from_yaml_error;
use dbt_schemas::schemas::serde::StringOrInteger;
use fs_deps::get_or_install_packages;
use pathdiff::diff_paths;
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::time::SystemTime;

use dbt_common::constants::{
    DBT_INTERNAL_PACKAGES_DIR_NAME, DBT_PACKAGES_DIR_NAME, DBT_PROJECT_YML, LOADING,
};
use dbt_common::error::LiftableResult;
use project::DbtProject;

use dbt_common::stdfs::last_modified;
use dbt_common::{ectx, err, show_progress, with_progress, ErrorCode};
use dbt_common::{fs_err, FsResult};
use dbt_schemas::schemas::project::{self, DbtProjectSimplified, ProjectDbtCloudConfig};
use dbt_schemas::state::{DbtAsset, DbtPackage, DbtState, DbtVars, ResourcePathKind};

use crate::args::LoadArgs;
use crate::dbt_project_yml_loader::load_project_yml;
use crate::download_publication::download_publication_artifacts;
use crate::utils::{collect_file_info, identify_package_dependencies, load_raw_yml};
use crate::{
    load_internal_packages, load_packages, load_profiles, load_vars, persist_internal_packages,
};
use dbt_common::fsinfo;

pub async fn load(
    arg: &LoadArgs,
    iarg: &InvocationArgs,
) -> FsResult<(DbtState, Option<usize>, Option<ProjectDbtCloudConfig>)> {
    let _pb = with_progress!(arg.io, spinner => LOADING);

    // Read the input file
    let dbt_project_path = arg.io.in_dir.join(DBT_PROJECT_YML);
    let raw_dbt_project: DbtProjectSimplified = load_raw_yml(&dbt_project_path)?;
    if raw_dbt_project.data_paths.is_some() {
        return err!(
            ErrorCode::InvalidConfig,
            "'data-paths' cannot be specified in dbt_project.yml",
        );
    }
    if raw_dbt_project.source_paths.is_some() {
        return err!(
            ErrorCode::InvalidConfig,
            "'source-paths' cannot be specified in dbt_project.yml",
        );
    }
    if raw_dbt_project
        .log_path
        .as_ref()
        .is_some_and(|path| path != "logs")
    {
        return err!(
            ErrorCode::InvalidConfig,
            "'log-path' cannot be specified in dbt_project.yml",
        );
    }
    if raw_dbt_project
        .target_path
        .as_ref()
        .is_some_and(|path| path != "target")
    {
        return err!(
            ErrorCode::InvalidConfig,
            "'target-path' cannot be specified in dbt_project.yml",
        );
    }

    let mut dbt_profile = load_profiles(arg, iarg, &raw_dbt_project)?;
    // Check if .gitignore exists and add dbt_internal_packages/ if not present
    let gitignore_path = arg.io.in_dir.join(".gitignore");
    if gitignore_path.exists() {
        let gitignore_content = fs::read_to_string(&gitignore_path)?;
        if !gitignore_content.contains(format!("{}/", DBT_INTERNAL_PACKAGES_DIR_NAME).as_str()) {
            let mut updated_content = gitignore_content;
            if !updated_content.ends_with('\n') {
                updated_content.push('\n');
            }
            updated_content.push_str(format!("{}/\n", DBT_INTERNAL_PACKAGES_DIR_NAME).as_str());
            fs::write(&gitignore_path, updated_content)?;
        }
    }

    let final_threads = if iarg.num_threads.is_none() {
        if let Some(threads) = dbt_profile.db_config.get_threads() {
            // Convert StringOrInteger to Option<usize>
            match threads {
                StringOrInteger::Integer(n) => Some(n as usize),
                StringOrInteger::String(ref s) => Some(s.parse::<usize>().map_err(|_| {
                    fs_err!(
                        ErrorCode::Generic,
                        "Invalid number of threads in profiles.yml : {}",
                        s
                    )
                })?),
            }
        } else {
            None
        }
    } else {
        iarg.num_threads
    };

    dbt_profile
        .db_config
        .set_threads(Some(StringOrInteger::Integer(
            final_threads.unwrap_or(0) as i64
        )));
    let iarg = iarg.set_num_threads(final_threads);

    let mut dbt_state = DbtState {
        dbt_profile,
        run_started_at: run_started_at(),
        packages: vec![],
        vars: BTreeMap::new(),
        cli_vars: arg.vars.clone(),
    };

    // If we are running `dbt debug` we don't need to collect dbt_project.yml files
    if arg.debug_profile {
        return Ok((dbt_state, final_threads, raw_dbt_project.dbt_cloud));
    }

    // Load the packages.yml file, if it exists and install the packages if arg.install_deps is true
    let (packages_install_path, internal_packages_install_path) = get_packages_install_path(
        &arg.io.in_dir,
        &arg.packages_install_path,
        &arg.internal_packages_install_path,
        &raw_dbt_project,
    );

    persist_internal_packages(
        &internal_packages_install_path,
        &dbt_state.dbt_profile.db_config.adapter_type(),
    )?;
    let flags: BTreeMap<String, minijinja::Value> = iarg.to_dict();

    let mut env = initialize_load_jinja_environment(
        &dbt_state.dbt_profile.profile,
        &dbt_state.dbt_profile.target,
        &dbt_state.dbt_profile.db_config.adapter_type(),
        &dbt_state.dbt_profile.db_config,
        dbt_state.run_started_at,
        &flags,
        arg.io.clone(),
    )?;

    let (packages_lock, upstream_projects) = get_or_install_packages(
        &arg.io,
        &mut env,
        &packages_install_path,
        arg.install_deps,
        arg.vars.clone(),
    )
    .await?;
    // get publication artifact for each upstream project
    download_publication_artifacts(&upstream_projects, &raw_dbt_project.dbt_cloud, &arg.io).await?;
    // If we are running `dbt deps` we don't need to collect files
    if arg.install_deps {
        return Ok((dbt_state, final_threads, raw_dbt_project.dbt_cloud));
    }

    let lookup_map = packages_lock.lookup_map();
    let mut collected_vars = vec![];
    {
        let _pb = with_progress!( arg.io, spinner => LOADING, item => "packages" );

        let packages = load_packages(
            arg,
            &mut env,
            &mut collected_vars,
            &lookup_map,
            &packages_install_path,
        )
        .await?;
        dbt_state.packages = packages;
    }

    {
        let _pb = with_progress!( arg.io, spinner => LOADING, item => "internal packages" );

        let packages = load_internal_packages(
            arg,
            &mut env,
            &mut collected_vars,
            &internal_packages_install_path,
        )
        .await?;
        dbt_state.packages.extend(packages);
        dbt_state.vars = collected_vars.into_iter().collect();
    }
    Ok((dbt_state, final_threads, raw_dbt_project.dbt_cloud))
}

pub async fn load_inner(
    arg: &LoadArgs,
    env: &mut JinjaEnvironment<'static>,
    package_path: &Path,
    package_lookup_map: &BTreeMap<String, String>,
    collected_vars: &mut Vec<(String, BTreeMap<String, DbtVars>)>,
) -> FsResult<DbtPackage> {
    // all read files
    let mut all_files: HashMap<ResourcePathKind, Vec<(PathBuf, SystemTime)>> = HashMap::new();

    let dbt_project_path = package_path.join(DBT_PROJECT_YML);

    let show_base_path = if package_path != arg.io.in_dir {
        // Show path from the packages `install-dir`
        package_path
            .parent()
            .expect("Failed to get parent directory")
            .parent()
            .expect("Failed to get parent directory")
            .to_path_buf()
    } else {
        arg.io.in_dir.clone()
    };

    let show_project_path = diff_paths(&dbt_project_path, &show_base_path).unwrap();
    show_progress!(
        arg.io,
        fsinfo!(LOADING.into(), show_project_path.display().to_string())
    );

    let dbt_project = load_project_yml(&arg.io, env, &dbt_project_path, arg.vars.clone())?;
    load_vars(
        &dbt_project.name,
        dbt_project
            .vars
            .as_ref()
            .map(|vars| Deserialize::deserialize(vars.clone()))
            .transpose()
            .map_err(|e| from_yaml_error(e, Some(&dbt_project_path)))?,
        collected_vars,
    )?;
    // Set dispatch config for future use
    if package_path == arg.io.in_dir {
        let dispatch_config_map = if let Some(dispatch_configs) = dbt_project.dispatch.clone() {
            dispatch_configs
                .iter()
                .map(|dispatch_config| {
                    (
                        dispatch_config.macro_namespace.clone(),
                        dispatch_config.search_order.clone(),
                    )
                })
                .collect()
        } else {
            BTreeMap::new()
        };
        // Only set the dispatch config on first load of the project (mainly impacts testing)
        if DISPATCH_CONFIG.get().is_none() {
            DISPATCH_CONFIG
                .set(RwLock::new(dispatch_config_map))
                .unwrap();
        }
    }

    let dbt_project_modified = last_modified(&dbt_project_path)?;
    all_files.insert(
        ResourcePathKind::ProjectPaths,
        vec![(dbt_project_path, dbt_project_modified)],
    );

    // Collect file paths and their timestamps for fields with a suffix `_paths`
    let all_dirs = collect_paths(&dbt_project);
    let all_included_files: HashMap<ResourcePathKind, Vec<(PathBuf, SystemTime)>> =
        collect_all_files(all_dirs, package_path)?;
    all_files.extend(all_included_files);

    // make all paths relative to the project directory
    for (_, files) in all_files.iter_mut() {
        for (path, _) in files.iter_mut() {
            *path = diff_paths(&mut *path, package_path).unwrap().to_path_buf();
        }
        //
        // make deterministic: Sort files based on their relative paths
        files.sort_by(|a, b| a.0.cmp(&b.0));
    }

    let python_files = find_files_by_kind_and_extension(
        package_path,
        &dbt_project.name,
        &ResourcePathKind::ModelPaths,
        &["py"],
        &all_files,
    );

    if !python_files.is_empty() {
        return err!(
            code => ErrorCode::UnsupportedFileExtension,
            loc => python_files[0].path.clone(),
            "Python models are not currently supported"
        );
    }

    // todo: we could optimize here, but for now just take everything,...
    let mut dbt_properties = find_files_by_kind_and_extension(
        package_path,
        &dbt_project.name,
        &ResourcePathKind::ModelPaths,
        &["yml", "yaml"],
        &all_files,
    );
    // additonal paths can have ym files (add generic tests etc)
    let seed_ymls = find_files_by_kind_and_extension(
        package_path,
        &dbt_project.name,
        &ResourcePathKind::SeedPaths,
        &["yml", "yaml"],
        &all_files,
    );
    let snapshot_ymls = find_files_by_kind_and_extension(
        package_path,
        &dbt_project.name,
        &ResourcePathKind::SnapshotPaths,
        &["yml", "yaml"],
        &all_files,
    );
    let analysis_ymls = find_files_by_kind_and_extension(
        package_path,
        &dbt_project.name,
        &ResourcePathKind::AnalysisPaths,
        &["yml", "yaml"],
        &all_files,
    );

    let test_ymls = find_files_by_kind_and_extension(
        package_path,
        &dbt_project.name,
        &ResourcePathKind::TestPaths,
        &["yml", "yaml"],
        &all_files,
    );

    // todo: change dbt_properties to be BTreeSet, this may require many goldies updates
    for item in seed_ymls
        .iter()
        .chain(&snapshot_ymls)
        .chain(&analysis_ymls)
        .chain(&test_ymls)
    {
        if !dbt_properties.contains(item) {
            dbt_properties.push(item.clone());
        }
    }

    let analysis_files = find_files_by_kind_and_extension(
        package_path,
        &dbt_project.name,
        &ResourcePathKind::AnalysisPaths,
        &["sql"],
        &all_files,
    );
    let model_sql_files = find_files_by_kind_and_extension(
        package_path,
        &dbt_project.name,
        &ResourcePathKind::ModelPaths,
        &["sql"],
        &all_files,
    );
    let macro_files = find_files_by_kind_and_extension(
        package_path,
        &dbt_project.name,
        &ResourcePathKind::MacroPaths,
        &["sql"],
        &all_files,
    );
    let test_files = find_files_by_kind_and_extension(
        package_path,
        &dbt_project.name,
        &ResourcePathKind::TestPaths,
        &["sql"],
        &all_files,
    );
    let seed_files = find_files_by_kind_and_extension(
        package_path,
        &dbt_project.name,
        &ResourcePathKind::SeedPaths,
        &["csv", "parquet", "json"],
        &all_files,
    );
    let docs_files = find_files_by_kind_and_extension(
        package_path,
        &dbt_project.name,
        &ResourcePathKind::DocsPaths,
        &["md"],
        &all_files,
    );
    let snapshot_files = find_files_by_kind_and_extension(
        package_path,
        &dbt_project.name,
        &ResourcePathKind::SnapshotPaths,
        &["sql"],
        &all_files,
    );

    Ok(DbtPackage {
        dbt_project,
        dbt_properties,
        analysis_files,
        model_sql_files,
        test_files,
        seed_files,
        macro_files,
        docs_files,
        snapshot_files,
        dependencies: identify_package_dependencies(package_path, package_lookup_map)?,
        all_paths: all_files,
    })
}

/// outputs the timestamp that this run started
fn run_started_at() -> DateTime<Tz> {
    let utc_now = Utc::now();
    let tz_now: DateTime<Tz> = utc_now.with_timezone(&Tz::UTC);
    tz_now
}

fn should_exclude_path(kind: &ResourcePathKind, path: &Path) -> bool {
    match kind {
        ResourcePathKind::TestPaths => {
            // Only exclude paths directly under <test-paths>/generic/
            let components: Vec<_> = path.components().collect();
            components.len() >= 2 && components[1].as_os_str() == "generic"
        }
        _ => false,
    }
}

fn find_files_by_kind_and_extension(
    in_dir: &Path,
    project_name: &str,
    path_kind: &ResourcePathKind,
    extensions: &[&str],
    all_paths: &HashMap<ResourcePathKind, Vec<(PathBuf, SystemTime)>>,
) -> Vec<DbtAsset> {
    let default = vec![];
    let paths_to_filter: Vec<_> = all_paths
        .get(path_kind)
        .unwrap_or(&default)
        .iter()
        .collect();

    let mut paths = paths_to_filter
        .iter()
        .filter_map(|(path, _)| {
            path.extension()
                .and_then(OsStr::to_str)
                .filter(|ext| extensions.contains(&ext.to_lowercase().as_str()))
                .filter(|_| !should_exclude_path(path_kind, path))
                .map(|_| DbtAsset {
                    package_name: project_name.to_string(),
                    base_path: in_dir.to_path_buf(),
                    path: path.clone(),
                })
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    paths.sort_by(|a, b| a.path.cmp(&b.path));
    paths
}

fn collect_all_files(
    all_dirs: HashMap<ResourcePathKind, Vec<String>>,
    base_path: &Path,
) -> FsResult<HashMap<ResourcePathKind, Vec<(PathBuf, SystemTime)>>> {
    let mut all_paths: HashMap<ResourcePathKind, Vec<(PathBuf, SystemTime)>> = HashMap::new();
    for (kind, paths) in &all_dirs {
        let mut info_paths = Vec::new();
        collect_file_info(base_path, paths, &mut info_paths).lift(ectx!(
            "Failed to collect file info: {}, {}",
            base_path.display(),
            paths.join(",")
        ))?;
        all_paths.insert(kind.clone(), info_paths);
    }
    Ok(all_paths)
}

fn collect_paths(dbt_project: &DbtProject) -> HashMap<ResourcePathKind, Vec<String>> {
    let mut all_dirs: HashMap<ResourcePathKind, Vec<String>> = HashMap::new();
    all_dirs.insert(
        ResourcePathKind::ModelPaths,
        dbt_project.model_paths.clone().unwrap_or_default(),
    );
    all_dirs.insert(
        ResourcePathKind::AnalysisPaths,
        dbt_project.analysis_paths.clone().unwrap_or_default(),
    );
    all_dirs.insert(
        ResourcePathKind::AssetPaths,
        dbt_project.asset_paths.clone().unwrap_or_default(),
    );
    all_dirs.insert(
        ResourcePathKind::MacroPaths,
        dbt_project.macro_paths.clone().unwrap_or_default(),
    );
    all_dirs.insert(
        ResourcePathKind::SeedPaths,
        dbt_project.seed_paths.clone().unwrap_or_default(),
    );
    all_dirs.insert(
        ResourcePathKind::SnapshotPaths,
        dbt_project.snapshot_paths.clone().unwrap_or_default(),
    );
    all_dirs.insert(
        ResourcePathKind::TestPaths,
        dbt_project.test_paths.clone().unwrap_or_default(),
    );
    // Only register docs paths if they are explicitly specified
    if dbt_project.docs_paths.is_some() && !dbt_project.docs_paths.as_ref().unwrap().is_empty() {
        all_dirs.insert(
            ResourcePathKind::DocsPaths,
            dbt_project.docs_paths.clone().unwrap_or_default(),
        );
    } else {
        // The default is to read all files in the following directories for '*.md' files
        let mut result: Vec<String> = vec![];

        result.extend_from_slice(dbt_project.analysis_paths.as_deref().unwrap_or_default());
        result.extend_from_slice(dbt_project.macro_paths.as_deref().unwrap_or_default());
        result.extend_from_slice(dbt_project.model_paths.as_deref().unwrap_or_default());
        result.extend_from_slice(dbt_project.seed_paths.as_deref().unwrap_or_default());
        result.extend_from_slice(dbt_project.snapshot_paths.as_deref().unwrap_or_default());
        result.extend_from_slice(dbt_project.test_paths.as_deref().unwrap_or_default());

        all_dirs.insert(ResourcePathKind::DocsPaths, result);
    }
    all_dirs
}

// returns (packages_install_path, internal_packages_install_path)
fn get_packages_install_path(
    in_dir: &Path,
    arg_packages_install_path: &Option<PathBuf>,
    arg_internal_packages_install_path: &Option<PathBuf>,
    dbt_project: &DbtProjectSimplified,
) -> (PathBuf, PathBuf) {
    let packages_install_path = if let Some(path) = arg_packages_install_path {
        if path.is_absolute() {
            path.clone()
        } else {
            in_dir.join(path)
        }
    } else if let Some(path) = &dbt_project.packages_install_path {
        let mut path_buf = PathBuf::from(path);
        if !path_buf.is_absolute() {
            path_buf = in_dir.join(path_buf);
        }
        path_buf
    } else {
        in_dir.join(DBT_PACKAGES_DIR_NAME)
    };

    let internal_packages_install_path = if let Some(path) = arg_internal_packages_install_path {
        if path.is_absolute() {
            path.clone()
        } else {
            in_dir.join(path)
        }
    } else {
        packages_install_path.with_file_name(DBT_INTERNAL_PACKAGES_DIR_NAME)
    };

    (packages_install_path, internal_packages_install_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_schemas::state::ResourcePathKind;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::time::SystemTime;

    #[test]
    fn test_find_files_by_kind_and_extension_excludes_generic_test_paths() {
        // Setup test data
        let in_dir = PathBuf::from("/project");
        let project_name = "test_project";
        let extensions = &["sql", "yml"];

        // Create mock file paths with timestamps
        let now = SystemTime::now();
        let mut all_paths: HashMap<ResourcePathKind, Vec<(PathBuf, SystemTime)>> = HashMap::new();

        // Add test files - paths include test directory name as first component
        let test_files = vec![
            (PathBuf::from("tests/test_model.sql"), now),
            (PathBuf::from("tests/integration/test_integration.sql"), now),
            (PathBuf::from("tests/generic/test_generic.sql"), now), // Should be excluded
            (PathBuf::from("tests/generic/nested/test_nested.sql"), now), // Should be excluded
            (PathBuf::from("tests/custom/test_custom.sql"), now),
            (PathBuf::from("tests/schema.yml"), now),
            (PathBuf::from("tests/generic/schema.yml"), now), // Should be excluded
            (PathBuf::from("data-tests/generic/is_even.sql"), now), // Should be excluded
            (PathBuf::from("data-tests/singular/my_test.sql"), now),
        ];

        all_paths.insert(ResourcePathKind::TestPaths, test_files);

        // Call the function under test
        let result = find_files_by_kind_and_extension(
            &in_dir,
            project_name,
            &ResourcePathKind::TestPaths,
            extensions,
            &all_paths,
        );

        // Verify results - should exclude 3 generic files
        assert_eq!(result.len(), 5, "Should have 6 non-generic test files");

        // Check that all returned files are not in generic directories
        for asset in &result {
            let components: Vec<_> = asset.path.components().collect();
            assert!(
                !(components.len() >= 2 && components[1].as_os_str() == "generic"),
                "File {:?} should not have 'generic' as second component",
                asset.path
            );
        }

        // Check specific files that should be included
        let included_paths: Vec<&PathBuf> = result.iter().map(|asset| &asset.path).collect();
        assert!(included_paths.contains(&&PathBuf::from("tests/test_model.sql")));
        assert!(included_paths.contains(&&PathBuf::from("tests/integration/test_integration.sql")));
        assert!(included_paths.contains(&&PathBuf::from("tests/custom/test_custom.sql")));
        assert!(included_paths.contains(&&PathBuf::from("tests/schema.yml")));
        assert!(included_paths.contains(&&PathBuf::from("data-tests/singular/my_test.sql")));

        // Check that generic files are excluded
        assert!(!included_paths.contains(&&PathBuf::from("tests/generic/test_generic.sql")));
        assert!(!included_paths.contains(&&PathBuf::from("tests/generic/nested/test_nested.sql")));
        assert!(!included_paths.contains(&&PathBuf::from("tests/generic/schema.yml")));
        assert!(!included_paths.contains(&&PathBuf::from("data-tests/generic/is_even.sql")));

        // Verify asset properties
        for asset in &result {
            assert_eq!(asset.package_name, project_name);
            assert_eq!(asset.base_path, in_dir);
        }
    }

    #[test]
    fn test_should_exclude_path_function() {
        // Test the should_exclude_path function directly

        // Test paths should exclude generic directories (second component)
        assert!(should_exclude_path(
            &ResourcePathKind::TestPaths,
            &PathBuf::from("tests/generic/test.sql")
        ));

        assert!(should_exclude_path(
            &ResourcePathKind::TestPaths,
            &PathBuf::from("data-tests/generic/test.sql")
        ));

        assert!(should_exclude_path(
            &ResourcePathKind::TestPaths,
            &PathBuf::from("tests/generic/nested/test.sql")
        ));

        // Test paths should NOT exclude non-generic directories
        assert!(!should_exclude_path(
            &ResourcePathKind::TestPaths,
            &PathBuf::from("tests/integration/test.sql")
        ));

        assert!(!should_exclude_path(
            &ResourcePathKind::TestPaths,
            &PathBuf::from("tests/unit/test.sql")
        ));

        assert!(!should_exclude_path(
            &ResourcePathKind::TestPaths,
            &PathBuf::from("data-tests/singular/test.sql")
        ));

        assert!(!should_exclude_path(
            &ResourcePathKind::TestPaths,
            &PathBuf::from("tests/unit/generic/test.sql") // generic is not second component
        ));

        // Edge cases
        assert!(!should_exclude_path(
            &ResourcePathKind::TestPaths,
            &PathBuf::from("generic/test.sql") // only one component
        ));

        assert!(!should_exclude_path(
            &ResourcePathKind::TestPaths,
            &PathBuf::from("tests") // only one component
        ));

        // Non-test paths should never exclude generic directories
        assert!(!should_exclude_path(
            &ResourcePathKind::ModelPaths,
            &PathBuf::from("models/generic/model.sql")
        ));

        assert!(!should_exclude_path(
            &ResourcePathKind::MacroPaths,
            &PathBuf::from("macros/generic/macro.sql")
        ));

        assert!(!should_exclude_path(
            &ResourcePathKind::SeedPaths,
            &PathBuf::from("seeds/generic/seed.csv")
        ));
    }

    #[test]
    fn test_find_files_by_kind_and_extension_empty_paths() {
        // Test with empty paths
        let in_dir = PathBuf::from("/project");
        let project_name = "test_project";
        let extensions = &["sql"];
        let all_paths: HashMap<ResourcePathKind, Vec<(PathBuf, SystemTime)>> = HashMap::new();

        let result = find_files_by_kind_and_extension(
            &in_dir,
            project_name,
            &ResourcePathKind::TestPaths,
            extensions,
            &all_paths,
        );

        assert!(
            result.is_empty(),
            "Should return empty vector for empty paths"
        );
    }

    #[test]
    fn test_find_files_by_kind_and_extension_extension_filtering() {
        // Test that only files with specified extensions are included
        let in_dir = PathBuf::from("/project");
        let project_name = "test_project";
        let extensions = &["sql"]; // Only SQL files

        let now = SystemTime::now();
        let mut all_paths: HashMap<ResourcePathKind, Vec<(PathBuf, SystemTime)>> = HashMap::new();

        let test_files = vec![
            (PathBuf::from("tests/test.sql"), now), // Should be included
            (PathBuf::from("tests/test.yml"), now), // Should be excluded (wrong extension)
            (PathBuf::from("tests/test.py"), now),  // Should be excluded (wrong extension)
            (PathBuf::from("tests/test"), now),     // Should be excluded (no extension)
        ];

        all_paths.insert(ResourcePathKind::TestPaths, test_files);

        let result = find_files_by_kind_and_extension(
            &in_dir,
            project_name,
            &ResourcePathKind::TestPaths,
            extensions,
            &all_paths,
        );

        assert_eq!(result.len(), 1, "Should only include SQL files");
        assert_eq!(result[0].path, PathBuf::from("tests/test.sql"));
    }

    #[test]
    fn test_find_files_by_kind_and_extension_non_test_paths_not_excluded() {
        // Setup test data for non-test paths (should not exclude generic directories)
        let in_dir = PathBuf::from("/project");
        let project_name = "test_project";
        let extensions = &["sql"];

        let now = SystemTime::now();
        let mut all_paths: HashMap<ResourcePathKind, Vec<(PathBuf, SystemTime)>> = HashMap::new();

        // Add model files with generic in path (should NOT be excluded for models)
        let model_files = vec![
            (PathBuf::from("models/generic/my_model.sql"), now),
            (PathBuf::from("models/other/model.sql"), now),
        ];

        all_paths.insert(ResourcePathKind::ModelPaths, model_files);

        // Call the function under test for ModelPaths
        let result = find_files_by_kind_and_extension(
            &in_dir,
            project_name,
            &ResourcePathKind::ModelPaths,
            extensions,
            &all_paths,
        );

        // Verify that generic directories are NOT excluded for non-test paths
        assert_eq!(
            result.len(),
            2,
            "Should have 2 model files including generic path"
        );

        let included_paths: Vec<&PathBuf> = result.iter().map(|asset| &asset.path).collect();
        assert!(included_paths.contains(&&PathBuf::from("models/generic/my_model.sql")));
        assert!(included_paths.contains(&&PathBuf::from("models/other/model.sql")));
    }
}
