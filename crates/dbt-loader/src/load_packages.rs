use dbt_common::adapter::AdapterType;
use dbt_common::cancellation::CancellationToken;
use dbt_common::path::DbtPath;
use dbt_common::tracing::emit::emit_warn_log_message;
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use indexmap::IndexMap;
use sha2::{Digest, Sha256};
use walkdir::WalkDir;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io::Read;
use std::path::{Path, PathBuf};

use dbt_common::constants::DBT_PROJECT_YML;

use dbt_common::stdfs;

use dbt_common::err;
use dbt_common::{ErrorCode, FsResult, fs_err, unexpected_fs_err};
use dbt_schemas::schemas::project::DbtProject;
use dbt_schemas::state::{DbtAsset, DbtPackage, DbtProfile, DbtVars, ResourcePathKind};

use crate::args::LoadArgs;
use crate::loader::load_inner;

mod assets {
    #![allow(clippy::disallowed_methods)] // RustEmbed generates calls to std::path::Path::canonicalize

    use rust_embed::RustEmbed;

    #[derive(RustEmbed)]
    #[folder = "src/dbt_macro_assets/"]
    pub struct MacroAssets;
}

pub async fn load_packages(
    arg: &LoadArgs,
    env: &JinjaEnv,
    dbt_profile: &DbtProfile,
    collected_vars: &mut Vec<(String, IndexMap<String, DbtVars>)>,
    lookup_map: &BTreeMap<String, String>,
    packages_install_path: &Path,
    token: &CancellationToken,
) -> FsResult<Vec<DbtPackage>> {
    // Collect dependency package paths with a flag set to `true`
    // indicating that they are indeed dependencies. This is necessary
    // to differentiate between root project and dependencies later on.
    let mut dirs = if packages_install_path.exists() {
        stdfs::read_dir(packages_install_path)?
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_type()
                    .map(|ft| ft.is_dir() || ft.is_symlink())
                    .unwrap_or(false)
            })
            .map(|e| (e.path(), true))
            .collect()
    } else {
        vec![]
    };
    // Sort packages to make the output deterministic
    dirs.sort();
    // Add root package to the front of the list
    // `false` indicates that this is a root project
    dirs.insert(0, (arg.io.in_dir.clone(), false));

    collect_packages(
        arg,
        env,
        dbt_profile,
        collected_vars,
        dirs,
        lookup_map,
        token,
    )
    .await
}

pub async fn load_internal_packages(
    arg: &LoadArgs,
    env: &JinjaEnv,
    dbt_profile: &DbtProfile,
    collected_vars: &mut Vec<(String, IndexMap<String, DbtVars>)>,
    internal_packages_install_path: &Path,
    token: &CancellationToken,
) -> FsResult<Vec<DbtPackage>> {
    let mut dbt_internal_packages_dirs: Vec<(PathBuf, bool)> =
        stdfs::read_dir(internal_packages_install_path)?
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|ft| ft.is_dir()).unwrap_or(false)) // `true` indicates that this package path is a "dependency", not a root project
            .map(|e| (e.path(), true))
            .collect();
    dbt_internal_packages_dirs.sort();
    collect_packages(
        arg,
        env,
        dbt_profile,
        collected_vars,
        dbt_internal_packages_dirs,
        &BTreeMap::new(),
        token,
    )
    .await
}

/// Sync internal packages to disk using hash comparison.
/// Only writes files that have changed and removes stale files.
///
/// This function ensures that the internal dbt packages required for
/// the specified adapter type are present in the given installation path.
///
/// It will also ensure that all the files are exactly as stored in the embedded assets,
/// writing only those that differ based on SHA-256 hash comparison.
pub fn persist_internal_packages(
    internal_packages_install_path: &Path,
    adapter_type: AdapterType,
    #[allow(unused)] enable_persist_compare_package: bool,
) -> FsResult<()> {
    // Copy the dbt-adapters and dbt-{adapter_type} to the packages_install_path
    let internal_packages = internal_package_names(adapter_type);
    // Track expected file paths for cleanup
    let mut expected_files: HashSet<PathBuf> = HashSet::new();

    for package in internal_packages {
        let mut found = false;
        for asset in assets::MacroAssets::iter() {
            let asset_path = asset.as_ref();
            if !asset_path.starts_with(&package) {
                continue;
            }
            found = true;

            let install_path = internal_packages_install_path.join(asset_path);
            expected_files.insert(install_path.clone());

            let asset_contents = assets::MacroAssets::get(asset_path).expect("Asset must exist");
            let embedded_data = asset_contents.data.as_ref();

            // Check if file needs to be written
            let needs_write = if install_path.exists() {
                // Compare hashes
                let mut existing_data = Vec::new();
                std::fs::File::open(&install_path)
                    .and_then(|mut f| f.read_to_end(&mut existing_data))
                    .map(|_| {
                        let embedded_hash = Sha256::digest(embedded_data);
                        let existing_hash = Sha256::digest(&existing_data);
                        embedded_hash != existing_hash
                    })
                    .unwrap_or(true) // If read fails, write it
            } else {
                true // File doesn't exist, needs write
            };

            if needs_write {
                if let Some(parent) = install_path.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        fs_err!(
                            ErrorCode::IoError,
                            "Failed to create directory for dbt adapter package {}: {}",
                            parent.display(),
                            e
                        )
                    })?;
                }
                std::fs::write(&install_path, embedded_data).map_err(|e| {
                    fs_err!(
                        ErrorCode::IoError,
                        "Failed to write file for dbt adapter package {}: {}",
                        install_path.display(),
                        e
                    )
                })?;
            }
        }

        if !found {
            return err!(
                ErrorCode::InvalidConfig,
                "Missing default macro package '{}' for adapter type '{}'",
                package,
                adapter_type
            );
        }
    }

    // Remove extra files not in expected set
    if internal_packages_install_path.exists() {
        for entry in WalkDir::new(internal_packages_install_path)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_file() {
                let path = entry.path().to_path_buf();
                if !expected_files.contains(&path) {
                    std::fs::remove_file(&path).map_err(|e| {
                        fs_err!(
                            ErrorCode::IoError,
                            "Failed to remove stale adapter package file {}: {}",
                            path.display(),
                            e
                        )
                    })?;
                }
            }
        }
    }

    Ok(())
}

async fn collect_packages(
    arg: &LoadArgs,
    env: &JinjaEnv,
    dbt_profile: &DbtProfile,
    collected_vars: &mut Vec<(String, IndexMap<String, DbtVars>)>,
    package_paths: Vec<(PathBuf, bool)>,
    lookup_map: &BTreeMap<String, String>,
    token: &CancellationToken,
) -> FsResult<Vec<DbtPackage>> {
    let mut packages = vec![];
    // `is_dependency` Indicates if we are loading a dependency or a root project
    for (package_path, is_dependency) in package_paths {
        token.check_cancellation()?;
        if package_path.is_dir() {
            if package_path.join(DBT_PROJECT_YML).exists() {
                let package = load_inner(
                    arg,
                    env,
                    &package_path,
                    dbt_profile,
                    is_dependency,
                    lookup_map,
                    false,
                    collected_vars,
                )
                .await?;
                packages.push(package);
            } else {
                emit_warn_log_message(
                    ErrorCode::InvalidConfig,
                    format!(
                        "Package {} does not contain a dbt_project.yml file",
                        package_path.file_name().unwrap().to_str().unwrap()
                    ),
                    arg.io.status_reporter.as_ref(),
                );
            }
        }
    }
    Ok(packages)
}

/// Returns the list of internal package directory names for a given adapter type.
pub fn internal_package_names(adapter_type: AdapterType) -> Vec<String> {
    let adapter_package = format!("dbt-{adapter_type}");
    let mut packages = vec!["dbt-adapters".to_string(), adapter_package];
    match adapter_type {
        AdapterType::Redshift => packages.push("dbt-postgres".to_string()),
        AdapterType::Databricks => packages.push("dbt-spark".to_string()),
        _ => {}
    }
    packages
}

/// Check if a file path is under macros/ or tests/ directories
fn is_under_macros_or_tests(path: &Path) -> bool {
    if let Some(first_component) = path.components().next() {
        let dir_name = first_component.as_os_str().to_str().unwrap_or("");
        return dir_name == "macros" || dir_name == "tests";
    }
    false
}

/// Check if a file is a metadata/config file that should be ignored
fn is_metadata_file(path: &Path) -> bool {
    matches!(
        path.to_str().unwrap_or(""),
        "dbt_project.yml" | "packages.yml" | "profile_template.yml" | "__init__.py"
    )
}

/// Build `DbtPackage`s directly from `MacroAssets` (RustEmbed) without any disk I/O.
/// This function assumes only macros (and generic tests) are included in the internal packages.
/// A debug assertion will warn if files exist outside macros/ and tests/ directories.
pub fn construct_internal_packages(
    adapter_type: AdapterType,
    synthetic_root: &Path,
) -> FsResult<Vec<DbtPackage>> {
    let package_names = internal_package_names(adapter_type);
    let mut packages = vec![];

    for package_dir_name in &package_names {
        let prefix = format!("{package_dir_name}/");
        let package_root = synthetic_root
            .join("dbt_internal_packages")
            .join(package_dir_name);

        // 1. Read & parse dbt_project.yml from embedded assets
        let yml_path = format!("{package_dir_name}/dbt_project.yml");
        let yml_content = assets::MacroAssets::get(&yml_path)
            .map(|f| String::from_utf8_lossy(&f.data).into_owned())
            .ok_or_else(|| {
                fs_err!(
                    ErrorCode::InvalidConfig,
                    "Missing dbt_project.yml in embedded package '{}'",
                    package_dir_name
                )
            })?;
        let parsed: DbtProject = dbt_yaml::from_str(&yml_content).map_err(|e| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "Failed to parse internal dbt_project.yml: {}",
                e
            )
        })?;
        let dbt_project = build_internal_dbt_project(parsed)?;
        let project_name = dbt_project.name.clone();

        // 2. Enumerate ALL files for this package, cache contents
        let mut macro_files = vec![];
        let mut embedded_file_contents = HashMap::new();

        for asset_path in assets::MacroAssets::iter() {
            let asset_str = asset_path.as_ref();
            if !asset_str.starts_with(&prefix) {
                continue;
            }
            let relative = &asset_str[prefix.len()..];
            let rel_path = PathBuf::from(relative);

            // Ignore anything inside the target/ directory
            if let Some(first_component) = rel_path.components().next() {
                let dir_name = first_component.as_os_str().to_str().unwrap_or("");
                if dir_name == "target" {
                    continue;
                }
            }

            // Cache all file contents
            if let Some(file) = assets::MacroAssets::get(asset_str) {
                let content = String::from_utf8_lossy(&file.data).into_owned();
                embedded_file_contents.insert(DbtPath::from_path(&rel_path), content);
            }

            // Only add .sql files to macro_files
            if relative.ends_with(".sql") {
                macro_files.push(DbtAsset {
                    package_name: project_name.clone(),
                    base_path: package_root.clone(),
                    path: rel_path.clone(),
                    original_path: rel_path,
                });
            }
        }
        macro_files.sort_by(|a, b| a.path.cmp(&b.path));

        // Debug-only check: assert all files are under macros/ or tests/ directories
        debug_assert!(
            embedded_file_contents
                .keys()
                .all(|path| is_metadata_file(path.as_path())
                    || is_under_macros_or_tests(path.as_path())),
            "Internal package '{}' contains files outside macros/ and tests/ directories. \
             Files found: {:?}. The construct_internal_packages function may need to be extended \
             to handle these resource types.",
            package_dir_name,
            embedded_file_contents
                .keys()
                .filter(|path| !is_metadata_file(path.as_path())
                    && !is_under_macros_or_tests(path.as_path()))
                .collect::<Vec<_>>()
        );

        packages.push(DbtPackage {
            dbt_project,
            package_root_path: package_root,
            macro_files,
            embedded_file_contents: Some(embedded_file_contents),
            dependencies: BTreeSet::new(),
            dbt_properties: vec![],
            analysis_files: vec![],
            model_sql_files: vec![],
            function_sql_files: vec![],
            test_files: vec![],
            fixture_files: vec![],
            seed_files: vec![],
            docs_files: vec![],
            snapshot_files: vec![],
            inline_file: None,
            all_paths: HashMap::from([
                (ResourcePathKind::ModelPaths, vec![]),
                (ResourcePathKind::AnalysisPaths, vec![]),
                (ResourcePathKind::AssetPaths, vec![]),
                (ResourcePathKind::DocsPaths, vec![]),
                (ResourcePathKind::MacroPaths, vec![]),
                (ResourcePathKind::SeedPaths, vec![]),
                (ResourcePathKind::SnapshotPaths, vec![]),
                (ResourcePathKind::TestPaths, vec![]),
                (ResourcePathKind::FixturePaths, vec![]),
                (ResourcePathKind::FunctionPaths, vec![]),
            ]),
        });
    }
    Ok(packages)
}

/// Fill default paths and clean targets for a parsed DbtProject.
pub(crate) fn build_internal_dbt_project(mut dbt_project: DbtProject) -> FsResult<DbtProject> {
    fill_default(&mut dbt_project.analysis_paths, &["analysis", "analyses"]);
    fill_default(&mut dbt_project.asset_paths, &["assets"]);
    fill_default(&mut dbt_project.function_paths, &["functions"]);
    fill_default(&mut dbt_project.macro_paths, &["macros"]);
    fill_default(&mut dbt_project.model_paths, &["models"]);
    fill_default(&mut dbt_project.seed_paths, &["seeds"]);
    fill_default(&mut dbt_project.snapshot_paths, &["snapshots"]);
    fill_default(&mut dbt_project.test_paths, &["tests"]);

    // Add generic test paths to macro_paths
    for test_path in dbt_project.test_paths.as_deref().unwrap_or_default() {
        let path = PathBuf::from(test_path);
        dbt_project
            .macro_paths
            .as_mut()
            .ok_or_else(|| unexpected_fs_err!("Macro paths should exist"))?
            .push(path.join("generic").to_string_lossy().to_string());
    }

    if dbt_project.clean_targets.is_none() {
        dbt_project.clean_targets = Some(vec![]);
    }
    Ok(dbt_project)
}

fn fill_default(paths: &mut Option<Vec<String>>, defaults: &[&str]) {
    if paths.as_ref().is_none_or(|v| v.is_empty()) {
        *paths = Some(defaults.iter().map(|value| (*value).to_string()).collect());
    }
}
