//! Package addition functionality for dbt-deps
//!
//! This module provides functionality for adding packages to `packages.yml` files.
//! It includes utilities for:
//! - Converting package strings to structured data
//! - Checking for duplicate packages
//! - Creating properly formatted package entries
//! - Adding packages to existing packages.yml files
//!

use dbt_common::{ErrorCode, FsResult, err, stdfs};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Package information structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Package {
    pub name: String,
    pub version: Option<String>,
}

/// Convert a string value to a Package struct
/// Returns Package with name and optional version
pub fn convert(value: &str) -> Package {
    if let Some((package_name, package_version)) = value.split_once('@') {
        Package {
            name: package_name.to_string(),
            version: Some(package_version.to_string()),
        }
    } else {
        Package {
            name: value.to_string(),
            version: None,
        }
    }
}

/// Packages YAML structure
#[derive(Debug, Serialize, Deserialize)]
pub struct PackagesYaml {
    pub packages: Vec<HashMap<String, dbt_serde_yaml::Value>>,
}

/// Arguments for adding a package
#[derive(Debug)]
pub struct AddPackageArgs {
    pub add_package: Package,
    pub source: String,
}

/// Filter out duplicate packages in packages.yml so we don't have two entries for the same package
/// Loop through contents of `packages.yml` to ensure no duplicate package names + versions.
/// This will take into consideration exact match of a package name, as well as
/// a check to see if a package name exists within a name (i.e. a package name inside a git URL).
pub fn filter_out_duplicate_packages(
    mut packages_yml: PackagesYaml,
    args: &AddPackageArgs,
) -> PackagesYaml {
    packages_yml.packages.retain(|pkg_entry| {
        for val in pkg_entry.values() {
            if let Some(val_str) = val.as_str() {
                if val_str.contains(&args.add_package.name) {
                    return false; // Remove this package
                }
            }
        }
        true // Keep this package
    });

    packages_yml
}

/// Create a formatted entry to add to `packages.yml` or `package-lock.yml` file
fn create_packages_yml_entry(
    package: &str,
    version: Option<&str>,
    source: &str,
) -> HashMap<String, dbt_serde_yaml::Value> {
    let mut packages_yml_entry = HashMap::new();

    let package_key = if source == "hub" { "package" } else { source };
    let version_key = if source == "git" {
        "revision"
    } else {
        "version"
    };

    // Use serde to serialize the package name
    packages_yml_entry.insert(
        package_key.to_string(),
        dbt_serde_yaml::to_value(package).unwrap(),
    );

    // For local packages, version is not applicable
    if source != "local" && source != "tarball" {
        if let Some(ver) = version {
            if ver.contains(',') {
                let versions: Vec<String> = ver.split(',').map(|v| v.trim().to_string()).collect();
                packages_yml_entry.insert(
                    version_key.to_string(),
                    dbt_serde_yaml::to_value(versions).unwrap(),
                );
            } else {
                packages_yml_entry.insert(
                    version_key.to_string(),
                    dbt_serde_yaml::to_value(ver).unwrap(),
                );
            }
        }
    }

    packages_yml_entry
}

/// Add a package to packages.yml
pub fn add_package_to_yml(
    args: &AddPackageArgs,
    project_root: &str,
    packages_path: &str,
) -> FsResult<()> {
    let packages_yml_filepath = format!("{project_root}/{packages_path}");
    let packages_path = Path::new(&packages_yml_filepath);

    // Create packages.yml if it doesn't exist
    if !packages_path.exists() {
        let initial_content = PackagesYaml {
            packages: Vec::new(),
        };
        let yaml_content = match dbt_serde_yaml::to_string(&initial_content) {
            Ok(yaml) => yaml,
            Err(e) => {
                return err!(
                    ErrorCode::IoError,
                    "Failed to serialize packages.yml: {}",
                    e
                );
            }
        };
        stdfs::write(packages_path, yaml_content)?;
    }

    // Read existing packages.yml
    let yaml_content = stdfs::read_to_string(packages_path)?;
    let mut packages_yml: PackagesYaml = match dbt_serde_yaml::from_str(&yaml_content) {
        Ok(yml) => yml,
        Err(e) => return err!(ErrorCode::IoError, "Failed to parse packages.yml: {}", e),
    };

    // Check for duplicates
    packages_yml = filter_out_duplicate_packages(packages_yml, args);

    // Create new package entry
    let new_package_entry = create_packages_yml_entry(
        &args.add_package.name,
        args.add_package.version.as_deref(),
        &args.source,
    );

    // Add the new package
    packages_yml.packages.push(new_package_entry);

    // Write back to file
    let yaml_content = match dbt_serde_yaml::to_string(&packages_yml) {
        Ok(yaml) => yaml,
        Err(e) => {
            return err!(
                ErrorCode::IoError,
                "Failed to serialize packages.yml: {}",
                e
            );
        }
    };
    stdfs::write(packages_path, yaml_content)?;

    Ok(())
}

/// Main function to add a package to packages.yml
/// This function is called from mod.rs and handles the conversion from string to Package struct
pub fn add_package(package_str: &str, project_dir: &Path) -> FsResult<()> {
    // Convert the package string to a Package struct
    let package = convert(package_str);

    // Determine the source based on the package string
    let source = if package_str.contains('@') && !package_str.contains("://") {
        "hub"
    } else if package_str.contains("://")
        && (package_str.starts_with("git@")
            || package_str.contains("github.com")
            || package_str.contains("gitlab.com"))
    {
        "git"
    } else if package_str.contains("://")
        && (package_str.ends_with(".tar.gz") || package_str.ends_with(".tgz"))
    {
        "tarball"
    } else {
        // For local packages, validate that the path exists or is a valid relative path
        let local_path = Path::new(&package.name);
        if local_path.is_absolute() {
            if !local_path.exists() {
                return err!(
                    ErrorCode::InvalidConfig,
                    "Local package path does not exist: {}",
                    package.name
                );
            }
        } else {
            // For relative paths, check if they exist relative to the project directory
            let full_path = project_dir.join(local_path);
            if !full_path.exists() {
                return err!(
                    ErrorCode::InvalidConfig,
                    "Local package path does not exist: {} (resolved to: {})",
                    package.name,
                    full_path.display()
                );
            }
        }
        "local"
    };

    // Create the arguments for adding the package
    let args = AddPackageArgs {
        add_package: package,
        source: source.to_string(),
    };

    // Convert project_dir to string for the function call
    let project_root = match project_dir.to_str() {
        Some(root) => root,
        None => {
            return err!(ErrorCode::InvalidConfig, "Invalid project directory path");
        }
    };

    // Add the package to packages.yml
    match add_package_to_yml(&args, project_root, "packages.yml") {
        Ok(()) => Ok(()),
        Err(e) => err!(
            ErrorCode::IoError,
            "Failed to add package to packages.yml: {}",
            e
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_with_version() {
        let result = convert("test-package@1.0.0");
        assert_eq!(result.name, "test-package");
        assert_eq!(result.version, Some("1.0.0".to_string()));
    }

    #[test]
    fn test_convert_without_version() {
        let result = convert("test-package");
        assert_eq!(result.name, "test-package");
        assert_eq!(result.version, None);
    }

    #[test]
    fn test_create_packages_yml_entry_hub() {
        let entry = create_packages_yml_entry("test-package", Some("1.0.0"), "hub");
        assert_eq!(
            entry.get("package").unwrap().as_str().unwrap(),
            "test-package"
        );
        assert_eq!(entry.get("version").unwrap().as_str().unwrap(), "1.0.0");
    }

    #[test]
    fn test_create_packages_yml_entry_git() {
        let entry = create_packages_yml_entry("test-package", Some("main"), "git");
        assert_eq!(entry.get("git").unwrap().as_str().unwrap(), "test-package");
        assert_eq!(entry.get("revision").unwrap().as_str().unwrap(), "main");
    }

    #[test]
    fn test_create_packages_yml_entry_with_multiple_versions() {
        let entry = create_packages_yml_entry("test-package", Some("1.0.0,1.1.0"), "hub");
        assert_eq!(
            entry.get("package").unwrap().as_str().unwrap(),
            "test-package"
        );

        if let Some(versions) = entry.get("version").unwrap().as_sequence() {
            assert_eq!(versions.len(), 2);
            assert_eq!(versions[0].as_str().unwrap(), "1.0.0");
            assert_eq!(versions[1].as_str().unwrap(), "1.1.0");
        } else {
            panic!("Expected sequence for multiple versions");
        }
    }

    #[test]
    fn test_create_packages_yml_entry_local() {
        let entry = create_packages_yml_entry("packages/my-local-package", None, "local");
        assert_eq!(
            entry.get("local").unwrap().as_str().unwrap(),
            "packages/my-local-package"
        );
        // Local packages should not have a version field
        assert!(!entry.contains_key("version"));
    }

    #[test]
    fn test_convert_local_package() {
        let result = convert("packages/my-local-package");
        assert_eq!(result.name, "packages/my-local-package");
        assert_eq!(result.version, None);
    }

    #[test]
    fn test_add_local_package_integration() {
        use std::fs;
        use tempfile::TempDir;

        // Create a temporary directory structure
        let temp_dir = TempDir::new().unwrap();
        let project_dir = temp_dir.path();

        // Create a local package directory
        let local_package_dir = project_dir.join("packages").join("my-local-package");
        fs::create_dir_all(&local_package_dir).unwrap();

        // Create a dbt_project.yml in the local package to make it valid
        let dbt_project_content = r#"
name: "my-local-package"
version: "1.0.0"
config-version: 2
"#;
        stdfs::write(
            local_package_dir.join("dbt_project.yml"),
            dbt_project_content,
        )
        .unwrap();

        // Test adding the local package
        let result = add_package("packages/my-local-package", project_dir);
        assert!(result.is_ok(), "Failed to add local package: {result:?}");

        // Verify the packages.yml was created with correct content
        let packages_yml_path = project_dir.join("packages.yml");
        assert!(packages_yml_path.exists(), "packages.yml should be created");

        let content = fs::read_to_string(&packages_yml_path).unwrap();
        assert!(
            content.contains("local: packages/my-local-package"),
            "packages.yml should contain local package entry"
        );
        assert!(
            !content.contains("version:"),
            "local packages should not have version field"
        );
    }
}
