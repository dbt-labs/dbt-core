use crate::semver::{Version, VersionSpecifier, versions_compatible};
use dbt_common::tracing::emit::emit_warn_log_message;
use dbt_common::{ErrorCode, FsResult, err, fs_err, io_args::IoArgs};
use dbt_schemas::schemas::packages::DbtPackageEntry;
use dbt_schemas::schemas::serde::StringOrArrayOfStrings;
use reqwest::{Client, StatusCode};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{
    RetryTransientMiddleware, policies::ExponentialBackoff as RetryExponentialBackoff,
};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

pub const DBT_HUB_URL: &str = "https://hub.getdbt.com";
pub const DBT_CORE_FIXED_VERSION: &str = "1.8.7";
const MAX_CLIENT_RETRIES: u32 = 3;

#[derive(Deserialize, Clone, Debug)]
pub struct HubPackageDownloads {
    pub tarball: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct HubPackageVersion {
    pub name: String,
    pub packages: Vec<DbtPackageEntry>,
    pub downloads: HubPackageDownloads,
    #[serde(rename = "fusion-schema-compat")]
    pub fusion_schema_compat: Option<bool>,
    #[serde(default)]
    pub require_dbt_version: Option<StringOrArrayOfStrings>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct HubPackageJson {
    pub name: String,
    pub versions: HashMap<String, HubPackageVersion>,
    #[serde(default)]
    pub deprecated: bool,
    #[serde(default)]
    pub redirectnamespace: Option<String>,
    #[serde(default)]
    pub redirectname: Option<String>,
    #[serde(rename = "latest-fusion-schema-compat")]
    pub latest_fusion_schema_compat: Option<bool>,
}

pub struct HubClient {
    pub client: ClientWithMiddleware,
    pub base_url: String,
    pub index: Option<HashSet<String>>,
    pub hub_packages: HashMap<String, HubPackageJson>,
}

impl HubClient {
    pub fn new(base_url: &str) -> Self {
        let retry_policy =
            RetryExponentialBackoff::builder().build_with_max_retries(MAX_CLIENT_RETRIES);
        let client = ClientBuilder::new(Client::new())
            // Retry failed requests.
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();
        Self {
            client,
            base_url: base_url.to_string(),
            index: None,
            hub_packages: HashMap::new(),
        }
    }

    pub async fn hydrate_index(&mut self) -> FsResult<()> {
        if self.index.is_some() {
            return Ok(());
        }
        let url = format!("{}/api/v1/index.json", self.base_url);
        let res = self.client.get(&url).send().await.map_err(|e| {
            fs_err!(
                ErrorCode::RuntimeError,
                "Failed to get index from {url}; status: {}",
                e
            )
        })?;
        if res.status().is_success() {
            let index: Vec<String> = res.json().await.map_err(|e| {
                fs_err!(
                    ErrorCode::RuntimeError,
                    "Failed to parse index from {url}; status: {}",
                    e
                )
            })?;
            self.index = Some(index.into_iter().collect());
            Ok(())
        } else {
            err!(
                ErrorCode::RuntimeError,
                "Failed to get index from {url}; status: {}",
                res.status()
            )
        }
    }

    pub async fn get_hub_package(&mut self, package: &str) -> FsResult<HubPackageJson> {
        if let Some(hub_package) = self.hub_packages.get(package) {
            return Ok(hub_package.clone());
        }
        let url = format!("{}/api/v1/{}.json", self.base_url, package);
        let res = self.client.get(&url).send().await.map_err(|e| {
            fs_err!(
                ErrorCode::RuntimeError,
                "Failed to get package from {url}; status: {}",
                e.status().unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
            )
        })?;
        if res.status().is_success() {
            let hub_package: HubPackageJson = res.json().await.map_err(|e| {
                fs_err!(
                    ErrorCode::RuntimeError,
                    "Failed to parse package from {url}; status: {}",
                    e.status().unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
                )
            })?;
            self.hub_packages
                .insert(package.to_string(), hub_package.clone());
            Ok(hub_package)
        } else {
            err!(
                ErrorCode::RuntimeError,
                "Failed to get package from {url}; status: {}",
                res.status()
            )
        }
    }

    pub async fn check_index(&mut self, package: &str) -> FsResult<bool> {
        if self.index.is_none() {
            self.hydrate_index().await?;
        }
        if let Some(index) = &self.index {
            Ok(index.contains(package))
        } else {
            Ok(false)
        }
    }

    pub async fn get_compatible_versions(
        &mut self,
        hub_package: &HubPackageJson,
        _dbt_version: &str,
        _should_version_check: bool,
    ) -> FsResult<Vec<String>> {
        // TODO: Implement version filtering. This should be done
        // once most of the regularly used hub packages have a
        // fusion compatible version in require_dbt_version.
        Ok(hub_package.versions.keys().cloned().collect())
    }

    /// Checks if the current dbt version satisfies the package's require_dbt_version constraint
    /// and issues a warning if it doesn't.
    ///
    /// Uses CARGO_PKG_VERSION as the current dbt version.
    ///
    /// # Arguments
    /// * `io` - IO arguments for emitting warnings
    /// * `package_name` - The name of the package being checked
    /// * `package_version` - The specific version metadata from the hub
    pub fn check_require_dbt_version(
        &self,
        io: &IoArgs,
        package_name: &str,
        package_version: &HubPackageVersion,
    ) {
        let current_version = env!("CARGO_PKG_VERSION");
        // Check if package has version requirements
        if let Some(ref required_versions) = package_version.require_dbt_version {
            // Convert StringOrArrayOfStrings to Vec<String>
            let version_strings: Vec<String> = match required_versions {
                StringOrArrayOfStrings::String(s) => vec![s.clone()],
                StringOrArrayOfStrings::ArrayOfStrings(arr) => arr.clone(),
            };

            // Parse required versions
            let mut all_versions = Vec::new();
            for version_str in &version_strings {
                match VersionSpecifier::from_str(version_str) {
                    Ok(spec) => all_versions.push(Version::Spec(spec)),
                    Err(_) => {
                        // If we can't parse a version requirement, skip validation
                        return;
                    }
                }
            }

            // Add current version as exact match
            match VersionSpecifier::from_str(&format!("={}", current_version)) {
                Ok(current_spec) => all_versions.push(Version::Spec(current_spec)),
                Err(_) => {
                    // If we can't parse current version, skip validation
                    return;
                }
            }

            // Check if versions are compatible
            if !versions_compatible(&all_versions) {
                let version_display = if version_strings.len() == 1 {
                    version_strings[0].clone()
                } else {
                    format!("[{}]", version_strings.join(", "))
                };

                emit_warn_log_message(
                    ErrorCode::DependencyWarning,
                    format!(
                        "Package '{}' requires dbt version {}, but current version is {}. \
                         This package may not be compatible with your dbt version.",
                        package_name, version_display, current_version
                    ),
                    io.status_reporter.as_ref(),
                );
            }
        }
    }

    /// Checks if a package is deprecated or redirected and shows appropriate warnings
    pub fn check_package_deprecation(&self, io: &IoArgs, hub_package: &HubPackageJson) {
        // Check for package redirect
        if let Some(redirect_namespace) = &hub_package.redirectnamespace {
            if let Some(redirect_name) = &hub_package.redirectname {
                emit_warn_log_message(
                    ErrorCode::DependencyWarning,
                    format!(
                        "Package '{}' has been moved to '{}/{}'. Please update your package reference.",
                        hub_package.name, redirect_namespace, redirect_name
                    ),
                    io.status_reporter.as_ref(),
                );
            } else {
                emit_warn_log_message(
                    ErrorCode::DependencyWarning,
                    format!(
                        "Package '{}' has been moved to namespace '{}'. Please update your package reference.",
                        hub_package.name, redirect_namespace
                    ),
                    io.status_reporter.as_ref(),
                );
            }
        } else if let Some(redirect_name) = &hub_package.redirectname {
            emit_warn_log_message(
                ErrorCode::DependencyWarning,
                format!(
                    "Package '{}' has been renamed to '{}'. Please update your package reference.",
                    hub_package.name, redirect_name
                ),
                io.status_reporter.as_ref(),
            );
        }

        // Check for deprecation
        if hub_package.deprecated {
            emit_warn_log_message(
                ErrorCode::DependencyWarning,
                format!(
                    "Package '{}' has been deprecated. Consider finding an alternative package.",
                    hub_package.name
                ),
                io.status_reporter.as_ref(),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_common::FsError;
    use dbt_common::io_args::{IoArgs, StaticAnalysisOffReason};
    use dbt_common::io_utils::StatusReporter;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    // Mock status reporter for testing
    #[derive(Default)]
    struct MockStatusReporter {
        warnings: Arc<Mutex<Vec<String>>>,
    }

    impl MockStatusReporter {
        fn new() -> Self {
            Self {
                warnings: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn get_warnings(&self) -> Vec<String> {
            self.warnings.lock().unwrap().clone()
        }

        fn warning_count(&self) -> usize {
            self.warnings.lock().unwrap().len()
        }
    }

    impl StatusReporter for MockStatusReporter {
        fn collect_error(&self, _error: &FsError) {}

        fn collect_warning(&self, warning: &FsError) {
            self.warnings.lock().unwrap().push(warning.message());
        }

        fn collect_node_evaluation(
            &self,
            _file_path: PathBuf,
            _execution_phase: dbt_telemetry::ExecutionPhase,
            _node_outcome: dbt_telemetry::NodeOutcome,
            _upstream_target: Option<(String, String, bool)>,
            _static_analysis_off_reason: (Option<StaticAnalysisOffReason>, dbt_serde_yaml::Span),
        ) {
        }

        fn show_progress(&self, _action: &str, _target: &str, _description: Option<&str>) {}

        fn bulk_publish_empty(&self, _file_paths: Vec<PathBuf>) {}
    }

    // Helper function to create a test IoArgs
    fn create_test_io_args() -> IoArgs {
        IoArgs::default()
    }

    // Helper function to create a test IoArgs with mock status reporter
    #[allow(clippy::field_reassign_with_default)]
    fn create_test_io_args_with_reporter() -> (IoArgs, Arc<MockStatusReporter>) {
        let reporter = Arc::new(MockStatusReporter::new());
        let mut io_args = IoArgs::default();
        io_args.status_reporter = Some(reporter.clone());
        //let io_args = IoArgs {
        //    status_reporter: Some(reporter.clone()),
        //    ..Default::default()
        //};
        (io_args, reporter)
    }

    // Helper function to create a test HubPackageJson with deprecated flag
    fn create_deprecated_package() -> HubPackageJson {
        let mut versions = HashMap::new();
        versions.insert(
            "0.7.0".to_string(),
            HubPackageVersion {
                name: "dbt_utils".to_string(),
                packages: vec![],
                downloads: HubPackageDownloads {
                    tarball: "https://example.com/tarball.tar.gz".to_string(),
                },
                fusion_schema_compat: None,
                require_dbt_version: None,
            },
        );

        HubPackageJson {
            name: "fishtown-analytics/dbt_utils".to_string(),
            versions,
            deprecated: true,
            redirectnamespace: None,
            redirectname: None,
            latest_fusion_schema_compat: None,
        }
    }

    // Helper function to create a test HubPackageJson with redirect to new namespace and name
    fn create_redirected_package_full() -> HubPackageJson {
        let mut versions = HashMap::new();
        versions.insert(
            "0.7.0".to_string(),
            HubPackageVersion {
                name: "dbt_utils".to_string(),
                packages: vec![],
                downloads: HubPackageDownloads {
                    tarball: "https://example.com/tarball.tar.gz".to_string(),
                },
                fusion_schema_compat: None,
                require_dbt_version: None,
            },
        );

        HubPackageJson {
            name: "fishtown-analytics/dbt_utils".to_string(),
            versions,
            deprecated: false,
            redirectnamespace: Some("dbt-labs".to_string()),
            redirectname: Some("dbt_utils".to_string()),
            latest_fusion_schema_compat: None,
        }
    }

    // Helper function to create a test HubPackageJson with namespace redirect only
    fn create_redirected_package_namespace_only() -> HubPackageJson {
        let mut versions = HashMap::new();
        versions.insert(
            "1.0.0".to_string(),
            HubPackageVersion {
                name: "some_package".to_string(),
                packages: vec![],
                downloads: HubPackageDownloads {
                    tarball: "https://example.com/tarball.tar.gz".to_string(),
                },
                fusion_schema_compat: None,
                require_dbt_version: None,
            },
        );

        HubPackageJson {
            name: "old-org/some_package".to_string(),
            versions,
            deprecated: false,
            redirectnamespace: Some("new-org".to_string()),
            redirectname: None,
            latest_fusion_schema_compat: None,
        }
    }

    // Helper function to create a test HubPackageJson with name redirect only
    fn create_redirected_package_name_only() -> HubPackageJson {
        let mut versions = HashMap::new();
        versions.insert(
            "1.0.0".to_string(),
            HubPackageVersion {
                name: "old_name".to_string(),
                packages: vec![],
                downloads: HubPackageDownloads {
                    tarball: "https://example.com/tarball.tar.gz".to_string(),
                },
                fusion_schema_compat: None,
                require_dbt_version: None,
            },
        );

        HubPackageJson {
            name: "org/old_name".to_string(),
            versions,
            deprecated: false,
            redirectnamespace: None,
            redirectname: Some("new_name".to_string()),
            latest_fusion_schema_compat: None,
        }
    }

    #[test]
    fn test_deserialize_deprecated_package() {
        let json = r#"
        {
            "name": "fishtown-analytics/dbt_utils",
            "versions": {
                "0.7.0": {
                    "name": "dbt_utils",
                    "packages": [],
                    "downloads": {
                        "tarball": "https://example.com/tarball.tar.gz"
                    }
                }
            },
            "deprecated": true
        }
        "#;

        let package: HubPackageJson = serde_json::from_str(json).unwrap();
        assert_eq!(package.name, "fishtown-analytics/dbt_utils");
        assert!(package.deprecated);
        assert!(package.redirectnamespace.is_none());
        assert!(package.redirectname.is_none());
        assert!(package.latest_fusion_schema_compat.is_none());
    }

    #[test]
    fn test_deserialize_redirected_package_full() {
        let json = r#"
        {
            "name": "fishtown-analytics/dbt_utils",
            "versions": {
                "0.7.0": {
                    "name": "dbt_utils",
                    "packages": [],
                    "downloads": {
                        "tarball": "https://example.com/tarball.tar.gz"
                    }
                }
            },
            "redirectnamespace": "dbt-labs",
            "redirectname": "dbt_utils"
        }
        "#;

        let package: HubPackageJson = serde_json::from_str(json).unwrap();
        assert_eq!(package.name, "fishtown-analytics/dbt_utils");
        assert!(!package.deprecated); // Should default to false
        assert_eq!(package.redirectnamespace.as_ref().unwrap(), "dbt-labs");
        assert_eq!(package.redirectname.as_ref().unwrap(), "dbt_utils");
        assert!(package.latest_fusion_schema_compat.is_none());
    }

    #[test]
    fn test_deserialize_package_no_redirect_fields() {
        let json = r#"
        {
            "name": "some-org/some_package",
            "versions": {
                "1.0.0": {
                    "name": "some_package",
                    "packages": [],
                    "downloads": {
                        "tarball": "https://example.com/tarball.tar.gz"
                    }
                }
            }
        }
        "#;

        let package: HubPackageJson = serde_json::from_str(json).unwrap();
        assert_eq!(package.name, "some-org/some_package");
        assert!(!package.deprecated); // Should default to false
        assert!(package.redirectnamespace.is_none());
        assert!(package.redirectname.is_none());
        assert!(package.latest_fusion_schema_compat.is_none());
    }

    #[test]
    fn test_deserialize_package_with_fusion_schema_compat() {
        let json = r#"
        {
            "name": "some-org/fusion_package",
            "versions": {
                "1.0.0": {
                    "name": "fusion_package",
                    "packages": [],
                    "downloads": {
                        "tarball": "https://example.com/tarball.tar.gz"
                    },
                    "fusion-schema-compat": true
                }
            },
            "latest-fusion-schema-compat": true
        }
        "#;

        let package: HubPackageJson = serde_json::from_str(json).unwrap();
        assert_eq!(package.name, "some-org/fusion_package");
        assert!(!package.deprecated);
        assert!(package.redirectnamespace.is_none());
        assert!(package.redirectname.is_none());
        assert_eq!(package.latest_fusion_schema_compat, Some(true));

        let version = package.versions.get("1.0.0").unwrap();
        assert_eq!(version.fusion_schema_compat, Some(true));
    }

    #[test]
    fn test_check_package_deprecation_deprecated_package() {
        let client = HubClient::new("https://test.example.com");
        let package = create_deprecated_package();
        let io_args = create_test_io_args();

        // This test verifies the function runs without panicking
        // In a real scenario, this would trigger a warning through the logging system
        client.check_package_deprecation(&io_args, &package);
    }

    #[test]
    fn test_check_package_deprecation_full_redirect() {
        let client = HubClient::new("https://test.example.com");
        let package = create_redirected_package_full();
        let io_args = create_test_io_args();

        // This test verifies the function runs without panicking
        // In a real scenario, this would trigger a redirect warning
        client.check_package_deprecation(&io_args, &package);
    }

    #[test]
    fn test_check_package_deprecation_namespace_redirect() {
        let client = HubClient::new("https://test.example.com");
        let package = create_redirected_package_namespace_only();
        let io_args = create_test_io_args();

        // This test verifies the function runs without panicking
        client.check_package_deprecation(&io_args, &package);
    }

    #[test]
    fn test_check_package_deprecation_name_redirect() {
        let client = HubClient::new("https://test.example.com");
        let package = create_redirected_package_name_only();
        let io_args = create_test_io_args();

        // This test verifies the function runs without panicking
        client.check_package_deprecation(&io_args, &package);
    }

    #[test]
    fn test_fishtown_analytics_dbt_utils_case() {
        // This test specifically simulates the case mentioned in the original Python code
        let client = HubClient::new("https://test.example.com");
        let package = create_deprecated_package();
        let io_args = create_test_io_args();

        // Verify package properties match the expected case
        assert_eq!(package.name, "fishtown-analytics/dbt_utils");
        assert!(package.deprecated);
        assert!(package.versions.contains_key("0.7.0"));

        // This would trigger the deprecation warning in a real scenario
        client.check_package_deprecation(&io_args, &package);
    }

    #[test]
    fn test_deserialize_package_with_require_dbt_version_string() {
        let json = r#"
        {
            "name": "some-org/versioned_package",
            "versions": {
                "1.0.0": {
                    "name": "versioned_package",
                    "packages": [],
                    "downloads": {
                        "tarball": "https://example.com/tarball.tar.gz"
                    },
                    "require_dbt_version": ">=1.5.0"
                }
            }
        }
        "#;

        let package: HubPackageJson = serde_json::from_str(json).unwrap();
        assert_eq!(package.name, "some-org/versioned_package");

        let version = package.versions.get("1.0.0").unwrap();
        assert!(version.require_dbt_version.is_some());

        // Verify it's a string variant
        if let Some(StringOrArrayOfStrings::String(version_req)) = &version.require_dbt_version {
            assert_eq!(version_req, ">=1.5.0");
        } else {
            panic!("Expected StringOrArrayOfStrings::String variant");
        }
    }

    #[test]
    fn test_deserialize_package_with_require_dbt_version_array() {
        let json = r#"
        {
            "name": "some-org/versioned_package",
            "versions": {
                "1.0.0": {
                    "name": "versioned_package",
                    "packages": [],
                    "downloads": {
                        "tarball": "https://example.com/tarball.tar.gz"
                    },
                    "require_dbt_version": [">=1.5.0", "<2.0.0"]
                }
            }
        }
        "#;

        let package: HubPackageJson = serde_json::from_str(json).unwrap();
        assert_eq!(package.name, "some-org/versioned_package");

        let version = package.versions.get("1.0.0").unwrap();
        assert!(version.require_dbt_version.is_some());

        // Verify it's an array variant
        if let Some(StringOrArrayOfStrings::ArrayOfStrings(versions)) = &version.require_dbt_version
        {
            assert_eq!(versions.len(), 2);
            assert_eq!(versions[0], ">=1.5.0");
            assert_eq!(versions[1], "<2.0.0");
        } else {
            panic!("Expected StringOrArrayOfStrings::ArrayOfStrings variant");
        }
    }

    #[test]
    fn test_deserialize_package_without_require_dbt_version() {
        let json = r#"
        {
            "name": "some-org/unversioned_package",
            "versions": {
                "1.0.0": {
                    "name": "unversioned_package",
                    "packages": [],
                    "downloads": {
                        "tarball": "https://example.com/tarball.tar.gz"
                    }
                }
            }
        }
        "#;

        let package: HubPackageJson = serde_json::from_str(json).unwrap();
        assert_eq!(package.name, "some-org/unversioned_package");

        let version = package.versions.get("1.0.0").unwrap();
        assert!(version.require_dbt_version.is_none());
    }

    #[test]
    fn test_check_require_dbt_version_compatible() {
        let client = HubClient::new("https://test.example.com");
        let version = HubPackageVersion {
            name: "test_package".to_string(),
            packages: vec![],
            downloads: HubPackageDownloads {
                tarball: "https://example.com/tarball.tar.gz".to_string(),
            },
            fusion_schema_compat: None,
            require_dbt_version: Some(StringOrArrayOfStrings::String(">=1.5.0".to_string())),
        };

        let (io_args, reporter) = create_test_io_args_with_reporter();

        // This should NOT trigger a warning since CARGO_PKG_VERSION >= 1.5.0
        client.check_require_dbt_version(&io_args, "test-org/test_package", &version);

        // Verify no warnings were issued
        assert_eq!(
            reporter.warning_count(),
            0,
            "Expected no warnings for compatible version"
        );
    }

    #[test]
    fn test_check_require_dbt_version_incompatible() {
        let client = HubClient::new("https://test.example.com");
        let version = HubPackageVersion {
            name: "test_package".to_string(),
            packages: vec![],
            downloads: HubPackageDownloads {
                tarball: "https://example.com/tarball.tar.gz".to_string(),
            },
            fusion_schema_compat: None,
            require_dbt_version: Some(StringOrArrayOfStrings::String(">=100.0.0".to_string())),
        };

        let (io_args, reporter) = create_test_io_args_with_reporter();

        // This SHOULD trigger a warning since CARGO_PKG_VERSION < 100.0.0
        client.check_require_dbt_version(&io_args, "test-org/test_package", &version);

        // Verify a warning was issued
        assert_eq!(
            reporter.warning_count(),
            1,
            "Expected one warning for incompatible version"
        );
        let warnings = reporter.get_warnings();
        assert!(
            warnings[0].contains("test-org/test_package"),
            "Warning should mention package name"
        );
        assert!(
            warnings[0].contains(">=100.0.0"),
            "Warning should mention required version"
        );
    }

    #[test]
    fn test_check_require_dbt_version_range_compatible() {
        let client = HubClient::new("https://test.example.com");
        let version = HubPackageVersion {
            name: "test_package".to_string(),
            packages: vec![],
            downloads: HubPackageDownloads {
                tarball: "https://example.com/tarball.tar.gz".to_string(),
            },
            fusion_schema_compat: None,
            require_dbt_version: Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                ">=1.0.0".to_string(),
                "<100.0.0".to_string(),
            ])),
        };

        let (io_args, reporter) = create_test_io_args_with_reporter();

        // This should NOT trigger a warning since 1.0.0 <= CARGO_PKG_VERSION < 100.0.0
        client.check_require_dbt_version(&io_args, "test-org/test_package", &version);

        // Verify no warnings were issued
        assert_eq!(
            reporter.warning_count(),
            0,
            "Expected no warnings for compatible version range"
        );
    }

    #[test]
    fn test_check_require_dbt_version_range_incompatible() {
        let client = HubClient::new("https://test.example.com");
        let version = HubPackageVersion {
            name: "test_package".to_string(),
            packages: vec![],
            downloads: HubPackageDownloads {
                tarball: "https://example.com/tarball.tar.gz".to_string(),
            },
            fusion_schema_compat: None,
            require_dbt_version: Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                ">=100.0.0".to_string(),
                "<200.0.0".to_string(),
            ])),
        };

        let (io_args, reporter) = create_test_io_args_with_reporter();

        // This SHOULD trigger a warning since CARGO_PKG_VERSION < 100.0.0 (outside range)
        client.check_require_dbt_version(&io_args, "test-org/test_package", &version);

        // Verify a warning was issued
        assert_eq!(
            reporter.warning_count(),
            1,
            "Expected one warning for incompatible version range"
        );
        let warnings = reporter.get_warnings();
        assert!(
            warnings[0].contains("test-org/test_package"),
            "Warning should mention package name"
        );
        assert!(
            warnings[0].contains("[>=100.0.0, <200.0.0]"),
            "Warning should mention required version range"
        );
    }

    #[test]
    fn test_check_require_dbt_version_no_requirement() {
        let client = HubClient::new("https://test.example.com");
        let version = HubPackageVersion {
            name: "test_package".to_string(),
            packages: vec![],
            downloads: HubPackageDownloads {
                tarball: "https://example.com/tarball.tar.gz".to_string(),
            },
            fusion_schema_compat: None,
            require_dbt_version: None, // No version requirement
        };

        let (io_args, reporter) = create_test_io_args_with_reporter();

        // This should NOT trigger any warning (no requirement = compatible)
        client.check_require_dbt_version(&io_args, "test-org/test_package", &version);

        // Verify no warnings were issued
        assert_eq!(
            reporter.warning_count(),
            0,
            "Expected no warnings when no version requirement exists"
        );
    }
}
