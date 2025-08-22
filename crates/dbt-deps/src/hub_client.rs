use dbt_common::{ErrorCode, FsResult, err, fs_err, io_args::IoArgs, show_warning};
use dbt_schemas::schemas::packages::DbtPackageEntry;
use reqwest::{Client, StatusCode};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{
    RetryTransientMiddleware, policies::ExponentialBackoff as RetryExponentialBackoff,
};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};

pub const DBT_HUB_URL: &str = "https://hub.getdbt.com";
pub const DBT_CORE_FIXED_VERSION: &str = "1.8.7";
const MAX_CLIENT_RETRIES: u32 = 3;

#[derive(Deserialize, Clone)]
pub struct HubPackageDownloads {
    pub tarball: String,
}

#[derive(Deserialize, Clone)]
pub struct HubPackageVersion {
    pub name: String,
    pub packages: Vec<DbtPackageEntry>,
    pub downloads: HubPackageDownloads,
    #[serde(rename = "fusion-schema-compat")]
    pub fusion_schema_compat: Option<bool>,
}

#[derive(Deserialize, Clone)]
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
        // TODO: Implement version checking
        Ok(hub_package.versions.keys().cloned().collect())
    }

    /// Checks if a package is deprecated or redirected and shows appropriate warnings
    pub fn check_package_deprecation(&self, io: &IoArgs, hub_package: &HubPackageJson) {
        // Check for package redirect
        if let Some(redirect_namespace) = &hub_package.redirectnamespace {
            if let Some(redirect_name) = &hub_package.redirectname {
                show_warning!(
                    io,
                    fs_err!(
                        ErrorCode::DependencyWarning,
                        "Package '{}' has been moved to '{}/{}'. Please update your package reference.",
                        hub_package.name,
                        redirect_namespace,
                        redirect_name
                    )
                );
            } else {
                show_warning!(
                    io,
                    fs_err!(
                        ErrorCode::DependencyWarning,
                        "Package '{}' has been moved to namespace '{}'. Please update your package reference.",
                        hub_package.name,
                        redirect_namespace
                    )
                );
            }
        } else if let Some(redirect_name) = &hub_package.redirectname {
            show_warning!(
                io,
                fs_err!(
                    ErrorCode::DependencyWarning,
                    "Package '{}' has been renamed to '{}'. Please update your package reference.",
                    hub_package.name,
                    redirect_name
                )
            );
        }

        // Check for deprecation
        if hub_package.deprecated {
            show_warning!(
                io,
                fs_err!(
                    ErrorCode::DependencyWarning,
                    "Package '{}' has been deprecated. Consider finding an alternative package.",
                    hub_package.name
                )
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_common::io_args::IoArgs;
    use std::collections::HashMap;

    // Helper function to create a test IoArgs
    fn create_test_io_args() -> IoArgs {
        IoArgs::default()
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
}
