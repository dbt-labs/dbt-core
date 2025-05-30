use dbt_common::{err, fs_err, ErrorCode, FsResult};
use dbt_schemas::schemas::packages::DbtPackageEntry;
use reqwest::{Client, StatusCode};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{
    policies::ExponentialBackoff as RetryExponentialBackoff, RetryTransientMiddleware,
};
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::Write,
    path::Path,
};

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
}

#[derive(Deserialize, Clone)]
pub struct HubPackageJson {
    pub name: String,
    pub versions: HashMap<String, HubPackageVersion>,
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

    pub async fn download_tarball(&mut self, download_url: &str, out_path: &Path) -> FsResult<()> {
        let tarball_res = self.client.get(download_url).send().await.map_err(|e| {
            fs_err!(
                ErrorCode::RuntimeError,
                "Failed to get tarball from {download_url}; status: {}",
                e.status().unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
            )
        })?;
        if tarball_res.status().is_success() {
            let mut file = File::create(out_path).map_err(|e| {
                fs_err!(
                    ErrorCode::IoError,
                    "Failed to create file at {}; {}",
                    out_path.display(),
                    e
                )
            })?;
            file.write_all(
                tarball_res
                    .bytes()
                    .await
                    .map_err(|e| {
                        fs_err!(
                            ErrorCode::RuntimeError,
                            "Failed to write to file at {}; status: {}",
                            out_path.display(),
                            e
                        )
                    })?
                    .as_ref(),
            )
            .map_err(|e| {
                fs_err!(
                    ErrorCode::IoError,
                    "Failed to write to file at {}; {}",
                    out_path.display(),
                    e
                )
            })?;
        }
        Ok(())
    }
}
