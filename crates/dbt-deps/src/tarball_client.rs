use dbt_common::{fs_err, ErrorCode, FsResult};
use reqwest::{Client, StatusCode};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{
    policies::ExponentialBackoff as RetryExponentialBackoff, RetryTransientMiddleware,
};
use std::{fs::File, io::Write, path::Path};

const MAX_CLIENT_RETRIES: u32 = 3;

pub struct TarballClient {
    pub client: ClientWithMiddleware,
}

impl TarballClient {
    pub fn new() -> Self {
        let retry_policy =
            RetryExponentialBackoff::builder().build_with_max_retries(MAX_CLIENT_RETRIES);
        let client = ClientBuilder::new(Client::new())
            // Retry failed requests.
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();
        Self { client }
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

    pub async fn download_and_extract_tarball(
        &mut self,
        download_url: &str,
        tar_path: &Path,
        untar_path: &tempfile::TempDir,
        _package_type: &str,
    ) -> FsResult<()> {
        // Download the tarball
        self.download_tarball(download_url, tar_path).await?;

        // Extract the tarball
        let tar = File::open(tar_path)
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to open tar file: {}", e))?;

        let gz = flate2::read::GzDecoder::new(tar);
        let mut tar = tar::Archive::new(gz);

        tar.unpack(untar_path)
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to unpack tar file: {}", e))?;

        Ok(())
    }
}
