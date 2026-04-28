//! Tarball download and extraction client.
//!
//! Downloads tarballs (`.tar.gz`) from URLs and extracts them directly to disk.
//! Uses async streaming extraction (no intermediate files or memory buffering)
//! and automatic retry logic for transient failures.
//! Supports selective extraction with root directory stripping and subdirectory filtering.

use async_compression::tokio::bufread::GzipDecoder;
use dbt_common::{ErrorCode, FsResult, err, fs_err};
use futures::StreamExt;
use reqwest::{Client, StatusCode};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{
    RetryTransientMiddleware, policies::ExponentialBackoff as RetryExponentialBackoff,
};
use std::io;
use std::path::{Path, PathBuf};
use tokio_tar::Archive;
use tokio_util::io::StreamReader;

const MAX_CLIENT_RETRIES: u32 = 3;

/// Client for downloading and extracting tarball archives.
pub struct TarballClient {
    pub client: ClientWithMiddleware,
}

impl TarballClient {
    /// Create a new tarball client with retry logic configured.
    pub fn new() -> Self {
        let retry_policy =
            RetryExponentialBackoff::builder().build_with_max_retries(MAX_CLIENT_RETRIES);
        let client = ClientBuilder::new(Client::new())
            // Retry failed requests.
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();
        Self { client }
    }

    /// Download tarball from URL and extract to target directory with optional filtering.
    ///
    /// # Arguments
    /// * `download_url` - URL of the tarball to download
    /// * `target_path` - Directory to extract contents into. **Must already exist
    ///   and be writable; lifecycle (creation and cleanup on error) is the
    ///   caller's responsibility.**
    /// * `strip_root` - If true, strip the single root directory from archive
    /// * `subdirectory` - If provided, only extract entries from this subdirectory
    /// * `headers` - Additional HTTP request headers (e.g. `Authorization`);
    ///   pass `&[]` when none are needed
    ///
    /// Streams download directly from network through gzip decoder to tar extractor,
    /// avoiding intermediate memory buffering or file I/O.
    pub async fn download_and_extract_tarball(
        &self,
        download_url: &str,
        target_path: &Path,
        strip_root: bool,
        subdirectory: Option<&str>,
        headers: &[(&str, &str)],
    ) -> FsResult<PathBuf> {
        let mut req = self.client.get(download_url);
        for (name, value) in headers {
            req = req.header(*name, *value);
        }
        let res = req.send().await.map_err(|e| {
            fs_err!(
                ErrorCode::RuntimeError,
                "Failed to get tarball from {download_url}; status: {}",
                e.status().unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
            )
        })?;

        if !res.status().is_success() {
            return err!(
                ErrorCode::RuntimeError,
                "Failed to download tarball from {download_url}; status: {}",
                res.status()
            );
        }

        // Convert reqwest stream to AsyncRead
        let stream = res.bytes_stream().map(|result| {
            result.map_err(|e| io::Error::other(format!("Failed to read stream: {}", e)))
        });

        let reader = StreamReader::new(stream);
        let decoder = GzipDecoder::new(reader);
        let mut archive = Archive::new(decoder);

        let mut entries = archive
            .entries()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to read tar entries: {}", e))?;

        let mut root_dir: Option<String> = None;
        let mut prefix = PathBuf::new();
        let mut extracted_any = false;

        while let Some(entry_result) = entries.next().await {
            let mut entry = entry_result
                .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to read tar entry: {}", e))?;

            let entry_path: PathBuf = entry
                .path()
                .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get entry path: {}", e))?
                .into_owned();

            // Determine/validate root directory
            if strip_root {
                // Skip special entries like pax_global_header and macOS resource forks
                let path_str = entry_path.to_string_lossy();
                if path_str == "pax_global_header" || path_str.starts_with("._") {
                    continue;
                }

                let first = entry_path
                    .components()
                    .next()
                    .and_then(|c| match c {
                        std::path::Component::Normal(s) => Some(s.to_string_lossy().into_owned()),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        fs_err!(
                            ErrorCode::InvalidConfig,
                            "Invalid tar entry path: {}",
                            entry_path.display()
                        )
                    })?;

                match &root_dir {
                    None => {
                        // Compute prefix once when root is discovered
                        prefix = match subdirectory {
                            Some(subdir) => PathBuf::from(&first).join(subdir),
                            None => PathBuf::from(&first),
                        };
                        root_dir = Some(first);
                    }
                    Some(existing_root) => {
                        if *existing_root != first {
                            return err!(
                                ErrorCode::InvalidConfig,
                                "Tarball has multiple root directories: '{}' and '{}'. Expected single root directory.",
                                existing_root,
                                first
                            );
                        }
                    }
                }
            } else if root_dir.is_none() && subdirectory.is_some() {
                // For non-strip-root with subdirectory, compute prefix once
                root_dir = Some(String::new()); // sentinel to avoid re-entering
                prefix = PathBuf::from(subdirectory.unwrap());
            }

            // Filter: skip entries outside the prefix
            if !prefix.as_os_str().is_empty() && !entry_path.starts_with(&prefix) {
                continue;
            }

            // Strip prefix to get relative path
            let relative_path: &Path = if !prefix.as_os_str().is_empty() {
                entry_path.strip_prefix(&prefix).unwrap_or(&entry_path)
            } else {
                &entry_path
            };

            // Skip empty paths (the prefix directory entry itself)
            if relative_path.as_os_str().is_empty() {
                continue;
            }

            let target_entry_path = target_path.join(relative_path);

            // Security: reject paths that escape the target directory (e.g. via ".." components)
            if target_entry_path
                .components()
                .any(|c| c == std::path::Component::ParentDir)
            {
                return err!(
                    ErrorCode::InvalidConfig,
                    "Refusing to extract tar entry with path traversal: {}",
                    entry_path.display()
                );
            }

            entry.unpack(&target_entry_path).await.map_err(|e| {
                fs_err!(
                    ErrorCode::IoError,
                    "Failed to unpack entry {}: {}",
                    entry_path.display(),
                    e
                )
            })?;

            extracted_any = true;
        }

        // Validate that we extracted something
        if !extracted_any {
            if let Some(subdir) = subdirectory {
                return err!(
                    ErrorCode::InvalidConfig,
                    "No entries found matching subdirectory '{}' in tarball from {}",
                    subdir,
                    download_url
                );
            } else if strip_root {
                return err!(
                    ErrorCode::InvalidConfig,
                    "No root directory found in tarball from {}",
                    download_url
                );
            } else {
                return err!(
                    ErrorCode::InvalidConfig,
                    "No entries found in tarball from {}",
                    download_url
                );
            }
        }

        Ok(target_path.to_path_buf())
    }
}

/// Download and extract a tarball package to a directory.
///
/// `download_dir` must already exist and be writable; cleanup on error is the
/// caller's responsibility.
pub async fn download_tarball_package(tarball_url: &str, download_dir: &Path) -> FsResult<PathBuf> {
    let client = TarballClient::new();
    client
        .download_and_extract_tarball(tarball_url, download_dir, true, None, &[])
        .await
}
