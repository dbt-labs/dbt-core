use dbt_common::io_args::IoArgs;
use dbt_common::{ErrorCode, FsResult, fs_err, fsinfo, show_progress, show_warning};
use dbt_schemas::schemas::project::ProjectDbtCloudConfig;
use flate2::read::GzDecoder;
use reqwest_middleware::ClientBuilder;
use reqwest_retry::{
    RetryTransientMiddleware, policies::ExponentialBackoff as RetryExponentialBackoff,
};
use std::error::Error;
use std::io::Read;
use std::path::PathBuf;
use std::time::SystemTime;

use crate::utils::load_raw_yml;

const DOWNLOAD_INTERVAL: u64 = 3600; // 1 hour
const MAX_CLIENT_RETRIES: u32 = 3;

/// Process manifest bytes - handles both plain JSON and gzip-compressed JSON
/// Returns the valid JSON bytes or None if the data is invalid
fn process_manifest_bytes(bytes: &[u8]) -> Option<Vec<u8>> {
    // First, check if it's already valid JSON
    if serde_json::from_slice::<serde_json::Value>(bytes).is_ok() {
        return Some(bytes.to_vec());
    }

    // Not valid JSON, try to decompress as gzip
    let mut decoder = GzDecoder::new(bytes);
    let mut decompressed = Vec::new();

    match decoder.read_to_end(&mut decompressed) {
        Ok(_) => {
            // Check if decompressed content is valid JSON
            if serde_json::from_slice::<serde_json::Value>(&decompressed).is_ok() {
                Some(decompressed)
            } else {
                // Decompressed but still not valid JSON
                None
            }
        }
        Err(_) => {
            // Failed to decompress
            None
        }
    }
}

/// Downloads manifest from dbt Cloud if available and not recently cached
#[allow(clippy::cognitive_complexity)]
pub async fn hydrate_or_download_manifest_from_cloud(
    dbt_cloud_config: &Option<ProjectDbtCloudConfig>,
    io: &IoArgs,
) -> FsResult<Option<PathBuf>> {
    // Check if dbt cloud config exists and has project_id
    let project_id = match dbt_cloud_config {
        Some(config) => match &config.project_id {
            Some(id) => id,
            None => return Ok(None),
        },
        None => return Ok(None),
    };

    // Create directory for manifest
    let default_dir = io.out_dir.join("dbt_cloud_defer");
    std::fs::create_dir_all(&default_dir)?;

    let manifest_path = default_dir.join("manifest.json");
    let info_path = default_dir.join("manifest.info");

    // Check if manifest already exists and is recent (less than an hour old)
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get system time: {}", e))?
        .as_secs();

    if manifest_path.exists() && info_path.exists() {
        // Read the timestamp from info file
        if let Ok(timestamp_str) = std::fs::read_to_string(&info_path) {
            if let Ok(last_download_time) = timestamp_str.trim().parse::<u64>() {
                // If less than an hour has passed, use existing manifest
                if now - last_download_time <= DOWNLOAD_INTERVAL {
                    return Ok(Some(default_dir));
                }
            }
        }
    }

    // Get home directory
    let home_dir = match dirs::home_dir() {
        Some(dir) => dir,
        None => {
            return Err(fs_err!(
                ErrorCode::IoError,
                "Could not determine home directory"
            ));
        }
    };

    // Check if dbt_cloud.yml exists
    let dbt_cloud_config_path = home_dir.join(".dbt").join("dbt_cloud.yml");
    if !dbt_cloud_config_path.exists() {
        return Ok(None);
    }

    // Load dbt cloud configuration
    let dbt_cloud_config: dbt_schemas::schemas::DbtCloudConfig =
        load_raw_yml(io, &dbt_cloud_config_path, None)?;
    // Determine which manifest path to use based on defer_env_id
    // If defer_env_id is specified, use the manifest/{env_id}/ path
    // Otherwise, use the manifest/latest/ path which will use the default staging > prod precedence
    let manifest_path_suffix = match &dbt_cloud_config.context.defer_env_id {
        Some(env_id) => {
            show_progress!(
                io,
                fsinfo!(
                    "INFO".into(),
                    format!("Using defer_env_id '{}' for manifest download", env_id)
                )
            );
            format!("manifest/{env_id}/")
        }
        None => "manifest/latest/".to_string(),
    };

    let project = match dbt_cloud_config.get_project_by_id(project_id.to_string().as_str()) {
        Some(p) => p,
        None => return Ok(None), // Project not found in dbt_cloud.yml, skip download
    };

    let (account_id, account_host, token) = (
        project.account_id.clone(),
        project.account_host.clone(),
        project.token_value.clone(),
    );

    // Construct API URL to get presigned link
    let url = format!(
        "https://{account_host}/api/private/accounts/{account_id}/projects/{project_id}/{manifest_path_suffix}"
    );

    // Log download attempt
    show_progress!(
        io,
        fsinfo!("DOWNLOADING".into(), "deferral manifest".to_string())
    );

    // First request to get presigned URL
    let retry_policy =
        RetryExponentialBackoff::builder().build_with_max_retries(MAX_CLIENT_RETRIES);
    let client = ClientBuilder::new(reqwest::Client::new())
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build();
    let response = match client
        .get(&url)
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
    {
        Ok(response) => response,
        Err(e) => {
            // Don't fail the entire operation if API request fails
            show_warning!(
                io,
                fs_err!(
                    ErrorCode::Generic,
                    "Failed to request deferral manifest from the dbt platform for project {}, continuing without deferral. Error: {}",
                    project_id,
                    e
                )
            );
            return Ok(None);
        }
    };

    let status = response.status();
    if !status.is_success() {
        let error_message = if let Ok(text) = response.text().await {
            // Try to parse JSON and extract user_message
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&text) {
                if let Some(user_message) = json_value["status"]["user_message"].as_str() {
                    format!(": {user_message}")
                } else {
                    format!(" - {text}")
                }
            } else {
                format!(" - {text}")
            }
        } else {
            "".to_string()
        };

        show_warning!(
            io,
            fs_err!(
                ErrorCode::Generic,
                "Failed to request deferral manifest from the dbt platform for project {}, continuing without deferral. HTTP status {}{}",
                project_id,
                status,
                error_message
            )
        );
        return Ok(None);
    }

    // Parse response to get presigned URL
    let response_json: serde_json::Value = response.json().await.map_err(|e| {
        fs_err!(
            ErrorCode::IoError,
            "Failed to parse response as JSON: {}",
            e
        )
    })?;

    let presigned_url = response_json["data"]["manifest_href"]
        .as_str()
        .ok_or_else(|| {
            fs_err!(
                ErrorCode::IoError,
                "Failed to extract manifest_href from response"
            )
        })?;

    // Download manifest from presigned URL
    let manifest_response = match client.get(presigned_url).send().await {
        Ok(response) => response,
        Err(e) => {
            // Extract the source error from middleware/retry errors
            let source_error = if let Some(source) = e.source() {
                format!(" (source: {source})")
            } else {
                String::new()
            };
            show_warning!(
                io,
                fs_err!(
                    ErrorCode::Generic,
                    "Failed to download manifest: {}{}",
                    e,
                    source_error
                )
            );
            return Ok(None);
        }
    };

    if !manifest_response.status().is_success() {
        show_warning!(
            io,
            fs_err!(
                ErrorCode::Generic,
                "Failed to download deferral manifest from the dbt platform for project {}, continuing without deferral. HTTP status {}{}",
                project_id,
                manifest_response.status(),
                if let Ok(text) = manifest_response.text().await {
                    format!(" - {text}")
                } else {
                    "".to_string()
                }
            )
        );
        return Ok(None);
    }

    // Download manifest bytes
    let manifest_bytes = manifest_response
        .bytes()
        .await
        .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to read manifest body: {}", e))?;

    // Process the manifest bytes to ensure we have valid JSON
    let json_bytes = match process_manifest_bytes(&manifest_bytes) {
        Some(json) => {
            // Log if we had to decompress
            if json.len() != manifest_bytes.len() {
                show_progress!(
                    io,
                    fsinfo!(
                        "INFO".into(),
                        "Decompressed gzip-encoded deferral manifest".to_string()
                    )
                );
            }
            json
        }
        None => {
            // Invalid manifest data, fail gracefully
            show_warning!(
                io,
                fs_err!(
                    ErrorCode::Generic,
                    "Downloaded manifest is neither valid JSON nor gzip-compressed JSON. Continuing without deferral."
                )
            );
            return Ok(None);
        }
    };

    // Write the valid JSON to file
    std::fs::write(&manifest_path, json_bytes).map_err(|e| {
        fs_err!(
            ErrorCode::IoError,
            "Failed to write manifest to file: {}",
            e
        )
    })?;

    // Write timestamp to info file
    std::fs::write(&info_path, now.to_string())
        .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to write info file: {}", e))?;

    // Log successful download
    show_progress!(
        io,
        fsinfo!(
            "DOWNLOADED".into(),
            format!("deferral manifest to {}", manifest_path.display())
        )
    );

    Ok(Some(default_dir))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn create_sample_manifest() -> serde_json::Value {
        serde_json::json!({
            "version": 12,
            "project_id": "test_project",
            "metadata": {
                "project_id": "test_project",
                "dbt_version": "1.0.0"
            },
            "nodes": {}
        })
    }

    #[test]
    fn test_process_manifest_bytes_plain_json() {
        // Create plain JSON manifest
        let manifest = create_sample_manifest();
        let json_bytes = serde_json::to_vec(&manifest).unwrap();

        // Process should return the same bytes
        let result = process_manifest_bytes(&json_bytes);
        assert!(result.is_some());

        let processed = result.unwrap();
        assert_eq!(processed, json_bytes);

        // Verify the result is valid JSON
        let parsed: serde_json::Value = serde_json::from_slice(&processed).unwrap();
        assert_eq!(parsed["version"], 12);
    }

    #[test]
    fn test_process_manifest_bytes_gzipped_json() {
        // Create gzipped JSON manifest
        let manifest = create_sample_manifest();
        let json_bytes = serde_json::to_vec(&manifest).unwrap();

        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&json_bytes).unwrap();
        let compressed_bytes = encoder.finish().unwrap();

        // Process should decompress and return valid JSON
        let result = process_manifest_bytes(&compressed_bytes);
        assert!(result.is_some());

        let processed = result.unwrap();
        assert_ne!(processed.len(), compressed_bytes.len()); // Should be different size after decompression

        // Verify the result is valid JSON
        let parsed: serde_json::Value = serde_json::from_slice(&processed).unwrap();
        assert_eq!(parsed["version"], 12);
        assert_eq!(parsed["project_id"], "test_project");
    }

    #[test]
    fn test_process_manifest_bytes_invalid_data() {
        // Test with data that's neither JSON nor gzip
        let invalid_data = b"This is not JSON or gzip data";

        // Process should return None
        let result = process_manifest_bytes(invalid_data);
        assert!(result.is_none());
    }

    #[test]
    fn test_process_manifest_bytes_gzipped_non_json() {
        // Create gzipped non-JSON data
        let non_json_data = b"This is not JSON but will be gzipped";

        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(non_json_data).unwrap();
        let compressed_bytes = encoder.finish().unwrap();

        // Process should return None because decompressed data is not JSON
        let result = process_manifest_bytes(&compressed_bytes);
        assert!(result.is_none());
    }

    #[test]
    fn test_process_manifest_bytes_corrupt_gzip() {
        // Create corrupted gzip data
        let corrupt_gzip = b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x00corrupted data";

        // Process should return None
        let result = process_manifest_bytes(corrupt_gzip);
        assert!(result.is_none());
    }
}
