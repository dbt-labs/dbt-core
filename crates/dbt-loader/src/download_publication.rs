use crate::cloud_http_client::{CloudAuthScheme, build_cloud_api_client, build_private_api_url};
use dbt_cloud_config::ResolvedCloudConfig;
use dbt_common::io_args::IoArgs;
use dbt_common::tracing::emit::{emit_debug_log_message, emit_info_progress_message};
use dbt_common::{ErrorCode, FsResult, fs_err};
use dbt_schemas::schemas::packages::UpstreamProject;
use dbt_telemetry::ProgressMessage;
use std::time::SystemTime;

const DOWNLOAD_INTERVAL: u64 = 3600; // 1 hour

#[allow(clippy::cognitive_complexity)]
/// Downloads publication artifacts for each upstream project
///
/// This function checks if the environment variable `DBT_CLOUD_PUBLICATIONS_DIR` is set.
/// If it is, it uses the specified directory for storing publication artifacts.
/// Otherwise it will download the publication artifacts to the target directory if upstream_projects are specified.
///
pub(crate) async fn download_publication_artifacts(
    upstream_projects: &[UpstreamProject],
    dbt_cloud_config: &Option<ResolvedCloudConfig>,
    io: &IoArgs,
) -> FsResult<()> {
    // Skip if environment variable is set or no upstream projects
    if std::env::var("DBT_CLOUD_PUBLICATIONS_DIR").is_ok() || upstream_projects.is_empty() {
        return Ok(());
    }

    // Create directory for publication artifacts
    let default_dir = io.out_dir.join("dbt_cloud_publications");
    std::fs::create_dir_all(&default_dir)?;

    // Check if all artifacts are recent (less than an hour old)
    let mut all_artifacts_recent = true;
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get system time: {}", e))?
        .as_secs();

    for upstream_project in upstream_projects {
        let artifact_path = default_dir.join(format!("{}.json", upstream_project.name));
        let info_path = default_dir.join(format!("{}.info", upstream_project.name));

        if artifact_path.exists() && info_path.exists() {
            // Read the timestamp from info file
            if let Ok(timestamp_str) = std::fs::read_to_string(&info_path)
                && let Ok(last_download_time) = timestamp_str.trim().parse::<u64>()
            {
                // If less than an hour has passed, continue to next artifact
                if now - last_download_time <= DOWNLOAD_INTERVAL {
                    continue;
                }
            }
        }

        // If we get here, at least one artifact needs downloading
        all_artifacts_recent = false;
        break;
    }

    // If all artifacts are recent, we can skip the download process
    #[allow(clippy::disallowed_methods)]
    if all_artifacts_recent {
        unsafe {
            std::env::set_var(
                "DBT_CLOUD_PUBLICATIONS_DIR",
                default_dir.display().to_string(),
            );
        }
        return Ok(());
    }
    // remove all files in the default_dir
    std::fs::remove_dir_all(&default_dir)?;
    std::fs::create_dir_all(&default_dir)?;

    let creds = dbt_cloud_config
        .as_ref()
        .and_then(|c| c.credentials.as_ref())
        .ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "Cannot download publication artifacts: no dbt Cloud credentials configured."
            )
        })?;
    let project_id = dbt_cloud_config
        .as_ref()
        .and_then(|c| c.project_id.as_deref())
        .ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "Cannot download publication artifacts: no project ID configured."
            )
        })?;
    let (account_id, account_host, token) = (
        creds.account_id.as_str(),
        creds.host.as_str(),
        creds.token.as_str(),
    );
    emit_debug_log_message(format!(
        "Publication download config: host={}, account_id={}",
        account_host, account_id
    ));
    let cloud_client = build_cloud_api_client(token, CloudAuthScheme::Bearer, None)?;

    // Download artifacts for each upstream project
    for upstream_project in upstream_projects {
        let artifact_path = default_dir.join(format!("{}.json", upstream_project.name));
        let info_path = default_dir.join(format!("{}.info", upstream_project.name));

        // Check if artifact already exists and is recent (less than an hour old)
        let should_download = if artifact_path.exists() && info_path.exists() {
            // Read the timestamp from info file
            let timestamp_str = std::fs::read_to_string(&info_path)
                .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to read info file: {}", e))?;

            let last_download_time: u64 = timestamp_str.trim().parse().map_err(|e| {
                fs_err!(
                    ErrorCode::IoError,
                    "Failed to parse timestamp from info file: {}",
                    e
                )
            })?;

            // Check if more than an hour has passed
            now - last_download_time > DOWNLOAD_INTERVAL
        } else {
            true
        };

        if !should_download {
            emit_debug_log_message(format!(
                "Skipping download for {}, cached artifact is still fresh",
                upstream_project.name
            ));
            continue;
        }

        // The API expects the consumer (current) project ID in the path,
        // and the producer (upstream) project name as a query parameter.
        let url = build_private_api_url(
            account_host,
            account_id,
            &format!(
                "projects/{}/artifacts/publication/?dbt_project_name={}",
                project_id, upstream_project.name
            ),
        );

        emit_debug_log_message(format!("Publication download URL: {}", url));

        // Log download attempt
        emit_info_progress_message(
            ProgressMessage::new_from_action_and_target(
                "Downloading".to_string(),
                format!("publication artifact for {}", upstream_project.name),
            ),
            io.status_reporter.as_ref(),
        );

        // Execute HTTP request
        let response =
            cloud_client.get(&url).send().await.map_err(|e| {
                fs_err!(ErrorCode::IoError, "Failed to execute HTTP request: {}", e)
            })?;

        if !response.status().is_success() {
            return Err(fs_err!(
                ErrorCode::IoError,
                "Failed to download publication artifact from {}: HTTP status {}",
                url,
                response.status()
            ));
        }

        // Process and save response
        let bytes = response
            .bytes()
            .await
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to read response body: {}", e))?;

        let mut response_json: serde_json::Value = serde_json::from_slice(&bytes).map_err(|e| {
            fs_err!(
                ErrorCode::IoError,
                "Failed to parse response as JSON: {}",
                e
            )
        })?;

        // Set all public_node_dependencies to empty lists, if we don't do this, extend is very slow
        if let Some(data) = response_json.get_mut("data")
            && let Some(public_models) = data.get_mut("public_models")
            && let Some(models_obj) = public_models.as_object_mut()
        {
            for (_, model) in models_obj.iter_mut() {
                if let Some(model_obj) = model.as_object_mut() {
                    // Replace public_node_dependencies with an empty array
                    model_obj.insert(
                        "public_node_dependencies".to_string(),
                        serde_json::Value::Array(vec![]),
                    );
                }
            }
        }

        let publication_json = serde_json::to_string(&response_json["data"]).map_err(|e| {
            fs_err!(
                ErrorCode::IoError,
                "Failed to serialize JSON data to string: {}",
                e
            )
        })?;

        // Ensure parent directory exists before writing file
        if let Some(parent) = artifact_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                fs_err!(
                    ErrorCode::IoError,
                    "Failed to create directory for artifact: {}",
                    e
                )
            })?;
        }
        std::fs::write(&artifact_path, publication_json).map_err(|e| {
            fs_err!(
                ErrorCode::IoError,
                "Failed to write artifact to file: {}",
                e
            )
        })?;

        // Write timestamp to info file
        std::fs::write(&info_path, now.to_string())
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to write info file: {}", e))?;

        // Log successful download
        emit_info_progress_message(
            ProgressMessage::new_from_action_and_target(
                "Downloaded".to_string(),
                format!(
                    "publication artifact for {} to {}",
                    upstream_project.name,
                    artifact_path.display()
                ),
            ),
            io.status_reporter.as_ref(),
        );
    }

    // Set environment variable to the download directory
    unsafe {
        #[allow(clippy::disallowed_methods)]
        std::env::set_var(
            "DBT_CLOUD_PUBLICATIONS_DIR",
            default_dir.display().to_string(),
        );
    }

    Ok(())
}
