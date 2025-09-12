use dbt_common::io_args::IoArgs;
use dbt_common::{ErrorCode, FsResult, fs_err, fsinfo, show_progress};
use dbt_schemas::schemas::{packages::UpstreamProject, project::ProjectDbtCloudConfig};
use std::time::SystemTime;

use crate::utils::load_raw_yml;

const DOWNLOAD_INTERVAL: u64 = 3600; // 1 hour

#[allow(clippy::cognitive_complexity)]
/// Downloads publication artifacts for each upstream project
///
/// This function checks if the environment variable `DBT_CLOUD_PUBLICATIONS_DIR` is set.
/// If it is, it uses the specified directory for storing publication artifacts.
/// Otherwise it will download the publication artifacts to the target directory if upstream_projects are specifid.
///
pub(crate) async fn download_publication_artifacts(
    upstream_projects: &Vec<UpstreamProject>,
    dbt_cloud_config: &Option<ProjectDbtCloudConfig>,
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
            if let Ok(timestamp_str) = std::fs::read_to_string(&info_path) {
                if let Ok(last_download_time) = timestamp_str.trim().parse::<u64>() {
                    // If less than an hour has passed, continue to next artifact
                    if now - last_download_time <= DOWNLOAD_INTERVAL {
                        continue;
                    }
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

    // Get dbt cloud project ID
    let project_id = match dbt_cloud_config {
        Some(config) => match &config.project_id {
            Some(id) => id,
            None => {
                return Err(fs_err!(
                    ErrorCode::IoError,
                    "Trying to download publication artifacts but project_id not found in dbt_cloud configuration"
                ));
            }
        },
        None => {
            return Err(fs_err!(
                ErrorCode::IoError,
                "Trying to download publication artifacts but dbt_cloud configuration not found in project"
            ));
        }
    };

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

    // Load dbt cloud configuration
    let dbt_cloud_config_path = home_dir.join(".dbt").join("dbt_cloud.yml");
    let (account_id, account_host, token) = if dbt_cloud_config_path.exists() {
        let dbt_cloud_config: dbt_schemas::schemas::DbtCloudConfig =
            load_raw_yml(io, &dbt_cloud_config_path, None)?;

        let project = match dbt_cloud_config.get_project_by_id(project_id.to_string().as_str()) {
            Some(p) => p,
            None => {
                return Err(fs_err!(
                    ErrorCode::IoError,
                    "Trying to download publication artifacts but project_id not found in dbt_cloud.yml"
                ));
            }
        };

        (
            project.account_id.clone(),
            project.account_host.clone(),
            project.token_value.clone(),
        )
    } else {
        return Err(fs_err!(
            ErrorCode::IoError,
            "Trying to download publication artifacts but dbt_cloud.yml not found"
        ));
    };

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
            now - last_download_time > 3600
        } else {
            true
        };

        if !should_download {
            continue;
        }

        // Construct API URL
        let url = format!(
            "https://{}/api/private/accounts/{}/projects/{}/artifacts/publication/?dbt_project_name={}",
            account_host, account_id, project_id, upstream_project.name
        );

        // Log download attempt
        show_progress!(
            io,
            fsinfo!(
                "DOWNLOADING".into(),
                format!("publication artifact for {}", upstream_project.name)
            )
        );

        // Execute HTTP request
        let client = reqwest::Client::new();
        let response = client
            .get(&url)
            .header("Content-Type", "application/json")
            .header("Authorization", format!("Bearer {token}"))
            .send()
            .await
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to execute HTTP request: {}", e))?;

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
        if let Some(data) = response_json.get_mut("data") {
            if let Some(public_models) = data.get_mut("public_models") {
                if let Some(models_obj) = public_models.as_object_mut() {
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
        show_progress!(
            io,
            fsinfo!(
                "DOWNLOADED".into(),
                format!(
                    "publication artifact for {} to {}",
                    upstream_project.name,
                    artifact_path.display()
                )
            )
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
