use dbt_cloud_api::{
    apis::{configuration::Configuration, connections_api, users_api, whoami_api},
    models,
};
use dbt_common::{ErrorCode, FsResult, fs_err};
use dbt_schemas::schemas::profiles::{
    BigqueryDbConfig, DatabricksDbConfig, DbConfig, PostgresDbConfig, RedshiftDbConfig,
    SnowflakeDbConfig,
};
use dbt_schemas::schemas::serde::StringOrInteger;

use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudProject {
    #[serde(rename = "project-name")]
    pub project_name: String,
    #[serde(rename = "project-id")]
    pub project_id: String,
    #[serde(rename = "account-name")]
    pub account_name: String,
    #[serde(rename = "account-id")]
    pub account_id: String,
    #[serde(rename = "account-host")]
    pub account_host: String,
    #[serde(rename = "token-name")]
    pub token_name: String,
    #[serde(rename = "token-value")]
    pub token_value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbtCloudYml {
    pub version: String,
    pub context: DbtCloudContext,
    pub projects: Vec<CloudProject>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbtCloudContext {
    #[serde(rename = "active-host")]
    pub active_host: String,
    #[serde(rename = "active-project")]
    pub active_project: String,
}

// Helper struct to provide information about the credential without the full ConfigMap
#[derive(Debug, Clone)]
pub struct CredentialInfo {
    pub adapter_type: String,
    pub target_name: String,
    pub project_id: Option<i32>,
    pub state: i32,
}

impl From<&models::UserCredentialsResponse> for CredentialInfo {
    fn from(user_cred: &models::UserCredentialsResponse) -> Self {
        let (adapter_type, target_name) = match &*user_cred.credentials {
            models::UserCredentialsResponseCredentials::PostgresCredentials(postgres) => {
                ("postgres".to_string(), postgres.target_name.clone())
            }
            models::UserCredentialsResponseCredentials::SnowflakeCredentials(snowflake) => {
                ("snowflake".to_string(), snowflake.target_name.clone())
            }
            models::UserCredentialsResponseCredentials::BigqueryCredentials(bigquery) => {
                ("bigquery".to_string(), bigquery.target_name.clone())
            }
            models::UserCredentialsResponseCredentials::RedshiftCredentials(redshift) => {
                ("redshift".to_string(), redshift.target_name.clone())
            }
            models::UserCredentialsResponseCredentials::DbtAdapterCredentials(adapter) => {
                // Map adapter_version to specific adapter type for databricks
                let adapter_type = if let Some(adapter_version) = &adapter.adapter_version {
                    match adapter_version {
                        models::AdapterVersionEnum::DatabricksV0
                        | models::AdapterVersionEnum::DatabricksSparkV0 => "databricks".to_string(),
                        _ => "adapter".to_string(),
                    }
                } else {
                    "adapter".to_string()
                };
                (adapter_type, adapter.target_name.clone())
            }
        };

        let state = user_cred.state.unwrap_or(match &*user_cred.credentials {
            models::UserCredentialsResponseCredentials::PostgresCredentials(postgres) => {
                postgres.state
            }
            models::UserCredentialsResponseCredentials::SnowflakeCredentials(snowflake) => {
                snowflake.state
            }
            models::UserCredentialsResponseCredentials::BigqueryCredentials(bigquery) => {
                bigquery.state
            }
            models::UserCredentialsResponseCredentials::RedshiftCredentials(redshift) => {
                redshift.state
            }
            models::UserCredentialsResponseCredentials::DbtAdapterCredentials(adapter) => {
                adapter.state
            }
        });

        CredentialInfo {
            adapter_type,
            target_name,
            project_id: user_cred.project_id,
            state,
        }
    }
}

/// Create a complete DbConfig by merging user credential and connection data
fn create_merged_db_config(
    user_cred: &models::UserCredentialsResponse,
    connection_config: Option<&models::Config>,
) -> Option<DbConfig> {
    use merge::Merge;

    // Get threads for all adapters
    let threads = match &*user_cred.credentials {
        models::UserCredentialsResponseCredentials::PostgresCredentials(postgres) => {
            postgres.threads
        }
        models::UserCredentialsResponseCredentials::SnowflakeCredentials(snowflake) => {
            snowflake.threads
        }
        models::UserCredentialsResponseCredentials::BigqueryCredentials(bigquery) => {
            bigquery.threads
        }
        models::UserCredentialsResponseCredentials::RedshiftCredentials(redshift) => {
            redshift.threads
        }
        models::UserCredentialsResponseCredentials::DbtAdapterCredentials(adapter) => {
            adapter.threads
        }
    };

    // Create base config from user credential data
    let mut base_config = match &*user_cred.credentials {
        models::UserCredentialsResponseCredentials::PostgresCredentials(postgres) => {
            DbConfig::Postgres(Box::new(PostgresDbConfig {
                user: Some(postgres.username.clone()),
                schema: Some(postgres.default_schema.clone()),
                threads: Some(StringOrInteger::Integer(threads as i64)),
                // Note: Host, Password, Port, Database are not available in the credential response
                ..Default::default()
            }))
        }
        models::UserCredentialsResponseCredentials::SnowflakeCredentials(snowflake) => {
            DbConfig::Snowflake(Box::new(SnowflakeDbConfig {
                user: snowflake.user.clone(),
                role: snowflake.role.clone(),
                database: snowflake.database.clone(),
                warehouse: snowflake.warehouse.clone(),
                schema: Some(snowflake.schema.clone()),
                threads: Some(StringOrInteger::Integer(threads as i64)),
                // Note: Account field is intentionally NOT set here because:
                // 1. The API only provides account_id (integer), not the account name (string)
                // 2. The account name is required for Snowflake connections (e.g. "myaccount.snowflakecomputing.com")
                // 3. By leaving this field unset, the user will be prompted to enter it during profile setup
                ..Default::default()
            }))
        }
        models::UserCredentialsResponseCredentials::BigqueryCredentials(bigquery) => {
            DbConfig::Bigquery(Box::new(BigqueryDbConfig {
                schema: Some(bigquery.schema.clone()),
                threads: Some(StringOrInteger::Integer(threads as i64)),
                // Note: Method, Keyfile, Project are not available in the credential response
                database: None,
                profile_type: None,
                timeout_seconds: None,
                priority: None,
                method: None,
                maximum_bytes_billed: None,
                impersonate_service_account: None,
                refresh_token: None,
                client_id: None,
                client_secret: None,
                token_uri: None,
                token: None,
                keyfile: None,
                retries: None,
                location: None,
                scopes: None,
                keyfile_json: None,
                execution_project: None,
                compute_region: None,
                dataproc_batch: None,
                dataproc_cluster_name: None,
                dataproc_region: None,
                gcs_bucket: None,
                job_creation_timeout_seconds: None,
                job_execution_timeout_seconds: None,
                job_retries: None,
                job_retry_deadline_seconds: None,
                target_name: None,
            }))
        }
        models::UserCredentialsResponseCredentials::RedshiftCredentials(redshift) => {
            DbConfig::Redshift(Box::new(RedshiftDbConfig {
                user: redshift.username.clone(),
                schema: Some(redshift.default_schema.clone()),
                threads: Some(StringOrInteger::Integer(threads as i64)),
                // Note: Host, Password, Database are not available in the credential response
                ..Default::default()
            }))
        }
        models::UserCredentialsResponseCredentials::DbtAdapterCredentials(adapter) => {
            // Check if this is a databricks adapter
            match adapter.adapter_version {
                Some(models::AdapterVersionEnum::DatabricksV0)
                | Some(models::AdapterVersionEnum::DatabricksSparkV0) => {
                    DbConfig::Databricks(Box::new(DatabricksDbConfig {
                        threads: Some(StringOrInteger::Integer(threads as i64)),
                        // Note: Most fields are not available in the credential response
                        // This would need additional API calls or different credential structure
                        ..Default::default()
                    }))
                }
                _ => return None, // Unsupported adapter type
            }
        }
    };

    // Merge connection data if available and matches the adapter type
    if let Some(connection) = connection_config {
        match (&mut base_config, connection) {
            (
                DbConfig::Snowflake(snowflake_config),
                models::Config::SnowflakeConnection(snowflake),
            ) => {
                // Create connection details to merge - this provides the missing account/infrastructure info
                let connection_details = SnowflakeDbConfig {
                    account: if !snowflake.account.is_empty() {
                        Some(snowflake.account.clone())
                    } else {
                        None
                    },
                    database: if !snowflake.database.is_empty() {
                        Some(snowflake.database.clone())
                    } else {
                        None
                    },
                    warehouse: if !snowflake.warehouse.is_empty() {
                        Some(snowflake.warehouse.clone())
                    } else {
                        None
                    },
                    role: snowflake.role.clone(),
                    ..Default::default()
                };
                snowflake_config.merge(connection_details);
            }
            (DbConfig::Postgres(postgres_config), models::Config::PostgresConnection(postgres)) => {
                // Extract all available connection details for Postgres
                let connection_details = PostgresDbConfig {
                    host: if !postgres.hostname.is_empty() {
                        Some(postgres.hostname.clone())
                    } else {
                        None
                    },
                    database: if !postgres.dbname.is_empty() {
                        Some(postgres.dbname.clone())
                    } else {
                        None
                    },
                    port: postgres.port.map(|p| StringOrInteger::Integer(p as i64)),
                    retries: postgres.retries.map(|r| StringOrInteger::Integer(r as i64)),
                    ..Default::default()
                };
                postgres_config.merge(connection_details);
            }
            (DbConfig::Bigquery(bigquery_config), models::Config::BigqueryConnection(bigquery)) => {
                // Extract all available authentication and configuration details from connection
                let connection_details = BigqueryDbConfig {
                    database: if !bigquery.project_id.is_empty() {
                        Some(bigquery.project_id.clone())
                    } else {
                        None
                    },
                    timeout_seconds: Some(bigquery.timeout_seconds as i64),
                    priority: bigquery.priority.as_ref().map(|p| format!("{p:?}")),
                    location: bigquery.location.clone(),
                    maximum_bytes_billed: bigquery.maximum_bytes_billed.map(|mb| mb as i64),
                    execution_project: bigquery.execution_project.clone(),
                    impersonate_service_account: bigquery.impersonate_service_account.clone(),
                    retries: bigquery.retries.map(|r| r as i64),
                    scopes: bigquery.scopes.clone(),
                    // Authentication details - these could be used to construct keyfile_json
                    client_id: Some(bigquery.client_id.clone()),
                    token_uri: Some(bigquery.token_uri.clone()),
                    // Keep other fields as None since they come from credentials or aren't in connection
                    threads: None,
                    profile_type: None,
                    schema: None,
                    method: None,
                    refresh_token: None,
                    client_secret: None,
                    token: None,
                    keyfile: None,
                    keyfile_json: None,
                    compute_region: None,
                    dataproc_batch: None,
                    dataproc_cluster_name: None,
                    dataproc_region: None,
                    gcs_bucket: None,
                    job_creation_timeout_seconds: None,
                    job_execution_timeout_seconds: None,
                    job_retries: None,
                    job_retry_deadline_seconds: None,
                    target_name: None,
                };
                bigquery_config.merge(connection_details);
            }
            (
                DbConfig::Bigquery(bigquery_config),
                models::Config::BigqueryConnectionV1(bigquery_v1),
            ) => {
                // Similar extraction for BigQuery V1 connections
                let connection_details = BigqueryDbConfig {
                    database: if !bigquery_v1.project_id.is_empty() {
                        Some(bigquery_v1.project_id.clone())
                    } else {
                        None
                    },
                    // V1 doesn't have timeout_seconds, but has job_execution_timeout_seconds
                    job_execution_timeout_seconds: bigquery_v1
                        .job_execution_timeout_seconds
                        .map(|t| t as i64),
                    priority: bigquery_v1.priority.as_ref().map(|p| format!("{p:?}")),
                    location: bigquery_v1.location.clone(),
                    maximum_bytes_billed: bigquery_v1.maximum_bytes_billed.map(|mb| mb as i64),
                    execution_project: bigquery_v1.execution_project.clone(),
                    impersonate_service_account: bigquery_v1.impersonate_service_account.clone(),
                    retries: bigquery_v1.retries.map(|r| r as i64),
                    scopes: bigquery_v1.scopes.clone(),
                    gcs_bucket: bigquery_v1.gcs_bucket.clone(),
                    dataproc_region: bigquery_v1.dataproc_region.clone(),
                    dataproc_cluster_name: bigquery_v1.dataproc_cluster_name.clone(),
                    job_retry_deadline_seconds: bigquery_v1
                        .job_retry_deadline_seconds
                        .map(|jrd| jrd as i64),
                    job_creation_timeout_seconds: bigquery_v1
                        .job_creation_timeout_seconds
                        .map(|jct| jct as i64),
                    // Authentication details - these are Option<String> so we directly assign them
                    client_id: bigquery_v1.client_id.clone(),
                    token_uri: bigquery_v1.token_uri.clone(),
                    // Keep other fields as None
                    threads: None,
                    profile_type: None,
                    schema: None,
                    method: None,
                    refresh_token: None,
                    client_secret: None,
                    token: None,
                    keyfile: None,
                    keyfile_json: None,
                    compute_region: None,
                    dataproc_batch: None,
                    timeout_seconds: None,
                    job_retries: None,
                    target_name: None,
                };
                bigquery_config.merge(connection_details);
            }
            (DbConfig::Redshift(redshift_config), models::Config::RedshiftConnection(redshift)) => {
                // Extract all available connection details for Redshift
                let connection_details = RedshiftDbConfig {
                    host: if !redshift.hostname.is_empty() {
                        Some(redshift.hostname.clone())
                    } else {
                        None
                    },
                    database: if !redshift.dbname.is_empty() {
                        Some(redshift.dbname.clone())
                    } else {
                        None
                    },
                    port: redshift.port.map(|p| StringOrInteger::Integer(p as i64)),
                    retries: redshift.retries.map(|r| r as i64),
                    ..Default::default()
                };
                redshift_config.merge(connection_details);
            }
            (DbConfig::Databricks(_), models::Config::DatabricksConnection(_)) => {
                // TODO: Implement Databricks merging when needed
                log::debug!("Databricks connection merging not yet implemented");
            }
            _ => {
                log::warn!("Adapter type mismatch between credential and connection");
            }
        }
    }

    Some(base_config)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserCredential {
    pub id: u64,
    pub account_id: u64,
    pub user_id: u64,
    pub project_id: u64,
    pub credentials_id: u64,
    pub state: u64,
    pub created_at: String,
    pub updated_at: String,
    pub credentials: CredentialDetails,
    pub project: ProjectDetails,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CredentialDetails {
    pub id: u64,
    pub account_id: u64,
    pub project_id: u64,
    #[serde(rename = "type")]
    pub adapter_type: String,
    pub state: u64,
    pub threads: Option<u64>,
    pub schema: Option<String>,
    pub target_name: Option<String>,
    pub username: Option<String>,
    pub is_configured_for_oauth: Option<bool>,
    pub has_refresh_token: Option<bool>,
    pub adapter_version: String,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectDetails {
    pub id: u64,
    pub name: String,
    pub account_id: u64,
    pub description: Option<String>,
    pub connection_id: u64,
    pub connection: ConnectionDetails,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionDetails {
    pub id: u64,
    pub account_id: u64,
    pub project_id: u64,
    pub name: String,
    #[serde(rename = "type")]
    pub connection_type: String,
    pub adapter_version: String,
    pub created_by_id: u64,
    pub created_by_service_token_id: Option<u64>,
    pub details: serde_json::Map<String, serde_json::Value>,
    pub state: u64,
    pub oauth_redirect_uri: Option<String>,
}

pub struct DbtCloudClient;

impl DbtCloudClient {
    /// Parse the active cloud project from dbt_cloud.yml based on active-project setting
    pub fn parse_active_cloud_project() -> FsResult<Option<CloudProject>> {
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
            log::info!(
                "dbt_cloud.yml not found at {}",
                dbt_cloud_config_path.display()
            );
            return Ok(None);
        }

        // Read and parse the dbt_cloud.yml file
        let content = fs::read_to_string(&dbt_cloud_config_path)?;
        let config: DbtCloudYml = dbt_serde_yaml::from_str(&content)
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to parse dbt_cloud.yml: {}", e))?;

        // Get the active project ID from context
        let active_project_id = &config.context.active_project;

        // Find the project that matches the active project ID
        let active_project = config
            .projects
            .into_iter()
            .find(|project| project.project_id == *active_project_id);

        if active_project.is_none() {
            log::warn!("No project found with active project ID: {active_project_id}");
        }

        Ok(active_project)
    }

    /// Get current user ID from dbt Cloud API
    pub async fn get_current_user_id(base_url: &str) -> FsResult<u64> {
        let cloud_project = match Self::parse_active_cloud_project()? {
            Some(project) => project,
            None => {
                return Err(fs_err!(
                    ErrorCode::IoError,
                    "No active cloud project configuration found"
                ));
            }
        };

        // Configure the generated client
        let configuration = Configuration {
            base_path: base_url.to_string(),
            user_agent: Some("dbt-sa/1.0".to_string()),
            client: reqwest::Client::new(),
            basic_auth: None,
            oauth_access_token: None,
            bearer_access_token: Some(cloud_project.token_value),
            api_key: None,
        };

        // Call the generated API
        let whoami_response = whoami_api::whoami(&configuration).await.map_err(|e| {
            fs_err!(
                ErrorCode::IoError,
                "Failed to make whoami request to dbt Cloud API: {}",
                e
            )
        })?;

        if !whoami_response.status.is_success {
            return Err(fs_err!(
                ErrorCode::IoError,
                "Whoami API returned error: {}",
                whoami_response.status.user_message
            ));
        }
        Ok(whoami_response.data.user.id)
    }

    /// Get the DbConfig for a specific credential
    pub async fn get_credential_db_config(
        base_url: &str,
        project_id: Option<&str>,
        adapter_type: Option<&str>,
    ) -> FsResult<Option<DbConfig>> {
        let cloud_project = match Self::parse_active_cloud_project()? {
            Some(project) => project,
            None => {
                return Err(fs_err!(
                    ErrorCode::IoError,
                    "No active cloud project configuration found"
                ));
            }
        };

        // Get the current user ID first
        let user_id = Self::get_current_user_id(base_url).await?;

        // Parse project_id as integer if provided for filtering
        let project_id_int: Option<i32> = if let Some(project_id) = project_id {
            Some(project_id.parse().map_err(|e| {
                fs_err!(
                    ErrorCode::InvalidArgument,
                    "Invalid project ID '{}': {}",
                    project_id,
                    e
                )
            })?)
        } else {
            None
        };

        // Configure the generated client
        let configuration = Configuration {
            base_path: base_url.to_string(),
            user_agent: Some("dbt-sa/1.0".to_string()),
            client: reqwest::Client::new(),
            basic_auth: None,
            oauth_access_token: None,
            bearer_access_token: Some(cloud_project.token_value),
            api_key: None,
        };

        // Call the generated API
        let response = users_api::list_user_credentials(&configuration, user_id as i32)
            .await
            .map_err(|e| {
                fs_err!(
                    ErrorCode::IoError,
                    "Failed to fetch user credentials: {}",
                    e
                )
            })?;

        if !response.status.is_success {
            return Err(fs_err!(
                ErrorCode::IoError,
                "User credentials API returned error: {}",
                response.status.user_message
            ));
        }

        // Find the first matching credential
        let matching_credential = response.data.iter().find(|user_cred| {
            // Filter by state=1 (active)
            let state = user_cred.state.unwrap_or(match &*user_cred.credentials {
                models::UserCredentialsResponseCredentials::PostgresCredentials(postgres) => {
                    postgres.state
                }
                models::UserCredentialsResponseCredentials::SnowflakeCredentials(snowflake) => {
                    snowflake.state
                }
                models::UserCredentialsResponseCredentials::BigqueryCredentials(bigquery) => {
                    bigquery.state
                }
                models::UserCredentialsResponseCredentials::RedshiftCredentials(redshift) => {
                    redshift.state
                }
                models::UserCredentialsResponseCredentials::DbtAdapterCredentials(adapter) => {
                    adapter.state
                }
            });

            let mut basic_filter = state == 1;

            // If project_id is specified, also filter by that
            if let Some(target_project_id) = project_id_int {
                basic_filter = basic_filter && user_cred.project_id == Some(target_project_id);
            }

            // If adapter_type is specified, also filter by that
            if let Some(adapter) = adapter_type {
                let cred_info = CredentialInfo::from(*user_cred);
                basic_filter = basic_filter && cred_info.adapter_type.eq_ignore_ascii_case(adapter);
            }

            basic_filter
        });

        if let Some(credential) = matching_credential {
            // Fetch connection details if available
            let connection_response = if let Some(connection_id) = credential.project.connection_id
            {
                match connections_api::retrieve_account_connection(
                    &configuration,
                    credential.project.account_id,
                    connection_id,
                )
                .await
                {
                    Ok(response) => Some(response),
                    Err(e) => {
                        log::warn!(
                            "Failed to fetch connection details for connection_id {connection_id}: {e}"
                        );
                        None
                    }
                }
            } else {
                None
            };

            let connection_config = connection_response.as_ref().map(|r| &*r.data.config);

            // Create merged DbConfig from both credential and connection data
            match create_merged_db_config(credential, connection_config) {
                Some(merged_config) => Ok(Some(merged_config)),
                None => {
                    log::warn!(
                        "Unable to create DbConfig from user credential and connection data"
                    );
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }
}
