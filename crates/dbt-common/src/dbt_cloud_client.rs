use crate::adapter_config::{ConfigMap, common::FieldValue};
use crate::{ErrorCode, FsResult, fs_err};
use dbt_cloud_api::{
    apis::{configuration::Configuration, connections_api, users_api, whoami_api},
    models,
};

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

fn user_credential_to_config_map(user_cred: &models::UserCredentialsResponse) -> ConfigMap {
    let mut config = ConfigMap::new();

    // Add threads for all adapters
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
    config.insert("threads".to_string(), FieldValue::Number(threads as usize));

    match &*user_cred.credentials {
        models::UserCredentialsResponseCredentials::PostgresCredentials(postgres) => {
            // Map to PostgresFieldId fields: Host, User, Password, Port, DbName, Schema
            config.insert(
                "user".to_string(),
                FieldValue::String(postgres.username.clone()),
            );
            config.insert(
                "schema".to_string(),
                FieldValue::String(postgres.default_schema.clone()),
            );
            // Note: Host, Password, Port, DbName are not available in the credential response
        }
        models::UserCredentialsResponseCredentials::SnowflakeCredentials(snowflake) => {
            // Map to SnowflakeFieldId fields: Account, User, Role, Database, Warehouse, Schema
            if let Some(user) = &snowflake.user {
                config.insert("user".to_string(), FieldValue::String(user.clone()));
            }
            if let Some(role) = &snowflake.role {
                config.insert("role".to_string(), FieldValue::String(role.clone()));
            }
            if let Some(database) = &snowflake.database {
                config.insert("database".to_string(), FieldValue::String(database.clone()));
            }
            if let Some(warehouse) = &snowflake.warehouse {
                config.insert(
                    "warehouse".to_string(),
                    FieldValue::String(warehouse.clone()),
                );
            }
            config.insert(
                "schema".to_string(),
                FieldValue::String(snowflake.schema.clone()),
            );
            // Note: Account field is intentionally NOT set here because:
            // 1. The API only provides account_id (integer), not the account name (string)
            // 2. The account name is required for Snowflake connections (e.g. "myaccount.snowflakecomputing.com")
            // 3. By leaving this field unset, the user will be prompted to enter it during profile setup
            log::debug!(
                "Snowflake account field not set - user will be prompted during profile setup"
            );
        }
        models::UserCredentialsResponseCredentials::BigqueryCredentials(bigquery) => {
            // Map to BigQueryFieldId fields: Method, Keyfile, Project, Dataset
            config.insert(
                "dataset".to_string(),
                FieldValue::String(bigquery.schema.clone()),
            );
            // Note: Method, Keyfile, Project are not available in the credential response
        }
        models::UserCredentialsResponseCredentials::RedshiftCredentials(redshift) => {
            // Map to RedshiftFieldId fields: Host, User, Password, DbName, Schema
            if let Some(username) = &redshift.username {
                config.insert("user".to_string(), FieldValue::String(username.clone()));
            }
            config.insert(
                "schema".to_string(),
                FieldValue::String(redshift.default_schema.clone()),
            );
            // Note: Host, Password, DbName are not available in the credential response
        }
        models::UserCredentialsResponseCredentials::DbtAdapterCredentials(_adapter) => {
            // For databricks: Schema, Host, HttpPath, Catalog, Token, ClientId, ClientSecret
            // Note: Most fields are not available in the credential response
            // This would need additional API calls or different credential structure
        }
    }

    config
}

/// Fetch connection details from the connections API
async fn fetch_connection_details(
    configuration: &Configuration,
    account_id: i32,
    connection_id: i32,
) -> FsResult<ConfigMap> {
    let response =
        connections_api::retrieve_account_connection(configuration, account_id, connection_id)
            .await
            .map_err(|e| {
                fs_err!(
                    ErrorCode::IoError,
                    "Failed to fetch connection details: {}",
                    e
                )
            })?;

    let mut config = ConfigMap::new();
    // Match on the Config enum to extract connection-specific fields
    match &*response.data.config {
        models::Config::SnowflakeConnection(snowflake) => {
            // Extract Snowflake connection details
            if !snowflake.account.is_empty() {
                config.insert(
                    "account".to_string(),
                    FieldValue::String(snowflake.account.clone()),
                );
            }
            if !snowflake.database.is_empty() {
                config.insert(
                    "database".to_string(),
                    FieldValue::String(snowflake.database.clone()),
                );
            }
            if !snowflake.warehouse.is_empty() {
                config.insert(
                    "warehouse".to_string(),
                    FieldValue::String(snowflake.warehouse.clone()),
                );
            }
            if let Some(role) = &snowflake.role {
                config.insert("role".to_string(), FieldValue::String(role.clone()));
            }
        }
        models::Config::PostgresConnection(postgres) => {
            // Extract Postgres connection details
            if !postgres.hostname.is_empty() {
                config.insert(
                    "host".to_string(),
                    FieldValue::String(postgres.hostname.clone()),
                );
            }
            if !postgres.dbname.is_empty() {
                config.insert(
                    "dbname".to_string(),
                    FieldValue::String(postgres.dbname.clone()),
                );
            }
            if let Some(port) = postgres.port {
                config.insert("port".to_string(), FieldValue::Number(port as usize));
            }
        }
        models::Config::RedshiftConnection(_redshift) => {
            // Extract Redshift connection details (similar to Postgres)
            log::debug!("Redshift connection details found");
        }
        models::Config::BigqueryConnection(bigquery) => {
            // Extract BigQuery connection details
            if !bigquery.project_id.is_empty() {
                config.insert(
                    "project".to_string(),
                    FieldValue::String(bigquery.project_id.clone()),
                );
            }
        }
        models::Config::BigqueryConnectionV1(bigquery_v1) => {
            // Extract BigQuery V1 connection details
            if !bigquery_v1.project_id.is_empty() {
                config.insert(
                    "project".to_string(),
                    FieldValue::String(bigquery_v1.project_id.clone()),
                );
            }
        }
        models::Config::DatabricksConnection(_databricks) => {
            // Extract Databricks connection details
            log::debug!("Databricks connection details found");
        }
    }
    Ok(config)
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

    /// Get the ConfigMap for a specific credential
    pub async fn get_credential_config_map(
        base_url: &str,
        project_id: Option<&str>,
        adapter_type: Option<&str>,
    ) -> FsResult<Option<ConfigMap>> {
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
            // Start with user credential config
            let mut merged_config = user_credential_to_config_map(credential);

            // Fetch connection details if available
            if let Some(connection_id) = credential.project.connection_id {
                match fetch_connection_details(
                    &configuration,
                    credential.project.account_id,
                    connection_id,
                )
                .await
                {
                    Ok(connection_config) => {
                        for (key, value) in connection_config {
                            merged_config.insert(key, value);
                        }
                    }
                    Err(e) => {
                        log::warn!(
                            "Failed to fetch connection details for connection_id {connection_id}: {e}"
                        );
                    }
                }
            }

            Ok(Some(merged_config))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_credential_config_map() {
        let base_url = "https://cloud.getdbt.com";

        println!("Testing get_credential_config_map:");

        // Try different project IDs that might have credentials
        let test_projects = ["409574", "323280", "297117"];

        for project_id in &test_projects {
            println!("\nTesting project: {project_id}");
            match DbtCloudClient::get_credential_config_map(base_url, Some(project_id), None).await
            {
                Ok(Some(config_map)) => {
                    println!("  Config map found with {} fields", config_map.len());
                    for (key, value) in &config_map {
                        println!("    {key}: {value:?}");
                    }
                }
                Ok(None) => {
                    println!("  No credentials found for project {project_id}");
                }
                Err(e) => {
                    println!("  Error: {e}");
                }
            }
        }
    }

    #[tokio::test]
    async fn test_get_current_user_id() {
        let base_url = "https://cloud.getdbt.com";

        match DbtCloudClient::get_current_user_id(base_url).await {
            Ok(user_id) => {
                println!("Current user ID: {user_id}");
                assert!(user_id > 0);
            }
            Err(e) => {
                println!("Error getting user ID: {e}");
            }
        }
    }

    #[test]
    fn test_parse_active_cloud_project() {
        match DbtCloudClient::parse_active_cloud_project() {
            Ok(Some(project)) => {
                println!("Found active cloud project: {}", project.project_name);
                println!("Project ID: {}", project.project_id);
                println!("Account ID: {}", project.account_id);
                println!("Account Host: {}", project.account_host);
                println!("Token starts with: {}...", &project.token_value[..10]);

                // Verify it matches the active project ID
                assert_eq!(project.project_id, "409574");
                assert_eq!(project.project_name, "Analytics");
            }
            Ok(None) => {
                println!("No active cloud project configuration found");
            }
            Err(e) => {
                println!("Error parsing active cloud project: {e}");
            }
        }
    }
}
