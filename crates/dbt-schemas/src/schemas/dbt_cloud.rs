use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

/// Represents a dbt Cloud project configuration
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbtCloudProject {
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

/// Represents the context section of the dbt Cloud configuration
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbtCloudContext {
    #[serde(rename = "active-project")]
    pub active_project: String,
    #[serde(rename = "active-host")]
    pub active_host: String,
    #[serde(rename = "defer-env-id")]
    pub defer_env_id: Option<String>,
}

/// Represents the top-level dbt Cloud configuration file
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbtCloudConfig {
    pub version: String,
    pub context: DbtCloudContext,
    pub projects: Vec<DbtCloudProject>,
}

impl DbtCloudConfig {
    pub fn get_project_by_id(&self, project_id: &str) -> Option<&DbtCloudProject> {
        self.projects.iter().find(|p| p.project_id == project_id)
    }
}
