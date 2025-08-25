use std::collections::BTreeMap;

use dbt_serde_yaml::{JsonSchema, Value};
#[cfg(test)]
use fake::Dummy;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use super::process::ProcessInfo;

#[skip_serializing_none]
#[cfg_attr(test, derive(Dummy))]
#[derive(Debug, Clone, Serialize, Deserialize, Default, JsonSchema, PartialEq)]
pub struct InvocationMetrics {
    pub total_errors: Option<u64>,
    pub total_warnings: Option<u64>,
    pub autofix_suggestions: Option<u64>,
}

#[skip_serializing_none]
#[cfg_attr(test, derive(Dummy))]
#[derive(Debug, Clone, Serialize, Deserialize, Default, JsonSchema, PartialEq)]
pub struct InvocationCloudAttributes {
    /// dbt Cloud Account ID from DBT_CLOUD_ACCOUNT_ID
    pub account_id: Option<String>,
    /// dbt Cloud Environment ID from DBT_CLOUD_ENVIRONMENT_ID
    pub environment_id: Option<String>,
    /// dbt Cloud Job ID from DBT_CLOUD_JOB_ID
    pub job_id: Option<String>,
    /// dbt Cloud Run ID from DBT_CLOUD_RUN_ID
    pub run_id: Option<String>,
    /// dbt Cloud Run Reason from DBT_CLOUD_RUN_REASON
    pub run_reason: Option<String>,
    /// dbt Cloud Run Reason Category from DBT_CLOUD_RUN_REASON_CATEGORY
    pub run_reason_category: Option<String>,
    /// dbt Cloud Run Trigger Category from DBT_CLOUD_RUN_TRIGGER_CATEGORY
    pub run_trigger_category: Option<String>,
    /// dbt Cloud Project ID from DBT_CLOUD_PROJECT_ID
    pub project_id: Option<String>,
}

#[skip_serializing_none]
#[cfg_attr(test, derive(Dummy))]
#[derive(Debug, Clone, Serialize, Deserialize, Default, JsonSchema, PartialEq)]
pub struct InvocationEvalArgs {
    /// The dbt command that was executed, e.g. "run", "test", "build"
    pub command: String,
    /// The profile directory to load the profiles from
    pub profiles_dir: Option<String>,
    /// The directory to install packages
    pub packages_install_path: Option<String>,
    /// dbt target, e.g. "dev", "prod"
    pub target: Option<String>,
    /// Profile name used for the invocation
    pub profile: Option<String>,
    /// Vars to pass to the jinja environment
    #[cfg_attr(
        test,
        dummy(expr = "BTreeMap::from([(\"key\".into(), Value::string(\"value\".into()))])")
    )]
    pub vars: BTreeMap<String, Value>,
    /// Limiting number of shown rows. None means no limit, run with --limit -1 to remove limit
    pub limit: Option<u64>,
    /// The number of threads to use
    pub num_threads: Option<u64>,
    /// yaml selector
    pub selector: Option<String>,
    /// Select nodes to operate on
    pub select: Vec<String>,
    /// Select nodes to exclude from selected nodes
    pub exclude: Vec<String>,
    /// Indirect selection mode
    pub indirect_selection: Option<String>,
    /// Show output keys
    pub output_keys: Vec<String>,
    /// Resource types to filter by
    pub resource_types: Vec<String>,
    /// Exclude nodes of a specific type
    pub exclude_resource_types: Vec<String>,
    /// Debug flag
    pub debug: bool,
    /// logging format
    pub log_format: String,
    /// minimum severity for console/log file
    pub log_level: Option<String>,
    /// 'log-path' for the current run, overriding 'DBT_LOG_PATH'.
    pub log_path: Option<String>,
    /// The output directory for all produced assets
    pub target_path: Option<String>,
    /// The directory to load the dbt project from
    pub project_dir: Option<String>,
    /// Suppress all non-error logging to stdout
    pub quiet: bool,
    /// Write JSON artifacts to disk
    pub write_json: bool,
    /// Write a catalog.json file to the target directory
    pub write_catalog: bool,

    // -- fields from the private branch
    pub update_deps: bool,
    pub replay_mode: Option<String>,
    pub replay_path: Option<String>,
    pub static_analysis: String,
    pub interactive: bool,
    pub task_cache_url: String,
    pub run_cache_mode: String,
    pub show_scans: bool,
    pub max_depth: u64,
    pub use_fqtn: bool,
    pub skip_unreferenced_table_check: bool,
    pub state: Option<String>,
    pub defer_state: Option<String>,
    pub connection: bool,
    pub warn_error: bool,
    #[cfg_attr(
        test,
        dummy(expr = "BTreeMap::from([(\"key\".into(), Value::string(\"value\".into()))])")
    )]
    pub warn_error_options: BTreeMap<String, Value>,
    pub version_check: bool,
    pub defer: Option<bool>,
    pub fail_fast: bool,
    pub empty: bool,
    pub sample: Option<String>,
    pub full_refresh: bool,
    pub favor_state: bool,
    pub refresh_sources: bool,
    pub send_anonymous_usage_stats: bool,
    pub check_all: bool,
}

impl InvocationCloudAttributes {
    /// Creates a new instance of `InvocationCloudAttributes` from environment variables.
    /// Missing variables will be set to `None`.
    pub fn from_env_lossy() -> Self {
        Self {
            account_id: std::env::var("DBT_CLOUD_ACCOUNT_ID").ok(),
            environment_id: std::env::var("DBT_CLOUD_ENVIRONMENT_ID").ok(),
            job_id: std::env::var("DBT_CLOUD_JOB_ID").ok(),
            run_id: std::env::var("DBT_CLOUD_RUN_ID").ok(),
            run_reason: std::env::var("DBT_CLOUD_RUN_REASON").ok(),
            run_reason_category: std::env::var("DBT_CLOUD_RUN_REASON_CATEGORY").ok(),
            run_trigger_category: std::env::var("DBT_CLOUD_RUN_TRIGGER_CATEGORY").ok(),
            project_id: std::env::var("DBT_CLOUD_PROJECT_ID").ok(),
        }
    }
}

#[skip_serializing_none]
#[cfg_attr(test, derive(Dummy))]
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct InvocationInfo {
    /// Unique identifier for the invocation
    pub invocation_id: String,

    /// Raw command string as executed
    pub raw_command: String,

    /// Structured evaluation arguments
    #[serde(flatten)]
    pub eval_args: InvocationEvalArgs,

    // The following process-wide attributes are duplicated for convenience
    /// Process attributes
    #[serde(flatten)]
    pub process_info: ProcessInfo,

    /// Cloud environment attributes
    #[serde(flatten)]
    pub cloud_args: InvocationCloudAttributes,

    /// Invocation aggregate metrics
    #[serde(flatten)]
    pub metrics: InvocationMetrics,
}
