use chrono::{DateTime, Utc};
use dbt_common::{ErrorCode, FsResult, fs_err, stdfs};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::{collections::BTreeMap, path::Path};

// Type aliases for clarity
type JsonValue = serde_json::Value;

/// Metadata about the dbt run invocation.
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct RunResultsMetadata {
    pub dbt_schema_version: String,
    pub dbt_version: String,
    pub generated_at: DateTime<Utc>,
    pub invocation_id: String,
    /// Timestamp when the invocation started, if available.
    pub invocation_started_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

/// Timing information for a specific phase of a node execution.
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TimingInfo {
    pub name: String, // e.g., "compile", "execute"
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}

/// Represents the batch results structure within a RunResult.
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct BatchResults {
    pub successful: Vec<(String, String)>,
    pub failed: Vec<(String, String)>,
}

/// Result object for a single node execution.
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RunResult {
    /// Final status of the node execution (e.g., "success", "error", "skipped", "pass", "fail").
    pub status: String,
    /// List of timing information for different phases.
    pub timing: Vec<TimingInfo>,
    /// ID of the thread that executed the node.
    pub thread_id: String,
    /// Total execution time for the node in seconds.
    pub execution_time: f64,
    /// Adapter-specific response information.
    pub adapter_response: BTreeMap<String, JsonValue>,
    /// Execution message (e.g., error message).
    pub message: Option<String>,
    /// Information about failures (often used for tests).
    pub failures: Option<i64>,
    /// Unique identifier for the dbt node.
    pub unique_id: String,
    /// Indicates if the node was compiled.
    pub compiled: Option<bool>,
    /// Compiled SQL code for the node.
    pub compiled_code: Option<String>,
    /// Fully qualified relation name in the database.
    pub relation_name: Option<String>,
    /// Results specific to batch processing, if applicable.
    #[serde(default)]
    pub batch_results: Option<BatchResults>,
}

/// Arguments passed to the dbt command.
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RunResultsArgs {
    /// The specific dbt command executed (e.g., "run", "test").
    pub command: String,
    /// Alias for the command executed.
    pub which: String,
    /// Capture any other arguments passed via CLI using flatten
    #[serde(flatten)]
    pub other: BTreeMap<String, JsonValue>,
}

/// Represents the structure of the run_results.json artifact.
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RunResultsArtifact {
    /// Metadata about the dbt invocation.
    pub metadata: RunResultsMetadata,
    /// List of results for each executed node.
    pub results: Vec<RunResult>,
    /// Total elapsed time for the entire dbt invocation in seconds.
    pub elapsed_time: f64,
    /// Arguments passed to the dbt command.
    pub args: RunResultsArgs,
}

impl RunResultsArtifact {
    pub fn from_file(path: &Path) -> FsResult<Self> {
        let run_results = stdfs::read_to_string(path).map_err(|_| {
            fs_err!(
                ErrorCode::IoError,
                "Failed to read run_results.json from {}",
                path.display()
            )
        })?;
        let run_results: RunResultsArtifact = serde_json::from_str(&run_results).map_err(|_| {
            fs_err!(
                ErrorCode::FileNotFound,
                "Failed to parse run_results.json at {}",
                path.display()
            )
        })?;
        Ok(run_results)
    }
}
