use chrono::{DateTime, Utc};
use dbt_common::FsResult;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::{collections::BTreeMap, path::Path, sync::Arc};

use crate::schemas::InternalDbtNodeAttributes;

use crate::schemas::serde::typed_struct_from_json_file;

// Type aliases for clarity
type YmlValue = dbt_serde_yaml::Value;

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

fn serialize_internal_dbt_node<S>(
    node: &Option<Arc<dyn InternalDbtNodeAttributes>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match node {
        Some(node) => node.serialize().serialize(serializer),
        None => serializer.serialize_none(),
    }
}

/// Result object for a single node execution.
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ContextRunResult {
    /// Final status of the node execution (e.g., "success", "error", "skipped", "pass", "fail").
    pub status: String,
    /// List of timing information for different phases.
    pub timing: Vec<TimingInfo>,
    /// ID of the thread that executed the node.
    pub thread_id: String,
    /// Total execution time for the node in seconds.
    pub execution_time: f64,
    /// Adapter-specific response information.
    pub adapter_response: BTreeMap<String, YmlValue>,
    /// Execution message (e.g., error message).
    pub message: Option<String>,
    /// Information about failures (often used for tests).
    pub failures: Option<i64>,
    /// The Node that was executed
    #[serde(serialize_with = "serialize_internal_dbt_node")]
    pub node: Option<Arc<dyn InternalDbtNodeAttributes>>,
    /// Unique identifier for the dbt node.
    pub unique_id: String,
    /// Results specific to batch processing, if applicable.
    #[serde(default)]
    pub batch_results: Option<BatchResults>,
}

impl From<ContextRunResult> for RunResultOutput {
    fn from(result: ContextRunResult) -> Self {
        let (unique_id, relation_name) = match result.node {
            Some(node) => (
                Some(node.common().unique_id.clone()),
                node.base().relation_name.clone(),
            ),
            None => (None, None),
        };

        // Stats is also used for non internal dbtNodes so if its none, we use stat.unique_id
        let unique_id = unique_id.unwrap_or(result.unique_id);

        RunResultOutput {
            status: result.status,
            timing: result.timing,
            thread_id: result.thread_id,
            execution_time: result.execution_time,
            adapter_response: result.adapter_response,
            message: result.message,
            failures: result.failures,
            unique_id,
            compiled: None, // TODO: Handle compiled i think its a deprecated field
            compiled_code: None, // TODO: Handle compiled_code i think its a deprecated field
            relation_name,
            batch_results: result.batch_results,
        }
    }
}

/// Result object for a single node execution.
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RunResultOutput {
    /// Final status of the node execution (e.g., "success", "error", "skipped", "pass", "fail").
    pub status: String,
    /// List of timing information for different phases.
    pub timing: Vec<TimingInfo>,
    /// ID of the thread that executed the node.
    pub thread_id: String,
    /// Total execution time for the node in seconds.
    pub execution_time: f64,
    /// Adapter-specific response information.
    pub adapter_response: BTreeMap<String, YmlValue>,
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
    pub __other__: BTreeMap<String, YmlValue>,
}

/// Represents the structure of the run_results.json artifact.
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RunResultsArtifact {
    /// Metadata about the dbt invocation.
    pub metadata: RunResultsMetadata,
    /// List of results for each executed node.
    pub results: Vec<RunResultOutput>,
    /// Total elapsed time for the entire dbt invocation in seconds.
    pub elapsed_time: f64,
    /// Arguments passed to the dbt command.
    pub args: RunResultsArgs,
}

impl RunResultsArtifact {
    pub fn from_file(path: &Path) -> FsResult<Self> {
        typed_struct_from_json_file(path)
    }
}
