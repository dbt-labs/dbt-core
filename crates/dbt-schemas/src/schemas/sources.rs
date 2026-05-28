use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::BTreeMap;

use super::{
    TimingInfo,
    common::{FreshnessDefinition, FreshnessStatus},
};

/// Metadata about the dbt run invocation.
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct FreshnessResultsMetadata {
    pub dbt_schema_version: String,
    pub dbt_version: String,
    pub generated_at: DateTime<Utc>,
    pub invocation_id: String,
    /// Timestamp when the invocation started, if available.
    pub invocation_started_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct FreshnessResultsNode {
    pub unique_id: String,
    pub max_loaded_at: DateTime<Utc>,
    pub snapshotted_at: DateTime<Utc>,
    pub max_loaded_at_time_ago_in_s: f64,
    pub status: FreshnessStatus,
    pub criteria: FreshnessDefinition,
    pub adapter_response: BTreeMap<String, String>,
    pub timing: Vec<TimingInfo>,
    pub thread_id: String,
    pub execution_time: f64,
}

/// Represents the structure of the sources.json artifact.
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct FreshnessResultsArtifact {
    /// Metadata about the dbt invocation.
    pub metadata: FreshnessResultsMetadata,
    /// List of results for each executed node.
    pub results: Vec<FreshnessResultsNode>,
    /// Total elapsed time for the entire dbt invocation in seconds.
    pub elapsed_time: f64,
}
