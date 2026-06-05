use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::{collections::BTreeMap, sync::Arc};

use super::{
    InternalDbtNodeAttributes, TimingInfo,
    common::{FreshnessDefinition, FreshnessStatus},
};

fn serialize_internal_dbt_node<S>(
    node: &Option<Arc<dyn InternalDbtNodeAttributes>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match node {
        Some(node) => node.serialize_keep_none().serialize(serializer),
        None => serializer.serialize_none(),
    }
}

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

/// Result for a single source freshness check.
///
/// Used both for the sources.json artifact (where `node` is `None` and omitted) and
/// for the Jinja `on_run_end` context (where `node` is populated, matching dbt-core behavior).
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
    /// The source node that was checked for freshness.
    /// Populated when passed to `on_run_end` hooks; `None` (and omitted) in the artifact.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        skip_deserializing,
        serialize_with = "serialize_internal_dbt_node"
    )]
    pub node: Option<Arc<dyn InternalDbtNodeAttributes>>,
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
