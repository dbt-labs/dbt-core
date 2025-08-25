use dbt_serde_yaml::JsonSchema;
#[cfg(test)]
use fake::Dummy;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use super::phase::BuildPhase;

#[cfg_attr(test, derive(Dummy))]
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct NodeIdentifier {
    /// The unique ID of the node.
    pub unique_id: String,
    /// The name of the node.
    pub fqn: String,
}

// Custom display implementation is used to derive a readable/helpful span name.
// See display for the `NodeInfo` struct which relies on this.
impl std::fmt::Display for NodeIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.unique_id)
    }
}

/// TODO: this is a duplicate from `dbt-schemas` crate due to current circular dependency
/// remove redundancy when `dbt-schemas` crate is available
/// Represents the detailed status of a phase in the execution of a node.
#[cfg_attr(test, derive(Dummy))]
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Default, JsonSchema)]
pub enum NodeExecutionStatus {
    #[default]
    Success,
    Error,
    Skipped,
    Aborted, // e.g. interrupted by user.
    Reused,
    Passed, // For test nodes.
    Failed, // For test nodes.
}

/// Represents a node span within one of the build phases.
#[skip_serializing_none]
#[cfg_attr(test, derive(Dummy))]
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct NodeInfo {
    #[serde(flatten)]
    pub node_id: NodeIdentifier, // this is flattened into inner attrs, hence `node_id` and not `id`
    pub phase: BuildPhase,
    /// Final status of the node execution.
    pub status: Option<NodeExecutionStatus>,
    /// The number of resulting rows produced by the node, if recorded.
    pub num_rows: Option<u64>,
}

// Custom display implementation is used to derive a readable/helpful span name.
impl std::fmt::Display for NodeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} | {}", self.phase, self.node_id)
    }
}
