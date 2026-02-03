//! Event schemas for the time-machine recording system.
//!
//! This module defines the events captured during adapter execution,
//! enabling cross-version artifact compatibility testing.

use serde::{Deserialize, Serialize};

use super::semantic::SemanticCategory;

/// Union of all recorded events from different adapter layers.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "source")]
pub enum RecordedEvent {
    /// From BridgeAdapter::call_method
    AdapterCall(AdapterCallEvent),
    /// From MetadataAdapter
    MetadataCall(MetadataCallEvent),
    /// SAO skip event
    Sao(SaoEvent),
}

impl RecordedEvent {
    /// Get the node/caller ID for this event
    pub fn node_id(&self) -> &str {
        match self {
            RecordedEvent::AdapterCall(e) => &e.node_id,
            RecordedEvent::MetadataCall(e) => &e.caller_id,
            RecordedEvent::Sao(e) => &e.node_id,
        }
    }

    /// Get the sequence number for this event
    pub fn seq(&self) -> u32 {
        match self {
            RecordedEvent::AdapterCall(e) => e.seq,
            RecordedEvent::MetadataCall(e) => e.seq,
            RecordedEvent::Sao(_) => 0,
        }
    }

    /// Get the timestamp in nanoseconds since recording start
    pub fn timestamp_ns(&self) -> u64 {
        match self {
            RecordedEvent::AdapterCall(e) => e.timestamp_ns,
            RecordedEvent::MetadataCall(e) => e.timestamp_ns,
            RecordedEvent::Sao(e) => e.timestamp_ns,
        }
    }
}

/// An adapter call event captured from BridgeAdapter::call_method.
///
/// These are synchronous calls made from Jinja templates via `adapter.xxx()`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdapterCallEvent {
    /// Node ID from TARGET_UNIQUE_ID in Jinja state, or "global" for non-node calls
    pub node_id: String,
    /// Monotonic sequence number within this node
    pub seq: u32,
    /// Method name (e.g., "execute", "get_relation", "drop_relation")
    pub method: String,
    /// Semantic category for dependency analysis
    pub semantic_category: SemanticCategory,
    /// Serialized args
    pub args: serde_json::Value,
    /// Serialized result
    pub result: serde_json::Value,
    /// Whether the call succeeded
    pub success: bool,
    /// Error message if the call failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Timestamp in nanoseconds since recording start
    pub timestamp_ns: u64,
}

/// A metadata adapter call event captured from MetadataAdapter methods.
///
/// These are typically async calls for things like for bulk schema download, relation listing, and freshness checks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataCallEvent {
    /// Caller identifier (unique_id of the node or phase name like "pre_compile")
    pub caller_id: String,
    /// Monotonic sequence number within this caller
    pub seq: u32,
    /// Method name (e.g., "list_relations_schemas", "freshness")
    pub method: String,
    /// Semantic category for dependency analysis
    pub semantic_category: SemanticCategory,
    /// Structured arguments specific to each method type
    pub args: MetadataCallArgs,
    /// Serialized result
    pub result: serde_json::Value,
    /// Whether the call succeeded
    pub success: bool,
    /// Error message if the call failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Duration of the async call in milliseconds
    pub duration_ms: u64,
    /// Timestamp in nanoseconds since recording start
    pub timestamp_ns: u64,
}

/// SAO skip event.
///
/// Recorded when a node is skipped due to a cache hit. This enables
/// replay to skip execution for nodes that were also skipped during recording.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SaoEvent {
    /// Node unique_id
    pub node_id: String,
    /// Why the node was skipped
    pub status: SaoStatus,
    /// Human message explaining the skip reason
    pub message: String,
    /// The hash of the node at the time of the skip decision
    pub stored_hash: String,
    /// Timestamp in nanoseconds since recording start
    pub timestamp_ns: u64,
}

/// SAO status variants representing different skip reasons.
/// Subset of NodeStatus
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SaoStatus {
    ReusedNoChanges,
    ReusedStillFresh {
        freshness_seconds: u64,
        last_updated_seconds: u64,
    },
    ReusedStillFreshNoChanges,
}

impl From<SaoStatus> for dbt_common::stats::NodeStatus {
    fn from(status: SaoStatus) -> Self {
        use dbt_common::stats::NodeStatus;
        match status {
            SaoStatus::ReusedNoChanges => {
                NodeStatus::ReusedNoChanges("Reused (SAO replay)".to_string())
            }
            SaoStatus::ReusedStillFresh {
                freshness_seconds,
                last_updated_seconds,
            } => NodeStatus::ReusedStillFresh(
                "Reused (SAO replay)".to_string(),
                freshness_seconds,
                last_updated_seconds,
            ),
            SaoStatus::ReusedStillFreshNoChanges => {
                NodeStatus::ReusedStillFreshNoChanges("Reused (SAO replay)".to_string())
            }
        }
    }
}

/// Structured arguments for MetadataAdapter method calls.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum MetadataCallArgs {
    /// Arguments for list_relations_schemas
    ListRelationsSchemas {
        /// Optional unique_id of the requesting node
        #[serde(skip_serializing_if = "Option::is_none")]
        unique_id: Option<String>,
        /// Execution phase (if available)
        #[serde(skip_serializing_if = "Option::is_none")]
        phase: Option<String>,
        /// List of relation FQNs being queried
        relations: Vec<String>,
    },
    /// Arguments for list_relations_in_parallel
    ListRelationsInParallel {
        /// List of (catalog, schema) pairs
        db_schemas: Vec<CatalogSchema>,
    },
    /// Arguments for freshness checks
    Freshness {
        /// List of relation FQNs being checked
        relations: Vec<String>,
    },
    /// Arguments for list_user_defined_functions
    ListUserDefinedFunctions {
        /// Map of catalog -> schemas
        catalog_schemas: Vec<CatalogSchemas>,
    },
    /// Arguments for list_relations_schemas_by_patterns
    ListRelationsSchemasByPatterns {
        /// Relation patterns being matched
        patterns: Vec<String>,
    },
    /// Arguments for create_schemas_if_not_exists
    CreateSchemasIfNotExists {
        /// List of (catalog, schema, unique_id) to create
        catalog_schemas: Vec<(String, String, String)>,
    },
}

/// A (catalog, schema) pair for serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogSchema {
    pub catalog: String,
    pub schema: String,
}

/// A catalog with its schemas for serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogSchemas {
    pub catalog: String,
    pub schemas: Vec<String>,
}

/// Header metadata written at the start of a recording session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordingHeader {
    /// Format version for forward compatibility
    pub format_version: u32,
    /// Fusion engine version that created this recording
    pub fusion_version: String,
    /// Adapter type (snowflake, bigquery, etc.)
    pub adapter_type: String,
    /// ISO timestamp of recording started
    pub recorded_at: String,
    /// Invocation ID for this run
    pub invocation_id: String,
    /// The command that was executed (e.g., "dbt build --select ...")
    /// Helps disambiguate recordings when multiple commands were run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invocation_command: Option<String>,
    /// Optional additional metadata
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub metadata: serde_json::Map<String, serde_json::Value>,
}

impl RecordingHeader {
    /// Current format version
    pub const FORMAT_VERSION: u32 = 1;

    /// Create a new recording header
    pub fn new(adapter_type: impl Into<String>, invocation_id: impl Into<String>) -> Self {
        Self {
            format_version: Self::FORMAT_VERSION,
            fusion_version: env!("CARGO_PKG_VERSION").to_string(),
            adapter_type: adapter_type.into(),
            recorded_at: chrono::Utc::now().to_rfc3339(),
            invocation_id: invocation_id.into(),
            invocation_command: None,
            metadata: serde_json::Map::new(),
        }
    }

    /// Set the invocation command for this recording header.
    pub fn with_invocation_command(mut self, command: impl Into<String>) -> Self {
        self.invocation_command = Some(command.into());
        self
    }
}

/// Index entry for a node's events in the recording.
///
/// Note: Events are written in arrival order (interleaved across nodes),
// TODO: Record vector of offsets for optimized replay
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeIndex {
    /// Number of events recorded for this node
    pub event_count: u32,
    /// Timestamp (ns) of the first event for this node
    pub first_timestamp_ns: u64,
    /// Timestamp (ns) of the last event for this node
    pub last_timestamp_ns: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_serialization_roundtrip() {
        let event = RecordedEvent::AdapterCall(AdapterCallEvent {
            node_id: "model.my_project.orders".to_string(),
            seq: 0,
            method: "execute".to_string(),
            semantic_category: SemanticCategory::Write,
            args: serde_json::json!(["CREATE TABLE orders ..."]),
            result: serde_json::json!({"rows_affected": 0}),
            success: true,
            error: None,
            timestamp_ns: 12345,
        });

        let json = serde_json::to_string(&event).unwrap();
        let parsed: RecordedEvent = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.node_id(), "model.my_project.orders");
        assert_eq!(parsed.seq(), 0);
    }

    #[test]
    fn test_header_creation() {
        let header = RecordingHeader::new("snowflake", "abc-123");
        assert_eq!(header.format_version, RecordingHeader::FORMAT_VERSION);
        assert_eq!(header.adapter_type, "snowflake");
        assert_eq!(header.invocation_id, "abc-123");
    }

    #[test]
    fn test_sao_event_serialization_roundtrip() {
        let event = RecordedEvent::Sao(SaoEvent {
            node_id: "model.my_project.orders".to_string(),
            status: SaoStatus::ReusedNoChanges,
            message: "No new changes on any upstreams".to_string(),
            stored_hash: "abc123".to_string(),
            timestamp_ns: 12345,
        });

        let json = serde_json::to_string(&event).unwrap();
        let parsed: RecordedEvent = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.node_id(), "model.my_project.orders");
        assert_eq!(parsed.seq(), 0);
        assert_eq!(parsed.timestamp_ns(), 12345);
    }

    #[test]
    fn test_sao_status_with_freshness() {
        let event = RecordedEvent::Sao(SaoEvent {
            node_id: "model.my_project.orders".to_string(),
            status: SaoStatus::ReusedStillFresh {
                freshness_seconds: 3600,
                last_updated_seconds: 1800,
            },
            message: "Still within freshness period".to_string(),
            stored_hash: "def456".to_string(),
            timestamp_ns: 67890,
        });

        let json = serde_json::to_string(&event).unwrap();
        let parsed: RecordedEvent = serde_json::from_str(&json).unwrap();

        if let RecordedEvent::Sao(sao) = parsed {
            if let SaoStatus::ReusedStillFresh {
                freshness_seconds,
                last_updated_seconds,
            } = sao.status
            {
                assert_eq!(freshness_seconds, 3600);
                assert_eq!(last_updated_seconds, 1800);
            } else {
                panic!("Expected ReusedStillFresh status");
            }
        } else {
            panic!("Expected Sao event");
        }
    }
}
