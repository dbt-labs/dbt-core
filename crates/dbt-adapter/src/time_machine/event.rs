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
}

impl RecordedEvent {
    /// Get the node/caller ID for this event
    pub fn node_id(&self) -> &str {
        match self {
            RecordedEvent::AdapterCall(e) => &e.node_id,
            RecordedEvent::MetadataCall(e) => &e.caller_id,
        }
    }

    /// Get the sequence number for this event
    pub fn seq(&self) -> u32 {
        match self {
            RecordedEvent::AdapterCall(e) => e.seq,
            RecordedEvent::MetadataCall(e) => e.seq,
        }
    }

    /// Get the timestamp in nanoseconds since recording start
    pub fn timestamp_ns(&self) -> u64 {
        match self {
            RecordedEvent::AdapterCall(e) => e.timestamp_ns,
            RecordedEvent::MetadataCall(e) => e.timestamp_ns,
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
        /// Map of catalog -> schemas to create
        catalog_schemas: Vec<CatalogSchemas>,
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
            metadata: serde_json::Map::new(),
        }
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
}
