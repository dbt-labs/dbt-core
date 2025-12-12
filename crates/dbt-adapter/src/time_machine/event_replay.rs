//! Replay infrastructure for time-machine recordings.
//!
//! This module provides functionality to load and navigate recorded adapter events.
//!
//! # Replay Modes
//!
//! The replay system supports two ordering modes:
//!
//! - **Strict**: Events must match in exact recorded sequence order.
//!   This is the default and ensures deterministic replay.
//!
//! - **Semantic**: Events are matched based on semantic constraints:
//!   - Write operations are barriers and must match in strict order
//!   - Read operations can match flexibly within a "segment" (between writes)
//!   - This enables replay tolerance for minor ordering variations
//!
//! The semantic mode treats the recording as a graph segmentation problem where
//! write operations create ordering constraints (barriers) while read operations
//! within a segment can be matched in any order.

use std::collections::HashMap;
use std::io::{self, BufRead, BufReader};
use std::path::Path;

use flate2::read::GzDecoder;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::event::{
    AdapterCallEvent, MetadataCallArgs, MetadataCallEvent, RecordedEvent, RecordingHeader,
};
use super::semantic::SemanticCategory;
use super::serde::values_match;

/// Compare two MetadataCallArgs for semantic equality.
///
/// This compares the structured arguments to ensure we match the correct
/// recorded event when replaying.
fn metadata_args_match(recorded: &MetadataCallArgs, actual: &MetadataCallArgs) -> bool {
    match (recorded, actual) {
        // For ListRelationsSchemas, check if recorded is a superset of actual.
        // If recorded contains all the relations requested in actual, we have
        // all the data needed to satisfy this request.
        (
            MetadataCallArgs::ListRelationsSchemas {
                relations: recorded_relations,
                ..
            },
            MetadataCallArgs::ListRelationsSchemas {
                relations: actual_relations,
                ..
            },
        ) => {
            // Check that every relation in actual is present in recorded (superset check)
            let recorded_set: std::collections::HashSet<_> = recorded_relations.iter().collect();
            actual_relations
                .iter()
                .all(|rel| recorded_set.contains(rel))
        }
        // For other types, the args are not volatile
        _ => {
            std::mem::discriminant(recorded) == std::mem::discriminant(actual) && {
                match (serde_json::to_value(recorded), serde_json::to_value(actual)) {
                    (Ok(r), Ok(a)) => values_match(&r, &a),
                    _ => false,
                }
            }
        }
    }
}

/// Replay ordering mode.
///
/// Controls how events are matched during replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayMode {
    /// Events must match in exact recorded sequence order.
    ///
    /// This is the most restrictive mode and enforces deterministic replay.
    /// Used if calls must all strictly be in the same order.
    #[default]
    Strict,

    /// Events are matched based on semantic constraints.
    ///
    /// Write operations act as barriers and must match in strict order.
    /// Read operations can match flexibly within a "segment" (between writes).
    ///
    /// This mode is useful when:
    /// - The code version being tested may have minor reordering of reads
    /// - You want to test semantic equivalence rather than exact call sequence
    Semantic,
}

impl ReplayMode {
    /// Returns true if this mode allows flexible matching of reads.
    pub fn allows_flexible_reads(&self) -> bool {
        matches!(self, Self::Semantic)
    }
}

impl std::fmt::Display for ReplayMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Strict => write!(f, "strict"),
            Self::Semantic => write!(f, "semantic"),
        }
    }
}

/// Errors that can occur during replay.
#[derive(Debug)]
pub enum ReplayError {
    /// IO error
    Io(io::Error),
    /// JSON parse error
    Json(serde_json::Error),
    /// Recording not found
    NotFound(String),
    /// Invalid recording format
    InvalidFormat(String),
    /// Method mismatch
    MethodMismatch { expected: String, actual: String },
    /// No recorded event found
    NoRecordedEvent {
        node_id: String,
        method: String,
        seq: u32,
    },
    /// Recorded call failed
    RecordedFailure(String),
}

impl std::fmt::Display for ReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {}", e),
            Self::Json(e) => write!(f, "JSON parse error: {}", e),
            Self::NotFound(path) => write!(f, "Recording not found at: {}", path),
            Self::InvalidFormat(msg) => write!(f, "Invalid recording format: {}", msg),
            Self::MethodMismatch { expected, actual } => {
                write!(
                    f,
                    "Method mismatch: expected '{}', got '{}'",
                    expected, actual
                )
            }
            Self::NoRecordedEvent {
                node_id,
                method,
                seq,
            } => {
                write!(
                    f,
                    "No recorded event for node '{}' method '{}' (seq {})",
                    node_id, method, seq
                )
            }
            Self::RecordedFailure(msg) => write!(f, "Recorded call failed: {}", msg),
        }
    }
}

impl std::error::Error for ReplayError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Json(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for ReplayError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<serde_json::Error> for ReplayError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

/// A loaded recording ready for replay.
#[derive(Debug)]
pub struct Recording {
    /// Recording metadata
    pub header: RecordingHeader,
    /// Adapter call events indexed by node_id, sorted by seq
    adapter_events_by_node: HashMap<String, Vec<AdapterCallEvent>>,
    /// Metadata call events indexed by caller_id, sorted by seq
    metadata_events_by_caller: HashMap<String, Vec<MetadataCallEvent>>,
    /// Current replay position per node for adapter calls (strict mode)
    adapter_positions: RwLock<HashMap<String, usize>>,
    /// Current replay position per caller for metadata calls (strict mode)
    metadata_positions: RwLock<HashMap<String, usize>>,
    /// Semantic mode state: tracks write barriers per node.
    /// Only writes are tracked - reads can be matched any number of times.
    semantic_adapter_state: RwLock<HashMap<String, SemanticReplayState>>,
    /// Semantic mode state for metadata calls
    semantic_metadata_state: RwLock<HashMap<String, SemanticReplayState>>,
}

/// State for semantic replay mode per node/caller.
///
/// In semantic mode:
/// - Writes are barriers that must match in order (tracked via segment_start)
/// - Reads can match any number of times within a segment (NOT tracked)
#[derive(Debug, Clone, Default)]
struct SemanticReplayState {
    /// Index of the first event in the current segment.
    /// A segment is the range of events between two write barriers.
    /// After consuming a write at index N, segment_start becomes N+1.
    segment_start: usize,
    /// Index of the last write barrier we've passed (for debugging/stats)
    last_write_barrier: Option<usize>,
}

impl Recording {
    /// Load a recording from disk.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ReplayError> {
        let path = path.as_ref();

        if !path.exists() {
            return Err(ReplayError::NotFound(path.display().to_string()));
        }

        // Load header
        let header_path = path.join("header.json");
        let header_content = std::fs::read_to_string(&header_path).map_err(|e| {
            ReplayError::InvalidFormat(format!("Failed to read header.json: {}", e))
        })?;
        let header: RecordingHeader = serde_json::from_str(&header_content)?;

        // Load events
        let events_path = path.join("events.ndjson.gz");
        let events = if events_path.exists() {
            load_events_gzipped(&events_path)?
        } else {
            let events_path = path.join("events.ndjson");
            if events_path.exists() {
                load_events_plain(&events_path)?
            } else {
                return Err(ReplayError::InvalidFormat(
                    "No events.ndjson.gz or events.ndjson found".to_string(),
                ));
            }
        };

        // Index events by node_id/caller_id
        let mut adapter_events_by_node: HashMap<String, Vec<AdapterCallEvent>> = HashMap::new();
        let mut metadata_events_by_caller: HashMap<String, Vec<MetadataCallEvent>> = HashMap::new();

        for event in events {
            match event {
                RecordedEvent::AdapterCall(adapter_event) => {
                    adapter_events_by_node
                        .entry(adapter_event.node_id.clone())
                        .or_default()
                        .push(adapter_event);
                }
                RecordedEvent::MetadataCall(metadata_event) => {
                    metadata_events_by_caller
                        .entry(metadata_event.caller_id.clone())
                        .or_default()
                        .push(metadata_event);
                }
            }
        }

        // Sort each node's events by seq
        for events in adapter_events_by_node.values_mut() {
            events.sort_by_key(|e| e.seq);
        }
        for events in metadata_events_by_caller.values_mut() {
            events.sort_by_key(|e| e.seq);
        }

        Ok(Self {
            header,
            adapter_events_by_node,
            metadata_events_by_caller,
            adapter_positions: RwLock::new(HashMap::new()),
            metadata_positions: RwLock::new(HashMap::new()),
            semantic_adapter_state: RwLock::new(HashMap::new()),
            semantic_metadata_state: RwLock::new(HashMap::new()),
        })
    }

    // -------------------------------------------------------------------------
    // Adapter call methods
    // -------------------------------------------------------------------------

    /// Get the next recorded adapter event for a node without advancing the position.
    pub fn peek_next(&self, node_id: &str) -> Option<&AdapterCallEvent> {
        let positions = self.adapter_positions.read();
        let pos = positions.get(node_id).copied().unwrap_or(0);
        self.adapter_events_by_node.get(node_id)?.get(pos)
    }

    /// Get the next recorded adapter event for a node and advance the position.
    pub fn take_next(&self, node_id: &str) -> Option<&AdapterCallEvent> {
        let events = self.adapter_events_by_node.get(node_id)?;
        let mut positions = self.adapter_positions.write();
        let pos = positions.entry(node_id.to_string()).or_insert(0);
        let event = events.get(*pos)?;
        *pos += 1;
        Some(event)
    }

    /// Get all recorded adapter events for a node.
    pub fn events_for_node(&self, node_id: &str) -> Option<&[AdapterCallEvent]> {
        self.adapter_events_by_node
            .get(node_id)
            .map(|v| v.as_slice())
    }

    /// Get all node IDs in the recording.
    pub fn node_ids(&self) -> impl Iterator<Item = &str> {
        self.adapter_events_by_node.keys().map(|s| s.as_str())
    }

    // -------------------------------------------------------------------------
    // Semantic mode adapter methods
    // -------------------------------------------------------------------------

    /// Find and consume a matching adapter event using semantic rules.
    ///
    /// For Write operations: Must match the next write in sequence (barrier semantics).
    ///     Writes are tracked and consumed - they act as segment barriers.
    ///     Args are verified to ensure we're matching the correct write.
    /// For MetadataRead operations: Can match any read in the current segment with matching args.
    ///     Reads are NOT tracked - the same read can be matched multiple times.
    ///
    /// PRECONDITION: Pure/Cache operations are filtered at the bridge_adapter level and should never reach here.
    ///
    /// Returns the matched event if found.
    pub fn take_semantic_match(
        &self,
        node_id: &str,
        method: &str,
        args: &serde_json::Value,
        category: SemanticCategory,
    ) -> Option<&AdapterCallEvent> {
        let events = self.adapter_events_by_node.get(node_id)?;
        let mut state = self.semantic_adapter_state.write();
        let node_state = state.entry(node_id.to_string()).or_default();

        match category {
            SemanticCategory::Write => {
                // Writes are barriers - must match the next write in sequence.
                // Writes ARE tracked and consumed. Args must also match.
                self.find_next_write_in_segment(events, node_state, method, args)
            }
            SemanticCategory::MetadataRead => {
                // Reads can match any read in the current segment with matching args.
                // Reads are NOT tracked - same read can be matched 0 or more times.
                self.find_read_in_segment_untracked(events, node_state, method, args)
            }
            SemanticCategory::Pure | SemanticCategory::Cache => {
                unreachable!()
            }
        }
    }

    /// Find the next write operation in sequence (barrier semantics).
    ///
    /// Writes are tracked and consumed. They act as segment barriers.
    /// In semantic mode, we only match by method name - the sequence order
    /// provides correctness, and SQL content may contain dynamic values
    /// (timestamps, query tags, invocation IDs) that differ between runs.
    fn find_next_write_in_segment<'a>(
        &'a self,
        events: &'a [AdapterCallEvent],
        state: &mut SemanticReplayState,
        method: &str,
        _args: &serde_json::Value,
    ) -> Option<&'a AdapterCallEvent> {
        let search_start = state.segment_start;

        // Find the first write from segment_start onwards
        for (idx, event) in events[search_start..].iter().enumerate() {
            // Skip non-write operations
            if !event.semantic_category.is_mutating() {
                continue;
            }

            // Found a write - method name must match (args are not checked in semantic mode
            // because SQL may contain dynamic content like timestamps, query tags, etc.)
            if event.method == method {
                // Advance the segment past this write
                let abs_idx = search_start + idx;
                state.segment_start = abs_idx + 1;
                state.last_write_barrier = Some(abs_idx);
                return Some(event);
            } else {
                // Write mismatch (method) - sequencing error
                return None;
            }
        }

        None
    }

    /// Find a matching read operation within the current segment (untracked).
    ///
    /// In semantic mode, reads are NOT tracked for consumption. The same read
    /// can be matched multiple times. This reflects that reads are idempotent
    /// and the replay code may call them more or fewer times than recorded.
    ///
    /// Matching is done on both method name AND arguments to ensure we return
    /// the correct result for the specific call.
    fn find_read_in_segment_untracked<'a>(
        &'a self,
        events: &'a [AdapterCallEvent],
        state: &SemanticReplayState,
        method: &str,
        args: &serde_json::Value,
    ) -> Option<&'a AdapterCallEvent> {
        let search_start = state.segment_start;

        // Determine segment end (next write or end of events)
        let segment_end = events[search_start..]
            .iter()
            .position(|e| e.semantic_category.is_mutating())
            .map(|pos| search_start + pos)
            .unwrap_or(events.len());

        // Search within the segment for a matching read (method + args)
        events[search_start..segment_end]
            .iter()
            .find(|event| event.method == method && values_match(&event.args, args))
    }

    /// Peek at the next event in semantic mode without consuming it.
    ///
    /// For reads: Returns the first matching event in the current segment
    /// For writes: Returns the next write barrier
    pub fn peek_semantic(
        &self,
        node_id: &str,
        category: SemanticCategory,
    ) -> Option<&AdapterCallEvent> {
        let events = self.adapter_events_by_node.get(node_id)?;
        let state = self.semantic_adapter_state.read();
        let node_state = state.get(node_id).cloned().unwrap_or_default();

        let search_start = node_state.segment_start;

        match category {
            SemanticCategory::Write => {
                // Find the next write (writes ARE tracked)
                events[search_start..]
                    .iter()
                    .find(|e| e.semantic_category.is_mutating())
            }
            SemanticCategory::MetadataRead => {
                // Find first read in segment (reads are NOT tracked)
                let segment_end = events[search_start..]
                    .iter()
                    .position(|e| e.semantic_category.is_mutating())
                    .map(|pos| search_start + pos)
                    .unwrap_or(events.len());

                events[search_start..segment_end].first()
            }
            SemanticCategory::Pure | SemanticCategory::Cache => {
                // Already handled above
                unreachable!()
            }
        }
    }

    /// Check if the current segment has any reads available.
    ///
    /// Note: In semantic mode, reads are not tracked for consumption,
    /// so this just checks if any reads exist in the current segment.
    pub fn has_reads_in_segment(&self, node_id: &str) -> bool {
        let Some(events) = self.adapter_events_by_node.get(node_id) else {
            return false;
        };
        let state = self.semantic_adapter_state.read();
        let node_state = state.get(node_id).cloned().unwrap_or_default();

        let search_start = node_state.segment_start;
        let segment_end = events[search_start..]
            .iter()
            .position(|e| e.semantic_category.is_mutating())
            .map(|pos| search_start + pos)
            .unwrap_or(events.len());

        // Check if there are any non-write events in the segment
        events[search_start..segment_end]
            .iter()
            .any(|e| !e.semantic_category.is_mutating())
    }

    /// Check if there is a pending write barrier for a node.
    pub fn has_pending_write(&self, node_id: &str) -> bool {
        let Some(events) = self.adapter_events_by_node.get(node_id) else {
            return false;
        };
        let state = self.semantic_adapter_state.read();
        let node_state = state.get(node_id).cloned().unwrap_or_default();

        events[node_state.segment_start..]
            .iter()
            .any(|e| e.semantic_category.is_mutating())
    }

    // -------------------------------------------------------------------------
    // Semantic mode metadata methods
    // -------------------------------------------------------------------------

    /// Find and consume a matching metadata event using semantic rules.
    ///
    /// Same semantics as adapter calls:
    /// - Writes are tracked and must match in order (barriers)
    /// - Reads are NOT tracked and can match 0 or more times
    ///
    /// Args are matched to ensure we return the correct result.
    pub fn take_semantic_metadata_match(
        &self,
        caller_id: &str,
        method: &str,
        args: &MetadataCallArgs,
        category: SemanticCategory,
    ) -> Option<&MetadataCallEvent> {
        let events = self.metadata_events_by_caller.get(caller_id)?;
        let mut state = self.semantic_metadata_state.write();
        let caller_state = state.entry(caller_id.to_string()).or_default();

        match category {
            SemanticCategory::Write => {
                // Writes are barriers - tracked and consumed
                self.find_next_metadata_write(events, caller_state, method, args)
            }
            SemanticCategory::MetadataRead => {
                // Reads can match any read in segment with matching args - NOT tracked
                self.find_metadata_read_in_segment_untracked(events, caller_state, method, args)
            }
            SemanticCategory::Pure | SemanticCategory::Cache => {
                unreachable!()
            }
        }
    }

    /// Find the next metadata write operation in sequence (tracked).
    fn find_next_metadata_write<'a>(
        &'a self,
        events: &'a [MetadataCallEvent],
        state: &mut SemanticReplayState,
        method: &str,
        args: &MetadataCallArgs,
    ) -> Option<&'a MetadataCallEvent> {
        let search_start = state.segment_start;

        // Find the first write from segment_start onwards
        for (idx, event) in events[search_start..].iter().enumerate() {
            if !event.semantic_category.is_mutating() {
                continue;
            }

            // Found a write - method and args must match
            if event.method == method && metadata_args_match(&event.args, args) {
                let abs_idx = search_start + idx;
                state.segment_start = abs_idx + 1; // Advance past this write
                state.last_write_barrier = Some(abs_idx);
                return Some(event);
            } else {
                // Write mismatch (method or args) - sequencing error
                return None;
            }
        }

        None
    }

    /// Find a matching metadata read within the current segment (untracked).
    fn find_metadata_read_in_segment_untracked<'a>(
        &'a self,
        events: &'a [MetadataCallEvent],
        state: &SemanticReplayState,
        method: &str,
        args: &MetadataCallArgs,
    ) -> Option<&'a MetadataCallEvent> {
        let search_start = state.segment_start;

        let segment_end = events[search_start..]
            .iter()
            .position(|e| e.semantic_category.is_mutating())
            .map(|pos| search_start + pos)
            .unwrap_or(events.len());

        // Search for matching read (method + args) - no consumption tracking
        events[search_start..segment_end]
            .iter()
            .find(|event| event.method == method && metadata_args_match(&event.args, args))
    }

    // -------------------------------------------------------------------------
    // Metadata call methods (strict mode)
    // -------------------------------------------------------------------------

    /// Check if we have any recorded metadata events for a caller.
    pub fn has_metadata_events_for_caller(&self, caller_id: &str) -> bool {
        self.metadata_events_by_caller.contains_key(caller_id)
    }

    /// Find a matching metadata read across ALL callers using superset matching
    /// within their current segment.
    /// This cross-node check is necessary as there may be non-determinism in
    /// which node facilitates hydration of shared resources such as the schema cache.
    pub fn find_metadata_read_across_all_callers(
        &self,
        method: &str,
        args: &MetadataCallArgs,
    ) -> Option<&MetadataCallEvent> {
        let state = self.semantic_metadata_state.read();

        for (caller_id, events) in &self.metadata_events_by_caller {
            // Get segment start for this caller (0 if not yet visited)
            let segment_start = state.get(caller_id).map(|s| s.segment_start).unwrap_or(0);

            // Find segment end (next write or end of events)
            let segment_end = events[segment_start..]
                .iter()
                .position(|e| e.semantic_category.is_mutating())
                .map(|pos| segment_start + pos)
                .unwrap_or(events.len());

            // Search within this caller's current segment
            if let Some(event) = events[segment_start..segment_end]
                .iter()
                .find(|e| e.method == method && metadata_args_match(&e.args, args))
            {
                return Some(event);
            }
        }
        None
    }

    /// Get the next recorded metadata event for a caller without advancing the position.
    pub fn peek_next_metadata(&self, caller_id: &str) -> Option<&MetadataCallEvent> {
        let positions = self.metadata_positions.read();
        let pos = positions.get(caller_id).copied().unwrap_or(0);
        self.metadata_events_by_caller.get(caller_id)?.get(pos)
    }

    /// Get the next recorded metadata event for a caller and advance the position.
    pub fn take_next_metadata(&self, caller_id: &str) -> Option<&MetadataCallEvent> {
        let events = self.metadata_events_by_caller.get(caller_id)?;
        let mut positions = self.metadata_positions.write();
        let pos = positions.entry(caller_id.to_string()).or_insert(0);
        let event = events.get(*pos)?;
        *pos += 1;
        Some(event)
    }

    /// Get all recorded metadata events for a caller.
    pub fn metadata_events_for_caller(&self, caller_id: &str) -> Option<&[MetadataCallEvent]> {
        self.metadata_events_by_caller
            .get(caller_id)
            .map(|v| v.as_slice())
    }

    /// Get all caller IDs for metadata events.
    pub fn metadata_caller_ids(&self) -> impl Iterator<Item = &str> {
        self.metadata_events_by_caller.keys().map(|s| s.as_str())
    }

    // -------------------------------------------------------------------------
    // Statistics and reset
    // -------------------------------------------------------------------------

    /// Get the total number of events (both adapter and metadata).
    pub fn total_events(&self) -> usize {
        self.adapter_events_by_node
            .values()
            .map(|v| v.len())
            .sum::<usize>()
            + self
                .metadata_events_by_caller
                .values()
                .map(|v| v.len())
                .sum::<usize>()
    }

    /// Get total adapter events count.
    pub fn total_adapter_events(&self) -> usize {
        self.adapter_events_by_node.values().map(|v| v.len()).sum()
    }

    /// Get total metadata events count.
    pub fn total_metadata_events(&self) -> usize {
        self.metadata_events_by_caller
            .values()
            .map(|v| v.len())
            .sum()
    }

    /// Reset replay positions for all nodes and callers.
    pub fn reset(&self) {
        self.adapter_positions.write().clear();
        self.metadata_positions.write().clear();
        self.semantic_adapter_state.write().clear();
        self.semantic_metadata_state.write().clear();
    }

    /// Reset replay position for a specific node.
    pub fn reset_node(&self, node_id: &str) {
        self.adapter_positions.write().remove(node_id);
        self.semantic_adapter_state.write().remove(node_id);
    }

    /// Reset replay position for a specific metadata caller.
    pub fn reset_metadata_caller(&self, caller_id: &str) {
        self.metadata_positions.write().remove(caller_id);
        self.semantic_metadata_state.write().remove(caller_id);
    }
}

/// Load events from a gzipped NDJSON file.
fn load_events_gzipped(path: &Path) -> Result<Vec<RecordedEvent>, ReplayError> {
    let file = std::fs::File::open(path)?;
    let decoder = GzDecoder::new(file);
    let reader = BufReader::new(decoder);
    load_events_from_reader(reader)
}

/// Load events from a plain NDJSON file.
fn load_events_plain(path: &Path) -> Result<Vec<RecordedEvent>, ReplayError> {
    let file = std::fs::File::open(path)?;
    let reader = BufReader::new(file);
    load_events_from_reader(reader)
}

/// Load events from a reader.
fn load_events_from_reader<R: BufRead>(reader: R) -> Result<Vec<RecordedEvent>, ReplayError> {
    let mut events = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let event: RecordedEvent = serde_json::from_str(&line)?;
        events.push(event);
    }
    Ok(events)
}

// -----------------------------------------------------------------------------
// Replay validation
// -----------------------------------------------------------------------------

/// Result of replaying an event.
#[derive(Debug)]
pub struct ReplayResult {
    /// The recorded event that was matched
    pub recorded: AdapterCallEvent,
    /// Whether the replay matched the recording
    pub matched: bool,
    /// Differences found (if any)
    pub differences: Vec<ReplayDifference>,
}

/// A difference found during replay.
#[derive(Debug)]
pub enum ReplayDifference {
    MethodMismatch {
        expected: String,
        actual: String,
    },
    ArgCountMismatch {
        expected: usize,
        actual: usize,
    },
    ArgValueMismatch {
        index: usize,
        expected: serde_json::Value,
        actual: serde_json::Value,
    },
}

/// Validate a replay call against a recorded event.
pub fn validate_replay(
    recorded: &AdapterCallEvent,
    method: &str,
    args: &serde_json::Value,
) -> ReplayResult {
    let mut differences = Vec::new();

    // Check method name
    if recorded.method != method {
        differences.push(ReplayDifference::MethodMismatch {
            expected: recorded.method.clone(),
            actual: method.to_string(),
        });
    }

    // Check args
    if let (Some(recorded_args), Some(actual_args)) = (recorded.args.as_array(), args.as_array()) {
        if recorded_args.len() != actual_args.len() {
            differences.push(ReplayDifference::ArgCountMismatch {
                expected: recorded_args.len(),
                actual: actual_args.len(),
            });
        } else {
            for (i, (expected, actual)) in recorded_args.iter().zip(actual_args.iter()).enumerate()
            {
                if !values_match(expected, actual) {
                    differences.push(ReplayDifference::ArgValueMismatch {
                        index: i,
                        expected: expected.clone(),
                        actual: actual.clone(),
                    });
                }
            }
        }
    }

    ReplayResult {
        recorded: recorded.clone(),
        matched: differences.is_empty(),
        differences,
    }
}

// Tests for serde functionality have been moved to the `serde` module.
// See `super::serde::tests` for json_to_value and values_match tests.

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to create a test AdapterCallEvent
    fn make_event(
        node_id: &str,
        seq: u32,
        method: &str,
        category: SemanticCategory,
    ) -> AdapterCallEvent {
        AdapterCallEvent {
            node_id: node_id.to_string(),
            seq,
            method: method.to_string(),
            semantic_category: category,
            args: serde_json::json!([]),
            result: serde_json::json!(null),
            success: true,
            error: None,
            timestamp_ns: seq as u64 * 1000,
        }
    }

    /// Helper to get empty args for test calls
    fn empty_args() -> serde_json::Value {
        serde_json::json!([])
    }

    /// Helper to create a test Recording from events
    fn make_recording(events: Vec<AdapterCallEvent>) -> Recording {
        let mut adapter_events_by_node: HashMap<String, Vec<AdapterCallEvent>> = HashMap::new();
        for event in events {
            adapter_events_by_node
                .entry(event.node_id.clone())
                .or_default()
                .push(event);
        }
        for events in adapter_events_by_node.values_mut() {
            events.sort_by_key(|e| e.seq);
        }

        Recording {
            header: RecordingHeader {
                format_version: 1,
                fusion_version: "test".to_string(),
                adapter_type: "snowflake".to_string(),
                recorded_at: "2024-01-01T00:00:00Z".to_string(),
                invocation_id: "test-123".to_string(),
                metadata: serde_json::Map::new(),
            },
            adapter_events_by_node,
            metadata_events_by_caller: HashMap::new(),
            adapter_positions: RwLock::new(HashMap::new()),
            metadata_positions: RwLock::new(HashMap::new()),
            semantic_adapter_state: RwLock::new(HashMap::new()),
            semantic_metadata_state: RwLock::new(HashMap::new()),
        }
    }

    #[test]
    fn test_replay_mode_default_is_strict() {
        assert_eq!(ReplayMode::default(), ReplayMode::Strict);
    }

    #[test]
    fn test_replay_mode_display() {
        assert_eq!(format!("{}", ReplayMode::Strict), "strict");
        assert_eq!(format!("{}", ReplayMode::Semantic), "semantic");
    }

    #[test]
    fn test_replay_mode_serialization() {
        let strict = ReplayMode::Strict;
        let json = serde_json::to_string(&strict).unwrap();
        assert_eq!(json, "\"strict\"");

        let semantic = ReplayMode::Semantic;
        let json = serde_json::to_string(&semantic).unwrap();
        assert_eq!(json, "\"semantic\"");

        let parsed: ReplayMode = serde_json::from_str("\"semantic\"").unwrap();
        assert_eq!(parsed, ReplayMode::Semantic);
    }

    #[test]
    fn test_strict_mode_sequential_matching() {
        // In strict mode, events must match in exact sequence
        let events = vec![
            make_event("node1", 0, "get_relation", SemanticCategory::MetadataRead),
            make_event(
                "node1",
                1,
                "get_columns_in_relation",
                SemanticCategory::MetadataRead,
            ),
            make_event("node1", 2, "execute", SemanticCategory::Write),
        ];
        let recording = make_recording(events);

        // First call should match first event
        let event = recording.take_next("node1").unwrap();
        assert_eq!(event.method, "get_relation");

        // Second call should match second event
        let event = recording.take_next("node1").unwrap();
        assert_eq!(event.method, "get_columns_in_relation");

        // Third call should match third event
        let event = recording.take_next("node1").unwrap();
        assert_eq!(event.method, "execute");

        // No more events
        assert!(recording.take_next("node1").is_none());
    }

    #[test]
    fn test_semantic_mode_reads_can_match_out_of_order() {
        // In semantic mode, reads within a segment can match in any order
        let events = vec![
            make_event("node1", 0, "get_relation", SemanticCategory::MetadataRead),
            make_event(
                "node1",
                1,
                "get_columns_in_relation",
                SemanticCategory::MetadataRead,
            ),
            make_event("node1", 2, "list_schemas", SemanticCategory::MetadataRead),
            make_event("node1", 3, "execute", SemanticCategory::Write),
        ];
        let recording = make_recording(events);

        // Request in different order than recorded
        // First, get_columns_in_relation (recorded as seq 1)
        let event = recording
            .take_semantic_match(
                "node1",
                "get_columns_in_relation",
                &empty_args(),
                SemanticCategory::MetadataRead,
            )
            .unwrap();
        assert_eq!(event.method, "get_columns_in_relation");

        // Second, list_schemas (recorded as seq 2)
        let event = recording
            .take_semantic_match(
                "node1",
                "list_schemas",
                &empty_args(),
                SemanticCategory::MetadataRead,
            )
            .unwrap();
        assert_eq!(event.method, "list_schemas");

        // Third, get_relation (recorded as seq 0)
        let event = recording
            .take_semantic_match(
                "node1",
                "get_relation",
                &empty_args(),
                SemanticCategory::MetadataRead,
            )
            .unwrap();
        assert_eq!(event.method, "get_relation");

        // Now the write barrier - must still be next
        let event = recording
            .take_semantic_match("node1", "execute", &empty_args(), SemanticCategory::Write)
            .unwrap();
        assert_eq!(event.method, "execute");
    }

    #[test]
    fn test_semantic_mode_writes_are_barriers() {
        // Writes must match in sequence order - they act as barriers
        let events = vec![
            make_event("node1", 0, "get_relation", SemanticCategory::MetadataRead),
            make_event("node1", 1, "execute", SemanticCategory::Write),
            make_event("node1", 2, "get_relation", SemanticCategory::MetadataRead),
            make_event("node1", 3, "drop_relation", SemanticCategory::Write),
        ];
        let recording = make_recording(events);

        // Consume the read in first segment
        let event = recording
            .take_semantic_match(
                "node1",
                "get_relation",
                &empty_args(),
                SemanticCategory::MetadataRead,
            )
            .unwrap();
        assert_eq!(event.seq, 0);

        // Now we must match the write barrier
        let event = recording
            .take_semantic_match("node1", "execute", &empty_args(), SemanticCategory::Write)
            .unwrap();
        assert_eq!(event.seq, 1);

        // Now we're in a new segment - can match the second get_relation
        let event = recording
            .take_semantic_match(
                "node1",
                "get_relation",
                &empty_args(),
                SemanticCategory::MetadataRead,
            )
            .unwrap();
        assert_eq!(event.seq, 2);

        // Match the second write barrier
        let event = recording
            .take_semantic_match(
                "node1",
                "drop_relation",
                &empty_args(),
                SemanticCategory::Write,
            )
            .unwrap();
        assert_eq!(event.seq, 3);
    }

    #[test]
    fn test_semantic_mode_wrong_write_order_fails() {
        // Trying to match writes out of order should fail
        let events = vec![
            make_event("node1", 0, "execute", SemanticCategory::Write),
            make_event("node1", 1, "drop_relation", SemanticCategory::Write),
        ];
        let recording = make_recording(events);

        // Try to match drop_relation first (but execute is the next write barrier)
        let result = recording.take_semantic_match(
            "node1",
            "drop_relation",
            &empty_args(),
            SemanticCategory::Write,
        );
        assert!(result.is_none(), "Should not match wrong write order");
    }

    #[test]
    fn test_semantic_mode_read_not_in_segment_fails() {
        // Trying to match a read that's past the current segment boundary should fail
        let events = vec![
            make_event("node1", 0, "get_relation", SemanticCategory::MetadataRead),
            make_event("node1", 1, "execute", SemanticCategory::Write),
            make_event("node1", 2, "list_schemas", SemanticCategory::MetadataRead),
        ];
        let recording = make_recording(events);

        // Try to match list_schemas (which is after the write barrier in segment 2)
        let result = recording.take_semantic_match(
            "node1",
            "list_schemas",
            &empty_args(),
            SemanticCategory::MetadataRead,
        );
        assert!(
            result.is_none(),
            "Should not match read from future segment"
        );

        // But we can match get_relation from current segment
        let event = recording
            .take_semantic_match(
                "node1",
                "get_relation",
                &empty_args(),
                SemanticCategory::MetadataRead,
            )
            .unwrap();
        assert_eq!(event.seq, 0);
    }

    #[test]
    fn test_semantic_mode_multiple_segments() {
        // Test behavior across multiple segments
        let events = vec![
            // Segment 1
            make_event("node1", 0, "get_relation", SemanticCategory::MetadataRead),
            make_event(
                "node1",
                1,
                "get_columns_in_relation",
                SemanticCategory::MetadataRead,
            ),
            make_event("node1", 2, "execute", SemanticCategory::Write),
            // Segment 2
            make_event("node1", 3, "list_schemas", SemanticCategory::MetadataRead),
            make_event(
                "node1",
                4,
                "check_schema_exists",
                SemanticCategory::MetadataRead,
            ),
            make_event("node1", 5, "create_schema", SemanticCategory::Write),
            // Segment 3
            make_event("node1", 6, "get_relation", SemanticCategory::MetadataRead),
        ];
        let recording = make_recording(events);

        // Segment 1: Match reads in reverse order
        let _ = recording
            .take_semantic_match(
                "node1",
                "get_columns_in_relation",
                &empty_args(),
                SemanticCategory::MetadataRead,
            )
            .unwrap();
        let _ = recording
            .take_semantic_match(
                "node1",
                "get_relation",
                &empty_args(),
                SemanticCategory::MetadataRead,
            )
            .unwrap();
        let _ = recording
            .take_semantic_match("node1", "execute", &empty_args(), SemanticCategory::Write)
            .unwrap();

        // Segment 2: Match reads in reverse order
        let _ = recording
            .take_semantic_match(
                "node1",
                "check_schema_exists",
                &empty_args(),
                SemanticCategory::MetadataRead,
            )
            .unwrap();
        let _ = recording
            .take_semantic_match(
                "node1",
                "list_schemas",
                &empty_args(),
                SemanticCategory::MetadataRead,
            )
            .unwrap();
        let _ = recording
            .take_semantic_match(
                "node1",
                "create_schema",
                &empty_args(),
                SemanticCategory::Write,
            )
            .unwrap();

        // Segment 3 (no write barrier at end)
        let event = recording
            .take_semantic_match(
                "node1",
                "get_relation",
                &empty_args(),
                SemanticCategory::MetadataRead,
            )
            .unwrap();
        assert_eq!(event.seq, 6);
    }

    #[test]
    fn test_semantic_mode_peek_does_not_consume() {
        let events = vec![
            make_event("node1", 0, "get_relation", SemanticCategory::MetadataRead),
            make_event("node1", 1, "execute", SemanticCategory::Write),
        ];
        let recording = make_recording(events);

        // Peek multiple times
        let event1 = recording.peek_semantic("node1", SemanticCategory::MetadataRead);
        let event2 = recording.peek_semantic("node1", SemanticCategory::MetadataRead);

        assert!(event1.is_some());
        assert!(event2.is_some());
        assert_eq!(event1.unwrap().method, event2.unwrap().method);

        // Now actually consume it
        let consumed = recording
            .take_semantic_match(
                "node1",
                "get_relation",
                &empty_args(),
                SemanticCategory::MetadataRead,
            )
            .unwrap();
        assert_eq!(consumed.method, "get_relation");
    }

    #[test]
    fn test_reset_clears_semantic_state() {
        let events = vec![
            make_event("node1", 0, "get_relation", SemanticCategory::MetadataRead),
            make_event("node1", 1, "execute", SemanticCategory::Write),
        ];
        let recording = make_recording(events);

        // Consume the read
        let _ = recording
            .take_semantic_match(
                "node1",
                "get_relation",
                &empty_args(),
                SemanticCategory::MetadataRead,
            )
            .unwrap();

        // Reset
        recording.reset();

        // Should be able to match again
        let event = recording
            .take_semantic_match(
                "node1",
                "get_relation",
                &empty_args(),
                SemanticCategory::MetadataRead,
            )
            .unwrap();
        assert_eq!(event.seq, 0);
    }

    #[test]
    fn test_has_reads_in_segment() {
        let events = vec![
            make_event("node1", 0, "get_relation", SemanticCategory::MetadataRead),
            make_event("node1", 1, "execute", SemanticCategory::Write),
            make_event("node1", 2, "list_schemas", SemanticCategory::MetadataRead),
        ];
        let recording = make_recording(events);

        // Initially has reads in segment
        assert!(recording.has_reads_in_segment("node1"));

        // Consume the write to advance to next segment
        let _ = recording
            .take_semantic_match("node1", "execute", &empty_args(), SemanticCategory::Write)
            .unwrap();

        // New segment also has reads
        assert!(recording.has_reads_in_segment("node1"));
    }

    #[test]
    fn test_semantic_mode_reads_can_match_multiple_times() {
        // In semantic mode, reads are NOT tracked - same read can be matched multiple times
        let events = vec![
            make_event("node1", 0, "get_relation", SemanticCategory::MetadataRead),
            make_event("node1", 1, "execute", SemanticCategory::Write),
        ];
        let recording = make_recording(events);

        // Match the same read multiple times - should succeed each time
        let event1 = recording
            .take_semantic_match(
                "node1",
                "get_relation",
                &empty_args(),
                SemanticCategory::MetadataRead,
            )
            .unwrap();
        assert_eq!(event1.method, "get_relation");

        let event2 = recording
            .take_semantic_match(
                "node1",
                "get_relation",
                &empty_args(),
                SemanticCategory::MetadataRead,
            )
            .unwrap();
        assert_eq!(event2.method, "get_relation");

        let event3 = recording
            .take_semantic_match(
                "node1",
                "get_relation",
                &empty_args(),
                SemanticCategory::MetadataRead,
            )
            .unwrap();
        assert_eq!(event3.method, "get_relation");

        // But writes are still tracked - can only match once
        let write = recording
            .take_semantic_match("node1", "execute", &empty_args(), SemanticCategory::Write)
            .unwrap();
        assert_eq!(write.method, "execute");

        // Trying to match the write again fails (no more writes)
        let no_write = recording.take_semantic_match(
            "node1",
            "execute",
            &empty_args(),
            SemanticCategory::Write,
        );
        assert!(no_write.is_none());
    }

    #[test]
    fn test_has_pending_write() {
        let events = vec![
            make_event("node1", 0, "get_relation", SemanticCategory::MetadataRead),
            make_event("node1", 1, "execute", SemanticCategory::Write),
            make_event("node1", 2, "get_relation", SemanticCategory::MetadataRead),
        ];
        let recording = make_recording(events);

        // Has pending write
        assert!(recording.has_pending_write("node1"));

        // Consume the write
        let _ = recording
            .take_semantic_match("node1", "execute", &empty_args(), SemanticCategory::Write)
            .unwrap();

        // No more pending writes
        assert!(!recording.has_pending_write("node1"));
    }

    #[test]
    fn test_semantic_mode_matches_by_args() {
        // Helper to create an event with specific args
        fn make_event_with_args(
            node_id: &str,
            seq: u32,
            method: &str,
            args: serde_json::Value,
            category: SemanticCategory,
        ) -> AdapterCallEvent {
            AdapterCallEvent {
                node_id: node_id.to_string(),
                seq,
                method: method.to_string(),
                semantic_category: category,
                args,
                result: serde_json::json!({"seq": seq}), // Different result for each
                success: true,
                error: None,
                timestamp_ns: seq as u64 * 1000,
            }
        }

        // Create events with same method but different args
        let events = vec![
            make_event_with_args(
                "node1",
                0,
                "get_relation",
                serde_json::json!(["DB", "SCHEMA", "TABLE_A"]),
                SemanticCategory::MetadataRead,
            ),
            make_event_with_args(
                "node1",
                1,
                "get_relation",
                serde_json::json!(["DB", "SCHEMA", "TABLE_B"]),
                SemanticCategory::MetadataRead,
            ),
            make_event_with_args(
                "node1",
                2,
                "execute",
                serde_json::json!(["CREATE TABLE foo"]),
                SemanticCategory::Write,
            ),
        ];

        let mut adapter_events_by_node: HashMap<String, Vec<AdapterCallEvent>> = HashMap::new();
        for event in events {
            adapter_events_by_node
                .entry(event.node_id.clone())
                .or_default()
                .push(event);
        }

        let recording = Recording {
            header: RecordingHeader {
                format_version: 1,
                fusion_version: "test".to_string(),
                adapter_type: "snowflake".to_string(),
                recorded_at: "2024-01-01T00:00:00Z".to_string(),
                invocation_id: "test-123".to_string(),
                metadata: serde_json::Map::new(),
            },
            adapter_events_by_node,
            metadata_events_by_caller: HashMap::new(),
            adapter_positions: RwLock::new(HashMap::new()),
            metadata_positions: RwLock::new(HashMap::new()),
            semantic_adapter_state: RwLock::new(HashMap::new()),
            semantic_metadata_state: RwLock::new(HashMap::new()),
        };

        // Request TABLE_B first (out of order by seq, but should match by args)
        let args_b = serde_json::json!(["DB", "SCHEMA", "TABLE_B"]);
        let event = recording
            .take_semantic_match(
                "node1",
                "get_relation",
                &args_b,
                SemanticCategory::MetadataRead,
            )
            .unwrap();
        // Should get the TABLE_B result (seq 1)
        assert_eq!(event.result, serde_json::json!({"seq": 1}));

        // Request TABLE_A (should still find it since reads aren't consumed)
        let args_a = serde_json::json!(["DB", "SCHEMA", "TABLE_A"]);
        let event = recording
            .take_semantic_match(
                "node1",
                "get_relation",
                &args_a,
                SemanticCategory::MetadataRead,
            )
            .unwrap();
        // Should get the TABLE_A result (seq 0)
        assert_eq!(event.result, serde_json::json!({"seq": 0}));

        // Request TABLE_C which doesn't exist - should return None
        let args_c = serde_json::json!(["DB", "SCHEMA", "TABLE_C"]);
        let result = recording.take_semantic_match(
            "node1",
            "get_relation",
            &args_c,
            SemanticCategory::MetadataRead,
        );
        assert!(result.is_none(), "Should not find TABLE_C");
    }
}
