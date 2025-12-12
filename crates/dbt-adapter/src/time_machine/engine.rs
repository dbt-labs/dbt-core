//! Unified time machine engine for recording and replay.
//!
//! The `TimeMachine` enum provides a single abstraction for both:
//! - Recording adapter calls during execution
//! - Replaying recorded calls for compatibility testing

use std::sync::Arc;

use minijinja::Value;

use dbt_common::adapter::AdapterType;
use dbt_schemas::schemas::common::ResolvedQuoting;

use super::event::MetadataCallArgs;
use super::event_recorder::EventRecorder;
use super::event_replay::{Recording, ReplayError, ReplayMode};
use super::semantic::SemanticCategory;
use super::serde::{ReplayContext, json_to_value_with_context};

/// Unified time machine for recording or replaying adapter calls.
#[derive(Clone)]
pub enum TimeMachine {
    /// Recording mode - capture adapter calls
    Record(Arc<EventRecorder>),
    /// Replay mode - return recorded results
    Replay(Arc<EventReplayer>),
}

impl TimeMachine {
    /// Create a time machine in recording mode.
    pub fn recorder(recorder: Arc<EventRecorder>) -> Self {
        Self::Record(recorder)
    }

    /// Create a time machine in replay mode.
    pub fn replayer(replayer: Arc<EventReplayer>) -> Self {
        Self::Replay(replayer)
    }

    /// Check if this is in recording mode.
    pub fn is_recording(&self) -> bool {
        matches!(self, Self::Record(_))
    }

    /// Check if this is in replay mode.
    pub fn is_replaying(&self) -> bool {
        matches!(self, Self::Replay(_))
    }

    /// Try to get a replay result for an adapter call.
    ///
    /// Returns:
    /// - `Some(Ok(value))` - Use this recorded result instead of calling the real adapter
    /// - `Some(Err(e))` - The recorded call failed with this error
    /// - `None` - Not in replay mode
    pub fn try_replay(
        &self,
        node_id: &str,
        method: &str,
        args: &[Value],
    ) -> Option<Result<Value, ReplayCallError>> {
        match self {
            Self::Record(_) => None, // Recording mode doesn't intercept
            Self::Replay(replayer) => Some(replayer.get_result(node_id, method, args)),
        }
    }

    /// Record an adapter call result.
    ///
    /// Only does something in recording mode.
    pub fn record_call(
        &self,
        node_id: impl Into<String>,
        method: impl Into<String>,
        args: serde_json::Value,
        result: serde_json::Value,
        success: bool,
        error: Option<String>,
    ) {
        if let Self::Record(recorder) = self {
            recorder.record_adapter_call(node_id, method, args, result, success, error);
        }
    }
}

/// Error returned when a replayed call fails.
#[derive(Debug, Clone)]
pub struct ReplayCallError {
    pub message: String,
    pub recorded_error: Option<String>,
}

impl std::fmt::Display for ReplayCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref err) = self.recorded_error {
            write!(f, "Recorded call failed: {}", err)
        } else {
            write!(f, "{}", self.message)
        }
    }
}

impl std::error::Error for ReplayCallError {}

/// Event replayer - returns recorded results for adapter calls.
pub struct EventReplayer {
    recording: Recording,
    /// Replay ordering mode
    replay_mode: ReplayMode,
    /// Context for value reconstruction
    replay_ctx: ReplayContext,
}

impl EventReplayer {
    /// Create a new replayer from a recording.
    pub fn new(recording: Recording) -> Self {
        // Parse adapter type from header
        let adapter_type = recording
            .header
            .adapter_type
            .parse()
            .unwrap_or(AdapterType::Snowflake);

        Self {
            recording,
            replay_mode: ReplayMode::default(),
            replay_ctx: ReplayContext {
                adapter_type,
                quoting: ResolvedQuoting::default(),
            },
        }
    }

    /// Load a replayer from a recording directory.
    pub fn load(path: impl AsRef<std::path::Path>) -> Result<Self, ReplayError> {
        let recording = Recording::load(path)?;
        Ok(Self::new(recording))
    }

    /// Set the replay ordering mode.
    ///
    /// - `Strict`: Events must match in exact sequence order (default)
    /// - `Semantic`: Write operations are barriers; reads can match flexibly within segments
    pub fn with_replay_mode(mut self, mode: ReplayMode) -> Self {
        self.replay_mode = mode;
        self
    }

    /// Get the current replay mode.
    pub fn replay_mode(&self) -> ReplayMode {
        self.replay_mode
    }

    /// Set custom quoting for relation reconstruction.
    pub fn with_quoting(mut self, quoting: ResolvedQuoting) -> Self {
        self.replay_ctx.quoting = quoting;
        self
    }

    /// Get the recording header.
    pub fn header(&self) -> &super::event::RecordingHeader {
        &self.recording.header
    }

    /// Get the result for an adapter call.
    ///
    /// Pure/Cache calls are filtered at the bridge_adapter level and should never reach here.
    pub fn get_result(
        &self,
        node_id: &str,
        method: &str,
        args: &[Value],
    ) -> Result<Value, ReplayCallError> {
        let call_category = SemanticCategory::from_adapter_method(method);
        let serialized_args = super::serde::serialize_args(args);

        // Dispatch to the appropriate matching strategy
        match self.replay_mode {
            ReplayMode::Strict => {
                self.get_result_strict(node_id, method, &serialized_args, call_category)
            }
            ReplayMode::Semantic => {
                self.get_result_semantic(node_id, method, &serialized_args, call_category)
            }
        }
    }

    /// Get result using strict sequential matching.
    fn get_result_strict(
        &self,
        node_id: &str,
        method: &str,
        args: &serde_json::Value,
        call_category: SemanticCategory,
    ) -> Result<Value, ReplayCallError> {
        let event = match self.recording.peek_next(node_id) {
            Some(event) => event,
            None => {
                return Err(ReplayCallError {
                    message: format!(
                        "No recorded event for {} call '{}' on node '{}' with args '{}'. \
                         Recording may be incomplete or from a different code version.",
                        call_category, method, node_id, args
                    ),
                    recorded_error: None,
                });
            }
        };

        // Validate method name
        if event.method != method {
            return Err(ReplayCallError {
                message: format!(
                    "Method mismatch on node '{}': expected '{}', got '{}' (seq {})",
                    node_id, event.method, method, event.seq
                ),
                recorded_error: None,
            });
        }

        // Validate args only for reads - writes may contain dynamic SQL content
        // (timestamps, query tags, invocation IDs) that differs between runs.
        // The sequence ordering provides correctness for writes.
        if !call_category.is_mutating() && !super::serde::values_match(&event.args, args) {
            return Err(ReplayCallError {
                message: format!(
                    "Args mismatch on node '{}' for method '{}' (seq {})",
                    node_id, method, event.seq
                ),
                recorded_error: None,
            });
        }

        // Consume the matching event for replay
        let event = self
            .recording
            .take_next(node_id)
            .expect("event should exist after peek");
        self.convert_event_to_result(event)
    }

    /// Get result using semantic segment-based matching.
    ///
    /// Semantic mode has relaxed matching constraints:
    /// - **Writes**: Must match the next write barrier in sequence (error if mismatch)
    /// - **Reads**: Can match any read in segment with matching args, same read can match 0 or more times.
    ///   If no matching read found, returns error (can't service the request).
    fn get_result_semantic(
        &self,
        node_id: &str,
        method: &str,
        args: &serde_json::Value,
        call_category: SemanticCategory,
    ) -> Result<Value, ReplayCallError> {
        // Use semantic matching from the Recording
        let event = match self
            .recording
            .take_semantic_match(node_id, method, args, call_category)
        {
            Some(event) => event,
            None => {
                // No matching event found - error for both reads and writes
                let context = if call_category.is_mutating() {
                    "Write operations must match the next write barrier in sequence."
                } else {
                    "No matching read found in current segment. \
                     The same read can be matched multiple times, but at least one must exist."
                };
                return Err(ReplayCallError {
                    message: format!(
                        "No recorded event for {} call '{}' on node '{}'. {}",
                        call_category, method, node_id, context
                    ),
                    recorded_error: None,
                });
            }
        };

        self.convert_event_to_result(event)
    }

    /// Convert a matched event to a replay result.
    fn convert_event_to_result(
        &self,
        event: &super::event::AdapterCallEvent,
    ) -> Result<Value, ReplayCallError> {
        // Check if the recorded call succeeded
        if !event.success {
            return Err(ReplayCallError {
                message: "Recorded call failed".to_string(),
                recorded_error: event.error.clone(),
            });
        }

        // Convert the recorded result back to a Value
        let value = json_to_value_with_context(&event.result, &self.replay_ctx);
        Ok(value)
    }

    /// Get the result for a metadata adapter call.
    ///
    /// Returns:
    /// - `Some(Ok(json))` - The recorded result as JSON
    /// - `Some(Err(e))` - The recorded call failed
    /// - `None` - No recorded event found for this call
    pub fn get_metadata_result(
        &self,
        caller_id: &str,
        method: &str,
        args: &MetadataCallArgs,
    ) -> Option<Result<serde_json::Value, ReplayCallError>> {
        // Try exact caller_id first, then fall back to "global"
        // This handles cases where recording used "global" but replay uses specific node IDs
        let caller_ids_to_try = if caller_id == "global" {
            vec![caller_id]
        } else {
            vec![caller_id, "global"]
        };

        for try_caller_id in caller_ids_to_try {
            let result = match self.replay_mode {
                ReplayMode::Strict => self.try_get_metadata_result_strict(try_caller_id, method),
                ReplayMode::Semantic => {
                    self.try_get_metadata_result_semantic(try_caller_id, method, args)
                }
            };
            // Only return if we got a successful result or a write error.
            // For reads, continue searching (None means no match, Some(Err) for strict reads
            // should try cross-caller search first).
            match &result {
                Some(Ok(_)) => return result,
                Some(Err(_)) => {
                    // For semantic mode metadata reads, try cross-caller search before erroring
                    let category = SemanticCategory::from_metadata_method(method);
                    if matches!(self.replay_mode, ReplayMode::Semantic)
                        && matches!(category, SemanticCategory::MetadataRead)
                    {
                        // Continue to cross-caller search below
                        continue;
                    }
                    return result;
                }
                None => continue,
            }
        }

        // For semantic mode metadata reads, search across all callers as last resort.
        // Due to parallel execution, the same query might be recorded under a different caller.
        // We use superset matching: if recorded args contain all requested relations, it's a match.
        if matches!(self.replay_mode, ReplayMode::Semantic) {
            let category = SemanticCategory::from_metadata_method(method);
            if matches!(category, SemanticCategory::MetadataRead)
                && let Some(event) = self
                    .recording
                    .find_metadata_read_across_all_callers(method, args)
            {
                return self.convert_metadata_event_to_result(event);
            }
        }

        None
    }

    /// Internal helper to try getting a metadata result using strict sequential matching.
    fn try_get_metadata_result_strict(
        &self,
        caller_id: &str,
        method: &str,
    ) -> Option<Result<serde_json::Value, ReplayCallError>> {
        // For metadata calls, we match by caller_id and method
        let event = match self.recording.peek_next_metadata(caller_id) {
            Some(event) => event,
            None => {
                // No recorded event for this caller_id
                return None;
            }
        };

        // Validate method name
        if event.method != method {
            return Some(Err(ReplayCallError {
                message: format!(
                    "Metadata method mismatch for caller '{}': expected '{}', got '{}' (seq {})",
                    caller_id, event.method, method, event.seq
                ),
                recorded_error: None,
            }));
        }

        // Consume the matching event for replay
        let event = self.recording.take_next_metadata(caller_id)?;
        self.convert_metadata_event_to_result(event)
    }

    /// Internal helper to try getting a metadata result using semantic matching.
    ///
    /// Same semantics as adapter calls:
    /// - Writes must match in order (error if no match)
    /// - Reads can match 0 or more times with matching args
    ///
    /// Returns:
    /// - `Some(Ok(...))` - Found matching event
    /// - `Some(Err(...))` - Caller has events but no match
    /// - `None` - Caller has no events
    fn try_get_metadata_result_semantic(
        &self,
        caller_id: &str,
        method: &str,
        args: &MetadataCallArgs,
    ) -> Option<Result<serde_json::Value, ReplayCallError>> {
        // Determine the semantic category of this metadata method
        let category = SemanticCategory::from_metadata_method(method);

        // First check if this caller has any events at all
        // If not, return None to allow fallback to other callers (e.g., "global")
        if !self.recording.has_metadata_events_for_caller(caller_id) {
            return None;
        }

        match self
            .recording
            .take_semantic_metadata_match(caller_id, method, args, category)
        {
            Some(event) => self.convert_metadata_event_to_result(event),
            None => {
                // Caller has events but no match found
                let context = if category.is_mutating() {
                    "Write operations must match in sequence.".to_string()
                } else {
                    format!(
                        "No matching read with args {:?} found in current segment.",
                        args
                    )
                };
                Some(Err(ReplayCallError {
                    message: format!(
                        "No recorded event for metadata {} call '{}' on caller '{}'. {}",
                        category, method, caller_id, context
                    ),
                    recorded_error: None,
                }))
            }
        }
    }

    /// Convert a matched metadata event to a result.
    fn convert_metadata_event_to_result(
        &self,
        event: &super::event::MetadataCallEvent,
    ) -> Option<Result<serde_json::Value, ReplayCallError>> {
        // Check if the recorded call succeeded
        if !event.success {
            return Some(Err(ReplayCallError {
                message: "Recorded metadata call failed".to_string(),
                recorded_error: event.error.clone(),
            }));
        }

        Some(Ok(event.result.clone()))
    }

    /// Check if there are any metadata events recorded.
    pub fn has_metadata_events(&self) -> bool {
        self.recording.total_metadata_events() > 0
    }

    /// Reset replay state for all nodes.
    pub fn reset(&self) {
        self.recording.reset();
    }

    /// Get statistics about the recording.
    pub fn stats(&self) -> ReplayerStats {
        ReplayerStats {
            total_events: self.recording.total_events(),
            adapter_events: self.recording.total_adapter_events(),
            metadata_events: self.recording.total_metadata_events(),
            node_count: self.recording.node_ids().count(),
            metadata_caller_count: self.recording.metadata_caller_ids().count(),
        }
    }
}

/// Statistics about a replayer's recording.
#[derive(Debug, Clone)]
pub struct ReplayerStats {
    pub total_events: usize,
    pub adapter_events: usize,
    pub metadata_events: usize,
    pub node_count: usize,
    pub metadata_caller_count: usize,
}
