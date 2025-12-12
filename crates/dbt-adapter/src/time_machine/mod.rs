//! Time Machine: Cross-Version Record/Replay System
//!
//! This module provides infrastructure for recording and replaying adapter-level
//! behavior, enabling backward compatibility testing across Fusion versions.
//!
//! # Architecture
//!
//! The system captures events at two levels:
//!
//! 1. **BridgeAdapter** (sync): Jinja `adapter.xxx()` calls via `Object::call_method`
//! 2. **MetadataAdapter** (async): Schema discovery, freshness, and relation listing
//!
//! Events are emitted via an MPSC channel to a background writer task, ensuring
//! minimal overhead on the hot path.
//!
//! # Recording Format
//!
//! Recordings are stored as:
//! ```text
//! {invocation_id}/             # UUID v7 (time-ordered) e.g., 019384a5-b6c7-7def-8901-234567890abc
//!   header.json          # Metadata: engine version, adapter type
//!   events.ndjson.gz     # All events in arrival order (compressed)
//!   index.json           # Informational summary to help process events
//! ```
//!
//! # Global Recording Session
//!
//! The recorder uses a global singleton pattern to ensure it stays alive for the
//! entire Fusion run. Initialize it once at startup and access it from anywhere:
//!
//! ```ignore
//! use dbt_adapter::time_machine::{init_recording, global_recorder, shutdown_recording};
//! use dbt_common::cancellation::CancellationToken;
//!
//! // Initialize at start of run (returns a handle)
//! let handle = init_recording("./recordings/run-123", "snowflake", invocation_id, token)?;
//!
//! // BridgeAdapter: pass TimeMachine explicitly
//! let bridge = BridgeAdapter::with_time_machine(
//!     typed_adapter,
//!     db,
//!     cache,
//!     TimeMachine::recorder(global_recorder().unwrap().clone()),
//! );
//!
//! // MetadataAdapter: uses global_recorder() internally via with_metadata_recording()
//! // No explicit setup needed - it finds the global automatically
//!
//! // Shutdown at end of run
//! let result = handle.shutdown().await?;
//! println!("Recorded {} events", result.event_count);
//! ```
//!
//! # Semantic Categories
//!
//! Events are classified by their side effects for dependency analysis:
//!
//! - **MetadataRead**: Query database state without mutation (SELECT/SHOW)
//! - **Write**: Mutate database state (DDL/DML)
//! - **Cache**: Internal bookkeeping (no DB I/O)
//! - **Pure**: Local computation (no I/O at all)

use std::{
    io,
    sync::{Arc, OnceLock},
};

use dbt_common::cancellation::CancellationToken;
use parking_lot::Mutex;
use tokio::task::JoinHandle;

pub mod engine;
pub mod event;
pub mod event_recorder;
pub mod event_replay;
pub mod metadata;
pub mod semantic;
pub mod serde;
pub mod serializable;
mod serializable_impls;
pub mod writer;

// Re-export commonly used types
pub use engine::{EventReplayer, ReplayCallError, ReplayerStats, TimeMachine};
pub use event::{
    AdapterCallEvent, CatalogSchema, CatalogSchemas, MetadataCallArgs, MetadataCallEvent,
    NodeIndex, RecordedEvent, RecordingHeader,
};
pub use event_recorder::EventRecorder;
pub use event_replay::{
    Recording, ReplayDifference, ReplayError, ReplayMode, ReplayResult, validate_replay,
};

// Re-export time machine ordering type and provide conversion
use dbt_common::io_args::TimeMachineReplayOrdering;

impl From<TimeMachineReplayOrdering> for ReplayMode {
    fn from(ordering: TimeMachineReplayOrdering) -> Self {
        match ordering {
            TimeMachineReplayOrdering::Strict => ReplayMode::Strict,
            TimeMachineReplayOrdering::Semantic => ReplayMode::Semantic,
        }
    }
}
pub use metadata::{
    MetadataResultDeserialize, MetadataResultSerialize, args_create_schemas_if_not_exists,
    args_freshness, args_list_relations_in_parallel, args_list_relations_schemas,
    args_list_relations_schemas_by_patterns, args_list_udfs, with_time_machine_metadata_wrapper,
};
pub use semantic::SemanticCategory;
pub use serde::{
    ReplayContext, json_to_value, json_to_value_with_context, serialize_args, serialize_value,
    values_match,
};
pub use serializable::{
    DeserializeFn, JsonExtractor, TimeMachineSerializable, TypeEntry, deserialize_object, registry,
    serialize_object,
};
pub use writer::{RecordingResult, WriterConfig, spawn_writer};

/// Global recording session
struct RecordingSession {
    recorder: Arc<EventRecorder>,
    writer_handle: Mutex<Option<JoinHandle<io::Result<RecordingResult>>>>,
}

/// Global recording session singleton - initialized once, accessible everywhere.
static GLOBAL_SESSION: OnceLock<RecordingSession> = OnceLock::new();

/// Global replayer singleton - initialized once for replay mode.
static GLOBAL_REPLAYER: OnceLock<Arc<EventReplayer>> = OnceLock::new();

/// Initialize the global recording session.
///
/// This should be called at the start of a Fusion run. If recording is already
/// initialized (e.g., by another adapter), this is a no-op — the existing session
/// is used and the provided config is ignored.
///
/// # Arguments
///
/// * `output_path` - Directory to write recording files
/// * `adapter_type` - Type of adapter being recorded (e.g., "snowflake")
/// * `invocation_id` - Unique identifier for this run
/// * `token` - Cancellation token for graceful shutdown on CTRL+C
///
/// # Returns
///
/// A `RecordingHandle` that can be used to shut down the recording session.
pub fn get_or_init_recording(
    output_path: impl Into<std::path::PathBuf>,
    adapter_type: impl Into<String>,
    invocation_id: impl Into<String>,
    token: CancellationToken,
) -> RecordingHandle {
    let output_path = output_path.into();
    let adapter_type = adapter_type.into();
    let invocation_id = invocation_id.into();

    GLOBAL_SESSION.get_or_init(|| {
        let (recorder, receiver) = EventRecorder::new();
        let header = RecordingHeader::new(adapter_type, invocation_id);
        let config = WriterConfig::new(output_path);
        let writer_handle = spawn_writer(receiver, header, config, token);

        RecordingSession {
            recorder: Arc::new(recorder),
            writer_handle: Mutex::new(Some(writer_handle)),
        }
    });

    RecordingHandle { _private: () }
}

/// Get the global recorder, if initialized.
///
/// This is the primary way to access the recorder from anywhere in the codebase.
/// Returns `None` if recording has not been initialized.
///
/// # Example
///
/// ```ignore
/// if let Some(recorder) = global_recorder() {
///     recorder.record_adapter_call(
///         node_id,
///         "execute",
///         serde_json::json!(["SELECT 1"]),
///         serde_json::json!(null),
///         true,
///         None,
///     );
/// }
/// ```
pub fn global_recorder() -> Option<&'static Arc<EventRecorder>> {
    GLOBAL_SESSION.get().map(|s| &s.recorder)
}

/// Check if recording is currently active.
pub fn is_recording() -> bool {
    GLOBAL_SESSION.get().is_some()
}

/// Get or initialize the global replayer.
///
/// If a replayer is already set, returns a clone of it.
/// Otherwise, calls the closure to create one, stores it globally, and returns it.
///
/// This enables sharing the same replayer between `BridgeAdapter` (which holds
/// `TimeMachine` directly) and `MetadataAdapter` (which accesses via `global_replayer()`).
///
/// ```ignore
/// let replayer = get_or_init_replayer(|| {
///     Arc::new(EventReplayer::load(path)?.with_replay_mode(mode))
/// })?;
/// let bridge = BridgeAdapter::with_time_machine(..., TimeMachine::replayer(replayer));
/// ```
pub fn get_or_init_replayer<F>(f: F) -> Result<Arc<EventReplayer>, ReplayError>
where
    F: FnOnce() -> Result<Arc<EventReplayer>, ReplayError>,
{
    // Don't allow both recording and replay at the same time
    if is_recording() {
        return Err(ReplayError::InvalidFormat(
            "Cannot initialize replay while recording is active".to_string(),
        ));
    }

    // If already initialized, return the existing replayer
    if let Some(existing) = GLOBAL_REPLAYER.get() {
        return Ok(Arc::clone(existing));
    }

    // Create the replayer via closure
    let replayer = f()?;

    // Try to store it - if race lost, use the winner's replayer
    match GLOBAL_REPLAYER.set(Arc::clone(&replayer)) {
        Ok(()) => Ok(replayer),
        Err(_) => Ok(Arc::clone(GLOBAL_REPLAYER.get().unwrap())),
    }
}

/// Get the global replayer, if initialized.
///
/// This is the primary way to access the replayer from anywhere in the codebase.
/// Returns `None` if replay has not been initialized.
pub fn global_replayer() -> Option<&'static Arc<EventReplayer>> {
    GLOBAL_REPLAYER.get()
}

/// Check if replay is currently active.
pub fn is_replaying() -> bool {
    GLOBAL_REPLAYER.get().is_some()
}

/// Handle for the global replay session.
pub struct ReplayHandle {
    _private: (),
}

impl ReplayHandle {
    /// Get the replayer statistics.
    pub fn stats(&self) -> Option<ReplayerStats> {
        GLOBAL_REPLAYER.get().map(|r| r.stats())
    }

    /// Reset replay state for all callers.
    pub fn reset(&self) {
        if let Some(replayer) = GLOBAL_REPLAYER.get() {
            replayer.reset();
        }
    }
}

/// Handle for the global recording session.
///
/// This handle controls the lifetime of the recording session. When dropped
/// without calling `shutdown()`, it will log a warning but recording will
/// continue until the process exits.
///
/// For proper cleanup, call `shutdown()` which:
/// 1. Signals the recorder that no more events will be emitted
/// 2. Waits for the writer to finish flushing all events
/// 3. Returns the recording result with statistics
pub struct RecordingHandle {
    _private: (),
}

impl RecordingHandle {
    /// Shutdown the recording session and wait for all events to be written.
    ///
    /// This is the proper way to finalize a recording:
    /// 1. Close the recorder channel (signals writer to finish)
    /// 2. Wait for the writer to flush all buffered events to disk
    /// 3. Return the recording result with statistics
    ///
    /// After shutdown, the global recorder is still accessible but events
    /// will be silently dropped.
    ///
    /// # Returns
    ///
    /// The `RecordingResult` containing statistics and file paths.
    pub async fn shutdown(self) -> Result<RecordingResult, RecordingError> {
        let session = GLOBAL_SESSION.get().ok_or(RecordingError::NotInitialized)?;

        // Close the recorder to signal the writer to finish.
        // This drops the sender, which closes the channel.
        session.recorder.close();

        // Take the writer handle
        let handle = session
            .writer_handle
            .lock()
            .take()
            .ok_or(RecordingError::AlreadyShutdown)?;

        // Wait for the writer to drain remaining events and complete
        handle
            .await
            .map_err(|e| RecordingError::WriterPanic(e.to_string()))?
            .map_err(RecordingError::IoError)
    }

    /// Get statistics about the current recording session.
    pub fn stats(&self) -> Option<RecordingStats> {
        GLOBAL_SESSION.get().map(|s| RecordingStats {
            event_count: s.recorder.event_count(),
        })
    }
}

/// Statistics about an active recording session.
#[derive(Debug, Clone)]
pub struct RecordingStats {
    /// Total number of events emitted so far.
    pub event_count: u64,
}

/// Errors that can occur during recording operations.
#[derive(Debug)]
pub enum RecordingError {
    /// Recording was not initialized.
    NotInitialized,
    /// Recording was already shut down.
    AlreadyShutdown,
    /// Writer task panicked.
    WriterPanic(String),
    /// I/O error during writing.
    IoError(io::Error),
}

impl std::fmt::Display for RecordingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotInitialized => write!(f, "Recording is not initialized"),
            Self::AlreadyShutdown => write!(f, "Recording was already shut down"),
            Self::WriterPanic(e) => write!(f, "Writer task panicked: {}", e),
            Self::IoError(e) => write!(f, "I/O error: {}", e),
        }
    }
}

impl std::error::Error for RecordingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::IoError(e) => Some(e),
            _ => None,
        }
    }
}

#[allow(dead_code)] // Used in tests
/// NOT FOR PROD USE: Create a recording session with default configuration.
///
/// This is a convenience function for testing or to establish multiple
/// independent recording sessions. For production use, prefer `init_recording()`
/// which sets up a global singleton.
pub(crate) fn start_recording(
    output_path: impl Into<std::path::PathBuf>,
    adapter_type: impl Into<String>,
    invocation_id: impl Into<String>,
) -> (EventRecorder, JoinHandle<io::Result<RecordingResult>>) {
    let (recorder, receiver) = EventRecorder::new();
    let header = RecordingHeader::new(adapter_type, invocation_id);
    let config = WriterConfig::new(output_path);
    let writer_handle = spawn_writer(receiver, header, config, CancellationToken::never_cancels());

    (recorder, writer_handle)
}
