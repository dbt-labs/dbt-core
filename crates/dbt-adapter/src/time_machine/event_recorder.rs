//! Event recorder with MPSC channel for non-blocking event emission.
//!
//! The recorder provides a unified interface for emitting events from both
//! synchronous (BridgeAdapter) and asynchronous (MetadataAdapter) contexts.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::Instant;

use std::sync::Arc;
use tokio::sync::mpsc;

use super::event::{
    AdapterCallEvent, CatalogSchema, CatalogSchemas, MetadataCallArgs, MetadataCallEvent,
    RecordedEvent,
};
use super::semantic::SemanticCategory;

/// Number of events to keep in buffer
const CHANNEL_BUFFER_SIZE: usize = 10_000;

/// Event recorder that captures adapter operations via MPSC channel.
#[derive(Clone)]
pub struct EventRecorder {
    /// Channel sender for emitting events
    sender: mpsc::Sender<RecordedEvent>,
    /// Per-node sequence counters
    seq_counters: Arc<dashmap::DashMap<String, AtomicU32>>,
    /// Recording start time for relative timestamps
    start_time: Instant,
    /// Global event counter
    event_count: Arc<AtomicU64>,
    /// Closed flag - when true, emits are no-ops
    closed: Arc<AtomicBool>,
}

impl EventRecorder {
    /// Create a new event recorder.
    ///
    /// Returns the recorder and the receiver end of the channel.
    /// The caller is responsible for spawning the writer task with the receiver.
    pub fn new() -> (Self, mpsc::Receiver<RecordedEvent>) {
        let (sender, receiver) = mpsc::channel(CHANNEL_BUFFER_SIZE);

        let recorder = Self {
            sender,
            seq_counters: Arc::new(dashmap::DashMap::new()),
            start_time: Instant::now(),
            event_count: Arc::new(AtomicU64::new(0)),
            closed: Arc::new(AtomicBool::new(false)),
        };

        (recorder, receiver)
    }

    /// Mark the recorder as closed.
    ///
    /// After calling this, further events will be silently dropped.
    /// The channel closes when this EventRecorder (and all clones) is dropped,
    /// allowing the writer to drain remaining events.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    /// Check if the recorder has been closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    /// Get the next sequence number for a node (thread-safe).
    #[inline]
    pub fn next_seq(&self, node_id: &str) -> u32 {
        // Fast path: check existing entry without allocation
        if let Some(counter) = self.seq_counters.get(node_id) {
            return counter.fetch_add(1, Ordering::Relaxed);
        }
        // Slow path: insert new counter
        self.seq_counters
            .entry(node_id.to_string())
            .or_insert_with(|| AtomicU32::new(0))
            .fetch_add(1, Ordering::Relaxed)
    }

    /// Get elapsed time since recording start in nanoseconds.
    pub fn elapsed_ns(&self) -> u64 {
        self.start_time.elapsed().as_nanos() as u64
    }

    /// Get total number of events emitted.
    pub fn event_count(&self) -> u64 {
        self.event_count.load(Ordering::Relaxed)
    }

    /// Emit an event from a sync context.
    ///
    /// Uses `try_send` to avoid blocking. If the channel is full,
    /// the event is dropped. This is a tradeoff to avoid performance
    /// regressions in synchronous code paths.
    #[inline]
    pub fn emit_sync(&self, event: RecordedEvent) {
        if self.is_closed() {
            return;
        }
        self.event_count.fetch_add(1, Ordering::Relaxed);
        let _ = self.sender.try_send(event);
    }

    /// Emit an event from an async context with backpressure.
    ///
    /// Awaits if the channel is full, applying backpressure to the caller.
    /// This is preferable for memory-constrained scenarios.
    pub async fn emit_async(&self, event: RecordedEvent) {
        if self.is_closed() {
            return;
        }
        self.event_count.fetch_add(1, Ordering::Relaxed);
        let _ = self.sender.send(event).await;
    }

    // -------------------------------------------------------------------------
    // Helper methods for recording specific event types
    // -------------------------------------------------------------------------

    /// Record an adapter call from BridgeAdapter::call_method.
    ///
    /// This is the primary entry point for recording Jinja adapter.xxx() calls.
    #[allow(clippy::too_many_arguments)]
    pub fn record_adapter_call(
        &self,
        node_id: impl Into<String>,
        method: impl Into<String>,
        args: serde_json::Value,
        result: serde_json::Value,
        success: bool,
        error: Option<String>,
    ) {
        let node_id = node_id.into();
        let method = method.into();
        let seq = self.next_seq(&node_id);
        let semantic_category = SemanticCategory::from_adapter_method(&method);

        self.emit_sync(RecordedEvent::AdapterCall(AdapterCallEvent {
            node_id,
            seq,
            method,
            semantic_category,
            args,
            result,
            success,
            error,
            timestamp_ns: self.elapsed_ns(),
        }));
    }

    /// Record a metadata adapter call.
    ///
    /// This is for async MetadataAdapter methods.
    #[allow(clippy::too_many_arguments)]
    pub async fn record_metadata_call(
        &self,
        caller_id: impl Into<String>,
        method: impl Into<String>,
        args: MetadataCallArgs,
        result: serde_json::Value,
        success: bool,
        error: Option<String>,
        duration_ms: u64,
    ) {
        let caller_id = caller_id.into();
        let method = method.into();
        let seq = self.next_seq(&caller_id);
        let semantic_category = SemanticCategory::from_metadata_method(&method);

        self.emit_async(RecordedEvent::MetadataCall(MetadataCallEvent {
            caller_id,
            seq,
            method,
            semantic_category,
            args,
            result,
            success,
            error,
            duration_ms,
            timestamp_ns: self.elapsed_ns(),
        }))
        .await;
    }
}

impl Default for EventRecorder {
    fn default() -> Self {
        Self::new().0
    }
}

impl std::fmt::Debug for EventRecorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventRecorder")
            .field("event_count", &self.event_count())
            .field("is_closed", &self.is_closed())
            .finish()
    }
}

/// Create MetadataCallArgs for list_relations_schemas.
pub fn args_list_relations_schemas(
    unique_id: Option<String>,
    phase: Option<String>,
    relations: impl IntoIterator<Item = impl AsRef<str>>,
) -> MetadataCallArgs {
    MetadataCallArgs::ListRelationsSchemas {
        unique_id,
        phase,
        relations: relations
            .into_iter()
            .map(|r| r.as_ref().to_string())
            .collect(),
    }
}

/// Create MetadataCallArgs for list_relations_in_parallel.
pub fn args_list_relations_in_parallel(
    db_schemas: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
) -> MetadataCallArgs {
    MetadataCallArgs::ListRelationsInParallel {
        db_schemas: db_schemas
            .into_iter()
            .map(|(c, s)| CatalogSchema {
                catalog: c.into(),
                schema: s.into(),
            })
            .collect(),
    }
}

/// Create MetadataCallArgs for freshness.
pub fn args_freshness(relations: impl IntoIterator<Item = impl AsRef<str>>) -> MetadataCallArgs {
    MetadataCallArgs::Freshness {
        relations: relations
            .into_iter()
            .map(|r| r.as_ref().to_string())
            .collect(),
    }
}

/// Create MetadataCallArgs for list_user_defined_functions.
pub fn args_list_udfs(
    catalog_schemas: impl IntoIterator<
        Item = (
            impl Into<String>,
            impl IntoIterator<Item = impl Into<String>>,
        ),
    >,
) -> MetadataCallArgs {
    MetadataCallArgs::ListUserDefinedFunctions {
        catalog_schemas: catalog_schemas
            .into_iter()
            .map(|(c, schemas)| CatalogSchemas {
                catalog: c.into(),
                schemas: schemas.into_iter().map(|s| s.into()).collect(),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_recorder_basic() {
        let (recorder, mut receiver) = EventRecorder::new();

        // Emit a sync event
        recorder.record_adapter_call(
            "model.test.orders",
            "execute",
            serde_json::json!(["SELECT 1"]),
            serde_json::json!({"rows": 1}),
            true,
            None,
        );

        // Should receive the event
        let event = receiver.recv().await.unwrap();
        assert_eq!(event.node_id(), "model.test.orders");
        assert_eq!(event.seq(), 0);
    }

    #[tokio::test]
    async fn test_sequence_numbers() {
        let (recorder, mut receiver) = EventRecorder::new();

        // Emit multiple events for same node
        for i in 0..3 {
            recorder.record_adapter_call(
                "model.test.orders",
                "execute",
                serde_json::json!([format!("query {}", i)]),
                serde_json::json!(null),
                true,
                None,
            );
        }

        // Verify sequence numbers
        for expected_seq in 0..3 {
            let event = receiver.recv().await.unwrap();
            assert_eq!(event.seq(), expected_seq);
        }
    }

    #[tokio::test]
    async fn test_multiple_nodes() {
        let (recorder, mut receiver) = EventRecorder::new();

        // Events for different nodes
        recorder.record_adapter_call(
            "node_a",
            "execute",
            serde_json::json!([]),
            serde_json::json!(null),
            true,
            None,
        );
        recorder.record_adapter_call(
            "node_b",
            "execute",
            serde_json::json!([]),
            serde_json::json!(null),
            true,
            None,
        );
        recorder.record_adapter_call(
            "node_a",
            "execute",
            serde_json::json!([]),
            serde_json::json!(null),
            true,
            None,
        );

        // node_a should have seq 0, 1
        // node_b should have seq 0
        let e1 = receiver.recv().await.unwrap();
        let e2 = receiver.recv().await.unwrap();
        let e3 = receiver.recv().await.unwrap();

        assert_eq!((e1.node_id(), e1.seq()), ("node_a", 0));
        assert_eq!((e2.node_id(), e2.seq()), ("node_b", 0));
        assert_eq!((e3.node_id(), e3.seq()), ("node_a", 1));
    }
}
