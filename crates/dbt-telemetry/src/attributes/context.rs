use proto_rust::v1::public::events::fusion::phase::ExecutionPhase;

/// Context we can extract from spans/events and propagate to children and logs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TelemetryContext {
    /// Current execution phase, if any.
    pub phase: Option<ExecutionPhase>,
    /// Unique ID of the current node, if any.
    pub unique_id: Option<String>,
}
