use crate::proto::v1::public::events::fusion::phase::ExecutionPhase;
use std::{any::Any, fmt, sync::Arc};

/// Type-erased context extracted from spans/events and propagated to children and logs.
#[derive(Clone)]
pub struct TelemetryContext {
    value: Arc<dyn Any + Send + Sync>,
}

impl TelemetryContext {
    /// Wraps a concrete context value for propagation through the telemetry data layer.
    pub fn new<T>(value: T) -> Self
    where
        T: Any + Send + Sync,
    {
        Self {
            value: Arc::new(value),
        }
    }

    /// Returns the wrapped context value if it has the requested concrete type.
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.value.downcast_ref()
    }
}

impl fmt::Debug for TelemetryContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("TelemetryContext")
            .field(&self.value.type_id())
            .finish()
    }
}

/// dbt context extracted from dbt spans/events and propagated to children and logs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DbtTelemetryContext {
    /// Current execution phase, if any.
    pub phase: Option<ExecutionPhase>,
    /// Unique ID of the current node, if any.
    pub unique_id: Option<String>,
}

impl From<DbtTelemetryContext> for TelemetryContext {
    fn from(value: DbtTelemetryContext) -> Self {
        TelemetryContext::new(value)
    }
}
