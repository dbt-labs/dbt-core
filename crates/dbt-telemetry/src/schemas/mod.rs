// Trait implementations for span record type events
mod span;
// Trait implementations for log record type events
mod log;

// Re-export proto types for event attributes and top level envelope types directly
// for the outside world
pub use dbt_tracing::{
    LogRecordInfo, RecordCodeLocation, SpanEndInfo, SpanLinkInfo, SpanStartInfo, SpanStatus,
    StatusCode, TelemetryRecord, TelemetryRecordRef, TelemetryRecordType,
};
pub use log::*;
pub use span::*;
