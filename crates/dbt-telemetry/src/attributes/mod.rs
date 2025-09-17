//! Extensible telemetry attributes system.
//!
//! This module provides the infrastructure for defining custom telemetry event data types
//! also known as "attributes" that can be used in telemetry records, enabling downstream
//! users to extend the telemetry system with their own attribute types.

mod context;
mod export;
mod registry;
mod traits;
mod wrapper;

pub use context::TelemetryContext;
pub use export::TelemetryExportFlags;
pub use registry::TelemetryEventTypeRegistry;
pub use traits::{AnyTelemetryEvent, TelemetryEventRecType};
pub(crate) use traits::{ArrowSerializableTelemetryEvent, ProtoTelemetryEvent};
pub use wrapper::TelemetryAttributes;
