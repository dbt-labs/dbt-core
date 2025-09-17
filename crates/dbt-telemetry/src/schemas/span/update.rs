use crate::{AnyTelemetryEvent, TelemetryExportFlags, attributes::TelemetryEventRecType};
use dbt_serde_yaml::Value as JsonValue;
use proto_rust::StaticName;
pub use proto_rust::v1::public::events::fusion::update::PackageUpdate;
use std::any::Any;

// Our shorthand `ProtoTelemetryEvent` trait requires arrow trait implementation,
// and since this one is not exported to Parquet, we have to implement the
// `AnyTelemetryEvent` methods directly.

impl AnyTelemetryEvent for PackageUpdate {
    fn event_type(&self) -> &'static str {
        Self::FULL_NAME
    }

    fn event_eq(&self, other: &dyn AnyTelemetryEvent) -> bool {
        self.event_type() == other.event_type()
            && other
                .as_any()
                .downcast_ref::<Self>()
                .is_some_and(|other| self == other)
    }

    fn display_name(&self) -> String {
        format!("Update: {} -> {}", self.package, self.version)
    }

    fn record_category(&self) -> TelemetryEventRecType {
        TelemetryEventRecType::Span
    }

    fn export_flags(&self) -> TelemetryExportFlags {
        TelemetryExportFlags::EXPORT_JSONL_AND_OTLP
    }

    fn has_sensitive_data(&self) -> bool {
        true
    }

    fn clone_without_sensitive_data(&self) -> Option<Box<dyn AnyTelemetryEvent>> {
        // PackageUpdate is considered sensitive as it may carry sensitive path
        // in the `exe_path` field. We strip it out here.
        Some(Box::new(PackageUpdate {
            package: self.package.clone(),
            version: self.version.clone(),
            exe_path: None,
        }))
    }

    /// Helper for downcasting to concrete types.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Helper for downcasting to concrete types (mutable).
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    /// Clone the event as a boxed trait object.
    fn clone_box(&self) -> Box<dyn AnyTelemetryEvent> {
        Box::new(self.clone())
    }

    fn to_json(&self) -> Result<JsonValue, String> {
        dbt_serde_yaml::to_value(self).map_err(|e| format!("Failed to serialize: {e}"))
    }
}
