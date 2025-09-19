use dbt_telemetry::{AnyTelemetryEvent, TelemetryEventRecType, TelemetryExportFlags};
use serde::Serialize;

fn serialize_flags<S>(flags: &TelemetryExportFlags, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_u32(flags.bits())
}

// Mock dynamic span event with instance-based export flags
#[derive(Debug, Clone, PartialEq, Serialize, Default)]
pub struct MockDynSpanEvent {
    pub name: String,
    #[serde(serialize_with = "serialize_flags")]
    pub flags: TelemetryExportFlags,
    pub has_sensitive: bool,
    pub was_scrubbed: bool,
}

impl AnyTelemetryEvent for MockDynSpanEvent {
    fn event_type(&self) -> &'static str {
        "v1.public.events.fusion.dev.MockDynSpanEvent"
    }

    fn display_name(&self) -> String {
        format!("Mock Dyn Span Event: {}", self.name)
    }

    fn record_category(&self) -> TelemetryEventRecType {
        TelemetryEventRecType::Span
    }

    fn export_flags(&self) -> TelemetryExportFlags {
        self.flags
    }

    fn event_eq(&self, other: &dyn AnyTelemetryEvent) -> bool {
        other
            .as_any()
            .downcast_ref::<Self>()
            .is_some_and(|rhs| rhs == self)
    }

    fn has_sensitive_data(&self) -> bool {
        self.has_sensitive
    }

    fn clone_without_sensitive_data(&self) -> Option<Box<dyn AnyTelemetryEvent>> {
        Some(Box::new(Self {
            name: self.name.clone(),
            flags: self.flags,
            has_sensitive: self.has_sensitive,
            was_scrubbed: true,
        }))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn clone_box(&self) -> Box<dyn AnyTelemetryEvent> {
        Box::new(self.clone())
    }

    fn to_json(&self) -> Result<serde_json::Value, String> {
        serde_json::to_value(self).map_err(|e| format!("Failed to serialize: {e}"))
    }
}

// Mock dynamic log event with instance-based export flags
#[derive(Debug, Clone, PartialEq, Serialize, Default)]
pub struct MockDynLogEvent {
    pub code: i32,
    #[serde(serialize_with = "serialize_flags")]
    pub flags: TelemetryExportFlags,
    pub has_sensitive: bool,
    pub was_scrubbed: bool,
}

impl AnyTelemetryEvent for MockDynLogEvent {
    fn event_type(&self) -> &'static str {
        "v1.public.events.fusion.dev.MockDynLogEvent"
    }

    fn display_name(&self) -> String {
        format!("Mock Dyn Log Event: {}", self.code)
    }

    fn record_category(&self) -> TelemetryEventRecType {
        TelemetryEventRecType::Log
    }

    fn export_flags(&self) -> TelemetryExportFlags {
        self.flags
    }

    fn event_eq(&self, other: &dyn AnyTelemetryEvent) -> bool {
        other
            .as_any()
            .downcast_ref::<Self>()
            .is_some_and(|rhs| rhs == self)
    }

    fn has_sensitive_data(&self) -> bool {
        self.has_sensitive
    }

    fn clone_without_sensitive_data(&self) -> Option<Box<dyn AnyTelemetryEvent>> {
        Some(Box::new(Self {
            code: self.code,
            flags: self.flags,
            has_sensitive: self.has_sensitive,
            was_scrubbed: true,
        }))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn clone_box(&self) -> Box<dyn AnyTelemetryEvent> {
        Box::new(self.clone())
    }

    fn to_json(&self) -> Result<serde_json::Value, String> {
        serde_json::to_value(self).map_err(|e| format!("Failed to serialize: {e}"))
    }
}
