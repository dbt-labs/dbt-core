use crate::{
    TelemetryOutputFlags,
    attributes::{ArrowSerializableTelemetryEvent, ProtoTelemetryEvent, TelemetryEventRecType},
    serialize::arrow::ArrowAttributes,
};
use prost::Name;

pub use proto_rust::v1::public::events::fusion::log::CompiledCodeInline;

impl ProtoTelemetryEvent for CompiledCodeInline {
    const RECORD_CATEGORY: TelemetryEventRecType = TelemetryEventRecType::Log;
    const OUTPUT_FLAGS: TelemetryOutputFlags = TelemetryOutputFlags::ALL;

    fn event_display_name(&self) -> String {
        "Compiled SQL (inline)".to_string()
    }

    fn has_sensitive_data(&self) -> bool {
        true
    }

    fn clone_without_sensitive_data(
        &self,
    ) -> Option<Box<dyn crate::attributes::AnyTelemetryEvent>> {
        // Drop this event entirely when scrubbing sensitive data.
        None
    }
}

impl ArrowSerializableTelemetryEvent for CompiledCodeInline {
    fn to_arrow_record(&self) -> ArrowAttributes<'_> {
        ArrowAttributes {
            json_payload: serde_json::to_string(self)
                .expect("Failed to serialize CompiledCodeInline to JSON")
                .into(),
            ..Default::default()
        }
    }

    fn from_arrow_record(record: &ArrowAttributes) -> Result<Self, String> {
        serde_json::from_str(record.json_payload.as_ref().ok_or_else(|| {
            format!(
                "Missing json payload for event type \"{}\"",
                Self::full_name()
            )
        })?)
        .map_err(|e| {
            format!(
                "Failed to deserialize {} from JSON payload: {}",
                Self::full_name(),
                e
            )
        })
    }
}
