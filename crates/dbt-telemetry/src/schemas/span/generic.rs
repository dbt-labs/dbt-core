use crate::{
    TelemetryOutputFlags,
    attributes::{ArrowSerializableTelemetryEvent, ProtoTelemetryEvent, TelemetryEventRecType},
    serialize::arrow::ArrowAttributes,
};
use prost::Name;

pub use crate::proto::v1::public::events::fusion::generic::{
    GenericOpExecuted, GenericOpItemProcessed,
};

impl ProtoTelemetryEvent for GenericOpExecuted {
    const RECORD_CATEGORY: TelemetryEventRecType = TelemetryEventRecType::Span;
    const OUTPUT_FLAGS: TelemetryOutputFlags = TelemetryOutputFlags::ALL;

    fn event_display_name(&self) -> String {
        match self.item_count_total {
            Some(c) => format!("GenericOp: {} ({} items)", self.display_action, c),
            None => format!("GenericOp: {}", self.display_action),
        }
    }

    fn has_sensitive_data(&self) -> bool {
        false
    }
}

impl ArrowSerializableTelemetryEvent for GenericOpExecuted {
    fn to_arrow_record(&self) -> ArrowAttributes<'_> {
        ArrowAttributes {
            json_payload: serde_json::to_string(self)
                .unwrap_or_else(|_| {
                    panic!(
                        "Failed to serialize event type \"{}\" to JSON",
                        Self::full_name()
                    )
                })
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
                "Failed to deserialize event type \"{}\" from JSON: {}",
                Self::full_name(),
                e
            )
        })
    }
}

impl ProtoTelemetryEvent for GenericOpItemProcessed {
    const RECORD_CATEGORY: TelemetryEventRecType = TelemetryEventRecType::Span;
    const OUTPUT_FLAGS: TelemetryOutputFlags = TelemetryOutputFlags::ALL;

    fn event_display_name(&self) -> String {
        format!(
            "GenericOpItem: {} ({})",
            self.display_in_progress_action, self.target
        )
    }

    fn has_sensitive_data(&self) -> bool {
        false
    }
}

impl ArrowSerializableTelemetryEvent for GenericOpItemProcessed {
    fn to_arrow_record(&self) -> ArrowAttributes<'_> {
        ArrowAttributes {
            json_payload: serde_json::to_string(self)
                .unwrap_or_else(|_| {
                    panic!(
                        "Failed to serialize event type \"{}\" to JSON",
                        Self::full_name()
                    )
                })
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
                "Failed to deserialize event type \"{}\" from JSON: {}",
                Self::full_name(),
                e
            )
        })
    }
}
