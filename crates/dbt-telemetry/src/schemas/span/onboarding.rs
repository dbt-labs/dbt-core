use crate::{
    TelemetryOutputFlags,
    attributes::{ArrowSerializableTelemetryEvent, ProtoTelemetryEvent, TelemetryEventRecType},
    serialize::arrow::ArrowAttributes,
};
use prost::Name;

pub use proto_rust::v1::public::events::fusion::onboarding::OnboardingScreenShown;

impl ProtoTelemetryEvent for OnboardingScreenShown {
    const RECORD_CATEGORY: TelemetryEventRecType = TelemetryEventRecType::Span;
    const OUTPUT_FLAGS: TelemetryOutputFlags = TelemetryOutputFlags::EXPORT_ALL;

    fn event_display_name(&self) -> String {
        format!("Onboarding screen shown: {}", self.screen().as_str_name())
    }

    fn has_sensitive_data(&self) -> bool {
        false
    }
}

impl ArrowSerializableTelemetryEvent for OnboardingScreenShown {
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
