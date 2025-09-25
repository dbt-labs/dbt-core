use crate::{
    TelemetryOutputFlags,
    attributes::{ArrowSerializableTelemetryEvent, ProtoTelemetryEvent, TelemetryEventRecType},
    serialize::arrow::ArrowAttributes,
};
use prost::Name;
use std::borrow::Cow;

pub use proto_rust::v1::public::events::fusion::artifact::{ArtifactType, ArtifactWritten};

impl ProtoTelemetryEvent for ArtifactWritten {
    const RECORD_CATEGORY: TelemetryEventRecType = TelemetryEventRecType::Span;
    const OUTPUT_FLAGS: TelemetryOutputFlags = TelemetryOutputFlags::EXPORT_ALL;

    fn event_display_name(&self) -> String {
        format!("Artifact: {}", self.relative_path)
    }

    fn has_sensitive_data(&self) -> bool {
        // path is always relative to project, so pretty much static (depends on our code)
        // and thus definitely not sensitive
        false
    }
}

impl ArrowSerializableTelemetryEvent for ArtifactWritten {
    fn to_arrow_record(&self) -> ArrowAttributes<'_> {
        ArrowAttributes {
            relative_path: Some(Cow::from(self.relative_path.as_str())),
            artifact_type: Some(self.artifact_type()),
            ..Default::default()
        }
    }

    fn from_arrow_record(record: &ArrowAttributes) -> Result<Self, String> {
        Ok(Self {
            relative_path: record
                .relative_path
                .as_deref()
                .map(str::to_string)
                .ok_or_else(|| {
                    format!(
                        "Missing `relative_path` for event type \"{}\"",
                        Self::full_name()
                    )
                })?,
            artifact_type: record.artifact_type.map(|v| v as i32).ok_or_else(|| {
                format!(
                    "Missing `artifact_type` for event type \"{}\"",
                    Self::full_name()
                )
            })?,
        })
    }
}
