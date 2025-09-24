use crate::{
    TelemetryOutputFlags,
    attributes::{
        ArrowSerializableTelemetryEvent, ProtoTelemetryEvent, TelemetryContext,
        TelemetryEventRecType,
    },
    schemas::RecordCodeLocation,
    serialize::arrow::ArrowAttributes,
};
use prost::Name;
pub use proto_rust::v1::public::events::fusion::compat::SeverityNumber;
pub use proto_rust::v1::public::events::fusion::log::LogMessage;
use std::borrow::Cow;

impl ProtoTelemetryEvent for LogMessage {
    const RECORD_CATEGORY: TelemetryEventRecType = TelemetryEventRecType::Log;
    const OUTPUT_FLAGS: TelemetryOutputFlags = TelemetryOutputFlags::ALL;

    fn event_display_name(&self) -> String {
        format!("LogMessage ({})", self.code())
    }

    fn code_location(&self) -> Option<RecordCodeLocation> {
        Some(RecordCodeLocation {
            file: self.file.clone(),
            line: self.line,
            ..Default::default()
        })
    }

    fn with_code_location(&mut self, location: RecordCodeLocation) {
        // If we don't have a file yet, take it from the location.
        if let (None, Some(f)) = (self.file.clone(), location.file) {
            self.file = Some(f)
        }

        // If we don't have a line yet, take it from the location.
        if let (None, Some(l)) = (self.line, location.line) {
            self.line = Some(l)
        }
    }

    fn has_sensitive_data(&self) -> bool {
        false
    }

    fn with_context(&mut self, context: &TelemetryContext) {
        // Inject unique_id if not set and provided by context
        if self.unique_id.is_none() {
            self.unique_id = context.unique_id.clone();
        }

        // Inject phase if not set and provided by context
        if self.phase.is_none() {
            if let Some(p) = context.phase {
                self.phase = Some(p as i32);
            }
        }
    }
}

impl ArrowSerializableTelemetryEvent for LogMessage {
    fn to_arrow_record(&self) -> ArrowAttributes {
        ArrowAttributes {
            code: self.code,
            dbt_core_event_code: self.dbt_core_event_code.as_deref().map(Cow::Borrowed),
            original_severity_number: Some(self.original_severity_number),
            original_severity_text: Some(Cow::Borrowed(self.original_severity_text.as_str())),
            unique_id: self.unique_id.as_deref().map(Cow::Borrowed),
            phase: self.phase.map(|_| self.phase()),
            file: self.file.as_deref().map(Cow::Borrowed),
            line: self.line,
            ..Default::default()
        }
    }

    fn from_arrow_record(record: &ArrowAttributes) -> Result<Self, String> {
        Ok(Self {
            code: record.code,
            dbt_core_event_code: record.dbt_core_event_code.as_deref().map(str::to_string),
            original_severity_number: record.original_severity_number.ok_or_else(|| {
                format!(
                    "Missing severity number in event type \"{}\"",
                    Self::full_name()
                )
            })?,
            original_severity_text: record
                .original_severity_text
                .as_deref()
                .map(str::to_string)
                .ok_or_else(|| {
                    format!(
                        "Missing severity text in event type \"{}\"",
                        Self::full_name()
                    )
                })?,
            unique_id: record.unique_id.as_deref().map(str::to_string),
            phase: record.phase.map(|v| v as i32),
            file: record.file.as_deref().map(str::to_string),
            line: record.line,
        })
    }
}
