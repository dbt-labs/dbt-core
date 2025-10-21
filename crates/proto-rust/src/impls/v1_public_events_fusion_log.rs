use crate::v1::public::events::fusion::{compat::SeverityNumber, log::LogMessage};

impl LogMessage {
    /// Create a new LogMessage with just the original severity level and an error code.
    pub fn new_from_level_and_code(code: impl Into<u32>, level: impl Into<SeverityNumber>) -> Self {
        let original_severity_number = level.into();

        Self {
            code: Some(code.into()),
            dbt_core_event_code: None,
            original_severity_number: original_severity_number as i32,
            original_severity_text: original_severity_number.as_str().to_string(),
            package_name: None,
            // Auto-filled fields
            unique_id: None,
            file: None,
            line: None,
            phase: None,
        }
    }

    /// Create a new LogMessage with just the original severity level and nothing else.
    pub fn new_from_level(level: impl Into<SeverityNumber>) -> Self {
        let original_severity_number = level.into();

        Self {
            code: None,
            dbt_core_event_code: None,
            original_severity_number: original_severity_number as i32,
            original_severity_text: original_severity_number.as_str().to_string(),
            package_name: None,
            // Auto-filled fields
            unique_id: None,
            file: None,
            line: None,
            phase: None,
        }
    }
}
