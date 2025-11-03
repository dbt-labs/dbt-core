use crate::v1::public::events::fusion::{
    compat::SeverityNumber,
    log::{LogMessage, UserLogMessage},
};

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

impl UserLogMessage {
    /// Creates a new `UserLogMessage` event for print() calls.
    ///
    /// This is a helper that creates a UserLogMessage with the appropriate
    /// dbt core event code "Z052" (PrintEvent) and is_print set to true.
    pub fn print(
        package_name: Option<String>,
        line: Option<u32>,
        column: Option<u32>,
        relative_path: Option<String>,
    ) -> Self {
        Self {
            is_print: true,
            dbt_core_event_code: "Z052".to_string(),
            package_name,
            line,
            column,
            relative_path,
            // Auto-injected fields
            unique_id: None,
            phase: None,
        }
    }

    /// Creates a new `UserLogMessage` event for log(.., info=true) calls.
    ///
    /// This is a helper that creates a UserLogMessage with the appropriate
    /// dbt core event code (I062 - JinjaLogInfo) and is_print set to false.    
    pub fn log_info(
        package_name: Option<String>,
        line: Option<u32>,
        column: Option<u32>,
        relative_path: Option<String>,
    ) -> Self {
        Self {
            is_print: false,
            dbt_core_event_code: "I062".to_string(),
            package_name,
            line,
            column,
            relative_path,
            // Auto-injected fields
            unique_id: None,
            phase: None,
        }
    }

    /// Creates a new `UserLogMessage` event for log(.., info=true) calls.
    ///
    /// This is a helper that creates a UserLogMessage with the appropriate
    /// dbt core event code (I063 - JinjaLogDebug) and is_print set to false.    
    pub fn log_debug(
        package_name: Option<String>,
        line: Option<u32>,
        column: Option<u32>,
        relative_path: Option<String>,
    ) -> Self {
        Self {
            is_print: false,
            dbt_core_event_code: "I063".to_string(),
            package_name,
            line,
            column,
            relative_path,
            // Auto-injected fields
            unique_id: None,
            phase: None,
        }
    }
}
