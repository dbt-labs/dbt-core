use crate::v1::public::events::fusion::{
    compat::SeverityNumber,
    log::{LogMessage, ProgressMessage, ShowDataOutput, ShowDataOutputFormat, UserLogMessage},
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

impl ProgressMessage {
    /// Creates a new `ProgressMessage` with a known legacy dbt-core event code.
    ///
    /// Arguments:
    /// * `action` - The action being performed (e.g., "Debugging", "Loading").
    /// * `target` - The text describing the target of the action (e.g., "project", "dependencies").
    /// * `description` - Optional additional description providing more context.
    /// * `dbt_core_event_code` - legacy dbt-core event code
    pub fn new_with_code(
        action: String,
        target: String,
        description: Option<String>,
        dbt_core_event_code: String,
    ) -> Self {
        Self {
            action,
            target,
            description,
            dbt_core_event_code: Some(dbt_core_event_code),
            // Auto-injected fields
            unique_id: None,
            phase: None,
            file: None,
            line: None,
        }
    }
}

impl ShowDataOutput {
    /// Creates a new ShowDataOutput message.
    ///
    /// Arguments:
    /// * output_format - Format of the output
    /// * content - The data in the specified format (e.g., pretty-printed table)
    /// * node_name - Name of the node being shown (e.g., "my_model" or "inline")
    /// * is_inline - Whether this is an inline/ad-hoc query (true) or a defined node (false)
    /// * unique_id - The unique_id of the node being shown (e.g., "model.my_project.my_model"). Unset for ad-hoc queries
    /// * columns - Column names for the data
    pub fn new_with_default_code(
        output_format: ShowDataOutputFormat,
        content: String,
        node_name: String,
        is_inline: bool,
        unique_id: Option<String>,
        columns: Vec<String>,
    ) -> Self {
        Self::new(
            output_format,
            content,
            node_name,
            is_inline,
            unique_id,
            columns,
            "Q041".to_string(),
        )
    }
}

impl ShowDataOutputFormat {
    pub fn as_str(&self) -> &str {
        match self {
            ShowDataOutputFormat::Text => "text",
            ShowDataOutputFormat::Csv => "csv",
            ShowDataOutputFormat::Tsv => "tsv",
            ShowDataOutputFormat::Json => "json",
            ShowDataOutputFormat::Ndjson => "ndjson",
            ShowDataOutputFormat::Unspecified => "unspecified",
            ShowDataOutputFormat::Yml => "yml",
        }
    }
}
