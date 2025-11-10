//! Formatter for log messages.
//!
//! This module provides formatters that add error code prefixes and optional
//! color formatting to messages.

use dbt_error::ErrorCode;
use dbt_telemetry::SeverityNumber;

use crate::{constants, pretty_string::color_quotes};

use super::color::{maybe_apply_color, severity_to_color_style};

pub fn severity_to_prefix(severity_number: SeverityNumber) -> Option<&'static str> {
    match severity_number {
        SeverityNumber::Error => Some(constants::ERROR),
        SeverityNumber::Warn => Some(constants::WARNING),
        SeverityNumber::Unspecified
        | SeverityNumber::Trace
        | SeverityNumber::Debug
        | SeverityNumber::Info => None,
    }
}

/// Format a message with "dbt{code}: " prefix and optional colorization.
///
/// # Arguments
/// * `error_code` - Optional error code to include in the prefix
/// * `message` - The error message text
/// * `message_severity` - The severity level of the message
/// * `colorize` - If true, applies red color to the prefix
/// * `include_level_prefix` - If true, includes the severity level prefix (ERROR, WARNING, etc.)
///
/// # Returns
/// Formatted string like "dbt1000: message text" or colored version
pub fn format_log_message(
    error_code: Option<ErrorCode>,
    message: impl AsRef<str>,
    message_severity: SeverityNumber,
    colorize: bool,
    include_level_prefix: bool,
) -> String {
    // Extract error code from the message
    let code_prefix = error_code.map(|c| format!("dbt{c}: ")).unwrap_or_default();

    let prefix =
        if include_level_prefix && let Some(prefix_text) = severity_to_prefix(message_severity) {
            let color_style = severity_to_color_style(message_severity);
            maybe_apply_color(color_style, &format!("{prefix_text} "), colorize)
        } else {
            "".to_string()
        };

    if colorize {
        return format!("{prefix}{code_prefix}{}", color_quotes(message.as_ref()));
    };

    format!("{prefix}{code_prefix}{}", message.as_ref())
}
