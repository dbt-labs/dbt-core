//! Formatter for log messages.
//!
//! This module provides formatters that add error code prefixes and optional
//! color formatting to messages.

use dbt_error::ErrorCode;
use dbt_telemetry::{LogMessage, SeverityNumber};

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
/// * `message_attributes` - The LogMessage attributes containing the error code
/// * `message` - The error message text
/// * `message_severity` - The severity level of the message
/// * `colorize` - If true, applies red color to the prefix
/// * `include_level_prefix` - If true, includes the severity level prefix (ERROR, WARNING, etc.)
///
/// # Returns
/// Formatted string like "dbt1000: message text" or colored version
pub fn format_log_message(
    message_attributes: &LogMessage,
    message: &str,
    message_severity: SeverityNumber,
    colorize: bool,
    include_level_prefix: bool,
) -> String {
    // Extract error code from the message
    let code_prefix = message_attributes
        .code
        .and_then(|c| u16::try_from(c).ok())
        .and_then(|c| ErrorCode::try_from(c).ok())
        .map(|c| format!("dbt{c}: "))
        .unwrap_or_default();

    let prefix =
        if include_level_prefix && let Some(prefix_text) = severity_to_prefix(message_severity) {
            let color_style = severity_to_color_style(message_severity);
            maybe_apply_color(color_style, &format!("{prefix_text} "), colorize)
        } else {
            "".to_string()
        };

    format!("{prefix}{code_prefix}{}", color_quotes(message))
}
