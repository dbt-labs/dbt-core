use console::Style;
use dbt_telemetry::{ProgressMessage, SeverityNumber};

use super::{
    color::{GREEN, PLAIN, RED, YELLOW, maybe_apply_color},
    layout::right_align_action,
};

/// Map severity number to corresponding color style.
///
/// Unlike with log messages, progress messages use green for info severity.
pub fn severity_to_color_style(severity_number: SeverityNumber) -> &'static Style {
    match severity_number {
        SeverityNumber::Info => &GREEN,
        SeverityNumber::Error => &RED,
        SeverityNumber::Warn => &YELLOW,
        SeverityNumber::Unspecified | SeverityNumber::Trace | SeverityNumber::Debug => &PLAIN,
    }
}

/// Format a progress message for display.
///
/// # Arguments
/// * `progress` - The progress message to format
/// * `message_severity` - The severity level of the message
/// * `pad_action` - Whether to right-pad the action field
/// * `colorize` - Whether to apply color formatting
///
/// # Returns
/// Formatted string with padded action and message
pub fn format_progress_message(
    progress: &ProgressMessage,
    message_severity: SeverityNumber,
    pad_action: bool,
    colorize: bool,
) -> String {
    // Right-pad action to ACTION_WIDTH characters
    let maybe_padded_action = if pad_action {
        right_align_action(progress.action.as_str())
    } else {
        progress.action.clone()
    };

    let action = if colorize {
        let style = severity_to_color_style(message_severity);
        maybe_apply_color(style, &maybe_padded_action, colorize)
    } else {
        maybe_padded_action
    };

    match progress.description.as_ref() {
        Some(desc) if !desc.is_empty() => {
            format!("{} {} ({})", action, progress.target, desc)
        }
        _ => format!("{} {}", action, progress.target),
    }
}
