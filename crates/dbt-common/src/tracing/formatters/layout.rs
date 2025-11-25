use super::{
    color::{DIM, maybe_apply_color},
    constants::DEFAULT_TERMINAL_WIDTH,
};

/// Formats a centered delimiter line with '=' padding characters and optional DIM color.
///
/// This is used to create section headers in terminal output, such as
/// "Execution Summary", "Test Failures", and "Errors and Warnings".
///
/// # Arguments
/// * `text` - The text to center within the delimiter
/// * `width` - Optional terminal width. Uses DEFAULT_TERMINAL_WIDTH if None.
/// * `colorize` - Whether to apply DIM color styling to the delimiter
///
/// # Example
/// ```
/// let header = format_delimiter(" Execution Summary ", Some(80), true);
/// // Returns: DIM-colored "========================= Execution Summary ========================="
/// ```
pub fn format_delimiter(text: &str, width: Option<usize>, colorize: bool) -> String {
    let width = width.unwrap_or(DEFAULT_TERMINAL_WIDTH);
    let raw = format!("{:=^width$}", text, width = width);
    maybe_apply_color(&DIM, &raw, colorize)
}
