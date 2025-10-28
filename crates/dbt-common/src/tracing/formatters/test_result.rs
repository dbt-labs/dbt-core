//! Formatter for test results.
//!
//! This module provides formatters for test failure messages with optional
//! color formatting.

use crate::pretty_string::RED;

use super::color::maybe_apply_color;

/// Format a test failure message with "Test failed: " prefix and optional colorization.
///
/// # Arguments
/// * `test_name` - The name of the test that failed
/// * `diff_table` - The diff table showing the test failure details
/// * `colorize` - If true, applies red color to the "Test failed:" prefix
///
/// # Returns
/// Formatted string like "Test failed: test_name\n{diff_table}\n" or colored version
pub fn format_test_failure(test_name: &str, diff_table: &str, colorize: bool) -> String {
    let prefix = maybe_apply_color(&RED, "Test failed: ", colorize);
    format!("{prefix}{test_name}\n{diff_table}")
}
