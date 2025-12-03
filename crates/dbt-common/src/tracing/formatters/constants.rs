/// Default terminal width used for formatting when actual terminal width is unavailable
pub(super) const DEFAULT_TERMINAL_WIDTH: usize = 60;

/// Maximum length for displaying schema information in formatted outputs
pub(super) const MAX_SCHEMA_DISPLAY_LEN: usize = 200;

/// Minimum width allocated for node type display in formatted outputs
pub(super) const MIN_NODE_TYPE_WIDTH: usize = 5; // Length of "model"

// Schema suffix for unit tests
pub(super) const UNIT_TEST_SCHEMA_SUFFIX: &str = "_dbt_test__audit";

/// Width for action labels
pub(super) const ACTION_WIDTH: usize = 10;
