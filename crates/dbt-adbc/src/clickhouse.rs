// Names of ClickHouse driver options -------------------------------------

/// Key prefix for passing a ClickHouse server setting through the driver:
/// `clickhouse.setting.<name>`, string-valued, accepted at Database,
/// Connection and Statement level; sent as a URL query parameter on every
/// HTTP request.
pub const SETTING_PREFIX: &str = "clickhouse.setting.";

/// Build the driver option key for a ClickHouse server setting.
pub fn setting_key(setting_name: &str) -> String {
    format!("{SETTING_PREFIX}{setting_name}")
}
