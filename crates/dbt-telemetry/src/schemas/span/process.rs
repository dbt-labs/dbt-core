use super::super::constants::{TELEMETRY_SCHEMA_URL, TELEMETRY_SCHEMA_VERSION};

use dbt_serde_yaml::JsonSchema;
#[cfg(test)]
use fake::Dummy;
use serde::{Deserialize, Serialize};

#[cfg_attr(test, derive(Dummy))]
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ProcessInfo {
    /// Json Schema URL for the telemetry schema.
    pub schema_url: String,
    /// Schema version
    pub schema_version: u16,
    /// name of the package emitting the telemetry, e.g. `dbt` or `dbt-lsp`
    pub package: String,
    /// dbt fusion version, e.g. "1.2.3"
    pub version: String,
    /// The host operating system, e.g. "linux", "darwin", "windows"
    pub host_os: String,
    /// The host architecture, e.g. "x86_64", "aarch64"
    pub host_arch: String,
}

impl ProcessInfo {
    /// Creates a new instance of `ProcessInfo` with the current process information.
    pub fn new(package: &str) -> Self {
        Self {
            schema_url: TELEMETRY_SCHEMA_URL.to_string(),
            schema_version: TELEMETRY_SCHEMA_VERSION,
            package: package.to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            host_os: std::env::consts::OS.to_string(),
            host_arch: std::env::consts::ARCH.to_string(),
        }
    }
}
