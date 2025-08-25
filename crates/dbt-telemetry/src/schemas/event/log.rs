use dbt_serde_yaml::JsonSchema;
#[cfg(test)]
use fake::Dummy;
use serde::{Deserialize, Serialize};

use super::super::{location::RecordCodeLocation, otlp::SeverityNumber};

#[cfg_attr(test, derive(Dummy))]
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct LogEventInfo {
    // TODO: use ErrorCode enum for error codes
    /// Option error/warning code
    pub code: Option<u32>,

    /// An optional legacy codes dbt-core code (e.g. "Z048")
    pub dbt_core_code: Option<String>,

    /// Numerical value of the severity, normalized to values described in OTEL Log Data Model.
    ///
    /// This is the original severity before user up/down-grade configuration applied
    pub original_severity_number: SeverityNumber,

    /// The severity text (also known as log level).
    ///
    /// This is the original severity before user up/down-grade configuration applied
    pub original_severity_text: String,

    #[serde(flatten)]
    pub location: RecordCodeLocation,
}

#[cfg_attr(test, derive(Dummy))]
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct LegacyLogEventInfo {
    /// Numerical value of the severity, normalized to values described in OTEL Log Data Model.
    ///
    /// This is the original severity before user up/down-grade configuration applied
    pub original_severity_number: SeverityNumber,

    /// The severity text (also known as log level).
    ///
    /// This is the original severity before user up/down-grade configuration applied
    pub original_severity_text: String,

    #[serde(flatten)]
    pub location: RecordCodeLocation,
}
