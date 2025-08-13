//! Types/models exactly matching the OpenTelemetry OTLP proto specs used by
//! dbt-fusion for telemetry.
//!
//! These are not generated or imported from opentelemetry-proto for two reasons:
//! 1. Only a handful of native models from the OTLP spec are used as is
//! 2. We derive JSON Schema for these structs to allow for validation and documentation generation

use dbt_serde_yaml::JsonSchema;
use schemars::JsonSchema_repr;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

#[derive(Serialize_repr, Deserialize_repr, Debug, JsonSchema_repr, Clone, PartialEq, Eq)]
#[repr(i32)]
#[derive(Default)]
pub enum StatusCode {
    /// The default status.
    #[default]
    Unset = 0,
    /// The Span has been validated by an Application developer or Operator to
    /// have completed successfully.
    Ok = 1,
    /// The Span contains an error.
    Error = 2,
}

/// The final status of a span
#[derive(Serialize, Deserialize, Debug, JsonSchema, Clone, Default)]
pub struct SpanStatus {
    pub message: Option<String>,
    pub code: StatusCode,
}

/// Possible values for LogRecord.SeverityNumber.
#[derive(
    Serialize_repr, Deserialize_repr, Debug, JsonSchema_repr, Clone, Default, PartialEq, Eq,
)]
#[repr(i32)]
pub enum SeverityNumber {
    #[default]
    Trace = 1,
    Trace2 = 2,
    Trace3 = 3,
    Trace4 = 4,
    Debug = 5,
    Debug2 = 6,
    Debug3 = 7,
    Debug4 = 8,
    Info = 9,
    Info2 = 10,
    Info3 = 11,
    Info4 = 12,
    Warn = 13,
    Warn2 = 14,
    Warn3 = 15,
    Warn4 = 16,
    Error = 17,
    Error2 = 18,
    Error3 = 19,
    Error4 = 20,
    Fatal = 21,
    Fatal2 = 22,
    Fatal3 = 23,
    Fatal4 = 24,
}
