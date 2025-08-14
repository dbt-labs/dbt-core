//! Types/models matching the OpenTelemetry OTLP proto specs used by
//! dbt-fusion for telemetry.
//!
//! These are not generated or imported from opentelemetry-proto for three reasons:
//! 1. Only a handful of native models from the OTLP spec are used as is
//! 2. We derive JSON Schema for these structs to allow for validation and documentation generation
//! 3. We subset some of the enums as we are not using all of the values defined in the OTLP spec

use dbt_serde_yaml::JsonSchema;
use schemars::JsonSchema_repr;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use strum::FromRepr;

#[derive(
    Serialize_repr, Deserialize_repr, Debug, JsonSchema_repr, Clone, Copy, PartialEq, Eq, FromRepr,
)]
#[repr(u8)]
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
#[derive(Serialize, Deserialize, Debug, JsonSchema, Clone, Default, PartialEq, Eq)]
pub struct SpanStatus {
    pub message: Option<String>,
    pub code: StatusCode,
}

/// Possible values for LogRecord.SeverityNumber.
#[derive(
    Serialize_repr,
    Deserialize_repr,
    Debug,
    JsonSchema_repr,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    FromRepr,
)]
#[repr(u8)]
pub enum SeverityNumber {
    #[default]
    Trace = 1,
    Debug = 5,
    Info = 9,
    Warn = 13,
    Error = 17,
    Fatal = 21,
}
