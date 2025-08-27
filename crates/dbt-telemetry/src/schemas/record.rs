//! Telemetry record definitions for dbt Fusion.

use std::time::SystemTime;

use super::{
    super::serialize::{
        deserialize_optional_span_id, deserialize_span_id, deserialize_timestamp,
        deserialize_trace_id, serialize_optional_span_id, serialize_span_id, serialize_timestamp,
        serialize_trace_id,
    },
    event::artifact::WriteArtifactInfo,
    event::log::{LegacyLogEventInfo, LogEventInfo},
    location::RecordCodeLocation,
    otlp::{SeverityNumber, SpanStatus},
    span::dev::{DevInternalInfo, UnknownInfo},
    span::invocation::InvocationInfo,
    span::node::NodeInfo,
    span::onboarding::OnboardingInfo,
    span::phase::BuildPhaseInfo,
    span::process::ProcessInfo,
    span::update::UpdateInfo,
};
use dbt_serde_yaml::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
#[cfg(test)]
use strum::EnumIter;
use strum::{AsRefStr, EnumDiscriminants, IntoStaticStr};

#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, PartialEq)]
pub struct SpanStartInfo {
    /// Unique identifier for a trace. All spans from the same trace share
    /// the same `trace_id`. 16-byte identifier stored as 32-character hex string (invalid if all zeroes).
    #[serde(
        serialize_with = "serialize_trace_id",
        deserialize_with = "deserialize_trace_id"
    )]
    #[schemars(with = "String")]
    pub trace_id: u128,

    /// Unique identifier for a span within a trace, assigned when the span
    /// is created. 8-byte identifier stored as 16-character hex string (invalid if all zeroes).
    #[serde(
        serialize_with = "serialize_span_id",
        deserialize_with = "deserialize_span_id"
    )]
    #[schemars(with = "String")]
    pub span_id: u64,

    /// A description of the span's operation.
    pub span_name: String,

    /// The `span_id` of this span's parent span. Empty for root spans.
    #[serde(
        serialize_with = "serialize_optional_span_id",
        deserialize_with = "deserialize_optional_span_id"
    )]
    #[schemars(with = "Option<String>")]
    pub parent_span_id: Option<u64>,

    /// Start time of the span as UNIX timestamp in nanoseconds.
    #[serde(
        serialize_with = "serialize_timestamp",
        deserialize_with = "deserialize_timestamp"
    )]
    #[schemars(with = "String")]
    pub start_time_unix_nano: SystemTime,

    /// Severity level as a number (OpenTelemetry standard values).
    pub severity_number: SeverityNumber,

    /// Severity level as text: "DEBUG", "INFO", "WARNING", "ERROR", "TRACE".
    pub severity_text: String,

    /// Structured attributes for this span using a discriminated union type.
    /// Serialized as: `{ trace_id: "...", ..., "event_type": "discriminator", "attributes": { ... } }`
    #[serde(flatten)]
    pub attributes: TelemetryAttributes,
}

#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, PartialEq)]
pub struct SpanEndInfo {
    /// Unique identifier for a trace. All spans from the same trace share
    /// the same `trace_id`. 16-byte identifier stored as 32-character hex string (invalid if all zeroes).
    #[serde(
        serialize_with = "serialize_trace_id",
        deserialize_with = "deserialize_trace_id"
    )]
    #[schemars(with = "String")]
    pub trace_id: u128,

    /// Unique identifier for a span within a trace, assigned when the span
    /// is created. 8-byte identifier stored as 16-character hex string (invalid if all zeroes).
    #[serde(
        serialize_with = "serialize_span_id",
        deserialize_with = "deserialize_span_id"
    )]
    #[schemars(with = "String")]
    pub span_id: u64,

    /// A description of the span's operation.
    pub span_name: String,

    /// The `span_id` of this span's parent span. Empty for root spans.
    #[serde(
        serialize_with = "serialize_optional_span_id",
        deserialize_with = "deserialize_optional_span_id"
    )]
    #[schemars(with = "Option<String>")]
    pub parent_span_id: Option<u64>,

    /// Start time of the span as UNIX timestamp in nanoseconds.
    #[serde(
        serialize_with = "serialize_timestamp",
        deserialize_with = "deserialize_timestamp"
    )]
    #[schemars(with = "String")]
    pub start_time_unix_nano: SystemTime,

    /// End time of the span as UNIX timestamp in nanoseconds.
    #[serde(
        serialize_with = "serialize_timestamp",
        deserialize_with = "deserialize_timestamp"
    )]
    #[schemars(with = "String")]
    pub end_time_unix_nano: SystemTime,

    /// Severity level as a number (OpenTelemetry standard values).
    pub severity_number: SeverityNumber,

    /// Severity level as text: "DEBUG", "INFO", "WARNING", "ERROR", "TRACE".
    pub severity_text: String,

    /// Final status for this span. When not set, assumes unset status (code = 0).
    pub status: Option<SpanStatus>,

    /// Structured attributes for this span using a discriminated union type.
    /// Serialized as: `{ trace_id: "...", ..., "event_type": "discriminator", "attributes": { ... } }`
    #[serde(flatten)]
    pub attributes: TelemetryAttributes,
}

#[derive(Serialize, Deserialize, Debug, JsonSchema, Clone, PartialEq)]
pub struct LogRecordInfo {
    /// Unique identifier for a trace. All logs from the same trace share
    /// the same `trace_id`. 16-byte identifier stored as 32-character hex string (invalid if all zeroes).
    #[serde(
        serialize_with = "serialize_trace_id",
        deserialize_with = "deserialize_trace_id"
    )]
    #[schemars(with = "String")]
    pub trace_id: u128,

    /// Unique identifier for the span active when the log is created.
    /// 8-byte identifier stored as 16-character hex string (invalid if all zeroes).
    #[serde(
        serialize_with = "serialize_optional_span_id",
        deserialize_with = "deserialize_optional_span_id"
    )]
    #[schemars(with = "Option<String>")]
    pub span_id: Option<u64>,

    /// Name of the span active when the log is created.
    /// Used to simplify log grouping without span lookups.
    pub span_name: Option<String>,

    /// Time when the event occurred as UNIX timestamp in nanoseconds.
    /// Value of 0 indicates unknown or missing timestamp.
    #[serde(
        serialize_with = "serialize_timestamp",
        deserialize_with = "deserialize_timestamp"
    )]
    #[schemars(with = "String")]
    pub time_unix_nano: SystemTime,

    /// Severity level as a number (OpenTelemetry standard values).
    pub severity_number: SeverityNumber,

    /// Severity level as text: "DEBUG", "INFO", "WARNING", "ERROR", "TRACE".
    pub severity_text: String,

    /// Human-readable message describing the event.
    pub body: String,

    /// Structured attributes for this log using a discriminated union type.
    /// Serialized as: `{ trace_id: "...", ..., "event_type": "discriminator", "attributes": { ... } }`
    #[serde(flatten)]
    pub attributes: TelemetryAttributes,
}

/// Represents a telemetry record which loosely follows OpenTelemetry
/// log and trace signal logical models & semantics (but not OTLP schema!)
/// and combines them under a single enum type.
///
/// This is a discriminated union on `record_type` field, which is not part of the OTLP schema.
#[derive(Serialize, Deserialize, Debug, JsonSchema, EnumDiscriminants, PartialEq)]
#[serde(tag = "record_type")]
// The following derives a variant disciriminator enum for the telemetry records,
// used for type-safe (de)serialization and matching.
#[strum_discriminants(derive(Serialize, Deserialize), name(TelemetryRecordType))]
pub enum TelemetryRecord {
    /// # Span Start
    /// Represents the start of a span in a trace.
    ///
    /// This is a partial-span record emitted as soon as the span is created.
    /// The corresponding `SpanEnd` event is guaranteed to have the same
    /// values for all same-named fields except attributes and
    SpanStart(SpanStartInfo),

    /// # Span
    /// Represents a completed span in a trace.
    ///
    /// This is a full-span record emitted when the span is completed.
    SpanEnd(SpanEndInfo),

    /// # Log Record
    ///
    /// Represents a log record, which is a structured point in time event that can be emitted
    /// during the execution of a span.
    LogRecord(LogRecordInfo),
}

/// A reference to a telemetry record, used in tracing to avoiding cloning. Make sure
/// it matches the `TelemetryRecord` enum.
#[derive(Serialize)]
#[serde(tag = "record_type")]
pub enum TelemetryRecordRef<'a> {
    /// # Span Start
    /// Represents the start of a span in a trace.
    ///
    /// This is a partial-span record emitted as soon as the span is created.
    /// The corresponding `SpanEnd` event is guaranteed to have the same
    /// values for all same-named fields except attributes and
    SpanStart(&'a SpanStartInfo),

    /// # Span
    /// Represents a span in a trace.
    ///
    /// This is a full-span record emitted when the span is completed.
    SpanEnd(&'a SpanEndInfo),

    /// # Log Record
    ///
    /// Represents a log record, which is a structured point in time event that can be emitted
    /// during the execution of a span.
    LogRecord(&'a LogRecordInfo),
}

/// Top-level event enum, tagged by `event_type`.
///
/// This is the core of the telemetry schema. While Span and LogRecord,
/// are stable, unchanging, simple "envelops", the actual structured data
/// is stored in the `TelemetryAttributes` enum. This enum, as well as structs
/// used in its variants, are supposedto grow as telemetry evolves, covers
/// more of the dbt Fusion functionality and serves more use cases.
///
/// Besides the data itself, it's various derived traits are used to auto
/// generate human-readable span names, produce discriminated union of
/// attributes for (de)serialization.
#[skip_serializing_none]
#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    JsonSchema,
    AsRefStr,
    IntoStaticStr,
    PartialEq, // Used for equality checks. As of today in tests, but probably helpful later
    strum::Display, // Used to generate a "nide" span name from the attributes
    EnumDiscriminants, // Used to handle (de)serialization, matching serialized tags
)]
#[serde(tag = "event_type", content = "attributes")]
// The following derives a variant disciriminator enum for the telemetry attributes,
// used for type-safe (de)serialization and matching.
#[strum_discriminants(derive(Serialize, Deserialize), name(TelemetryAttributesType))]
#[cfg_attr(test, strum_discriminants(derive(EnumIter)))]
pub enum TelemetryAttributes {
    // ---------------------
    // Span attributes first
    // ---------------------
    /// # Process attributes
    /// Fusion produces one process span per execution of the cli or lsp.
    /// There can be multiple Invocations per Process.
    Process(ProcessInfo),

    /// # Invocation attributes
    Invocation(Box<InvocationInfo>),

    // Command/operation spans
    /// # Update command attributes
    Update(UpdateInfo),
    /// # Onboarding attributes
    Onboarding(OnboardingInfo),

    // Parse, compile, build phases
    /// # Phase attributes
    #[strum(to_string = "Phase({0})")]
    Phase(BuildPhaseInfo),

    /// # Node attributes
    #[strum(to_string = "Node({0})")]
    Node(NodeInfo),

    /// # Trace level span attributes
    ///
    /// This is used for detailed tracing of internal operations and only available
    /// when TRACE level is explicitly enabled.
    #[strum(to_string = "DevInternal({0})")]
    DevInternal(DevInternalInfo),

    /// # Fallback span attributes
    ///
    /// This is used for spans that weren't properly instrumented. Report a bug if you see this.
    Unknown(UnknownInfo),
    // ---------------------
    // Log attributes
    // ---------------------
    /// # Regular log record
    ///
    /// Is used for all log levels, messages that do not have specific meaning
    Log(LogEventInfo),

    /// # Unstructured log record
    ///
    /// Is used for all log emitted by pre-tracing code that hasn't migrated to tracing yet.
    LegacyLog(LegacyLogEventInfo),

    /// # Write Artifact
    WriteArtifact(WriteArtifactInfo),
}

impl TelemetryAttributes {
    /// Returns the expected type of the telemetry record based on the attributes.
    /// I.e. the given attributes, should only be used in the context of the returned
    /// record type.
    ///
    /// Note that this will return `SpanEnd` in lieu of both `SpanStart` and `SpanEnd`.
    pub fn record_type(&self) -> TelemetryRecordType {
        match self {
            TelemetryAttributes::Process(_)
            | TelemetryAttributes::Invocation(_)
            | TelemetryAttributes::Update(_)
            | TelemetryAttributes::Onboarding(_)
            | TelemetryAttributes::Phase(_)
            | TelemetryAttributes::Node(_)
            | TelemetryAttributes::DevInternal(_)
            | TelemetryAttributes::Unknown(_) => TelemetryRecordType::SpanEnd,
            TelemetryAttributes::Log(_)
            | TelemetryAttributes::LegacyLog(_)
            | TelemetryAttributes::WriteArtifact(_) => TelemetryRecordType::LogRecord,
        }
    }
    pub fn has_empty_location(&self) -> bool {
        match self {
            TelemetryAttributes::Log(LogEventInfo { location, .. }) => location.is_none(),
            TelemetryAttributes::LegacyLog(LegacyLogEventInfo { location, .. }) => {
                location.is_none()
            }
            TelemetryAttributes::DevInternal(DevInternalInfo { location, .. }) => {
                location.is_none()
            }
            TelemetryAttributes::Unknown(UnknownInfo { location, .. }) => location.is_none(),
            _ => false,
        }
    }
    pub fn with_location(self, location: RecordCodeLocation) -> Self {
        match self {
            TelemetryAttributes::Log(LogEventInfo {
                code,
                dbt_core_code,
                original_severity_number,
                original_severity_text,
                ..
            }) => TelemetryAttributes::Log(LogEventInfo {
                code,
                dbt_core_code,
                original_severity_number,
                original_severity_text,
                location,
            }),
            TelemetryAttributes::LegacyLog(LegacyLogEventInfo {
                original_severity_number,
                original_severity_text,
                ..
            }) => TelemetryAttributes::LegacyLog(LegacyLogEventInfo {
                original_severity_number,
                original_severity_text,
                location,
            }),
            TelemetryAttributes::DevInternal(DevInternalInfo { name, extra, .. }) => {
                TelemetryAttributes::DevInternal(DevInternalInfo {
                    name,
                    location,
                    extra,
                })
            }
            TelemetryAttributes::Unknown(UnknownInfo { name, .. }) => {
                TelemetryAttributes::Unknown(UnknownInfo { name, location })
            }
            _ => {
                // For other variants, we don't have a location, so we just return self
                self
            }
        }
    }
}
