//! Telemetry record definitions for dbt Fusion.

use std::time::SystemTime;

use super::{
    super::serialize::{
        deserialize_optional_span_id, deserialize_span_id, deserialize_timestamp,
        deserialize_trace_id, serialize_optional_span_id, serialize_span_id, serialize_timestamp,
        serialize_trace_id,
    },
    location::RecordCodeLocation,
    otlp::{SeverityNumber, SpanStatus},
};
use dbt_serde_yaml::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
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

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Default)]
pub struct SharedPhaseInfo {
    // Invocation id is added to all phase for consumer convenience.
    // It will always match the `invocation_id` in the root `Invocation` span.
    /// Unique identifier for the invocation
    pub invocation_id: String,
}

#[derive(
    Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, EnumDiscriminants, strum::Display,
)]
#[strum_discriminants(derive(
    Serialize,
    Deserialize,
    JsonSchema,
    strum::Display,
    IntoStaticStr,
    Hash
))]
#[strum_discriminants(name(BuildPhase))]
#[serde(tag = "phase")]
pub enum BuildPhaseInfo {
    /// # File Discovery
    /// Analyzing dbt_project, profiles.yml and scanning files
    Loading {
        #[serde(flatten)]
        shared: SharedPhaseInfo,
    },

    /// # Dependency Loading
    /// Check that dependencies are met
    DependencyLoading {
        #[serde(flatten)]
        shared: SharedPhaseInfo,
    },

    /// # Parsing
    /// Parsing and macro name resolution of all dbt files
    Parsing {
        #[serde(flatten)]
        shared: SharedPhaseInfo,
    },

    /// # Scheduling
    /// Graph construction and graph slicing
    Scheduling {
        #[serde(flatten)]
        shared: SharedPhaseInfo,
    },

    /// # Freshness Analysis
    /// Freshness analysis of sources and models
    FreshnessAnalysis {
        #[serde(flatten)]
        shared: SharedPhaseInfo,
    },

    /// # Lineage
    /// Analysis of individual node lineages
    Lineage {
        #[serde(flatten)]
        shared: SharedPhaseInfo,
    },

    /// # Analyzing
    /// Dbt compile (called render) and Sql analysis
    Analyzing {
        #[serde(flatten)]
        shared: SharedPhaseInfo,
        node_count: u64,
    },

    /// # Compiling
    /// Dbt compile (called render) and Sql analysis
    Compiling {
        #[serde(flatten)]
        shared: SharedPhaseInfo,
        node_count: u64,
    },

    /// # Executing
    /// Execution against the target database
    Executing {
        #[serde(flatten)]
        shared: SharedPhaseInfo,
        node_count: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct NodeIdentifier {
    /// The unique ID of the node.
    pub unique_id: String,
    /// The name of the node.
    pub fqn: String,
}

impl std::fmt::Display for NodeIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.unique_id)
    }
}

/// TODO: this is a duplicate from `dbt-schemas` crate due to current circular dependency
/// remove redundancy when `dbt-schemas` crate is available
/// Represents the detailed status of a phase in the execution of a node.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Default, JsonSchema)]
pub enum NodeExecutionStatus {
    #[default]
    Success,
    Error,
    Skipped,
    Aborted, // e.g. interrupted by user.
    Reused,
    Passed, // For test nodes.
    Failed, // For test nodes.
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct InvocationMetrics {
    pub total_errors: Option<u64>,
    pub total_warnings: Option<u64>,
    pub autofix_suggestions: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(untagged)]
pub enum DebugValue {
    Float64(f64),
    Int64(i64),
    UInt64(u64),
    Bool(bool),
    String(String),
    Bytes(Vec<u8>),
}

/// Top-level event enum, tagged by `event_type` for downstream routing
#[skip_serializing_none]
#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    JsonSchema,
    AsRefStr,
    IntoStaticStr,
    PartialEq,
    strum::Display,
    EnumDiscriminants,
)]
#[serde(tag = "event_type", content = "attributes")]
#[strum_discriminants(derive(Serialize, Deserialize), name(TelemetryAttributesType))]
pub enum TelemetryAttributes {
    // ---------------------
    // Span attributes first
    // ---------------------
    /// # Process attributes
    /// Fusion produces one process span per execution of the cli or lsp.
    /// There can be multiple Invocations per Process.
    Process {
        /// dbt fusion version, e.g. "1.2.3"
        version: String,
        /// The host operating system, e.g. "linux", "darwin", "windows"
        host_os: String,
        /// The host architecture, e.g. "x86_64", "aarch64"
        host_arch: String,
    },

    /// # Invocation attributes
    Invocation {
        /// Unique identifier for the invocation
        invocation_id: String,
        /// The dbt command that was executed, e.g. "run", "test", "build"
        command: String,
        /// dbt target, e.g. "dev", "prod"
        target: Option<String>,

        // The following process-wide attributes are duplicated for convenience
        /// dbt fusion version, e.g. "1.2.3"
        version: String,
        /// The host operating system, e.g. "linux", "darwin", "windows"
        host_os: String,
        /// The host architecture, e.g. "x86_64", "aarch64"
        host_arch: String,

        // Metrics
        #[serde(flatten)]
        metrics: Option<InvocationMetrics>,
    },

    // Operation spans
    /// # Session attributes
    Update {
        /// Update dbt to this version (e.g. 1.2.3) [default: latest version]
        version: Option<String>,
        /// Package to update (e.g. dbt) [default: dbt]
        package: Option<String>,
        /// The discovered path to the dbt executable
        exe_path: Option<String>,
    },

    // Phases
    /// # Phase attributes
    #[strum(to_string = "Phase({0})")]
    Phase(BuildPhaseInfo),

    /// # Node attributes
    #[strum(to_string = "Node({phase}|{node_id})")]
    Node {
        #[serde(flatten)]
        node_id: NodeIdentifier, // this is flattened into inner attrs, hence `node_id` and not `id`
        phase: BuildPhase,
        /// Final status of the node execution.
        status: Option<NodeExecutionStatus>,
        /// The number of resulting rows produced by the node, if recorded.
        num_rows: Option<u64>,
    },

    /// # Trace level span attributes
    ///
    /// This is used for detailed tracing of internal operations and only available
    /// when TRACE level is explicitly enabled.
    #[strum(to_string = "DevInternal({name} | {location})")]
    DevInternal {
        /// Internal developer span name, often the function
        name: String,
        #[serde(flatten)]
        location: RecordCodeLocation,
        /// Arbitrary extra string for debugging purposes.
        extra: Option<std::collections::BTreeMap<String, DebugValue>>,
    },

    /// # Fallback span attributes
    ///
    /// This is used for spans that weren't properly instrumented. Report a bug if you see this.
    Unknown {
        /// Internal developer span name, often the function
        name: String,
        #[serde(flatten)]
        location: RecordCodeLocation,
    },
    // ---------------------
    // Log attributes
    // ---------------------
    /// # Regular log record
    ///
    /// Is used for all log levels, messages that do not have specific meaning
    Log {
        // TODO: use ErrorCode enum for error codes
        /// Option error/warning code
        code: Option<u32>,

        /// An optional legacy codes dbt-core code (e.g. "Z048")
        dbt_core_code: Option<String>,

        /// Numerical value of the severity, normalized to values described in OTEL Log Data Model.
        ///
        /// This is the original severity before user up/down-grade configuration applied
        original_severity_number: SeverityNumber,

        /// The severity text (also known as log level).
        ///
        /// This is the original severity before user up/down-grade configuration applied
        original_severity_text: String,

        #[serde(flatten)]
        location: RecordCodeLocation,
    },

    /// # Unstructured log record
    ///
    /// Is used for all log emitted by pre-tracing code that hasn't migrated to tracing yet.
    LegacyLog {
        /// Numerical value of the severity, normalized to values described in OTEL Log Data Model.
        ///
        /// This is the original severity before user up/down-grade configuration applied
        original_severity_number: SeverityNumber,

        /// The severity text (also known as log level).
        ///
        /// This is the original severity before user up/down-grade configuration applied
        original_severity_text: String,

        #[serde(flatten)]
        location: RecordCodeLocation,
    },

    /// # Write Artifact
    WriteArtifact {
        /// The path to the artifact.
        relative_path: Option<String>,
        /// Time it took to write the artifact in milliseconds.
        duration_ms: Option<u64>,
    },
}

impl TelemetryAttributes {
    pub fn has_empty_location(&self) -> bool {
        match self {
            TelemetryAttributes::Log { location, .. } => location.is_none(),
            TelemetryAttributes::LegacyLog { location, .. } => location.is_none(),
            TelemetryAttributes::DevInternal { location, .. } => location.is_none(),
            TelemetryAttributes::Unknown { location, .. } => location.is_none(),
            _ => false,
        }
    }
    pub fn with_location(self, location: RecordCodeLocation) -> Self {
        match self {
            TelemetryAttributes::Log {
                code,
                dbt_core_code,
                original_severity_number,
                original_severity_text,
                ..
            } => TelemetryAttributes::Log {
                code,
                dbt_core_code,
                original_severity_number,
                original_severity_text,
                location,
            },
            TelemetryAttributes::LegacyLog {
                original_severity_number,
                original_severity_text,
                ..
            } => TelemetryAttributes::LegacyLog {
                original_severity_number,
                original_severity_text,
                location,
            },
            TelemetryAttributes::DevInternal { name, extra, .. } => {
                TelemetryAttributes::DevInternal {
                    name,
                    location,
                    extra,
                }
            }
            TelemetryAttributes::Unknown { name, .. } => {
                TelemetryAttributes::Unknown { name, location }
            }
            _ => {
                // For other variants, we don't have a location, so we just return self
                self
            }
        }
    }
}
