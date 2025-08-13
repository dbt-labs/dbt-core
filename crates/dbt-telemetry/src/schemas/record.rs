//! Telemetry record definitions for dbt Fusion.

use super::{
    location::RecordCodeLocation,
    otlp::{SeverityNumber, SpanStatus},
};
use crate::serialize::*;
use dbt_serde_yaml::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use strum::{AsRefStr, EnumDiscriminants, IntoStaticStr};

#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SpanStartInfo {
    /// A unique identifier for a trace. All spans from the same trace share
    /// the same `trace_id`. The ID is a 16-byte array. An ID with all zeroes OR
    /// of length other than 16 bytes is considered invalid (empty string in OTLP/JSON
    /// is zero-length and thus is also invalid).
    #[serde(
        serialize_with = "serialize_trace_id",
        deserialize_with = "deserialize_trace_id"
    )]
    #[schemars(with = "String")]
    pub trace_id: u128,
    /// A unique identifier for a span within a trace, assigned when the span
    /// is created. The ID is an 8-byte array. An ID with all zeroes OR of length
    /// other than 8 bytes is considered invalid (empty string in OTLP/JSON
    /// is zero-length and thus is also invalid).
    #[serde(
        serialize_with = "serialize_span_id",
        deserialize_with = "deserialize_span_id"
    )]
    #[schemars(with = "String")]
    pub span_id: u64,
    /// The `span_id` of this span's parent span. If this is a root span, then this
    /// field must be empty. The ID is an 8-byte array.
    #[serde(
        serialize_with = "serialize_optional_span_id",
        deserialize_with = "deserialize_optional_span_id"
    )]
    #[schemars(with = "Option<String>")]
    pub parent_span_id: Option<u64>,
    /// A description of the span's operation.
    pub name: String,
    /// start_time_unix_nano is the start time of the span. On the client side, this is the time
    /// kept by the local machine where the span execution starts. On the server side, this
    /// is the time when the server's application handler starts running.
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January 1970.
    #[serde(
        serialize_with = "serialize_timestamp",
        deserialize_with = "deserialize_timestamp"
    )]
    #[schemars(with = "String")]
    pub start_time_unix_nano: u64,
    /// Attributes are a collection of key/value pairs in OTEL. Our schema
    /// uses a stricter discriminated union type `SpanEventAttributes` to provide
    /// a structured schema for attributes.
    ///
    /// A unique identifier of event category/type is available via `eventName`.
    /// All events with the same `eventName` will have a corresponding
    /// schema for their attributes, with `SpanEventAttributes` type title matching
    /// the `event_name`.
    ///
    /// NOTE: OTLP schema doesn't not restrict attributes or provides an `eventName` field,
    /// to discriminate between different attribute schemas.
    #[serde(flatten)]
    pub attributes: SpanAttributes,

    // Below are LogRecord fields that are not strictly necessary for a span,
    // but are included to make span model a superset of LogRecord model.
    /// time_unix_nano is the time when the span was started. It is the same as
    /// `start_time_unix_nano`.
    #[serde(
        serialize_with = "serialize_timestamp",
        deserialize_with = "deserialize_timestamp"
    )]
    #[schemars(with = "String")]
    pub time_unix_nano: u64,
    /// Numerical value of the severity, normalized to values described in Log Data Model.
    pub severity_number: SeverityNumber,
    /// The severity text (also known as log level). The original string representation as
    /// it is known at the source.
    pub severity_text: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SpanEndInfo {
    /// A unique identifier for a trace. All spans from the same trace share
    /// the same `trace_id`. The ID is a 16-byte array. An ID with all zeroes OR
    /// of length other than 16 bytes is considered invalid (empty string in OTLP/JSON
    /// is zero-length and thus is also invalid).
    #[serde(
        serialize_with = "serialize_trace_id",
        deserialize_with = "deserialize_trace_id"
    )]
    #[schemars(with = "String")]
    pub trace_id: u128,
    /// A unique identifier for a span within a trace, assigned when the span
    /// is created. The ID is an 8-byte array. An ID with all zeroes OR of length
    /// other than 8 bytes is considered invalid (empty string in OTLP/JSON
    /// is zero-length and thus is also invalid).
    #[serde(
        serialize_with = "serialize_span_id",
        deserialize_with = "deserialize_span_id"
    )]
    #[schemars(with = "String")]
    pub span_id: u64,
    /// The `span_id` of this span's parent span. If this is a root span, then this
    /// field must be empty. The ID is an 8-byte array.
    #[serde(
        serialize_with = "serialize_optional_span_id",
        deserialize_with = "deserialize_optional_span_id"
    )]
    #[schemars(with = "Option<String>")]
    pub parent_span_id: Option<u64>,
    /// A description of the span's operation.
    pub name: String,
    /// start_time_unix_nano is the start time of the span. On the client side, this is the time
    /// kept by the local machine where the span execution starts. On the server side, this
    /// is the time when the server's application handler starts running.
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January 1970.
    #[serde(
        serialize_with = "serialize_timestamp",
        deserialize_with = "deserialize_timestamp"
    )]
    #[schemars(with = "String")]
    pub start_time_unix_nano: u64,
    /// end_time_unix_nano is the end time of the span. On the client side, this is the time
    /// kept by the local machine where the span execution ends. On the server side, this
    /// is the time when the server application handler stops running.
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January 1970.
    #[serde(
        serialize_with = "serialize_timestamp",
        deserialize_with = "deserialize_timestamp"
    )]
    #[schemars(with = "String")]
    pub end_time_unix_nano: u64,
    /// attributes is a collection of key/value pairs in OTEL. Our schema
    /// uses a stricter discriminated union type `SpanEventAttributes` to provide
    /// a structured schema for attributes.
    ///
    /// A unique identifier of event category/type is available via `eventName`.
    /// All events with the same `eventName` will have a correspinding
    /// schema for their attributes, with `SpanEventAttributes` type title matching
    /// the `event_name`.
    ///
    /// NOTE: OTLP schema doesn't not restrict attributes or provides an `eventName` field,
    /// to discriminate between different attribute schemas.
    #[serde(flatten)]
    pub attributes: SpanAttributes,
    /// An optional final status for this span. Semantically when Status isn't set, it means
    /// span's status code is unset, i.e. assume STATUS_CODE_UNSET (code = 0).
    pub status: Option<SpanStatus>,

    // Below are LogRecord fields that are not strictly necessary for a span,
    // but are included to make span model a superset of LogRecord model.
    /// time_unix_nano is the time when the span was completed. It is the same as
    /// `end_time_unix_nano`.
    #[serde(
        serialize_with = "serialize_timestamp",
        deserialize_with = "deserialize_timestamp"
    )]
    #[schemars(with = "String")]
    pub time_unix_nano: u64,
    /// Numerical value of the severity, normalized to values described in Log Data Model.
    pub severity_number: SeverityNumber,
    /// The severity text (also known as log level). The original string representation as
    /// it is known at the source.
    pub severity_text: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, JsonSchema, Clone)]
#[serde(rename_all = "camelCase")]
pub struct LogRecordInfo {
    /// time_unix_nano is the time when the event occurred.
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January 1970.
    /// Value of 0 indicates unknown or missing timestamp.
    #[serde(
        serialize_with = "serialize_timestamp",
        deserialize_with = "deserialize_timestamp"
    )]
    #[schemars(with = "String")]
    pub time_unix_nano: u64,
    /// A unique identifier for a trace. All logs from the same trace share
    /// the same `trace_id`. The ID is a 16-byte array. An ID with all zeroes OR
    /// of length other than 16 bytes is considered invalid (empty string in OTLP/JSON
    /// is zero-length and thus is also invalid).
    #[serde(
        serialize_with = "serialize_trace_id",
        deserialize_with = "deserialize_trace_id"
    )]
    #[schemars(with = "String")]
    pub trace_id: u128,
    /// A unique identifier for a span within a trace, active when the log
    /// is created. The ID is an 8-byte array. An ID with all zeroes OR of length
    /// other than 8 bytes is considered invalid (empty string in OTLP/JSON
    /// is zero-length and thus is also invalid).
    #[serde(
        serialize_with = "serialize_optional_span_id",
        deserialize_with = "deserialize_optional_span_id"
    )]
    #[schemars(with = "Option<String>")]
    pub span_id: Option<u64>,
    /// The name of the span that is active when the log is created.
    ///
    /// NOTE: this is not part of the OTLP schema, but is used to
    /// simplify log grouping avoiding the need to
    /// look up the span name by trace_id/span_id.
    pub span_name: Option<String>,
    /// Numerical value of the severity, normalized to values described in Log Data Model.
    pub severity_number: SeverityNumber,
    /// The severity text (also known as log level). The original string representation as
    /// it is known at the source.
    pub severity_text: Option<String>,
    /// A value containing the body (message) of the log record. A human-readable
    /// string message (including multi-line) describing the event in a free form
    pub body: String,
    /// attributes is a collection of key/value pairs in OTEL. Our schema
    /// uses a stricter discriminated union type `LogEventAttributes` to provide
    /// a structured schema for attributes.
    /// A unique identifier of event category/type is available via `eventName`.
    /// All events with the same eventName will have a correspinding
    /// schema for their attributes, with `LogEventAttributes` type title matching
    /// the event_name.
    #[serde(flatten)]
    pub attributes: LogAttributes,
}

/// Represents a telemetry record which loosely follows OpenTelemetry's
/// log and trace signal logical models & semantics (but not OTLP schema!)
/// and combines them under a single enum type.
///
/// This is a discriminated union on `recordType` field, which is not part of the OTLP schema.
///
/// `SpanStart` and `SpanEnd` top-level fields are a strict superset of
/// `LogRecord` which introduces some data redundancy, but simplifies
/// reading records in some use-cases.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(tag = "recordType")]
#[serde(rename_all = "camelCase")]
pub enum TelemetryRecord {
    /// # Span Start
    /// Represents the start of a span in a trace.
    ///
    /// This is a partial-span record emitted as soon as the span is created.
    /// The corresponding `SpanEnd` event is guaranteed to have the same
    /// values for all same-named fields except attributes and
    #[serde(rename_all = "camelCase")]
    SpanStart(SpanStartInfo),
    /// # Span
    /// Represents a span in a trace.
    ///
    /// This is a full-span record emitted when the span is completed.
    #[serde(rename_all = "camelCase")]
    SpanEnd(SpanEndInfo),
    /// # Log Record
    ///
    /// Represents a log record, which is a structured point in time event that can be emitted
    /// during the execution of a span.
    #[serde(rename_all = "camelCase")]
    LogRecord(LogRecordInfo),
}

/// A reference to a telemetry record, used in tracing to avoiding cloning. Make sure
/// it matches the `TelemetryRecord` enum.
#[derive(Serialize)]
#[serde(tag = "recordType")]
#[serde(rename_all = "camelCase")]
pub enum TelemetryRecordRef<'a> {
    /// # Span Start
    /// Represents the start of a span in a trace.
    ///
    /// This is a partial-span record emitted as soon as the span is created.
    /// The corresponding `SpanEnd` event is guaranteed to have the same
    /// values for all same-named fields except attributes and
    #[serde(rename_all = "camelCase")]
    SpanStart(&'a SpanStartInfo),
    /// # Span
    /// Represents a span in a trace.
    ///
    /// This is a full-span record emitted when the span is completed.
    #[serde(rename_all = "camelCase")]
    SpanEnd(&'a SpanEndInfo),
    /// # Log Record
    ///
    /// Represents a log record, which is a structured point in time event that can be emitted
    /// during the execution of a span.
    #[serde(rename_all = "camelCase")]
    LogRecord(&'a LogRecordInfo),
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
pub struct SharedPhaseInfo {
    #[serde(rename = "dbt.fusion.invocation.id")]
    pub invocation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EnumDiscriminants, strum::Display)]
#[strum_discriminants(derive(
    Serialize,
    Deserialize,
    JsonSchema,
    strum::Display,
    IntoStaticStr,
    Hash
))]
#[strum_discriminants(name(BuildPhase))]
#[serde(tag = "dbt.fusion.phase.name")]
pub enum BuildPhaseInfo {
    /// # File Discovery
    /// Analyzing dbt_project, profiles.yml and scanning files
    Loading {
        #[serde(flatten)]
        shared: SharedPhaseInfo,
        // EXAMPLE: The number of files discovered
        // #[serde(rename = "dbt.fusion.phase.file_count")]
        // file_count: u64,
    },
    /// # Dependency Loading
    /// Check that dependencies are met
    DependencyLoading {
        #[serde(flatten)]
        shared: SharedPhaseInfo,
        // EXAMPLE: The number of dependencies loaded
        // #[serde(rename = "dbt.fusion.phase.dependency_loading.dependency_count")]
        // dependency_count: u64,
    },
    /// # Parsing
    /// Parsing and macro name resolution of all dbt files
    Parsing {
        #[serde(flatten)]
        shared: SharedPhaseInfo,
        // EXAMPLE: The number of dbt files parsed
        // #[serde(rename = "dbt.fusion.phase.parsing.parsed_file_count")]
        // parsed_file_count: u64,
    },
    /// # Scheduling
    /// Graph construction and graph slicing
    Scheduling {
        #[serde(flatten)]
        shared: SharedPhaseInfo,
        // EXAMPLE: The number of nodes in the graph
        // #[serde(rename = "dbt.fusion.phase.scheduling.node_count")]
        // node_count: u64,
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
        #[serde(rename = "dbt.fusion.phase.node.count")]
        node_count: u64,
    },
    /// # Compiling
    /// Dbt compile (called render) and Sql analysis
    Compiling {
        #[serde(flatten)]
        shared: SharedPhaseInfo,
        #[serde(rename = "dbt.fusion.phase.node.count")]
        node_count: u64,
    },
    /// # Executing
    /// Execution against the target database
    Executing {
        #[serde(flatten)]
        shared: SharedPhaseInfo,
        #[serde(rename = "dbt.fusion.phase.node.count")]
        node_count: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct NodeIdentifier {
    /// The unique ID of the node.
    #[serde(rename = "dbt.fusion.node.unique.id")]
    pub unique_id: String,
    /// The name of the node.
    #[serde(rename = "dbt.fusion.node.fqn")]
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
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default, JsonSchema)]
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
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct InvocationMetrics {
    #[serde(rename = "dbt.fusion.invocation.metrics.total.errors")]
    pub total_errors: Option<u64>,
    #[serde(rename = "dbt.fusion.invocation.metrics.total.warnings")]
    pub total_warnings: Option<u64>,
    #[serde(rename = "dbt.fusion.invocation.metrics.autofix.suggestions")]
    pub autofix_suggestions: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
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
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, strum::Display)]
#[serde(tag = "eventName", content = "attributes")]
pub enum SpanAttributes {
    /// # Process attributes
    /// Fusion produces one process span per execution of the cli or lsp.
    /// There can be multiple Invocations per Process.
    Process {
        #[serde(rename = "dbt.fusion.version")] // In OTEL `service.version`
        version: String,
        #[serde(rename = "dbt.fusion.host.os")]
        host_os: String,
        #[serde(rename = "dbt.fusion.host.arch")]
        host_arch: String,
    },
    /// # Invocation attributes
    Invocation {
        #[serde(rename = "dbt.fusion.invocation.id")]
        invocation_id: String,
        #[serde(rename = "dbt.fusion.session.start.target")]
        target: Option<String>,

        // The following process-wide attributes are duplicated for convenience
        #[serde(rename = "dbt.fusion.version")] // In OTEL `service.version`
        version: String,
        #[serde(rename = "dbt.fusion.host.os")]
        host_os: String,
        #[serde(rename = "dbt.fusion.host.arch")]
        host_arch: String,

        // Metrics
        #[serde(flatten)]
        metrics: Option<InvocationMetrics>,
    },
    // Operation spans
    /// # Session attributes
    Update {
        /// Update dbt to this version (e.g. 1.2.3) [default: latest version]
        #[serde(rename = "dbt.fusion.update.version")]
        version: Option<String>,
        /// Package to update (e.g. dbt) [default: dbt]
        #[serde(rename = "dbt.fusion.update.package")]
        package: Option<String>,
        /// The discovered path to the dbt executable
        #[serde(rename = "dbt.fusion.exe.path")]
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
        #[serde(rename = "dbt.fusion.node.phase")]
        phase: BuildPhase,
        /// Final status of the node execution.
        #[serde(rename = "dbt.fusion.node.status")]
        status: Option<NodeExecutionStatus>,
        /// The number of resulting rows produced by the node, if recorded.
        #[serde(rename = "dbt.fusion.node.num.rows")]
        num_rows: Option<u64>,
    },
    #[strum(to_string = "DevInternal({name} | {location})")]
    DevInternal {
        /// Internal developer span name, often the function
        #[serde(rename = "dbt.fusion.dev.span.name")]
        name: String,
        #[serde(flatten)]
        location: RecordCodeLocation,
        /// Arbitrary extra string for debugging purposes.
        #[serde(rename = "dbt.fusion.dev.span.extra")]
        extra: Option<std::collections::BTreeMap<String, DebugValue>>,
    },
    Unknown {
        /// Internal developer span name, often the function
        #[serde(rename = "dbt.fusion.dev.span.name")]
        name: String,
        #[serde(flatten)]
        location: RecordCodeLocation,
    },
}

#[skip_serializing_none]
#[derive(
    Debug, Clone, Serialize, Deserialize, JsonSchema, AsRefStr, IntoStaticStr, PartialEq, Eq,
)]
#[serde(tag = "eventName", content = "attributes")]
pub enum LogAttributes {
    /// # Regular log record
    ///
    /// Is used for all log levels, messages that do not have specific meaning
    Log {
        // TODO: use ErrorCode enum for error codes
        /// Option error/warning code
        #[serde(rename = "dbt.fusion.event.error.code")]
        code: Option<u32>,
        /// An optional legacy codes dbt-core code (e.g. "Z048")
        #[serde(rename = "dbt.fusion.event.legacy.code")]
        dbt_core_code: Option<String>,
        /// Numerical value of the severity, normalized to values described in OTEL Log Data Model.
        ///
        /// This is the original severity before user up/down-grade configuration applied
        #[serde(rename = "dbt.fusion.event.original.severity.number")]
        original_severity_number: SeverityNumber,
        /// The severity text (also known as log level).
        ///
        /// This is the original severity before user up/down-grade configuration applied
        #[serde(rename = "dbt.fusion.event.original.severity.text")]
        original_severity_text: Option<String>,
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
        #[serde(rename = "dbt.fusion.event.original.severity.number")]
        original_severity_number: SeverityNumber,
        /// The severity text (also known as log level).
        ///
        /// This is the original severity before user up/down-grade configuration applied
        #[serde(rename = "dbt.fusion.event.original.severity.text")]
        original_severity_text: Option<String>,
        #[serde(flatten)]
        location: RecordCodeLocation,
    },

    /// # Write Artifact
    WriteArtifact {
        /// The path to the artifact.
        #[serde(rename = "dbt.fusion.artifact.relative.path")]
        relative_path: Option<String>,
        /// Time it took to write the artifact in milliseconds.
        #[serde(rename = "dbt.fusion.artifact.duration.ms")]
        duration_ms: Option<u64>,
    },
}

impl LogAttributes {
    pub fn has_empty_location(&self) -> bool {
        match self {
            LogAttributes::Log { location, .. } => location.is_none(),
            LogAttributes::LegacyLog { location, .. } => location.is_none(),
            _ => false,
        }
    }
    pub fn with_location(self, location: RecordCodeLocation) -> Self {
        match self {
            LogAttributes::Log {
                code,
                dbt_core_code,
                original_severity_number,
                original_severity_text,
                ..
            } => LogAttributes::Log {
                code,
                dbt_core_code,
                original_severity_number,
                original_severity_text,
                location,
            },
            LogAttributes::LegacyLog {
                original_severity_number,
                original_severity_text,
                ..
            } => LogAttributes::LegacyLog {
                original_severity_number,
                original_severity_text,
                location,
            },
            _ => {
                // For other variants, we don't have a location, so we just return self
                self
            }
        }
    }
}
