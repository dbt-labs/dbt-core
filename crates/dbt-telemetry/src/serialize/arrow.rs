//! Arrow serialization support for telemetry records using serde_arrow.

use super::to_nanos;
use crate::{
    BuildPhase, BuildPhaseInfo, InvocationMetrics, LogRecordInfo, NodeExecutionStatus,
    NodeIdentifier, RecordCodeLocation, SeverityNumber, SharedPhaseInfo, SpanEndInfo,
    SpanStartInfo, SpanStatus, StatusCode, TelemetryAttributes, TelemetryAttributesType,
    TelemetryRecord, TelemetryRecordType,
};
use arrow::{
    array::Array,
    compute::{CastOptions, cast_with_options},
    datatypes::{DataType, Field, FieldRef, Schema, TimeUnit},
    record_batch::RecordBatch,
    util::display::FormatOptions,
};
use serde::{Deserialize, Serialize};
use serde_arrow::schema::{SchemaLike, TracingOptions};
use std::sync::Arc;
use std::time::SystemTime;

// Create sudo impls for defaults on these two enums. This is only necessary
// to make `ArrowTelemetryRecord` derive `Default` automatically, which in turn
// simplifies the conversion from `TelemetryRecord` to `ArrowTelemetryRecord`.
// During conversion we always set the `record_type` & `event_type` fields,
// so default implementations are not used in practice.
#[allow(clippy::derivable_impls)]
impl Default for TelemetryRecordType {
    fn default() -> Self {
        TelemetryRecordType::LogRecord
    }
}

#[allow(clippy::derivable_impls)]
impl Default for TelemetryAttributesType {
    fn default() -> Self {
        TelemetryAttributesType::Unknown
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ArrowTelemetryRecord<'a> {
    pub record_type: TelemetryRecordType,
    pub trace_id: String, // Arrow doesn't support u128 natively, so...
    pub span_id: Option<u64>,
    pub span_name: Option<&'a str>,
    pub parent_span_id: Option<u64>,
    pub start_time_unix_nano: Option<u64>,
    pub end_time_unix_nano: Option<u64>,
    pub time_unix_nano: Option<u64>,
    pub severity_number: u8,
    pub severity_text: &'a str,
    pub body: Option<&'a str>,
    pub status_code: Option<u32>,
    pub status_message: Option<&'a str>,
    pub event_type: TelemetryAttributesType,
    pub attributes: ArrowAttributes<'a>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ArrowAttributes<'a> {
    // Process fields
    pub version: Option<&'a str>,
    pub host_os: Option<&'a str>,
    pub host_arch: Option<&'a str>,
    // Invocation fields
    pub invocation_id: Option<&'a str>,
    pub command: Option<&'a str>,
    pub target: Option<&'a str>,
    pub total_errors: Option<u64>,
    pub total_warnings: Option<u64>,
    pub autofix_suggestions: Option<u64>,
    // Update fields
    pub update_version: Option<&'a str>,
    pub update_package: Option<&'a str>,
    pub exe_path: Option<&'a str>,
    // Phase fields - BuildPhaseInfo union fields
    pub phase: Option<BuildPhase>,
    pub node_count: Option<u64>,
    // Node fields
    pub unique_id: Option<&'a str>,
    pub fqn: Option<&'a str>,
    pub status: Option<NodeExecutionStatus>,
    pub num_rows: Option<u64>,
    // DevInternal/Unknown fields
    pub dev_name: Option<&'a str>,
    // Location fields (common to multiple variants)
    pub file: Option<&'a str>,
    pub line: Option<u32>,
    pub module_path: Option<&'a str>,
    pub location_target: Option<&'a str>,
    // Log fields
    pub code: Option<u32>,
    pub dbt_core_code: Option<&'a str>,
    pub original_severity_number: Option<u8>,
    pub original_severity_text: Option<&'a str>,
    // WriteArtifact fields
    pub relative_path: Option<&'a str>,
    pub duration_ms: Option<u64>,
    // Event type for discriminated union. This is a duplicate
    // from the owning `ArrowTelemetryRecord` but is usefull
    // as it allows working with attributes outside of the record context.
    pub event_type: TelemetryAttributesType,
}

#[inline]
fn nanos_to_system_time(nanos: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + std::time::Duration::from_nanos(nanos)
}

fn arrow_to_location(arrow: &ArrowAttributes) -> RecordCodeLocation {
    RecordCodeLocation {
        file: arrow.file.map(|s| s.to_string()),
        line: arrow.line,
        module_path: arrow.module_path.map(|s| s.to_string()),
        target: arrow.location_target.map(|s| s.to_string()),
    }
}

impl<'a> From<&'a TelemetryRecord> for ArrowTelemetryRecord<'a> {
    fn from(record: &'a TelemetryRecord) -> Self {
        match record {
            TelemetryRecord::SpanStart(span) => {
                let attributes = ArrowAttributes::from(&span.attributes);
                ArrowTelemetryRecord {
                    record_type: record.into(),
                    trace_id: format!("{:032x}", span.trace_id),
                    span_id: Some(span.span_id),
                    span_name: Some(&span.span_name),
                    parent_span_id: span.parent_span_id,
                    start_time_unix_nano: Some(to_nanos(&span.start_time_unix_nano)),
                    end_time_unix_nano: None,
                    time_unix_nano: None,
                    severity_number: span.severity_number as u8,
                    severity_text: span.severity_text.as_ref(),
                    body: None,
                    status_code: None,
                    status_message: None,
                    event_type: TelemetryAttributesType::from(&span.attributes),
                    attributes,
                }
            }
            TelemetryRecord::SpanEnd(span) => {
                let attributes = ArrowAttributes::from(&span.attributes);
                ArrowTelemetryRecord {
                    record_type: record.into(),
                    trace_id: format!("{:032x}", span.trace_id),
                    span_id: Some(span.span_id),
                    span_name: Some(&span.span_name),
                    parent_span_id: span.parent_span_id,
                    start_time_unix_nano: Some(to_nanos(&span.start_time_unix_nano)),
                    end_time_unix_nano: Some(to_nanos(&span.end_time_unix_nano)),
                    time_unix_nano: None,
                    severity_number: span.severity_number as u8,
                    severity_text: span.severity_text.as_ref(),
                    body: None,
                    status_code: span.status.as_ref().map(|s| s.code as u32),
                    status_message: span.status.as_ref().and_then(|s| s.message.as_deref()),
                    event_type: TelemetryAttributesType::from(&span.attributes),
                    attributes,
                }
            }
            TelemetryRecord::LogRecord(log) => {
                let attributes = ArrowAttributes::from(&log.attributes);
                ArrowTelemetryRecord {
                    record_type: record.into(),
                    trace_id: format!("{:032x}", log.trace_id),
                    span_id: log.span_id,
                    span_name: log.span_name.as_deref(),
                    parent_span_id: None,
                    start_time_unix_nano: None,
                    end_time_unix_nano: None,
                    time_unix_nano: Some(to_nanos(&log.time_unix_nano)),
                    severity_number: log.severity_number as u8,
                    severity_text: log.severity_text.as_ref(),
                    body: Some(log.body.as_ref()),
                    status_code: None,
                    status_message: None,
                    event_type: TelemetryAttributesType::from(&log.attributes),
                    attributes,
                }
            }
        }
    }
}

impl<'a> From<&'a TelemetryAttributes> for ArrowAttributes<'a> {
    fn from(attr: &'a TelemetryAttributes) -> Self {
        match attr {
            TelemetryAttributes::Process {
                version,
                host_os,
                host_arch,
            } => ArrowAttributes {
                version: Some(version),
                host_os: Some(host_os),
                host_arch: Some(host_arch),
                event_type: TelemetryAttributesType::from(attr),
                ..Default::default()
            },
            TelemetryAttributes::Invocation {
                invocation_id,
                command,
                target,
                version,
                host_os,
                host_arch,
                metrics,
            } => ArrowAttributes {
                version: Some(version),
                host_os: Some(host_os),
                host_arch: Some(host_arch),
                invocation_id: Some(invocation_id),
                command: Some(command),
                target: target.as_deref(),
                total_errors: metrics.as_ref().and_then(|m| m.total_errors),
                total_warnings: metrics.as_ref().and_then(|m| m.total_warnings),
                autofix_suggestions: metrics.as_ref().and_then(|m| m.autofix_suggestions),
                event_type: TelemetryAttributesType::from(attr),
                ..Default::default()
            },
            TelemetryAttributes::Update {
                version,
                package,
                exe_path,
            } => ArrowAttributes {
                update_version: version.as_deref(),
                update_package: package.as_deref(),
                exe_path: exe_path.as_deref(),
                event_type: TelemetryAttributesType::from(attr),
                ..Default::default()
            },
            TelemetryAttributes::Phase(phase_info) => match phase_info {
                BuildPhaseInfo::Loading { shared }
                | BuildPhaseInfo::DependencyLoading { shared }
                | BuildPhaseInfo::Parsing { shared }
                | BuildPhaseInfo::Scheduling { shared }
                | BuildPhaseInfo::FreshnessAnalysis { shared }
                | BuildPhaseInfo::Lineage { shared } => ArrowAttributes {
                    invocation_id: Some(&shared.invocation_id),
                    event_type: TelemetryAttributesType::from(attr),
                    ..Default::default()
                },
                BuildPhaseInfo::Analyzing { shared, node_count }
                | BuildPhaseInfo::Compiling { shared, node_count }
                | BuildPhaseInfo::Executing { shared, node_count } => ArrowAttributes {
                    invocation_id: Some(&shared.invocation_id),
                    node_count: Some(*node_count),
                    event_type: TelemetryAttributesType::from(attr),
                    ..Default::default()
                },
            },
            TelemetryAttributes::Node {
                node_id,
                phase,
                status,
                num_rows,
            } => ArrowAttributes {
                unique_id: Some(&node_id.unique_id),
                fqn: Some(&node_id.fqn),
                phase: Some(*phase),
                status: *status,
                num_rows: *num_rows,
                event_type: TelemetryAttributesType::from(attr),
                ..Default::default()
            },
            TelemetryAttributes::DevInternal {
                name,
                location,
                extra: _, // never serialized
            }
            | TelemetryAttributes::Unknown { name, location } => ArrowAttributes {
                dev_name: Some(name),
                file: location.file.as_deref(),
                line: location.line,
                module_path: location.module_path.as_deref(),
                location_target: location.target.as_deref(),
                event_type: TelemetryAttributesType::from(attr),
                ..Default::default()
            },
            TelemetryAttributes::Log {
                code,
                dbt_core_code,
                original_severity_number,
                original_severity_text,
                location,
            } => ArrowAttributes {
                file: location.file.as_deref(),
                line: location.line,
                module_path: location.module_path.as_deref(),
                location_target: location.target.as_deref(),
                code: *code,
                dbt_core_code: dbt_core_code.as_deref(),
                original_severity_number: Some(*original_severity_number as u8),
                original_severity_text: Some(original_severity_text.as_ref()),
                event_type: TelemetryAttributesType::from(attr),
                ..Default::default()
            },
            TelemetryAttributes::LegacyLog {
                original_severity_number,
                original_severity_text,
                location,
            } => ArrowAttributes {
                file: location.file.as_deref(),
                line: location.line,
                module_path: location.module_path.as_deref(),
                location_target: location.target.as_deref(),
                original_severity_number: Some(*original_severity_number as u8),
                original_severity_text: Some(original_severity_text.as_ref()),
                event_type: TelemetryAttributesType::from(attr),
                ..Default::default()
            },
            TelemetryAttributes::WriteArtifact {
                relative_path,
                duration_ms,
            } => ArrowAttributes {
                relative_path: relative_path.as_deref(),
                duration_ms: *duration_ms,
                event_type: TelemetryAttributesType::from(attr),
                ..Default::default()
            },
        }
    }
}

impl TryFrom<ArrowTelemetryRecord<'_>> for TelemetryRecord {
    type Error = String;

    fn try_from(arrow: ArrowTelemetryRecord) -> Result<Self, Self::Error> {
        match arrow.record_type {
            TelemetryRecordType::SpanStart => {
                let span_id = arrow
                    .span_id
                    .ok_or("Missing span_id for SpanStart record")?;
                let span_name = arrow
                    .span_name
                    .ok_or("Missing span_name for SpanStart record")?;
                let start_time_unix_nano = arrow
                    .start_time_unix_nano
                    .ok_or("Missing start_time_unix_nano for SpanStart record")?;

                Ok(TelemetryRecord::SpanStart(SpanStartInfo {
                    trace_id: u128::from_str_radix(&arrow.trace_id, 16)
                        .map_err(|e| format!("Invalid trace_id: {e}"))?,
                    span_id,
                    parent_span_id: arrow.parent_span_id,
                    span_name: span_name.to_string(),
                    start_time_unix_nano: nanos_to_system_time(start_time_unix_nano),
                    attributes: TelemetryAttributes::try_from(arrow.attributes)?,
                    severity_number: SeverityNumber::from_repr(arrow.severity_number)
                        .ok_or("Invalid severity_number")?,
                    severity_text: arrow.severity_text.to_string(),
                }))
            }
            TelemetryRecordType::SpanEnd => {
                let span_id = arrow.span_id.ok_or("Missing span_id for SpanEnd record")?;
                let span_name = arrow
                    .span_name
                    .ok_or("Missing span_name for SpanEnd record")?;
                let start_time_unix_nano = arrow
                    .start_time_unix_nano
                    .ok_or("Missing start_time_unix_nano for SpanEnd record")?;
                let end_time_unix_nano = arrow
                    .end_time_unix_nano
                    .ok_or("Missing end_time_unix_nano for SpanEnd record")?;

                let status = if arrow.status_code.is_some() || arrow.status_message.is_some() {
                    Some(SpanStatus {
                        code: StatusCode::from_repr(arrow.status_code.unwrap_or(0) as u8)
                            .unwrap_or(StatusCode::Unset),
                        message: arrow.status_message.map(|s| s.to_string()),
                    })
                } else {
                    None
                };

                Ok(TelemetryRecord::SpanEnd(SpanEndInfo {
                    trace_id: u128::from_str_radix(&arrow.trace_id, 16)
                        .map_err(|e| format!("Invalid trace_id: {e}"))?,
                    span_id,
                    parent_span_id: arrow.parent_span_id,
                    span_name: span_name.to_string(),
                    start_time_unix_nano: nanos_to_system_time(start_time_unix_nano),
                    end_time_unix_nano: nanos_to_system_time(end_time_unix_nano),
                    attributes: TelemetryAttributes::try_from(arrow.attributes)?,
                    status,
                    severity_number: SeverityNumber::from_repr(arrow.severity_number)
                        .ok_or("Invalid severity_number")?,
                    severity_text: arrow.severity_text.to_string(),
                }))
            }
            TelemetryRecordType::LogRecord => {
                let time_unix_nano = arrow
                    .time_unix_nano
                    .ok_or("Missing time_unix_nano for LogRecord")?;
                let body = arrow.body.ok_or("Missing body for LogRecord")?;

                Ok(TelemetryRecord::LogRecord(LogRecordInfo {
                    time_unix_nano: nanos_to_system_time(time_unix_nano),
                    trace_id: u128::from_str_radix(&arrow.trace_id, 16)
                        .map_err(|e| format!("Invalid trace_id: {e}"))?,
                    span_id: arrow.span_id,
                    span_name: arrow.span_name.map(|s| s.to_string()),
                    severity_number: SeverityNumber::from_repr(arrow.severity_number)
                        .ok_or("Invalid severity_number")?,
                    severity_text: arrow.severity_text.to_string(),
                    body: body.to_string(),
                    attributes: TelemetryAttributes::try_from(arrow.attributes)?,
                }))
            }
        }
    }
}

impl TryFrom<ArrowAttributes<'_>> for TelemetryAttributes {
    type Error = String;

    fn try_from(arrow: ArrowAttributes) -> Result<Self, Self::Error> {
        match arrow.event_type {
            TelemetryAttributesType::Process => Ok(TelemetryAttributes::Process {
                version: arrow
                    .version
                    .ok_or("Missing version for Process attributes")?
                    .to_string(),
                host_os: arrow
                    .host_os
                    .ok_or("Missing host_os for Process attributes")?
                    .to_string(),
                host_arch: arrow
                    .host_arch
                    .ok_or("Missing host_arch for Process attributes")?
                    .to_string(),
            }),
            TelemetryAttributesType::Invocation => {
                let metrics = if arrow.total_errors.is_some()
                    || arrow.total_warnings.is_some()
                    || arrow.autofix_suggestions.is_some()
                {
                    Some(InvocationMetrics {
                        total_errors: arrow.total_errors,
                        total_warnings: arrow.total_warnings,
                        autofix_suggestions: arrow.autofix_suggestions,
                    })
                } else {
                    None
                };
                Ok(TelemetryAttributes::Invocation {
                    invocation_id: arrow
                        .invocation_id
                        .ok_or("Missing invocation_id for Invocation attributes")?
                        .to_string(),
                    command: arrow
                        .command
                        .ok_or("Missing command for Invocation attributes")?
                        .to_string(),
                    target: arrow.target.map(|s| s.to_string()),
                    version: arrow
                        .version
                        .ok_or("Missing version for Invocation attributes")?
                        .to_string(),
                    host_os: arrow
                        .host_os
                        .ok_or("Missing host_os for Invocation attributes")?
                        .to_string(),
                    host_arch: arrow
                        .host_arch
                        .ok_or("Missing host_arch for Invocation attributes")?
                        .to_string(),
                    metrics,
                })
            }
            TelemetryAttributesType::Update => Ok(TelemetryAttributes::Update {
                version: arrow.update_version.map(|s| s.to_string()),
                package: arrow.update_package.map(|s| s.to_string()),
                exe_path: arrow.exe_path.map(|s| s.to_string()),
            }),
            TelemetryAttributesType::Phase => {
                let shared = SharedPhaseInfo {
                    invocation_id: arrow
                        .invocation_id
                        .ok_or("Missing invocation_id for Phase attributes")?
                        .to_string(),
                };
                let phase = arrow.phase.ok_or("Missing phase for Phase attributes")?;
                let phase_info = match phase {
                    BuildPhase::Loading => BuildPhaseInfo::Loading { shared },
                    BuildPhase::DependencyLoading => BuildPhaseInfo::DependencyLoading { shared },
                    BuildPhase::Parsing => BuildPhaseInfo::Parsing { shared },
                    BuildPhase::Scheduling => BuildPhaseInfo::Scheduling { shared },
                    BuildPhase::FreshnessAnalysis => BuildPhaseInfo::FreshnessAnalysis { shared },
                    BuildPhase::Lineage => BuildPhaseInfo::Lineage { shared },
                    BuildPhase::Analyzing => BuildPhaseInfo::Analyzing {
                        shared,
                        node_count: arrow.node_count.unwrap_or(0),
                    },
                    BuildPhase::Compiling => BuildPhaseInfo::Compiling {
                        shared,
                        node_count: arrow.node_count.unwrap_or(0),
                    },
                    BuildPhase::Executing => BuildPhaseInfo::Executing {
                        shared,
                        node_count: arrow.node_count.unwrap_or(0),
                    },
                };
                Ok(TelemetryAttributes::Phase(phase_info))
            }
            TelemetryAttributesType::Node => {
                let node_id = NodeIdentifier {
                    unique_id: arrow
                        .unique_id
                        .ok_or("Missing unique_id for Node attributes")?
                        .to_string(),
                    fqn: arrow
                        .fqn
                        .ok_or("Missing fqn for Node attributes")?
                        .to_string(),
                };
                let phase = arrow.phase.ok_or("Missing phase for Node attributes")?;
                Ok(TelemetryAttributes::Node {
                    node_id,
                    phase,
                    status: arrow.status,
                    num_rows: arrow.num_rows,
                })
            }
            TelemetryAttributesType::DevInternal => {
                let location = arrow_to_location(&arrow);
                Ok(TelemetryAttributes::DevInternal {
                    name: arrow
                        .dev_name
                        .ok_or("Missing dev_name for DevInternal attributes")?
                        .to_string(),
                    location,
                    extra: None, // Arrow format doesn't store extra debug info
                })
            }
            TelemetryAttributesType::Unknown => {
                let location = arrow_to_location(&arrow);
                Ok(TelemetryAttributes::Unknown {
                    name: arrow
                        .dev_name
                        .ok_or("Missing dev_name for Unknown attributes")?
                        .to_string(),
                    location,
                })
            }
            TelemetryAttributesType::Log => {
                let location = arrow_to_location(&arrow);
                Ok(TelemetryAttributes::Log {
                    code: arrow.code,
                    dbt_core_code: arrow.dbt_core_code.map(|s| s.to_string()),
                    original_severity_number: SeverityNumber::from_repr(
                        arrow.original_severity_number.unwrap_or(1),
                    )
                    .unwrap_or_default(),
                    original_severity_text: arrow
                        .original_severity_text
                        .unwrap_or("INFO")
                        .to_string(),
                    location,
                })
            }
            TelemetryAttributesType::LegacyLog => {
                let location = arrow_to_location(&arrow);
                Ok(TelemetryAttributes::LegacyLog {
                    original_severity_number: SeverityNumber::from_repr(
                        arrow.original_severity_number.unwrap_or(1),
                    )
                    .unwrap_or_default(),
                    original_severity_text: arrow
                        .original_severity_text
                        .unwrap_or("INFO")
                        .to_string(),
                    location,
                })
            }
            TelemetryAttributesType::WriteArtifact => Ok(TelemetryAttributes::WriteArtifact {
                relative_path: arrow.relative_path.map(|s| s.to_string()),
                duration_ms: arrow.duration_ms,
            }),
        }
    }
}

/// Creates an Arrow schema for telemetry records.
///
/// This generates the Arrow schema definition that can be used to serialize
/// telemetry records to Parquet or other Arrow-compatible formats.
///
/// Timestamp fields are not using Timestamp(NANOSECOND) type due to serde_arrow
/// limitations. However, serialization function casts them to Timestamp(NANOSECOND)
/// after initial serialization to u64.
///
/// # Returns
///
/// Returns a vector of Arrow field references that define the schema structure,
/// or an error if schema generation fails.
///
/// # Examples
///
/// ```rust
/// use dbt_telemetry::serialize::arrow::create_arrow_schema;
///
/// let schema = create_arrow_schema().expect("Failed to create schema");
/// // Use schema for serialization...
/// ```
pub fn create_arrow_schema() -> Result<(Vec<FieldRef>, Vec<FieldRef>), serde_arrow::Error> {
    let tracing_options = TracingOptions::default().enums_without_data_as_strings(true);

    let serialisable_schema = Vec::<FieldRef>::from_type::<ArrowTelemetryRecord>(tracing_options)?;

    // Convert timestamp columns from u64 to Timestamp
    let schema_with_timestamps = serialisable_schema
        .iter()
        .map(|f| {
            if f.name() == "start_time_unix_nano"
                || f.name() == "end_time_unix_nano"
                || f.name() == "time_unix_nano"
            {
                Arc::new(Field::new(
                    f.name(),
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    true,
                ))
            } else {
                f.clone()
            }
        })
        .collect();

    Ok((serialisable_schema, schema_with_timestamps))
}

/// Serializes telemetry records to an Arrow RecordBatch.
///
/// Converts a slice of telemetry records into an Arrow RecordBatch that can be
/// written to Parquet files or other Arrow-compatible storage formats.
///
/// # Arguments
///
/// * `records` - Slice of telemetry records to serialize
/// * `serialisable_schema` - Arrow schema definition without timestamps (created with [`create_arrow_schema`])
/// * `schema_with_timestamps` - Arrow schema definition with timestamps converted to Timestamp(NANOSECOND)
///
/// # Returns
///
/// Returns an Arrow RecordBatch containing the serialized records, or an error
/// if serialization fails.
///
/// # Examples
///
/// ```rust
/// use dbt_telemetry::serialize::arrow::{create_arrow_schema, serialize_to_arrow};
/// use dbt_telemetry::TelemetryRecord;
///
/// let records: Vec<TelemetryRecord> = vec![/* ... */];
/// let schema = create_arrow_schema().expect("Failed to create schema");
/// let batch = serialize_to_arrow(&records, &schema).expect("Failed to serialize");
/// ```
pub fn serialize_to_arrow(
    records: &[TelemetryRecord],
    serialisable_schema: &[FieldRef],
    schema_with_timestamps: &[FieldRef],
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let arrow_records: Vec<ArrowTelemetryRecord> =
        records.iter().map(ArrowTelemetryRecord::from).collect();

    // Serialize with the temporary schema
    let batch = serde_arrow::to_record_batch(serialisable_schema, &arrow_records)?;

    let mut columns = batch.columns().to_vec();

    for (i, field) in schema_with_timestamps.iter().enumerate() {
        if let DataType::Timestamp(TimeUnit::Nanosecond, None) = field.data_type() {
            if let Some(column) = columns.get(i) {
                columns[i] = cast_with_options(
                    column,
                    &DataType::Timestamp(TimeUnit::Nanosecond, None),
                    &CastOptions {
                        safe: false,
                        format_options: FormatOptions::new().with_display_error(false),
                    },
                )?
            }
        }
    }

    Ok(RecordBatch::try_new(
        Schema::new(schema_with_timestamps).into(),
        columns,
    )?)
}

/// Deserializes telemetry records from an Arrow RecordBatch.
///
/// Converts an Arrow RecordBatch (typically read from a Parquet file) back into
/// telemetry records. This function validates the data during deserialization
/// and will return errors for malformed or missing required fields.
///
/// # Arguments
///
/// * `batch` - Arrow RecordBatch to deserialize from
/// * `serialisable_schema` - Arrow schema definition without timestamps (created with [`create_arrow_schema`])
///
/// # Returns
///
/// Returns a vector of telemetry records, or an error if deserialization fails
/// due to invalid data format or missing required fields.
///
/// # Errors
///
/// This function will return an error if:
/// - The RecordBatch format is incompatible
/// - Required fields are missing (e.g., span_id for span records)
/// - Field values are invalid (e.g., malformed trace_id hex strings)
/// - Enum values are out of range (e.g., invalid severity numbers)
///
/// # Examples
///
/// ```rust
/// use dbt_telemetry::serialize::arrow::deserialize_from_arrow;
/// use arrow::record_batch::RecordBatch;
///
/// let batch: RecordBatch = /* read from file */;
/// let records = deserialize_from_arrow(&batch).expect("Failed to deserialize");
/// ```
pub fn deserialize_from_arrow(
    batch: &RecordBatch,
    serialisable_schema: &[FieldRef],
) -> Result<Vec<TelemetryRecord>, Box<dyn std::error::Error>> {
    // Convert timestamp columns back to u64 for serde_arrow deserialization
    let mut columns = batch.columns().to_vec();

    for col in columns.iter_mut() {
        if let DataType::Timestamp(TimeUnit::Nanosecond, None) = col.data_type() {
            *col = cast_with_options(
                col,
                &DataType::UInt64,
                &CastOptions {
                    safe: false,
                    format_options: FormatOptions::new().with_display_error(false),
                },
            )?;
        }
    }

    // Create temporary batch with u64 timestamp fields
    let temp_batch = RecordBatch::try_new(Schema::new(serialisable_schema).into(), columns)?;

    let arrow_records: Vec<ArrowTelemetryRecord> = serde_arrow::from_record_batch(&temp_batch)
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

    arrow_records
        .into_iter()
        .map(|record| {
            TelemetryRecord::try_from(record).map_err(|e| {
                Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
                    as Box<dyn std::error::Error>
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    // Generate pseudo-random but deterministic values for testing
    fn hash_seed(seed: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        seed.hash(&mut hasher);
        hasher.finish()
    }

    fn create_test_span_start(seed: &str) -> TelemetryRecord {
        let base_hash = hash_seed(seed);
        let trace_id = ((base_hash as u128) << 64) | (base_hash.wrapping_add(1) as u128);
        let span_id = base_hash.wrapping_add(2);
        let parent_span_id = base_hash.wrapping_add(3);
        let start_time = 1600000000000000000u64.wrapping_add(base_hash % 1000000000);

        TelemetryRecord::SpanStart(SpanStartInfo {
            trace_id,
            span_id,
            parent_span_id: Some(parent_span_id),
            span_name: format!("span_{}", base_hash % 1000),
            start_time_unix_nano: SystemTime::UNIX_EPOCH
                + std::time::Duration::from_nanos(start_time),
            attributes: TelemetryAttributes::Process {
                version: format!(
                    "v{}.{}.{}",
                    base_hash % 10,
                    (base_hash >> 8) % 10,
                    (base_hash >> 16) % 10
                ),
                host_os: ["linux", "darwin", "windows"][(base_hash % 3) as usize].to_string(),
                host_arch: ["x86_64", "aarch64", "arm64"][(base_hash % 3) as usize].to_string(),
            },
            severity_number: [
                SeverityNumber::Trace,
                SeverityNumber::Debug,
                SeverityNumber::Info,
                SeverityNumber::Warn,
            ][(base_hash % 4) as usize],
            severity_text: ["TRACE", "DEBUG", "INFO", "WARN"][(base_hash % 4) as usize].to_string(),
        })
    }

    fn create_test_span_end(seed: &str) -> TelemetryRecord {
        let base_hash = hash_seed(seed);
        let trace_id = ((base_hash as u128) << 64) | (base_hash.wrapping_add(1) as u128);
        let span_id = base_hash.wrapping_add(2);
        let parent_span_id = base_hash.wrapping_add(3);
        let start_time = 1600000000000000000u64.wrapping_add(base_hash % 1000000000);
        let end_time = start_time.wrapping_add(base_hash % 10000000);

        TelemetryRecord::SpanEnd(SpanEndInfo {
            trace_id,
            span_id,
            parent_span_id: Some(parent_span_id),
            span_name: format!("span_{}", base_hash % 1000),
            start_time_unix_nano: SystemTime::UNIX_EPOCH
                + std::time::Duration::from_nanos(start_time),
            end_time_unix_nano: SystemTime::UNIX_EPOCH + std::time::Duration::from_nanos(end_time),
            attributes: TelemetryAttributes::Invocation {
                invocation_id: format!("inv_{}", base_hash % 10000),
                command: ["run", "test", "build", "compile"][(base_hash % 4) as usize].to_string(),
                target: Some(["dev", "prod", "staging"][(base_hash % 3) as usize].to_string()),
                version: format!(
                    "v{}.{}.{}",
                    base_hash % 10,
                    (base_hash >> 8) % 10,
                    (base_hash >> 16) % 10
                ),
                host_os: ["linux", "darwin", "windows"][(base_hash % 3) as usize].to_string(),
                host_arch: ["x86_64", "aarch64", "arm64"][(base_hash % 3) as usize].to_string(),
                metrics: Some(InvocationMetrics {
                    total_errors: Some(base_hash % 10),
                    total_warnings: Some((base_hash >> 8) % 20),
                    autofix_suggestions: Some((base_hash >> 16) % 5),
                }),
            },
            status: Some(SpanStatus {
                code: [StatusCode::Unset, StatusCode::Ok, StatusCode::Error]
                    [(base_hash % 3) as usize],
                message: Some(format!("status_{}", base_hash % 100)),
            }),
            severity_number: [
                SeverityNumber::Trace,
                SeverityNumber::Debug,
                SeverityNumber::Info,
                SeverityNumber::Warn,
            ][(base_hash % 4) as usize],
            severity_text: ["TRACE", "DEBUG", "INFO", "WARN"][(base_hash % 4) as usize].to_string(),
        })
    }

    fn create_test_log_record(seed: &str) -> TelemetryRecord {
        let base_hash = hash_seed(seed);
        let trace_id = ((base_hash as u128) << 64) | (base_hash.wrapping_add(1) as u128);
        let span_id = base_hash.wrapping_add(2);
        let log_time = 1600000000000000000u64.wrapping_add(base_hash % 1000000000);

        TelemetryRecord::LogRecord(LogRecordInfo {
            time_unix_nano: SystemTime::UNIX_EPOCH + std::time::Duration::from_nanos(log_time),
            trace_id,
            span_id: Some(span_id),
            span_name: Some(format!("span_{}", base_hash % 1000)),
            severity_number: [
                SeverityNumber::Error,
                SeverityNumber::Warn,
                SeverityNumber::Info,
                SeverityNumber::Debug,
            ][(base_hash % 4) as usize],
            severity_text: ["ERROR", "WARN", "INFO", "DEBUG"][(base_hash % 4) as usize].to_string(),
            body: format!("Log message {}", base_hash % 10000),
            attributes: TelemetryAttributes::Log {
                code: Some((base_hash % 1000) as u32),
                dbt_core_code: Some(format!("E{:03}", base_hash % 999 + 1)),
                original_severity_number: [
                    SeverityNumber::Warn,
                    SeverityNumber::Error,
                    SeverityNumber::Info,
                ][(base_hash % 3) as usize],
                original_severity_text: ["WARN", "ERROR", "INFO"][(base_hash % 3) as usize]
                    .to_string(),
                location: RecordCodeLocation {
                    file: Some(format!(
                        "{}.rs",
                        ["main", "lib", "test", "utils"][(base_hash % 4) as usize]
                    )),
                    line: Some(((base_hash % 1000) + 1) as u32),
                    module_path: Some(format!(
                        "{}::module",
                        ["app", "core", "util", "test"][(base_hash % 4) as usize]
                    )),
                    target: Some(format!("target_{}", base_hash % 100)),
                },
            },
        })
    }

    #[test]
    fn test_arrow_roundtrip_all_record_types() {
        // Create 2 records of each type with different random seeds
        let original_records = vec![
            create_test_span_start("span_start_1"),
            create_test_span_start("span_start_2"),
            create_test_span_end("span_end_1"),
            create_test_span_end("span_end_2"),
            create_test_log_record("log_record_1"),
            create_test_log_record("log_record_2"),
        ];

        let (serialisable_schema, schema_with_timestamps) = create_arrow_schema().unwrap();
        let batch = serialize_to_arrow(
            &original_records,
            &serialisable_schema,
            &schema_with_timestamps,
        )
        .unwrap();
        let deserialized = deserialize_from_arrow(&batch, &serialisable_schema).unwrap();

        assert_eq!(deserialized.len(), 6);

        // Use PartialEq to compare entire records
        for (original, deserialized) in original_records.iter().zip(deserialized.iter()) {
            assert_eq!(
                original, deserialized,
                "Record roundtrip failed for: {original:?}"
            );
        }
    }

    #[test]
    fn test_schema_creation() {
        let (serialisable_schema, schema_with_timestamps) = create_arrow_schema().unwrap();
        assert!(!serialisable_schema.is_empty());
        assert!(!schema_with_timestamps.is_empty());

        // Assert all expected top-level keys present (they are stable)
        [
            "record_type",
            "trace_id",
            "span_id",
            "span_name",
            "parent_span_id",
            "start_time_unix_nano",
            "end_time_unix_nano",
            "time_unix_nano",
            "severity_number",
            "severity_text",
            "body",
            "status_code",
            "status_message",
            "event_type",
            "attributes",
        ]
        .iter()
        .for_each(|&field| {
            let serializable_schema_field = serialisable_schema
                .iter()
                .find(|f| f.name() == field)
                .expect("Missing field in `serialisable_schema`");
            let schema_with_timestamps_field = schema_with_timestamps
                .iter()
                .find(|f| f.name() == field)
                .expect("Missing field in `schema_with_timestamps`");

            if field == "start_time_unix_nano"
                || field == "end_time_unix_nano"
                || field == "time_unix_nano"
            {
                assert_eq!(
                    *schema_with_timestamps_field.data_type(),
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    "Field {field} should be Timestamp(NANOSECOND)"
                );
                assert_eq!(
                    *serializable_schema_field.data_type(),
                    DataType::UInt64,
                    "Field {field} should be UInt64 in `serialisable_schema`"
                );
            } else {
                assert_eq!(
                    serializable_schema_field.data_type(),
                    schema_with_timestamps_field.data_type(),
                    "Field {field} should have the same type in both schemas"
                );
            }
        });
    }
}
