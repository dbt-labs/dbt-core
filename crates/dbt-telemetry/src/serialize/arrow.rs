//! Arrow serialization support for telemetry records using serde_arrow.

use super::to_nanos;
use crate::{
    ArtifactType, ExecutionPhase, LogRecordInfo, NodeCancelReason, NodeErrorType,
    NodeMaterialization, NodeOutcome, NodeSkipReason, NodeType, QueryOutcome, SeverityNumber,
    SpanEndInfo, SpanLinkInfo, SpanStartInfo, SpanStatus, StatusCode, TelemetryAttributes,
    TelemetryEventTypeRegistry, TelemetryOutputFlags, TelemetryRecord, TelemetryRecordType,
};
use arrow::{
    array::{Array, ArrayRef, ListArray, StructArray, new_null_array},
    compute::{CastOptions, cast_with_options},
    datatypes::{DataType, Field, FieldRef, Fields, Schema, TimeUnit},
    record_batch::RecordBatch,
    util::display::FormatOptions,
};
use arrow_schema::extension::Json as JsonExtensionType;
use serde::{Deserialize, Serialize};
// no serde_arrow schema tracing; we build schema manually
use std::{
    borrow::Cow,
    sync::{Arc, LazyLock},
};
use std::{str::FromStr, time::SystemTime};

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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ArrowSpanLink {
    /// Arrow doesn't support u128 natively, so this is stored as a hex string.
    pub trace_id: String,
    pub span_id: u64,
    /// JSON serialized attributes for the link.
    pub json_payload: String,
}

impl<'a> TryFrom<&'a SpanLinkInfo> for ArrowSpanLink {
    type Error = String;

    fn try_from(link: &'a SpanLinkInfo) -> Result<Self, Self::Error> {
        Ok(ArrowSpanLink {
            trace_id: format!("{:032x}", link.trace_id),
            span_id: link.span_id,
            json_payload: serde_json::to_string(&link.attributes)
                .map_err(|e| format!("Failed to serialize SpanLink attributes to JSON: {e}"))?,
        })
    }
}

/// A special type used to derive the schema for telemetry records (envelope) in arrow
/// serialization, as well as a intermediate representation for serialization and deserialization.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ArrowTelemetryRecord<'a> {
    pub record_type: TelemetryRecordType,
    /// Arrow doesn't support u128 natively, so this is stored as a hex string.
    pub trace_id: String,
    pub span_id: Option<u64>,
    pub event_id: Option<Cow<'a, str>>,
    pub span_name: Option<Cow<'a, str>>,
    pub parent_span_id: Option<u64>,
    pub links: Option<Vec<ArrowSpanLink>>,
    pub start_time_unix_nano: Option<u64>,
    pub end_time_unix_nano: Option<u64>,
    pub time_unix_nano: Option<u64>,
    pub severity_number: i32,
    pub severity_text: Cow<'a, str>,
    pub body: Option<Cow<'a, str>>,
    pub status_code: Option<u32>,
    pub status_message: Option<Cow<'a, str>>,
    pub event_type: Cow<'a, str>,
    pub attributes: ArrowAttributes<'a>,
}

/// A special type used to derive the schema for telemetry event attributes in arrow
/// serialization, as well as a intermediate representation for serialization and deserialization.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ArrowAttributes<'a> {
    // This field is used to serialize all non-well known and commonly used attributes,
    // as a JSON blob. This is especially useful for events which are not frequent per
    // -invocation, as it avoids creating many sparse columns in the arrow table.
    pub json_payload: Option<String>,
    // Well-known fields common across many event types
    pub name: Option<Cow<'a, str>>,
    pub database: Option<Cow<'a, str>>,
    pub schema: Option<Cow<'a, str>>,
    pub identifier: Option<Cow<'a, str>>,
    pub dbt_core_event_code: Option<Cow<'a, str>>,
    // Well-known phase fields
    pub phase: Option<ExecutionPhase>,
    // Well-known node fields
    pub unique_id: Option<Cow<'a, str>>,
    pub materialization: Option<NodeMaterialization>,
    pub custom_materialization: Option<Cow<'a, str>>,
    pub node_type: Option<NodeType>,
    pub node_outcome: Option<NodeOutcome>,
    pub node_error_type: Option<NodeErrorType>,
    pub node_cancel_reason: Option<NodeCancelReason>,
    pub node_skip_reason: Option<NodeSkipReason>,
    // CallTrace/Unknown fields
    pub dev_name: Option<Cow<'a, str>>,
    // Code location fields
    pub file: Option<Cow<'a, str>>,
    pub line: Option<u32>,
    // Log fields
    pub code: Option<u32>,
    pub original_severity_number: Option<i32>,
    pub original_severity_text: Option<Cow<'a, str>>,
    // Artifact paths
    pub relative_path: Option<Cow<'a, str>>,
    pub artifact_type: Option<ArtifactType>,
    // Query fields
    pub query_id: Option<Cow<'a, str>>,
    pub query_outcome: Option<QueryOutcome>,
    pub adapter_type: Option<Cow<'a, str>>,
    pub query_error_vendor_code: Option<i32>,
    /// Associated content hash (e.g. can be CAS hash for artifacts stored in CAS).
    pub content_hash: Option<Cow<'a, str>>,
}

#[inline]
fn nanos_to_system_time(nanos: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + std::time::Duration::from_nanos(nanos)
}

impl<'a> TryFrom<&'a TelemetryRecord> for ArrowTelemetryRecord<'a> {
    type Error = String;

    fn try_from(value: &'a TelemetryRecord) -> Result<Self, Self::Error> {
        let event_type = value.attributes().event_type();

        let attributes =
            value.attributes().inner().to_arrow().ok_or_else(|| {
                format!("Missing arrow serializer for event type \"{event_type}\"")
            })?;

        let arrow_record = match value {
            TelemetryRecord::SpanStart(span) => ArrowTelemetryRecord {
                record_type: value.into(),
                trace_id: format!("{:032x}", span.trace_id),
                span_id: Some(span.span_id),
                event_id: None,
                span_name: Some(Cow::Borrowed(span.span_name.as_str())),
                parent_span_id: span.parent_span_id,
                links: span
                    .links
                    .as_deref()
                    .map(|links| {
                        links
                            .iter()
                            .map(ArrowSpanLink::try_from)
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .transpose()?,
                start_time_unix_nano: Some(to_nanos(&span.start_time_unix_nano)),
                end_time_unix_nano: None,
                time_unix_nano: None,
                severity_number: span.severity_number as i32,
                severity_text: Cow::Borrowed(span.severity_text.as_ref()),
                body: None,
                status_code: None,
                status_message: None,
                event_type: event_type.into(),
                attributes,
            },
            TelemetryRecord::SpanEnd(span) => ArrowTelemetryRecord {
                record_type: value.into(),
                trace_id: format!("{:032x}", span.trace_id),
                span_id: Some(span.span_id),
                event_id: None,
                span_name: Some(Cow::Borrowed(span.span_name.as_str())),
                parent_span_id: span.parent_span_id,
                links: span
                    .links
                    .as_deref()
                    .map(|links| {
                        links
                            .iter()
                            .map(ArrowSpanLink::try_from)
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .transpose()?,
                start_time_unix_nano: Some(to_nanos(&span.start_time_unix_nano)),
                end_time_unix_nano: Some(to_nanos(&span.end_time_unix_nano)),
                time_unix_nano: None,
                severity_number: span.severity_number as i32,
                severity_text: Cow::Borrowed(span.severity_text.as_ref()),
                body: None,
                status_code: span.status.as_ref().map(|s| s.code as u32),
                status_message: span
                    .status
                    .as_ref()
                    .and_then(|s| s.message.as_deref().map(Cow::Borrowed)),
                event_type: event_type.into(),
                attributes,
            },
            TelemetryRecord::LogRecord(log) => ArrowTelemetryRecord {
                record_type: value.into(),
                trace_id: format!("{:032x}", log.trace_id),
                span_id: log.span_id,
                event_id: Some(log.event_id.to_string().into()),
                span_name: log.span_name.as_deref().map(Cow::Borrowed),
                parent_span_id: None,
                links: None,
                start_time_unix_nano: None,
                end_time_unix_nano: None,
                time_unix_nano: Some(to_nanos(&log.time_unix_nano)),
                severity_number: log.severity_number as i32,
                severity_text: Cow::Borrowed(log.severity_text.as_ref()),
                body: Some(Cow::Borrowed(log.body.as_str())),
                status_code: None,
                status_message: None,
                event_type: event_type.into(),
                attributes,
            },
        };

        Ok(arrow_record)
    }
}

fn deserialize_record_from_arrow(
    arrow: ArrowTelemetryRecord,
    registry: &TelemetryEventTypeRegistry,
) -> Result<TelemetryRecord, String> {
    let trace_id =
        u128::from_str_radix(&arrow.trace_id, 16).map_err(|e| format!("Invalid trace_id: {e}"))?;

    let attributes_deserializer = registry
        .get_arrow_deserializer(arrow.event_type.as_ref())
        .ok_or_else(|| format!("Unknown event type\"{}\"", arrow.event_type))?;

    let attributes = TelemetryAttributes::new(attributes_deserializer(&arrow.attributes)?);

    let links = if let Some(arrow_links) = arrow.links {
        let mut span_links = Vec::with_capacity(arrow_links.len());
        for link in arrow_links {
            let trace_id = u128::from_str_radix(&link.trace_id, 16)
                .map_err(|e| format!("Invalid trace_id in SpanLink: {e}"))?;
            span_links.push(SpanLinkInfo {
                trace_id,
                span_id: link.span_id,
                attributes: serde_json::from_str(&link.json_payload).map_err(|e| {
                    format!("Failed to deserialize SpanLink attributes from JSON: {e}")
                })?,
            });
        }
        Some(span_links)
    } else {
        None
    };

    match arrow.record_type {
        TelemetryRecordType::SpanStart => {
            let span_id = arrow
                .span_id
                .ok_or("Missing span_id for SpanStart record")?;
            let span_name = arrow
                .span_name
                .ok_or("Missing span_name for SpanStart record")?
                .into_owned();
            let start_time_unix_nano = arrow
                .start_time_unix_nano
                .ok_or("Missing start_time_unix_nano for SpanStart record")?;
            let severity_text = arrow.severity_text.into_owned();

            Ok(TelemetryRecord::SpanStart(SpanStartInfo {
                trace_id,
                span_id,
                parent_span_id: arrow.parent_span_id,
                links,
                span_name,
                start_time_unix_nano: nanos_to_system_time(start_time_unix_nano),
                attributes,
                severity_number: SeverityNumber::try_from(arrow.severity_number)
                    .map_err(|_| "Invalid severity_number")?,
                severity_text,
            }))
        }
        TelemetryRecordType::SpanEnd => {
            let span_id = arrow.span_id.ok_or("Missing span_id for SpanEnd record")?;
            let span_name = arrow
                .span_name
                .ok_or("Missing span_name for SpanEnd record")?
                .into_owned();
            let start_time_unix_nano = arrow
                .start_time_unix_nano
                .ok_or("Missing start_time_unix_nano for SpanEnd record")?;
            let end_time_unix_nano = arrow
                .end_time_unix_nano
                .ok_or("Missing end_time_unix_nano for SpanEnd record")?;
            let severity_text = arrow.severity_text.into_owned();

            let status = if arrow.status_code.is_some() || arrow.status_message.is_some() {
                Some(SpanStatus {
                    code: StatusCode::from_repr(arrow.status_code.unwrap_or(0) as u8)
                        .unwrap_or(StatusCode::Unset),
                    message: arrow.status_message.map(Cow::into_owned),
                })
            } else {
                None
            };

            Ok(TelemetryRecord::SpanEnd(SpanEndInfo {
                trace_id,
                span_id,
                parent_span_id: arrow.parent_span_id,
                links,
                span_name,
                start_time_unix_nano: nanos_to_system_time(start_time_unix_nano),
                end_time_unix_nano: nanos_to_system_time(end_time_unix_nano),
                attributes,
                status,
                severity_number: SeverityNumber::try_from(arrow.severity_number)
                    .map_err(|_| "Invalid severity_number")?,
                severity_text,
            }))
        }
        TelemetryRecordType::LogRecord => {
            let time_unix_nano = arrow
                .time_unix_nano
                .ok_or("Missing time_unix_nano for LogRecord")?;
            let body = arrow.body.ok_or("Missing body for LogRecord")?.into_owned();
            let severity_text = arrow.severity_text.into_owned();

            Ok(TelemetryRecord::LogRecord(LogRecordInfo {
                time_unix_nano: nanos_to_system_time(time_unix_nano),
                trace_id,
                span_id: arrow.span_id,
                event_id: uuid::Uuid::from_str(arrow.event_id.ok_or("Missing event_id")?.as_ref())
                    .map_err(|e| format!("Failed to deserialize `event_id` from JSON: {e}"))?,
                span_name: arrow.span_name.map(|name| name.into_owned()),
                severity_number: SeverityNumber::try_from(arrow.severity_number)
                    .map_err(|_| "Invalid severity_number")?,
                severity_text,
                body,
                attributes,
            }))
        }
    }
}

fn large_utf8_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::LargeUtf8, nullable)
}

fn dict_utf8_field(name: &str, nullable: bool) -> Field {
    Field::new(
        name,
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        nullable,
    )
}

fn json_large_utf8_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::LargeUtf8, nullable)
        .with_extension_type(JsonExtensionType::default())
}

/// Creates an Arrow schema for telemetry records.
///
/// This generates the Arrow schema definition that can be used to serialize
/// telemetry records to Parquet or other Arrow-compatible formats.
///
/// It returns two schemas:
/// 1. `serialisable_schema`: Used to convert Vec<Struct> -> RecordBatch with timestamp fields as `u64`.
///    This is a current limitation of the `serde_arrow` library, which doesn't support serializing
///    `SystemTime` or `Timestamp` types directly. These RecordBatches are never returned or stored,
///    they are only an intermediate step in the serialization process.
/// 2. `schema_with_timestamps`: The final schema with timestamp fields converted to `Timestamp(NANOSECOND)`.
///
/// This function is used to generate lazy static schemas, that are then used  by `serialize_to_arrow` and `deserialize_from_arrow`.
///
/// # Returns
///
/// Returns two vectors of Arrow field references that define the schema structure,
/// or an error if schema generation fails.
fn create_arrow_schema() -> (Vec<FieldRef>, Vec<FieldRef>) {
    // ArrowSpanLink struct fields
    let span_link_fields = Fields::from(vec![
        dict_utf8_field("trace_id", false),
        Field::new("span_id", DataType::UInt64, false),
        json_large_utf8_field("json_payload", true),
    ]);

    // ArrowAttributes struct fields
    let attributes_fields = Fields::from(vec![
        // JSON blob for non well-known attributes
        json_large_utf8_field("json_payload", true),
        // Well-known common fields
        dict_utf8_field("name", true),
        dict_utf8_field("database", true),
        dict_utf8_field("schema", true),
        dict_utf8_field("identifier", true),
        dict_utf8_field("dbt_core_event_code", true),
        // Phase
        dict_utf8_field("phase", true),
        // Node fields
        dict_utf8_field("unique_id", true),
        dict_utf8_field("materialization", true),
        dict_utf8_field("custom_materialization", true),
        dict_utf8_field("node_type", true),
        dict_utf8_field("node_outcome", true),
        dict_utf8_field("node_error_type", true),
        dict_utf8_field("node_cancel_reason", true),
        dict_utf8_field("node_skip_reason", true),
        // CallTrace/Unknown fields
        dict_utf8_field("dev_name", true),
        // Code location fields
        dict_utf8_field("file", true),
        Field::new("line", DataType::UInt32, true),
        // Log fields
        Field::new("code", DataType::UInt32, true),
        Field::new("original_severity_number", DataType::Int32, true),
        dict_utf8_field("original_severity_text", true),
        // Artifact paths
        large_utf8_field("relative_path", true),
        dict_utf8_field("artifact_type", true),
        // Query fields
        large_utf8_field("query_id", true),
        dict_utf8_field("query_outcome", true),
        dict_utf8_field("adapter_type", true),
        Field::new("query_error_vendor_code", DataType::Int32, true),
        // Content hash (e.g. CAS hash for artifacts stored in CAS)
        large_utf8_field("content_hash", true),
    ]);

    // Top-level fields for ArrowTelemetryRecord
    let serialisable_schema: Vec<FieldRef> = vec![
        dict_utf8_field("record_type", false).into(),
        dict_utf8_field("trace_id", false).into(),
        Arc::new(Field::new("span_id", DataType::UInt64, true)),
        large_utf8_field("event_id", true).into(),
        large_utf8_field("span_name", true).into(),
        Arc::new(Field::new("parent_span_id", DataType::UInt64, true)),
        Arc::new(Field::new(
            "links",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(span_link_fields),
                false,
            ))),
            true,
        )),
        Arc::new(Field::new("start_time_unix_nano", DataType::UInt64, true)),
        Arc::new(Field::new("end_time_unix_nano", DataType::UInt64, true)),
        Arc::new(Field::new("time_unix_nano", DataType::UInt64, true)),
        Arc::new(Field::new("severity_number", DataType::Int32, false)),
        dict_utf8_field("severity_text", false).into(),
        large_utf8_field("body", true).into(),
        Arc::new(Field::new("status_code", DataType::UInt32, true)),
        large_utf8_field("status_message", true).into(),
        dict_utf8_field("event_type", false).into(),
        Arc::new(Field::new(
            "attributes",
            DataType::Struct(attributes_fields),
            false,
        )),
    ];

    // Convert timestamp columns from u64 to Timestamp(NANOSECOND)
    let schema_with_timestamps: Vec<FieldRef> = serialisable_schema
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

    (serialisable_schema, schema_with_timestamps)
}

static ARROW_SCHEMAS: LazyLock<(Vec<FieldRef>, Vec<FieldRef>)> = LazyLock::new(create_arrow_schema);

fn get_serialisable_schema() -> &'static [FieldRef] {
    &ARROW_SCHEMAS.0
}

pub fn get_telemetry_arrow_schema() -> &'static [FieldRef] {
    &ARROW_SCHEMAS.1
}

/// Serializes telemetry records to an Arrow RecordBatch.
///
/// Converts a slice of telemetry records into an Arrow RecordBatch that can be
/// written to Parquet files or other Arrow-compatible storage formats.
///
/// Top-level envelope datetime fields are converted to Timestamp(NANOSECOND) type.
///
/// # Arguments
///
/// * `records` - Slice of telemetry records to serialize
///
/// # Returns
///
/// Returns an Arrow RecordBatch containing the serialized records, or an error
/// if serialization fails.
///
/// # Examples
///
/// ```rust
/// use dbt_telemetry::serialize::arrow::serialize_to_arrow;
/// use dbt_telemetry::TelemetryRecord;
///
/// let records: Vec<TelemetryRecord> = vec![/* ... */];
/// let batch = serialize_to_arrow(&records).expect("Failed to serialize");
/// ```
pub fn serialize_to_arrow(
    records: &[TelemetryRecord],
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let mut errors: Vec<String> = Vec::new();

    let arrow_records: Vec<ArrowTelemetryRecord> = records
        .iter()
        .filter(|r| {
            // Only include records with serializable attributes
            r.attributes()
                .output_flags()
                .contains(TelemetryOutputFlags::EXPORT_PARQUET)
        })
        .filter_map(|r| {
            ArrowTelemetryRecord::try_from(r)
                .map_err(|e| errors.push(e))
                .ok()
        })
        .collect();

    if !errors.is_empty() {
        // As of today, this should never happen because we filter out records with non-serializable attributes
        // above via export flags and this is the only realistic error case.
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Failed to serialize some records: {}", errors.join("; ")),
        )));
    }

    // Serialize with the temporary schema (timestamp fields as u64),
    // see `create_arrow_schema` for details.
    let batch = serde_arrow::to_record_batch(get_serialisable_schema(), &arrow_records)?;

    let mut columns = batch.columns().to_vec();

    // Convert timestamp columns from u64 to Timestamp(NANOSECOND),
    // this is zero-copy, just metadata change.
    let schema_with_timestamps = get_telemetry_arrow_schema();
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
/// * `registry` - Registry of telemetry event types for deserialization
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
/// - Unknown event types are encountered - means that the registry is missing an entry
///
/// # Examples
///
/// ```rust
/// use dbt_telemetry::serialize::arrow::deserialize_from_arrow;
/// use dbt_telemetry::TelemetryEventTypeRegistry;
/// use arrow::record_batch::RecordBatch;
///
/// let batch: RecordBatch = /* read from file */;
/// let records = deserialize_from_arrow(&batch, &TelemetryEventTypeRegistry::public()).expect("Failed to deserialize");
/// ```
pub fn deserialize_from_arrow(
    batch: &RecordBatch,
    registry: &TelemetryEventTypeRegistry,
) -> Result<Vec<TelemetryRecord>, Box<dyn std::error::Error>> {
    let temp_batch = normalize_batch(batch)?;

    let arrow_records: Vec<ArrowTelemetryRecord> = serde_arrow::from_record_batch(&temp_batch)
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

    arrow_records
        .into_iter()
        .map(|record| {
            deserialize_record_from_arrow(record, registry).map_err(|e| {
                Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
                    as Box<dyn std::error::Error>
            })
        })
        .collect()
}

/// Normalizes incoming `RecordBatch` to make it compatible with the
/// serde_arrow schema used by telemetry deserialization.
///
/// * Columns are matched by name.
/// * Missing required columns and incompatible type conversions are accumulated
///   and reported together.
/// * Missing nullable columns are filled with nulls.
/// * Extra columns present in the input batch are ignored.
/// * String-like columns (plain or dictionary) are accepted without casting
///   so long as both sides are string compatible.
/// * other mismatches are cast to the expected type when possible.
fn normalize_batch(batch: &RecordBatch) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let serialisable_schema = get_serialisable_schema();
    let batch_schema = batch.schema();

    let mut missing_columns = Vec::new();
    let mut type_errors = Vec::new();
    let mut normalized_columns = Vec::with_capacity(serialisable_schema.len());

    for expected_field in serialisable_schema.iter() {
        let expected_field = expected_field.as_ref();
        let Some((index, actual_field)) = batch_schema.column_with_name(expected_field.name())
        else {
            if expected_field.is_nullable() {
                let array = new_null_array(expected_field.data_type(), batch.num_rows());
                normalized_columns.push(Some(NormalizedColumn {
                    field: Arc::new(expected_field.clone()),
                    array,
                    metadata_changed: true,
                }));
            } else {
                missing_columns.push(expected_field.name().to_string());
                normalized_columns.push(None);
            }
            continue;
        };

        let column = batch.column(index);
        match normalize_column(expected_field.name(), column, expected_field, actual_field) {
            Ok(normalized) => normalized_columns.push(Some(normalized)),
            Err(mut errors) => {
                type_errors.append(&mut errors);
                normalized_columns.push(None);
            }
        }
    }

    if !missing_columns.is_empty() || !type_errors.is_empty() {
        let mut parts = Vec::new();
        if !missing_columns.is_empty() {
            parts.push(format!("missing columns: {}", missing_columns.join(", ")));
        }
        if !type_errors.is_empty() {
            parts.push(format!("incompatible columns: {}", type_errors.join("; ")));
        }
        return Err(Box::new(arrow::error::ArrowError::SchemaError(
            parts.join("; "),
        )));
    }

    let normalized: Vec<NormalizedColumn> = normalized_columns
        .into_iter()
        .map(|opt| opt.expect("errors handled above"))
        .collect();

    let fields: Vec<FieldRef> = normalized.iter().map(|col| col.field.clone()).collect();
    let arrays: Vec<ArrayRef> = normalized.into_iter().map(|col| col.array).collect();

    Ok(RecordBatch::try_new(Schema::new(fields).into(), arrays)?)
}

struct NormalizedColumn {
    field: FieldRef,
    array: ArrayRef,
    metadata_changed: bool,
}

/// Validates and normalizes a single column (and any nested children) such that
/// it can be safely deserialized by serde_arrow into the expected type.
///
/// * String-like columns (`Utf8`, `LargeUtf8`, or dictionaries of those types)
///   are accepted without casting.
/// * Non-nullable fields in the batch are allowed in place of nullable expected
///   fields, but not vice versa.
/// * Struct and list columns are normalized recursively on their children.
/// * All other mismatches are cast to the expected type; failures return a
///   descriptive error message.
fn normalize_column(
    path: &str,
    array: &ArrayRef,
    expected_field: &Field,
    actual_field: &Field,
) -> Result<NormalizedColumn, Vec<String>> {
    let expected_type = expected_field.data_type();
    let actual_type = actual_field.data_type();

    if is_string_like(expected_type) && is_string_like(actual_type) {
        let (field, metadata_changed) =
            reconcile_field(expected_field, actual_field, actual_type.clone());
        return Ok(NormalizedColumn {
            field,
            array: array.clone(),
            metadata_changed,
        });
    }

    match expected_type {
        DataType::Struct(_) => normalize_struct_column(path, array, expected_field, actual_field),
        DataType::List(expected_child_field) => normalize_list_column(
            path,
            array,
            expected_field,
            actual_field,
            expected_child_field,
        ),
        _ => normalize_primitive_column(path, array, expected_field, actual_field),
    }
}

fn normalize_struct_column(
    path: &str,
    array: &ArrayRef,
    expected_field: &Field,
    actual_field: &Field,
) -> Result<NormalizedColumn, Vec<String>> {
    let struct_array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            vec![format!(
                "field {path}: expected Struct but found {:?}",
                array.data_type()
            )]
        })?;

    let DataType::Struct(expected_fields) = expected_field.data_type() else {
        unreachable!("expected_field should be Struct");
    };

    let DataType::Struct(actual_fields) = actual_field.data_type() else {
        return Err(vec![format!(
            "field {path}: expected Struct but found {:?}",
            actual_field.data_type()
        )]);
    };

    let mut child_arrays = Vec::with_capacity(expected_fields.len());
    let mut child_fields = Vec::with_capacity(expected_fields.len());
    let mut errors = Vec::new();
    let mut needs_rebuild = false;

    for expected_child in expected_fields.iter() {
        let child_path = format!("{path}.{}", expected_child.name());
        let Some((child_index, actual_child_field)) = actual_fields
            .iter()
            .enumerate()
            .find(|(_, actual_child)| actual_child.name() == expected_child.name())
        else {
            if expected_child.is_nullable() {
                child_arrays.push(new_null_array(
                    expected_child.data_type(),
                    struct_array.len(),
                ));
                child_fields.push(expected_child.clone());
                needs_rebuild = true;
            } else {
                errors.push(format!("field {child_path}: missing required field"));
            }
            continue;
        };

        let child_array = struct_array.column(child_index);
        match normalize_column(
            &child_path,
            child_array,
            expected_child.as_ref(),
            actual_child_field.as_ref(),
        ) {
            Ok(child_column) => {
                if !Arc::ptr_eq(&child_column.array, child_array) || child_column.metadata_changed {
                    needs_rebuild = true;
                }
                child_arrays.push(child_column.array);
                child_fields.push(child_column.field);
            }
            Err(mut child_errors) => errors.append(&mut child_errors),
        }
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    if child_arrays.len() != expected_fields.len() {
        return Err(vec![format!(
            "field {path}: unable to normalize because some children were missing"
        )]);
    }

    let child_fields_struct: Fields = child_fields.clone().into();
    let (new_field, parent_metadata_changed) = reconcile_field(
        expected_field,
        actual_field,
        DataType::Struct(child_fields_struct.clone()),
    );

    let array: ArrayRef = if needs_rebuild {
        Arc::new(StructArray::new(
            child_fields_struct,
            child_arrays,
            struct_array.logical_nulls(),
        ))
    } else {
        array.clone()
    };

    Ok(NormalizedColumn {
        field: new_field,
        array,
        metadata_changed: parent_metadata_changed || needs_rebuild,
    })
}

fn normalize_list_column(
    path: &str,
    array: &ArrayRef,
    expected_field: &Field,
    actual_field: &Field,
    expected_child_field: &FieldRef,
) -> Result<NormalizedColumn, Vec<String>> {
    let list_array = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        vec![format!(
            "field {path}: expected List but found {:?}",
            array.data_type()
        )]
    })?;

    let values = list_array.values();
    let actual_child_field = match actual_field.data_type() {
        DataType::List(field) => field.clone(),
        other => {
            return Err(vec![format!(
                "field {path}: expected List but found {:?}",
                other
            )]);
        }
    };
    let child_path = format!("{path}[]");
    let child_column = normalize_column(
        &child_path,
        values,
        expected_child_field.as_ref(),
        actual_child_field.as_ref(),
    )?;

    let needs_rebuild = child_column.metadata_changed || !Arc::ptr_eq(&child_column.array, values);

    let array: ArrayRef = if needs_rebuild {
        Arc::new(ListArray::new(
            child_column.field.clone(),
            list_array.offsets().clone(),
            child_column.array.clone(),
            list_array.nulls().cloned(),
        ))
    } else {
        array.clone()
    };

    let (new_field, parent_metadata_changed) = reconcile_field(
        expected_field,
        actual_field,
        DataType::List(child_column.field),
    );

    Ok(NormalizedColumn {
        field: new_field,
        array,
        metadata_changed: parent_metadata_changed || needs_rebuild,
    })
}

fn normalize_primitive_column(
    path: &str,
    array: &ArrayRef,
    expected_field: &Field,
    actual_field: &Field,
) -> Result<NormalizedColumn, Vec<String>> {
    let expected_type = expected_field.data_type();
    if array.data_type() == expected_type {
        let (field, metadata_changed) =
            reconcile_field(expected_field, actual_field, expected_type.clone());
        return Ok(NormalizedColumn {
            field,
            array: array.clone(),
            metadata_changed,
        });
    }

    match cast_array(array, expected_type) {
        Ok(casted) => {
            let (field, _) = reconcile_field(expected_field, actual_field, expected_type.clone());
            Ok(NormalizedColumn {
                field,
                array: casted,
                metadata_changed: true,
            })
        }
        Err(err) => Err(vec![format!(
            "field {path}: cannot cast from {:?} to {:?}: {err}",
            array.data_type(),
            expected_type
        )]),
    }
}

fn reconcile_field(
    expected_field: &Field,
    actual_field: &Field,
    data_type: DataType,
) -> (FieldRef, bool) {
    let updated = expected_field.clone().with_data_type(data_type);
    let metadata_changed = updated != *actual_field;
    (Arc::new(updated), metadata_changed)
}

fn is_string_like(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 => true,
        DataType::Dictionary(_, value) => is_string_like(value.as_ref()),
        _ => false,
    }
}

fn cast_array(
    array: &ArrayRef,
    data_type: &DataType,
) -> Result<ArrayRef, Box<dyn std::error::Error>> {
    Ok(cast_with_options(
        array.as_ref(),
        data_type,
        &CastOptions {
            safe: false,
            format_options: FormatOptions::new().with_display_error(false),
        },
    )?)
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Array, DictionaryArray, Int32Builder, LargeStringArray, StructArray, UInt64Array},
        datatypes::{DataType, Fields, Int32Type, Schema},
    };
    use fake::rand::SeedableRng;
    use fake::rand::rngs::StdRng;
    use fake::{Fake, Faker};
    use parquet::{
        arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
        basic::Compression,
        file::properties::WriterProperties,
    };

    use crate::TelemetryEventRecType;

    use super::*;
    use std::hash::{Hash, Hasher};
    use std::sync::Arc;
    use std::{
        collections::{HashMap, HashSet, hash_map::DefaultHasher},
        rc::Rc,
    };

    // Generate pseudo-random but deterministic values for testing
    fn hash_seed(seed: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        seed.hash(&mut hasher);
        hasher.finish()
    }

    fn create_fake_attributes(seed: &str, event_type: &'static str) -> TelemetryAttributes {
        let faker = TelemetryEventTypeRegistry::public()
            .get_faker(event_type)
            .unwrap_or_else(|| panic!("No faker defined for event type \"{event_type}\""));

        TelemetryAttributes::new(faker(seed))
    }

    fn create_all_fake_attributes(seed: &str) -> Vec<TelemetryAttributes> {
        let mut attributes = Vec::new();
        for event_type in TelemetryEventTypeRegistry::public().iter() {
            let attrs = create_fake_attributes(seed, event_type);

            // Skip variants that are known to not be serialized
            if !attrs
                .output_flags()
                .contains(TelemetryOutputFlags::EXPORT_PARQUET)
            {
                continue;
            }

            attributes.push(attrs);
        }
        attributes
    }

    fn create_test_span_start(seed: &str, attributes: TelemetryAttributes) -> TelemetryRecord {
        let hashed_seed = hash_seed(seed);
        let mut rng = StdRng::seed_from_u64(hashed_seed);
        let trace_id = Faker.fake_with_rng(&mut rng);
        let span_id = Faker.fake_with_rng(&mut rng);
        let parent_span_id = Faker.fake_with_rng(&mut rng);
        let start_time = Faker.fake_with_rng(&mut rng);

        TelemetryRecord::SpanStart(SpanStartInfo {
            trace_id,
            span_id,
            parent_span_id: Some(parent_span_id),
            links: None,
            span_name: attributes.event_display_name(),
            start_time_unix_nano: SystemTime::UNIX_EPOCH
                + std::time::Duration::from_nanos(start_time),
            attributes,
            severity_number: Faker.fake_with_rng(&mut rng),
            severity_text: ["TRACE", "DEBUG", "INFO", "WARN"][(hashed_seed % 4) as usize]
                .to_string(),
        })
    }

    fn create_test_span_end(seed: &str, span_start: &TelemetryRecord) -> TelemetryRecord {
        let TelemetryRecord::SpanStart(span_start_info) = span_start else {
            panic!("Expected SpanStart record");
        };

        let hashed_seed = hash_seed(seed);
        let mut rng = StdRng::seed_from_u64(hashed_seed);
        let elapsed = Faker.fake_with_rng(&mut rng);

        TelemetryRecord::SpanEnd(SpanEndInfo {
            trace_id: span_start_info.trace_id,
            span_id: span_start_info.span_id,
            parent_span_id: span_start_info.parent_span_id,
            links: span_start_info.links.clone(),
            span_name: span_start_info.span_name.clone(),
            start_time_unix_nano: span_start_info.start_time_unix_nano,
            end_time_unix_nano: span_start_info.start_time_unix_nano
                + std::time::Duration::from_nanos(elapsed),
            attributes: span_start_info.attributes.clone(),
            status: Some(SpanStatus {
                code: [StatusCode::Unset, StatusCode::Ok, StatusCode::Error]
                    [(hashed_seed % 3) as usize],
                message: Some(format!("status_{}", hashed_seed % 100)),
            }),
            severity_number: Faker.fake_with_rng(&mut rng),
            severity_text: ["TRACE", "DEBUG", "INFO", "WARN"][(hashed_seed % 4) as usize]
                .to_string(),
        })
    }

    fn create_test_log_record(seed: &str, attributes: TelemetryAttributes) -> TelemetryRecord {
        let hashed_seed = hash_seed(seed);
        let mut rng = StdRng::seed_from_u64(hashed_seed);
        let trace_id = Faker.fake_with_rng(&mut rng);
        let span_id = Faker.fake_with_rng(&mut rng);
        let log_time = Faker.fake_with_rng(&mut rng);

        TelemetryRecord::LogRecord(LogRecordInfo {
            time_unix_nano: SystemTime::UNIX_EPOCH + std::time::Duration::from_nanos(log_time),
            trace_id,
            span_id: Some(span_id),
            event_id: Faker.fake_with_rng(&mut rng),
            span_name: Some(attributes.event_display_name()),
            severity_number: Faker.fake_with_rng(&mut rng),
            severity_text: ["ERROR", "WARN", "INFO", "DEBUG"][(hashed_seed % 4) as usize]
                .to_string(),
            body: format!("Log message {}", hashed_seed % 10000),
            attributes,
        })
    }

    fn batch_with_dictionary_large_utf8_column(
        batch: &RecordBatch,
        column_name: &str,
    ) -> RecordBatch {
        let column_index = batch
            .schema()
            .index_of(column_name)
            .expect("column should exist");
        let column = batch.column(column_index);
        let large_utf8 =
            cast_array(column, &DataType::LargeUtf8).expect("failed to cast column to LargeUtf8");
        let string_array = large_utf8
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .expect("expected LargeUtf8 column");

        let mut builder = Int32Builder::with_capacity(string_array.len());
        let mut dictionary = Vec::new();
        let mut indices: HashMap<String, i32> = HashMap::new();

        for value in string_array.iter() {
            match value {
                Some(value) => {
                    let entry = indices.entry(value.to_string()).or_insert_with(|| {
                        let idx = dictionary.len() as i32;
                        dictionary.push(value.to_string());
                        idx
                    });
                    builder.append_value(*entry);
                }
                None => builder.append_null(),
            }
        }

        let keys = builder.finish();
        let values = Arc::new(LargeStringArray::from(dictionary)) as ArrayRef;
        let dictionary_array = Arc::new(
            DictionaryArray::<Int32Type>::try_new(keys, values)
                .expect("failed to build dictionary"),
        ) as ArrayRef;

        replace_column(batch, column_index, dictionary_array)
    }

    fn batch_with_large_utf8_column(batch: &RecordBatch, column_name: &str) -> RecordBatch {
        let column_index = batch
            .schema()
            .index_of(column_name)
            .expect("column should exist");
        let column = batch.column(column_index);
        let large_utf8 =
            cast_array(column, &DataType::LargeUtf8).expect("failed to cast to LargeUtf8");

        replace_column(batch, column_index, large_utf8)
    }

    fn batch_with_extra_column(batch: &RecordBatch) -> RecordBatch {
        let mut fields: Vec<FieldRef> = batch
            .schema()
            .fields()
            .iter()
            .map(|field| Arc::new(field.as_ref().clone()))
            .collect();
        fields.push(Arc::new(Field::new(
            "__test_extra_column",
            DataType::UInt64,
            true,
        )));

        let extra_values = UInt64Array::from(vec![Some(42u64); batch.num_rows()]);
        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(extra_values) as ArrayRef);

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns).expect("failed to build batch with extra column")
    }

    fn batch_with_non_nullable_body(batch: &RecordBatch) -> RecordBatch {
        let column_index = batch
            .schema()
            .index_of("body")
            .expect("body column should exist");
        assert!(
            batch
                .schema()
                .field_with_name("body")
                .expect("body column should exist")
                .is_nullable(),
            "Body field expected to be nullablefor this test"
        );
        let column = batch.column(column_index);
        assert_eq!(
            column.null_count(),
            0,
            "body column cannot be made non-nullable when it contains nulls"
        );

        let mut fields: Vec<FieldRef> = batch
            .schema()
            .fields()
            .iter()
            .map(|field| Arc::new(field.as_ref().clone()))
            .collect();
        fields[column_index] = Arc::new(
            batch
                .schema()
                .field(column_index)
                .clone()
                .with_nullable(false),
        );

        let columns = batch.columns().to_vec();
        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns).expect("failed to make body non-nullable")
    }

    fn batch_with_non_nullable_json_payload(batch: &RecordBatch) -> RecordBatch {
        let attributes_index = batch
            .schema()
            .index_of("attributes")
            .expect("attributes column should exist");

        let attributes_column = batch.column(attributes_index).clone();
        let struct_array = attributes_column
            .as_ref()
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("attributes column should be a StructArray");

        let DataType::Struct(child_fields) = attributes_column.data_type() else {
            panic!("attributes column should have struct data type");
        };

        let mut updated_fields: Vec<FieldRef> = child_fields
            .iter()
            .map(|field| Arc::new(field.as_ref().clone()))
            .collect();

        let json_index = updated_fields
            .iter()
            .position(|field| field.name() == "json_payload")
            .expect("json_payload field should exist");

        let json_column = struct_array.column(json_index);
        let json_values = json_column
            .as_ref()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .expect("json_payload should be LargeUtf8");

        let cleaned_json_column: ArrayRef = Arc::new(LargeStringArray::from_iter_values(
            json_values
                .iter()
                .map(|value| value.unwrap_or_default().to_owned()),
        ));

        let updated_json_field = updated_fields[json_index]
            .as_ref()
            .clone()
            .with_nullable(false);
        updated_fields[json_index] = Arc::new(updated_json_field);

        let mut child_columns: Vec<ArrayRef> = (0..struct_array.num_columns())
            .map(|idx| struct_array.column(idx).clone())
            .collect();
        child_columns[json_index] = cleaned_json_column;

        let new_struct = Arc::new(StructArray::new(
            Fields::from(updated_fields),
            child_columns,
            struct_array.logical_nulls(),
        )) as ArrayRef;

        replace_column(batch, attributes_index, new_struct)
    }

    fn batch_missing_column(batch: &RecordBatch, column_name: &str) -> RecordBatch {
        let column_index = batch
            .schema()
            .index_of(column_name)
            .expect("column should exist");

        let fields: Vec<FieldRef> = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|&(idx, _)| (idx != column_index))
            .map(|(_, field)| Arc::new(field.as_ref().clone()))
            .collect();

        let columns: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .enumerate()
            .filter(|&(idx, _)| (idx != column_index))
            .map(|(_, column)| column.clone())
            .collect();

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns).expect("failed to remove column")
    }

    fn replace_column(
        batch: &RecordBatch,
        column_index: usize,
        new_column: ArrayRef,
    ) -> RecordBatch {
        let mut fields: Vec<FieldRef> = batch
            .schema()
            .fields()
            .iter()
            .map(|field| Arc::new(field.as_ref().clone()))
            .collect();

        fields[column_index] = Arc::new(
            batch
                .schema()
                .field(column_index)
                .clone()
                .with_data_type(new_column.data_type().clone()),
        );

        let mut columns = batch.columns().to_vec();
        columns[column_index] = new_column;

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns).expect("failed to replace column")
    }

    #[test]
    fn test_deserialize_from_arrow_schema_normalization() {
        let attributes = create_all_fake_attributes("schema_norm_seed")
            .into_iter()
            .find(|attrs| matches!(attrs.record_category(), TelemetryEventRecType::Log))
            .expect("expected at least one log attribute");

        let log_record = create_test_log_record("schema_norm_seed", attributes);
        let records = vec![log_record];
        let base_batch = serialize_to_arrow(&records).expect("failed to serialize base batch");
        let registry = TelemetryEventTypeRegistry::public();

        let variations = vec![
            (
                "event_type_large_utf8_instead_of_dict_utf8",
                batch_with_large_utf8_column(&base_batch, "event_type"),
            ),
            (
                "event_type_dict_large_utf8_instead_of_dict_utf8",
                batch_with_dictionary_large_utf8_column(&base_batch, "event_type"),
            ),
            (
                "severity_text_large_utf8_instead_of_dict_utf8",
                batch_with_large_utf8_column(&base_batch, "severity_text"),
            ),
            ("extra_column", batch_with_extra_column(&base_batch)),
            (
                "body_non_nullable",
                batch_with_non_nullable_body(&base_batch),
            ),
            (
                "attributes_json_payload_non_nullable",
                batch_with_non_nullable_json_payload(&base_batch),
            ),
            (
                "missing_nullable_column",
                batch_missing_column(&base_batch, "links"),
            ),
        ];

        for (name, variant) in variations {
            let deserialized = deserialize_from_arrow(&variant, registry)
                .unwrap_or_else(|e| panic!("expected success for {name}: {e}"));
            assert_eq!(deserialized, records, "variation {name} mismatch");
        }

        let missing = batch_missing_column(&base_batch, "event_type");
        assert!(
            deserialize_from_arrow(&missing, registry).is_err(),
            "missing required column should fail"
        );
    }

    #[test]
    fn test_arrow_roundtrip_all_record_types() {
        // Create records of each record & event (aka attribute) type with a pseudo-random seed
        let mut original_records = vec![];
        create_all_fake_attributes("test_seed")
            .iter()
            .for_each(|attributes| {
                match attributes.record_category() {
                    // Span types
                    TelemetryEventRecType::Span => {
                        let span_start = create_test_span_start("test_seed", attributes.clone());
                        // Create a matching span end for the start
                        let span_end = create_test_span_end("test_seed", &span_start);
                        original_records.push(span_start);
                        original_records.push(span_end);
                    }
                    TelemetryEventRecType::Log => {
                        // Create a log record
                        let log_record = create_test_log_record("test_seed", attributes.clone());
                        original_records.push(log_record);
                    }
                }
            });

        let batch = serialize_to_arrow(&original_records).unwrap();
        let mut deserialized =
            deserialize_from_arrow(&batch, TelemetryEventTypeRegistry::public()).unwrap();

        // Use PartialEq to compare entire records
        for (original, deserialized) in original_records.iter().zip(deserialized.iter()) {
            assert_eq!(
                original, deserialized,
                "Record roundtrip failed for: {original:?}"
            );
        }

        // Now through parquet
        let mut buffer = Rc::new(Vec::new());
        {
            let cursor = std::io::Cursor::new(Rc::get_mut(&mut buffer).unwrap());

            let mut parquet_writer = ArrowWriter::try_new(
                cursor,
                arrow::datatypes::Schema::new(get_telemetry_arrow_schema()).into(),
                Some(
                    WriterProperties::builder()
                        .set_compression(Compression::SNAPPY)
                        .build(),
                ),
            )
            .expect("Failed to create Parquet writer");
            parquet_writer.write(&batch).expect("Failed to write batch");
            parquet_writer.close().expect("Failed to close writer");
        }

        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from_owner(
            Rc::into_inner(buffer).unwrap(),
        ))
        .unwrap()
        .build()
        .unwrap();

        deserialized.clear();
        for batch_result in parquet_reader {
            let records = deserialize_from_arrow(
                &batch_result.unwrap(),
                TelemetryEventTypeRegistry::public(),
            )
            .unwrap();
            deserialized.extend(records);
        }

        // Use PartialEq to compare entire records
        for (original, deserialized) in original_records.iter().zip(deserialized.iter()) {
            assert_eq!(
                original, deserialized,
                "Record roundtrip via parquet failed for: {original:?}"
            );
        }
    }

    #[test]
    fn test_schema_creation() {
        let serialisable_schema = get_serialisable_schema();
        let schema_with_timestamps = get_telemetry_arrow_schema();
        assert!(!serialisable_schema.is_empty());
        assert!(!schema_with_timestamps.is_empty());

        // Assert all expected top-level keys present (they are stable)
        [
            "record_type",
            "trace_id",
            "span_id",
            "event_id",
            "span_name",
            "parent_span_id",
            "links",
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

        // Test attributes struct schema has all keys from ArrowAttributes
        let attributes_field = serialisable_schema
            .iter()
            .find(|f| f.name() == "attributes")
            .expect("Missing attributes field");
        let DataType::Struct(attribute_fields) = attributes_field.data_type() else {
            panic!("Attributes field should be a Struct");
        };
        let attribute_field_names: HashSet<&str> =
            attribute_fields.iter().map(|f| f.name().as_str()).collect();

        let fake_attrs = serde_json::to_value(ArrowAttributes::default())
            .expect("Failed to serialize ArrowAttributes");
        let expected_field_names = fake_attrs
            .as_object()
            .expect("ArrowAttributes should serialize to a JSON object")
            .keys()
            .map(|s| s.as_str())
            .collect::<HashSet<&str>>();

        let missing_fields: Vec<&str> = expected_field_names
            .difference(&attribute_field_names)
            .copied()
            .collect();

        let extra_fields: Vec<&str> = attribute_field_names
            .difference(&expected_field_names)
            .copied()
            .collect();

        let mut err_msg = String::new();
        if !missing_fields.is_empty() {
            err_msg.push_str(&format!("Missing fields: {}.\n", missing_fields.join(", ")));
        }
        if !extra_fields.is_empty() {
            err_msg.push_str(&format!("Extra fields: {}.", extra_fields.join(", ")));
        }

        assert!(
            err_msg.is_empty(),
            "Attribute schema vs. struct fields mismatch: {err_msg}"
        );
    }
}
