//! Arrow serialization support for telemetry records using serde_arrow.

use super::to_nanos;
use crate::{
    ArtifactType, ExecutionPhase, LogRecordInfo, NodeCancelReason, NodeErrorType,
    NodeMaterialization, NodeOutcome, NodeSkipReason, NodeType, SeverityNumber, SpanEndInfo,
    SpanLinkInfo, SpanStartInfo, SpanStatus, StatusCode, TelemetryAttributes,
    TelemetryEventTypeRegistry, TelemetryExportFlags, TelemetryRecord, TelemetryRecordType,
};
use arrow::{
    array::Array,
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
    pub span_name: Option<&'a str>,
    pub parent_span_id: Option<u64>,
    pub links: Option<Vec<ArrowSpanLink>>,
    pub start_time_unix_nano: Option<u64>,
    pub end_time_unix_nano: Option<u64>,
    pub time_unix_nano: Option<u64>,
    pub severity_number: i32,
    pub severity_text: &'a str,
    pub body: Option<&'a str>,
    pub status_code: Option<u32>,
    pub status_message: Option<&'a str>,
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
    pub name: Option<&'a str>,
    pub database: Option<&'a str>,
    pub schema: Option<&'a str>,
    pub identifier: Option<&'a str>,
    pub dbt_core_event_code: Option<&'a str>,
    // Well-known phase fields
    pub phase: Option<ExecutionPhase>,
    // Well-known node fields
    pub unique_id: Option<&'a str>,
    pub materialization: Option<NodeMaterialization>,
    pub custom_materialization: Option<&'a str>,
    pub node_type: Option<NodeType>,
    pub node_outcome: Option<NodeOutcome>,
    pub node_error_type: Option<NodeErrorType>,
    pub node_cancel_reason: Option<NodeCancelReason>,
    pub node_skip_reason: Option<NodeSkipReason>,
    // CallTrace/Unknown fields
    pub dev_name: Option<&'a str>,
    // Code location fields
    pub file: Option<&'a str>,
    pub line: Option<u32>,
    // Log fields
    pub code: Option<u32>,
    pub original_severity_number: Option<i32>,
    pub original_severity_text: Option<&'a str>,
    // Artifact paths
    pub relative_path: Option<&'a str>,
    pub artifact_type: Option<ArtifactType>,
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
                span_name: Some(&span.span_name),
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
                severity_text: span.severity_text.as_ref(),
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
                span_name: Some(&span.span_name),
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
                severity_text: span.severity_text.as_ref(),
                body: None,
                status_code: span.status.as_ref().map(|s| s.code as u32),
                status_message: span.status.as_ref().and_then(|s| s.message.as_deref()),
                event_type: event_type.into(),
                attributes,
            },
            TelemetryRecord::LogRecord(log) => ArrowTelemetryRecord {
                record_type: value.into(),
                trace_id: format!("{:032x}", log.trace_id),
                span_id: log.span_id,
                event_id: Some(log.event_id.to_string().into()),
                span_name: log.span_name.as_deref(),
                parent_span_id: None,
                links: None,
                start_time_unix_nano: None,
                end_time_unix_nano: None,
                time_unix_nano: Some(to_nanos(&log.time_unix_nano)),
                severity_number: log.severity_number as i32,
                severity_text: log.severity_text.as_ref(),
                body: Some(log.body.as_ref()),
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
                .ok_or("Missing span_name for SpanStart record")?;
            let start_time_unix_nano = arrow
                .start_time_unix_nano
                .ok_or("Missing start_time_unix_nano for SpanStart record")?;

            Ok(TelemetryRecord::SpanStart(SpanStartInfo {
                trace_id,
                span_id,
                parent_span_id: arrow.parent_span_id,
                links,
                span_name: span_name.to_string(),
                start_time_unix_nano: nanos_to_system_time(start_time_unix_nano),
                attributes,
                severity_number: SeverityNumber::try_from(arrow.severity_number)
                    .map_err(|_| "Invalid severity_number")?,
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
                    message: arrow.status_message.map(str::to_string),
                })
            } else {
                None
            };

            Ok(TelemetryRecord::SpanEnd(SpanEndInfo {
                trace_id,
                span_id,
                parent_span_id: arrow.parent_span_id,
                links,
                span_name: span_name.to_string(),
                start_time_unix_nano: nanos_to_system_time(start_time_unix_nano),
                end_time_unix_nano: nanos_to_system_time(end_time_unix_nano),
                attributes,
                status,
                severity_number: SeverityNumber::try_from(arrow.severity_number)
                    .map_err(|_| "Invalid severity_number")?,
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
                trace_id,
                span_id: arrow.span_id,
                event_id: uuid::Uuid::from_str(arrow.event_id.ok_or("Missing event_id")?.as_ref())
                    .map_err(|e| format!("Failed to deserialize `event_id` from JSON: {e}"))?,
                span_name: arrow.span_name.map(str::to_string),
                severity_number: SeverityNumber::try_from(arrow.severity_number)
                    .map_err(|_| "Invalid severity_number")?,
                severity_text: arrow.severity_text.to_string(),
                body: body.to_string(),
                attributes,
            }))
        }
    }
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
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::UInt64, false),
        // Use JSON extension for json_payload
        Field::new("json_payload", DataType::Utf8, false)
            .with_extension_type(JsonExtensionType::default()),
    ]);

    // ArrowAttributes struct fields
    let attributes_fields = Fields::from(vec![
        // JSON blob for non well-known attributes
        Field::new("json_payload", DataType::Utf8, true)
            .with_extension_type(JsonExtensionType::default()),
        // Well-known common fields
        Field::new("name", DataType::Utf8, true),
        Field::new("database", DataType::Utf8, true),
        Field::new("schema", DataType::Utf8, true),
        Field::new("identifier", DataType::Utf8, true),
        Field::new("dbt_core_event_code", DataType::Utf8, true),
        // Phase
        Field::new("phase", DataType::Utf8, true),
        // Node fields
        Field::new("unique_id", DataType::Utf8, true),
        Field::new("materialization", DataType::Utf8, true),
        Field::new("custom_materialization", DataType::Utf8, true),
        Field::new("node_type", DataType::Utf8, true),
        Field::new("node_outcome", DataType::Utf8, true),
        Field::new("node_error_type", DataType::Utf8, true),
        Field::new("node_cancel_reason", DataType::Utf8, true),
        Field::new("node_skip_reason", DataType::Utf8, true),
        // CallTrace/Unknown fields
        Field::new("dev_name", DataType::Utf8, true),
        // Code location fields
        Field::new("file", DataType::Utf8, true),
        Field::new("line", DataType::UInt32, true),
        // Log fields
        Field::new("code", DataType::UInt32, true),
        Field::new("original_severity_number", DataType::Int32, true),
        Field::new("original_severity_text", DataType::Utf8, true),
        // Artifact paths
        Field::new("relative_path", DataType::Utf8, true),
        Field::new("artifact_type", DataType::Utf8, true),
    ]);

    // Top-level fields for ArrowTelemetryRecord
    let serialisable_schema: Vec<FieldRef> = vec![
        Arc::new(Field::new("record_type", DataType::Utf8, false)),
        Arc::new(Field::new("trace_id", DataType::Utf8, false)),
        Arc::new(Field::new("span_id", DataType::UInt64, true)),
        Arc::new(Field::new("event_id", DataType::Utf8, true)),
        Arc::new(Field::new("span_name", DataType::Utf8, true)),
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
        Arc::new(Field::new("severity_text", DataType::Utf8, false)),
        Arc::new(Field::new("body", DataType::Utf8, true)),
        Arc::new(Field::new("status_code", DataType::UInt32, true)),
        Arc::new(Field::new("status_message", DataType::Utf8, true)),
        Arc::new(Field::new("event_type", DataType::Utf8, false)),
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
                .export_flags()
                .contains(TelemetryExportFlags::EXPORT_PARQUET)
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
    let temp_batch = RecordBatch::try_new(Schema::new(get_serialisable_schema()).into(), columns)?;

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

#[cfg(test)]
mod tests {
    use fake::rand::SeedableRng;
    use fake::rand::rngs::StdRng;
    use fake::{Fake, Faker};

    use crate::TelemetryEventRecType;

    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

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
                .export_flags()
                .contains(TelemetryExportFlags::EXPORT_PARQUET)
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
            span_name: attributes.to_string(),
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
            span_name: Some(attributes.to_string()),
            severity_number: Faker.fake_with_rng(&mut rng),
            severity_text: ["ERROR", "WARN", "INFO", "DEBUG"][(hashed_seed % 4) as usize]
                .to_string(),
            body: format!("Log message {}", hashed_seed % 10000),
            attributes,
        })
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
        let deserialized =
            deserialize_from_arrow(&batch, TelemetryEventTypeRegistry::public()).unwrap();

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
    }
}
