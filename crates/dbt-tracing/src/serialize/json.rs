//! Registry-backed JSON deserialization for telemetry records.

use std::{
    borrow::Cow,
    collections::BTreeMap,
    error::Error,
    io::{self, ErrorKind},
    time::SystemTime,
};

use serde::Deserialize;
use serde_json::Value as JsonValue;
use uuid::Uuid;

use crate::{
    LogRecordInfo, SeverityNumber, SpanEndInfo, SpanLinkInfo, SpanStartInfo, SpanStatus,
    StatusCode, TelemetryAttributes, TelemetryEventRecType, TelemetryRecord,
    serialize::{
        envelope::{
            deserialize_optional_span_id, deserialize_span_id, deserialize_timestamp,
            deserialize_trace_id,
        },
        traits::JsonRegistryLookup,
    },
};

type BoxError = Box<dyn Error>;

pub fn deserialize_from_json_str<R>(input: &str, registry: &R) -> Result<TelemetryRecord, BoxError>
where
    R: JsonRegistryLookup,
{
    let record: TelemetryRecordJson<'_> = serde_json::from_str(input).map_err(invalid_data)?;
    record.into_telemetry_record(registry)
}

pub fn deserialize_from_json_value<R>(
    value: JsonValue,
    registry: &R,
) -> Result<TelemetryRecord, BoxError>
where
    R: JsonRegistryLookup,
{
    let input = serde_json::to_string(&value).map_err(invalid_data)?;
    deserialize_from_json_str(&input, registry)
}

pub fn deserialize_json_lines<R>(
    input: &str,
    registry: &R,
) -> Result<Vec<TelemetryRecord>, BoxError>
where
    R: JsonRegistryLookup,
{
    input
        .lines()
        .enumerate()
        .map(|(idx, line)| {
            deserialize_from_json_str(line, registry).map_err(|err| {
                invalid_data(format!(
                    "failed to deserialize JSON line {}: {err}",
                    idx + 1
                ))
            })
        })
        .collect()
}

#[derive(Deserialize)]
#[serde(tag = "record_type")]
enum TelemetryRecordJson<'a> {
    SpanStart(#[serde(borrow)] SpanStartJson<'a>),
    SpanEnd(#[serde(borrow)] SpanEndJson<'a>),
    LogRecord(#[serde(borrow)] LogRecordJson<'a>),
}

impl TelemetryRecordJson<'_> {
    fn into_telemetry_record<R>(self, registry: &R) -> Result<TelemetryRecord, BoxError>
    where
        R: JsonRegistryLookup,
    {
        match self {
            Self::SpanStart(record) => record.into_telemetry_record(registry),
            Self::SpanEnd(record) => record.into_telemetry_record(registry),
            Self::LogRecord(record) => record.into_telemetry_record(registry),
        }
    }
}

#[derive(Deserialize)]
struct SpanStartJson<'a> {
    #[serde(deserialize_with = "deserialize_trace_id")]
    trace_id: u128,
    #[serde(deserialize_with = "deserialize_span_id")]
    span_id: u64,
    #[serde(borrow)]
    span_name: Cow<'a, str>,
    #[serde(default, deserialize_with = "deserialize_optional_span_id")]
    parent_span_id: Option<u64>,
    links: Option<Vec<SpanLinkJson>>,
    #[serde(deserialize_with = "deserialize_timestamp")]
    start_time_unix_nano: SystemTime,
    severity_number: SeverityNumber,
    #[serde(borrow)]
    severity_text: Cow<'a, str>,
    #[serde(borrow)]
    event_type: Cow<'a, str>,
    attributes: JsonValue,
}

impl SpanStartJson<'_> {
    fn into_telemetry_record<R>(self, registry: &R) -> Result<TelemetryRecord, BoxError>
    where
        R: JsonRegistryLookup,
    {
        Ok(TelemetryRecord::SpanStart(SpanStartInfo {
            trace_id: self.trace_id,
            span_id: self.span_id,
            span_name: self.span_name.into_owned(),
            parent_span_id: self.parent_span_id,
            links: self.links.map(span_links_from_json),
            start_time_unix_nano: self.start_time_unix_nano,
            severity_number: self.severity_number,
            severity_text: self.severity_text.into_owned(),
            attributes: deserialize_attributes(
                registry,
                self.event_type.as_ref(),
                self.attributes,
                TelemetryEventRecType::Span,
            )?,
        }))
    }
}

#[derive(Deserialize)]
struct SpanEndJson<'a> {
    #[serde(deserialize_with = "deserialize_trace_id")]
    trace_id: u128,
    #[serde(deserialize_with = "deserialize_span_id")]
    span_id: u64,
    #[serde(borrow)]
    span_name: Cow<'a, str>,
    #[serde(default, deserialize_with = "deserialize_optional_span_id")]
    parent_span_id: Option<u64>,
    links: Option<Vec<SpanLinkJson>>,
    #[serde(deserialize_with = "deserialize_timestamp")]
    start_time_unix_nano: SystemTime,
    #[serde(deserialize_with = "deserialize_timestamp")]
    end_time_unix_nano: SystemTime,
    severity_number: SeverityNumber,
    #[serde(borrow)]
    severity_text: Cow<'a, str>,
    #[serde(borrow)]
    status: Option<SpanStatusJson<'a>>,
    #[serde(borrow)]
    event_type: Cow<'a, str>,
    attributes: JsonValue,
}

impl SpanEndJson<'_> {
    fn into_telemetry_record<R>(self, registry: &R) -> Result<TelemetryRecord, BoxError>
    where
        R: JsonRegistryLookup,
    {
        Ok(TelemetryRecord::SpanEnd(SpanEndInfo {
            trace_id: self.trace_id,
            span_id: self.span_id,
            span_name: self.span_name.into_owned(),
            parent_span_id: self.parent_span_id,
            links: self.links.map(span_links_from_json),
            start_time_unix_nano: self.start_time_unix_nano,
            end_time_unix_nano: self.end_time_unix_nano,
            severity_number: self.severity_number,
            severity_text: self.severity_text.into_owned(),
            status: self.status.map(Into::into),
            attributes: deserialize_attributes(
                registry,
                self.event_type.as_ref(),
                self.attributes,
                TelemetryEventRecType::Span,
            )?,
        }))
    }
}

#[derive(Deserialize)]
struct LogRecordJson<'a> {
    #[serde(deserialize_with = "deserialize_trace_id")]
    trace_id: u128,
    #[serde(default, deserialize_with = "deserialize_optional_span_id")]
    span_id: Option<u64>,
    #[serde(borrow)]
    span_name: Option<Cow<'a, str>>,
    event_id: Uuid,
    #[serde(deserialize_with = "deserialize_timestamp")]
    time_unix_nano: SystemTime,
    severity_number: SeverityNumber,
    #[serde(borrow)]
    severity_text: Cow<'a, str>,
    #[serde(borrow)]
    body: Cow<'a, str>,
    #[serde(borrow)]
    event_type: Cow<'a, str>,
    attributes: JsonValue,
}

impl LogRecordJson<'_> {
    fn into_telemetry_record<R>(self, registry: &R) -> Result<TelemetryRecord, BoxError>
    where
        R: JsonRegistryLookup,
    {
        Ok(TelemetryRecord::LogRecord(LogRecordInfo {
            trace_id: self.trace_id,
            span_id: self.span_id,
            span_name: self.span_name.map(Cow::into_owned),
            event_id: self.event_id,
            time_unix_nano: self.time_unix_nano,
            severity_number: self.severity_number,
            severity_text: self.severity_text.into_owned(),
            body: self.body.into_owned(),
            attributes: deserialize_attributes(
                registry,
                self.event_type.as_ref(),
                self.attributes,
                TelemetryEventRecType::Log,
            )?,
        }))
    }
}

#[derive(Deserialize)]
struct SpanStatusJson<'a> {
    #[serde(borrow)]
    message: Option<Cow<'a, str>>,
    code: StatusCode,
}

impl From<SpanStatusJson<'_>> for SpanStatus {
    fn from(status: SpanStatusJson<'_>) -> Self {
        Self {
            message: status.message.map(Cow::into_owned),
            code: status.code,
        }
    }
}

#[derive(Deserialize)]
struct SpanLinkJson {
    #[serde(deserialize_with = "deserialize_trace_id")]
    trace_id: u128,
    #[serde(deserialize_with = "deserialize_span_id")]
    span_id: u64,
    attributes: BTreeMap<String, JsonValue>,
}

fn span_links_from_json(links: Vec<SpanLinkJson>) -> Vec<SpanLinkInfo> {
    links
        .into_iter()
        .map(|link| SpanLinkInfo {
            trace_id: link.trace_id,
            span_id: link.span_id,
            attributes: link.attributes,
        })
        .collect()
}

fn deserialize_attributes<R>(
    registry: &R,
    event_type: &str,
    attributes: JsonValue,
    expected_category: TelemetryEventRecType,
) -> Result<TelemetryAttributes, BoxError>
where
    R: JsonRegistryLookup,
{
    let event = registry
        .deserialize_json_attributes(event_type, attributes)
        .map_err(invalid_data)?;
    let actual_category = event.record_category();

    if actual_category != expected_category {
        return Err(invalid_data(format!(
            "event type \"{event_type}\" has category {actual_category:?}, expected {expected_category:?}"
        )));
    }

    Ok(TelemetryAttributes::new(event))
}

fn invalid_data(error: impl ToString) -> BoxError {
    Box::new(io::Error::new(ErrorKind::InvalidData, error.to_string()))
}
