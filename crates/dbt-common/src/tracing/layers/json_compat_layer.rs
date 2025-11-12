use std::collections::HashMap;

use chrono::{DateTime, Utc};
use dbt_error::ErrorCode;
use dbt_telemetry::{
    Invocation, ListItemOutput, LogMessage, LogRecordInfo, QueryExecuted, SeverityNumber,
    SpanEndInfo, UserLogMessage,
};
use serde_json::json;
use tracing::level_filters::LevelFilter;

use super::super::{
    background_writer::BackgroundWriter,
    data_provider::DataProvider,
    formatters::{
        invocation::format_invocation_summary, log_message::format_log_message,
        query_log::format_query_log,
    },
    layer::{ConsumerLayer, TelemetryConsumer},
    metrics::{InvocationMetricKey, MetricKey},
    shared_writer::SharedWriter,
    shutdown::TelemetryShutdownItem,
};

use proto_rust::v1::public::fields::core_types::CoreEventInfo;

/// Build a JSON compatibility layer. This will write directly to the writer.
/// If you want to write to slow IO sink, prefer `build_json_compat_layer_with_background_writer`
pub fn build_json_compat_layer<W: SharedWriter + 'static>(
    writer: W,
    max_log_verbosity: LevelFilter,
    invocation_id: uuid::Uuid,
) -> ConsumerLayer {
    Box::new(JsonCompatLayer::new(writer, invocation_id).with_filter(max_log_verbosity))
}

/// Build a JSON compatibility layer with a background writer. This is preferred for writing to
/// slow IO sinks like files.
pub fn build_json_compat_layer_with_background_writer<W: std::io::Write + Send + 'static>(
    writer: W,
    max_log_verbosity: LevelFilter,
    invocation_id: uuid::Uuid,
) -> (ConsumerLayer, TelemetryShutdownItem) {
    let (writer, handle) = BackgroundWriter::new(writer);

    (
        build_json_compat_layer(writer, max_log_verbosity, invocation_id),
        Box::new(handle),
    )
}

/// A telemetry consumer that writes out events in a format compatible with dbt-core structured json logs.
///
/// It is only writing a small subset of event types and not 100% matching dbt-core schema and left only
/// for backward compatibility with what has been implemented in fusion by May 2025 launch.
struct JsonCompatLayer {
    writer: Box<dyn SharedWriter>,
    invocation_id: uuid::Uuid,
}

impl JsonCompatLayer {
    pub fn new<W: SharedWriter + 'static>(writer: W, invocation_id: uuid::Uuid) -> Self {
        Self {
            writer: Box::new(writer),
            invocation_id,
        }
    }

    fn build_core_event_info(
        &self,
        code: Option<&str>,
        name: Option<&str>,
        level: &str,
        msg: String,
    ) -> CoreEventInfo {
        CoreEventInfo {
            category: "".to_string(),
            code: code.unwrap_or("").to_string(),
            invocation_id: self.invocation_id.to_string(),
            name: name.unwrap_or("Generic").to_string(),
            pid: std::process::id() as i32,
            thread: std::thread::current().name().unwrap_or("main").to_string(),
            // drop the timezone offset and format as microseconds to conform to python logging timestamp parsing
            ts: Some(Utc::now().into()),
            msg,
            level: level.to_lowercase(),
            extra: HashMap::new(),
        }
    }
}

impl TelemetryConsumer for JsonCompatLayer {
    fn on_span_end(&self, span: &SpanEndInfo, data_provider: &mut DataProvider<'_>) {
        // Handle query log events
        if let Some(query_data) = span.attributes.downcast_ref::<QueryExecuted>() {
            // Format the query log using the shared formatter
            let formatted_query = format_query_log(
                query_data,
                span.start_time_unix_nano,
                span.end_time_unix_nano,
            );

            let info_json = serde_json::to_value(self.build_core_event_info(
                query_data.dbt_core_event_code.as_str().into(),
                Some("SQLQuery"),
                &span.severity_text,
                formatted_query,
            ))
            .expect("Failed to serialize core event info to JSON");

            let value = if let Some(unique_id) = query_data.unique_id.as_deref() {
                json!({
                    "info": info_json,
                    "data": {
                        "log_version": 3,
                        "version": env!("CARGO_PKG_VERSION"),
                        "sql": query_data.sql,
                        "node_info": {
                            "unique_id": unique_id
                        }
                    }
                })
            } else {
                json!({
                    "info": info_json,
                    "data": {
                        "log_version": 3,
                        "version": env!("CARGO_PKG_VERSION"),
                        "sql": query_data.sql,
                    }
                })
            };

            self.writer.writeln(value.to_string().as_str());
            return;
        }

        let Some(invocation) = span.attributes.downcast_ref::<Invocation>() else {
            return;
        };

        let formatted = format_invocation_summary(span, invocation, data_provider, false, None);

        if let Some(line) = formatted.autofix_line() {
            let info_json = serde_json::to_value(self.build_core_event_info(
                None,
                None,
                &span.severity_text,
                line.to_string(),
            ))
            .expect("Failed to serialize core event info to JSON");

            let value = json!({
                "info": info_json,
                "data": {
                    "log_version": 3,
                    "version": env!("CARGO_PKG_VERSION"),
                }
            })
            .to_string();

            self.writer.writeln(value.as_str());
        }

        let message = formatted
            .summary_lines()
            .map(|summary_lines| summary_lines.join("\n"))
            .unwrap_or_default();

        let elapsed_secs = span
            .end_time_unix_nano
            .duration_since(span.start_time_unix_nano)
            .unwrap_or_default()
            .as_secs_f32();

        let completed_at: DateTime<Utc> = span.end_time_unix_nano.into();
        let completed_at_str = completed_at.to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
        let success = data_provider.get_metric(MetricKey::InvocationMetric(
            InvocationMetricKey::TotalErrors,
        )) == 0;

        let info_json = serde_json::to_value(self.build_core_event_info(
            Some("Q039"),
            Some("CommandCompleted"),
            &span.severity_text,
            message,
        ))
        .expect("Failed to serialize core event info to JSON");

        // We only use structured type for info, because proto defined data
        // types are not matching what fusion emitted for compatibility by the
        // original launch date.
        let value = json!({
            "info": info_json,
            "data": {
                "log_version": 3,
                "version": env!("CARGO_PKG_VERSION"),
                "completed_at": completed_at_str,
                "elapsed": elapsed_secs,
                "success": success
            }
        })
        .to_string();

        self.writer.writeln(value.as_str());
    }

    fn on_log_record(&self, log_record: &LogRecordInfo, _data_provider: &mut DataProvider<'_>) {
        // Check if this is a LogMessage (error/warning)
        if let Some(log_msg) = log_record.attributes.downcast_ref::<LogMessage>() {
            // Format the message
            let formatted_message = format_log_message(
                log_msg
                    .code
                    .and_then(|c| u16::try_from(c).ok())
                    .and_then(|c| ErrorCode::try_from(c).ok()),
                // Unfortunately, we do not currently enforce log body to not contain ANSI codes,
                // so we need to make sure to strip them
                console::strip_ansi_codes(log_record.body.as_str()),
                log_record.severity_number,
                false,
                true,
            );

            let info_json = serde_json::to_value(self.build_core_event_info(
                None,
                None,
                &log_record.severity_text,
                formatted_message,
            ))
            .expect("Failed to serialize core event info to JSON");

            let value = json!({
                "info": info_json,
                "data": {
                    "log_version": 3,
                    "version": env!("CARGO_PKG_VERSION"),
                }
            })
            .to_string();

            self.writer.writeln(value.as_str());

            return;
        }

        // Handle UserLogMessage events (from Jinja print() and log() functions)
        // These map to dbt-core's JinjaLogInfo (I062), JinjaDebugInfo (I063) and PrintEvent ("Z052") events
        if let Some(user_log_msg) = log_record.attributes.downcast_ref::<UserLogMessage>() {
            let (event_name, event_code) = match (user_log_msg.is_print, log_record.severity_number)
            {
                (true, _) => {
                    // print() maps to PrintEvent (Z052)
                    ("PrintEvent", "Z052")
                }
                (false, SeverityNumber::Info) => {
                    // log(..., info=true) maps to JinjaLogInfo (I062)
                    ("JinjaLogInfo", "I062")
                }
                (false, SeverityNumber::Debug) => {
                    // log(..., info=false) maps to JinjaDebugInfo (I063)
                    ("JinjaDebugInfo", "I063")
                }
                _ => {
                    // Unknown combination, skip
                    return;
                }
            };

            let info_json = serde_json::to_value(self.build_core_event_info(
                Some(event_code),
                Some(event_name),
                &log_record.severity_text,
                log_record.body.clone(),
            ))
            .expect("Failed to serialize core event info to JSON");

            let value = json!({
                "info": info_json,
                "data": {
                    "msg": log_record.body, // Yes, duplicate the message here as well
                }
            })
            .to_string();

            self.writer.writeln(value.as_str());
            return;
        }

        // Handle ListItemOutput events (from dbt list command)
        // These map to PrintEvent (Z052) for backward compatibility
        if let Some(list_item) = log_record.attributes.downcast_ref::<ListItemOutput>() {
            let info_json = serde_json::to_value(self.build_core_event_info(
                Some("Z052"),
                Some("PrintEvent"),
                &log_record.severity_text,
                list_item.content.clone(),
            ))
            .expect("Failed to serialize core event info to JSON");

            let value = json!({
                "info": info_json,
                "data": {
                    "msg": &list_item.content, // Use content from event for backward compatibility
                }
            })
            .to_string();

            self.writer.writeln(value.as_str());
        }
    }
}
