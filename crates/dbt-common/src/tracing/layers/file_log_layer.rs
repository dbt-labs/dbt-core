use chrono::{DateTime, Utc};
use dbt_error::ErrorCode;
use dbt_telemetry::{
    Invocation, LogMessage, LogRecordInfo, NodeEvaluated, NodeOutcome, NodeOutcomeDetail, NodeType,
    QueryExecuted, SeverityNumber, SpanEndInfo, SpanStartInfo, UserLogMessage,
};
use std::time::SystemTime;
use tracing::level_filters::LevelFilter;

use super::super::{
    background_writer::BackgroundWriter,
    data_provider::DataProvider,
    formatters::{
        invocation::format_invocation_summary, log_message::format_log_message,
        test_result::format_test_failure,
    },
    layer::{ConsumerLayer, TelemetryConsumer},
    shared_writer::SharedWriter,
    shutdown::TelemetryShutdownItem,
};

/// Format a timestamp in ISO format with microseconds: HH:MM:SS.microseconds
fn format_timestamp(time: SystemTime) -> String {
    let datetime: DateTime<Utc> = time.into();
    datetime.format("%H:%M:%S%.6f").to_string()
}

/// Format a timestamp in ISO format with date: YYYY-MM-DD HH:MM:SS.microseconds
fn format_timestamp_with_date(time: SystemTime) -> String {
    let datetime: DateTime<Utc> = time.into();
    datetime.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
}

/// Format severity level as fixed-width bracketed string: [info ], [warn ], [error], etc.
fn format_severity(severity: SeverityNumber) -> String {
    let level_str = match severity {
        SeverityNumber::Error => "error",
        SeverityNumber::Warn => "warn ",
        SeverityNumber::Info => "info ",
        SeverityNumber::Debug => "debug",
        SeverityNumber::Trace => "trace",
        SeverityNumber::Unspecified => "info ",
    };
    format!("[{}]", level_str)
}

const HEADER_SEPARATOR: &str = "====================";

/// Build file log layer with a background writer. This is preferred for writing to
/// slow IO sinks like files.
pub fn build_file_log_layer_with_background_writer<W: std::io::Write + Send + 'static>(
    writer: W,
    max_log_verbosity: LevelFilter,
) -> (ConsumerLayer, TelemetryShutdownItem) {
    let (writer, handle) = BackgroundWriter::new(writer);

    (
        Box::new(FileLogLayer::new(writer).with_filter(max_log_verbosity)),
        Box::new(handle),
    )
}

pub struct FileLogLayer {
    writer: Box<dyn SharedWriter>,
}

impl FileLogLayer {
    pub fn new<W: SharedWriter + 'static>(writer: W) -> Self {
        Self {
            writer: Box::new(writer),
        }
    }

    /// Write log lines with timestamp and severity level prefix.
    /// Each string in the slice represents one distinct log line.
    fn write_log_lines(
        &self,
        timestamp: SystemTime,
        severity: SeverityNumber,
        lines: &[impl AsRef<str>],
    ) {
        let timestamp_str = format_timestamp(timestamp);
        let level_str = format_severity(severity);

        let mut line_iter = lines.iter();

        // First line with timestamp and level
        if let Some(first_line) = line_iter.next() {
            let formatted_line =
                format!("{} {}: {}", timestamp_str, level_str, first_line.as_ref());
            self.writer.writeln(&formatted_line);
        }

        // Subsequent lines are printed as is, without timestamp/level prefix
        for line in line_iter {
            self.writer.writeln(line.as_ref());
        }
    }
}

impl TelemetryConsumer for FileLogLayer {
    fn on_span_start(&self, span: &SpanStartInfo, _: &mut DataProvider<'_>) {
        // Write header line when invocation starts
        if let Some(invocation) = span.attributes.downcast_ref::<Invocation>() {
            let timestamp = format_timestamp_with_date(span.start_time_unix_nano);
            let invocation_id = &invocation.invocation_id;
            let header = format!(
                "{} {} | {} {}",
                HEADER_SEPARATOR, timestamp, invocation_id, HEADER_SEPARATOR
            );
            self.writer.writeln(&header);
        }
    }

    fn on_span_end(&self, span: &SpanEndInfo, data_provider: &mut DataProvider<'_>) {
        // Query log (it has a separate layer, a dedicated sql file, but also goes to dbt.log as of today)
        if let Some(query_data) = span.attributes.downcast_ref::<QueryExecuted>() {
            let node_id = query_data.unique_id.as_deref().unwrap_or("unknown");
            let formatted_query =
                format!("Query executed on node {}:\n{}", node_id, query_data.sql);
            self.write_log_lines(
                span.end_time_unix_nano,
                span.severity_number,
                &[formatted_query],
            );
            return;
        }

        // Print unit test summary messages
        if let Some(ne) = span.attributes.downcast_ref::<NodeEvaluated>()
            && (ne.node_type() == NodeType::Test || ne.node_type() == NodeType::UnitTest)
            && ne.node_outcome() == NodeOutcome::Success
            && let Some(NodeOutcomeDetail::NodeTestDetail(t_outcome)) = &ne.node_outcome_detail
            && let Some(diff_table) = t_outcome.diff_table.as_ref()
        {
            self.write_log_lines(
                span.end_time_unix_nano,
                span.severity_number,
                &[format_test_failure(&ne.name, diff_table, false)],
            );
        }

        // Invocation end
        let Some(invocation) = span.attributes.downcast_ref::<Invocation>() else {
            return;
        };

        let formatted = format_invocation_summary(span, invocation, data_provider, false, None);

        // Per pre-migration logic, autofix line were always printed ignoring show options
        if let Some(line) = formatted.autofix_line() {
            self.write_log_lines(span.end_time_unix_nano, span.severity_number, &[line]);
        }

        if let Some(summary_lines) = formatted.summary_lines() {
            self.write_log_lines(span.end_time_unix_nano, span.severity_number, summary_lines);
        }
    }

    fn on_log_record(&self, log_record: &LogRecordInfo, _: &mut DataProvider<'_>) {
        // Check if this is a LogMessage (error/warning)
        if let Some(log_msg) = log_record.attributes.downcast_ref::<LogMessage>() {
            // Format the message without level prefix (we add it via write_log_lines)
            let formatted_message = format_log_message(
                log_msg
                    .code
                    .and_then(|c| u16::try_from(c).ok())
                    .and_then(|c| ErrorCode::try_from(c).ok()),
                &log_record.body,
                log_record.severity_number,
                false,
                false, // Don't include level prefix, we add it via write_log_lines
            );

            self.write_log_lines(
                log_record.time_unix_nano,
                log_record.severity_number,
                &[formatted_message],
            );

            return;
        }

        // Handle UserLogMessage events (from Jinja print() and log() functions)
        if log_record.attributes.is::<UserLogMessage>() {
            // Write user log messages to file log
            self.write_log_lines(
                log_record.time_unix_nano,
                log_record.severity_number,
                std::slice::from_ref(&log_record.body),
            );
        }
    }
}
