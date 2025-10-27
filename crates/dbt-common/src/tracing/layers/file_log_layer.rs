use dbt_telemetry::{
    Invocation, LogMessage, LogRecordInfo, NodeEvaluated, NodeOutcome, NodeOutcomeDetail, NodeType,
    SpanEndInfo,
};
use tracing::level_filters::LevelFilter;

use super::super::{
    background_writer::BackgroundWriter,
    data_provider::DataProvider,
    formatters::{invocation::format_invocation_summary, log_message::format_log_message},
    layer::{ConsumerLayer, TelemetryConsumer},
    shared_writer::SharedWriter,
    shutdown::TelemetryShutdownItem,
};

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
}

impl TelemetryConsumer for FileLogLayer {
    fn on_span_end(&self, span: &SpanEndInfo, data_provider: &mut DataProvider<'_>) {
        // Print unit test summary messages
        if let Some(ne) = span.attributes.downcast_ref::<NodeEvaluated>()
            && (ne.node_type() == NodeType::Test || ne.node_type() == NodeType::UnitTest)
            && ne.node_outcome() == NodeOutcome::Success
            && let Some(NodeOutcomeDetail::NodeTestDetail(t_outcome)) = &ne.node_outcome_detail
            && let Some(diff_table) = t_outcome.diff_table.as_ref()
        {
            self.writer
                .write(format!("\nFAIL {}\n{diff_table}\n", ne.name).as_str());
        }

        // Invocation end
        let Some(invocation) = span.attributes.downcast_ref::<Invocation>() else {
            return;
        };

        let formatted = format_invocation_summary(span, invocation, data_provider, false, None);

        // Per pre-migration logic, autofix line were always printed ignoring show options
        if let Some(line) = formatted.autofix_line() {
            self.writer.writeln(line);
        }

        if let Some(summary_lines) = formatted.summary_lines() {
            for line in summary_lines {
                self.writer.writeln(line.as_str());
            }
        }
    }

    fn on_log_record(&self, log_record: &LogRecordInfo, _: &mut DataProvider<'_>) {
        // Check if this is a LogMessage (error/warning)
        if let Some(log_msg) = log_record.attributes.downcast_ref::<LogMessage>() {
            // Format the message
            let formatted_message =
                format_log_message(log_msg, &log_record.body, log_record.severity_number, false);

            self.writer.writeln(&formatted_message);
        }
    }
}
