use dbt_telemetry::{Invocation, SpanEndInfo};
use tracing::level_filters::LevelFilter;

use super::super::{
    background_writer::BackgroundWriter,
    layer::{ConsumerLayer, TelemetryConsumer},
    shared_writer::SharedWriter,
    shutdown::TelemetryShutdownItem,
};
use crate::tracing::{
    data_provider::DataProvider, formatters::invocation::format_invocation_summary,
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
    fn on_span_end(&self, span: &SpanEndInfo, data_provider: &DataProvider<'_>) {
        let Some(invocation) = span.attributes.downcast_ref::<Invocation>() else {
            return;
        };

        let formatted = format_invocation_summary(span, invocation, data_provider, false, None);

        // Per pre-migration logic, autofix line were always printed ignoring show options
        if let Some(line) = formatted.autofix_line() {
            let _ = self.writer.writeln(line);
        }

        if let Some(summary_lines) = formatted.summary_lines() {
            for line in summary_lines {
                let _ = self.writer.writeln(line.as_str());
            }
        }
    }
}
