use crate::{
    LogRecordInfo, SpanEndInfo, SpanStartInfo, TelemetryOutputFlags, TelemetryRecordRef,
    data_provider::DataProvider,
    layer::TelemetryConsumer,
    shared_writer::{SharedWriter, resolve_is_terminal, resolve_use_color},
};

pub type TelemetryRecordPrettyFormatter =
    Box<dyn Fn(TelemetryRecordRef, bool) -> Option<String> + Send + Sync>;

/// A tracing layer that renders telemetry events in a human-readable format.
///
/// The layer respects [`TelemetryOutputFlags`] to decide whether a record should be written and
/// relies on [`TelemetryRecordPrettyFormatter`] for event-specific formatting.
/// It is intended for simple console or log-file style sinks.
pub struct TelemetryPrettyWriterLayer {
    writer: Box<dyn SharedWriter>,
    formatter: TelemetryRecordPrettyFormatter,
    use_color: bool,
    filter_flag: TelemetryOutputFlags,
}

impl TelemetryPrettyWriterLayer {
    pub fn new<W, F>(writer: W, formatter: F) -> Self
    where
        W: SharedWriter + 'static,
        F: Fn(TelemetryRecordRef, bool) -> Option<String> + Send + Sync + 'static,
    {
        // Routing is based on terminal-ness alone (respecting FORCE_COLOR
        // but not NO_COLOR, so setting NO_COLOR on a real TTY doesn't drop
        // console-only records); styling additionally honors NO_COLOR.
        let is_tty = resolve_is_terminal(&writer);
        let use_color = resolve_use_color(&writer);

        Self {
            writer: Box::new(writer),
            formatter: Box::new(formatter),
            use_color,
            filter_flag: if is_tty {
                TelemetryOutputFlags::OUTPUT_CONSOLE
            } else {
                TelemetryOutputFlags::OUTPUT_LOG_FILE
            },
        }
    }
}

impl TelemetryConsumer for TelemetryPrettyWriterLayer {
    fn is_span_enabled(&self, span: &SpanStartInfo) -> bool {
        span.attributes.output_flags().contains(self.filter_flag)
    }

    fn is_log_enabled(&self, log_record: &LogRecordInfo) -> bool {
        log_record
            .attributes
            .output_flags()
            .contains(self.filter_flag)
    }

    fn on_span_start(&self, span: &SpanStartInfo, _: &mut DataProvider<'_>) {
        if let Some(line) = (self.formatter)(TelemetryRecordRef::SpanStart(span), self.use_color) {
            self.writer.writeln(&line);
        }
    }

    fn on_span_end(&self, span: &SpanEndInfo, _: &mut DataProvider<'_>) {
        if let Some(line) = (self.formatter)(TelemetryRecordRef::SpanEnd(span), self.use_color) {
            self.writer.writeln(&line);
        }
    }

    fn on_log_record(&self, record: &LogRecordInfo, _: &mut DataProvider<'_>) {
        if let Some(line) = (self.formatter)(TelemetryRecordRef::LogRecord(record), self.use_color)
        {
            self.writer.writeln(&line);
        }
    }
}
