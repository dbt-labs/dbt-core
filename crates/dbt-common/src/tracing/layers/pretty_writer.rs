use dbt_telemetry::{SpanEndInfo, SpanStartInfo, TelemetryOutputFlags, TelemetryRecordRef};
use tracing::{Event, Subscriber, span};
use tracing_subscriber::{Layer, layer::Context, registry::LookupSpan};

use super::super::{event_info::with_current_thread_log_record, shared_writer::SharedWriter};

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
    is_tty: bool,
    filter_flag: TelemetryOutputFlags,
}

impl TelemetryPrettyWriterLayer {
    pub fn new<W, F>(writer: W, formatter: F) -> Self
    where
        W: SharedWriter + 'static,
        F: Fn(TelemetryRecordRef, bool) -> Option<String> + Send + Sync + 'static,
    {
        let is_tty = writer.is_terminal();

        Self {
            writer: Box::new(writer),
            formatter: Box::new(formatter),
            is_tty,
            filter_flag: if is_tty {
                TelemetryOutputFlags::OUTPUT_CONSOLE
            } else {
                TelemetryOutputFlags::OUTPUT_LOG_FILE
            },
        }
    }
}

impl<S> Layer<S> for TelemetryPrettyWriterLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, _attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let span = ctx
            .span(id)
            .expect("Span must exist for id in the current context");

        // Get the TelemetryRecord from extensions. It must be there unless we messed
        // up data layer / layer order.
        if let Some(record) = span.extensions().get::<SpanStartInfo>() {
            // Honor export flags: only write if appropriate output is enabled
            if !record.attributes.output_flags().contains(self.filter_flag) {
                return;
            }

            if let Some(line) = (self.formatter)(TelemetryRecordRef::SpanStart(record), self.is_tty)
            {
                // Currently we silently ignore write errors. We expect writers to be
                // smart enough to avoid trying to write after fatal errors and report
                // them during shutdown.
                let _ = self.writer.writeln(&line);
            }
        } else {
            unreachable!("Unexpectedly missing span start data!");
        }
    }

    fn on_close(&self, id: span::Id, ctx: Context<'_, S>) {
        let span = ctx
            .span(&id)
            .expect("Span must exist for id in the current context");

        // Get the TelemetryRecord from extensions. It must be there unless we messed
        // up data layer / layer order.
        if let Some(record) = span.extensions().get::<SpanEndInfo>() {
            // Honor export flags: only write if appropriate output is enabled
            if !record.attributes.output_flags().contains(self.filter_flag) {
                return;
            }

            if let Some(line) = (self.formatter)(TelemetryRecordRef::SpanEnd(record), self.is_tty) {
                // Currently we silently ignore write errors. We expect writers to be
                // smart enough to avoid trying to write after fatal errors and report
                // them during shutdown.
                let _ = self.writer.writeln(&line);
            }
        } else {
            unreachable!("Unexpectedly missing span end data!");
        }
    }

    fn on_event(&self, _event: &Event<'_>, _ctx: Context<'_, S>) {
        with_current_thread_log_record(|log_record| {
            // Honor export flags: only write if appropriate output is enabled
            if !log_record
                .attributes
                .output_flags()
                .contains(self.filter_flag)
            {
                return;
            }

            if let Some(line) =
                (self.formatter)(TelemetryRecordRef::LogRecord(log_record), self.is_tty)
            {
                // Currently we silently ignore write errors. We expect writers to be
                // smart enough to avoid trying to write after fatal errors and report
                // them during shutdown.
                let _ = self.writer.writeln(&line);
            }
        });
    }
}
