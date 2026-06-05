use std::io::Write;

use dbt_telemetry::{QueryExecuted, SpanStartInfo, TelemetryRecordRef};

use super::{
    super::{
        background_writer::BackgroundWriter,
        filter::{TelemetryFilterFn, disable_all_logs},
        formatters::query_log::format_query_log,
        layer::{ConsumerLayer, TelemetryConsumer},
        shutdown::TelemetryShutdownItem,
    },
    pretty_writer::TelemetryPrettyWriterLayer,
};

/// Wrapper for query_log layer that handles TelemetryRecordRef
fn format_query_log_record(record: TelemetryRecordRef, _is_tty: bool) -> Option<String> {
    if let TelemetryRecordRef::SpanEnd(span_end) = record
        && let Some(query_data) = span_end.attributes.downcast_ref::<QueryExecuted>()
    {
        return Some(format_query_log(
            query_data,
            span_end.start_time_unix_nano,
            span_end.end_time_unix_nano,
        ));
    }

    None
}

/// Build a query log writing layer with a background writer. Do not wrap
/// or buffer the writer, as the layer already does its own buffering
/// and operates on a non-blocking worker thread.
pub fn build_query_log_layer_with_background_writer<W: Write + Send + 'static>(
    writer: W,
) -> (ConsumerLayer, TelemetryShutdownItem) {
    let (writer, handle) = BackgroundWriter::new(writer);

    let layer = TelemetryPrettyWriterLayer::new(writer, format_query_log_record).with_filter(
        TelemetryFilterFn::new(
            |span: &SpanStartInfo| span.attributes.is::<QueryExecuted>(),
            disable_all_logs,
        ),
    );

    (Box::new(layer), Box::new(handle))
}
