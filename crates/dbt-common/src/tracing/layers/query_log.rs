use std::io::Write;

use dbt_telemetry::{QueryExecuted, SpanStartInfo};

use super::{
    super::{
        background_writer::BackgroundWriter,
        filter::{TelemetryFilterFn, disable_all_logs},
        formatters::query_log::format_query_log_event,
        layer::{ConsumerLayer, TelemetryConsumer},
        shutdown::TelemetryShutdownItem,
    },
    pretty_writer::TelemetryPrettyWriterLayer,
};

/// Build a query log writing layer with a background writer. Do not wrap
/// or buffer the writer, as the layer already does its own buffering
/// and operates on a non-blocking worker thread.
pub fn build_query_log_layer_with_background_writer<W: Write + Send + 'static>(
    writer: W,
) -> (ConsumerLayer, TelemetryShutdownItem) {
    let (writer, handle) = BackgroundWriter::new(writer);

    let layer = TelemetryPrettyWriterLayer::new(writer, format_query_log_event).with_filter(
        TelemetryFilterFn::new(
            |span: &SpanStartInfo, _| span.attributes.is::<QueryExecuted>(),
            disable_all_logs,
        ),
    );

    (Box::new(layer), Box::new(handle))
}
