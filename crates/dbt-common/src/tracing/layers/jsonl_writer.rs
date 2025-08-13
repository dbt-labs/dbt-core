use std::sync::{Arc, RwLock};

use dbt_telemetry::{SpanEndInfo, SpanStartInfo, TelemetryRecordRef};
use tracing::{Subscriber, span};
use tracing_subscriber::{Layer, layer::Context};

use super::super::{event_info::with_current_thread_event_data, file_writer::TelemetryFileWriter};

/// A tracing layer that reads telemetry data from extensions and writes it as JSON.
///
/// This layer reads TelemetryRecord data from span extensions and serializes
/// it to JSON using the provided writer.
pub struct TelemetryWriterLayer {
    writer: Arc<RwLock<TelemetryFileWriter>>,
}

impl TelemetryWriterLayer {
    pub fn new(writer: Arc<RwLock<TelemetryFileWriter>>) -> Self {
        Self { writer }
    }
}

impl<S> Layer<S> for TelemetryWriterLayer
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_new_span(&self, _attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let span = ctx
            .span(id)
            .expect("Span must exist for id in the current context");

        // Get the TelemetryRecord from extensions. It must be there unless we messed
        // up data layer / layer order.
        if let Some(record) = span.extensions().get::<SpanStartInfo>() {
            if let Ok(json) = serde_json::to_string(&TelemetryRecordRef::SpanStart(record)) {
                if let Ok(w) = self.writer.try_read() {
                    w.write(json)
                }
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
            if let Ok(json) = serde_json::to_string(&TelemetryRecordRef::SpanEnd(record)) {
                if let Ok(w) = self.writer.try_read() {
                    w.write(json)
                }
            }
        } else {
            unreachable!("Unexpectedly missing span end data!");
        }
    }

    fn on_event(&self, _event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        with_current_thread_event_data(|log_record| {
            if let Ok(json) = serde_json::to_string(&TelemetryRecordRef::LogRecord(log_record)) {
                if let Ok(w) = self.writer.try_read() {
                    w.write(json)
                }
            }
        });
    }
}
