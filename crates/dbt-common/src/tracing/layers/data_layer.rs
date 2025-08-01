use super::super::{
    convert::{current_time_nanos, tracing_level_to_severity},
    init::process_span,
    log_info::{get_log_event_attrs, get_log_message},
    span_info::{get_span_debug_extra_attrs, get_span_event_attrs},
};
use tracing_log::NormalizeEvent;

use std::sync::atomic::AtomicU64;

use tracing::{Level, Subscriber, span};
use tracing_subscriber::{Layer, layer::Context};

use dbt_telemetry::{
    LogAttributes, LogRecordInfo, RecordCodeLocation, SpanAttributes, SpanEndInfo, SpanStartInfo,
    SpanStatus,
};

/// A tracing layer that creates structured telemetry data and stores it in span extensions.
///
/// This layer captures span events and converts them to structured telemetry
/// records that include the trace ID for correlation across systems.
pub struct TelemetryDataLayer<S>
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    /// The trace ID for the current invocation
    trace_id: u128,
    /// A globally unique span ID generator. Unlike `tracing` span IDs, this
    /// generator ensures that span IDs are unique across the entire process
    next_span_id: AtomicU64,
    /// Whether to strip code location from span & log attributes.
    strip_code_location: bool,
    __phantom: std::marker::PhantomData<S>,
}

impl<S> TelemetryDataLayer<S>
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    pub(crate) fn new(trace_id: u128, strip_code_location: bool) -> Self {
        Self {
            trace_id,
            next_span_id: AtomicU64::new(1),
            strip_code_location,
            __phantom: std::marker::PhantomData,
        }
    }

    /// Returns a global unique span ID for the next span. We can't use the span ID from
    /// `tracing` directly because it is not guaranteed to be unique across even within a single
    /// process, especially in a multi-threaded environment.
    fn next_span_id(&self) -> u64 {
        self.next_span_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    fn get_location(&self, metadata: &tracing::Metadata<'_>) -> RecordCodeLocation {
        if self.strip_code_location {
            RecordCodeLocation::default()
        } else {
            // Extract code location from metadata
            RecordCodeLocation::from(metadata)
        }
    }
}

impl<S> Layer<S> for TelemetryDataLayer<S>
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let span = ctx
            .span(id)
            .expect("Span must exist for id in the current context");
        let metadata = span.metadata();

        let global_span_id = self.next_span_id();

        let global_parent_span_id = span
            .parent()
            .or_else(|| {
                // If no parent span is found, use process span as parent.
                // This will trigger for the process span itself,
                // but process span helper will just return None
                process_span(&ctx)
            })
            .and_then(|parent_span| {
                parent_span
                    .extensions()
                    .get::<SpanStartInfo>()
                    .map(|parent_span_record| parent_span_record.span_id)
            });

        let start_time = current_time_nanos();
        let (severity_number, severity_text) = tracing_level_to_severity(metadata.level());

        // Extract event attributes if any. To avoid leakage, we only extract internal metadata
        // such as location, name etc. in debug builds

        // TODO: auto-inject location if missing for attr types that have them. See log for example
        let attributes = get_span_event_attrs(attrs.values().into()).unwrap_or_else(|| {
            if metadata.level() == &Level::TRACE {
                // Trace spans without explicit attributes considered dev internal
                SpanAttributes::DevInternal {
                    name: metadata.name().to_string(),
                    location: self.get_location(metadata),
                    extra: get_span_debug_extra_attrs(attrs.values().into()),
                }
            } else {
                SpanAttributes::Unknown {
                    name: metadata.name().to_string(),
                    location: self.get_location(metadata),
                }
            }
        });

        let record = SpanStartInfo {
            trace_id: self.trace_id,
            span_id: global_span_id,
            parent_span_id: global_parent_span_id,
            name: attributes.to_string(),
            start_time_unix_nano: start_time,
            attributes: attributes.clone(),
            time_unix_nano: start_time,
            severity_number,
            severity_text,
        };

        // Store the record in span extensions
        span.extensions_mut().insert(record);

        // And store the attributes in the span extensions as well,
        // we use this to update them post creation and add to closing span record
        span.extensions_mut().insert(attributes);
    }

    fn on_close(&self, id: span::Id, ctx: Context<'_, S>) {
        let span = ctx
            .span(&id)
            .expect("Span must exist for id in the current context");
        let metadata = span.metadata();

        // Get the span_id and start_time from the stored SpanStart record
        let (span_id, start_time_unix_nano, parent_span_id, start_attributes) =
            if let Some(SpanStartInfo {
                span_id,
                start_time_unix_nano,
                parent_span_id,
                attributes,
                ..
            }) = span.extensions().get::<SpanStartInfo>()
            {
                (
                    *span_id,
                    *start_time_unix_nano,
                    *parent_span_id,
                    attributes.clone(),
                )
            } else {
                (
                    self.next_span_id(),
                    current_time_nanos(),
                    None,
                    SpanAttributes::Unknown {
                        name: metadata.name().to_string(),
                        location: self.get_location(metadata),
                    },
                ) // Fallback. Should not happen
            };

        let status = span.extensions().get::<SpanStatus>().cloned();

        let attributes = span
            .extensions()
            .get::<SpanAttributes>()
            .cloned()
            .unwrap_or({
                // If no attributes were recorded, use the start attributes
                start_attributes
            });

        let end_time = current_time_nanos();
        let (severity_number, severity_text) = tracing_level_to_severity(metadata.level());

        let record = SpanEndInfo {
            trace_id: self.trace_id,
            span_id,
            parent_span_id,
            name: attributes.to_string(),
            start_time_unix_nano,
            end_time_unix_nano: end_time,
            attributes,
            status,
            time_unix_nano: end_time,
            severity_number,
            severity_text,
        };

        // Store the record in span extensions
        span.extensions_mut().insert(record);
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        // Get the current span to extract span information
        let Some(current_span) = ctx.lookup_current().or_else(|| process_span(&ctx)) else {
            // If no current span is found, we can't log the event.
            // This may happen if tracing is not initialized (e.g. in tests)
            return;
        };

        // Extract needed data from span record and release the immutable borrow
        let (span_id, span_name) = {
            if let Some(SpanStartInfo { span_id, name, .. }) =
                current_span.extensions().get::<SpanStartInfo>()
            {
                (*span_id, name.clone())
            } else {
                unreachable!(
                    "SpanStartInfo should always be present in the current span extensions"
                )
            }
        }; // span_record is dropped here, releasing the immutable borrow

        let time_unix_nano = current_time_nanos();
        // TODO: calculate modified severity based on user config when such feature is implemented
        let (severity_number, severity_text) = tracing_level_to_severity(event.metadata().level());

        // Extract message from event
        let message = get_log_message(event);

        let attributes = if let Some(legacy_log_meta) = event.normalized_metadata() {
            // This means the event is coming from `tracing-log` bridge
            LogAttributes::LegacyLog {
                original_severity_number: severity_number.clone(),
                original_severity_text: severity_text.clone(),
                location: self.get_location(&legacy_log_meta),
            }
        } else {
            get_log_event_attrs(event.into())
                // Auto-inject location if missing
                .map(|attrs| {
                    if attrs.has_empty_location() {
                        attrs.with_location(self.get_location(event.metadata()))
                    } else {
                        attrs
                    }
                })
                .unwrap_or_else(|| LogAttributes::Log {
                    code: None,
                    dbt_core_code: None,
                    original_severity_number: severity_number.clone(),
                    original_severity_text: severity_text.clone(),
                    location: self.get_location(event.metadata()),
                })
        };

        let log_record = LogRecordInfo {
            time_unix_nano,
            trace_id: self.trace_id,
            span_id,
            span_name,
            severity_number,
            severity_text,
            body: message,
            attributes,
        };

        // Now safe to get mutable borrow since immutable borrow is released
        current_span.extensions_mut().replace(log_record);
    }
}
