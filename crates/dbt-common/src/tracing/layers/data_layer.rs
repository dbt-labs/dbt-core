use super::super::{
    convert::tracing_level_to_severity,
    event_info::{get_log_event_attrs, get_log_message, store_event_data, take_event_attributes},
    init::process_span,
    span_info::{get_span_debug_extra_attrs, get_span_event_attrs},
};
use tracing_log::NormalizeEvent;

use std::{sync::atomic::AtomicU64, time::SystemTime};

use tracing::{Level, Subscriber, span};
use tracing_subscriber::{Layer, layer::Context};

use dbt_telemetry::{
    LogRecordInfo, RecordCodeLocation, SpanEndInfo, SpanStartInfo, SpanStatus, TelemetryAttributes,
};

/// A tracing layer that creates structured telemetry data and stores it in span extensions.
///
/// This layer captures span events and converts them to structured telemetry
/// records that include the trace ID for correlation across systems.
pub(in crate::tracing) struct TelemetryDataLayer<S>
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

        let start_time = SystemTime::now();
        let (severity_number, severity_text) = tracing_level_to_severity(metadata.level());

        // Extract event attributes if any. To avoid leakage, we extract internal metadata
        // such as location, name etc. only in debug builds

        // TODO: auto-inject location if missing for attr types that have them. See log for example
        let attributes = get_span_event_attrs(attrs.values().into()).unwrap_or_else(|| {
            if metadata.level() == &Level::TRACE {
                // Trace spans without explicit attributes considered dev internal
                TelemetryAttributes::DevInternal {
                    name: metadata.name().to_string(),
                    location: self.get_location(metadata),
                    extra: get_span_debug_extra_attrs(attrs.values().into()),
                }
            } else {
                TelemetryAttributes::Unknown {
                    name: metadata.name().to_string(),
                    location: self.get_location(metadata),
                }
            }
        });

        let record = SpanStartInfo {
            trace_id: self.trace_id,
            span_id: global_span_id,
            span_name: attributes.to_string(),
            parent_span_id: global_parent_span_id,
            start_time_unix_nano: start_time,
            severity_number,
            severity_text: severity_text.to_string(),
            attributes: attributes.clone(),
        };

        let mut ext_mut = span.extensions_mut();

        // Store the record in span extensions
        ext_mut.insert(record);

        // And store the attributes in the span extensions as well,
        // we use this to update them post creation and add to closing span record
        ext_mut.insert(attributes);
    }

    fn on_close(&self, id: span::Id, ctx: Context<'_, S>) {
        let span = ctx
            .span(&id)
            .expect("Span must exist for id in the current context");
        let metadata = span.metadata();

        // Get the shared info from the stored SpanStart record
        let (
            span_id,
            parent_span_id,
            start_time_unix_nano,
            severity_number,
            severity_text,
            start_attributes,
        ) = if let Some(SpanStartInfo {
            span_id,
            parent_span_id,
            start_time_unix_nano,
            severity_number,
            severity_text,
            attributes,
            ..
        }) = span.extensions().get::<SpanStartInfo>()
        {
            (
                *span_id,
                *parent_span_id,
                *start_time_unix_nano,
                *severity_number,
                severity_text.clone(),
                attributes.clone(),
            )
        } else {
            let (severity_number, severity_text) = tracing_level_to_severity(metadata.level());

            (
                self.next_span_id(),
                None,
                SystemTime::now(),
                severity_number,
                severity_text.to_string(),
                TelemetryAttributes::Unknown {
                    name: metadata.name().to_string(),
                    location: self.get_location(metadata),
                },
            ) // Fallback. Should not happen
        };

        let status = span.extensions().get::<SpanStatus>().cloned();

        let attributes = span
            .extensions()
            .get::<TelemetryAttributes>()
            .cloned()
            .unwrap_or({
                // If no attributes were recorded, use the start attributes
                start_attributes
            });

        let record = SpanEndInfo {
            trace_id: self.trace_id,
            span_id,
            span_name: attributes.to_string(),
            parent_span_id,
            start_time_unix_nano,
            end_time_unix_nano: SystemTime::now(),
            severity_number,
            severity_text,
            status,
            attributes,
        };

        // Store the record in span extensions
        span.extensions_mut().insert(record);
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        // Extract information about the current span
        let (span_id, span_name) = ctx
            .event_span(event)
            .or_else(|| process_span(&ctx))
            // Get the parent span to extract span information
            .and_then(|current_span| {
                current_span
                    .extensions()
                    .get::<SpanStartInfo>()
                    .map(|parent_span_start_info| {
                        (
                            Some(parent_span_start_info.span_id),
                            Some(parent_span_start_info.span_name.clone()),
                        )
                    })
            })
            .unwrap_or_default();

        // Get event metadata. If the event is coming from `tracing-log` bridge,
        // it will have normalized metadata, otherwise it will be None and we will use
        // the event metadata directly.
        let bridged_log_meta = event.normalized_metadata();
        let metadata = bridged_log_meta
            .as_ref()
            .unwrap_or_else(|| event.metadata());

        // TODO: calculate modified severity based on user config when such feature is implemented
        let (severity_number, severity_text) = tracing_level_to_severity(metadata.level());

        // Extract message from event
        let message = get_log_message(event);

        // Extract attributes in the following priority:
        // - Pre-populated attributes (most efficient way, but requires the caller to use our custom logging APIs)
        // - Legacy log metadata (if the event is coming from `tracing-log` bridge)
        // - Attributes from the event itself (if any, otherwise use default log attributes)
        let attributes = if let Some(attrs) = take_event_attributes() {
            if attrs.has_empty_location() {
                attrs.with_location(self.get_location(metadata))
            } else {
                attrs
            }
        } else if event.is_log() {
            // This means the event is coming from `tracing-log` bridge
            TelemetryAttributes::LegacyLog {
                original_severity_number: severity_number,
                original_severity_text: severity_text.to_string(),
                location: self.get_location(metadata),
            }
        } else {
            get_log_event_attrs(event.into())
                // Auto-inject location if missing
                .map(|attrs| {
                    if attrs.has_empty_location() {
                        attrs.with_location(self.get_location(metadata))
                    } else {
                        attrs
                    }
                })
                .unwrap_or_else(|| TelemetryAttributes::Log {
                    code: None,
                    dbt_core_code: None,
                    original_severity_number: severity_number,
                    original_severity_text: severity_text.to_string(),
                    location: self.get_location(metadata),
                })
        };

        let log_record = LogRecordInfo {
            time_unix_nano: SystemTime::now(),
            trace_id: self.trace_id,
            span_id,
            span_name,
            severity_number,
            severity_text: severity_text.to_string(),
            body: message,
            attributes,
        };

        // Set the data for this event
        store_event_data(log_record);
    }
}
