use super::super::{
    convert::tracing_level_to_severity,
    event_info::{get_log_message, set_current_log_record, take_event_attributes},
    init::process_span,
    span_info::get_span_debug_extra_attrs,
};
use rand::RngCore;

use std::time::SystemTime;

use tracing::{Level, Subscriber, span};
use tracing_subscriber::{Layer, layer::Context};

use dbt_telemetry::{
    CallTrace, Invocation, LogMessage, LogRecordInfo, RecordCodeLocation, SpanEndInfo,
    SpanStartInfo, SpanStatus, TelemetryAttributes, TelemetryContext, TelemetryEventRecType,
    Unknown,
};

/// A tracing layer that creates structured telemetry data and stores it in span extensions.
///
/// This layer captures span events and converts them to structured telemetry
/// records that include the trace ID for correlation across systems.
pub(in crate::tracing) struct TelemetryDataLayer<S>
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    /// The trace ID used for spans & events lacking a proper parent span
    /// (essentially the root span and any buggy tracing calls missing proper invocation
    /// span tree in their context).
    fallback_trace_id: u128,
    /// Whether to strip code location from span & log attributes.
    strip_code_location: bool,
    __phantom: std::marker::PhantomData<S>,
}

impl<S> TelemetryDataLayer<S>
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    pub(crate) fn new(fallback_trace_id: u128, strip_code_location: bool) -> Self {
        Self {
            fallback_trace_id,
            strip_code_location,
            __phantom: std::marker::PhantomData,
        }
    }

    /// Returns a globally unique span ID for the next span. We can't use the span ID from
    /// `tracing` directly because it is not guaranteed to be unique across even within a single
    /// process, especially in a multi-threaded environment.
    ///
    /// This uses a thread-local random number generator which is thread-safe by design.
    /// The probability of collision for a 64-bit random number is negligible in practice.
    fn next_span_id(&self) -> u64 {
        rand::rng().next_u64()
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

        // Start by extracting event attributes if any. To avoid leakage, we extract internal metadata
        // such as location, name etc. only in debug builds

        // Calculate code location once
        let location = self.get_location(metadata);

        // Extract attributes in the following priority:
        // - Pre-populated attributes (the "normal" way for all non-trace level spans)
        // - Fallback to default attributes based on metadata (shouldn't happen for properly instrumented spans)
        let mut attributes = if let Some(mut attrs) = take_event_attributes() {
            attrs.inner_mut().with_code_location(location);
            attrs
        } else if metadata.level() == &Level::TRACE {
            // Trace spans without explicit attributes considered dev internal
            CallTrace {
                name: metadata.name().to_string(),
                file: location.file.clone(),
                line: location.line,
                extra: get_span_debug_extra_attrs(attrs.values().into()),
            }
            .into()
        } else {
            Unknown {
                name: metadata.name().to_string(),
                file: location
                    .file
                    .as_ref()
                    .map_or("<unknown>", |v| v)
                    .to_string(),
                line: location.line.unwrap_or_default(),
            }
            .into()
        };

        // check that attributes are of expected record type
        debug_assert_eq!(attributes.record_category(), TelemetryEventRecType::Span);

        // Pull the trace ID, parent span ID & parent ctx from the parent span (if any)
        let (trace_id, global_parent_span_id, parent_ctx) = span
            .parent()
            .and_then(|parent_span| {
                let parent_span_ext = parent_span.extensions();
                let parent_ctx = parent_span_ext.get::<TelemetryContext>();
                parent_span_ext
                    .get::<SpanStartInfo>()
                    .map(|parent_span_record| {
                        (
                            parent_span_record.trace_id,
                            Some(parent_span_record.span_id),
                            parent_ctx.cloned(),
                        )
                    })
            })
            .unwrap_or_else(|| {
                // If no parent span is found, we have a couple possible scenarios:
                // 1. This is the root span of the trace, in which case we use the fallback trace ID, and no parent span ID
                // 2. This is an invocation span and we calculate the trace ID from `invocation_id` of the span
                // 3. This is a buggy tracing call missing proper invocation span tree in their context,
                //  in which case we fallback to the fallback trace ID and no parent span ID
                if let Some(Invocation { invocation_id, .. }) =
                    &attributes.downcast_ref::<Invocation>()
                {
                    (
                        // We use proto's to define event structures, which doesn't allow
                        // storing u128/uuid directly, so we store UUID string and convert it back here
                        uuid::Uuid::parse_str(invocation_id)
                            .expect("invocation_id Must be a valid UUID string")
                            .as_u128(),
                        None,
                        None,
                    )
                } else {
                    (self.fallback_trace_id, None, None)
                }
            });

        // First inject parent span context into the new span attributes (if any)
        if let Some(ctx) = &parent_ctx {
            attributes.inner_mut().with_context(ctx);
        }

        // Determine the context for this span: either this span provides it, or we inherit from parent
        let this_ctx = attributes.inner().context().or(parent_ctx);

        let start_time = SystemTime::now();
        let (severity_number, severity_text) = tracing_level_to_severity(metadata.level());

        let record = SpanStartInfo {
            trace_id,
            span_id: global_span_id,
            span_name: attributes.to_string(),
            parent_span_id: global_parent_span_id,
            links: None, // TODO: implement links from `follows_from`
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

        // Store computed context for this span (if any)
        if let Some(ctx) = this_ctx {
            ext_mut.insert(ctx);
        }
    }

    fn on_close(&self, id: span::Id, ctx: Context<'_, S>) {
        let span = ctx
            .span(&id)
            .expect("Span must exist for id in the current context");
        let metadata = span.metadata();

        // Acquire a read-only reference to span extensions
        let span_ext = span.extensions();

        // Get the shared info from the stored SpanStart record
        let (
            trace_id,
            span_id,
            parent_span_id,
            start_time_unix_nano,
            severity_number,
            severity_text,
            start_attributes,
        ) = if let Some(SpanStartInfo {
            trace_id,
            span_id,
            parent_span_id,
            start_time_unix_nano,
            severity_number,
            severity_text,
            attributes,
            ..
        }) = span_ext.get::<SpanStartInfo>()
        {
            (
                *trace_id,
                *span_id,
                *parent_span_id,
                *start_time_unix_nano,
                *severity_number,
                severity_text.clone(),
                attributes.clone(),
            )
        } else {
            let (severity_number, severity_text) = tracing_level_to_severity(metadata.level());
            let location = self.get_location(metadata);

            (
                self.fallback_trace_id,
                self.next_span_id(),
                None,
                SystemTime::now(),
                severity_number,
                severity_text.to_string(),
                Unknown {
                    name: metadata.name().to_string(),
                    file: location
                        .file
                        .as_ref()
                        .map_or("<unknown>", |v| v)
                        .to_string(),
                    line: location.line.unwrap_or_default(),
                }
                .into(),
            ) // Fallback. Should not happen
        };

        let status = span_ext.get::<SpanStatus>().cloned();

        // Pull the current span context (if any) to inject into closing attributes
        let current_ctx = span_ext.get::<TelemetryContext>().cloned();

        let mut attributes = span_ext.get::<TelemetryAttributes>().cloned().unwrap_or({
            // If no attributes were recorded, use the start attributes
            start_attributes
        });

        // check that attributes are of expected record type
        debug_assert_eq!(attributes.record_category(), TelemetryEventRecType::Span);

        // Apply current span context (if any) before finalizing
        if let Some(ctx) = &current_ctx {
            attributes.inner_mut().with_context(ctx);
        }

        // Drops the read-only reference to span extensions, necessary to acquire mutable reference later
        drop(span_ext);

        let record = SpanEndInfo {
            trace_id,
            span_id,
            span_name: attributes.to_string(),
            parent_span_id,
            links: None, // TODO: implement links from `follows_from`
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
        let (trace_id, span_id, span_name, parent_ctx) = ctx
            .event_span(event)
            .or_else(|| process_span(&ctx))
            // Get the parent span to extract span information
            .and_then(|current_span| {
                let current_span_ext = current_span.extensions();
                let parent_ctx = current_span_ext.get::<TelemetryContext>();
                current_span_ext
                    .get::<SpanStartInfo>()
                    .map(|parent_span_start_info| {
                        (
                            parent_span_start_info.trace_id,
                            Some(parent_span_start_info.span_id),
                            Some(parent_span_start_info.span_name.clone()),
                            parent_ctx.cloned(),
                        )
                    })
            })
            .unwrap_or_default();

        // Get event metadata
        let metadata = event.metadata();

        // Calculate code location
        let location = self.get_location(metadata);

        // TODO: calculate modified severity based on user config when such feature is implemented
        let (severity_number, severity_text) = tracing_level_to_severity(metadata.level());

        // Extract message from event
        let message = get_log_message(event);

        // Extract attributes in the following priority:
        // - Pre-populated attributes (the "normal" way)
        // - Attributes from the event itself (if any, otherwise use default log attributes)
        let mut attributes = if let Some(mut attrs) = take_event_attributes() {
            attrs.inner_mut().with_code_location(location);
            attrs
        } else {
            LogMessage {
                code: None,
                dbt_core_event_code: None,
                original_severity_number: severity_number as i32,
                original_severity_text: severity_text.to_string(),
                unique_id: None,
                phase: None,
                file: location.file,
                line: location.line,
            }
            .into()
        };

        // check that attributes are of expected record type
        debug_assert_eq!(attributes.record_category(), TelemetryEventRecType::Log);

        // Inject parent span context into the log attributes (if any)
        if let Some(ctx_val) = parent_ctx {
            attributes.inner_mut().with_context(&ctx_val);
        }

        let time_unix_nano = SystemTime::now();
        let event_id = uuid::Uuid::now_v7();

        let log_record = LogRecordInfo {
            time_unix_nano,
            trace_id,
            span_id,
            span_name,
            event_id,
            severity_number,
            severity_text: severity_text.to_string(),
            body: message,
            attributes,
        };

        // Set the data for this event
        set_current_log_record(log_record);
    }
}
