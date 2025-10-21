use super::super::{
    constants::{PROCESS_SPAN_NAME, ROOT_SPAN_NAME},
    data_provider::DataProvider,
    emit::get_file_and_line,
    event_info::{get_log_message, take_event_attributes},
    init::process_span,
    layer::{ConsumerLayer, MiddlewareLayer},
    metrics::init_metrics_storage_on_root_span,
    shared::Recordable,
    span_info::get_span_debug_extra_attrs,
};
use rand::RngCore;

use std::{sync::atomic::AtomicU64, time::SystemTime};

use tracing::{Level, Subscriber, span};
use tracing_subscriber::{
    Layer,
    layer::Context,
    registry::{LookupSpan, SpanRef},
};

use dbt_telemetry::{
    CallTrace, Invocation, LogMessage, LogRecordInfo, RecordCodeLocation, SeverityNumber,
    SpanEndInfo, SpanStartInfo, SpanStatus, TelemetryAttributes, TelemetryContext,
    TelemetryEventRecType, Unknown,
};

/// A bitmask used to represent which consumers are not interested in a given
/// telemetry span. Consumers are indexed by their position in the consumer list
/// of a TelemetryDataLayer.
///
/// Keep private to this module to avoid potential modification by consumers
/// or middleware layers via data provider access.
#[derive(Debug, Default, Clone, Copy)]
struct FilterMask(u64);

impl FilterMask {
    /// Returns an empty filter mask (meaning no consumers are disabled)
    pub(super) fn empty() -> Self {
        Self::default()
    }

    /// Returns a full filter mask (meaning all consumers are disabled)
    pub(super) fn disabled() -> Self {
        Self(u64::MAX)
    }

    /// Returns true if this is a filter mask that disables all consumers
    pub(super) fn is_disabled(&self) -> bool {
        self.0 == u64::MAX
    }

    pub(super) fn set_filtered(&mut self, index: usize) {
        // Shift would panic in debug if index >= 64, but we add a nice message via debug_assert
        debug_assert!(
            index < 64,
            "Exceeding mask length. Index must be less than 64"
        );
        self.0 |= 1 << index;
    }

    pub(super) fn is_filtered(&self, index: usize) -> bool {
        // Shift would panic in debug if index >= 64, but we add a nice message via debug_assert
        debug_assert!(
            index < 64,
            "Exceeding mask length. Index must be less than 64"
        );
        self.0 & (1 << index) != 0
    }
}

// Private newtypes to protect internal data layer state from being accessed
// by middleware or consumer layers. These types are stored in span extensions
// and are only accessible by the data layer itself.

/// Private wrapper for SpanStartInfo stored in span extensions.
struct DLSpanStartInfo(SpanStartInfo);

/// Private wrapper for TelemetryContext stored in span extensions.
struct DLTelemetryContext(TelemetryContext);

/// A tracing layer that creates structured telemetry data and stores it in span extensions.
///
/// This layer captures span events and converts them to structured telemetry
/// records that include the trace ID for correlation across systems.
pub struct TelemetryDataLayer<S>
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    /// The trace ID used for spans & events lacking a proper parent span
    /// (essentially the root span and any buggy tracing calls missing proper invocation
    /// span tree in their context).
    fallback_trace_id: u128,
    /// Whether to strip code location from span & log attributes.
    strip_code_location: bool,
    /// The telemetry middlewares to apply before notifying consumers.
    middlewares: Vec<MiddlewareLayer>,
    /// The telemetry consumers to notify of span & event events.
    consumers: Vec<ConsumerLayer>,
    /// If set, uses sequential span & event IDs for easier testing and debugging.
    /// Normally this is None and we use a thread-local RNG to generate
    /// unique span IDs, uuid::Uuid::new_v7() for event IDs.
    next_id: Option<AtomicU64>,
    /// Phantom data to associate with the subscriber type.
    __phantom: std::marker::PhantomData<S>,
}

impl<S> TelemetryDataLayer<S>
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    pub(in crate::tracing) fn new(
        fallback_trace_id: u128,
        strip_code_location: bool,
        middlewares: impl Iterator<Item = MiddlewareLayer>,
        consumers: impl Iterator<Item = ConsumerLayer>,
    ) -> Self {
        Self {
            fallback_trace_id,
            strip_code_location,
            middlewares: middlewares.collect(),
            consumers: consumers.collect(),
            next_id: None,
            __phantom: std::marker::PhantomData,
        }
    }

    /// For testing and debugging purposes, enables sequential span IDs
    pub(in crate::tracing) fn with_sequential_ids(&mut self) {
        self.next_id = Some(AtomicU64::new(1));
    }

    /// Returns a globally unique span ID for the next span. We can't use the span ID from
    /// `tracing` directly because it is not guaranteed to be unique across even within a single
    /// process, especially in a multi-threaded environment.
    ///
    /// This uses a thread-local random number generator which is thread-safe by design.
    /// The probability of collision for a 64-bit random number is negligible in practice.
    fn next_span_id(&self) -> u64 {
        self.next_id
            .as_ref()
            .map(|next_span_id| next_span_id.fetch_add(1, std::sync::atomic::Ordering::AcqRel))
            .unwrap_or_else(|| rand::rng().next_u64())
    }

    /// Returns a globally unique event ID for the next event.
    fn next_event_id(&self) -> uuid::Uuid {
        self.next_id
            .as_ref()
            .map(|next_event_id| {
                let id = next_event_id.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                // Convert the 64-bit ID to a UUID by padding with zeros
                let mut bytes = [0u8; 16];
                bytes[8..].copy_from_slice(&id.to_be_bytes());
                uuid::Uuid::from_bytes(bytes)
            })
            .unwrap_or_else(uuid::Uuid::now_v7)
    }

    fn get_location(
        &self,
        metadata: &tracing::Metadata<'_>,
        values: Option<Recordable<'_>>,
    ) -> RecordCodeLocation {
        if self.strip_code_location {
            RecordCodeLocation::default()
        } else {
            // Try extracting using our custom location
            if let Some((file, line)) = values.and_then(|values| get_file_and_line(values)) {
                return RecordCodeLocation {
                    file: Some(file),
                    line: Some(line),
                    ..Default::default()
                };
            }
            // Extract code location from metadata
            RecordCodeLocation::from(metadata)
        }
    }
}

impl<S> Layer<S> for TelemetryDataLayer<S>
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
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
        let location = self.get_location(metadata, Some(attrs.values().into()));

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
        let (trace_id, global_parent_span_id, parent_ctx, parent_span_filter_mask) = span
            .parent()
            .and_then(|parent_span| {
                let parent_span_ext = parent_span.extensions();
                let parent_span_filter_mask = parent_span_ext
                    .get::<FilterMask>()
                    .cloned()
                    .unwrap_or_else(FilterMask::empty);

                let parent_ctx = parent_span_ext.get::<DLTelemetryContext>();
                parent_span_ext
                    .get::<DLSpanStartInfo>()
                    .map(|parent_span_record| {
                        (
                            parent_span_record.0.trace_id,
                            Some(parent_span_record.0.span_id),
                            parent_ctx.map(|pcx| pcx.0.clone()),
                            parent_span_filter_mask,
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
                        FilterMask::empty(),
                    )
                } else {
                    (self.fallback_trace_id, None, None, FilterMask::empty())
                }
            });

        // First inject parent span context into the new span attributes (if any)
        if let Some(ctx) = &parent_ctx {
            attributes.inner_mut().with_context(ctx);
        }

        // Determine the context for this span: either this span provides it, or we inherit from parent
        let this_ctx = attributes.inner().context().or(parent_ctx);

        let start_time = SystemTime::now();
        let severity_number = metadata.level().into();

        let mut record = SpanStartInfo {
            trace_id,
            span_id: global_span_id,
            span_name: attributes.event_display_name(),
            parent_span_id: global_parent_span_id,
            links: None, // TODO: implement links from `follows_from`
            start_time_unix_nano: start_time,
            severity_number,
            severity_text: severity_number.as_str().to_string(),
            attributes: attributes.clone(),
        };

        // If this is the root span, initialize invocation-level metrics storage.
        // We have to do it early to ensure it is available to middlewares
        if span.parent().is_none() {
            init_metrics_storage_on_root_span(&span);
        }

        // Get root_span for data provider
        let root_span = span
            .scope()
            .from_root()
            .next()
            .expect("Root span must exist");

        // If tracing is set up correctly, the only time when root span doesn't have our special root
        // span name marker is when it is the process span created during tracing initialization.
        debug_assert!(
            root_span.name() == ROOT_SPAN_NAME || span.name() == PROCESS_SPAN_NAME || cfg!(test),
            "Expected root span created via `create_root_info_span`. Got: {}.
            Are you running code not instrumented under an invocation span tree?",
            root_span.name()
        );

        // For each span we save which consumers have filtered out this span
        let mut span_filter_mask = FilterMask::empty();

        if !self.middlewares.is_empty() {
            // In case middleware filters out the span, we need to rebuild
            // the original record to store in span extensions. By storing
            // something even for filtered out spans, we maintain invariants
            // for user code API's used to modify span attributes post-creation
            let rebuild_span_start_record = || SpanStartInfo {
                trace_id,
                span_id: global_span_id,
                span_name: attributes.event_display_name(),
                parent_span_id: global_parent_span_id,
                links: None, // TODO: implement links from `follows_from`
                start_time_unix_nano: start_time,
                severity_number,
                severity_text: severity_number.as_str().to_string(),
                attributes: attributes.clone(),
            };

            // This block scope ensures that we don't hold mutable extensions beyond middleware calls.
            // This is important because current span may be the root span itself, and we later take
            // another mutable reference to store the data there.
            let mut data_provider = DataProvider::new(&root_span);

            for middleware in &self.middlewares {
                match middleware.on_span_start(record, &mut data_provider) {
                    Some(next_record) => {
                        record = next_record;
                    }
                    None => {
                        span_filter_mask = FilterMask::disabled();
                        record = rebuild_span_start_record();
                        break;
                    }
                }
            }

            // Update attributes with the latest from the record in case middleware modified them.
            // But only if the span was not filtered out by middleware.
            if !span_filter_mask.is_disabled() {
                attributes = record.attributes.clone();
            }
        }

        // Notify consumers if the span was not filtered out by middleware
        // This block also creates scope to limit read-only borrow of span extensions
        // as we need mutable borrow later
        if !span_filter_mask.is_disabled() {
            let mut data_provider = DataProvider::new(&root_span);

            for (index, consumer) in self.consumers.iter().enumerate() {
                debug_assert!(
                    index < 64,
                    "Consumer index must be less than 64. Invariant is preserved by construction."
                );

                // Check if span is enabled for this consumer
                if !consumer.is_span_enabled(&record) {
                    // Mark this consumer as filtered out for this span
                    span_filter_mask.set_filtered(index);
                    continue;
                }

                // Check parent span is enabled for this consumer. This is the fast path
                // for the common case where parent span is not filtered out and we
                // can pass the record as is
                if !parent_span_filter_mask.is_filtered(index) {
                    // Parent span is not filtered out, we can pass the record as is
                    // No need to search for unfiltered parent
                    consumer.on_span_start(&record, &mut data_provider);
                    continue;
                }

                // Slow path: parent span was filtered out for this consumer.
                // Find the closest unfiltered parent span ID for this consumer (if any)
                // and then create a new record with the updated parent span ID
                let active_parent_span_id = lookup_filtered_parent_span_id(
                    index,
                    &span.parent().expect(
                        "Parent span must exist or otherwise we would have taken the other branch",
                    ),
                );

                // Now create a new record with the updated parent span ID
                let modified_record = SpanStartInfo {
                    parent_span_id: active_parent_span_id,
                    ..record.clone()
                };

                consumer.on_span_start(&modified_record, &mut data_provider);
            }
        }

        // Get a mutable reference to span extensions to store our data
        let mut ext_mut = span.extensions_mut();

        // First, private data that should only be accessible to data layer itself.
        // We use private newtype wrappers to avoid accidental modification by middleware
        // or consumer layers via data provider access.
        // Store the filter mask for this span
        ext_mut.insert(span_filter_mask);

        // Store an immutable start record in span extensions. Used later to build the SpanEnd record
        ext_mut.insert(DLSpanStartInfo(record));

        // Store computed context for this span (if any)
        if let Some(ctx) = this_ctx {
            ext_mut.insert(DLTelemetryContext(ctx));
        }

        // Finally store "mutable", user modifiable attributes.
        // This allows both the app code as well as middleware to
        // modify span attributes post-creation before they are finalized at span end.
        ext_mut.insert(attributes);
    }

    fn on_close(&self, id: span::Id, ctx: Context<'_, S>) {
        let span = ctx
            .span(&id)
            .expect("Span must exist for id in the current context");
        let metadata = span.metadata();

        // Extract the span end info from span extensions in block to limit borrow scope
        let (mut record, span_filter_mask) = {
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
            ) = if let Some(DLSpanStartInfo(SpanStartInfo {
                trace_id,
                span_id,
                parent_span_id,
                start_time_unix_nano,
                severity_number,
                severity_text,
                attributes,
                ..
            })) = span_ext.get::<DLSpanStartInfo>()
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
                let severity_number: SeverityNumber = metadata.level().into();
                let location = self.get_location(metadata, None);

                (
                    self.fallback_trace_id,
                    self.next_span_id(),
                    None,
                    SystemTime::now(),
                    severity_number,
                    severity_number.as_str().to_string(),
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

            // Pull the current span context (if any) to inject into closing attributes
            let current_ctx = span_ext.get::<DLTelemetryContext>().map(|c| c.0.clone());

            let mut attributes = span_ext.get::<TelemetryAttributes>().cloned().unwrap_or({
                // If no attributes were recorded, use the start attributes
                start_attributes
            });

            // Pull record status from span extensions (if any) or try to infer from attributes
            let status = span_ext
                .get::<SpanStatus>()
                .cloned()
                .or_else(|| attributes.get_span_status());

            // check that attributes are of expected record type
            debug_assert_eq!(attributes.record_category(), TelemetryEventRecType::Span);

            // Apply current span context (if any) before finalizing
            if let Some(ctx) = &current_ctx {
                attributes.inner_mut().with_context(ctx);
            }

            // For each span we have a mask of which consumers have filtered out this span.
            // It is determined at span creation time and stored in span extensions.
            let span_filter_mask = span_ext
                .get::<FilterMask>()
                .copied()
                .unwrap_or_else(FilterMask::empty);

            let record = SpanEndInfo {
                trace_id,
                span_id,
                span_name: attributes.event_display_name(),
                parent_span_id,
                links: None, // TODO: implement links from `follows_from`
                start_time_unix_nano,
                end_time_unix_nano: SystemTime::now(),
                severity_number,
                severity_text,
                status,
                attributes,
            };

            (record, span_filter_mask)
        };

        if span_filter_mask.is_disabled() {
            // Span was filtered out at creation by middleware or all consumers
            // are not interested in this span, so nothing to do here
            return;
        }

        let root_span = span
            .scope()
            .from_root()
            .next()
            .expect("Root span must exist");

        // If tracing is set up correctly, the only time when root span doesn't have our special root
        // span name marker is when it is the process span created during tracing initialization.
        debug_assert!(
            root_span.name() == ROOT_SPAN_NAME || span.name() == PROCESS_SPAN_NAME || cfg!(test),
            "Expected root span created via `create_root_info_span`. Got: {}.
            Are you running code not instrumented under an invocation span tree?",
            root_span.name()
        );

        let mut data_provider = DataProvider::new(&root_span);

        if !self.middlewares.is_empty() {
            for middleware in &self.middlewares {
                match middleware.on_span_end(record, &mut data_provider) {
                    Some(next_record) => {
                        record = next_record;
                    }
                    None => {
                        // Span was filtered out by middleware on close, early return
                        return;
                    }
                }
            }
        }

        // Notify consumers if the span was not filtered out by middleware
        let curr_parent = span.parent();
        let parent_span_filter_mask = curr_parent
            .as_ref()
            .and_then(|parent_span| parent_span.extensions().get::<FilterMask>().copied())
            .unwrap_or_else(FilterMask::empty);

        for (index, consumer) in self.consumers.iter().enumerate() {
            debug_assert!(
                index < 64,
                "Consumer index must be less than 64. Invariant is preserved by construction."
            );

            // Check if span is enabled for this consumer
            if span_filter_mask.is_filtered(index) {
                continue;
            }

            let Some(curr_parent) = curr_parent.as_ref() else {
                // No parent span, so no filtering to do. Call consumer and continue
                consumer.on_span_end(&record, &mut data_provider);
                continue;
            };

            // Check parent span is enabled for this consumer. This is the fast path
            // for the common case where parent span is not filtered out and we
            // can pass the record as is
            if !parent_span_filter_mask.is_filtered(index) {
                // Parent span is not filtered out, we can pass the record as is
                // No need to search for unfiltered parent
                consumer.on_span_end(&record, &mut data_provider);
                continue;
            }

            // Slow path: parent span was filtered out for this consumer.
            // Find the closest unfiltered parent span ID for this consumer (if any)
            // and then create a new record with the updated parent span ID
            let active_parent_span_id = lookup_filtered_parent_span_id(index, curr_parent);

            // Now create a new record with the updated parent span ID
            let modified_record = SpanEndInfo {
                parent_span_id: active_parent_span_id,
                ..record.clone()
            };

            consumer.on_span_end(&modified_record, &mut data_provider);
        }
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        // Extract information about the current span
        let (trace_id, span_id, span_name, parent_ctx, parent_span_filter_mask, parent_span) = ctx
            .event_span(event)
            .or_else(|| process_span(&ctx))
            // Get the parent span to extract span information
            .and_then(|parent_span| {
                let ctx_data = {
                    let parent_span_ext = parent_span.extensions();
                    let parent_span_filter_mask = parent_span_ext
                        .get::<FilterMask>()
                        .cloned()
                        .unwrap_or_else(FilterMask::empty);

                    let parent_ctx = parent_span_ext.get::<DLTelemetryContext>();
                    parent_span_ext
                        .get::<DLSpanStartInfo>()
                        .map(|parent_span_start_info| {
                            (
                                parent_span_start_info.0.trace_id,
                                Some(parent_span_start_info.0.span_id),
                                Some(parent_span_start_info.0.span_name.clone()),
                                parent_ctx.map(|pcx| pcx.0.clone()),
                                parent_span_filter_mask,
                            )
                        })
                };

                ctx_data.map(|cd| (cd.0, cd.1, cd.2, cd.3, cd.4, Some(parent_span)))
            })
            .unwrap_or_else(||
                // If no parent is found this is definitely a buggy tracing call (before our init & outside of any span)
                (self.fallback_trace_id, None, None, None, FilterMask::empty(), None));

        // Get event metadata
        let metadata = event.metadata();

        // Calculate code location
        let location = self.get_location(metadata, Some(event.into()));

        // TODO: calculate modified severity based on user config when such feature is implemented
        let severity_number: SeverityNumber = metadata.level().into();

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
                original_severity_text: severity_number.as_str().to_string(),
                package_name: None,
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

        let mut log_record = LogRecordInfo {
            time_unix_nano,
            trace_id,
            span_id,
            span_name,
            event_id: self.next_event_id(),
            severity_number,
            severity_text: severity_number.as_str().to_string(),
            body: message,
            attributes,
        };

        let root_span = parent_span
            .as_ref()
            .map(|ps| ps.scope().from_root().next().expect("Root span must exist"));

        // If tracing is set up correctly, all event's should be under a span tree rooted with our special root
        debug_assert!(
            root_span
                .as_ref()
                .is_some_and(|s| s.name() == ROOT_SPAN_NAME)
                || cfg!(test),
            "Expected root span created via `create_root_info_span`. Got: {}.
            Are you running code not instrumented under an invocation span tree?",
            root_span
                .as_ref()
                .map(|s| s.name())
                .unwrap_or("<no root span>")
        );

        let mut data_provider = root_span
            .as_ref()
            .map(|root_span| DataProvider::new(root_span))
            .unwrap_or_else(DataProvider::none);

        if !self.middlewares.is_empty() {
            for middleware in &self.middlewares {
                match middleware.on_log_record(log_record, &mut data_provider) {
                    Some(next_record) => {
                        log_record = next_record;
                    }
                    None => {
                        // Event was filtered out by middleware, early return
                        return;
                    }
                }
            }
        }

        // Notify consumers if the event was not filtered out by middleware
        for (index, consumer) in self.consumers.iter().enumerate() {
            debug_assert!(
                index < 64,
                "Consumer index must be less than 64. Invariant is preserved by construction."
            );

            if !consumer.is_log_enabled(&log_record) {
                continue;
            }

            // Check parent span is enabled for this consumer. This is the fast path
            // for the common case where parent span is not filtered out and we
            // can pass the record as is
            if !parent_span_filter_mask.is_filtered(index) {
                // Parent span is not filtered out, we can pass the record as is
                // No need to search for unfiltered parent
                consumer.on_log_record(&log_record, &mut data_provider);
                continue;
            }

            // Looking up filtered parent span ID requires a parent span, so check
            // we have one
            let Some(curr_parent) = parent_span.as_ref() else {
                // No parent span, so no filtering to do & can't get a real data provider
                consumer.on_log_record(&log_record, &mut data_provider);
                continue;
            };

            // Slow path: parent span was filtered out for this consumer.
            // Find the closest unfiltered parent span ID for this consumer (if any)
            // and then create a new record with the updated parent span ID
            let active_parent_span_id = lookup_filtered_parent_span_id(index, curr_parent);

            // Now create a new record with the updated parent span ID
            let modified_record = LogRecordInfo {
                span_id: active_parent_span_id,
                ..log_record.clone()
            };

            consumer.on_log_record(&modified_record, &mut data_provider);
        }
    }
}

fn lookup_filtered_parent_span_id<S>(index: usize, curr: &SpanRef<'_, S>) -> Option<u64>
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    let Some(mut parent) = curr.parent() else {
        // No parent span, so no active parent span ID
        return None;
    };

    loop {
        // Create a block scope to limit the borrow of parent extensions
        {
            let parent_ext = parent.extensions();
            let parent_span_filter_mask = parent_ext
                .get::<FilterMask>()
                .copied()
                .unwrap_or_else(FilterMask::empty);

            // Check if this parent span was filtered out for this consumer
            if !parent_span_filter_mask.is_filtered(index) {
                // Found an unfiltered parent span for this consumer. Extract its span ID
                if let Some(parent_span_record) = parent_ext.get::<DLSpanStartInfo>() {
                    return Some(parent_span_record.0.span_id);
                } else {
                    unreachable!("Parent span must have a SpanStartInfo record in its extensions");
                }
            }
        }

        let Some(grand_parent) = parent.parent() else {
            // No parent span, so no active parent span ID
            return None;
        };

        parent = grand_parent;
    }
}

#[cfg(test)]
pub(in crate::tracing) fn get_span_start_info_from_span(
    span: &SpanRef<'_, impl Subscriber + for<'lookup> LookupSpan<'lookup>>,
) -> Option<SpanStartInfo> {
    let span_ext = span.extensions();
    span_ext
        .get::<DLSpanStartInfo>()
        .map(|start_info| start_info.0.clone())
}

#[cfg(test)]
mod tests {
    use super::FilterMask;

    #[test]
    fn empty_mask_reports_empty() {
        let mask = FilterMask::empty();

        for i in 0..64 {
            assert!(!mask.is_filtered(i));
        }
    }

    #[test]
    fn full_mask_reports_full() {
        let mask = FilterMask::disabled();

        assert!(mask.is_disabled());
        for i in 0..64 {
            assert!(mask.is_filtered(i));
        }
    }

    #[test]
    fn set_filtered_marks_bits() {
        let mut mask = FilterMask::empty();

        assert!(!mask.is_filtered(1));
        assert!(!mask.is_filtered(63));

        mask.set_filtered(1);
        mask.set_filtered(63);

        assert!(mask.is_filtered(1));
        assert!(mask.is_filtered(63));
        assert!(!mask.is_filtered(0));
    }
}
