use std::sync::OnceLock;

use dbt_telemetry::create_process_event_data;
use tracing::{Subscriber, level_filters::LevelFilter, span};

use tracing_subscriber::{
    EnvFilter, Layer, Registry,
    layer::{Context, Layered, SubscriberExt},
    registry::{LookupSpan, SpanRef},
};

use super::{
    config::FsTraceConfig, constants::PROCESS_SPAN_NAME, event_info::store_event_attributes,
    layers::data_layer::TelemetryDataLayer, shutdown::TelemetryShutdownItem,
};
use dbt_error::{FsError, FsResult};

// We use a global to store a special "process" span Id, that
// is created during initialization and used as a fallback span
// if any logs or spans are emitted outside of the context of our infrastructure.
//
// This may happen for two reasons:
// - some library used in the code flow before "Invocation" span is created that
// is not filtered by our `tracing` filters.
// - Intentionally emitted logs outside of the "Invocation" span
//
// Normally any binary using our infra should go through initialisation
// that will assign this span. However, in some scenarios, such as unit
// tests - it may stay uninitialized
static PROCESS_SPAN: OnceLock<span::Id> = OnceLock::new();

/// The process span for the current process. Only available after
/// tracing has been initialized and before tracing handle is dropped.
///
/// See `PROCESS_SPAN` for more details.
pub(super) fn process_span<'a, S>(ctx: &'a Context<'a, S>) -> Option<SpanRef<'a, S>>
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    let process_span_id = PROCESS_SPAN.get()?;

    ctx.span(process_span_id)
}

pub type BaseSubscriber = Layered<EnvFilter, Registry>;

/// The handle returned by the telemetry initialization function.
///
/// Make sure to call `shutdown` on it when you are done with telemetry,
/// to ensure that all telemetry resources are released properly.
pub struct TelemetryHandle {
    items: Vec<TelemetryShutdownItem>,
    // We have Option here to allow first dropping the handle
    // during shutdown, and then closing all layers
    process_span_handle: Option<span::Span>,
}

impl TelemetryHandle {
    pub(crate) fn new(items: Vec<TelemetryShutdownItem>, process_span_handle: span::Span) -> Self {
        TelemetryHandle {
            items,
            process_span_handle: Some(process_span_handle),
        }
    }

    /// Gracefully shuts down telemetry
    pub fn shutdown(&mut self) -> Vec<FsError> {
        // First, drop the process span handle to ensure that
        // the process span is closed properly.
        if let Some(handle) = self.process_span_handle.take() {
            drop(handle);
        }

        // Then, do shutdown of all items.
        self.items
            .iter_mut()
            .filter_map(|item| item.shutdown().err())
            .map(|err| *err)
            .collect()
    }
}

/// Initializes tracing with consumer layers defined by the provided configuration.
///
/// This function will set up a global tracing subscriber and will fail on re-entry.
///
/// If you need to change or add layers after initialization, `init_tracing_with_consumer_layer`
/// can be used to set up a reloadable data layer. See `super::reload::create_realodable_data_layer`.
///
/// # Returns
///
/// On success, returns a `TelemetryHandle` that should be used for graceful shutdown.
pub fn init_tracing(config: FsTraceConfig) -> FsResult<TelemetryHandle> {
    // Convert invocation ID to trace ID
    let trace_id = config.invocation_id.as_u128();

    let (middlewares, consumer_layers, shutdown_items) = config.build_layers()?;

    // Strip code location in non-debug builds
    let strip_code_location = !cfg!(debug_assertions);

    let data_layer = TelemetryDataLayer::new(
        trace_id,
        config.parent_span_id,
        strip_code_location,
        middlewares.into_iter(),
        consumer_layers.into_iter(),
    );

    let process_span =
        init_tracing_with_consumer_layer(config.max_log_verbosity, config.package, data_layer)?;

    Ok(TelemetryHandle::new(shutdown_items, process_span))
}

/// Initializes tracing with the provided data layer, which is ultimately
/// composed of middleware and consumer layers.
///
/// This function will set up a global tracing subscriber and will fail on re-entry.
///
/// If you need to change or add layers after initialization, use `super::reload::create_realodable_data_layer`,
/// to get a reloadable data layer.
///
/// IMPORTANT: there are a number of extra constraints on consumer layers beyond what
/// the `TelemetryConsumer` trait itself implies:
/// - Never rely on or read span/event attributes provided by `tracing` directly! All of the
///   necessary data for your consumer must come from the structured records (`SpanStartInfo`,
///   `SpanEndInfo`, `LogRecordInfo`). If you lack something, extend the schema of
///   an existing event or add new one and pass new fields at call-sites accordingly.
/// - Apply filtering via `with_filter`, `with_span_filter` and/or `with_log_filter`
///   methods defined for all consumer layers - this will facilitate modularity
///
/// # Returns
///
/// On success, returns the "process" span, used as a parent span fallback of last resort
/// in data layer for events (but not for spans!).
pub fn init_tracing_with_consumer_layer<D: Layer<BaseSubscriber> + Send + Sync + 'static>(
    max_log_verbosity: LevelFilter,
    package: &str,
    data_layer: D,
) -> FsResult<span::Span> {
    // Check if tracing is already initialized
    if PROCESS_SPAN.get().is_some() {
        return Err(unexpected_fs_err!("Tracing is already initialized"));
    }

    let subscriber = create_tracing_subcriber_with_layer(max_log_verbosity, data_layer);

    tracing::subscriber::set_global_default(subscriber)
        .map_err(|_| unexpected_fs_err!("Failed to set-up tracing"))?;

    // Create the process span and store it in the global PROCESS_SPAN
    store_event_attributes(create_process_event_data(package));
    let process_span = tracing::info_span!(PROCESS_SPAN_NAME);

    PROCESS_SPAN
        .set(process_span.id().expect("Process span must have an ID"))
        .expect("Process span must be set only once");

    Ok(process_span)
}

/// Creates a tracing subscriber implementing our telemetry data pipeline.
///
/// See module README for details on the pipeline.
pub(super) fn create_tracing_subcriber_with_layer<
    D: Layer<BaseSubscriber> + Send + Sync + 'static,
>(
    max_log_verbosity: LevelFilter,
    data_layer: D,
) -> impl Subscriber + Send + Sync + 'static {
    // Set-up global filters first.
    //
    // IMPORTANT! This is not the user provided output log level!
    // At tracing subscriber level we use either DEBUG or TRACE, but not lower
    // than that. This way only developer spans/events with trace level can
    // be fully filtered out, but otherwise everything goes into our
    // tracing pipeline. User preferences are applied on a per-consumer layer
    // level. This way we can have different output on stdout, log file, telemetry,
    // and other consumers.
    //
    // In addition to that, in debug builds we allow RUST_LOG to control the global level filter
    let base_telemetry_level = if max_log_verbosity > LevelFilter::DEBUG {
        LevelFilter::TRACE
    } else {
        LevelFilter::DEBUG
    };

    #[cfg(debug_assertions)]
    let base_telemetry_filter = EnvFilter::builder()
        .with_default_directive(base_telemetry_level.into())
        .from_env_lossy();

    // For prod builds it is almost the same except RUST_LOG is not used
    #[cfg(not(debug_assertions))]
    let base_telemetry_filter = EnvFilter::builder().parse_lossy(base_telemetry_level.to_string());

    // Turn off logging for some common libraries that are too verbose
    let base_telemetry_filter = base_telemetry_filter
        .add_directive("hyper=off".parse().expect("Must be ok"))
        .add_directive("h2=off".parse().expect("Must be ok"))
        .add_directive("reqwest=off".parse().expect("Must be ok"))
        .add_directive("ureq=off".parse().expect("Must be ok"))
        // Shut off OTLP exporter's own logging
        .add_directive("opentelemetry=off".parse().expect("Must be ok"));

    // Compose the registry with global filter and data layer
    Registry::default()
        .with(base_telemetry_filter)
        .with(data_layer)
}
