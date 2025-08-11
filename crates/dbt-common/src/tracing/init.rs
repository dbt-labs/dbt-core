use std::sync::{Arc, OnceLock, RwLock};

use dbt_telemetry::SpanAttributes;
use tracing::{Subscriber, level_filters::LevelFilter, span};

use tracing_subscriber::{
    EnvFilter, Registry,
    fmt::format::FmtSpan,
    layer::{Context, SubscriberExt},
    registry::SpanRef,
};

use super::{
    ToTracingValue,
    config::FsTraceConfig,
    constants::TRACING_ATTR_FIELD,
    file_writer::TelemetryFileWriter,
    layers::{data_layer::TelemetryDataLayer, jsonl_writer::TelemetryWriterLayer},
};
use crate::{FsError, FsResult, stdfs::File};

// We use a global to store the a special "process" span Id, that
// is created during initialization and used as a fallback span
// if any logs or spans are emitted outside of our infrastructure.
//
// This may happen for two reasons:
// - some library used in the code flow before Invocation span is created that
// is not filtered by our `tracing` filters.
// - Intentionally emitted logs outside of the Invocation span
//
// This is also used to ensure that tracing is initialized at most once.
static PROCESS_SPAN: OnceLock<span::Id> = OnceLock::new();

/// This trait is used by the handle that telemetry initialization returns,
/// to allow the caller to shut down the telemetry system gracefully.
pub trait TelemetryShutdown {
    fn shutdown(&mut self) -> FsResult<()>;
}

/// The handle returned by the telemetry initialization function.
///
/// Make sure to call `shutdown` on it when you are done with telemetry,
/// to ensure that all telemetry resources are released properly.
pub struct TelemetryHandle {
    items: Vec<Box<dyn TelemetryShutdown + Send>>,
    // We have Option here to allow first dropping the handle
    // during shutdown, and then closing all layers
    process_span_handle: Option<span::Span>,
}

impl TelemetryHandle {
    fn new(items: Vec<Box<dyn TelemetryShutdown + Send>>, process_span_handle: span::Span) -> Self {
        TelemetryHandle {
            items,
            process_span_handle: Some(process_span_handle),
        }
    }

    pub fn process_span(&self) -> &span::Span {
        self.process_span_handle
            .as_ref()
            .expect("Do not call this function after shutdown")
    }

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

/// The process span for the current process. Only available after
/// tracing has been initialized and before tracing handle is dropped.
///
/// See `PROCESS_SPAN` for more details.
pub(super) fn process_span<'a, S>(ctx: &'a Context<'a, S>) -> Option<SpanRef<'a, S>>
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    let process_span_id = PROCESS_SPAN.get()?;

    ctx.span(process_span_id)
}

pub fn init_tracing(config: FsTraceConfig) -> FsResult<TelemetryHandle> {
    // Check if tracing is already initialized
    if PROCESS_SPAN.get().is_some() {
        return Err(unexpected_fs_err!("Tracing is already initialized"));
    }

    // Convert invocation ID to trace ID
    let trace_id = config.invocation_id.as_u128();

    // This will hold all items that need to be shutdown before the process exits.
    let mut shutdown_items: Vec<Box<dyn TelemetryShutdown + Send>> = Vec::new();

    // Set-up global filters first
    // In debug builds we allow RUST_LOG to control the global level filter
    #[cfg(debug_assertions)]
    let global_level_filter = EnvFilter::builder()
        .with_default_directive(config.max_log_level.into())
        .from_env_lossy();

    // For prod builds it is almost the same except RUST_LOG is not used
    #[cfg(not(debug_assertions))]
    let global_level_filter = EnvFilter::builder().parse_lossy(config.max_log_level.to_string());

    // Turn off logging for some common libraries that are too verbose
    let global_level_filter = global_level_filter
        .add_directive("hyper=off".parse().expect("Must be ok"))
        .add_directive("h2=off".parse().expect("Must be ok"))
        .add_directive("reqwest=off".parse().expect("Must be ok"))
        .add_directive("ureq=off".parse().expect("Must be ok"));

    // TODO: If OTLP exporter is enabled, we need to shut off it's own logging
    // as it currently breaks the global span logic (it fires before the first span
    // and we panic without one)
    #[cfg(all(debug_assertions, feature = "otlp"))]
    let global_level_filter =
        global_level_filter.add_directive("opentelemetry=off".parse().expect("Must be ok"));

    // Strip code location if the calculated level if below TRACE.
    let strip_code_location = global_level_filter
        .max_level_hint()
        .map(|max_level_filter| max_level_filter <= LevelFilter::DEBUG)
        .unwrap_or(false);

    // Create the data layer
    let data_layer = TelemetryDataLayer::new(trace_id, strip_code_location);

    // Add debug writer layer (dumps traces to stdout) if enabled
    // TODO: Currently never enabled because `config.print_to_stdout`,
    // need to add a debug only cli flag to enable it.
    let debug_layer = if cfg!(debug_assertions) && config.print_to_stdout {
        Some(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stdout)
                .with_span_events(FmtSpan::FULL)
                .with_ansi(false),
        )
    } else {
        None
    };

    // Create jsonl writer layer if file path provided
    let jsonl_writer_layer = if let Some(file_path) = config.otm_file_path {
        let file = Arc::new(File::create(file_path)?);
        let channel_writer = Arc::new(RwLock::new(TelemetryFileWriter::new(Box::new(file))));

        shutdown_items.push(Box::new(channel_writer.clone()));

        Some(TelemetryWriterLayer::new(channel_writer))
    } else {
        None
    };

    // Compose the registry with all layers using Option<Layer> pattern
    let subscriber = Registry::default()
        .with(global_level_filter)
        .with(data_layer)
        .with(jsonl_writer_layer)
        .with(debug_layer);

    // Create OTLP layer - Only in debug builds, feature enabled and if endpoint provided
    #[cfg(all(debug_assertions, feature = "otlp"))]
    let subscriber = {
        use super::layers::otlp::OTLPExporterLayer;

        let maybe_otlp_layer = if config.export_to_otlp
            && let Some(otlp_layer) = OTLPExporterLayer::new()
        {
            shutdown_items.push(Box::new(otlp_layer.tracer_provider()));
            shutdown_items.push(Box::new(otlp_layer.logger_provider()));
            Some(otlp_layer)
        } else {
            None
        };
        subscriber.with(maybe_otlp_layer)
    };

    tracing::subscriber::set_global_default(subscriber)
        .map_err(|_| unexpected_fs_err!("Failed to set-up tracing"))?;

    // Create the process span and store it in the global PROCESS_SPAN
    let process_span = tracing::info_span!(
        "Process",
        { TRACING_ATTR_FIELD } = SpanAttributes::Process {
            version: env!("CARGO_PKG_VERSION").to_string(),
            host_os: std::env::consts::OS.to_string(),
            host_arch: std::env::consts::ARCH.to_string(),
        }
        .to_tracing_value(),
    );

    PROCESS_SPAN
        .set(process_span.id().expect("Process span must have an ID"))
        .expect("Process span must be set only once");

    Ok(TelemetryHandle::new(shutdown_items, process_span))
}
