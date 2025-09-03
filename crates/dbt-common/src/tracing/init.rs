use std::sync::OnceLock;

use dbt_telemetry::{ProcessInfo, TelemetryAttributes};
use tracing::{Subscriber, level_filters::LevelFilter, span};

use tracing_subscriber::{
    EnvFilter, Layer, Registry,
    layer::{Context, Layered, SubscriberExt},
    registry::{LookupSpan, SpanRef},
};

use super::{
    background_writer::BackgroundWriter,
    config::FsTraceConfig,
    event_info::store_event_attributes,
    layers::{
        data_layer::TelemetryDataLayer, jsonl_writer::TelemetryJsonlWriterLayer,
        otlp::OTLPExporterLayer, parquet_writer::TelemetryParquetWriterLayer,
    },
};
use crate::{
    ErrorCode, FsError, FsResult, logging::LogFormat, stdfs::File,
    tracing::layers::progress_bar::ProgressBarLayer,
};

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
    pub(crate) fn new(
        items: Vec<Box<dyn TelemetryShutdown + Send>>,
        process_span_handle: span::Span,
    ) -> Self {
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

/// Initializes tracing with the provided configuration and default layers (aka consumers).
pub fn init_tracing(config: FsTraceConfig) -> FsResult<TelemetryHandle> {
    init_tracing_with_layer(
        config,
        None::<Box<dyn Layer<Layered<EnvFilter, Registry>> + Send + Sync>>,
    )
}

/// Initializes tracing with the provided configuration and an optional extra (consuming) layer(s).
///
/// If you need to add multiple consumers, remember that `Vec<Layer>` is also a valid layer!
///
/// IMPORTANT: there are a number of extra constraints on the `extra_layer` beyond what
/// `tracing` itself implies:
/// - Never rely on or read span/event attributes (aka structured fields)! All of the
///   necessary data for your layer must either come from span/event metadata or
///   from structured data as described below. If you lack something, you must
///   extend the schema of `SpanAttributes` or `LogRecordInfo` definitions and pass
///   new fields at call-sites accordingly.
/// - Prefer structured data available in span extensions for spans
///   See `dbt-common::tracing::jsonl_writer::TelemetryWriterLayer` `on_new_span` and
///   `on_close` methods for example.
/// - Use data available via `event_info::with_current_thread_event_data` to
///   access structured event data. See `dbt-common::tracing::jsonl_writer::TelemetryWriterLayer`
///   `on_event` method for example.
/// - Extra layer should never return `false` from `enabled` method. Remember that any layer
///   that returns `false` turns that span/event off for all layers, which will break
///   our guarantees about structured data availability.
///   Instead use `with_filter` method to apply filtering and access structured data
///   inside the filtered closure if needed.
/// - Layers must extract event data in the thread where `on_event` is called. Thus,
///   even if some processing must be moved into a different thread (including async tasks),
///   the data should be extracted first and cloned into the new thread.
pub fn init_tracing_with_layer<L>(
    config: FsTraceConfig,
    extra_layer: L,
) -> FsResult<TelemetryHandle>
where
    L: Layer<Layered<EnvFilter, Registry>> + Send + Sync + 'static,
{
    // Check if tracing is already initialized
    if PROCESS_SPAN.get().is_some() {
        return Err(unexpected_fs_err!("Tracing is already initialized"));
    }

    let package = config.package;

    let (subscriber, shutdown_items) = create_tracing_subcriber_with_layer(config, extra_layer)?;

    tracing::subscriber::set_global_default(subscriber)
        .map_err(|_| unexpected_fs_err!("Failed to set-up tracing"))?;

    // Create the process span and store it in the global PROCESS_SPAN
    store_event_attributes(TelemetryAttributes::Process(ProcessInfo::new(package)));
    let process_span = tracing::info_span!("Process");

    PROCESS_SPAN
        .set(process_span.id().expect("Process span must have an ID"))
        .expect("Process span must be set only once");

    Ok(TelemetryHandle::new(shutdown_items, process_span))
}

pub(crate) fn create_tracing_subcriber_with_layer<L>(
    config: FsTraceConfig,
    extra_layer: L,
) -> FsResult<(
    impl Subscriber + Send + Sync + 'static,
    Vec<Box<dyn TelemetryShutdown + Send>>,
)>
where
    L: Layer<Layered<EnvFilter, Registry>> + Send + Sync + 'static,
{
    // Convert invocation ID to trace ID
    let trace_id = config.invocation_id.as_u128();

    // This will hold all items that need to be shutdown before the process exits.
    let mut shutdown_items: Vec<Box<dyn TelemetryShutdown + Send>> = Vec::new();

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
    let base_telemetry_level = if config.max_log_verbosity > LevelFilter::DEBUG {
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
        .add_directive("ureq=off".parse().expect("Must be ok"));

    // TODO: If OTLP exporter is enabled, we need to shut off it's own logging
    // as it currently breaks the global span logic (it fires before the first span
    // and we panic without one)
    let base_telemetry_filter =
        base_telemetry_filter.add_directive("opentelemetry=off".parse().expect("Must be ok"));

    // Strip code location in non-debug builds
    let strip_code_location = !cfg!(debug_assertions);

    // Create the data layer
    let data_layer = TelemetryDataLayer::new(trace_id, strip_code_location);

    // Create jsonl writer layer if file path provided
    let jsonl_writer_layer = if let Some(file_path) = config.otm_file_path {
        let file = File::create(file_path)?;
        let (writer, handle) = BackgroundWriter::new(file);

        // Keep a handle for shutdown
        shutdown_items.push(Box::new(handle));

        // Create layer and apply user specified filtering
        Some(TelemetryJsonlWriterLayer::new(writer).with_filter(config.max_log_verbosity))
    } else {
        None
    };

    // Create jsonl writer layer on stdout if log format is OTEL
    let jsonl_stdout_writer_layer = if config.log_format == LogFormat::Otel {
        // No shutdown logic as we flushing to stdout as we write anyway
        Some(
            TelemetryJsonlWriterLayer::new(std::io::stdout()).with_filter(config.max_log_verbosity),
        )
    } else {
        None
    };

    // Create parquet writer layer if file path provided
    let parquet_writer_layer = if let Some(file_path) = config.otm_parquet_file_path {
        // Create the file and initialize the Parquet layer
        let file_dir = file_path.parent().ok_or_else(|| {
            fs_err!(
                ErrorCode::IoError,
                "Failed to get parent directory for file path"
            )
        })?;

        crate::stdfs::create_dir_all(file_dir)?;

        let file = std::fs::File::create(&file_path)
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to create parquet file: {}", e))?;

        let (parquet_layer, writer_handle) = TelemetryParquetWriterLayer::new(file)?;

        shutdown_items.push(Box::new(writer_handle));

        // Create layer. User specified filtering is not applied here
        Some(parquet_layer)
    } else {
        None
    };

    // Create progress bar layer if log-format default enabled (but not for Otel)
    let progress_bar_layer = if config.enable_progress && config.log_format != LogFormat::Otel {
        // Create layer and apply user specified filtering
        Some(ProgressBarLayer.with_filter(config.max_log_verbosity))
    } else {
        None
    };

    // Create OTLP layer - if enabled and endpoint is set via env vars
    let maybe_otlp_layer = if config.export_to_otlp
        && let Some(otlp_layer) = OTLPExporterLayer::new()
    {
        shutdown_items.push(Box::new(otlp_layer.tracer_provider()));
        shutdown_items.push(Box::new(otlp_layer.logger_provider()));
        Some(otlp_layer)
    } else {
        None
    };

    // Compose all layers as sequential except the global filter. The latter
    // must be passed via `with` below, otherwise registry elides that LevelFilter is off
    // and disables all tracing
    let layers = data_layer
        .and_then(jsonl_writer_layer)
        .and_then(jsonl_stdout_writer_layer)
        .and_then(parquet_writer_layer)
        .and_then(progress_bar_layer)
        .and_then(maybe_otlp_layer)
        .and_then(extra_layer);

    // Compose the registry with global filter and all layers
    let subscriber = Registry::default().with(base_telemetry_filter).with(layers);

    Ok((subscriber, shutdown_items))
}
