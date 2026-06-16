//! Dbt-specific tracing initialization built on top of the generic subscriber setup.

use dbt_error::{FsError, FsResult};

use super::{
    config::FsTraceConfig,
    dbt_data_layer::{dbt_data_layer_config, dbt_process_span_attributes},
    tracing_feature_handles::TracingConfigProvider,
};
use dbt_tracing::{
    init::{TelemetryHandle, init_tracing_with_consumer_layer},
    layers::data_layer::TelemetryDataLayer,
};

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
pub fn init_tracing(
    config: FsTraceConfig,
) -> FsResult<(TelemetryHandle, Box<dyn TracingConfigProvider>)> {
    // Convert invocation ID to trace ID
    let trace_id = config.invocation_id.as_u128();

    let (middlewares, consumer_layers, shutdown_items, feature_handle) =
        config.build_layers()?.into_parts();

    // Strip code location in non-debug builds
    let strip_code_location = !cfg!(debug_assertions);

    let data_layer = TelemetryDataLayer::new(
        dbt_data_layer_config(trace_id, config.parent_span_id),
        strip_code_location,
        middlewares.into_iter(),
        consumer_layers.into_iter(),
    );

    // Base filter must allow events at the highest configured verbosity across all sinks
    // (e.g., stdout may be INFO while file log is TRACE)
    let effective_max_verbosity =
        std::cmp::max(config.max_log_verbosity, config.max_file_log_verbosity);

    let process_span = init_tracing_with_consumer_layer(
        effective_max_verbosity,
        dbt_process_span_attributes(config.package),
        data_layer,
    )
    .map_err(FsError::from)?;

    Ok((
        TelemetryHandle::new(shutdown_items, process_span),
        feature_handle,
    ))
}
