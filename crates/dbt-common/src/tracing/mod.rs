mod async_tracing;
mod background_writer;
mod config;
pub mod constants;
pub mod convert;
pub mod data_provider;
pub mod dbt_metrics;
pub mod emit;
pub mod error;
pub mod event_classifiers;
pub mod event_info;
pub mod filter;
pub mod formatters;
mod fs_error;
mod init;
pub mod invocation;
pub mod layer;
pub mod layers;
pub mod metrics;
pub mod middlewares;
mod private_events;
pub mod reload;
mod rotating_file_writer;
mod shared;
mod shared_writer;
pub mod shutdown;
pub mod span_info;
pub mod tracing_feature_handles;

pub use async_tracing::{spawn_blocking_traced, spawn_traced, spawn_traced_block_in_place};
pub use config::FsTraceConfig;
pub use emit::{
    create_debug_span, create_debug_span_with_parent, create_info_span,
    create_info_span_with_parent, create_root_info_span,
};
pub use init::{BaseSubscriber, TelemetryHandle, init_tracing, init_tracing_with_consumer_layer};
pub use tracing_feature_handles::{TracingConfigProvider, noop_tracing_config_provider};

#[cfg(test)]
mod tests;
