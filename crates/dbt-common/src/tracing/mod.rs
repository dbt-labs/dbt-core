mod background_writer;
mod config;
mod constants;
pub mod convert;
pub mod data_provider;
pub mod emit;
pub mod event_info;
pub mod filter;
pub mod formatters;
mod init;
pub mod invocation;
pub mod layer;
pub mod layers;
pub mod metrics;
pub mod middlewares;
mod private_events;
pub mod reload;
mod shared;
mod shared_writer;
pub mod shutdown;
pub mod span_info;

pub use config::FsTraceConfig;
pub use emit::{
    create_debug_span, create_debug_span_with_parent, create_info_span,
    create_info_span_with_parent, create_root_info_span,
};
pub use init::{BaseSubscriber, init_tracing, init_tracing_with_consumer_layer};

#[cfg(test)]
mod tests;
