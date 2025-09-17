mod config;
mod convert;
#[macro_use]
pub mod emit;
mod background_writer;
pub mod event_info;
mod init;
mod invocation;
mod layers;
pub mod metrics;
mod shared;
mod shared_writer;
pub mod span_info;

pub use config::FsTraceConfig;
pub use convert::log_level_to_severity;
pub use init::{TelemetryShutdown, init_tracing};
pub use invocation::create_invocation_attributes;

#[cfg(test)]
mod tests;
