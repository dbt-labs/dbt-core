//! Module for configuring the jinja environment for the run phase

mod run_config;
mod run_node_context;

pub use run_node_context::{build_run_node_context, extend_base_context_stateful_fn};
