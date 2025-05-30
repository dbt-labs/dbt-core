//! This module contains everything related to configuring the Jinja environment for the parse phase.

mod resolve_context;
mod resolve_model_context;

pub mod init;
pub mod sql_resource;

pub use resolve_context::build_resolve_context;
pub use resolve_model_context::{build_resolve_model_context, render_extract_ref_or_source_expr};
