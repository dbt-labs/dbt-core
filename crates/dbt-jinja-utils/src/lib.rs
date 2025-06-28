#![deny(missing_docs)]
//! This crate provides utilities for working with Jinja templates in the context of dbt.

/// Module for rendering event listener functionality
pub mod listener;

/// Module for serialization/deserialization functionality
pub mod serde;

/// Module containing utility functions and helpers
pub mod utils;

/// Module for functions implementations for the dbt jinja context
mod functions;

pub use functions::silence_base_context;
pub use functions::var_fn;

/// Module for the Jinja Environment
pub mod jinja_environment;

/// Module for building a Minijinja Environment
mod environment_builder;

/// Implements dbt's flags object for Minijinja
pub mod flags;

/// Module for the different phases of the dbt jinja environment
pub mod phases;

/// Module for the Invocation Args
pub mod invocation_args;

/// Module for the Refs and Sources
pub mod refs_and_sources;
