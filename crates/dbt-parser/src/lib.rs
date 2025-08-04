//! Resolver is a crate for resolving a dbt project. It is responsible
//! for resolving all project source files (i.e. models, seeds, tests, macros etc.)
//! and propagating all configuration properties.
//!
//! The result of resolution is a "parse" phase `manifest.json`, whereby
//! "parse" is used to describe the pre-compiled manifest output, which
//! features the results of having rendered the dbt sql to extract the
//! full project configuration.

#![deny(missing_docs)]

pub mod args;
/// DbtNamespace for intercepting dbt macro calls during parse phase
pub mod dbt_namespace;
pub mod dbt_project_config;
pub mod renderer;
#[cfg(test)]
mod renderer_test;
/// All of the individual resolve functions broken out into their own files
pub mod resolve;
pub mod resolver;
pub mod sql_file_info;
pub mod tests;
pub mod utils;
