//! The dbt adapter layer.

#![allow(clippy::let_and_return)]

mod macro_exec;
mod value;

pub mod adapter;
pub mod cache;
pub mod catalog_relation;
pub mod column;
/// Connection management, thread-local storage, and connection backpressure.
pub mod connection;
pub mod engine;
pub mod errors;
pub mod format_ident;
pub mod formatter;
pub mod information_schema;
pub mod load_catalogs;
pub mod metadata;
pub mod need_quotes;
pub(crate) mod python;
pub mod query_cache;
pub mod query_ctx;
pub mod relation;
pub mod render_constraint;
pub mod response;
pub(crate) mod seed;
pub mod snapshots;
/// Tokenizing and fuzzy diffing of SQL strings
pub mod sql;
pub mod sql_types;
pub mod statement;
pub mod stmt_splitter;

/// Cross-Version Record/Replay System
pub mod time_machine;

// Re-export types and modules that were moved to dbt_auth
pub mod auth {
    pub use dbt_auth::Auth;
}
pub mod config {
    pub use dbt_auth::AdapterConfig;
}

/// Parse adapter
pub mod parse;

pub mod mock;

/// Record batch utils
pub mod record_batch_utils;

pub mod cast_util;

/// SqlEngine
pub use engine::AdapterEngine;

/// Functions exposed to jinja
pub mod load_store;

pub use adapter::Adapter;
pub use adapter::AdapterImpl;
pub use column::{Column, ColumnBuilder};
pub use dbt_adapter_core::AdapterType;
pub use errors::AdapterResult;
pub use macro_exec::{
    convert_macro_result_to_record_batch, execute_macro_with_package,
    execute_macro_wrapper_with_package,
};
pub use response::AdapterResponse;
