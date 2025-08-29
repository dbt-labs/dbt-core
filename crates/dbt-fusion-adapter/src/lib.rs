//! This crate provides dbt adapters.

/// Macros
#[macro_use]
mod macros;

pub mod base_adapter;
pub mod bridge_adapter;
pub mod cache;
pub mod convert_type;
pub mod errors;
pub mod factory;
pub mod formatter;
pub mod funcs;
pub mod information_schema;
pub mod need_quotes;
pub mod query_ctx;
pub mod record_and_replay;
pub mod render_constraint;
pub mod reserved_keywords;
pub mod response;
pub mod snapshots;
pub mod sql_engine;
pub mod typed_adapter;

// Re-export types and modules that were moved to dbt_auth
pub mod auth {
    pub use dbt_auth::Auth;
}
pub mod config {
    pub use dbt_auth::AdapterConfig;
}

mod statement;
pub use statement::{StmtCancellationReport, TrackedStatement, cancel_all_tracked_statements};

// Adapters for warehouses / dbs
/// Bigquery adapter
pub mod bigquery;
/// Databricks adapter
pub mod databricks;
/// Metadata adapter
pub mod metadata;
/// Parse adapter
pub mod parse;
/// Postgres adapter
pub mod postgres;
/// Redshift adapter
pub mod redshift;
/// Salesforce adapter
pub mod salesforce;
/// Snowflake adapter
pub mod snowflake;

/// Record batch utils
pub mod record_batch_utils;

pub mod cast_util;
/// Utils
pub mod relation_object;

/// SqlEngine
pub use sql_engine::SqlEngine;

/// Functions exposed to jinja
pub mod load_store;

pub use base_adapter::{AdapterType, AdapterTyping, BaseAdapter};
pub use bridge_adapter::BridgeAdapter;
pub use errors::AdapterResult;
pub use funcs::{execute_macro_with_package, execute_macro_wrapper_with_package};
pub use parse::adapter::ParseAdapter;
pub use response::AdapterResponse;
pub use typed_adapter::TypedBaseAdapter;

// Exposing structs for testing
pub use dbt_auth::AdapterConfig as AdapterConfigForTesting;
pub use sql_engine::SqlEngine as SqlEngineForTesting;
