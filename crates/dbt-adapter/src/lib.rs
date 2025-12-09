//! The dbt adapter layer.

#![allow(clippy::let_and_return)]

#[macro_use]
mod macros;

pub mod adapter_engine;
pub mod base_adapter;
pub mod bridge_adapter;
pub mod cache;
pub mod catalog_relation;
pub mod column;
pub mod errors;
pub mod factory;
pub mod formatter;
pub mod funcs;
pub mod information_schema;
pub mod load_catalogs;
pub mod metadata;
pub mod need_quotes;
pub(crate) mod python;
pub mod query_cache;
pub mod query_comment;
pub mod query_ctx;
pub mod record_and_replay;
pub mod relation;
pub mod render_constraint;
pub mod response;
pub mod snapshots;
/// Tokenizing and fuzzy diffing of SQL strings
pub mod sql;
pub mod sql_types;
pub mod statement;
pub mod stmt_splitter;
pub mod typed_adapter;

// Re-export types and modules that were moved to dbt_auth
pub mod auth {
    pub use dbt_auth::Auth;
}
pub mod config {
    pub use dbt_auth::AdapterConfig;
}

// Adapters for warehouses / dbs
/// Bigquery adapter
pub mod bigquery;
/// Databricks adapter
pub mod databricks;
/// Parse adapter
pub mod parse;
/// Postgres adapter
pub mod postgres;
/// Redshift adapter
pub mod redshift;
/// Snowflake adapter
pub mod snowflake;

pub mod mock;

/// Record batch utils
pub mod record_batch_utils;

pub mod cast_util;

/// SqlEngine
pub use adapter_engine::AdapterEngine;

/// Functions exposed to jinja
pub mod load_store;

pub use base_adapter::{AdapterType, AdapterTyping, BaseAdapter};
pub use bridge_adapter::BridgeAdapter;
pub use column::{Column, ColumnBuilder};
pub use errors::AdapterResult;
pub use funcs::{execute_macro_with_package, execute_macro_wrapper_with_package};
pub use parse::adapter::ParseAdapter;
pub use response::AdapterResponse;
pub use typed_adapter::TypedBaseAdapter;

// Exposing structs for testing
pub use adapter_engine::AdapterEngine as SqlEngineForTesting;
pub use dbt_auth::AdapterConfig as AdapterConfigForTesting;
pub use postgres::adapter::PostgresAdapter as PostgresAdapterForTesting;
pub use snowflake::adapter::SnowflakeAdapter as SnowflakeAdapterForTesting;
