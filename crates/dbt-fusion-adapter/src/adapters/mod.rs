// In this section of the file nothing should be `pub`.
pub mod auth;
pub mod base_adapter;
pub mod bridge_adapter;
pub mod config;
pub mod convert_type;
pub mod errors;
pub mod factory;
pub mod funcs;
pub mod information_schema;
pub mod query_ctx;
pub mod record_and_replay;
pub mod render_constraint;
pub mod response;
pub mod snapshots;
pub mod sql_engine;
pub mod typed_adapter;

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
/// Record batch utils
pub mod record_batch_utils;
/// Redshift adapter
pub mod redshift;
/// Snowflake adapter
pub mod snowflake;

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
pub use config::AdapterConfig as AdapterConfigForTesting;
pub use sql_engine::SqlEngine as SqlEngineForTesting;
