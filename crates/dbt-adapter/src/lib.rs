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

/// Parse a profile/model config value as a boolean, accepting
/// `"true"`/`"false"` in any casing. Returns `Ok(None)` for missing
/// keys (`s == None`); returns a `Configuration` error for values that
/// are neither `true` nor `false` ignoring case.
///
/// Exists because dbt-core users may write boolean config values
/// either as YAML literals (`ra3_node: true`) or as quoted strings in
/// any casing (`ra3_node: "True"`); `bool::from_str` only accepts
/// lowercase and would reject the latter. Whitespace is not trimmed —
/// callers needing that must do it themselves.
pub(crate) fn try_parse_bool_str(s: Option<&str>, key: &str) -> AdapterResult<Option<bool>> {
    use dbt_common::error::{AdapterError, AdapterErrorKind};
    match s {
        None => Ok(None),
        Some(v) if v.eq_ignore_ascii_case("true") => Ok(Some(true)),
        Some(v) if v.eq_ignore_ascii_case("false") => Ok(Some(false)),
        Some(_) => Err(AdapterError::new(
            AdapterErrorKind::Configuration,
            format!(r#"Failed to parse {key}, expected "true" or "false""#),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_common::error::AdapterErrorKind;

    #[test]
    fn missing_input_yields_none() {
        assert_eq!(try_parse_bool_str(None, "ra3_node").unwrap(), None);
    }

    #[test]
    fn parses_true_in_any_casing() {
        for s in ["true", "True", "TRUE", "tRuE"] {
            assert_eq!(try_parse_bool_str(Some(s), "k").unwrap(), Some(true));
        }
    }

    #[test]
    fn parses_false_in_any_casing() {
        for s in ["false", "False", "FALSE", "fAlSe"] {
            assert_eq!(try_parse_bool_str(Some(s), "k").unwrap(), Some(false));
        }
    }

    #[test]
    fn unparseable_yields_configuration_error_naming_the_key() {
        let err = try_parse_bool_str(Some("yes"), "ra3_node").unwrap_err();
        assert_eq!(err.kind(), AdapterErrorKind::Configuration);
        assert!(
            err.to_string().contains("ra3_node"),
            "error should name the failing key, got: {err}"
        );
    }

    #[test]
    fn does_not_trim_whitespace() {
        assert!(try_parse_bool_str(Some(" true"), "k").is_err());
        assert!(try_parse_bool_str(Some("true "), "k").is_err());
    }
}
