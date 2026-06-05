//! Public types for view-definition fetching.
//!
//! `ViewDefinition` is the per-view payload that the adapter's
//! `fetch_view_definitions` returns. Recursive traversal that parses these
//! definitions to discover upstream references is performed by callers.

use dbt_frontend_common::Dialect;

/// A single fetched view definition.
#[derive(Debug, Clone)]
pub struct ViewDefinition {
    /// Fully-qualified, quoted name of the view.
    pub fqn: String,

    /// Verbatim DDL as returned by the adapter. For Snowflake this is the
    /// output of `GET_DDL('VIEW', <fqn>)` (or the user-supplied override).
    pub definition: String,

    /// SQL dialect to use when parsing `definition`.
    pub dialect: Dialect,

    /// Catalog used to qualify any unqualified references inside `definition`.
    pub default_catalog: String,

    /// Schema used to qualify any unqualified references inside `definition`.
    pub default_schema: String,
}
