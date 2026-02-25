use dbt_frontend_common::dialect::Dialect;
use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display, EnumString};

/// The type of the adapter.
///
/// Used to identify the specific database adapter being used.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Display, AsRefStr, EnumString, Deserialize, Serialize,
)]
#[strum(serialize_all = "lowercase", ascii_case_insensitive)]
#[serde(rename_all = "lowercase")]
pub enum AdapterType {
    /// Postgres
    Postgres,
    /// Snowflake
    Snowflake,
    /// Bigquery
    Bigquery,
    /// Databricks
    Databricks,
    /// Redshift
    Redshift,
    /// Salesforce
    Salesforce,
    /// Spark
    Spark,
    /// DuckDB
    DuckDB,
    // Microsoft Fabric DWH
    Fabric,
    /// Sidecar (internal dispatch type for DuckDB backend in sidecar mode)
    Sidecar,
}

pub fn dialect_of(adapter_type: AdapterType) -> Option<Dialect> {
    use AdapterType::*;
    let dialect = match adapter_type {
        Postgres => Dialect::Postgresql,
        Snowflake => Dialect::Snowflake,
        Bigquery => Dialect::Bigquery,
        // TODO(serramatutu): switch Spark to Spark dialect once frontend looks good
        Databricks | Spark => Dialect::Databricks,
        Redshift => Dialect::Redshift,
        // Salesforce dialect is unclear, it claims ANSI vaguely
        // https://developer.salesforce.com/docs/data/data-cloud-query-guide/references/data-cloud-query-api-reference/c360a-api-query-v2-call-overview.html
        // falls back to Postgresql at the moment
        Salesforce => Dialect::Postgresql,
        // DuckDB is Postgres-compatible, use Redshift dialect (also Postgres-based) for typing support
        DuckDB => Dialect::Redshift,
        // Sidecar uses DuckDB backend but should be treated as Postgres-like
        Sidecar => Dialect::Postgresql,
        _ => return None,
    };
    Some(dialect)
}

pub fn quote_char(adapter_type: AdapterType) -> char {
    match dialect_of(adapter_type) {
        Some(dialect) => dialect.quote_char(),
        None => match adapter_type {
            AdapterType::Fabric => '"',
            AdapterType::DuckDB => '"',
            _ => unimplemented!("quote_char() is not defined for {adapter_type}"),
        },
    }
}

pub const DBT_EXECUTION_PHASE_RENDER: &str = "render";
pub const DBT_EXECUTION_PHASE_ANALYZE: &str = "analyze";
pub const DBT_EXECUTION_PHASE_RUN: &str = "run";

pub const DBT_EXECUTION_PHASES: [&str; 3] = [
    DBT_EXECUTION_PHASE_RENDER,
    DBT_EXECUTION_PHASE_ANALYZE,
    DBT_EXECUTION_PHASE_RUN,
];

#[derive(Clone, Copy, Debug)]
pub enum ExecutionPhase {
    Render,
    Analyze,
    Run,
}

impl ExecutionPhase {
    pub const fn as_str(&self) -> &'static str {
        match self {
            ExecutionPhase::Render => DBT_EXECUTION_PHASE_RENDER,
            ExecutionPhase::Analyze => DBT_EXECUTION_PHASE_ANALYZE,
            ExecutionPhase::Run => DBT_EXECUTION_PHASE_RUN,
        }
    }
}

pub fn adapter_type_supports_static_analysis(adapter_type: AdapterType) -> bool {
    matches!(
        adapter_type,
        AdapterType::Snowflake
            | AdapterType::Bigquery
            | AdapterType::Redshift
            | AdapterType::Databricks
            | AdapterType::Spark
            | AdapterType::DuckDB
    )
}
