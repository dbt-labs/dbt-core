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
    /// ClickHouse
    ClickHouse,
    /// Exasol
    Exasol,
    /// Athena
    Athena,
    /// Starburst
    Starburst,
    /// Trino
    Trino,
    /// Datafusion
    Datafusion,
    /// Dremio
    Dremio,
    /// Oracle
    Oracle,
}

pub fn quote_char(adapter_type: AdapterType) -> char {
    use AdapterType::*;
    match adapter_type {
        Snowflake => '"',
        // https://github.com/dbt-labs/dbt-adapters/blob/2a94cc75dba1f98fa5caff1f396f5af7ee444598/dbt-bigquery/src/dbt/adapters/bigquery/relation.py#L30
        Bigquery => '`',
        Databricks | Spark => '`',
        Redshift => '"',
        Postgres | Salesforce => '"',
        Fabric => '"',
        DuckDB => '"',
        Athena | Trino | Starburst => '"',
        Datafusion => '"',
        ClickHouse => '"',
        Exasol => '"',
        Sidecar => '"',
        Dremio => todo!("Dremio"),
        Oracle => todo!("Oracle"),
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

/// Returns whether the adapter supports concurrent execution of microbatch models.
///
/// This mirrors dbt-core's adapter capability for `Capability.MicrobatchConcurrency`.
pub fn adapter_type_supports_microbatch_concurrency(adapter_type: AdapterType) -> bool {
    matches!(adapter_type, AdapterType::Snowflake)
}
