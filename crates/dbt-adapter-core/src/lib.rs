use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display, EnumIter, EnumString, IntoEnumIterator, IntoStaticStr};

/// The type of the adapter.
///
/// Used to identify the specific database adapter being used.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Display,
    AsRefStr,
    EnumIter,
    EnumString,
    IntoStaticStr,
    Deserialize,
    Serialize,
)]
#[strum(serialize_all = "lowercase", ascii_case_insensitive)]
#[serde(rename_all = "lowercase")]
pub enum AdapterType {
    /// Snowflake
    Snowflake,
    /// Bigquery
    Bigquery,
    /// Databricks
    Databricks,
    /// Redshift
    Redshift,
    /// Spark
    Spark,
    /// DuckDB
    DuckDB,
    /// Postgres
    #[strum(to_string = "postgres", serialize = "postgresql")]
    Postgres,
    /// Salesforce
    Salesforce,
    // Microsoft Fabric DWH
    Fabric,
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

impl AdapterType {
    /// Returns an iterator of `(AdapterType, &'static str)` pairs.
    ///
    /// The string is the lowercased name of the variant. `Postgres` is
    /// rendered as `"postgresql"`.
    pub fn iter_with_names() -> impl Iterator<Item = (AdapterType, &'static str)> {
        Self::iter().map(|v| {
            let name: &'static str = match v {
                AdapterType::Postgres => "postgresql",
                _ => v.into(),
            };
            (v, name)
        })
    }
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

pub const NON_EXPERIMENTAL_ADAPTERS: &[AdapterType] = &[
    AdapterType::Snowflake,
    AdapterType::Bigquery,
    AdapterType::Databricks,
    AdapterType::Redshift,
    AdapterType::DuckDB,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_str() {
        let cases = [
            ("pOstgres", AdapterType::Postgres),
            ("pOstgresql", AdapterType::Postgres),
            ("sNowflake", AdapterType::Snowflake),
            ("bIgquery", AdapterType::Bigquery),
            ("dAtabricks", AdapterType::Databricks),
            ("rEdshift", AdapterType::Redshift),
            ("sAlesforce", AdapterType::Salesforce),
            ("sPark", AdapterType::Spark),
            ("dUckdb", AdapterType::DuckDB),
            ("fAbric", AdapterType::Fabric),
            ("cLickhouse", AdapterType::ClickHouse),
            ("aThena", AdapterType::Athena),
            ("sTarburst", AdapterType::Starburst),
            ("tRino", AdapterType::Trino),
            ("dAtafusion", AdapterType::Datafusion),
        ];
        for (input, expected) in cases {
            let res = input.parse::<AdapterType>();
            match res {
                Ok(parsed) => assert_eq!(parsed, expected),
                Err(e) => panic!("Failed to parse '{}': {}", input, e),
            }
        }
    }

    #[test]
    fn test_postgres_string_representations() {
        let pg = AdapterType::Postgres;
        // Display/AsRef/IntoStaticStr must all return "postgres" — not "postgresql".
        // Dispatch, materialization resolution, and internal packages all depend on
        // the adapter name being "postgres". "postgresql" is only a parse alias
        // (handled by EnumString via the extra serialize attribute).
        assert_eq!(pg.to_string(), "postgres");
        assert_eq!(pg.as_ref(), "postgres");
        let s: &'static str = pg.into();
        assert_eq!(s, "postgres");
    }

    #[test]
    fn test_iter_with_names() {
        let entries: Vec<_> = AdapterType::iter_with_names().take(7).collect();
        assert_eq!(
            entries,
            vec![
                (AdapterType::Snowflake, "snowflake"),
                (AdapterType::Bigquery, "bigquery"),
                (AdapterType::Databricks, "databricks"),
                (AdapterType::Redshift, "redshift"),
                (AdapterType::Spark, "spark"),
                (AdapterType::DuckDB, "duckdb"),
                (AdapterType::Postgres, "postgresql"),
            ]
        );
    }
}
