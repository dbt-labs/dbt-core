use std::fmt;

use serde::{Deserialize, Serialize};

/// Enum representing different types of relations.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RelationType {
    /// An enum for table relations.
    Table,
    /// An enum for view relations.
    View,
    /// An enum for CTE relations.
    CTE,
    /// An enum for materialized view relations.
    MaterializedView,
    /// An enum for ephemeral relations.
    Ephemeral,
    /// An enum for any relation that dbt is aware of.
    // Note (copied from dbt-adapters): this is a "catch all" that is better than `None` == external to anything dbt is aware of
    External,
    /// An enum for pointer table
    PointerTable,
    /// An enum for a dynamic table (snowflake only)
    DynamicTable,
}

impl RelationType {
    /// Convert BigQuery table type to dbt RelationType
    ///
    /// https://cloud.google.com/bigquery/docs/information-schema-tables
    pub fn from_bigquery_table_type(table_type: &str) -> Self {
        match table_type {
            "BASE TABLE" | "CLONE" | "SNAPSHOT" => RelationType::Table,
            "VIEW" => RelationType::View,
            "MATERIALIZED VIEW" => RelationType::MaterializedView,
            "EXTERNAL" => RelationType::External,
            _ => panic!("unknown table type: {}", table_type),
        }
    }
}

// Implement Display so that we can easily get a string representation.
impl fmt::Display for RelationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            RelationType::Table => "table",
            RelationType::CTE => "cte",
            RelationType::View => "view",
            RelationType::External => "external",
            RelationType::MaterializedView => "materialized_view",
            RelationType::Ephemeral => "ephemeral",
            RelationType::PointerTable => "pointer_table",
            RelationType::DynamicTable => "dynamic_table",
        };
        write!(f, "{}", s)
    }
}

impl From<&str> for RelationType {
    fn from(s: &str) -> Self {
        match s {
            "table" => RelationType::Table,
            "view" => RelationType::View,
            "cte" => RelationType::CTE,
            "materialized_view" => RelationType::MaterializedView,
            "ephemeral" => RelationType::Ephemeral,
            "external" => RelationType::External,
            "dynamic_table" => RelationType::DynamicTable,
            _ => panic!("Invalid relation type: {}", s),
        }
    }
}
