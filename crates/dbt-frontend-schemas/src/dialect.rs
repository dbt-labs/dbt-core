use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

#[derive(
    Copy, Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash,
)]
pub enum Dialect {
    #[serde(rename = "sdf")]
    Sdf,
    #[default]
    #[serde(rename = "trino")]
    #[serde(alias = "presto")]
    Trino,
    #[serde(rename = "snowflake")]
    Snowflake,
    #[serde(rename = "postgresql")]
    Postgresql,
    #[serde(rename = "bigquery")]
    Bigquery,
    #[serde(rename = "datafusion")]
    DataFusion,
    #[serde(rename = "sparksql")]
    SparkSql,
    #[serde(rename = "sparklp")]
    SparkLp,
    #[serde(rename = "redshift")]
    Redshift,
    #[serde(rename = "databricks")]
    Databricks,
}

impl Display for Dialect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Trino => write!(f, "trino"),
            Self::Snowflake => write!(f, "snowflake"),
            Self::Postgresql => write!(f, "postgresql"),
            Self::Bigquery => write!(f, "bigquery"),
            Self::DataFusion => write!(f, "datafusion"),
            Self::SparkSql => write!(f, "sparksql"),
            Self::SparkLp => write!(f, "sparklp"),
            Self::Redshift => write!(f, "redshift"),
            Self::Databricks => write!(f, "databricks"),
            Self::Sdf => write!(f, "sdf"),
        }
    }
}

impl FromStr for Dialect {
    type Err = Box<dyn std::error::Error>;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input.to_ascii_lowercase().as_str() {
            "trino" => Ok(Self::Trino),
            "snowflake" => Ok(Self::Snowflake),
            "postgresql" => Ok(Self::Postgresql),
            "bigquery" => Ok(Self::Bigquery),
            "datafusion" => Ok(Self::DataFusion),
            "sparksql" => Ok(Self::SparkSql),
            "sparklp" => Ok(Self::SparkLp),
            "redshift" => Ok(Self::Redshift),
            "databricks" => Ok(Self::Databricks),
            "sdf" => Ok(Self::Sdf),

            // "passthrough" adapter type is used to disable most local semantic
            // analysis, so we just map it to the default dialect.
            "passthrough" => Ok(Self::default()),

            _ => Err(format!("Invalid dialect value: '{}'", input).into()),
        }
    }
}
