use std::sync::Arc;

use arrow_schema::Schema;
use dbt_frontend_common::dialect::Dialect;
use strum::{AsRefStr, Display, EnumString};

/// Schema registry access interface.
pub trait SchemaRegistry: Send + Sync {
    /// Get the schema of a table by its unique identifier.
    fn get_schema(&self, unique_id: &str) -> Option<Arc<Schema>>;

    /// Get the schema of a table by its fully-qualified name (FQN).
    fn get_schema_by_fqn(&self, fqn: &str) -> Option<Arc<Schema>>;
}

/// The type of the adapter.
///
/// Used to identify the specific database adapter being used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, AsRefStr, EnumString)]
#[strum(serialize_all = "lowercase", ascii_case_insensitive)]
pub enum AdapterType {
    /// Adapter used in parse phase
    Parse,
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
}

impl From<AdapterType> for Dialect {
    fn from(value: AdapterType) -> Self {
        match value {
            AdapterType::Postgres => Dialect::Postgresql,
            AdapterType::Snowflake => Dialect::Snowflake,
            AdapterType::Bigquery => Dialect::Bigquery,
            AdapterType::Databricks => Dialect::Databricks,
            AdapterType::Redshift => Dialect::Redshift,
            AdapterType::Parse => unimplemented!("Parse adapter type is not supported"),
        }
    }
}
