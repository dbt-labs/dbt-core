use arrow_schema::SchemaRef;
use dbt_frontend_common::{FullyQualifiedName, dialect::Dialect};
use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display, EnumString};

#[derive(Clone)]
pub struct SchemaRegistryEntry {
    /// The original schema from the source (e.g. the database driver).
    pub original: Option<SchemaRef>,
    /// The schema after SDF-specific transformations or a schema produced
    /// by static analysis.
    pub schema: SchemaRef,
}

impl SchemaRegistryEntry {
    pub fn new(original: Option<SchemaRef>, schema: SchemaRef) -> Self {
        Self { original, schema }
    }
}

/// Schema registry access interface.
pub trait SchemaRegistry: Send + Sync {
    /// Get the schema of a table by its unique identifier.
    fn get_schema_by_unique_id(&self, unique_id: &str) -> Option<SchemaRegistryEntry>;

    /// Get the schema of a table by its fully-qualified name (FQN).
    fn get_schema(&self, fqn: &FullyQualifiedName) -> Option<SchemaRegistryEntry>;
}

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
}

impl From<AdapterType> for Dialect {
    fn from(value: AdapterType) -> Self {
        match value {
            AdapterType::Postgres => Dialect::Postgresql,
            AdapterType::Snowflake => Dialect::Snowflake,
            AdapterType::Bigquery => Dialect::Bigquery,
            AdapterType::Databricks => Dialect::Databricks,
            AdapterType::Redshift => Dialect::Redshift,
            // Salesforce dialect is unclear, it claims ANSI vaguely
            // https://developer.salesforce.com/docs/data/data-cloud-query-guide/references/data-cloud-query-api-reference/c360a-api-query-v2-call-overview.html
            // falls back to Postgresql at the moment
            AdapterType::Salesforce => Dialect::Postgresql,
        }
    }
}
