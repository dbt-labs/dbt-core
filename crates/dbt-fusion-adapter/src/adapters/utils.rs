use std::sync::Arc;

use dbt_schemas::{
    dbt_types::RelationType,
    schemas::{
        common::ResolvedQuoting,
        relations::base::{BaseRelation, TableFormat},
    },
};
use minijinja::Error as MinijinjaError;

use super::{
    bigquery::relation::BigqueryRelation, databricks::relation::DatabricksRelation,
    postgres::relation::PostgresRelation, redshift::relation::RedshiftRelation,
    snowflake::relation::SnowflakeRelation,
};

/// Creates a relation based on the adapter type
pub fn create_relation(
    adapter_type: String,
    database: String,
    schema: String,
    identifier: Option<String>,
    relation_type: Option<RelationType>,
    custom_quoting: ResolvedQuoting,
) -> Result<Arc<dyn BaseRelation>, MinijinjaError> {
    let relation = match adapter_type.to_lowercase().as_str() {
        "postgres" => Arc::new(PostgresRelation::try_new(
            Some(database),
            Some(schema),
            identifier,
            relation_type,
            custom_quoting,
        )?) as Arc<dyn BaseRelation>,
        "snowflake" => Arc::new(SnowflakeRelation::new(
            Some(database),
            Some(schema),
            identifier,
            relation_type,
            TableFormat::Default,
            custom_quoting,
        )) as Arc<dyn BaseRelation>,
        "bigquery" => Arc::new(BigqueryRelation::new(
            Some(database),
            Some(schema),
            identifier,
            relation_type,
            None,
            custom_quoting,
        )) as Arc<dyn BaseRelation>,
        "redshift" => Arc::new(RedshiftRelation::new(
            Some(database),
            Some(schema),
            identifier,
            relation_type,
            None,
            custom_quoting,
        )) as Arc<dyn BaseRelation>,
        "databricks" => Arc::new(DatabricksRelation::new(
            Some(database),
            Some(schema),
            identifier,
            relation_type,
            None,
            custom_quoting,
            None,
            false,
        )) as Arc<dyn BaseRelation>,
        _ => panic!("not supported"),
    };
    Ok(relation)
}
