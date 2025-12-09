use std::sync::Arc;

use crate::AdapterType;
use crate::relation::StaticBaseRelationObject;
use crate::relation::bigquery::BigqueryRelationType;
use crate::relation::databricks::DatabricksRelationType;
use crate::relation::postgres::PostgresRelationType;
use crate::relation::redshift::RedshiftRelationType;
use crate::relation::salesforce::SalesforceRelationType;
use crate::relation::snowflake::SnowflakeRelationType;

use dbt_schemas::schemas::common::ResolvedQuoting;
use minijinja::Value;

/// Create a static relation value from an adapter type
/// To be used as api.Relation in the Jinja environment
pub fn create_static_relation(
    adapter_type: AdapterType,
    quoting: ResolvedQuoting,
) -> Option<Value> {
    let result = match adapter_type {
        AdapterType::Snowflake => {
            let snowflake_relation_type = SnowflakeRelationType(quoting);
            StaticBaseRelationObject::new(Arc::new(snowflake_relation_type))
        }
        AdapterType::Postgres => {
            let postgres_relation_type = PostgresRelationType(quoting);
            StaticBaseRelationObject::new(Arc::new(postgres_relation_type))
        }
        AdapterType::Bigquery => {
            let bigquery_relation_type = BigqueryRelationType(quoting);
            StaticBaseRelationObject::new(Arc::new(bigquery_relation_type))
        }
        AdapterType::Databricks => {
            let databricks_relation_type = DatabricksRelationType(quoting);
            StaticBaseRelationObject::new(Arc::new(databricks_relation_type))
        }
        AdapterType::Redshift => {
            let redshift_relation_type = RedshiftRelationType(quoting);
            StaticBaseRelationObject::new(Arc::new(redshift_relation_type))
        }
        AdapterType::Salesforce => {
            let salesforce_relation_type = SalesforceRelationType(quoting);
            StaticBaseRelationObject::new(Arc::new(salesforce_relation_type))
        }
    };
    Some(Value::from_object(result))
}
