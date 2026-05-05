use std::sync::Arc;

use dbt_adapter_core::AdapterType;

use crate::relation::StaticBaseRelationObject;
use crate::relation::bigquery::BigqueryRelationType;
use crate::relation::databricks::GenericRelationType;
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
    use AdapterType::*;
    let result = match adapter_type {
        Snowflake => {
            let snowflake_relation_type = SnowflakeRelationType(quoting);
            StaticBaseRelationObject::new(Arc::new(snowflake_relation_type))
        }
        Postgres => {
            let postgres_relation_type = PostgresRelationType(quoting);
            StaticBaseRelationObject::new(Arc::new(postgres_relation_type))
        }
        Bigquery => {
            let bigquery_relation_type = BigqueryRelationType(quoting);
            StaticBaseRelationObject::new(Arc::new(bigquery_relation_type))
        }
        Redshift => {
            let redshift_relation_type = RedshiftRelationType(quoting);
            StaticBaseRelationObject::new(Arc::new(redshift_relation_type))
        }
        Salesforce => {
            let salesforce_relation_type = SalesforceRelationType(quoting);
            StaticBaseRelationObject::new(Arc::new(salesforce_relation_type))
        }
        Databricks | Spark | Fabric | DuckDB | Exasol => {
            let relation_type = GenericRelationType {
                adapter_type,
                quoting,
            };
            StaticBaseRelationObject::new(Arc::new(relation_type))
        }
        ClickHouse => todo!("ClickHouse"),
        Starburst => todo!("Starburst"),
        Athena => todo!("Athena"),
        Trino => todo!("Trino"),
        Dremio => todo!("Dremio"),
        Oracle => todo!("Oracle"),
        Datafusion => todo!("Datafusion"),
    };
    Some(Value::from_object(result))
}
