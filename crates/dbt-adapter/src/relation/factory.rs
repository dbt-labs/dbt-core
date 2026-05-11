use std::sync::Arc;

use dbt_adapter_core::AdapterType;

use crate::relation::RelationStatic;
use crate::relation::StaticBaseRelationObject;
use crate::relation::bigquery::BigqueryRelationType;
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
        Bigquery => {
            let bigquery_relation_type = BigqueryRelationType(quoting);
            StaticBaseRelationObject::new(Arc::new(bigquery_relation_type))
        }
        Databricks | Spark | Fabric | DuckDB | Exasol | Postgres | Redshift | Salesforce => {
            let relation_type = RelationStatic {
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
