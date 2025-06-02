use crate::adapters::bigquery::relation::BigqueryRelationType;
use crate::adapters::databricks::relation::DatabricksRelationType;
use crate::adapters::postgres::relation::PostgresRelationType;
use crate::adapters::redshift::relation::RedshiftRelationType;
use crate::adapters::relation_object::StaticBaseRelation;
use crate::adapters::snowflake::relation::SnowflakeRelationType;
use crate::adapters::AdapterType;

use minijinja::Value;

/// Create a static relation value from an adapter type
/// To be used as api.Relation in the Jinja environment
pub fn create_static_relation(adapter_type: AdapterType) -> Option<Value> {
    let result = match adapter_type {
        AdapterType::Snowflake => &SnowflakeRelationType as &dyn StaticBaseRelation,
        AdapterType::Postgres => &PostgresRelationType as &dyn StaticBaseRelation,
        AdapterType::Bigquery => &BigqueryRelationType as &dyn StaticBaseRelation,
        AdapterType::Databricks => &DatabricksRelationType as &dyn StaticBaseRelation,
        AdapterType::Redshift => &RedshiftRelationType as &dyn StaticBaseRelation,
        _ => unimplemented!("{} doesn't support relation types", adapter_type),
    };
    Some(Value::from_object(result))
}
