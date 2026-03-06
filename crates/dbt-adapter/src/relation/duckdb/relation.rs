use crate::relation::postgres::PostgresRelation;
use crate::relation::{RelationObject, StaticBaseRelation};

use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::relations::base::{Policy, RelationPath};
use minijinja::Value;

use std::sync::Arc;

/// DuckDB local file databases use 2-part names (`schema.table`) without
/// catalog prefix, but attached catalogs (including MotherDuck aliases)
/// require fully qualified names.
#[derive(Clone, Debug)]
pub struct DuckDBRelationType(pub ResolvedQuoting);

impl StaticBaseRelation for DuckDBRelationType {
    fn try_new(
        &self,
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        custom_quoting: Option<ResolvedQuoting>,
        _temporary: Option<bool>,
    ) -> Result<Value, minijinja::Error> {
        // Keep api.Relation.create backwards-compatible for DuckDB:
        // render as schema.identifier regardless of provided database.
        let include_policy = Policy::new(false, true, true);
        Ok(
            RelationObject::new(Arc::new(PostgresRelation::try_new_with_policy(
                RelationPath {
                    database: database.filter(|s| !s.is_empty()),
                    schema,
                    identifier,
                },
                relation_type,
                include_policy,
                custom_quoting.unwrap_or(self.0),
            )?))
            .into_value(),
        )
    }

    fn get_adapter_type(&self) -> String {
        "duckdb".to_string()
    }
}
