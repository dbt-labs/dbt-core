use arrow::array::RecordBatch;
use dbt_adapter_proc_macros::{BaseRelationObject, StaticBaseRelationObject};
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::relations::base::{
    BaseRelation, BaseRelationProperties, Policy, RelationPath, StaticBaseRelation,
};
use minijinja::value::Enumerator;
use minijinja::{Error as MinijinjaError, State, Value};

use std::any::Any;
use std::sync::Arc;

/// A struct representing the relation type for use with static methods
#[derive(Clone, Debug, StaticBaseRelationObject)]
pub struct BigqueryRelationType;

impl StaticBaseRelation for BigqueryRelationType {
    fn try_new(
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        custom_quoting: ResolvedQuoting,
    ) -> Result<Value, MinijinjaError> {
        Ok(Value::from_object(BigqueryRelation::new(
            database,
            schema,
            identifier,
            relation_type,
            None,
            custom_quoting,
        )))
    }

    fn get_adapter_type() -> String {
        "bigquery".to_string()
    }
}

/// A relation object for bigquery adapter
#[derive(Clone, Debug, BaseRelationObject)]
pub struct BigqueryRelation {
    /// The path of the relation
    pub path: RelationPath,
    /// The relation type (default: None)
    pub relation_type: Option<RelationType>,
    /// Include policy
    pub include_policy: Policy,
    /// Quote policy
    pub quote_policy: Policy,
    /// The actual schema of the relation we got from db
    #[allow(dead_code)]
    pub native_schema: Option<RecordBatch>,
}

impl BaseRelationProperties for BigqueryRelation {
    fn include_policy(&self) -> Policy {
        self.include_policy
    }

    fn quote_policy(&self) -> Policy {
        self.quote_policy
    }

    /// See [reference](https://github.com/dbt-labs/dbt-adapters/blob/2a94cc75dba1f98fa5caff1f396f5af7ee444598/dbt-bigquery/src/dbt/adapters/bigquery/relation.py#L30)
    fn quote_character(&self) -> char {
        '`'
    }
}

impl BigqueryRelation {
    /// Creates a new relation
    pub fn new(
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        native_schema: Option<RecordBatch>,
        custom_quoting: ResolvedQuoting,
    ) -> Self {
        Self {
            path: RelationPath {
                database,
                schema,
                identifier,
            },
            relation_type,
            include_policy: Policy::trues(),
            quote_policy: custom_quoting,
            native_schema,
        }
    }

    /// Create a new relation with a policy
    pub fn new_with_policy(
        path: RelationPath,
        relation_type: Option<RelationType>,
        include_policy: Policy,
        quote_policy: Policy,
    ) -> Self {
        Self {
            path,
            relation_type,
            include_policy,
            native_schema: None,
            quote_policy,
        }
    }
}

impl BaseRelation for BigqueryRelation {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create_from(&self, _: &State, _: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!()
    }

    fn database(&self) -> Value {
        Value::from(self.path.database.clone())
    }

    fn schema(&self) -> Value {
        Value::from(self.path.schema.clone())
    }

    fn identifier(&self) -> Value {
        Value::from(self.path.identifier.clone())
    }

    fn quoted(&self, s: &str) -> String {
        format!("`{}`", s)
    }

    fn relation_type(&self) -> Option<RelationType> {
        self.relation_type.clone()
    }

    /// Helper: is this relation renamable?
    fn can_be_renamed(&self) -> bool {
        matches!(self.relation_type(), Some(RelationType::Table))
    }

    fn as_value(&self) -> Value {
        Value::from_object(self.clone())
    }

    fn adapter_type(&self) -> Option<String> {
        Some("bigquery".to_string())
    }

    fn include_inner(&self, policy: Policy) -> Result<Value, MinijinjaError> {
        let relation = Self::new_with_policy(
            self.path.clone(),
            self.relation_type.clone(),
            policy,
            self.quote_policy,
        );

        Ok(relation.as_value())
    }

    fn normalize_component(&self, component: &str) -> String {
        component.to_lowercase()
    }

    fn create_relation(
        &self,
        database: String,
        schema: String,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        custom_quoting: Policy,
    ) -> Result<Arc<dyn BaseRelation>, MinijinjaError> {
        Ok(Arc::new(BigqueryRelation::new(
            Some(database),
            Some(schema),
            identifier,
            relation_type,
            None,
            custom_quoting,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_schemas::{dbt_types::RelationType, schemas::relations::DEFAULT_RESOLVED_QUOTING};

    #[test]
    fn test_try_new_via_static_base_relation() {
        let relation = BigqueryRelationType::try_new(
            Some("d".to_string()),
            Some("s".to_string()),
            Some("i".to_string()),
            Some(RelationType::Table),
            DEFAULT_RESOLVED_QUOTING,
        )
        .unwrap();

        let relation = relation.downcast_object::<BigqueryRelation>().unwrap();
        assert_eq!(
            relation.render_self().unwrap().as_str().unwrap(),
            "`d`.`s`.`i`"
        );
        assert_eq!(relation.relation_type().unwrap(), RelationType::Table);
    }
}
