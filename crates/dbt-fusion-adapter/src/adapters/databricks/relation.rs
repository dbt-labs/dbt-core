use dbt_schemas::schemas::relations::base::{
    BaseRelation, BaseRelationProperties, Policy, RelationPath, StaticBaseRelation,
};

use arrow::array::RecordBatch;
use dbt_adapter_proc_macros::{BaseRelationObject, StaticBaseRelationObject};
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::common::ResolvedQuoting;
use minijinja::value::Enumerator;
use minijinja::{Error as MinijinjaError, State, Value};

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Default databricks database
pub const DEFAULT_DATABRICKS_DATABASE: &str = "hive_metastore";

/// A struct representing the relation type for use with static methods
#[derive(Clone, Debug, StaticBaseRelationObject)]
pub struct DatabricksRelationType;

impl StaticBaseRelation for DatabricksRelationType {
    fn try_new(
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        custom_quoting: ResolvedQuoting,
    ) -> Result<Value, MinijinjaError> {
        Ok(Value::from_object(DatabricksRelation::new(
            database,
            schema,
            identifier,
            relation_type,
            // api.Relation.create doesn't set everything below
            None,
            custom_quoting,
            None,
            false,
        )))
    }

    fn get_adapter_type() -> String {
        "databricks".to_string()
    }
}

/// A relation object for the adapter
#[derive(Clone, Debug, BaseRelationObject)]
pub struct DatabricksRelation {
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
    /// Metadata about the relation
    pub metadata: Option<BTreeMap<String, String>>,
    /// Whether the relation is a delta table
    pub is_delta: bool,
}

impl BaseRelationProperties for DatabricksRelation {
    fn include_policy(&self) -> Policy {
        self.include_policy
    }

    fn quote_policy(&self) -> Policy {
        self.quote_policy
    }

    /// See [reference](https://github.com/databricks/dbt-databricks/blob/822b105b15e644676d9e1f47cbfd765cd4c1541f/dbt/adapters/databricks/relation.py#L64)
    fn quote_character(&self) -> char {
        '`'
    }
}

impl DatabricksRelation {
    /// Creates a new relation
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        native_schema: Option<RecordBatch>,
        custom_quoting: ResolvedQuoting,
        metadata: Option<BTreeMap<String, String>>,
        is_delta: bool,
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
            metadata,
            is_delta,
        }
    }

    /// Create a new relation with a policy
    pub fn new_with_policy(
        path: RelationPath,
        relation_type: Option<RelationType>,
        include_policy: Policy,
        quote_policy: Policy,
        metadata: Option<BTreeMap<String, String>>,
        is_delta: bool,
    ) -> Self {
        Self {
            path,
            relation_type,
            include_policy,
            quote_policy,
            native_schema: None,
            metadata,
            is_delta,
        }
    }
}

impl BaseRelation for DatabricksRelation {
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

    fn relation_type(&self) -> Option<RelationType> {
        self.relation_type.clone()
    }

    fn as_value(&self) -> Value {
        Value::from_object(self.clone())
    }

    fn adapter_type(&self) -> Option<String> {
        Some("databricks".to_string())
    }

    fn include_inner(&self, policy: Policy) -> Result<Value, MinijinjaError> {
        let relation = Self::new_with_policy(
            self.path.clone(),
            self.relation_type.clone(),
            policy,
            self.quote_policy,
            self.metadata.clone(),
            self.is_delta,
        );

        Ok(relation.as_value())
    }

    fn is_hive_metastore(&self) -> Value {
        let result = self.path.database.is_none()
            || self.path.database.as_ref().map(|s| s.to_lowercase())
                == Some(DEFAULT_DATABRICKS_DATABASE.to_string());

        Value::from(result)
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
        Ok(Arc::new(DatabricksRelation::new(
            Some(database),
            Some(schema),
            identifier,
            relation_type,
            None,
            custom_quoting,
            None,
            false,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_schemas::{dbt_types::RelationType, schemas::relations::DEFAULT_RESOLVED_QUOTING};

    #[test]
    fn test_try_new_via_static_base_relation() {
        let relation = DatabricksRelationType::try_new(
            Some("d".to_string()),
            Some("s".to_string()),
            Some("i".to_string()),
            Some(RelationType::Table),
            DEFAULT_RESOLVED_QUOTING,
        )
        .unwrap();

        let relation = relation.downcast_object::<DatabricksRelation>().unwrap();
        assert_eq!(
            relation.render_self().unwrap().as_str().unwrap(),
            "`d`.`s`.`i`"
        );
        assert_eq!(relation.relation_type().unwrap(), RelationType::Table);
    }

    #[test]
    fn test_try_new_via_static_base_relation_with_default_database() {
        let relation = DatabricksRelationType::try_new(
            None,
            Some("s".to_string()),
            Some("i".to_string()),
            Some(RelationType::Table),
            DEFAULT_RESOLVED_QUOTING,
        )
        .unwrap();

        let relation = relation.downcast_object::<DatabricksRelation>().unwrap();
        assert_eq!(relation.render_self().unwrap().as_str().unwrap(), "`s`.`i`");
    }
}
