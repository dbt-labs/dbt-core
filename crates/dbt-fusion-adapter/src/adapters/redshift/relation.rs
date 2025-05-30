use arrow::array::RecordBatch;
use dbt_adapter_proc_macros::{BaseRelationObject, StaticBaseRelationObject};
use dbt_common::current_function_name;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::relations::base::{
    BaseRelation, BaseRelationProperties, Policy, RelationPath, StaticBaseRelation,
};
use minijinja::arg_utils::{check_num_args, ArgParser};
use minijinja::value::Enumerator;
use minijinja::{Error as MinijinjaError, State, Value};

use std::any::Any;
use std::sync::Arc;

const MAX_CHARACTERS_IN_IDENTIFIER: u32 = 127;

/// A struct representing the relation type for use with static methods
#[derive(Clone, Debug, StaticBaseRelationObject)]
pub struct RedshiftRelationType;

impl StaticBaseRelation for RedshiftRelationType {
    fn try_new(
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        custom_quoting: ResolvedQuoting,
    ) -> Result<Value, MinijinjaError> {
        Ok(Value::from_object(RedshiftRelation::new(
            database,
            schema,
            identifier,
            relation_type,
            None,
            custom_quoting,
        )))
    }

    fn get_adapter_type() -> String {
        "redshift".to_string()
    }
}

/// A relation object for the adapter
#[derive(Clone, Debug, BaseRelationObject)]
pub struct RedshiftRelation {
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

impl BaseRelationProperties for RedshiftRelation {
    fn include_policy(&self) -> Policy {
        self.include_policy
    }

    fn quote_policy(&self) -> Policy {
        self.quote_policy
    }

    fn quote_character(&self) -> char {
        '"'
    }
}

impl RedshiftRelation {
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
            include_policy: Policy::enabled(),
            quote_policy: custom_quoting,
            native_schema,
        }
    }

    pub fn new_with_policy(
        path: RelationPath,
        relation_type: Option<RelationType>,
        include_policy: Policy,
    ) -> Self {
        Self {
            path,
            relation_type,
            include_policy,
            native_schema: None,
            quote_policy: Policy::enabled(),
        }
    }
}

impl BaseRelation for RedshiftRelation {
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
        Some("redshift".to_string())
    }

    fn include_inner(&self, policy: Policy) -> Result<Value, MinijinjaError> {
        let relation = Self::new_with_policy(self.path.clone(), self.relation_type.clone(), policy);

        Ok(relation.as_value())
    }

    fn relation_max_name_length(&self, args: &[Value]) -> Result<Value, MinijinjaError> {
        let args = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &args, 0, 0)?;
        Ok(Value::from(MAX_CHARACTERS_IN_IDENTIFIER))
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
        Ok(Arc::new(RedshiftRelation::new(
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
        let relation = RedshiftRelationType::try_new(
            Some("d".to_string()),
            Some("s".to_string()),
            Some("i".to_string()),
            Some(RelationType::Table),
            DEFAULT_RESOLVED_QUOTING,
        )
        .unwrap();

        let relation = relation.downcast_object::<RedshiftRelation>().unwrap();
        assert_eq!(
            relation.render_self().unwrap().as_str().unwrap(),
            "\"d\".\"s\".\"i\""
        );
        assert_eq!(relation.relation_type().unwrap(), RelationType::Table);
    }
}
