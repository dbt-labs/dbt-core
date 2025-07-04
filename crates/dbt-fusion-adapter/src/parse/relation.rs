use crate::funcs::{empty_string_value, none_value};
use crate::relation_object::RelationObject;

use dbt_common::FsResult;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::relations::base::{BaseRelation, BaseRelationProperties, Policy};
use minijinja::{Error as MinijinjaError, State, Value};

use std::any::Any;
use std::sync::Arc;

/// Empty relation
///
/// A relation that returns empty values for all fields.
#[derive(Clone, Debug, Default)]
pub struct EmptyRelation {}

impl BaseRelationProperties for EmptyRelation {
    fn include_policy(&self) -> Policy {
        unimplemented!("include policy is unavailable for EmptyRelation")
    }

    fn quote_policy(&self) -> Policy {
        unimplemented!("quote policy is unavailable for EmptyRelation")
    }

    fn quote_character(&self) -> char {
        unimplemented!("quote character is unavailable for EmptyRelation")
    }

    fn get_database(&self) -> FsResult<String> {
        Ok(String::new())
    }

    fn get_schema(&self) -> FsResult<String> {
        Ok(String::new())
    }

    fn get_identifier(&self) -> FsResult<String> {
        Ok(String::new())
    }
}

impl BaseRelation for EmptyRelation {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn create_from(&self, _: &State, _: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!()
    }

    fn database(&self) -> Value {
        empty_string_value()
    }

    fn schema(&self) -> Value {
        empty_string_value()
    }

    fn identifier(&self) -> Value {
        empty_string_value()
    }

    fn relation_type(&self) -> Option<RelationType> {
        None
    }

    fn as_value(&self) -> Value {
        RelationObject::new(Arc::new(self.clone())).into_value()
    }

    fn adapter_type(&self) -> Option<String> {
        None
    }

    fn include(&self, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(self.as_value())
    }

    fn include_inner(&self, _args: Policy) -> Result<Value, MinijinjaError> {
        Ok(self.as_value())
    }

    fn render_self(&self) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn needs_to_drop(&self, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(Value::from(true))
    }

    fn incorporate(&self, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(self.as_value())
    }

    fn get_ddl_prefix_for_create(&self, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(empty_string_value())
    }

    fn get_ddl_prefix_for_alter(&self) -> Result<Value, MinijinjaError> {
        Ok(empty_string_value())
    }

    fn get_iceberg_ddl_options(&self, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn dynamic_table_config_changeset(&self, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn from_config(&self, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn normalize_component(&self, component: &str) -> String {
        component.to_string()
    }

    fn create_relation(
        &self,
        _database: Option<String>,
        _schema: Option<String>,
        _identifier: Option<String>,
        _relation_type: Option<RelationType>,
        _quote_policy: Policy,
    ) -> Result<Arc<dyn BaseRelation>, MinijinjaError> {
        Ok(Arc::new(self.clone()))
    }

    fn information_schema_inner(
        &self,
        _database: Option<String>,
        _view_name: &str,
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }
}
