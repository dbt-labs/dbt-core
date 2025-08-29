use crate::relation_object::{RelationObject, StaticBaseRelation};

use dbt_common::{ErrorCode, FsResult, fs_err};
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::relations::base::{
    BaseRelation, BaseRelationProperties, Policy, RelationPath,
};
use minijinja::{Error as MinijinjaError, State, Value};

use std::any::Any;
use std::sync::Arc;

/// A struct representing the Salesforce relation type for use with static methods
#[derive(Clone, Debug, Copy)]
pub struct SalesforceRelationType(pub ResolvedQuoting);

impl StaticBaseRelation for SalesforceRelationType {
    fn try_new(
        &self,
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        _custom_quoting: Option<ResolvedQuoting>,
    ) -> Result<Value, MinijinjaError> {
        Ok(RelationObject::new(Arc::new(SalesforceRelation::new(
            database,
            schema,
            identifier,
            relation_type,
        )))
        .into_value())
    }

    fn get_adapter_type(&self) -> String {
        "salesforce".to_string()
    }
}

/// A struct representing a Salesforce relation
#[derive(Clone, Debug)]
pub struct SalesforceRelation {
    /// The path of the relation
    pub path: RelationPath,
    /// The relation type (default: None)
    pub relation_type: Option<RelationType>,
}

impl BaseRelationProperties for SalesforceRelation {
    fn quote_policy(&self) -> Policy {
        Policy::disabled()
    }

    fn include_policy(&self) -> Policy {
        Policy::new(true, false, true)
    }

    fn quote_character(&self) -> char {
        unimplemented!("Salesforce quote character")
    }

    fn get_database(&self) -> FsResult<String> {
        self.path.database.clone().ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "database is required for salesforce relation",
            )
        })
    }

    fn get_schema(&self) -> FsResult<String> {
        unimplemented!("Salesforce schema")
    }

    fn get_identifier(&self) -> FsResult<String> {
        self.path.identifier.clone().ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "identifier is required for salesforce relation",
            )
        })
    }
}

impl SalesforceRelation {
    /// Creates a new Salesforce relation
    pub fn new(
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
    ) -> Self {
        Self {
            path: RelationPath {
                database,
                schema,
                identifier,
            },
            relation_type,
        }
    }
}

impl BaseRelation for SalesforceRelation {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Creates a new Salesforce relation from a state and a list of values
    fn create_from(&self, _: &State, _: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("Salesforce relation creation from Jinja values")
    }

    /// Returns the database name
    fn database(&self) -> Value {
        Value::from(self.path.database.clone())
    }

    /// Returns the schema name
    fn schema(&self) -> Value {
        Value::from(self.path.schema.clone())
    }

    /// Returns the identifier name
    fn identifier(&self) -> Value {
        Value::from(self.path.identifier.clone())
    }

    /// Helper: is this relation renamable?
    fn can_be_renamed(&self) -> bool {
        unimplemented!("Salesforce can_be_renamed")
    }

    /// Helper: is this relation replaceable?
    fn can_be_replaced(&self) -> bool {
        unimplemented!("Salesforce can_be_replaced")
    }

    fn quoted(&self, s: &str) -> String {
        s.to_string()
    }

    /// Returns the relation type
    fn relation_type(&self) -> Option<RelationType> {
        self.relation_type
    }

    fn as_value(&self) -> Value {
        RelationObject::new(Arc::new(self.clone())).into_value()
    }

    fn adapter_type(&self) -> Option<String> {
        Some("salesforce".to_string())
    }

    fn needs_to_drop(&self, _args: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("Salesforce needs_to_drop logic")
    }

    fn get_ddl_prefix_for_create(&self, _args: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("Salesforce DDL prefix for create")
    }

    fn get_ddl_prefix_for_alter(&self) -> Result<Value, MinijinjaError> {
        unimplemented!("Salesforce DDL prefix for alter")
    }

    fn get_iceberg_ddl_options(&self, _args: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("Salesforce does not support Iceberg DDL options")
    }

    fn include_inner(&self, _policy: Policy) -> Result<Value, MinijinjaError> {
        unimplemented!("Salesforce include inner")
    }

    fn normalize_component(&self, component: &str) -> String {
        component.to_string()
    }

    fn create_relation(
        &self,
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        _custom_quoting: Policy,
    ) -> Result<Arc<dyn BaseRelation>, MinijinjaError> {
        Ok(Arc::new(SalesforceRelation::new(
            database,
            schema,
            identifier,
            relation_type,
        )))
    }

    fn information_schema_inner(
        &self,
        _database: Option<String>,
        _view_name: &str,
    ) -> Result<Value, MinijinjaError> {
        unimplemented!("Salesforce information schema inner")
    }
}
