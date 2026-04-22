use crate::funcs::none_value;

use dbt_adapter_core::AdapterType;
use dbt_common::FsResult;
use dbt_frontend_common::ident::Identifier;
use dbt_schema_store::CanonicalFqn;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::relations::base::{BaseRelation, BaseRelationProperties, Policy};
use minijinja::Value;

use std::any::Any;
use std::sync::Arc;

/// Empty relation
///
/// A relation that returns empty values for all fields.
#[derive(Clone, Debug)]
pub struct EmptyRelation {
    adapter_type: AdapterType,
}

impl EmptyRelation {
    pub fn new(adapter_type: AdapterType) -> Self {
        Self { adapter_type }
    }
}

impl BaseRelationProperties for EmptyRelation {
    fn is_database_relation(&self) -> bool {
        false
    }

    fn include_policy(&self) -> Policy {
        Policy {
            database: false,
            schema: false,
            identifier: false,
        }
    }

    fn quote_policy(&self) -> Policy {
        Policy {
            database: false,
            schema: false,
            identifier: false,
        }
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

    fn get_canonical_fqn(&self) -> FsResult<CanonicalFqn> {
        debug_assert!(false, "get_canonical_fqn is unavailable for EmptyRelation");
        Ok(CanonicalFqn::new(
            &Identifier::new(""),
            &Identifier::new(""),
            &Identifier::new(""),
        ))
    }
}

impl BaseRelation for EmptyRelation {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_owned(&self) -> Arc<dyn BaseRelation> {
        Arc::new(self.clone())
    }

    fn create_from(&self) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        unimplemented!("relation creation from Jinja values")
    }

    fn set_is_delta(&mut self, is_delta: Option<bool>) {
        debug_assert!(
            is_delta.is_none(),
            "set_is_delta is unavailable for EmptyRelation"
        );
    }

    fn database(&self) -> Option<&str> {
        None
    }

    fn schema(&self) -> Option<&str> {
        Some("")
    }

    fn identifier(&self) -> Option<&str> {
        Some("")
    }

    fn relation_type(&self) -> Option<RelationType> {
        None
    }

    fn adapter_type(&self) -> AdapterType {
        self.adapter_type
    }

    fn include_inner(&self, _args: Policy) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        Ok(Arc::new(self.clone()))
    }

    fn render_self_as_str(&self) -> String {
        String::new()
    }

    fn needs_to_drop(
        &self,
        _old_relation: Option<Arc<dyn BaseRelation>>,
    ) -> Result<bool, minijinja::Error> {
        Ok(true)
    }

    fn incorporate(
        &self,
        _path: Option<Value>,
        _relation_type: Option<RelationType>,
        _location: Option<String>,
    ) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        Ok(Arc::new(self.clone()))
    }

    fn get_ddl_prefix_for_create(
        &self,
        _model_config: Value,
        _temporary: bool,
    ) -> Result<String, minijinja::Error> {
        Ok(String::new())
    }

    fn get_ddl_prefix_for_alter(&self) -> Result<String, minijinja::Error> {
        Ok(String::new())
    }

    fn get_iceberg_ddl_options(&self, _config: Value) -> Result<String, minijinja::Error> {
        Ok(String::new())
    }

    fn dynamic_table_config_changeset(
        &self,
        _relation_results: &Value,
        _relation_config: &Value,
    ) -> Result<Value, minijinja::Error> {
        Ok(none_value())
    }

    fn from_config(&self, _config: &Value) -> Result<Value, minijinja::Error> {
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
    ) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        Ok(Arc::new(self.clone()))
    }

    fn information_schema_inner(
        &self,
        _database: Option<String>,
        _view_name: Option<&str>,
    ) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        Ok(Arc::new(self.clone()))
    }
}
