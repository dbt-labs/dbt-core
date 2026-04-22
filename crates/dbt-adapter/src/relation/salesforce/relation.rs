use crate::relation::{RelationObject, StaticBaseRelation};

use dbt_adapter_core::AdapterType;
use dbt_common::{ErrorCode, FsResult, fs_err};
use dbt_frontend_common::ident::Identifier;
use dbt_schema_store::CanonicalFqn;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::relations::base::{
    BaseRelation, BaseRelationProperties, Policy, RelationPath,
};
use minijinja::Value;

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
        _temporary: Option<bool>,
    ) -> Result<Value, minijinja::Error> {
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
        Policy::enabled()
    }

    fn include_policy(&self) -> Policy {
        Policy::new(false, false, true)
    }

    fn get_database(&self) -> FsResult<String> {
        self.path.database.clone().ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "database is required for salesforce relation",
            )
        })
    }

    // FIXME: this will cause trouble in a few known places
    // In unit_test.rs, where this sed to build SQL literals
    // In schema_cache where we expect 3 part fqn, non-applicable for now since static analysis is unsupported for Salesforce
    fn get_schema(&self) -> FsResult<String> {
        Ok(String::new())
    }

    fn get_identifier(&self) -> FsResult<String> {
        self.path.identifier.clone().ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "identifier is required for salesforce relation",
            )
        })
    }

    fn get_canonical_fqn(&self) -> FsResult<CanonicalFqn> {
        let database = if self.quote_policy().database {
            Identifier::new(self.get_database()?)
        } else {
            Identifier::new(self.get_database()?.to_ascii_uppercase())
        };
        let schema = if self.quote_policy().schema {
            Identifier::new(self.get_schema()?)
        } else {
            Identifier::new(self.get_schema()?.to_ascii_uppercase())
        };
        let identifier = if self.quote_policy().identifier {
            Identifier::new(self.get_identifier()?)
        } else {
            Identifier::new(self.get_identifier()?.to_ascii_uppercase())
        };
        Ok(CanonicalFqn::new(&database, &schema, &identifier))
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
                database: database.filter(|s| !s.is_empty()),
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

    fn to_owned(&self) -> Arc<dyn BaseRelation> {
        Arc::new(self.clone())
    }

    /// Creates a new Salesforce relation from a state and a list of values
    fn create_from(&self) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        unimplemented!("Salesforce relation creation from Jinja values")
    }

    fn set_is_delta(&mut self, _is_delta: Option<bool>) {
        // no-op
    }

    /// Returns the database name
    fn database(&self) -> Option<&str> {
        self.path.database.as_deref()
    }

    /// Returns the schema name
    fn schema(&self) -> Option<&str> {
        self.path.schema.as_deref()
    }

    /// Returns the identifier name
    fn identifier(&self) -> Option<&str> {
        self.path.identifier.as_deref()
    }

    /// Returns the relation type
    fn relation_type(&self) -> Option<RelationType> {
        self.relation_type
    }

    fn adapter_type(&self) -> AdapterType {
        AdapterType::Salesforce
    }

    fn needs_to_drop(
        &self,
        _old_relation: Option<Arc<dyn BaseRelation>>,
    ) -> Result<bool, minijinja::Error> {
        unimplemented!("Salesforce needs_to_drop logic")
    }

    fn get_ddl_prefix_for_create(
        &self,
        _model_config: Value,
        _temporary: bool,
    ) -> Result<String, minijinja::Error> {
        unimplemented!("Salesforce DDL prefix for create")
    }

    fn get_ddl_prefix_for_alter(&self) -> Result<String, minijinja::Error> {
        unimplemented!("Salesforce DDL prefix for alter")
    }

    fn get_iceberg_ddl_options(&self, _config: Value) -> Result<String, minijinja::Error> {
        unimplemented!("Salesforce does not support Iceberg DDL options")
    }

    fn include_inner(&self, _policy: Policy) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        Ok(Arc::new(self.clone()))
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
    ) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
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
        _view_name: Option<&str>,
    ) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        unimplemented!("Salesforce information schema inner")
    }
}
