use std::sync::Arc;

use dbt_common::{ErrorCode, FsResult, adapter::AdapterType, fs_err};
use dbt_frontend_common::ident::Identifier;
use dbt_schema_store::CanonicalFqn;
use dbt_schemas::{
    dbt_types::RelationType,
    schemas::{
        common::ResolvedQuoting,
        relations::base::{BaseRelation, BaseRelationProperties, Policy, RelationPath},
    },
};

use crate::relation::{RelationObject, StaticBaseRelation};

#[derive(Clone, Debug, Copy)]
pub struct FabricRelationType(pub ResolvedQuoting);

impl StaticBaseRelation for FabricRelationType {
    fn try_new(
        &self,
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        custom_quoting: Option<ResolvedQuoting>,
        temporary: Option<bool>,
    ) -> Result<minijinja::Value, minijinja::Error> {
        let _ = (
            database,
            schema,
            identifier,
            relation_type,
            custom_quoting,
            temporary,
        );
        todo!()
    }

    fn get_adapter_type(&self) -> String {
        "fabric".to_string()
    }
}

#[derive(Clone, Debug)]
pub struct FabricRelation {
    /// The path of the relation
    pub path: RelationPath,
    /// The relation type (default: None)
    pub relation_type: Option<RelationType>,
    /// Include policy
    pub include_policy: Policy,
    /// Quote policy
    pub quote_policy: Policy,
}

impl BaseRelationProperties for FabricRelation {
    fn include_policy(&self) -> Policy {
        self.include_policy
    }

    fn quote_policy(&self) -> Policy {
        self.quote_policy
    }

    fn quote_character(&self) -> char {
        '"'
    }

    fn get_database(&self) -> FsResult<String> {
        self.path.identifier.clone().ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "database is required for fabric relation",
            )
        })
    }

    fn get_schema(&self) -> FsResult<String> {
        self.path.schema.clone().ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "schema is required for fabric relation",
            )
        })
    }

    fn get_identifier(&self) -> FsResult<String> {
        self.path.identifier.clone().ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "identifier is required for fabric relation",
            )
        })
    }

    fn get_canonical_fqn(&self) -> FsResult<CanonicalFqn> {
        // Fabric is case sensitive, even if unquoted
        let d = self.get_database().map(Identifier::new)?;
        let s = self.get_schema().map(Identifier::new)?;
        let i = self.get_identifier().map(Identifier::new)?;
        Ok(CanonicalFqn::new(&d, &s, &i))
    }
}

impl FabricRelation {
    pub fn new(
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        custom_quoting: ResolvedQuoting,
    ) -> Self {
        Self {
            path: RelationPath {
                database,
                schema,
                identifier,
            },
            relation_type,
            include_policy: Policy::enabled(), // TODO(chasewalden): conservatively using FQN
            quote_policy: custom_quoting,
        }
    }
}

impl BaseRelation for FabricRelation {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn to_owned(&self) -> Arc<dyn BaseRelation> {
        Arc::new(self.clone())
    }

    fn create_from(
        &self,
        state: &minijinja::State,
        args: &[minijinja::Value],
    ) -> Result<minijinja::Value, minijinja::Error> {
        let _ = (state, args);
        todo!()
    }

    fn database(&self) -> minijinja::Value {
        self.path.database.clone().into()
    }

    fn schema(&self) -> minijinja::Value {
        self.path.schema.clone().into()
    }

    fn identifier(&self) -> minijinja::Value {
        self.path.identifier.clone().into()
    }

    fn adapter_type(&self) -> AdapterType {
        AdapterType::Fabric
    }

    fn set_is_delta(&mut self, is_delta: Option<bool>) {
        let _ = is_delta;
        todo!()
    }

    fn as_value(&self) -> minijinja::Value {
        RelationObject::new(BaseRelation::to_owned(self)).into_value()
    }

    fn normalize_component(&self, component: &str) -> String {
        let _ = component;
        todo!()
    }

    fn include_inner(&self, policy: Policy) -> Result<minijinja::Value, minijinja::Error> {
        let _ = policy;
        todo!()
    }

    fn create_relation(
        &self,
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        quote_policy: Policy,
    ) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        let _ = (database, schema, identifier, relation_type, quote_policy);
        todo!()
    }

    fn information_schema_inner(
        &self,
        database: Option<String>,
        view_name: Option<&str>,
    ) -> Result<minijinja::Value, minijinja::Error> {
        let _ = (database, view_name);
        todo!()
    }
}
