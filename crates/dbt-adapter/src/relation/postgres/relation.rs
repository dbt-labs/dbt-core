use crate::information_schema::InformationSchema;
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

const MAX_CHARACTERS_IN_IDENTIFIER: usize = 63;

/// A struct representing the Postgres relation type for use with static methods
#[derive(Clone, Debug)]
pub struct PostgresRelationType(pub ResolvedQuoting);

impl StaticBaseRelation for PostgresRelationType {
    fn try_new(
        &self,
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        custom_quoting: Option<ResolvedQuoting>,
        _temporary: Option<bool>,
    ) -> Result<Value, minijinja::Error> {
        Ok(RelationObject::new(Arc::new(PostgresRelation::try_new(
            database,
            schema,
            identifier,
            relation_type,
            custom_quoting.unwrap_or(self.0),
        )?))
        .into_value())
    }

    fn get_adapter_type(&self) -> String {
        "postgres".to_string()
    }
}

/// A relation object for postgres adapter
#[derive(Clone, Debug)]
pub struct PostgresRelation {
    /// The database, schema, and identifier of the relation
    pub path: RelationPath,
    /// The relation type
    pub relation_type: Option<RelationType>,
    /// Include policy
    pub include_policy: Policy,
    /// Quote policy
    pub quote_policy: Policy,
}

impl PostgresRelation {
    /// Creates a new PostgreSQL relation
    pub fn try_new(
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        custom_quoting: ResolvedQuoting,
    ) -> Result<Self, minijinja::Error> {
        Self::try_new_with_policy(
            RelationPath {
                database: database.filter(|s| !s.is_empty()),
                schema,
                identifier,
            },
            relation_type,
            Policy::enabled(),
            custom_quoting,
        )
    }

    /// Creates a new PostgreSQL relation
    pub fn try_new_with_policy(
        path: RelationPath,
        relation_type: Option<RelationType>,
        include_policy: Policy,
        quote_policy: Policy,
    ) -> Result<Self, minijinja::Error> {
        // Check identifier length limit
        // TODO (Ani): dbt ignores this check if the relation.type is none (indicating a test relation)
        if path.identifier.is_some()
            && path.identifier.as_deref().map(|i| i.len()).unwrap() > MAX_CHARACTERS_IN_IDENTIFIER
            && relation_type.is_some()
        {
            return Err(minijinja::Error::new(
                minijinja::ErrorKind::InvalidOperation,
                format!(
                    "Relation name '{}' is longer than {} characters",
                    path.identifier.as_deref().unwrap(),
                    MAX_CHARACTERS_IN_IDENTIFIER
                ),
            ));
        }

        Ok(Self {
            path,
            relation_type,
            include_policy,
            quote_policy,
        })
    }
}

impl BaseRelationProperties for PostgresRelation {
    fn include_policy(&self) -> Policy {
        self.include_policy
    }

    fn quote_policy(&self) -> Policy {
        self.quote_policy
    }

    fn get_database(&self) -> FsResult<String> {
        self.path.database.clone().ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "database is required for postgres relation",
            )
        })
    }

    fn get_schema(&self) -> FsResult<String> {
        self.path.schema.clone().ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "schema is required for postgres relation",
            )
        })
    }

    fn get_identifier(&self) -> FsResult<String> {
        self.path.identifier.clone().ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "identifier is required for postgres relation",
            )
        })
    }

    fn get_canonical_fqn(&self) -> FsResult<CanonicalFqn> {
        let database = if self.quote_policy().database {
            Identifier::new(self.get_database()?)
        } else {
            Identifier::new(self.get_database()?.to_ascii_lowercase())
        };
        let schema = if self.quote_policy().schema {
            Identifier::new(self.get_schema()?)
        } else {
            Identifier::new(self.get_schema()?.to_ascii_lowercase())
        };
        let identifier = if self.quote_policy().identifier {
            Identifier::new(self.get_identifier()?)
        } else {
            Identifier::new(self.get_identifier()?.to_ascii_lowercase())
        };
        Ok(CanonicalFqn::new(&database, &schema, &identifier))
    }
}

impl BaseRelation for PostgresRelation {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_owned(&self) -> Arc<dyn BaseRelation> {
        Arc::new(self.clone())
    }

    fn create_from(&self) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        unimplemented!("PostgreSQL relation creation from Jinja values")
    }

    fn set_is_delta(&mut self, _is_delta: Option<bool>) {
        // no-op
    }

    fn database(&self) -> Option<&str> {
        self.path.database.as_deref()
    }

    fn schema(&self) -> Option<&str> {
        self.path.schema.as_deref()
    }

    fn identifier(&self) -> Option<&str> {
        self.path.identifier.as_deref()
    }

    fn relation_type(&self) -> Option<RelationType> {
        self.relation_type
    }

    fn adapter_type(&self) -> AdapterType {
        AdapterType::Postgres
    }

    fn include_inner(
        &self,
        include_policy: Policy,
    ) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        let relation = PostgresRelation::try_new_with_policy(
            self.path.clone(),
            self.relation_type,
            include_policy,
            self.quote_policy,
        )?;
        Ok(Arc::new(relation))
    }

    fn relation_max_name_length(&self) -> Result<u32, minijinja::Error> {
        Ok(MAX_CHARACTERS_IN_IDENTIFIER as u32)
    }

    fn normalize_component(&self, component: &str) -> String {
        component.to_lowercase()
    }

    fn create_relation(
        &self,
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        custom_quoting: Policy,
    ) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        // Preserve the include_policy from the original relation (important for DuckDB)
        Ok(Arc::new(PostgresRelation::try_new_with_policy(
            RelationPath {
                database,
                schema,
                identifier,
            },
            relation_type,
            self.include_policy,
            custom_quoting,
        )?))
    }

    fn information_schema_inner(
        &self,
        database: Option<String>,
        view_name: Option<&str>,
    ) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        let result =
            InformationSchema::try_from_relation(self.adapter_type(), database, view_name)?;
        Ok(Arc::new(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_schemas::{dbt_types::RelationType, schemas::relations::DEFAULT_RESOLVED_QUOTING};

    #[test]
    fn test_try_new_via_static_base_relation() {
        let relation = PostgresRelationType(DEFAULT_RESOLVED_QUOTING)
            .try_new(
                Some("d".to_string()),
                Some("s".to_string()),
                Some("i".to_string()),
                Some(RelationType::Table),
                Some(DEFAULT_RESOLVED_QUOTING),
                None,
            )
            .unwrap();

        let relation = relation.downcast_object::<RelationObject>().unwrap();
        assert_eq!(relation.inner().render_self_as_str(), "\"d\".\"s\".\"i\"");
        assert_eq!(relation.relation_type().unwrap(), RelationType::Table);
    }
}
