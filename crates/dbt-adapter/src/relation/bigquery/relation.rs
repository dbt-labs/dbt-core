use crate::information_schema::InformationSchema;
use crate::need_quotes::need_quotes;
use crate::relation::{RelationObject, StaticBaseRelation};

use arrow::array::RecordBatch;
use dbt_adapter_core::AdapterType;
use dbt_common::{ErrorCode, FsResult, fs_err};
use dbt_frontend_common::ident::Identifier;
use dbt_schema_store::CanonicalFqn;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::InternalDbtNodeWrapper;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::relations::base::{
    BaseRelation, BaseRelationProperties, Policy, RelationPath,
};
use dbt_schemas::schemas::serde::minijinja_value_to_typed_struct;
use minijinja::Value;

use std::any::Any;
// use std::ops::Deref;
use std::sync::Arc;

use crate::relation::bigquery::*;

const INFORMATION_SCHEMA_SCHEMA: &str = "information_schema";

/// A struct representing the relation type for use with static methods
#[derive(Clone, Debug)]
pub struct BigqueryRelationType(pub ResolvedQuoting);

impl StaticBaseRelation for BigqueryRelationType {
    fn try_new(
        &self,
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        custom_quoting: Option<ResolvedQuoting>,
        _temporary: Option<bool>,
    ) -> Result<Value, minijinja::Error> {
        Ok(RelationObject::new(Arc::new(BigqueryRelation::new(
            database,
            schema,
            identifier,
            relation_type,
            None,
            custom_quoting.unwrap_or(self.0),
        )))
        .into_value())
    }

    fn get_adapter_type(&self) -> String {
        "bigquery".to_string()
    }
}

/// A relation object for bigquery adapter
#[derive(Clone, Debug)]
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
    /// The location/region for this relation (e.g., "US", "EU")
    pub location: Option<String>,
}

impl BaseRelationProperties for BigqueryRelation {
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
                "database is required for bigquery relation",
            )
        })
    }

    fn get_schema(&self) -> FsResult<String> {
        self.path.schema.clone().ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "schema is required for bigquery relation",
            )
        })
    }

    fn get_identifier(&self) -> FsResult<String> {
        self.path.identifier.clone().ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "identifier is required for bigquery relation",
            )
        })
    }
    fn get_canonical_fqn(&self) -> FsResult<CanonicalFqn> {
        Ok(CanonicalFqn::new(
            &Identifier::new(self.get_database()?),
            &Identifier::new(self.get_schema()?),
            &Identifier::new(self.get_identifier()?),
        ))
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
                database: database.filter(|s| !s.is_empty()),
                schema,
                identifier,
            },
            relation_type,
            include_policy: Policy::trues(),
            quote_policy: custom_quoting,
            native_schema,
            location: None,
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
            location: None,
        }
    }
}

impl BaseRelation for BigqueryRelation {
    fn is_system(&self) -> bool {
        self.path.schema.as_ref().map(|s| s.to_lowercase())
            == Some(INFORMATION_SCHEMA_SCHEMA.to_string())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_owned(&self) -> Arc<dyn BaseRelation> {
        Arc::new(self.clone())
    }

    fn set_is_delta(&mut self, _is_delta: Option<bool>) {
        // no-op
    }

    fn create_from(&self) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        unimplemented!("BigQuery relation creation from Jinja values")
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

    fn quoted(&self, s: &str) -> String {
        format!("`{s}`")
    }

    fn relation_type(&self) -> Option<RelationType> {
        self.relation_type
    }

    /// Helper: is this relation renamable?
    fn can_be_renamed(&self) -> bool {
        matches!(self.relation_type(), Some(RelationType::Table))
    }

    fn adapter_type(&self) -> AdapterType {
        AdapterType::Bigquery
    }

    fn include_inner(&self, policy: Policy) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        let relation = Self::new_with_policy(
            self.path.clone(),
            self.relation_type,
            policy,
            self.quote_policy,
        );

        Ok(Arc::new(relation))
    }

    fn quote_inner(&self, policy: Policy) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        let relation = Self::new_with_policy(
            self.path.clone(),
            self.relation_type,
            self.include_policy,
            policy,
        );

        Ok(Arc::new(relation))
    }

    fn post_incorporate(
        &self,
        location: Option<String>,
    ) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        if let Some(loc) = location {
            let mut cloned = self.clone();
            cloned.location = Some(loc);
            return Ok(Arc::new(cloned));
        }
        Ok(Arc::new(self.clone()))
    }

    /// In BigQuery, we don't normalize since quoting doesn't decide case sensitivity
    /// object names are case sensitive by default unless explicitly turned off via the is_case_insensitive option
    /// https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#schema_option_list
    fn normalize_component(&self, component: &str) -> String {
        component.to_string()
    }

    fn create_relation(
        &self,
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        custom_quoting: Policy,
    ) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        Ok(Arc::new(BigqueryRelation::new(
            database,
            schema,
            identifier,
            relation_type,
            None,
            custom_quoting,
        )))
    }

    fn information_schema_inner(
        &self,
        database: Option<String>,
        view_name: Option<&str>,
    ) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        let mut info_schema =
            InformationSchema::try_from_relation(self.adapter_type(), database.clone(), view_name)?;

        let quote_if_needed = |identifier: &str| -> String {
            if need_quotes(AdapterType::Bigquery, identifier) {
                self.quoted(identifier)
            } else {
                identifier.to_string()
            }
        };

        // BigQuery INFORMATION_SCHEMA scoping rules:
        // - OBJECT_PRIVILEGES: project-level with region → project.`region-<loc>`.INFORMATION_SCHEMA.<view>
        // - Other views: dataset-level → dataset.INFORMATION_SCHEMA.<view> (using the relation's own dataset)

        if let Some(view_name) = view_name
            && view_name.eq_ignore_ascii_case("OBJECT_PRIVILEGES")
        {
            // OBJECT_PRIVILEGES require a location. If the location is blank there is nothing
            // the user can do about it.
            let loc = self.location.as_ref().ok_or_else(|| {
                minijinja::Error::new(
                    minijinja::ErrorKind::InvalidOperation,
                    format!(
                        "No location/region found when trying to retrieve \"{}\"",
                        view_name
                    ),
                )
            })?;

            let project = database.or_else(|| self.path.database.clone());
            if let Some(proj) = project {
                info_schema.database = Some(quote_if_needed(&proj));
                info_schema.location = Some(loc.to_string());
            } else {
                return Err(minijinja::Error::new(
                    minijinja::ErrorKind::InvalidOperation,
                    "Database/project is required for OBJECT_PRIVILEGES view",
                ));
            }
        } else {
            // Dataset-level: use the relation's own project.dataset or just dataset
            info_schema.location = None;
            let project = self.path.database.clone();
            let dataset = self.path.schema.clone();
            match (project, dataset) {
                (Some(proj), Some(ds)) => {
                    info_schema.database = Some(format!(
                        "{}.{}",
                        quote_if_needed(&proj),
                        quote_if_needed(&ds)
                    ));
                }
                (Some(proj), None) => {
                    info_schema.database = Some(quote_if_needed(&proj));
                }
                (None, Some(ds)) => info_schema.database = Some(quote_if_needed(&ds)),
                _ => {}
            }
        }
        Ok(Arc::new(info_schema))
    }

    fn materialized_view_config_changeset(
        &self,
        relation_results_value: &Value,
        new_config_value: &Value,
    ) -> Result<Value, minijinja::Error> {
        let existing_mv = relation_results_value
            .as_object()
            .ok_or_else(|| {
                minijinja::Error::new(
                    minijinja::ErrorKind::InvalidArgument,
                    "relation_results must be Object",
                )
            })?
            .downcast_ref::<BigqueryMaterializedViewConfigObject>()
            .ok_or_else(|| {
                minijinja::Error::new(
                    minijinja::ErrorKind::InvalidArgument,
                    "relation_results must be BigqueryMaterializedViewConfigObject",
                )
            })?
            .inner();

        // TODO(serramatutu): minijinja_value_to_typed_struct does not work with references, so we
        // have to clone the value here...
        let new_config_node =
            minijinja_value_to_typed_struct::<InternalDbtNodeWrapper>(new_config_value.clone())
                .map_err(|e| {
                    minijinja::Error::new(
                        minijinja::ErrorKind::SerdeDeserializeError,
                        format!("Failed to deserialize InternalDbtNodeWrapper: {e}"),
                    )
                })?;

        let new_config_model = match new_config_node {
            InternalDbtNodeWrapper::Model(model) => model,
            _ => {
                return Err(minijinja::Error::new(
                    minijinja::ErrorKind::InvalidOperation,
                    "Expected a model node",
                ));
            }
        };

        let new_mv_config =
            <dyn BigqueryMaterializedViewConfig>::try_from_model(Arc::from(new_config_model))
                .map_err(|e| minijinja::Error::new(minijinja::ErrorKind::InvalidArgument, e))?;

        let changeset =
            BigqueryMaterializedViewConfigChangesetObject::new(existing_mv, new_mv_config);

        if changeset.has_changes() {
            Ok(Value::from_object(changeset))
        } else {
            Ok(Value::from(None::<()>))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_schemas::{dbt_types::RelationType, schemas::relations::DEFAULT_RESOLVED_QUOTING};

    #[test]
    fn test_try_new_via_static_base_relation() {
        let relation = BigqueryRelationType(DEFAULT_RESOLVED_QUOTING)
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
        assert_eq!(relation.inner().render_self_as_str(), "`d`.`s`.`i`");
        assert_eq!(relation.relation_type().unwrap(), RelationType::Table);
    }

    #[test]
    fn test_information_schema_with_database() {
        let relation = BigqueryRelation::new(
            Some("test_db".to_string()),
            Some("test_schema".to_string()),
            Some("test_table".to_string()),
            Some(RelationType::Table),
            None,
            DEFAULT_RESOLVED_QUOTING,
        );

        // Test TABLES view - BigQuery uses dataset-level INFORMATION_SCHEMA
        // When relation has both project and dataset, format as project.dataset.INFORMATION_SCHEMA
        let info_schema = relation
            .information_schema_inner(Some("other_db".to_string()), Some("TABLES"))
            .unwrap();

        let rendered = info_schema.render_self_as_str();
        assert_eq!(rendered, "test_db.test_schema.INFORMATION_SCHEMA.TABLES");

        // Test COLUMNS view
        let info_schema = relation
            .information_schema_inner(Some("other_db".to_string()), Some("COLUMNS"))
            .unwrap();

        let rendered = info_schema.render_self_as_str();
        assert_eq!(rendered, "test_db.test_schema.INFORMATION_SCHEMA.COLUMNS");

        // Test SCHEMATA view - still uses dataset-level with project.dataset format
        let info_schema = relation
            .information_schema_inner(None, Some("SCHEMATA"))
            .unwrap();

        let rendered = info_schema.render_self_as_str();
        assert_eq!(rendered, "test_db.test_schema.INFORMATION_SCHEMA.SCHEMATA");
    }

    #[test]
    fn test_information_schema_quotes_project_identifier() {
        let relation = BigqueryRelation::new(
            Some("my-project-1a".to_string()),
            Some("test_schema".to_string()),
            Some("test_table".to_string()),
            Some(RelationType::Table),
            None,
            DEFAULT_RESOLVED_QUOTING,
        );

        let info_schema = relation
            .information_schema_inner(None, Some("TABLES"))
            .unwrap();

        let rendered = info_schema.render_self_as_str();
        assert_eq!(
            rendered,
            "`my-project-1a`.test_schema.INFORMATION_SCHEMA.TABLES"
        );
    }

    #[test]
    fn test_object_privileges_requires_location() {
        let mut relation = BigqueryRelation::new(
            Some("test_db".to_string()),
            Some("test_schema".to_string()),
            Some("test_table".to_string()),
            Some(RelationType::Table),
            None,
            DEFAULT_RESOLVED_QUOTING,
        );

        // Test OBJECT_PRIVILEGES without location - should fail
        let result = relation
            .information_schema_inner(Some("test_db".to_string()), Some("OBJECT_PRIVILEGES"));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("No location/region found when trying to retrieve")
        );

        // Add location and test again - should succeed
        relation.location = Some("US".to_string());
        let info_schema = relation
            .information_schema_inner(Some("test_db".to_string()), Some("OBJECT_PRIVILEGES"))
            .unwrap();

        let rendered = info_schema.render_self_as_str();
        assert_eq!(
            rendered,
            "test_db.`region-US`.INFORMATION_SCHEMA.OBJECT_PRIVILEGES"
        );
    }

    #[test]
    fn test_object_privileges_quotes_project_identifier() {
        let mut relation = BigqueryRelation::new(
            Some("my-project-1a".to_string()),
            Some("test_schema".to_string()),
            Some("test_table".to_string()),
            Some(RelationType::Table),
            None,
            DEFAULT_RESOLVED_QUOTING,
        );
        relation.location = Some("US".to_string());

        let info_schema = relation
            .information_schema_inner(Some("my-project-1a".to_string()), Some("OBJECT_PRIVILEGES"))
            .unwrap();

        let rendered = info_schema.render_self_as_str();
        assert_eq!(
            rendered,
            "`my-project-1a`.`region-US`.INFORMATION_SCHEMA.OBJECT_PRIVILEGES"
        );
    }

    #[test]
    fn test_information_schema_without_database() {
        let relation = BigqueryRelation::new(
            None,
            Some("test_schema".to_string()),
            Some("test_table".to_string()),
            Some(RelationType::Table),
            None,
            DEFAULT_RESOLVED_QUOTING,
        );

        // Test TABLES view without database - uses dataset-level INFORMATION_SCHEMA
        let info_schema = relation
            .information_schema_inner(None, Some("TABLES"))
            .unwrap();

        let rendered = info_schema.render_self_as_str();
        assert_eq!(rendered, "test_schema.INFORMATION_SCHEMA.TABLES");
    }

    #[test]
    fn test_information_schema_without_view() {
        let relation = BigqueryRelation::new(
            None,
            Some("test_schema".to_string()),
            Some("test_table".to_string()),
            Some(RelationType::Table),
            None,
            DEFAULT_RESOLVED_QUOTING,
        );

        // Test TABLES view without database - uses dataset-level INFORMATION_SCHEMA
        let info_schema = relation.information_schema_inner(None, None).unwrap();

        let rendered = info_schema.render_self_as_str();
        assert_eq!(rendered, "test_schema.INFORMATION_SCHEMA");
    }
}
