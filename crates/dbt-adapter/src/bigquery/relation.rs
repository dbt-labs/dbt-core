use crate::bigquery::relation_config::{
    BigqueryMaterializedViewConfig, BigqueryMaterializedViewConfigChangesetObject,
    BigqueryMaterializedViewConfigObject,
};
use crate::information_schema::InformationSchema;
use crate::relation_object::{RelationObject, StaticBaseRelation};

use arrow::array::RecordBatch;
use dbt_common::{ErrorCode, FsResult, current_function_name, fs_err};
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::InternalDbtNodeWrapper;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::relations::base::{
    BaseRelation, BaseRelationProperties, Policy, RelationPath,
};
use dbt_schemas::schemas::serde::minijinja_value_to_typed_struct;
use minijinja::arg_utils::{ArgParser, ArgsIter};
use minijinja::{Error as MinijinjaError, ErrorKind as MinijinjaErrorKind, State, Value};

use std::any::Any;
// use std::ops::Deref;
use std::sync::Arc;

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
    ) -> Result<Value, MinijinjaError> {
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

    /// See [reference](https://github.com/dbt-labs/dbt-adapters/blob/2a94cc75dba1f98fa5caff1f396f5af7ee444598/dbt-bigquery/src/dbt/adapters/bigquery/relation.py#L30)
    fn quote_character(&self) -> char {
        '`'
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

    fn create_from(&self, _: &State, _: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("BigQuery relation creation from Jinja values")
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
        format!("`{s}`")
    }

    fn relation_type(&self) -> Option<RelationType> {
        self.relation_type
    }

    /// Helper: is this relation renamable?
    fn can_be_renamed(&self) -> bool {
        matches!(self.relation_type(), Some(RelationType::Table))
    }

    fn as_value(&self) -> Value {
        RelationObject::new(Arc::new(self.clone())).into_value()
    }

    fn adapter_type(&self) -> Option<String> {
        Some("bigquery".to_string())
    }

    fn include_inner(&self, policy: Policy) -> Result<Value, MinijinjaError> {
        let relation = Self::new_with_policy(
            self.path.clone(),
            self.relation_type,
            policy,
            self.quote_policy,
        );

        Ok(relation.as_value())
    }

    fn post_incorporate(&self, mut args: ArgParser) -> Result<Value, MinijinjaError> {
        // Extract and consume 'location' kwarg (consistent with base consume approach)
        let loc_opt: Option<String> = args.consume_optional_only_from_kwargs("location");
        if let Some(loc) = loc_opt {
            let mut cloned = self.clone();
            cloned.location = Some(loc);
            return Ok(cloned.as_value());
        }
        Ok(self.as_value())
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
    ) -> Result<Arc<dyn BaseRelation>, MinijinjaError> {
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
        view_name: &str,
    ) -> Result<Value, MinijinjaError> {
        let mut info_schema = InformationSchema::try_from_relation(database.clone(), view_name)?;

        // BigQuery INFORMATION_SCHEMA scoping rules:
        // - OBJECT_PRIVILEGES: project-level with region → project.`region-<loc>`.INFORMATION_SCHEMA.<view>
        // - Other views: dataset-level → dataset.INFORMATION_SCHEMA.<view> (using the relation's own dataset)

        if view_name.eq_ignore_ascii_case("OBJECT_PRIVILEGES") {
            // OBJECT_PRIVILEGES require a location. If the location is blank there is nothing
            // the user can do about it.
            let loc = self.location.as_ref().ok_or_else(|| {
                MinijinjaError::new(
                    minijinja::ErrorKind::InvalidOperation,
                    format!(
                        "No location/region found when trying to retrieve \"{}\"",
                        view_name
                    ),
                )
            })?;

            let project = database.or_else(|| self.path.database.clone());
            if let Some(proj) = project {
                info_schema.database = Some(proj);
                info_schema.location = Some(loc.to_string());
            } else {
                return Err(MinijinjaError::new(
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
                (Some(proj), Some(ds)) => info_schema.database = Some(format!("{}.{}", proj, ds)),
                (None, Some(ds)) => info_schema.database = Some(ds),
                _ => {}
            }
        }
        Ok(RelationObject::new(Arc::new(info_schema)).into_value())
    }

    fn materialized_view_config_changeset(&self, args: &[Value]) -> Result<Value, MinijinjaError> {
        let iter = ArgsIter::new(
            current_function_name!(),
            &["relation_results", "relation_config"],
            args,
        );

        let relation_results_value = iter.next_arg::<&Value>()?;
        let new_config_value = iter.next_arg::<&Value>()?;
        iter.finish()?;

        let existing_mv = relation_results_value
            .as_object()
            .ok_or_else(|| {
                MinijinjaError::new(
                    MinijinjaErrorKind::InvalidArgument,
                    "relation_results must be Object",
                )
            })?
            .downcast_ref::<BigqueryMaterializedViewConfigObject>()
            .ok_or_else(|| {
                MinijinjaError::new(
                    MinijinjaErrorKind::InvalidArgument,
                    "relation_results must be BigqueryMaterializedViewConfigObject",
                )
            })?
            .inner();

        // TODO(serramatutu): minijinja_value_to_typed_struct does not work with references, so we
        // have to clone the value here...
        let new_config_node =
            minijinja_value_to_typed_struct::<InternalDbtNodeWrapper>(new_config_value.clone())
                .map_err(|e| {
                    MinijinjaError::new(
                        MinijinjaErrorKind::SerdeDeserializeError,
                        format!("Failed to deserialize InternalDbtNodeWrapper: {e}"),
                    )
                })?;

        let new_config_model = match new_config_node {
            InternalDbtNodeWrapper::Model(model) => model,
            _ => {
                return Err(MinijinjaError::new(
                    MinijinjaErrorKind::InvalidOperation,
                    "Expected a model node",
                ));
            }
        };

        let new_mv_config =
            <dyn BigqueryMaterializedViewConfig>::try_from_model(Arc::from(new_config_model))
                .map_err(|e| MinijinjaError::new(MinijinjaErrorKind::InvalidArgument, e))?;

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
            )
            .unwrap();

        let relation = relation.downcast_object::<RelationObject>().unwrap();
        assert_eq!(
            relation.inner().render_self().unwrap().as_str().unwrap(),
            "`d`.`s`.`i`"
        );
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
            .information_schema_inner(Some("other_db".to_string()), "TABLES")
            .unwrap();

        let info_relation = info_schema.downcast_object::<RelationObject>().unwrap();
        let rendered = info_relation.inner().render_self().unwrap();
        assert_eq!(
            rendered.as_str().unwrap(),
            "test_db.test_schema.INFORMATION_SCHEMA.TABLES"
        );

        // Test COLUMNS view
        let info_schema = relation
            .information_schema_inner(Some("other_db".to_string()), "COLUMNS")
            .unwrap();

        let info_relation = info_schema.downcast_object::<RelationObject>().unwrap();
        let rendered = info_relation.inner().render_self().unwrap();
        assert_eq!(
            rendered.as_str().unwrap(),
            "test_db.test_schema.INFORMATION_SCHEMA.COLUMNS"
        );

        // Test SCHEMATA view - still uses dataset-level with project.dataset format
        let info_schema = relation.information_schema_inner(None, "SCHEMATA").unwrap();

        let info_relation = info_schema.downcast_object::<RelationObject>().unwrap();
        let rendered = info_relation.inner().render_self().unwrap();
        assert_eq!(
            rendered.as_str().unwrap(),
            "test_db.test_schema.INFORMATION_SCHEMA.SCHEMATA"
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
        let result =
            relation.information_schema_inner(Some("test_db".to_string()), "OBJECT_PRIVILEGES");
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
            .information_schema_inner(Some("test_db".to_string()), "OBJECT_PRIVILEGES")
            .unwrap();

        let info_relation = info_schema.downcast_object::<RelationObject>().unwrap();
        let rendered = info_relation.inner().render_self().unwrap();
        assert_eq!(
            rendered.as_str().unwrap(),
            "test_db.`region-US`.INFORMATION_SCHEMA.OBJECT_PRIVILEGES"
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
        let info_schema = relation.information_schema_inner(None, "TABLES").unwrap();

        let info_relation = info_schema.downcast_object::<RelationObject>().unwrap();
        let rendered = info_relation.inner().render_self().unwrap();
        assert_eq!(
            rendered.as_str().unwrap(),
            "test_schema.INFORMATION_SCHEMA.TABLES"
        );
    }
}
