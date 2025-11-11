use crate::information_schema::InformationSchema;
use crate::redshift::relation_configs::materialized_view_config::{
    DescribeMaterializedViewResults, RedshiftMaterializedViewConfig,
    RedshiftMaterializedViewConfigChangeset,
};
use crate::relation_object::{RelationObject, StaticBaseRelation};

use arrow::array::RecordBatch;
use dbt_common::{ErrorCode, FsResult, current_function_name, fs_err};
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::common::{DbtMaterialization, ResolvedQuoting};
use dbt_schemas::schemas::relations::base::{
    BaseRelation, BaseRelationProperties, Policy, RelationPath,
};
use dbt_schemas::schemas::{InternalDbtNodeWrapper, RelationChangeSet};
use minijinja::arg_utils::{ArgParser, ArgsIter, check_num_args};
use minijinja::{Error as MinijinjaError, ErrorKind as MinijinjaErrorKind, State, Value};
use serde::Deserialize;

use std::any::Any;
use std::sync::Arc;

const MAX_CHARACTERS_IN_IDENTIFIER: u32 = 127;

/// A struct representing the relation type for use with static methods
#[derive(Clone, Debug)]
pub struct RedshiftRelationType(pub ResolvedQuoting);

impl StaticBaseRelation for RedshiftRelationType {
    fn try_new(
        &self,
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        custom_quoting: Option<ResolvedQuoting>,
    ) -> Result<Value, MinijinjaError> {
        Ok(RelationObject::new(Arc::new(RedshiftRelation::new(
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
        "redshift".to_string()
    }
}

/// A relation object for the adapter
#[derive(Clone, Debug)]
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

    fn get_database(&self) -> FsResult<String> {
        self.path.database.clone().ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "database is required for redshift relation",
            )
        })
    }

    fn get_schema(&self) -> FsResult<String> {
        self.path.schema.clone().ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "schema is required for redshift relation",
            )
        })
    }

    fn get_identifier(&self) -> FsResult<String> {
        self.path.identifier.clone().ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "identifier is required for redshift relation",
            )
        })
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
        unimplemented!("Redshift relation creation from Jinja values")
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
        self.relation_type
    }

    fn as_value(&self) -> Value {
        RelationObject::new(Arc::new(self.clone())).into_value()
    }

    fn adapter_type(&self) -> Option<String> {
        Some("redshift".to_string())
    }

    fn include_inner(&self, policy: Policy) -> Result<Value, MinijinjaError> {
        let relation = Self::new_with_policy(self.path.clone(), self.relation_type, policy);

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
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        custom_quoting: Policy,
    ) -> Result<Arc<dyn BaseRelation>, MinijinjaError> {
        Ok(Arc::new(RedshiftRelation::new(
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
        let result = InformationSchema::try_from_relation(database, view_name)?;
        Ok(RelationObject::new(Arc::new(result)).into_value())
    }

    // https://github.com/dbt-labs/dbt-adapters/blob/f492c919d3bd415bf5065b3cd8cd1af23562feb0/dbt-redshift/src/dbt/adapters/redshift/relation.py#L34
    fn can_be_renamed(&self) -> bool {
        matches!(
            self.relation_type(),
            Some(RelationType::Table) | Some(RelationType::View)
        )
    }

    // https://github.com/dbt-labs/dbt-adapters/blob/f492c919d3bd415bf5065b3cd8cd1af23562feb0/dbt-redshift/src/dbt/adapters/redshift/relation.py#L42
    fn can_be_replaced(&self) -> bool {
        matches!(self.relation_type(), Some(RelationType::View))
    }

    // https://github.com/dbt-labs/dbt-adapters/blob/f492c919d3bd415bf5065b3cd8cd1af23562feb0/dbt-redshift/src/dbt/adapters/redshift/relation.py#L67
    fn from_config(&self, args: &[Value]) -> Result<Value, MinijinjaError> {
        let iter = ArgsIter::new(current_function_name!(), &["config"], args);
        let config_value = iter.next_arg::<&Value>()?;
        iter.finish()?;

        Ok(Value::from_object(
            node_value_to_redshift_materialized_view(config_value)?,
        ))
    }

    // https://github.com/dbt-labs/dbt-adapters/blob/f492c919d3bd415bf5065b3cd8cd1af23562feb0/dbt-redshift/src/dbt/adapters/redshift/relation.py#L78
    fn materialized_view_config_changeset(&self, args: &[Value]) -> Result<Value, MinijinjaError> {
        let iter = ArgsIter::new(
            current_function_name!(),
            &["relation_results", "relation_config"],
            args,
        );

        let relation_results_value = iter.next_arg::<&Value>()?;
        let new_config_value = iter.next_arg::<&Value>()?;
        iter.finish()?;

        let relation_results = DescribeMaterializedViewResults::try_from(relation_results_value)
            .map_err(|e| {
                MinijinjaError::new(
                    MinijinjaErrorKind::SerdeDeserializeError,
                    format!(
                        "from_config: Failed to serialized DescribeMaterializedViewResults: {e}"
                    ),
                )
            })?;

        let existing_config = RedshiftMaterializedViewConfig::try_from(relation_results)
        .map_err(|e| {
            MinijinjaError::new(
                MinijinjaErrorKind::SerdeDeserializeError,
                format!("materialized_view_config_changeset: Failed to deserialize RedshiftMaterializedViewConfig: {e}"),
            )
        })?;

        let new_materialized_view_config =
            node_value_to_redshift_materialized_view(new_config_value)?;

        let changeset = RedshiftMaterializedViewConfigChangeset::new(
            existing_config,
            new_materialized_view_config,
        );

        if changeset.has_changes() {
            Ok(Value::from_object(changeset))
        } else {
            Ok(Value::from(None::<()>))
        }
    }
}

fn node_value_to_redshift_materialized_view(
    node_value: &Value,
) -> Result<RedshiftMaterializedViewConfig, MinijinjaError> {
    let config_wrapper = InternalDbtNodeWrapper::deserialize(node_value).map_err(|e| {
        MinijinjaError::new(
            MinijinjaErrorKind::SerdeDeserializeError,
            format!("Failed to deserialize InternalDbtNodeWrapper: {e}"),
        )
    })?;

    let model = match config_wrapper {
        InternalDbtNodeWrapper::Model(model) => model,
        _ => {
            return Err(MinijinjaError::new(
                MinijinjaErrorKind::InvalidOperation,
                "Expected a model node",
            ));
        }
    };

    if model.__base_attr__.materialized != DbtMaterialization::MaterializedView {
        return Err(MinijinjaError::new(
            MinijinjaErrorKind::InvalidOperation,
            format!(
                "Unsupported operation for materialization type {}",
                &model.__base_attr__.materialized
            ),
        ));
    }

    RedshiftMaterializedViewConfig::try_from(&*model).map_err(|e| {
        MinijinjaError::new(
            MinijinjaErrorKind::SerdeDeserializeError,
            format!("Failed to deserialize RedshiftMaterializedViewConfig: {e}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_schemas::{dbt_types::RelationType, schemas::relations::DEFAULT_RESOLVED_QUOTING};

    #[test]
    fn test_try_new_via_static_base_relation() {
        let relation = RedshiftRelationType(DEFAULT_RESOLVED_QUOTING)
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
            "\"d\".\"s\".\"i\""
        );
        assert_eq!(relation.relation_type().unwrap(), RelationType::Table);
    }
}
