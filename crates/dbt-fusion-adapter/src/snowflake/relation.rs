use crate::information_schema::InformationSchema;
use crate::relation_object::{RelationObject, StaticBaseRelation};
use crate::snowflake::relation_configs::dynamic_table::{
    DescribeDynamicTableResults, SnowflakeDynamicTableConfig, SnowflakeDynamicTableConfigChangeset,
};

use dbt_common::{ErrorCode, FsResult, current_function_name, fs_err};
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::common::{DbtMaterialization, ResolvedQuoting};
use dbt_schemas::schemas::relations::base::{
    BaseRelation, BaseRelationProperties, Policy, RelationPath, TableFormat,
};
use dbt_schemas::schemas::{InternalDbtNodeWrapper, RelationChangeSet};
use minijinja::arg_utils::{ArgsIter, check_num_args};
use minijinja::{
    Error as MinijinjaError, ErrorKind as MinijinjaErrorKind, State, Value, arg_utils::ArgParser,
};
use serde::Deserialize;

use std::any::Any;
use std::sync::Arc;

/// A struct representing the Snowflake relation type for use with static methods
#[derive(Clone, Debug, Copy)]
pub struct SnowflakeRelationType(pub ResolvedQuoting);

impl StaticBaseRelation for SnowflakeRelationType {
    fn try_new(
        &self,
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        custom_quoting: Option<ResolvedQuoting>,
    ) -> Result<Value, MinijinjaError> {
        Ok(RelationObject::new(Arc::new(SnowflakeRelation::new(
            database,
            schema,
            identifier,
            relation_type,
            TableFormat::Default,
            custom_quoting.unwrap_or(self.0),
        )))
        .into_value())
    }

    fn get_adapter_type(&self) -> String {
        "snowflake".to_string()
    }
}

/// A struct representing a Snowflake relation
#[derive(Clone, Debug)]
pub struct SnowflakeRelation {
    /// The path of the relation
    pub path: RelationPath,
    /// The relation type (default: None)
    pub relation_type: Option<RelationType>,
    /// The table format of the relation
    pub table_format: TableFormat,
    /// Include policy
    pub include_policy: Policy,
    /// Quote policy
    pub quote_policy: Policy,
}

impl BaseRelationProperties for SnowflakeRelation {
    fn quote_policy(&self) -> Policy {
        self.quote_policy
    }

    fn include_policy(&self) -> Policy {
        self.include_policy
    }

    fn quote_character(&self) -> char {
        '"'
    }

    fn get_database(&self) -> FsResult<String> {
        self.path.database.clone().ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "database is required for snowflake relation",
            )
        })
    }

    fn get_schema(&self) -> FsResult<String> {
        self.path.schema.clone().ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "schema is required for snowflake relation",
            )
        })
    }

    fn get_identifier(&self) -> FsResult<String> {
        self.path.identifier.clone().ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "identifier is required for snowflake relation",
            )
        })
    }
}

impl SnowflakeRelation {
    /// Creates a new Snowflake relation
    pub fn new(
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        table_format: TableFormat,
        custom_quoting: ResolvedQuoting,
    ) -> Self {
        Self {
            path: RelationPath {
                database,
                schema,
                identifier,
            },
            relation_type,
            table_format,
            include_policy: Policy::enabled(),
            // https://github.com/dbt-labs/dbt-core/blob/main/env/lib/python3.12/site-packages/dbt/adapters/snowflake/relation_configs/policies.py#L22
            // default is all disabled
            quote_policy: custom_quoting,
        }
    }
}

impl BaseRelation for SnowflakeRelation {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Creates a new Snowflake relation from a state and a list of values
    fn create_from(&self, _: &State, _: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("Snowflake relation creation from Jinja values")
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
        matches!(
            self.relation_type(),
            Some(RelationType::Table) | Some(RelationType::View)
        )
        // TODO: and also is not iceberg_format
    }

    /// Helper: is this relation replaceable?
    fn can_be_replaced(&self) -> bool {
        matches!(
            self.relation_type(),
            Some(RelationType::Table) | Some(RelationType::View) | Some(RelationType::DynamicTable)
        )
    }

    // https://github.com/dbt-labs/dbt-adapters/blob/816d190c9e31391a48cee979bd049aeb34c89ad3/dbt-snowflake/src/dbt/adapters/snowflake/relation.py#L81
    fn from_config(&self, args: &[Value]) -> Result<Value, MinijinjaError> {
        let iter = ArgsIter::new(current_function_name!(), &["config"], args);
        let config_value = iter.next_arg::<&Value>()?;
        iter.finish()?;

        Ok(Value::from_object(node_value_to_snowflake_dynamic_table(
            config_value,
        )?))
    }

    // https://github.com/dbt-labs/dbt-adapters/blob/292d17301eff3c8a972fcd57f7deb3aac4c8a3cb/dbt-snowflake/src/dbt/adapters/snowflake/relation.py#L92
    fn dynamic_table_config_changeset(&self, args: &[Value]) -> Result<Value, MinijinjaError> {
        let iter = ArgsIter::new(
            current_function_name!(),
            &["relation_results", "relation_config"],
            args,
        );

        let relation_results_value = iter.next_arg::<&Value>()?;
        let relation_config_value = iter.next_arg::<&Value>()?;
        iter.finish()?;

        let relation_results = DescribeDynamicTableResults::try_from(relation_results_value)
            .map_err(|e| {
                MinijinjaError::new(
                    MinijinjaErrorKind::SerdeDeserializeError,
                    format!("from_config: Failed to serialize DescribeDynamicTableResults: {e}"),
                )
            })?;

        let existing_config = SnowflakeDynamicTableConfig::try_from(relation_results)
            .map_err(|e| {
                MinijinjaError::new(
                    MinijinjaErrorKind::SerdeDeserializeError, format!("dynamic_table_config_changeset: Failed to deserialize SnowflakeDynamicTableConfig: {e}")
                )
            })?;

        let new_config = node_value_to_snowflake_dynamic_table(relation_config_value)?;

        let changeset = SnowflakeDynamicTableConfigChangeset::new(existing_config, new_config);

        if changeset.has_changes() {
            Ok(Value::from_object(changeset))
        } else {
            Ok(Value::from(()))
        }
    }

    fn quoted(&self, s: &str) -> String {
        format!("\"{s}\"")
    }

    /// Returns the relation type
    fn relation_type(&self) -> Option<RelationType> {
        self.relation_type
    }

    fn as_value(&self) -> Value {
        RelationObject::new(Arc::new(self.clone())).into_value()
    }

    fn adapter_type(&self) -> Option<String> {
        Some("snowflake".to_string())
    }

    // https://github.com/dbt-labs/dbt-adapters/blob/2a94cc75dba1f98fa5caff1f396f5af7ee444598/dbt-snowflake/src/dbt/adapters/snowflake/relation.py#L223
    fn needs_to_drop(&self, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        let value = parser.get::<Value>("old_relation").unwrap();

        if let Some(old_relation) = value.downcast_object_ref::<RelationObject>() {
            let old_relation = old_relation.inner();
            if old_relation.is_table() {
                // TODO: iceberg-related code
                Ok(Value::from(false))
            } else {
                // An existing view must be dropped for model to build into a table.
                Ok(Value::from(true))
            }
        } else {
            Ok(Value::from(false))
        }
    }

    /// Returns the appropriate DDL prefix for creating a table
    ///
    /// # Arguments
    /// * `model_config` - The RunConfig containing model configuration
    /// * `temporary` - Whether the table should be temporary
    ///
    /// # Returns
    /// One of: "temporary", "iceberg", "transient", or "" (empty string)
    fn get_ddl_prefix_for_create(&self, args: &[Value]) -> Result<Value, MinijinjaError> {
        // Temporary tables take precedence over other options
        let mut arg_parser = ArgParser::new(args, None);
        let config = arg_parser.get::<Value>("config").unwrap();
        let temporary = arg_parser.get::<bool>("temporary").unwrap();

        if temporary {
            return Ok(Value::from("temporary"));
        }

        // Extract configuration values
        let is_iceberg = config
            .get_item(&Value::from("iceberg"))
            .map(|v| v.is_true())
            .unwrap_or(false);

        let transient_explicitly_set_true = config
            .get_item(&Value::from("transient"))
            .map(|v| v.is_true())
            .unwrap_or(false);

        // Check for Iceberg format
        if is_iceberg {
            // Warning if transient is explicitly set to true
            if transient_explicitly_set_true {
                eprintln!(
                    "Warning: Iceberg format relations cannot be transient. Please remove either \
                            the transient or iceberg config options from {}.{}.{}. If left unmodified, \
                            dbt will ignore 'transient'.",
                    self.path.database.as_deref().unwrap_or(""),
                    self.path.schema.as_deref().unwrap_or(""),
                    self.path.identifier.as_deref().unwrap_or("")
                );
            }
            return Ok(Value::from("iceberg"));
        }

        // Always supply transient unless explicitly set to false
        let transient = config
            .get_item(&Value::from("transient"))
            .map(|v| v.is_undefined() || v.is_true())
            .unwrap_or(true); // Default to true if not set

        match transient {
            true => Ok(Value::from("transient")),
            false => Ok(Value::from("")),
        }
    }

    fn get_ddl_prefix_for_alter(&self) -> Result<Value, MinijinjaError> {
        if self.table_format == TableFormat::Iceberg {
            Ok(Value::from("iceberg"))
        } else {
            Ok(Value::from(""))
        }
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/2a94cc75dba1f98fa5caff1f396f5af7ee444598/dbt-snowflake/src/dbt/adapters/snowflake/relation.py#L206
    fn get_iceberg_ddl_options(&self, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;

        let runtime_model_config = parser.get::<Value>("config")?;

        // If the base_location_root config is supplied, overwrite the default value ("_dbt/")
        let mut base_location = runtime_model_config
            .get_attr("base_location")?
            .as_str()
            .unwrap_or("_dbt")
            .to_string();

        base_location.push_str(&format!(
            "/{}/{}",
            self.schema_as_str().unwrap_or_default(),
            self.identifier_as_str().unwrap_or_default()
        ));

        if let Some(subpath) = runtime_model_config
            .get_attr("base_location_subpath")?
            .as_str()
        {
            base_location.push_str(&format!("/{subpath}"))
        }

        let external_volume = runtime_model_config
            .get_attr("external_volume")?
            .as_str()
            .ok_or_else(|| {
                MinijinjaError::new(MinijinjaErrorKind::NonKey, "external_volume is required")
            })?
            .to_string();

        let iceberg_ddl_predicates = format!(
            "\nexternal_volume = '{external_volume}'\ncatalog = 'snowflake'\nbase_location = '{base_location}'\n"
        );

        // Indent each line by 10 spaces
        let result = iceberg_ddl_predicates
            .lines()
            // the first argument is an empty string that then get 10 spaces padding
            .map(|line| format!("{:indent$}{line}", "", indent = 10))
            .collect::<Vec<String>>()
            .join("\n");

        Ok(Value::from(result))
    }

    fn include_inner(&self, policy: Policy) -> Result<Value, MinijinjaError> {
        let mut relation = self.clone();
        relation.include_policy = policy;

        Ok(relation.as_value())
    }

    fn normalize_component(&self, component: &str) -> String {
        component.to_uppercase()
    }

    fn create_relation(
        &self,
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        custom_quoting: Policy,
    ) -> Result<Arc<dyn BaseRelation>, MinijinjaError> {
        Ok(Arc::new(SnowflakeRelation::new(
            database,
            schema,
            identifier,
            relation_type,
            TableFormat::Default,
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
}

fn node_value_to_snowflake_dynamic_table(
    node_value: &Value,
) -> Result<SnowflakeDynamicTableConfig, MinijinjaError> {
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

    if model.__base_attr__.materialized != DbtMaterialization::DynamicTable {
        return Err(MinijinjaError::new(
            MinijinjaErrorKind::InvalidOperation,
            format!(
                "Unsupported operation for materialization type {}",
                &model.__base_attr__.materialized
            ),
        ));
    }

    SnowflakeDynamicTableConfig::try_from(&*model).map_err(|e| {
        MinijinjaError::new(
            MinijinjaErrorKind::SerdeDeserializeError,
            format!("Failed to deserialize SnowflakeDynamicTableConfig: {e}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_schemas::{dbt_types::RelationType, schemas::relations::DEFAULT_RESOLVED_QUOTING};

    #[test]
    fn test_try_new_via_static_base_relation() {
        let relation = SnowflakeRelationType(DEFAULT_RESOLVED_QUOTING)
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
            r#""d"."s"."i""#
        );
        assert_eq!(relation.relation_type().unwrap(), RelationType::Table);
    }
}
