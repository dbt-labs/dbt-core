use crate::information_schema::InformationSchema;
use crate::relation::RelationChangeSet;
use crate::relation::snowflake::dynamic_table::{
    DescribeDynamicTableResults, SnowflakeDynamicTableConfig, SnowflakeDynamicTableConfigChangeset,
};
use crate::relation::{RelationObject, StaticBaseRelation};

use dbt_adapter_core::AdapterType;
use dbt_common::{ErrorCode, FsResult, current_function_name, fs_err};
use dbt_frontend_common::ident::Identifier;
use dbt_schema_store::CanonicalFqn;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::InternalDbtNodeWrapper;
use dbt_schemas::schemas::common::{DbtMaterialization, DbtQuoting, ResolvedQuoting};
use dbt_schemas::schemas::relations::base::{
    BaseRelation, BaseRelationProperties, Policy, RelationPath, TableFormat,
};
use minijinja::Value;
use minijinja::arg_utils::ArgsIter;
use serde::Deserialize;

use std::any::Any;
use std::sync::Arc;

/// A struct representing the Snowflake relation type for use with static methods
#[derive(Clone, Debug, Copy)]
pub struct SnowflakeRelationType(pub ResolvedQuoting);

impl StaticBaseRelation for SnowflakeRelationType {
    fn try_new(
        &self,
        _database: Option<String>,
        _schema: Option<String>,
        _identifier: Option<String>,
        _relation_type: Option<RelationType>,
        _custom_quoting: Option<ResolvedQuoting>,
        _temporary: Option<bool>,
    ) -> Result<Value, minijinja::Error> {
        Err(minijinja::Error::new(
            minijinja::ErrorKind::InvalidOperation,
            "Not for used for SnowflakeRelationType due to custom create logic, but kept for trait compliance",
        ))
    }

    fn get_adapter_type(&self) -> String {
        "snowflake".to_string()
    }

    fn create(&self, args: &[Value]) -> Result<Value, minijinja::Error> {
        let iter = ArgsIter::new(current_function_name!(), &[], args);
        let database: Option<String> = iter.next_kwarg::<Option<String>>("database")?;
        // Support both 'schema' and 'schema_name' for backward compatibility
        let schema: Option<String> = iter.next_kwarg::<Option<String>>("schema")?;
        let identifier: Option<String> = iter.next_kwarg::<Option<String>>("identifier")?;
        let relation_type: Option<String> = iter.next_kwarg::<Option<String>>("type")?;
        let custom_quoting: Option<Value> = iter.next_kwarg::<Option<Value>>("quote_policy")?;
        let table_format: Option<String> = iter.next_kwarg::<Option<String>>("table_format")?;
        let _ = iter.trailing_kwargs()?;

        let custom_quoting = custom_quoting
            .and_then(|v| DbtQuoting::deserialize(v).ok())
            .map(|v| ResolvedQuoting {
                database: v.database.unwrap_or_default(),
                identifier: v.identifier.unwrap_or_default(),
                schema: v.schema.unwrap_or_default(),
            })
            .unwrap_or(self.0);

        let table_format = if table_format.is_some_and(|s| s.eq_ignore_ascii_case("iceberg")) {
            TableFormat::Iceberg
        } else {
            TableFormat::Default
        };

        let rel = RelationObject::new(Arc::new(SnowflakeRelation::new(
            database,
            schema,
            identifier,
            relation_type.map(|s| RelationType::from(s.as_str())),
            table_format,
            custom_quoting,
        )));
        Ok(Value::from_object(rel))
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
                database: database.filter(|s| !s.is_empty()),
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

    fn to_owned(&self) -> Arc<dyn BaseRelation> {
        Arc::new(self.clone())
    }

    /// Creates a new Snowflake relation from a state and a list of values
    fn create_from(&self) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        unimplemented!("Snowflake relation creation from Jinja values")
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

    /// Helper: is this relation renamable?
    fn can_be_renamed(&self) -> bool {
        !self.is_iceberg_format()
            && matches!(
                self.relation_type(),
                Some(RelationType::Table) | Some(RelationType::View)
            )
    }

    /// Helper: is this relation replaceable?
    fn can_be_replaced(&self) -> bool {
        matches!(
            self.relation_type(),
            Some(RelationType::Table) | Some(RelationType::View) | Some(RelationType::DynamicTable)
        )
    }

    // https://github.com/dbt-labs/dbt-adapters/blob/816d190c9e31391a48cee979bd049aeb34c89ad3/dbt-snowflake/src/dbt/adapters/snowflake/relation.py#L81
    fn from_config(&self, config: &Value) -> Result<Value, minijinja::Error> {
        Ok(Value::from_object(node_value_to_snowflake_dynamic_table(
            config,
        )?))
    }

    // https://github.com/dbt-labs/dbt-adapters/blob/292d17301eff3c8a972fcd57f7deb3aac4c8a3cb/dbt-snowflake/src/dbt/adapters/snowflake/relation.py#L92
    fn dynamic_table_config_changeset(
        &self,
        relation_results_value: &Value,
        relation_config_value: &Value,
    ) -> Result<Value, minijinja::Error> {
        let relation_results = DescribeDynamicTableResults::try_from(relation_results_value)
            .map_err(|e| {
                minijinja::Error::new(
                    minijinja::ErrorKind::SerdeDeserializeError,
                    format!("from_config: Failed to serialize DescribeDynamicTableResults: {e}"),
                )
            })?;

        let existing_config = SnowflakeDynamicTableConfig::try_from(relation_results)
            .map_err(|e| {
                minijinja::Error::new(
                    minijinja::ErrorKind::SerdeDeserializeError, format!("dynamic_table_config_changeset: Failed to deserialize SnowflakeDynamicTableConfig: {e}")
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

    fn adapter_type(&self) -> AdapterType {
        AdapterType::Snowflake
    }

    // https://github.com/dbt-labs/dbt-adapters/blob/2a94cc75dba1f98fa5caff1f396f5af7ee444598/dbt-snowflake/src/dbt/adapters/snowflake/relation.py#L223
    fn needs_to_drop(
        &self,
        old_relation: Option<Arc<dyn BaseRelation>>,
    ) -> Result<bool, minijinja::Error> {
        if let Some(old_relation) = old_relation {
            // core does only checks this for table conversions since dynamic tables
            // are expected to be rebuilt cross-catalog using full refresh mode
            if old_relation.is_table() {
                // invoke drop for table -> Iceberg or Iceberg -> table
                let old_relation_table_format = old_relation
                    .as_any()
                    .downcast_ref::<SnowflakeRelation>()
                    .unwrap()
                    .table_format;
                Ok(self.table_format != old_relation_table_format)
            } else {
                // An existing view must be dropped for model to build into a table.
                Ok(true)
            }
        } else {
            Ok(false)
        }
    }

    fn is_iceberg_format(&self) -> bool {
        matches!(self.table_format, TableFormat::Iceberg)
    }
    /// Returns the appropriate DDL prefix for creating a table
    ///
    /// # Arguments
    /// * `model_config` - The RunConfig containing model configuration
    /// * `temporary` - Whether the table should be temporary
    ///
    /// # Returns
    /// One of: "temporary", "iceberg", "transient", or "" (empty string)
    fn get_ddl_prefix_for_create(
        &self,
        config: Value,
        temporary: bool,
    ) -> Result<String, minijinja::Error> {
        if temporary {
            return Ok("temporary".to_string());
        }

        // Extract legacy Iceberg configuration values found in a model config.
        // https://docs.getdbt.com/docs/mesh/iceberg/snowflake-iceberg-support#example-configuration
        let is_iceberg = config
            .get_item(&Value::from("table_format"))
            .is_ok_and(|v| v.as_str().is_some_and(|s| s == "iceberg"));

        let transient_explicitly_set_true = config
            .get_item(&Value::from("transient"))
            .map(|v| v.is_true())
            .unwrap_or(false);

        if is_iceberg {
            if transient_explicitly_set_true {
                eprintln!(
                    "Warning: Iceberg format relations cannot be transient. Please remove either \
                            the transient=true or iceberg config options from {}.{}.{}. If left unmodified, \
                            dbt will ignore 'transient'.",
                    self.path.database.as_deref().unwrap_or(""),
                    self.path.schema.as_deref().unwrap_or(""),
                    self.path.identifier.as_deref().unwrap_or("")
                );
            }
            return Ok("iceberg".to_string());
        }

        let is_transient = config
            .get_item(&Value::from("transient"))
            .map(|v| v.is_true() || v.is_undefined())
            .unwrap_or(true);

        Ok(if is_transient {
            "transient".to_string()
        } else {
            String::new()
        })
    }

    fn get_ddl_prefix_for_alter(&self) -> Result<String, minijinja::Error> {
        if self.table_format == TableFormat::Iceberg {
            Ok("iceberg".to_string())
        } else {
            Ok(String::new())
        }
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/2a94cc75dba1f98fa5caff1f396f5af7ee444598/dbt-snowflake/src/dbt/adapters/snowflake/relation.py#L206
    fn get_iceberg_ddl_options(
        &self,
        runtime_model_config: Value,
    ) -> Result<String, minijinja::Error> {
        // If the base_location_root config is supplied, overwrite the default value ("_dbt/")
        let mut base_location = runtime_model_config
            .get_attr("base_location_root")?
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
                minijinja::Error::new(minijinja::ErrorKind::NonKey, "external_volume is required")
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

        Ok(result)
    }

    fn include_inner(&self, policy: Policy) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        let mut relation = self.clone();
        relation.include_policy = policy;

        Ok(Arc::new(relation))
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
    ) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        Ok(Arc::new(SnowflakeRelation::new(
            database,
            schema,
            identifier,
            relation_type,
            self.table_format,
            custom_quoting,
        )))
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

fn node_value_to_snowflake_dynamic_table(
    node_value: &Value,
) -> Result<SnowflakeDynamicTableConfig, minijinja::Error> {
    let config_wrapper = InternalDbtNodeWrapper::deserialize(node_value).map_err(|e| {
        minijinja::Error::new(
            minijinja::ErrorKind::SerdeDeserializeError,
            format!("Failed to deserialize InternalDbtNodeWrapper: {e}"),
        )
    })?;

    let model = match config_wrapper {
        InternalDbtNodeWrapper::Model(model) => model,
        _ => {
            return Err(minijinja::Error::new(
                minijinja::ErrorKind::InvalidOperation,
                "Expected a model node",
            ));
        }
    };

    if model.__base_attr__.materialized != DbtMaterialization::DynamicTable {
        return Err(minijinja::Error::new(
            minijinja::ErrorKind::InvalidOperation,
            format!(
                "Unsupported operation for materialization type {}",
                &model.__base_attr__.materialized
            ),
        ));
    }

    SnowflakeDynamicTableConfig::try_from(&*model).map_err(|e| {
        minijinja::Error::new(
            minijinja::ErrorKind::SerdeDeserializeError,
            format!("Failed to deserialize SnowflakeDynamicTableConfig: {e}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_schemas::{dbt_types::RelationType, schemas::relations::DEFAULT_RESOLVED_QUOTING};

    #[test]
    fn test_snowflake_create_via_static_base_relation() {
        let values = [
            Value::from("d"),
            Value::from("s"),
            Value::from("i"),
            Value::from("table"),
            Value::from("{database: true, identifier: true, schema: true}"),
        ];

        let relation = SnowflakeRelationType(DEFAULT_RESOLVED_QUOTING)
            .create(&values)
            .unwrap();

        let relation = relation.downcast_object::<RelationObject>().unwrap();
        assert_eq!(relation.inner().render_self_as_str(), r#""d"."s"."i""#);
        assert_eq!(relation.relation_type().unwrap(), RelationType::Table);
    }
}
