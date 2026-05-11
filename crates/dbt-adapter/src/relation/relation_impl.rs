use crate::information_schema::InformationSchema;
use crate::relation::RelationChangeSet;
use crate::relation::databricks;
use crate::relation::duckdb_should_include_database;
use crate::relation::redshift::materialized_view_config::{
    DescribeMaterializedViewResults, RedshiftMaterializedViewConfig,
    RedshiftMaterializedViewConfigChangeset,
};
use crate::relation::{RelationObject, StaticBaseRelation};

use dbt_adapter_core::AdapterType;
use dbt_adapter_sql::ident::max_identifier_length;
use dbt_common::{ErrorCode, FsResult, fs_err};
use dbt_frontend_common::ident::Identifier;
use dbt_schema_store::CanonicalFqn;
use dbt_schemas::schemas::InternalDbtNodeWrapper;
use dbt_schemas::schemas::common::DbtMaterialization;
use dbt_schemas::schemas::relations::base::{
    BaseRelation, BaseRelationProperties, Policy, RelationPath,
};

use arrow::array::RecordBatch;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::common::ResolvedQuoting;
use minijinja::Value;
use serde::Deserialize;

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

/// A struct representing the relation type for use with static methods
#[derive(Clone, Debug)]
pub struct RelationStatic {
    pub adapter_type: AdapterType,
    pub quoting: ResolvedQuoting,
}

impl StaticBaseRelation for RelationStatic {
    fn try_new(
        &self,
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        custom_quoting: Option<ResolvedQuoting>,
        temporary: Option<bool>,
    ) -> Result<Value, minijinja::Error> {
        let include_policy = match self.adapter_type {
            // Local DuckDB uses schema.table; attached catalogs need database.schema.table.
            AdapterType::DuckDB => Policy::new(
                duckdb_should_include_database(database.as_deref()),
                true,
                true,
            ),
            // Exasol does not support 3-part db.schema.table names.
            AdapterType::Exasol => Policy::new(false, true, true),
            AdapterType::Salesforce => Policy::new(false, false, true),
            _ => Policy::trues(),
        };
        Ok(RelationObject::new(Arc::new(Relation::new_with_policy(
            self.adapter_type,
            RelationPath {
                database: database.filter(|s| !s.is_empty()),
                schema,
                identifier,
            },
            relation_type,
            include_policy,
            custom_quoting.unwrap_or(self.quoting),
            // api.Relation.create doesn't set everything below
            None,
            false,
            temporary.unwrap_or(false),
        )?))
        .into_value())
    }

    fn get_adapter_type(&self) -> String {
        self.adapter_type.as_ref().to_string()
    }
}

/// A relation object for the adapter
#[derive(Clone, Debug)]
pub struct Relation {
    /// The adapter type this relation instance is for.
    pub adapter_type: AdapterType,
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
    /// Metadata about the relation
    pub metadata: Option<BTreeMap<String, String>>,
    /// Whether the relation is a delta table
    pub is_delta: bool,
    /// Constraints to be created with the table
    pub create_constraints: Vec<databricks::typed_constraint::TypedConstraint>,
    /// Constraints to be applied during ALTER operations
    pub alter_constraints: Vec<databricks::typed_constraint::TypedConstraint>,
    /// Whether the relation is a temporary view (session-scoped).
    pub temporary: bool,
}

impl BaseRelationProperties for Relation {
    fn include_policy(&self) -> Policy {
        self.include_policy
    }

    fn quote_policy(&self) -> Policy {
        self.quote_policy
    }

    fn get_database(&self) -> FsResult<String> {
        use AdapterType::*;
        match self.adapter_type {
            Databricks | Fabric | Postgres | Redshift | Salesforce => {
                self.path.database.clone().ok_or_else(|| {
                    fs_err!(
                        ErrorCode::InvalidConfig,
                        "database is required for {} relation",
                        self.adapter_type.as_ref()
                    )
                })
            }
            Spark => Ok(self.path.database.clone().unwrap_or_default()),
            _ => Ok(self.path.database.clone().unwrap_or_default()),
        }
    }

    fn get_schema(&self) -> FsResult<String> {
        match self.adapter_type {
            // FIXME: this will cause trouble in a few known places
            // In unit_test.rs, where this sed to build SQL literals
            // In schema_cache where we expect 3 part fqn, non-applicable for now since static analysis is unsupported for Salesforce
            AdapterType::Salesforce => Ok(String::new()),
            _ => self.path.schema.clone().ok_or_else(|| {
                fs_err!(
                    ErrorCode::InvalidConfig,
                    "schema is required for {} relation",
                    self.adapter_type.as_ref()
                )
            }),
        }
    }

    fn get_identifier(&self) -> FsResult<String> {
        self.path.identifier.clone().ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "identifier is required for {} relation",
                self.adapter_type.as_ref()
            )
        })
    }

    fn get_canonical_fqn(&self) -> FsResult<CanonicalFqn> {
        use AdapterType::*;

        let db_str = self.get_database()?;
        let schema_str = self.get_schema()?;
        let ident_str = self.get_identifier()?;

        let db = if self.quote_policy().database {
            db_str
        } else {
            match self.adapter_type {
                Fabric => db_str,
                Salesforce => db_str.to_ascii_uppercase(),
                _ => db_str.to_ascii_lowercase(),
            }
        };

        let schema = if self.quote_policy().database {
            schema_str
        } else {
            match self.adapter_type {
                Fabric => schema_str,
                Salesforce => schema_str.to_ascii_uppercase(),
                _ => schema_str.to_ascii_lowercase(),
            }
        };

        let ident = if self.quote_policy().database {
            ident_str
        } else {
            match self.adapter_type {
                Fabric => ident_str,
                Salesforce => ident_str.to_ascii_uppercase(),
                _ => ident_str.to_ascii_lowercase(),
            }
        };

        Ok(CanonicalFqn::new(
            &Identifier::new(db),
            &Identifier::new(schema),
            &Identifier::new(ident),
        ))
    }
}

impl Relation {
    /// Creates a new relation
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        adapter_type: AdapterType,
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        native_schema: Option<RecordBatch>,
        custom_quoting: ResolvedQuoting,
        metadata: Option<BTreeMap<String, String>>,
        is_delta: bool,
        temporary: bool,
    ) -> Self {
        let include_policy = match adapter_type {
            AdapterType::DuckDB => Policy::new(
                duckdb_should_include_database(database.as_deref()),
                true,
                true,
            ),
            _ => Policy::trues(),
        };
        Self {
            adapter_type,
            path: RelationPath {
                database: database.filter(|s| !s.is_empty()),
                schema,
                identifier,
            },
            relation_type,
            include_policy,
            quote_policy: custom_quoting,
            native_schema,
            metadata,
            is_delta,
            create_constraints: Vec::new(),
            alter_constraints: Vec::new(),
            temporary,
        }
    }

    pub fn new_fabric(
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        custom_quoting: ResolvedQuoting,
    ) -> Self {
        Self::new(
            AdapterType::Fabric,
            database,
            schema,
            identifier,
            relation_type,
            None,
            custom_quoting,
            None,
            false,
            false,
        )
    }

    /// Create a new relation with a policy
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_policy(
        adapter_type: AdapterType,
        path: RelationPath,
        relation_type: Option<RelationType>,
        include_policy: Policy,
        quote_policy: Policy,
        metadata: Option<BTreeMap<String, String>>,
        is_delta: bool,
        temporary: bool,
    ) -> Result<Self, minijinja::Error> {
        if let (Some(ident), Some(_relation_type), Some(max_len)) = (
            &path.identifier,
            &relation_type,
            max_identifier_length(adapter_type),
        ) {
            use minijinja::ErrorKind::InvalidOperation;
            if ident.len() > max_len.into() {
                let message = format!(
                    "Relation name '{}' is longer than {} characters",
                    ident, max_len
                );
                return Err(minijinja::Error::new(InvalidOperation, message));
            }
        }

        Ok(Self {
            adapter_type,
            path,
            relation_type,
            include_policy,
            quote_policy,
            native_schema: None,
            metadata,
            is_delta,
            create_constraints: Vec::new(),
            alter_constraints: Vec::new(),
            temporary,
        })
    }

    /// Add a constraint, routing to create_constraints or alter_constraints based on type
    pub fn add_constraint(&mut self, constraint: databricks::typed_constraint::TypedConstraint) {
        use dbt_schemas::schemas::common::ConstraintType;

        match constraint.constraint_type() {
            ConstraintType::Check => {
                self.alter_constraints.push(constraint);
            }
            _ => {
                self.create_constraints.push(constraint);
            }
        }
    }

    /// Create a copy of the relation with the given constraints added.
    ///
    /// Reference: https://github.com/databricks/dbt-databricks/blob/25caa2a14ed0535f08f6fd92e29b39df1f453e4d/dbt/adapters/databricks/relation.py#L213-L217
    pub fn enrich(&self, constraints: &[databricks::typed_constraint::TypedConstraint]) -> Self {
        let mut relation = self.clone();
        for constraint in constraints {
            relation.add_constraint(constraint.clone());
        }
        relation
    }

    /// Render constraint DDL for CREATE TABLE.
    ///
    /// Reference: https://github.com/databricks/dbt-databricks/blob/25caa2a14ed0535f08f6fd92e29b39df1f453e4d/dbt/adapters/databricks/relation.py#L219-L221
    pub fn render_constraints_for_create(&self) -> String {
        self.create_constraints
            .iter()
            .map(|c| c.render())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl BaseRelation for Relation {
    /// Whether the relation is a system table or not
    fn is_system(&self) -> bool {
        match self.adapter_type {
            AdapterType::Databricks | AdapterType::Spark => {
                // It might be relation under a `information_schema` schema or a `system` catalog
                // For example, system.billing.list_prices or [database].information_schema.tables
                // are both system tables
                self.path.database.as_ref().map(|s| s.to_lowercase())
                    == Some(databricks::SYSTEM_DATABASE.to_string())
                    || self.path.schema.as_ref().map(|s| s.to_lowercase())
                        == Some(databricks::INFORMATION_SCHEMA_SCHEMA.to_string())
            }
            _ => false,
        }
    }

    fn has_information(&self) -> bool {
        self.metadata.is_some()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_owned(&self) -> Arc<dyn BaseRelation> {
        Arc::new(self.clone())
    }

    fn create_from(&self) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        unimplemented!("{} relation creation from Jinja values", self.adapter_type)
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
        self.adapter_type
    }

    fn include_inner(&self, policy: Policy) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        let mut relation = Self::new_with_policy(
            self.adapter_type,
            self.path.clone(),
            self.relation_type,
            policy,
            self.quote_policy,
            self.metadata.clone(),
            self.is_delta,
            self.temporary,
        )?;

        // Preserve constraints
        relation.create_constraints = self.create_constraints.clone();
        relation.alter_constraints = self.alter_constraints.clone();

        Ok(Arc::new(relation))
    }

    fn is_hive_metastore(&self) -> bool {
        match self.adapter_type {
            AdapterType::Databricks | AdapterType::Spark => {
                // Match Python dbt-databricks semantics:
                // def is_hive_metastore(database: Optional[str], temporary: Optional[bool] = False) -> bool:
                //     return (database is None or database.lower() == "hive_metastore") and not temporary
                //
                // Note: The `temporary` field only tracks Unity Catalog temporary tables, not Hive Metastore temporary views.
                // Unity Catalog temporary tables are never considered to be in Hive Metastore.
                (self.path.database.is_none()
                    || self.path.database.as_ref().map(|s| s.to_lowercase())
                        == Some(databricks::DEFAULT_DATABRICKS_DATABASE.to_string()))
                    && !self.temporary
            }
            _ => false,
        }
    }

    fn is_temporary(&self) -> bool {
        self.temporary
    }

    fn is_delta(&self) -> bool {
        self.is_delta
    }

    fn set_is_delta(&mut self, is_delta: Option<bool>) {
        match self.adapter_type {
            AdapterType::Databricks | AdapterType::Spark => {
                self.is_delta = is_delta.unwrap_or(self.is_delta);
            }
            _ => {}
        }
    }

    fn is_materialized_view(&self) -> bool {
        let result = matches!(self.relation_type, Some(RelationType::MaterializedView));
        result
    }

    fn can_be_replaced(&self) -> bool {
        match self.adapter_type {
            AdapterType::Redshift => matches!(self.relation_type(), Some(RelationType::View)),
            _ => matches!(
                self.relation_type(),
                Some(RelationType::Table) | Some(RelationType::View)
            ),
        }
    }

    fn from_config(&self, config: &Value) -> Result<Value, minijinja::Error> {
        match self.adapter_type {
            AdapterType::Redshift => Ok(Value::from_object(
                node_value_to_redshift_materialized_view(config)?,
            )),
            _ => Err(minijinja::Error::new(
                minijinja::ErrorKind::InvalidOperation,
                "from_config: Only available for Snowflake and Redshift",
            )),
        }
    }

    fn normalize_component(&self, component: &str) -> String {
        match self.adapter_type {
            AdapterType::Salesforce => component.to_string(),
            _ => component.to_lowercase(),
        }
    }

    fn create_relation(
        &self,
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        custom_quoting: Policy,
    ) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        let include_policy = match self.adapter_type {
            AdapterType::DuckDB => Policy::new(
                duckdb_should_include_database(database.as_deref()),
                true,
                true,
            ),
            AdapterType::Postgres => self.include_policy,
            AdapterType::Salesforce => Policy::new(false, false, true),
            _ => Policy::trues(),
        };
        Ok(Arc::new(Relation::new_with_policy(
            self.adapter_type,
            RelationPath {
                database: database.filter(|s| !s.is_empty()),
                schema,
                identifier,
            },
            relation_type,
            include_policy,
            custom_quoting,
            self.metadata.clone(),
            self.is_delta,
            self.temporary,
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

    fn relation_max_name_length(&self) -> Result<u32, minijinja::Error> {
        Ok(max_identifier_length(self.adapter_type)
            .map(|v| v.get().try_into().unwrap_or(u32::MAX))
            .unwrap_or(u32::MAX))
    }

    fn materialized_view_config_changeset(
        &self,
        relation_results_value: &Value,
        new_config_value: &Value,
    ) -> Result<Value, minijinja::Error> {
        match self.adapter_type {
            // FIXME(serramatutu): port over to RelationConfig v2
            AdapterType::Redshift => {
                let relation_results = DescribeMaterializedViewResults::try_from(
                    relation_results_value,
                )
                .map_err(|e| {
                    minijinja::Error::new(
                        minijinja::ErrorKind::SerdeDeserializeError,
                        format!(
                            "from_config: Failed to serialized DescribeMaterializedViewResults: {e}"
                        ),
                    )
                })?;

                let existing_config = RedshiftMaterializedViewConfig::try_from(relation_results)
                    .map_err(|e| {
                        minijinja::Error::new(
                            minijinja::ErrorKind::SerdeDeserializeError,
                            format!(
                                "materialized_view_config_changeset: Failed to deserialize RedshiftMaterializedViewConfig: {e}"
                            ),
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
            _ => unimplemented!("Available only for BigQuery and Redshift"),
        }
    }
}

// FIXME(serramatutu): this should be deleted from here once Redshift Materialized
// Views are migrated to RelationConfig v2.
fn node_value_to_redshift_materialized_view(
    node_value: &Value,
) -> Result<RedshiftMaterializedViewConfig, minijinja::Error> {
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

    if model.__base_attr__.materialized != DbtMaterialization::MaterializedView {
        return Err(minijinja::Error::new(
            minijinja::ErrorKind::InvalidOperation,
            format!(
                "Unsupported operation for materialization type {}",
                &model.__base_attr__.materialized
            ),
        ));
    }

    RedshiftMaterializedViewConfig::try_from(&*model).map_err(|e| {
        minijinja::Error::new(
            minijinja::ErrorKind::SerdeDeserializeError,
            format!("Failed to deserialize RedshiftMaterializedViewConfig: {e}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_schemas::{dbt_types::RelationType, schemas::relations::DEFAULT_RESOLVED_QUOTING};

    #[test]
    fn test_try_new_via_static_base_relation_postgres() {
        let relation_type = RelationStatic {
            adapter_type: AdapterType::Postgres,
            quoting: DEFAULT_RESOLVED_QUOTING,
        };
        let relation = relation_type
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

    #[test]
    fn test_try_new_via_static_base_relation() {
        let relation_type = RelationStatic {
            adapter_type: AdapterType::Databricks,
            quoting: DEFAULT_RESOLVED_QUOTING,
        };
        let relation = relation_type
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
    fn test_try_new_via_static_base_relation_with_default_database() {
        let relation_type = RelationStatic {
            adapter_type: AdapterType::Databricks,
            quoting: DEFAULT_RESOLVED_QUOTING,
        };
        let relation = relation_type
            .try_new(
                None,
                Some("s".to_string()),
                Some("i".to_string()),
                Some(RelationType::Table),
                Some(DEFAULT_RESOLVED_QUOTING),
                None,
            )
            .unwrap();

        let relation = relation.downcast_object::<RelationObject>().unwrap();
        assert_eq!(relation.inner().render_self_as_str(), "`s`.`i`");
    }

    #[test]
    fn test_render_lowercases_identifiers() {
        // Python DatabricksRelation.render() calls super().render().lower(),
        // lowercasing the entire rendered relation string.
        // Databricks backtick-quoted identifiers are case-insensitive, so
        // this is semantically correct and matches Mantle's behavior.
        let relation_type = RelationStatic {
            adapter_type: AdapterType::Databricks,
            quoting: DEFAULT_RESOLVED_QUOTING,
        };
        let relation = relation_type
            .try_new(
                Some("dbt".to_string()),
                Some("dbt_staging".to_string()),
                Some("stg_pinterest_campaign_INT".to_string()),
                Some(RelationType::Table),
                Some(DEFAULT_RESOLVED_QUOTING),
                None,
            )
            .unwrap();

        let relation = relation.downcast_object::<RelationObject>().unwrap();
        assert_eq!(
            relation.inner().render_self_as_str(),
            "`dbt`.`dbt_staging`.`stg_pinterest_campaign_int`"
        );
    }

    #[test]
    fn test_is_system() {
        // Test system database (lowercase)
        let relation = Relation::new(
            AdapterType::Databricks,
            Some("system".to_string()),
            Some("schema".to_string()),
            Some("table".to_string()),
            Some(RelationType::Table),
            None,
            DEFAULT_RESOLVED_QUOTING,
            None,
            false,
            false,
        );
        assert!(relation.is_system());

        // Test system database (uppercase - case insensitive)
        let relation = Relation::new(
            AdapterType::Databricks,
            Some("SYSTEM".to_string()),
            Some("schema".to_string()),
            Some("table".to_string()),
            Some(RelationType::Table),
            None,
            DEFAULT_RESOLVED_QUOTING,
            None,
            false,
            false,
        );
        assert!(relation.is_system());

        // Test information_schema schema (lowercase)
        let relation = Relation::new(
            AdapterType::Databricks,
            Some("database".to_string()),
            Some("information_schema".to_string()),
            Some("table".to_string()),
            Some(RelationType::Table),
            None,
            DEFAULT_RESOLVED_QUOTING,
            None,
            false,
            false,
        );
        assert!(relation.is_system());

        // Test information_schema schema (uppercase - case insensitive)
        let relation = Relation::new(
            AdapterType::Databricks,
            Some("database".to_string()),
            Some("INFORMATION_SCHEMA".to_string()),
            Some("table".to_string()),
            Some(RelationType::Table),
            None,
            DEFAULT_RESOLVED_QUOTING,
            None,
            false,
            false,
        );
        assert!(relation.is_system());

        // Test neither system database nor information_schema schema
        let relation = Relation::new(
            AdapterType::Databricks,
            Some("regular_database".to_string()),
            Some("regular_schema".to_string()),
            Some("table".to_string()),
            Some(RelationType::Table),
            None,
            DEFAULT_RESOLVED_QUOTING,
            None,
            false,
            false,
        );
        assert!(!relation.is_system());

        // Test with None database and non-information_schema schema
        let relation = Relation::new(
            AdapterType::Databricks,
            None,
            Some("regular_schema".to_string()),
            Some("table".to_string()),
            Some(RelationType::Table),
            None,
            DEFAULT_RESOLVED_QUOTING,
            None,
            false,
            false,
        );
        assert!(!relation.is_system());

        // Test with non-system database and None schema
        let relation = Relation::new(
            AdapterType::Databricks,
            Some("regular_database".to_string()),
            None,
            Some("table".to_string()),
            Some(RelationType::Table),
            None,
            DEFAULT_RESOLVED_QUOTING,
            None,
            false,
            false,
        );
        assert!(!relation.is_system());

        // Test both system database and information_schema schema (should still be true)
        let relation = Relation::new(
            AdapterType::Databricks,
            Some("system".to_string()),
            Some("information_schema".to_string()),
            Some("table".to_string()),
            Some(RelationType::Table),
            None,
            DEFAULT_RESOLVED_QUOTING,
            None,
            false,
            false,
        );
        assert!(relation.is_system());
    }

    #[test]
    fn test_constraint_methods() {
        use crate::relation::databricks::typed_constraint::TypedConstraint;

        let mut relation = Relation::new(
            AdapterType::Databricks,
            Some("test_db".to_string()),
            Some("test_schema".to_string()),
            Some("test_table".to_string()),
            Some(RelationType::Table),
            None,
            DEFAULT_RESOLVED_QUOTING,
            None,
            false,
            false,
        );

        // Test check constraint goes to alter_constraints
        let check_constraint = TypedConstraint::Check {
            name: Some("positive_id".to_string()),
            expression: "id > 0".to_string(),
            columns: None,
        };
        relation.add_constraint(check_constraint);
        assert_eq!(relation.alter_constraints.len(), 1);
        assert_eq!(relation.create_constraints.len(), 0);

        // Test primary key constraint goes to create_constraints
        let pk_constraint = TypedConstraint::PrimaryKey {
            name: Some("pk_users".to_string()),
            columns: vec!["id".to_string()],
            expression: None,
        };
        relation.add_constraint(pk_constraint);
        assert_eq!(relation.alter_constraints.len(), 1);
        assert_eq!(relation.create_constraints.len(), 1);
    }
}
