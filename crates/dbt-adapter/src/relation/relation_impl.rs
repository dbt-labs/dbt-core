use crate::information_schema::InformationSchema;
use crate::need_quotes::need_quotes;
use crate::relation::RelationChangeSet;
use crate::relation::config_v2::RelationConfig;
use crate::relation::databricks;
use crate::relation::redshift::materialized_view_config::{
    DescribeMaterializedViewResults, RedshiftMaterializedViewConfig,
    RedshiftMaterializedViewConfigChangeset,
};
use crate::relation::snowflake::dynamic_table::{
    DescribeDynamicTableResults, SnowflakeDynamicTableConfig, SnowflakeDynamicTableConfigChangeset,
};
use crate::relation::{RelationObject, StaticBaseRelation};
use crate::value::none_value;

use dbt_adapter_core::AdapterType;
use dbt_adapter_sql::ident::max_identifier_length;
use dbt_common::{ErrorCode, FsResult, constants::DBT_CTE_PREFIX, fs_err};
use dbt_frontend_common::ident::Identifier;
use dbt_schema_store::CanonicalFqn;
use dbt_schemas::schemas::InternalDbtNodeWrapper;
use dbt_schemas::schemas::common::{DbtMaterialization, DbtQuoting};
use dbt_schemas::schemas::relations::base::{
    BaseRelation, BaseRelationProperties, Policy, RelationPath, TableFormat,
};
use dbt_schemas::schemas::serde::minijinja_value_to_typed_struct;

use arrow::array::RecordBatch;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::common::ResolvedQuoting;
use minijinja::Value;
use minijinja::arg_utils::ArgsIter;
use serde::Deserialize;

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

fn include_policy(adapter_type: AdapterType, path: &RelationPath) -> Policy {
    match adapter_type {
        AdapterType::DuckDB => Policy::new(
            path.database.as_ref().is_some_and(|db| {
                !db.is_empty()
                    && !db.eq_ignore_ascii_case("main")
                    && !db.eq_ignore_ascii_case("memory")
            }),
            true,
            true,
        ),
        AdapterType::ClickHouse | AdapterType::Exasol => Policy::new(false, true, true),
        AdapterType::Salesforce => Policy::new(false, false, true),
        _ => Policy::trues(),
    }
}

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
        let path = RelationPath {
            // https://github.com/ClickHouse/dbt-clickhouse/blob/main/dbt/adapters/clickhouse/relation.py
            database: match self.adapter_type {
                AdapterType::ClickHouse => Some(String::new()),
                _ => database.filter(|s| !s.is_empty()),
            },
            schema,
            identifier,
        };
        let include_policy = include_policy(self.adapter_type, &path);
        Ok(RelationObject::new(Arc::new(Relation::new_with_policy(
            self.adapter_type,
            path,
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

    fn create(&self, args: &[Value]) -> Result<Value, minijinja::Error> {
        match self.adapter_type {
            AdapterType::Snowflake => {
                let iter = ArgsIter::new("Relation.create", &[], args);
                let database: Option<String> = iter.next_kwarg::<Option<String>>("database")?;
                let schema: Option<String> = iter.next_kwarg::<Option<String>>("schema")?;
                let identifier: Option<String> = iter.next_kwarg::<Option<String>>("identifier")?;
                let relation_type: Option<String> = iter.next_kwarg::<Option<String>>("type")?;
                let custom_quoting: Option<Value> =
                    iter.next_kwarg::<Option<Value>>("quote_policy")?;
                let table_format: Option<String> =
                    iter.next_kwarg::<Option<String>>("table_format")?;
                let _ = iter.trailing_kwargs()?;

                let custom_quoting = custom_quoting
                    .and_then(|v| DbtQuoting::deserialize(v).ok())
                    .map(|v| ResolvedQuoting {
                        database: v.database.unwrap_or_default(),
                        identifier: v.identifier.unwrap_or_default(),
                        schema: v.schema.unwrap_or_default(),
                    })
                    .unwrap_or(self.quoting);

                let table_format =
                    if table_format.is_some_and(|s| s.eq_ignore_ascii_case("iceberg")) {
                        TableFormat::Iceberg
                    } else {
                        TableFormat::Default
                    };

                let mut relation = Relation::new(
                    AdapterType::Snowflake,
                    database,
                    schema,
                    identifier,
                    relation_type.map(|s| RelationType::from(s.as_str())),
                    None,
                    custom_quoting,
                    None,
                    false,
                    false,
                );
                relation.table_format = table_format;
                let rel = RelationObject::new(Arc::new(relation));
                Ok(Value::from_object(rel))
            }
            _ => {
                let iter = ArgsIter::new("Relation.create", &[], args);
                let database = iter.next_kwarg::<Option<String>>("database")?;
                let schema = iter.next_kwarg::<Option<String>>("schema")?;
                let identifier = iter.next_kwarg::<Option<String>>("identifier")?;
                let relation_type = iter.next_kwarg::<Option<Value>>("type")?;
                let custom_quoting = iter.next_kwarg::<Option<Value>>("quote_policy")?;
                let temporary = iter.next_kwarg::<Option<bool>>("temporary")?;
                iter.finish()?;

                let custom_quoting = custom_quoting
                    .and_then(|v| DbtQuoting::deserialize(v).ok())
                    .map(|v| ResolvedQuoting {
                        database: v.database.unwrap_or_default(),
                        identifier: v.identifier.unwrap_or_default(),
                        schema: v.schema.unwrap_or_default(),
                    });

                self.try_new(
                    database,
                    schema,
                    identifier,
                    relation_type.and_then(|v: Value| {
                        if v.is_none() || v.is_undefined() {
                            None
                        } else {
                            Some(RelationType::from(v.as_str().unwrap_or_default()))
                        }
                    }),
                    custom_quoting,
                    temporary,
                )
            }
        }
    }
}

/// Generic relation implementation shared by Databricks, Spark, Fabric, and DuckDB.
///
/// The module path is historical; adapter-specific behavior must still be
/// gated on `adapter_type`.
#[derive(Clone, Debug)]
pub struct Relation {
    /// Whether this relation should behave as a parse-time Relation
    ///
    /// NOTE: It is an unfortunate inheritance from dbt Core 1.0 where parse-time
    /// relation does not behave the same as compile-time relation, and this has
    /// consequences on deferral logic, manifest writing etc. So we need to emulate
    /// the same behavior here...
    pub is_parse_time: bool,
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
    /// The location/region for this relation (BigQuery only, e.g., "US", "EU").
    pub location: Option<String>,
    /// DuckDB external source location, rendered in place of schema/table.
    pub external: Option<String>,
    /// The table format of the relation
    pub table_format: TableFormat,
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
            Databricks | Fabric | Postgres | Redshift | Salesforce | Bigquery => {
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
                Fabric | Bigquery => db_str,
                Salesforce | Snowflake => db_str.to_ascii_uppercase(),
                _ => db_str.to_ascii_lowercase(),
            }
        };

        let schema = if self.quote_policy().database {
            schema_str
        } else {
            match self.adapter_type {
                Fabric | Bigquery => schema_str,
                Salesforce | Snowflake => schema_str.to_ascii_uppercase(),
                _ => schema_str.to_ascii_lowercase(),
            }
        };

        let ident = if self.quote_policy().database {
            ident_str
        } else {
            match self.adapter_type {
                Fabric | Bigquery => ident_str,
                Salesforce | Snowflake => ident_str.to_ascii_uppercase(),
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
        let path = RelationPath {
            database: database.filter(|s| !s.is_empty()),
            schema,
            identifier,
        };
        let include_policy = include_policy(adapter_type, &path);
        Self {
            is_parse_time: false,
            adapter_type,
            include_policy,
            path,
            relation_type,
            quote_policy: custom_quoting,
            native_schema,
            metadata,
            is_delta,
            create_constraints: Vec::new(),
            alter_constraints: Vec::new(),
            temporary,
            location: None,
            external: None,
            table_format: TableFormat::Default,
        }
    }

    /// Create a relation that stubs some stuff to be used during `dbt parse`
    pub(crate) fn new_parse_time(adapter_type: AdapterType) -> Self {
        let path = RelationPath {
            database: Some("".to_string()),
            schema: Some("".to_string()),
            identifier: Some("".to_string()),
        };
        Self {
            is_parse_time: true,
            adapter_type,
            include_policy: Policy::falses(),
            path,
            relation_type: None,
            quote_policy: Policy::falses(),
            native_schema: None,
            metadata: None,
            is_delta: false,
            create_constraints: Vec::default(),
            alter_constraints: Vec::default(),
            temporary: false,
            location: Some("".to_string()),
            external: None,
            table_format: TableFormat::Default,
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
            is_parse_time: false,
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
            location: None,
            external: None,
            table_format: TableFormat::Default,
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

    pub fn with_external(mut self, external: String) -> Self {
        self.external = Some(external);
        self
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
        use AdapterType::*;
        match self.adapter_type {
            // It might be relation under a `information_schema` schema or a `system` catalog
            // For example, system.billing.list_prices or [database].information_schema.tables
            // are both system tables
            Databricks | Spark => {
                self.path
                    .database
                    .as_ref()
                    .map(|db| db.eq_ignore_ascii_case(databricks::SYSTEM_DATABASE))
                    .unwrap_or(false)
                    || self
                        .path
                        .schema
                        .as_ref()
                        .map(|schema| {
                            schema.eq_ignore_ascii_case(databricks::INFORMATION_SCHEMA_SCHEMA)
                        })
                        .unwrap_or(false)
            }
            Bigquery => self
                .path
                .schema
                .as_ref()
                .map(|schema| schema.eq_ignore_ascii_case("information_schema"))
                .unwrap_or(false),
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
        if self.is_parse_time {
            return None;
        }

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
        relation.external = self.external.clone();
        relation.is_parse_time = self.is_parse_time;

        Ok(Arc::new(relation))
    }

    fn quote_inner(&self, policy: Policy) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        let mut relation = Self::new_with_policy(
            self.adapter_type,
            self.path.clone(),
            self.relation_type,
            self.include_policy,
            policy,
            self.metadata.clone(),
            self.is_delta,
            self.temporary,
        )?;
        relation.create_constraints = self.create_constraints.clone();
        relation.alter_constraints = self.alter_constraints.clone();
        relation.is_parse_time = self.is_parse_time;
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
        match self.adapter_type {
            AdapterType::Snowflake => {
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
            _ => Err(minijinja::Error::new(
                minijinja::ErrorKind::InvalidOperation,
                "Only available for snowflake",
            )),
        }
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/2a94cc75dba1f98fa5caff1f396f5af7ee444598/dbt-snowflake/src/dbt/adapters/snowflake/relation.py#L206
    fn get_iceberg_ddl_options(
        &self,
        runtime_model_config: Value,
    ) -> Result<String, minijinja::Error> {
        match self.adapter_type {
            AdapterType::Snowflake => {
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
                        minijinja::Error::new(
                            minijinja::ErrorKind::NonKey,
                            "external_volume is required",
                        )
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
            _ => Err(minijinja::Error::new(
                minijinja::ErrorKind::InvalidOperation,
                "Only available for snowflake",
            )),
        }
    }

    fn get_ddl_prefix_for_alter(&self) -> Result<String, minijinja::Error> {
        match self.adapter_type {
            AdapterType::Snowflake => {
                if self.table_format == TableFormat::Iceberg {
                    Ok("iceberg".to_string())
                } else {
                    Ok(String::new())
                }
            }
            _ => Err(minijinja::Error::new(
                minijinja::ErrorKind::InvalidOperation,
                "Only available for snowflake",
            )),
        }
    }

    // https://github.com/dbt-labs/dbt-adapters/blob/2a94cc75dba1f98fa5caff1f396f5af7ee444598/dbt-snowflake/src/dbt/adapters/snowflake/relation.py#L223
    fn needs_to_drop(
        &self,
        old_relation: Option<Arc<dyn BaseRelation>>,
    ) -> Result<bool, minijinja::Error> {
        match self.adapter_type {
            AdapterType::Snowflake => {
                if let Some(old_relation) = old_relation {
                    // core does only checks this for table conversions since dynamic tables
                    // are expected to be rebuilt cross-catalog using full refresh mode
                    if old_relation.is_table() {
                        // invoke drop for table -> Iceberg or Iceberg -> table
                        let old_relation_table_format = old_relation
                            .as_any()
                            .downcast_ref::<Relation>()
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
            _ => Err(minijinja::Error::new(
                minijinja::ErrorKind::InvalidOperation,
                "Only available for snowflake",
            )),
        }
    }

    fn can_be_renamed(&self) -> bool {
        use AdapterType::*;
        use RelationType::*;

        match (self.adapter_type, self.relation_type()) {
            (Bigquery, Some(Table)) => true,
            (Snowflake, Some(Table) | Some(View)) => !self.is_iceberg_format(),
            (_, Some(Table) | Some(View)) => true,
            (_, _) => false,
        }
    }

    fn can_be_replaced(&self) -> bool {
        use AdapterType::*;
        use RelationType::*;

        match (self.adapter_type, self.relation_type()) {
            (Redshift, Some(View)) => true,
            (Snowflake, Some(Table) | Some(View) | Some(DynamicTable)) => true,
            (_, Some(Table) | Some(View)) => true,
            (_, _) => false,
        }
    }

    // https://github.com/dbt-labs/dbt-adapters/blob/292d17301eff3c8a972fcd57f7deb3aac4c8a3cb/dbt-snowflake/src/dbt/adapters/snowflake/relation.py#L92
    fn dynamic_table_config_changeset(
        &self,
        relation_results_value: &Value,
        relation_config_value: &Value,
    ) -> Result<Value, minijinja::Error> {
        match self.adapter_type {
            AdapterType::Snowflake => {
                let relation_results =
                    DescribeDynamicTableResults::try_from(relation_results_value).map_err(|e| {
                        minijinja::Error::new(
                            minijinja::ErrorKind::SerdeDeserializeError,
                            format!(
                                "from_config: Failed to serialize DescribeDynamicTableResults: {e}"
                            ),
                        )
                    })?;

                let existing_config = SnowflakeDynamicTableConfig::try_from(relation_results)
                    .map_err(|e| {
                        minijinja::Error::new(
                            minijinja::ErrorKind::SerdeDeserializeError, format!("dynamic_table_config_changeset: Failed to deserialize SnowflakeDynamicTableConfig: {e}")
                        )
                    })?;

                let new_config = node_value_to_snowflake_dynamic_table(relation_config_value)?;

                let changeset =
                    SnowflakeDynamicTableConfigChangeset::new(existing_config, new_config);

                if changeset.has_changes() {
                    Ok(Value::from_object(changeset))
                } else {
                    Ok(Value::from(()))
                }
            }
            _ => Err(minijinja::Error::new(
                minijinja::ErrorKind::InvalidOperation,
                "Only available for snowflake",
            )),
        }
    }

    fn from_config(&self, config: &Value) -> Result<Value, minijinja::Error> {
        match self.adapter_type {
            AdapterType::Redshift => Ok(Value::from_object(
                node_value_to_redshift_materialized_view(config)?,
            )),
            // https://github.com/dbt-labs/dbt-adapters/blob/816d190c9e31391a48cee979bd049aeb34c89ad3/dbt-snowflake/src/dbt/adapters/snowflake/relation.py#L81
            AdapterType::Snowflake => Ok(Value::from_object(
                node_value_to_snowflake_dynamic_table(config)?,
            )),
            _ => Err(minijinja::Error::new(
                minijinja::ErrorKind::InvalidOperation,
                "from_config: Only available for Snowflake and Redshift",
            )),
        }
    }

    fn normalize_component(&self, component: &str) -> String {
        use AdapterType::*;
        match self.adapter_type {
            Salesforce | Bigquery | ClickHouse => component.to_string(),
            Snowflake => component.to_uppercase(),
            _ => component.to_lowercase(),
        }
    }

    fn render_self_as_str(&self) -> String {
        if self.adapter_type == AdapterType::DuckDB
            && let Some(external) = &self.external
        {
            return external.clone();
        }

        if let Some(RelationType::Ephemeral) = self.relation_type {
            return format!(
                "{}{}",
                DBT_CTE_PREFIX,
                self.path.identifier.as_deref().unwrap_or_default()
            );
        }

        let include_policy = self.include_policy;
        let quote_policy = self.quote_policy;
        let mut parts: Vec<String> = Vec::new();

        let quote_part = |val: &str, quote_policy: bool| {
            if quote_policy {
                self.quoted(val)
            } else {
                val.to_string()
            }
        };

        if include_policy.database
            && let Some(database) = self.database()
            && !database.is_empty()
        {
            parts.push(quote_part(database, quote_policy.database));
        }

        if include_policy.schema
            && let Some(schema) = self.schema()
        {
            parts.push(quote_part(schema, quote_policy.schema));
        }

        if include_policy.identifier
            && let Some(identifier) = self.identifier()
        {
            parts.push(quote_part(identifier, quote_policy.identifier));
        }

        let rendered = parts.join(".");

        if matches!(self.adapter_type, AdapterType::Databricks) {
            rendered.to_ascii_lowercase()
        } else {
            rendered
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
        let path = RelationPath {
            database: database.filter(|s| !s.is_empty()),
            schema,
            identifier,
        };
        let include_policy = include_policy(self.adapter_type, &path);
        Ok(Arc::new(Relation::new_with_policy(
            self.adapter_type,
            path,
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
        match self.adapter_type {
            AdapterType::Bigquery => {
                let mut info_schema = InformationSchema::try_from_relation(
                    self.adapter_type(),
                    database.clone(),
                    view_name,
                )?;

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

                    if let Some(proj) = &database {
                        info_schema.database = Some(quote_if_needed(proj));
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
                    match (&self.path.database, &self.path.schema) {
                        (Some(proj), Some(ds)) => {
                            info_schema.database =
                                Some(format!("{}.{}", quote_if_needed(proj), quote_if_needed(ds)));
                        }
                        (Some(proj), None) => {
                            info_schema.database = Some(quote_if_needed(proj));
                        }
                        (None, Some(ds)) => info_schema.database = Some(quote_if_needed(ds)),
                        _ => {}
                    }
                }
                Ok(Arc::new(info_schema))
            }
            _ => {
                let result =
                    InformationSchema::try_from_relation(self.adapter_type(), database, view_name)?;
                Ok(Arc::new(result))
            }
        }
    }

    fn relation_max_name_length(&self) -> Result<u32, minijinja::Error> {
        Ok(max_identifier_length(self.adapter_type)
            .map(|v| v.get().try_into().unwrap_or(u32::MAX))
            .unwrap_or(u32::MAX))
    }

    fn materialized_view_config_changeset(
        &self,
        remote_state_value: &Value,
        local_config_value: &Value,
    ) -> Result<Value, minijinja::Error> {
        use AdapterType::*;
        match self.adapter_type {
            // FIXME(serramatutu): port over to RelationConfig v2
            Redshift => {
                let remote_state = DescribeMaterializedViewResults::try_from(
                    remote_state_value,
                    )
                    .map_err(|e| {
                        minijinja::Error::new(
                            minijinja::ErrorKind::SerdeDeserializeError,
                            format!(
                                "from_config: Failed to serialized DescribeMaterializedViewResults: {e}"
                            )
                        )
                    })?;

                let remote_state = RedshiftMaterializedViewConfig::try_from(remote_state)
                    .map_err(|e| {
                        minijinja::Error::new(
                            minijinja::ErrorKind::SerdeDeserializeError,
                            format!(
                                "materialized_view_config_changeset: Failed to deserialize RedshiftMaterializedViewConfig: {e}"
                            ),
                        )
                    })?;

                let local_config = node_value_to_redshift_materialized_view(local_config_value)?;

                let changeset =
                    RedshiftMaterializedViewConfigChangeset::new(remote_state, local_config);

                if changeset.has_changes() {
                    Ok(Value::from_object(changeset))
                } else {
                    Ok(Value::from(None::<()>))
                }
            }
            Bigquery => {
                let current_state = remote_state_value
                    .as_object()
                    .ok_or_else(|| {
                        minijinja::Error::new(
                            minijinja::ErrorKind::InvalidArgument,
                            "remote_state must be Object",
                        )
                    })?
                    .downcast_ref::<RelationConfig>()
                    .ok_or_else(|| {
                        minijinja::Error::new(
                            minijinja::ErrorKind::InvalidArgument,
                            "remote_state must be RelationConfig",
                        )
                    })?;

                // TODO(serramatutu): minijinja_value_to_typed_struct does not work with
                // references, so we have to clone the value here...
                let local_config = minijinja_value_to_typed_struct::<InternalDbtNodeWrapper>(
                    local_config_value.clone(),
                )
                .map_err(|e| {
                    minijinja::Error::new(
                        minijinja::ErrorKind::SerdeDeserializeError,
                        format!("Failed to deserialize InternalDbtNodeWrapper: {e}"),
                    )
                })?;
                let local_config = match local_config {
                    InternalDbtNodeWrapper::Model(model) => model,
                    _ => {
                        return Err(minijinja::Error::new(
                            minijinja::ErrorKind::InvalidOperation,
                            "Expected a model node",
                        ));
                    }
                };
                let desired_state =
                    crate::relation::bigquery::config::relation_types::materialized_view::new_loader()
                        .from_local_config(local_config.as_ref())?;

                let changeset = RelationConfig::diff(&desired_state, current_state);

                if changeset.is_empty() {
                    Ok(none_value())
                } else {
                    Ok(Value::from_object(changeset))
                }
            }
            _ => unimplemented!("Available only for BigQuery and Redshift"),
        }
    }
}

// FIXME(serramatutu): this should be deleted from here once Snowflake Dynamic Tables
// are migrated to RelationConfig v2.
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

    mod postgres {
        use super::*;

        #[test]
        fn test_try_new_via_static_base_relation() {
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
    }

    mod databricks {
        use super::*;

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

    mod bigquery {
        use super::*;

        #[test]
        fn test_try_new_via_static_base_relation() {
            let relation_type = RelationStatic {
                adapter_type: AdapterType::Bigquery,
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
        fn test_information_schema_with_database() {
            let relation = Relation::new(
                AdapterType::Bigquery,
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
            let relation = Relation::new(
                AdapterType::Bigquery,
                Some("my-project-1a".to_string()),
                Some("test_schema".to_string()),
                Some("test_table".to_string()),
                Some(RelationType::Table),
                None,
                DEFAULT_RESOLVED_QUOTING,
                None,
                false,
                false,
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
            let mut relation = Relation::new(
                AdapterType::Bigquery,
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
            let mut relation = Relation::new(
                AdapterType::Bigquery,
                Some("my-project-1a".to_string()),
                Some("test_schema".to_string()),
                Some("test_table".to_string()),
                Some(RelationType::Table),
                None,
                DEFAULT_RESOLVED_QUOTING,
                None,
                false,
                false,
            );
            relation.location = Some("US".to_string());

            let info_schema = relation
                .information_schema_inner(
                    Some("my-project-1a".to_string()),
                    Some("OBJECT_PRIVILEGES"),
                )
                .unwrap();

            let rendered = info_schema.render_self_as_str();
            assert_eq!(
                rendered,
                "`my-project-1a`.`region-US`.INFORMATION_SCHEMA.OBJECT_PRIVILEGES"
            );
        }

        #[test]
        fn test_information_schema_without_database() {
            let relation = Relation::new(
                AdapterType::Bigquery,
                None,
                Some("test_schema".to_string()),
                Some("test_table".to_string()),
                Some(RelationType::Table),
                None,
                DEFAULT_RESOLVED_QUOTING,
                None,
                false,
                false,
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
            let relation = Relation::new(
                AdapterType::Bigquery,
                None,
                Some("test_schema".to_string()),
                Some("test_table".to_string()),
                Some(RelationType::Table),
                None,
                DEFAULT_RESOLVED_QUOTING,
                None,
                false,
                false,
            );

            // Test TABLES view without database - uses dataset-level INFORMATION_SCHEMA
            let info_schema = relation.information_schema_inner(None, None).unwrap();

            let rendered = info_schema.render_self_as_str();
            assert_eq!(rendered, "test_schema.INFORMATION_SCHEMA");
        }
    }

    mod duckdb {
        use super::*;
        #[test]
        fn test_external_relation_renders_location() {
            let relation = Relation::new(
                AdapterType::DuckDB,
                Some("main".to_string()),
                Some("raw".to_string()),
                Some("orders".to_string()),
                None,
                None,
                DEFAULT_RESOLVED_QUOTING,
                None,
                false,
                false,
            )
            .with_external("'data/RawOrders.csv'".to_string());

            assert_eq!(relation.render_self_as_str(), "'data/RawOrders.csv'");
        }

        fn path_from_db(db: Option<&str>) -> RelationPath {
            RelationPath {
                database: db.map(|db| db.to_string()),
                schema: Some("my_schema".to_string()),
                identifier: Some("my_table".to_string()),
            }
        }

        #[test]
        fn test_include_policy_for_attached_catalog() {
            assert!(
                include_policy(AdapterType::DuckDB, &path_from_db(Some("stocks_dev"))).database
            );
        }

        #[test]
        fn test_should_not_include_database_for_default_catalog() {
            assert!(!include_policy(AdapterType::DuckDB, &path_from_db(Some("main"))).database);
            assert!(!include_policy(AdapterType::DuckDB, &path_from_db(Some("memory"))).database);
            assert!(!include_policy(AdapterType::DuckDB, &path_from_db(Some("MEMORY"))).database);
            assert!(!include_policy(AdapterType::DuckDB, &path_from_db(Some(""))).database);
            assert!(!include_policy(AdapterType::DuckDB, &path_from_db(None)).database);
        }
    }
    mod snowflake {
        use super::*;

        #[test]
        fn test_create_via_static_base_relation() {
            let values = [
                Value::from("d"),
                Value::from("s"),
                Value::from("i"),
                Value::from("table"),
                Value::from("{database: true, identifier: true, schema: true}"),
            ];

            let relation = RelationStatic {
                adapter_type: AdapterType::Snowflake,
                quoting: DEFAULT_RESOLVED_QUOTING,
            }
            .create(&values)
            .unwrap();

            let relation = relation.downcast_object::<RelationObject>().unwrap();
            assert_eq!(relation.inner().render_self_as_str(), r#""d"."s"."i""#);
            assert_eq!(relation.relation_type().unwrap(), RelationType::Table);
        }
    }

    /// ClickHouse uses `schema` as the effective database — its include policy
    /// is `database=false`, so even when a database is supplied to
    /// `api.Relation.create(...)`, the rendered FQN must skip the database
    /// segment and produce `` `<schema>`.`<identifier>` ``.
    #[test]
    fn test_try_new_via_static_base_relation_clickhouse_normalizes_database_to_empty_string() {
        let relation_type = RelationStatic {
            adapter_type: AdapterType::ClickHouse,
            quoting: DEFAULT_RESOLVED_QUOTING,
        };

        let from_none = relation_type
            .try_new(
                None,
                Some("my_schema".to_string()),
                Some("my_table".to_string()),
                Some(RelationType::Table),
                Some(DEFAULT_RESOLVED_QUOTING),
                None,
            )
            .unwrap();
        let from_none = from_none.downcast_object::<RelationObject>().unwrap();
        assert_eq!(from_none.inner().database(), Some(""));
        assert_eq!(
            from_none.inner().render_self_as_str(),
            "`my_schema`.`my_table`"
        );

        let from_supplied = relation_type
            .try_new(
                Some("ignored_db".to_string()),
                Some("my_schema".to_string()),
                Some("my_table".to_string()),
                Some(RelationType::Table),
                Some(DEFAULT_RESOLVED_QUOTING),
                None,
            )
            .unwrap();
        let from_supplied = from_supplied.downcast_object::<RelationObject>().unwrap();
        assert_eq!(from_supplied.inner().database(), Some(""));
        assert_eq!(
            from_supplied.inner().render_self_as_str(),
            "`my_schema`.`my_table`"
        );
    }
}
