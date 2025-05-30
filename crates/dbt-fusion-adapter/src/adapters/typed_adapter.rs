use crate::adapters::cast_util::dyn_base_columns_to_value;
use crate::adapters::cast_util::BaseRelationExt;
use crate::adapters::errors::{AdapterError, AdapterErrorKind};
use crate::adapters::funcs::{execute_macro, none_value};
use crate::adapters::record_batch_utils::get_column_values;
use crate::adapters::response::{AdapterResponse, ResultObject};
use crate::adapters::snapshots::SnapshotStrategy;
use crate::adapters::sql_engine::{execute_query_with_retry, SqlEngine};
use crate::adapters::AdapterType;
use crate::adapters::{AdapterResult, AdapterTyping};
use dbt_agate::AgateTable;

use arrow::array::{RecordBatch, StringArray, TimestampMillisecondArray};
use arrow_schema::{DataType, Schema};
use dbt_common::behavior_flags::BehaviorFlag;
use dbt_frontend_schemas::dialect::Dialect;
use dbt_schemas::schemas::columns::base::{string_type, BaseColumn};
use dbt_schemas::schemas::common::Constraint;
use dbt_schemas::schemas::common::ConstraintSupport;
use dbt_schemas::schemas::common::ConstraintType;
use dbt_schemas::schemas::common::DbtIncrementalStrategy;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::dbt_column::DbtColumn;
use dbt_schemas::schemas::manifest::{
    BigqueryClusterConfig, BigqueryPartitionConfig, DbtModel, ManifestModelConfig,
};
use dbt_schemas::schemas::relations::base::{BaseRelation, ComponentName};
use dbt_xdbc::{Connection, QueryCtx};
use minijinja::value::Kwargs;
use minijinja::{State, Value};

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

/// Adapter with typed functions.
pub trait TypedBaseAdapter: fmt::Debug + Send + Sync + AdapterTyping {
    /// The set of standard builtin strategies which this adapter supports out-of-the-box.
    /// Not used to validate custom strategies defined by end users.
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L1684-L1685
    /// default, so far only used by BigQuery
    fn valid_incremental_strategies(&self) -> Vec<DbtIncrementalStrategy> {
        vec![DbtIncrementalStrategy::Append]
    }

    /// The set of standard builtin strategies which this adapter supports out-of-the-box.
    fn valid_incremental_strategies_as_values(&self) -> Value {
        unimplemented!("Only available with Databricks adapter")
    }

    /// Create a new connection
    fn new_connection(&self) -> AdapterResult<Box<dyn Connection>>;

    /// Split a sql statement into a list of statements
    fn self_split_statements(&self, sql: &str, dialect: Dialect) -> Vec<String>;

    /// Helper method for execute
    #[allow(clippy::too_many_arguments)]
    #[inline(always)]
    fn execute_inner(
        &self,
        dialect: Dialect,
        engine: Arc<SqlEngine>,
        conn: &'_ mut dyn Connection,
        query_ctx: &QueryCtx,
        _auto_begin: Option<bool>,
        _fetch: Option<bool>,
        _limit: Option<u32>,
    ) -> AdapterResult<(AdapterResponse, AgateTable)> {
        let sql = query_ctx.sql().ok_or_else(|| {
            AdapterError::new(AdapterErrorKind::Internal, "Missing query in the context")
        })?;

        let statements = self.self_split_statements(&sql, dialect);
        let mut last_batch = None;
        for statement in statements {
            last_batch = Some(execute_query_with_retry(
                engine.clone(),
                conn,
                &query_ctx.with_sql(statement),
                1,
            )?);
        }

        let table = match last_batch {
            Some(batch) => AgateTable::from_record_batch(Arc::new(batch)),
            None => AgateTable::default(),
        };

        let response = AdapterResponse {
            // TODO: This is hardcoded, should be derived from the sql statement?
            message: format!("SELECT {}", table.num_rows()),
            // TODO: This is hardcoded, should be derived from the sql statement?
            code: "SELECT".to_string(),
            rows_affected: table.num_rows() as i64,
            query_id: None,
        };

        Ok((response, table))
    }

    /// Execute a query
    fn execute(
        &self,
        conn: &'_ mut dyn Connection,
        query_ctx: &QueryCtx,
        auto_begin: Option<bool>,
        fetch: Option<bool>,
        limit: Option<u32>,
    ) -> AdapterResult<(AdapterResponse, AgateTable)>;

    /// Execute a query with a new connection
    fn execute_with_new_connection(
        &self,
        query_ctx: &QueryCtx,
        auto_begin: Option<bool>,
        fetch: Option<bool>,
        limit: Option<u32>,
    ) -> AdapterResult<(AdapterResponse, AgateTable)> {
        let mut conn = self.new_connection()?;
        self.execute(&mut *conn, query_ctx, auto_begin, fetch, limit)
    }

    /// Add a query to run
    #[allow(clippy::too_many_arguments)]
    fn add_query(
        &self,
        conn: &'_ mut dyn Connection,
        query_ctx: &QueryCtx,
        auto_begin: bool,
        abridge_sql_log: bool,
    ) -> AdapterResult<()>;

    /// Quote
    fn quote(&self, identifier: &str) -> String;

    /// List schemas
    fn list_schemas(&self, result: Arc<RecordBatch>) -> Vec<String>;

    /// Get relation that represents (database, schema, identifier)
    /// tuple. This function checks that the warehouse has the
    /// relation.
    fn get_relation(
        &self,
        query_ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        database: &str,
        schema: &str,
        identifier: &str,
        needs_information: Option<bool>,
    ) -> AdapterResult<Option<Arc<dyn BaseRelation>>>;

    /// Drop relation
    fn drop_relation(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<Value> {
        if relation.relation_type().is_none() {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                "relation has no type",
            ));
        }
        let args = vec![relation.to_value()?];
        execute_macro(state, &args, "drop_relation")?;
        Ok(none_value())
    }

    /// Get the full macro name for check_schema_exists
    ///
    /// # Returns
    ///
    /// Returns (package_name, macro_name)
    fn check_schema_exists_macro(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> AdapterResult<(String, String)> {
        Ok(("dbt".to_string(), "check_schema_exists".to_string()))
    }

    /// Rename relation
    fn rename_relation(
        &self,
        _conn: &'_ mut dyn Connection,
        _from_relation: Arc<dyn BaseRelation>,
        _to_relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<()> {
        unimplemented!("reserved for _rename_relation in bridge.rs")
    }

    /// Returns the columns that exist in the source_relations but not in the target_relations
    fn get_missing_columns(
        &self,
        state: &State,
        source_relation: Arc<dyn BaseRelation>,
        target_relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<Value> {
        // Get columns for both relations
        let source_cols = self.get_columns_in_relation(state, source_relation)?;
        let target_cols = self.get_columns_in_relation(state, target_relation)?;

        let source_cols_map: BTreeMap<_, _> = source_cols
            .into_iter()
            .map(|col| (col.name(), col))
            .collect();
        let target_cols_set: std::collections::HashSet<_> =
            target_cols.into_iter().map(|col| col.name()).collect();

        let result: Vec<Box<dyn BaseColumn>> = source_cols_map
            .into_iter()
            .filter_map(|(name, col)| {
                if target_cols_set.contains(&name) {
                    None
                } else {
                    Some(col)
                }
            })
            .collect();
        let result = dyn_base_columns_to_value(result)?;
        Ok(result)
    }

    /// Get columns in relation
    fn get_columns_in_relation(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<Vec<Box<dyn BaseColumn>>>;

    /// Convert a Schema of Arrow to be represented via BaseColumn
    fn arrow_schema_to_dbt_columns(&self, schema: Arc<Schema>) -> AdapterResult<Vec<Value>>;

    /// Truncate relation
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/sql/impl.py#L147
    fn truncate_relation(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<Value> {
        // downcast relation
        let relation = relation.to_value()?;
        execute_macro(state, &[relation], "truncate_relation")?;
        Ok(none_value())
    }

    /// Quote as configured
    fn quote_as_configured(&self, identifier: &str, quote_key: &ComponentName) -> String {
        if self.get_resolved_quoting().get_part(quote_key) {
            self.quote(identifier)
        } else {
            identifier.to_string()
        }
    }

    /// Get resolved quoting
    fn get_resolved_quoting(&self) -> ResolvedQuoting;

    /// Quote seed column, default to true if not provided
    /// reference: https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L1072
    fn quote_seed_column(&self, column: &str, quote_config: Option<bool>) -> String {
        if quote_config.unwrap_or(true) {
            self.quote(column)
        } else {
            column.to_string()
        }
    }

    fn convert_type_inner(&self, data_type: &DataType) -> AdapterResult<String>;

    /// Convert type.
    fn convert_type(&self, table: Arc<AgateTable>, col_idx: i64) -> AdapterResult<String> {
        let schema = table.to_record_batch().schema();
        let data_type = schema.field(col_idx as usize).data_type();

        self.convert_type_inner(data_type)
    }

    /// Expand the to_relation table's column types to match the schema of from_relation
    fn expand_target_column_types(
        &self,
        state: &State,
        from_relation: Arc<dyn BaseRelation>,
        to_relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<Value> {
        let from_columns = self.get_columns_in_relation(state, from_relation)?;
        let to_columns = self.get_columns_in_relation(state, to_relation.clone())?;

        // Create HashMaps for efficient lookup
        let from_columns_map = from_columns
            .into_iter()
            .map(|c| (c.name(), c))
            .collect::<BTreeMap<_, _>>();

        let to_columns_map = to_columns
            .into_iter()
            .map(|c| (c.name(), c))
            .collect::<BTreeMap<_, _>>();

        for (column_name, reference_column) in from_columns_map {
            if let Some(target_column) = to_columns_map.get(&column_name) {
                if target_column.can_expand_to(reference_column.to_value()?)? {
                    let col_string_size = reference_column.string_size()?;
                    let new_type = string_type(col_string_size);

                    // Create args for macro execution
                    let kwargs = Kwargs::from_iter([
                        ("relation", to_relation.to_value()?),
                        ("column_name", column_name),
                        ("new_column_type", Value::from(new_type)),
                    ]);
                    execute_macro(state, &[Value::from(kwargs)], "alter_column_type")?;
                }
            }
        }
        Ok(none_value())
    }

    /// update_columns
    fn update_columns_descriptions(
        &self,
        _state: &State,
        _conn: &'_ mut dyn Connection,
        _relation: Value,
        _columns: BTreeMap<String, DbtColumn>,
    ) -> AdapterResult<Value> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// render_raw_columns_constraints
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L1783
    fn render_raw_columns_constraints(
        &self,
        columns_map: BTreeMap<String, DbtColumn>,
    ) -> AdapterResult<Vec<String>> {
        let mut result = vec![];
        for (_, column) in columns_map {
            // TODO: handle quote
            let col_name = column.name.clone();
            let mut rendered_column_constraint = vec![format!(
                "{} {}",
                col_name,
                column.data_type.as_deref().unwrap_or_default()
            )];
            for constraint in column.constraints {
                let rendered = self.render_column_constraint(constraint);
                if let Some(rendered) = rendered {
                    rendered_column_constraint.push(rendered);
                }
            }
            result.push(rendered_column_constraint.join(" ").to_string())
        }
        Ok(result)
    }

    fn render_column_constraint(&self, constraint: Constraint) -> Option<String> {
        // TODO: revisit to support warn_supported, warn_unenforced
        // https://github.com/dbt-labs/dbt-adapters/blob/5379513bad9c75661b990a5ed5f32ac9c62a0758/dbt-adapters/src/dbt/adapters/base/impl.py#L1825
        let constraint_support = self.get_constraint_support(constraint.type_);
        if constraint_support == ConstraintSupport::NotSupported {
            return None;
        }

        let constraint_expression = constraint.expression.unwrap_or_default();

        let rendered = match constraint.type_ {
            ConstraintType::Check if !constraint_expression.is_empty() => {
                Some(format!("check ({})", constraint_expression))
            }
            ConstraintType::NotNull => Some(format!("not null {}", constraint_expression)),
            ConstraintType::Unique => Some(format!("unique {}", constraint_expression)),
            ConstraintType::PrimaryKey => Some(format!("primary key {}", constraint_expression)),
            ConstraintType::ForeignKey => {
                if let (Some(to), Some(to_columns)) = (constraint.to, constraint.to_columns) {
                    Some(format!("references {} ({})", to, to_columns.join(", ")))
                } else if !constraint_expression.is_empty() {
                    Some(format!("references {}", constraint_expression))
                } else {
                    None
                }
            }
            ConstraintType::Custom if !constraint_expression.is_empty() => {
                Some(constraint_expression)
            }
            _ => None,
        };
        rendered.and_then(|r| {
            if self.adapter_type() == AdapterType::Bigquery
                && (constraint.type_ == ConstraintType::PrimaryKey
                    || constraint.type_ == ConstraintType::ForeignKey)
            {
                Some(format!("{} not enforced", r))
            } else if self.adapter_type() == AdapterType::Bigquery {
                None
            } else {
                Some(r.trim().to_string())
            }
        })
    }

    /// Given a constraint, return the support status of the constraint on this adapter.
    /// https://github.com/dbt-labs/dbt-adapters/blob/5379513bad9c75661b990a5ed5f32ac9c62a0758/dbt-adapters/src/dbt/adapters/base/impl.py#L293
    fn get_constraint_support(&self, ct: ConstraintType) -> ConstraintSupport {
        match ct {
            ConstraintType::Check => ConstraintSupport::NotSupported,
            ConstraintType::NotNull | ConstraintType::ForeignKey => ConstraintSupport::Enforced,
            ConstraintType::Unique | ConstraintType::PrimaryKey => ConstraintSupport::NotEnforced,
            _ => ConstraintSupport::NotSupported,
        }
    }
    /// Translate the result of `show grants` (or equivalent) to match the
    /// grants which a user would configure in their project.
    /// Ideally, the SQL to show grants should also be filtering:
    /// filter OUT any grants TO the current user/role (e.g. OWNERSHIP).
    /// If that's not possible in SQL, it can be done in this method instead.
    /// reference: https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L733-L734
    fn standardize_grants_dict(
        &self,
        grants_table: Arc<AgateTable>,
    ) -> AdapterResult<BTreeMap<String, Vec<String>>> {
        let record_batch = grants_table.to_record_batch();

        let grantee_cols = get_column_values::<StringArray>(&record_batch, "grantee");
        let privilege_cols = get_column_values::<StringArray>(&record_batch, "privilege_type");

        let mut result = BTreeMap::new();
        for i in 0..record_batch.num_rows() {
            let privilege = privilege_cols.value(i);
            let grantee = grantee_cols.value(i);

            let list = result.entry(privilege.to_string()).or_insert_with(Vec::new);
            list.push(grantee.to_string());
        }

        Ok(result)
    }

    /// Docs see the impl of this method from bigquery/adapter.rs
    fn nest_column_data_types(
        &self,
        _columns: BTreeMap<String, DbtColumn>,
        _constraints: Option<BTreeMap<String, String>>,
    ) -> AdapterResult<BTreeMap<String, DbtColumn>> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// grant_access_to
    #[allow(clippy::too_many_arguments)]
    fn grant_access_to(
        &self,
        _state: &State,
        _conn: &'_ mut dyn Connection,
        _entity: &str,
        _entity_type: &str,
        _role: Option<&str>,
        _database: &str,
        _schema: &str,
    ) -> AdapterResult<Value> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// get_dataset_location
    fn get_dataset_location(
        &self,
        _state: &State,
        _conn: &'_ mut dyn Connection,
        _relation: Value,
    ) -> AdapterResult<Option<String>> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// update_table_description
    fn update_table_description(
        &self,
        _state: &State,
        _conn: &'_ mut dyn Connection,
        _database: &str,
        _schema: &str,
        _identifier: &str,
        _description: &str,
    ) -> AdapterResult<Value> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// alter_table_add_columns
    fn alter_table_add_columns(
        &self,
        _state: &State,
        _conn: &'_ mut dyn Connection,
        _relation: Value,
        _columns: Value,
    ) -> AdapterResult<Value> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// Given a list of sources (BaseRelations), calculate the metadata-based freshness in batch.
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L1390
    fn calculate_freshness_from_metadata_batch(
        &self,
        state: &State,
        sources: Vec<Value>,
    ) -> AdapterResult<Value> {
        let kwargs = Kwargs::from_iter([
            ("information_schema", Value::from("INFORMATION_SCHEMA")),
            ("relations", Value::from_object(sources)),
        ]);

        let result: Value =
            execute_macro(state, &[Value::from(kwargs)], "get_relation_last_modified")?;
        let result = result.downcast_object::<ResultObject>().unwrap();

        let table = result.table.as_ref().expect("AgateTable exists");
        let record_batch = table.to_record_batch();

        let identifier_column_values =
            get_column_values::<StringArray>(&record_batch, "IDENTIFIER");
        let schema_column_values = get_column_values::<StringArray>(&record_batch, "SCHEMA");
        let last_modified_column_values =
            get_column_values::<TimestampMillisecondArray>(&record_batch, "LAST_MODIFIED");

        let mut result = BTreeMap::new();
        for i in 0..record_batch.num_rows() {
            let identifier = identifier_column_values.value(i).to_lowercase();
            let schema = schema_column_values.value(i).to_lowercase();
            let last_modified = last_modified_column_values.value(i);
            result.insert((identifier, schema), last_modified);
        }
        let result = Value::from_serialize(result);

        Ok(result)
    }

    /// Get column schema from query
    fn get_column_schema_from_query(
        &self,
        conn: &mut dyn Connection,
        query_ctx: &QueryCtx,
    ) -> AdapterResult<Vec<Box<dyn BaseColumn>>>;

    /// Get columns in select sql
    fn get_columns_in_select_sql(
        &self,
        _conn: &'_ mut dyn Connection,
        _sql: &str,
    ) -> AdapterResult<Vec<Box<dyn BaseColumn>>> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// Used by redshift and postgres to check if the database string is consistent with what's in the project `config`
    fn verify_database(&self, _database: String) -> AdapterResult<Value> {
        unimplemented!("only available with either Postgres or Redshift adapter")
    }

    /// is_replaceable
    fn is_replaceable(
        &self,
        _conn: &'_ mut dyn Connection,
        _relation: Arc<dyn BaseRelation>,
        _partition_by: Option<BigqueryPartitionConfig>,
        _cluster_by: Option<BigqueryClusterConfig>,
    ) -> AdapterResult<bool> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// parse_partition_by
    fn parse_partition_by(&self, _partition_by: BigqueryPartitionConfig) -> AdapterResult<()> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// get_table_options
    fn get_table_options(
        &self,
        _config: ManifestModelConfig,
        _node: DbtModel,
        _temporary: bool,
    ) -> AdapterResult<BTreeMap<String, Value>> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// get_view_options
    fn get_view_options(
        &self,
        _config: ManifestModelConfig,
        _node: DbtModel,
    ) -> AdapterResult<BTreeMap<String, Value>> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// add_time_ingestion_partition_column
    fn add_time_ingestion_partition_column(
        &self,
        _columns: Value,
        _partition_config: BigqueryPartitionConfig,
    ) -> AdapterResult<Value> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// list_relations_without_caching
    fn list_relations_without_caching(
        &self,
        _state: &State,
        _conn: &'_ mut dyn Connection,
        _relation: Value,
    ) -> AdapterResult<Value> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// Behavior (flags)
    ///
    /// By default no adapter has extra flags, but each adapter can
    /// change this behavior
    fn behavior(&self) -> Vec<BehaviorFlag> {
        vec![]
    }

    /// compare_dbr_version
    fn compare_dbr_version(
        &self,
        _state: &State,
        _conn: &mut dyn Connection,
        _major: i64,
        _minor: i64,
    ) -> AdapterResult<Value> {
        unimplemented!("only available with Databricks adapter")
    }

    /// compute_external_path
    fn compute_external_path(
        &self,
        _config: ManifestModelConfig,
        _model: DbtModel,
        _is_incremental: bool,
    ) -> AdapterResult<String> {
        unimplemented!("only available with Databricks adapter")
    }

    /// update_tblproperties_for_iceberg
    fn update_tblproperties_for_iceberg(
        &self,
        _state: &State,
        _conn: &mut dyn Connection,
        _config: ManifestModelConfig,
        _tblproperties: &mut BTreeMap<String, Value>,
    ) -> AdapterResult<()> {
        unimplemented!("only available with Databricks adapter")
    }

    /// get_relation_config
    fn get_relation_config(&self, _relation: Arc<dyn BaseRelation>) -> AdapterResult<Value> {
        unimplemented!("only available with Databricks adapter")
    }

    /// get_config_from_model
    fn get_config_from_model(&self, _model: Value) -> AdapterResult<Value> {
        unimplemented!("only available with Databricks adapter")
    }

    /// relation_max_name_length
    fn relation_max_name_length(&self) -> AdapterResult<u32> {
        unimplemented!("only available with Postgres and Redshift adapters")
    }

    /// copy_table
    fn copy_table(
        &self,
        _state: &State,
        _conn: &'_ mut dyn Connection,
        _source: Arc<dyn BaseRelation>,
        _dest: Arc<dyn BaseRelation>,
        _materialization: String,
    ) -> AdapterResult<()> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// describe_relation
    fn describe_relation(
        &self,
        _conn: &'_ mut dyn Connection,
        _relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<Option<Value>> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// Ensure that the target relation is valid, by making sure it
    /// has the expected columns.
    ///
    /// Merged (it was not clear if we need to keep the legacy code in
    /// a separate method so we decided not to)
    /// https://github.com/dbt-labs/dbt-adapters/blob/5882b1df1f8f9ddcd0f4f5fcd09001b1948432e9/dbt-adapters/src/dbt/adapters/base/impl.py#L850
    /// https://github.com/dbt-labs/dbt-adapters/blob/5882b1df1f8f9ddcd0f4f5fcd09001b1948432e9/dbt-adapters/src/dbt/adapters/base/impl.py#L883
    fn assert_valid_snapshot_target_given_strategy(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
        column_names: Option<BTreeMap<String, String>>,
        strategy: Arc<SnapshotStrategy>,
    ) -> AdapterResult<()> {
        let columns = self.get_columns_in_relation(state, relation)?;
        let names_in_relation: Vec<String> = columns
            .iter()
            .map(|c| c.name_prop().to_lowercase())
            .collect();

        // missing columns
        let mut missing: Vec<String> = Vec::new();

        // Note: we're not checking dbt_updated_at or dbt_is_deleted
        // here because they aren't always present.
        let mut hardcoded_columns = vec!["dbt_scd_id", "dbt_valid_from", "dbt_valid_to"];

        if let Some(ref s) = strategy.hard_deletes {
            if s == "new_record" {
                hardcoded_columns.push("dbt_is_deleted");
            }
        }

        for column in hardcoded_columns {
            let desired = match column_names {
                Some(ref tree) => match tree.get(column) {
                    Some(v) => v.to_string(),
                    None => {
                        return Err(AdapterError::new(
                            AdapterErrorKind::Configuration,
                            format!("Could not find key {}", column),
                        ))
                    }
                },
                None => column.to_string(),
            };

            if !names_in_relation.contains(&desired.to_lowercase()) {
                missing.push(desired);
            }
        }

        if !missing.is_empty() {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                format!("There are missing columns: {:?}", missing),
            ));
        }

        Ok(())
    }

    /// generate_unique_temporary_table_suffix
    fn generate_unique_temporary_table_suffix(
        &self,
        _suffix_initial: Option<String>,
    ) -> AdapterResult<String> {
        unimplemented!("not only available for this adapter")
    }

    /// Check the hard_deletes config enum, and the legacy
    /// invalidate_hard_deletes config flag in order to determine
    /// which behavior should be used for deleted records in a
    /// snapshot. The default is to ignore them.
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/4467d4a65503659ede940d8d8d97f16fad9c72cb/dbt-adapters/src/dbt/adapters/base/impl.py#L1903
    fn get_hard_deletes_behavior(&self, config: BTreeMap<String, Value>) -> AdapterResult<String> {
        let invalidate_hard_deletes = config.get("invalidate_hard_deletes");
        let hard_deletes = config.get("hard_deletes");

        if invalidate_hard_deletes.is_some() && hard_deletes.is_some() {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                "You cannot set both the invalidate_hard_deletes and hard_deletes config properties on the same snapshot."
            ));
        }

        if invalidate_hard_deletes.is_some() {
            return Ok("invalidate".to_string());
        }

        match hard_deletes {
            None => Ok("ignore".to_string()),
            Some(val) => match val.as_str() {
                Some("invalidate") => Ok("invalidate".to_string()),
                Some("new_record") => Ok("new_record".to_string()),
                Some("ignore") => Ok("ignore".to_string()),
                Some(_) | None => Err(AdapterError::new(
                    AdapterErrorKind::Configuration,
                    "Invalid setting for property hard_deletes.",
                )),
            },
        }
    }
}
