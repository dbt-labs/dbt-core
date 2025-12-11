use crate::adapter_engine::{AdapterEngine, Options as ExecuteOptions, execute_query_with_retry};
use crate::catalog_relation::CatalogRelation;
use crate::column::{Column, ColumnBuilder};
use crate::errors::{AdapterError, AdapterErrorKind};
use crate::funcs::{convert_macro_result_to_record_batch, execute_macro, none_value};
use crate::information_schema::InformationSchema;
use crate::metadata::{self, CatalogAndSchema};
use crate::query_ctx::query_ctx_from_state;
use crate::record_batch_utils::{extract_first_value_as_i64, get_column_values};
use crate::relation::BaseRelationConfig;
use crate::relation::RelationObject;
use crate::render_constraint::render_column_constraint;
use crate::response::{AdapterResponse, ResultObject};
use crate::snapshots::SnapshotStrategy;
use crate::{
    AdapterResult, AdapterTyping, execute_macro_with_package, execute_macro_wrapper_with_package,
    load_catalogs, python,
};

use adbc_core::options::OptionValue;
use arrow::array::{RecordBatch, StringArray, TimestampMillisecondArray};
use arrow_array::Array as _;
use arrow_schema::{DataType, Schema};
use dbt_agate::AgateTable;
use dbt_common::adapter::AdapterType;
use dbt_common::behavior_flags::BehaviorFlag;
use dbt_common::{FsResult, unexpected_fs_err};
use dbt_frontend_common::dialect::Dialect;
use dbt_schemas::schemas::common::Constraint;
use dbt_schemas::schemas::common::ConstraintSupport;
use dbt_schemas::schemas::common::ConstraintType;
use dbt_schemas::schemas::common::DbtIncrementalStrategy;
use dbt_schemas::schemas::dbt_column::{DbtColumn, DbtColumnRef};
use dbt_schemas::schemas::manifest::{BigqueryClusterConfig, BigqueryPartitionConfig};
use dbt_schemas::schemas::project::ModelConfig;
use dbt_schemas::schemas::properties::ModelConstraint;
use dbt_schemas::schemas::relations::base::{BaseRelation, ComponentName};
use dbt_schemas::schemas::{CommonAttributes, InternalDbtNodeAttributes, InternalDbtNodeWrapper};
use dbt_serde_yaml::Value as YmlValue;
use dbt_xdbc::bigquery::QUERY_LINK_FAILED_JOB;
use dbt_xdbc::salesforce::DATA_TRANSFORM_RUN_TIMEOUT;
use dbt_xdbc::{Connection, QueryCtx};
use indexmap::IndexMap;
use minijinja::value::ValueMap;
use minijinja::{State, Value, args};

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

/// Adapter with typed functions.
pub trait TypedBaseAdapter: fmt::Debug + Send + Sync + AdapterTyping {
    /// Execute `use warehouse [name]` statement for SnowflakeAdapter
    /// For other warehouses, this is noop
    fn use_warehouse(
        &self,
        conn: &'_ mut dyn Connection,
        warehouse: String,
        node_id: &str,
    ) -> FsResult<()> {
        if let Some(replay_adapter) = self.as_replay() {
            return replay_adapter.replay_use_warehouse(conn, warehouse, node_id);
        }
        match self.adapter_type() {
            AdapterType::Snowflake => {
                let ctx = QueryCtx::default().with_node_id(node_id);
                let sql = format!("use warehouse {warehouse}");
                self.exec_stmt(&ctx, conn, &sql, false)?;
            }
            _ => debug_assert!(false, "only SnowflakeAdapter should call use_warehouse"),
        }
        Ok(())
    }

    /// Execute `use warehouse [name]` statement for SnowflakeAdapter
    /// For other warehouses, this is noop
    fn restore_warehouse(&self, conn: &'_ mut dyn Connection, node_id: &str) -> FsResult<()> {
        match self.adapter_type() {
            AdapterType::Snowflake => {
                let warehouse = self.get_db_config("warehouse").ok_or_else(|| {
                    unexpected_fs_err!("'warehouse' not found in Snowflake DB config")
                })?;
                let ctx = QueryCtx::default().with_node_id(node_id);
                let sql = format!("use warehouse {warehouse}");
                self.exec_stmt(&ctx, conn, &sql, false)?;
            }
            _ => debug_assert!(false, "only SnowflakeAdapter should call restore_warehouse"),
        }
        Ok(())
    }

    /// Get DB config by key
    fn get_db_config(&self, key: &str) -> Option<Cow<'_, str>> {
        self.engine().config(key)
    }

    fn get_db_config_value(&self, key: &str) -> Option<&YmlValue> {
        if self.engine().get_config().contains_key(key) {
            return self.engine().get_config().get(key);
        }
        None
    }

    fn valid_incremental_strategies(&self) -> &[DbtIncrementalStrategy] {
        use DbtIncrementalStrategy::*;
        static POSTGRES: [DbtIncrementalStrategy; 4] = [Append, DeleteInsert, Merge, Microbatch];
        static SNOWFLAKE: [DbtIncrementalStrategy; 5] =
            [Append, DeleteInsert, InsertOverwrite, Merge, Microbatch];
        static BIGQUERY: [DbtIncrementalStrategy; 1] = [Append];
        static DATABRICKS: [DbtIncrementalStrategy; 4] =
            [Append, Merge, InsertOverwrite, ReplaceWhere];
        static REDSHIFT: [DbtIncrementalStrategy; 4] = [Append, DeleteInsert, Merge, Microbatch];

        match self.adapter_type() {
            AdapterType::Postgres => &POSTGRES,
            AdapterType::Snowflake => &SNOWFLAKE,
            AdapterType::Bigquery => &BIGQUERY,
            AdapterType::Databricks => &DATABRICKS,
            AdapterType::Redshift => &REDSHIFT,
            AdapterType::Salesforce => {
                unimplemented!("Salesforce valid_incremental_strategies not implemented")
            }
        }
    }

    /// Redact credentials expressions from DDL statements
    fn redact_credentials(&self, _sql: &str) -> AdapterResult<String> {
        unimplemented!("Only available with Databricks adapter")
    }

    /// Create a new connection
    fn new_connection(
        &self,
        state: Option<&State>,
        node_id: Option<String>,
    ) -> AdapterResult<Box<dyn Connection>> {
        if let Some(replay_adapter) = self.as_replay() {
            replay_adapter.replay_new_connection(state, node_id)
        } else {
            self.engine().new_connection(state, node_id)
        }
    }

    /// Helper method for execute
    #[allow(clippy::too_many_arguments)]
    #[inline(always)]
    fn execute_inner(
        &self,
        dialect: Dialect,
        engine: Arc<AdapterEngine>,
        state: Option<&State>,
        conn: &'_ mut dyn Connection,
        ctx: &QueryCtx,
        sql: &str,
        _auto_begin: bool,
        fetch: bool,
        _limit: Option<i64>,
        options: Option<HashMap<String, String>>,
    ) -> AdapterResult<(AdapterResponse, AgateTable)> {
        // BigQuery API supports multi-statement
        // https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language
        let statements = if self.adapter_type() == AdapterType::Bigquery {
            if engine.splitter().is_empty(sql, dialect) {
                vec![]
            } else {
                vec![sql.to_owned()]
            }
        } else {
            engine.split_and_filter_statements(sql, dialect)
        };
        if statements.is_empty() {
            return Ok((AdapterResponse::default(), AgateTable::default()));
        }

        let mut options = options
            .unwrap_or_default()
            .into_iter()
            .map(|(key, value)| (key, OptionValue::String(value)))
            .collect::<Vec<_>>();
        if let Some(state) = state {
            options.extend(self.get_adbc_execute_options(state));
        }

        // Configure warehouse specific options
        #[allow(clippy::single_match)]
        match self.adapter_type() {
            AdapterType::Salesforce => {
                if let Some(timeout) = engine.config("data_transform_run_timeout") {
                    let timeout = timeout.parse::<i64>().map_err(|e| {
                        AdapterError::new(
                            AdapterErrorKind::Configuration,
                            format!("data_transform_run_timeout must be an integer string: {e}",),
                        )
                    })?;
                    options.push((
                        DATA_TRANSFORM_RUN_TIMEOUT.to_string(),
                        OptionValue::Int(timeout),
                    ));
                }
            }
            _ => {}
        }

        let mut last_batch = None;
        for sql in statements {
            last_batch = Some(execute_query_with_retry(
                engine.clone(),
                state,
                conn,
                ctx,
                &sql,
                1,
                &options,
                fetch,
            )?);
        }

        let last_batch = last_batch.expect("last_batch should never be None");

        let response = AdapterResponse::new(&last_batch, self.adapter_type());

        // Deduplicate column names to match dbt-core's behavior, which renames
        // duplicate columns to `col_2`, `col_3`, etc.
        // BigQuery is the exception to this deduping
        let last_batch = if self.adapter_type() != AdapterType::Bigquery {
            crate::record_batch_utils::disambiguate_column_names(last_batch)
        } else {
            last_batch
        };

        let table = AgateTable::from_record_batch(Arc::new(last_batch));

        Ok((response, table))
    }

    /// Query execution implementation for a specific adapter.
    #[allow(clippy::too_many_arguments)]
    fn execute(
        &self,
        state: Option<&State>,
        conn: &'_ mut dyn Connection,
        ctx: &QueryCtx,
        sql: &str,
        auto_begin: bool,
        fetch: bool,
        limit: Option<i64>,
        options: Option<HashMap<String, String>>,
    ) -> AdapterResult<(AdapterResponse, AgateTable)> {
        if let Some(replay_adapter) = self.as_replay() {
            return replay_adapter
                .replay_execute(state, conn, ctx, sql, auto_begin, fetch, limit, options);
        }
        self.execute_inner(
            self.adapter_type().into(),
            Arc::clone(self.engine()),
            state,
            conn,
            ctx,
            sql,
            auto_begin,
            fetch,
            limit,
            options,
        )
    }

    /// Execute a statement, expect no results.
    fn exec_stmt(
        &self,
        ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        sql: &str,
        auto_begin: bool,
    ) -> AdapterResult<AdapterResponse> {
        // default values are the same as in dispatch_adapter_calls()
        let (response, _) = self.execute(
            None,       // empty state
            conn,       // connection
            ctx,        // context around the SQL string
            sql,        // the SQL string
            auto_begin, // auto_begin
            false,      // fetch
            None,       // limit
            None,       // options
        )?;
        Ok(response)
    }

    /// Execute a query and get results in an [AgateTable].
    fn query(
        &self,
        ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        sql: &str,
        limit: Option<i64>,
    ) -> AdapterResult<(AdapterResponse, AgateTable)> {
        self.execute(
            None,  // state
            conn,  // connection
            ctx,   // context around the SQL string
            sql,   // the SQL string
            false, // auto_begin
            true,  // fetch
            limit, // limit
            None,  // options
        )
    }

    /// Execute a query with a new connection
    fn execute_with_new_connection(
        &self,
        ctx: &QueryCtx,
        sql: &str,
        auto_begin: bool,
        fetch: bool,
        limit: Option<i64>,
    ) -> AdapterResult<(AdapterResponse, AgateTable)> {
        let mut conn = self.new_connection(None, None)?;
        self.execute(None, &mut *conn, ctx, sql, auto_begin, fetch, limit, None)
    }

    /// Add a query to run.
    ///
    /// ```python
    /// def add_query(self, sql, auto_begin=True, bindings=None, abridge_sql_log=False):
    /// ```
    #[allow(clippy::too_many_arguments)]
    fn add_query(
        &self,
        ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        sql: &str,
        auto_begin: bool,
        bindings: Option<&Value>,
        abridge_sql_log: bool,
    ) -> AdapterResult<()> {
        if let Some(replay_adapter) = self.as_replay() {
            return replay_adapter.replay_add_query(
                ctx,
                conn,
                sql,
                auto_begin,
                bindings,
                abridge_sql_log,
            );
        }
        match self.adapter_type() {
            AdapterType::Bigquery => {
                // Bigquery does not support add_query
                // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L476-L477
                Err(AdapterError::new(
                    AdapterErrorKind::NotSupported,
                    "bigquery.add_query",
                ))
            }
            AdapterType::Postgres
            | AdapterType::Snowflake
            | AdapterType::Databricks
            | AdapterType::Redshift
            | AdapterType::Salesforce => {
                self.execute_inner(
                    self.adapter_type().into(),
                    Arc::clone(self.engine()),
                    None,
                    conn,
                    ctx,
                    sql,
                    auto_begin,
                    false, // fetch: default to false as in dispatch_adapter_calls()
                    None,
                    None,
                )?;
                Ok(())
            }
        }
    }

    /// Submit Python job
    ///
    /// Executes Python code in the warehouse's Python runtime.
    /// Default implementation raises Internal error.
    fn submit_python_job(
        &self,
        ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        state: &State,
        model: &Value,
        compiled_code: &str,
    ) -> AdapterResult<AdapterResponse> {
        match self.adapter_type() {
            AdapterType::Snowflake => {
                let code = python::snowflake::finalize_python_code(state, model, compiled_code)?;
                if let Some(replay_adapter) = self.as_replay() {
                    // In DBT Replay mode, route through the replay adapter to consume recorded execute calls.
                    let (response, _) = replay_adapter.replay_execute(
                        Some(state),
                        conn,
                        ctx,
                        &code,
                        false,
                        false,
                        None,
                        None,
                    )?;
                    Ok(response)
                } else {
                    let (response, _) = self.execute_inner(
                        self.adapter_type().into(),
                        self.engine().clone(),
                        Some(state),
                        conn,
                        ctx,
                        &code,
                        false,
                        false,
                        None,
                        None,
                    )?;
                    Ok(response)
                }
            }
            AdapterType::Databricks => {
                //TODO: enable once https://github.com/dbt-labs/fs/issues/6547 is fixed
                if let Some(_replay_adapter) = self.as_replay() {
                    return Err(AdapterError::new(
                        AdapterErrorKind::NotSupported,
                        "replay mode not supported for python models on Databricks",
                    ));
                }
                python::databricks::submit_python_job(
                    self.as_typed_base_adapter(),
                    ctx,
                    conn,
                    state,
                    model,
                    compiled_code,
                )
            }
            // https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup#running-python-models-on-bigquery-dataframes
            // https://docs.getdbt.com/reference/resource-configs/bigquery-configs#python-model-configuration
            //
            // https://docs.getdbt.com/reference/resource-configs/databricks-configs
            AdapterType::Bigquery => {
                //TODO: enable once https://github.com/dbt-labs/fs/issues/6547 is fixed
                if let Some(_replay_adapter) = self.as_replay() {
                    return Err(AdapterError::new(
                        AdapterErrorKind::NotSupported,
                        "replay mode not supported for python models on Bigquery",
                    ));
                }
                python::bigquery::submit_python_job(
                    self.as_typed_base_adapter(),
                    ctx,
                    conn,
                    state,
                    model,
                    compiled_code,
                )
            }
            AdapterType::Postgres | AdapterType::Redshift | AdapterType::Salesforce => {
                Err(AdapterError::new(
                    AdapterErrorKind::Internal,
                    format!(
                        "Python models are not supported for {} adapter",
                        self.adapter_type()
                    ),
                ))
            }
        }
    }

    /// Quote
    fn quote(&self, _state: &State, identifier: &str) -> AdapterResult<String> {
        let s = match self.adapter_type() {
            AdapterType::Snowflake
            | AdapterType::Redshift
            | AdapterType::Postgres
            | AdapterType::Salesforce => format!("\"{identifier}\""),
            AdapterType::Bigquery | AdapterType::Databricks => format!("`{identifier}`"),
        };
        Ok(s)
    }

    /// List schemas from a [RecordBatch] result of `show schemas` or equivalent.
    fn list_schemas(&self, result_set: Arc<RecordBatch>) -> AdapterResult<Vec<String>> {
        let schema_column_values = {
            let col_name = match self.adapter_type() {
                AdapterType::Snowflake => "name",
                AdapterType::Databricks => "databaseName",
                AdapterType::Bigquery => "schema_name",
                AdapterType::Postgres | AdapterType::Redshift => "nspname",
                AdapterType::Salesforce => "name",
            };
            get_column_values::<StringArray>(&result_set, col_name)?
        };

        let n = result_set.num_rows();
        let mut schemas = Vec::<String>::with_capacity(n);
        for i in 0..n {
            let name: &str = schema_column_values.value(i);
            schemas.push(name.to_string());
        }
        Ok(schemas)
    }

    /// Get relation that represents (database, schema, identifier)
    /// tuple. This function checks that the warehouse has the
    /// relation.
    fn get_relation(
        &self,
        state: &State,
        ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        database: &str,
        schema: &str,
        identifier: &str,
    ) -> AdapterResult<Option<Arc<dyn BaseRelation>>> {
        if let Some(replay_adapter) = self.as_replay() {
            return replay_adapter
                .replay_get_relation(state, ctx, conn, database, schema, identifier);
        }
        metadata::get_relation::get_relation(
            self.as_typed_base_adapter(),
            state,
            ctx,
            conn,
            database,
            schema,
            identifier,
        )
    }

    /// Get a catalog relation, which in Core is a serialized type.
    /// In Fusion, we treat it as a Jinja accessible flat container of values
    /// needed for Iceberg ddl generation.
    fn build_catalog_relation(&self, model: &Value) -> AdapterResult<CatalogRelation> {
        CatalogRelation::from_model_config_and_catalogs(
            &self.adapter_type(),
            model,
            load_catalogs::fetch_catalogs(),
        )
    }

    /// Get all relevant metadata about a dynamic table
    fn describe_dynamic_table(
        &self,
        state: &State,
        conn: &'_ mut dyn Connection,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        match self.adapter_type() {
            AdapterType::Snowflake => {
                let ctx = query_ctx_from_state(state)?.with_desc("describe_dynamic_table");

                let quoting = relation.quote_policy();

                let schema = if quoting.schema {
                    relation.schema_as_quoted_str()?
                } else {
                    relation.schema_as_str()?
                };

                let database = if quoting.database {
                    relation.database_as_quoted_str()?
                } else {
                    relation.database_as_str()?
                };

                let show_sql = format!(
                    "show dynamic tables like '{}' in schema {database}.{schema}",
                    relation.identifier_as_str()?
                );

                let (_, table) = self.query(&ctx, conn, &show_sql, None)?;

                let new_column_names: Vec<String> = table
                    .column_names()
                    .into_iter()
                    .map(|name| name.to_ascii_lowercase())
                    .collect();

                let table = table
                    .rename(Some(new_column_names), None, false, false)?
                    .select(&[
                        "name".to_string(),
                        "schema_name".to_string(),
                        "database_name".to_string(),
                        "text".to_string(),
                        "target_lag".to_string(),
                        "warehouse".to_string(),
                        "refresh_mode".to_string(),
                    ]);

                Ok(Value::from(ValueMap::from([(
                    Value::from("dynamic_table"),
                    Value::from_object(table),
                )])))
            }
            AdapterType::Postgres
            | AdapterType::Bigquery
            | AdapterType::Databricks
            | AdapterType::Redshift
            | AdapterType::Salesforce => {
                let err = format!(
                    "describe_dynamic_table is not supported by the {} adapter",
                    self.adapter_type()
                );
                Err(minijinja::Error::new(
                    minijinja::ErrorKind::InvalidOperation,
                    err,
                ))
            }
        }
    }

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
        let args = vec![RelationObject::new(relation).as_value()];
        execute_macro(state, &args, "drop_relation")?;
        Ok(none_value())
    }

    fn check_schema_exists(
        &self,
        state: &State,
        database: &str,
        schema: &str,
    ) -> Result<Value, minijinja::Error> {
        // Replay fast-path: consult trace-derived cache if available
        if self.as_replay().is_some() {
            // TODO: move this logic to the [ReplayAdapter]
            if let Some(exists) = self.schema_exists_from_trace(database, schema) {
                return Ok(Value::from(exists));
            }
        }

        let information_schema = InformationSchema {
            database: Some(database.to_string()),
            schema: "INFORMATION_SCHEMA".to_string(),
            identifier: None,
            location: None,
        };

        let (package_name, macro_name) = self.check_schema_exists_macro(state, &[])?;
        let batch = execute_macro_wrapper_with_package(
            state,
            &[information_schema.as_value(), Value::from(schema)],
            &macro_name,
            &package_name,
        )?;

        match extract_first_value_as_i64(&batch) {
            Some(0) => Ok(Value::from(false)),
            Some(1) => Ok(Value::from(true)),
            _ => Err(minijinja::Error::new(
                minijinja::ErrorKind::ReturnValue,
                "invalid return value",
            )),
        }
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

    /// Determine if the current Databricks connection points to a classic
    /// cluster (as opposed to a SQL warehouse).
    fn is_cluster(&self) -> AdapterResult<bool> {
        Err(AdapterError::new(
            AdapterErrorKind::NotSupported,
            "is_cluster is only available for the Databricks adapter",
        ))
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
    ) -> AdapterResult<Vec<Column>> {
        // Get columns for both relations
        let source_cols = self.get_columns_in_relation(state, source_relation)?;
        let target_cols = self.get_columns_in_relation(state, target_relation)?;

        let source_cols_map: BTreeMap<_, _> = source_cols
            .into_iter()
            .map(|col| (col.name().to_string(), col))
            .collect();
        let target_cols_set: std::collections::HashSet<_> =
            target_cols.into_iter().map(|col| col.into_name()).collect();

        Ok(source_cols_map
            .into_iter()
            .filter_map(|(name, col)| {
                if target_cols_set.contains(&name) {
                    None
                } else {
                    Some(col)
                }
            })
            .collect())
    }

    /// Get columns in relation
    fn get_columns_in_relation(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<Vec<Column>> {
        // Mock adapter: return fake column without executing jinja macro
        if self.engine().is_mock() {
            return Ok(vec![Column::new(
                self.adapter_type(),
                "one".to_string(),
                "text".to_string(),
                Some(256),
                None,
                None,
            )]);
        }

        let macro_execution_result: AdapterResult<Value> = match self.adapter_type() {
            AdapterType::Databricks => execute_macro_with_package(
                state,
                &[RelationObject::new(relation).as_value()],
                "get_columns_comments",
                "dbt_databricks",
            ),
            AdapterType::Postgres
            | AdapterType::Snowflake
            | AdapterType::Bigquery
            | AdapterType::Redshift => execute_macro(
                state,
                &[RelationObject::new(relation).as_value()],
                "get_columns_in_relation",
            ),
            AdapterType::Salesforce => {
                unimplemented!("Salesforce get_columns_in_relation not implemented")
            }
        };

        match self.adapter_type() {
            AdapterType::Postgres | AdapterType::Redshift => {
                let result = macro_execution_result?;
                Ok(Column::vec_from_jinja_value(self.adapter_type(), result)?)
            }
            AdapterType::Snowflake => {
                // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-snowflake/src/dbt/adapters/snowflake/impl.py#L191-L198
                let result = match macro_execution_result {
                    Ok(result) => result,
                    Err(err) => {
                        // TODO: switch to checking the vendor error code when available.
                        // See https://github.com/dbt-labs/fs/pull/4267#discussion_r2182835729
                        if err.message().contains("does not exist or not authorized") {
                            return Ok(Vec::new());
                        }
                        return Err(err);
                    }
                };

                Ok(Column::vec_from_jinja_value(
                    AdapterType::Snowflake,
                    result,
                )?)
            }
            AdapterType::Bigquery => {
                // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L246-L255
                // TODO(serramatutu): once this is moved over to Arrow, let's remove the fallback to DbtCoreBaseColumn
                // from Column::vec_from_jinja_value for BigQuery
                // FIXME(harry): the Python version uses googleapi GetTable, that doesn't return pseudocolumn like _PARTITIONDATE or _PARTITIONTIME
                let result = match macro_execution_result {
                    Ok(result) => result,
                    Err(err) => {
                        // Handle NotFound errors
                        if err.kind() == AdapterErrorKind::NotFound
                            || (err.kind() == AdapterErrorKind::UnexpectedResult
                                && err.message().contains("Error 404: Not found"))
                        {
                            return Ok(Vec::new());
                        }
                        return Err(err);
                    }
                };
                Ok(Column::vec_from_jinja_value(AdapterType::Bigquery, result)?)
            }
            AdapterType::Databricks => {
                // Databricks inherits the implementation from the Spark adapter.
                //
                // Spark implementation also implicitly filters out known HUDI metadata columns,
                // which we currently do not.
                //
                // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-spark/src/dbt/adapters/spark/impl.py#L317-L336
                let result = match macro_execution_result
                    .and_then(|r| convert_macro_result_to_record_batch(&r))
                {
                    Ok(result) => result,
                    Err(err) => {
                        // TODO: switch to checking the vendor error code when available.
                        // See https://github.com/dbt-labs/fs/pull/4267#discussion_r2182835729
                        // Only checks for the observed Databricks error message, avoiding
                        // all messages in the reference python Spark adapter.
                        if err.message().contains("[TABLE_OR_VIEW_NOT_FOUND]") {
                            return Ok(Vec::new());
                        }
                        return Err(err);
                    }
                };

                let name_string_array = get_column_values::<StringArray>(&result, "col_name")?;
                let dtype_string_array = get_column_values::<StringArray>(&result, "data_type")?;

                let columns = (0..name_string_array.len())
                    .map(|i| {
                        Column::new(
                            AdapterType::Databricks,
                            name_string_array.value(i).to_string(),
                            dtype_string_array.value(i).to_string(),
                            None, // char_size
                            None, // numeric_precision
                            None, // numeric_scale
                        )
                    })
                    .collect::<Vec<_>>();
                Ok(columns)
            }
            AdapterType::Salesforce => {
                unimplemented!("Salesforce get_columns_in_relation not implemented")
            }
        }
    }

    /// Truncate relation
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/sql/impl.py#L147
    fn truncate_relation(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<Value> {
        // downcast relation
        let relation = RelationObject::new(relation).as_value();
        execute_macro(state, &[relation], "truncate_relation")?;
        Ok(none_value())
    }

    /// Quote as configured
    fn quote_as_configured(
        &self,
        state: &State,
        identifier: &str,
        quote_key: &ComponentName,
    ) -> AdapterResult<String> {
        if self.quoting().get_part(quote_key) {
            self.quote(state, identifier)
        } else {
            Ok(identifier.to_string())
        }
    }

    /// Quote seed column, default to true if not provided
    fn quote_seed_column(
        &self,
        state: &State,
        column: &str,
        quote_config: Option<bool>,
    ) -> AdapterResult<String> {
        if let Some(replay_adapter) = self.as_replay() {
            return replay_adapter.replay_quote_seed_column(state, column, quote_config);
        }
        match self.adapter_type() {
            AdapterType::Snowflake | AdapterType::Salesforce => {
                // Snowflake is special and defaults quoting to false if config is not provided
                if quote_config.unwrap_or(false) {
                    self.quote(state, column)
                } else {
                    Ok(column.to_string())
                }
            }
            AdapterType::Postgres
            | AdapterType::Bigquery
            | AdapterType::Databricks
            | AdapterType::Redshift => {
                // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L1072
                if quote_config.unwrap_or(true) {
                    self.quote(state, column)
                } else {
                    Ok(column.to_string())
                }
            }
        }
    }

    /// Convert type.
    fn convert_type(
        &self,
        state: &State,
        table: Arc<AgateTable>,
        col_idx: i64,
    ) -> AdapterResult<String> {
        // XXX: Core uses the flattened agate table types. Here we use the original arrow
        // schema containing the original table types including nested types. This might
        // be what Core developers expected to get from Python agate types as well. (?)
        let schema = table.original_record_batch().schema();
        let data_type = schema.field(col_idx as usize).data_type();

        // XXX: There is divergence here with Core's behavior as Agate only supports a limited
        // set of datatypes. Our Agate implementation is lossless here and we have conversions with Arrow types.
        //
        // https://github.com/dbt-labs/dbt-fusion/issues/456
        // It looks like core defaults to a numeric type when the given type is null
        let data_type = if data_type.is_null() {
            &DataType::Int32
        } else {
            data_type
        };

        if let Some(replay_adapter) = self.as_replay() {
            // XXX: isn't the point of replay adapter to compare what it does against the actual code?
            return replay_adapter.replay_convert_type(state, data_type);
        }

        let mut out = String::new();
        self.engine()
            .type_ops()
            .format_arrow_type_as_sql(data_type, &mut out)?;
        Ok(out)
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
            .map(|c| (c.name().to_string(), c))
            .collect::<BTreeMap<_, _>>();

        let to_columns_map = to_columns
            .into_iter()
            .map(|c| (c.name().to_string(), c))
            .collect::<BTreeMap<_, _>>();

        for (column_name, reference_column) in from_columns_map {
            let to_relation_cloned = to_relation.clone();
            if let Some(target_column) = to_columns_map.get(&column_name)
                && target_column.can_expand_to(&reference_column)?
            {
                let col_string_size = reference_column
                    .string_size()
                    .map_err(|msg| AdapterError::new(AdapterErrorKind::UnexpectedResult, msg))?;
                let new_type = reference_column
                    .as_static()
                    .string_type(Some(col_string_size as usize));

                // Create args for macro execution
                execute_macro(
                    state,
                    args!(
                        relation => RelationObject::new(to_relation_cloned).as_value(),
                        column_name => column_name,
                        new_column_type => Value::from(new_type),
                    ),
                    "alter_column_type",
                )?;
            }
        }
        Ok(none_value())
    }

    /// update_columns
    fn update_columns_descriptions(
        &self,
        _state: &State,
        _conn: &'_ mut dyn Connection,
        _relation: Arc<dyn BaseRelation>,
        _columns: IndexMap<String, DbtColumn>,
    ) -> AdapterResult<Value> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// render_raw_columns_constraints
    fn render_raw_columns_constraints(
        &self,
        columns_map: IndexMap<String, DbtColumn>,
    ) -> AdapterResult<Vec<String>> {
        match self.adapter_type() {
            AdapterType::Postgres
            | AdapterType::Snowflake
            | AdapterType::Databricks
            | AdapterType::Redshift
            | AdapterType::Salesforce => {
                // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L1783
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
            AdapterType::Bigquery => {
                // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L924
                let mut rendered_constraints: BTreeMap<String, String> = BTreeMap::new();
                for (_, column) in columns_map.iter() {
                    for constraint in &column.constraints {
                        if let Some(rendered) =
                            render_column_constraint(self.adapter_type(), constraint.clone())
                        {
                            if let Some(s) = rendered_constraints.get_mut(&rendered) {
                                s.push_str(&format!(" {rendered}"));
                            } else {
                                rendered_constraints.insert(column.name.clone(), rendered);
                            }
                        }
                    }
                }
                let nested_columns =
                    self.nest_column_data_types(columns_map, Some(rendered_constraints))?;
                let result = nested_columns
                    .into_values()
                    .map(|column| {
                        format!("{} {}", column.name, column.data_type.unwrap_or_default())
                    })
                    .collect();
                Ok(result)
            }
        }
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
                Some(format!("check ({constraint_expression})"))
            }
            ConstraintType::NotNull => Some(format!("not null {constraint_expression}")),
            ConstraintType::Unique => Some(format!("unique {constraint_expression}")),
            ConstraintType::PrimaryKey => Some(format!("primary key {constraint_expression}")),
            ConstraintType::ForeignKey => {
                if let (Some(to), Some(to_columns)) = (constraint.to, constraint.to_columns) {
                    Some(format!("references {} ({})", to, to_columns.join(", ")))
                } else if !constraint_expression.is_empty() {
                    Some(format!("references {constraint_expression}"))
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
                Some(format!("{r} not enforced"))
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
        use AdapterType::*;
        use ConstraintSupport::*;
        use ConstraintType::*;

        match (self.adapter_type(), ct) {
            // Postgres
            (Postgres, NotNull) => Enforced,
            (Postgres, ForeignKey) => Enforced,
            (Postgres, Unique) => NotEnforced,
            (Postgres, PrimaryKey) => NotEnforced,
            (Postgres, Check) => NotSupported,
            (Postgres, Custom) => NotSupported,

            // Snowflake
            // https://github.com/dbt-labs/dbt-adapters/blob/aa1de3d16267a456326a36045701fb48a61a6b6c/dbt-snowflake/src/dbt/adapters/snowflake/impl.py#L74
            (Snowflake, NotNull) => Enforced,
            (Snowflake, ForeignKey) => Enforced,
            (Snowflake, Unique) => NotEnforced,
            (Snowflake, PrimaryKey) => NotEnforced,
            (Snowflake, Check) => NotSupported,
            (Snowflake, Custom) => NotSupported,

            // BigQuery
            // https://github.com/dbt-labs/dbt-adapters/blob/4a00354a497214d9043bf4122810fe2d04de17bb/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L132
            (Bigquery, NotNull) => Enforced,
            (Bigquery, Unique) => NotSupported,
            (Bigquery, PrimaryKey) => NotEnforced,
            (Bigquery, ForeignKey) => NotEnforced,
            (Bigquery, Check) => NotSupported,
            (Bigquery, Custom) => NotSupported,

            // Databricks
            // https://github.com/databricks/dbt-databricks/blob/822b105b15e644676d9e1f47cbfd765cd4c1541f/dbt/adapters/databricks/constraints.py#L17
            (Databricks, NotNull) => Enforced,
            (Databricks, Unique) => NotSupported,
            (Databricks, PrimaryKey) => NotEnforced,
            (Databricks, ForeignKey) => NotEnforced,
            (Databricks, Check) => Enforced,
            (Databricks, Custom) => NotSupported,

            // Redshift
            // https://github.com/dbt-labs/dbt-adapters/blob/2a94cc75dba1f98fa5caff1f396f5af7ee444598/dbt-redshift/src/dbt/adapters/redshift/impl.py#L53
            (Redshift, NotNull) => Enforced,
            (Redshift, Unique) => NotEnforced,
            (Redshift, PrimaryKey) => NotEnforced,
            (Redshift, ForeignKey) => NotEnforced,
            (Redshift, Check) => NotSupported,
            (Redshift, Custom) => NotSupported,

            // Salesforce
            (Salesforce, _) => unimplemented!("Salesforce constraint support not implemented"),
        }
    }

    /// Given existing columns and columns from our model
    /// we determine which columns to update and persist docs for
    /// This is only supported by Databricks
    fn get_persist_doc_columns(
        &self,
        _existing_columns: Vec<Column>,
        _model_columns: IndexMap<String, DbtColumnRef>,
    ) -> AdapterResult<IndexMap<String, DbtColumnRef>> {
        unimplemented!("Only available for Databricks Adapter")
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
        let record_batch = grants_table.original_record_batch();

        match self.adapter_type() {
            AdapterType::Postgres | AdapterType::Bigquery | AdapterType::Redshift => {
                let grantee_cols = get_column_values::<StringArray>(&record_batch, "grantee")?;
                let privilege_cols =
                    get_column_values::<StringArray>(&record_batch, "privilege_type")?;

                let mut result = BTreeMap::new();
                for i in 0..record_batch.num_rows() {
                    let privilege = privilege_cols.value(i);
                    let grantee = grantee_cols.value(i);

                    let list = result.entry(privilege.to_string()).or_insert_with(Vec::new);
                    list.push(grantee.to_string());
                }

                Ok(result)
            }
            AdapterType::Snowflake => {
                // reference: https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-snowflake/src/dbt/adapters/snowflake/impl.py#L329-L330
                let grantee_cols = get_column_values::<StringArray>(&record_batch, "grantee_name")?;
                let granted_to_cols =
                    get_column_values::<StringArray>(&record_batch, "granted_to")?;
                let privilege_cols = get_column_values::<StringArray>(&record_batch, "privilege")?;

                let mut result = BTreeMap::new();
                for i in 0..record_batch.num_rows() {
                    let privilege = privilege_cols.value(i);
                    let grantee = grantee_cols.value(i);
                    let granted_to = granted_to_cols.value(i);

                    if privilege != "OWNERSHIP"
                        && granted_to != "SHARE"
                        && granted_to != "DATABASE_ROLE"
                    {
                        let list = result.entry(privilege.to_string()).or_insert_with(Vec::new);
                        list.push(grantee.to_string());
                    }
                }

                Ok(result)
            }
            AdapterType::Databricks => {
                // https://github.com/dbt-labs/dbt-adapters/blob/c16cc7047e8678f8bb88ae294f43da2c68e9f5cc/dbt-spark/src/dbt/adapters/spark/impl.py#L500
                let grantee_cols = get_column_values::<StringArray>(&record_batch, "Principal")?;
                let privilege_cols = get_column_values::<StringArray>(&record_batch, "ActionType")?;
                let object_type_cols =
                    get_column_values::<StringArray>(&record_batch, "ObjectType")?;

                let mut result = BTreeMap::new();
                for i in 0..record_batch.num_rows() {
                    let privilege = privilege_cols.value(i);
                    let grantee = grantee_cols.value(i);
                    let object_type = object_type_cols.value(i);

                    if object_type == "TABLE" && privilege != "OWN" {
                        let list = result.entry(privilege.to_string()).or_insert_with(Vec::new);
                        list.push(grantee.to_string());
                    }
                }

                Ok(result)
            }
            AdapterType::Salesforce => unimplemented!("Salesforce grants not implemented"),
        }
    }

    /// Docs see the impl of this method from bigquery/adapter.rs
    fn nest_column_data_types(
        &self,
        _columns: IndexMap<String, DbtColumn>,
        _constraints: Option<BTreeMap<String, String>>,
    ) -> AdapterResult<IndexMap<String, DbtColumn>> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// grant_access_to
    #[allow(clippy::too_many_arguments)]
    fn grant_access_to(
        &self,
        _state: &State,
        _conn: &'_ mut dyn Connection,
        _entity: Arc<dyn BaseRelation>,
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
        _relation: Arc<dyn BaseRelation>,
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

    /// load dataframe only used by bigquery adapter
    #[allow(clippy::too_many_arguments)]
    fn load_dataframe(
        &self,
        _ctx: &QueryCtx,
        _conn: &'_ mut dyn Connection,
        _sql: &str,
        _database: &str,
        _schema: &str,
        _table_name: &str,
        _agate_table: Arc<AgateTable>,
        _file_path: &str,
        _field_delimiter: &str,
    ) -> AdapterResult<Value> {
        unimplemented!("only available with BigQuery or Salesforce adapter")
    }

    /// alter_table_add_columns
    fn alter_table_add_columns(
        &self,
        _state: &State,
        _conn: &'_ mut dyn Connection,
        _relation: Arc<dyn BaseRelation>,
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
        let kwargs = args!(
            information_schema => Value::from("INFORMATION_SCHEMA"),
            relations => Value::from_object(sources),
        );

        let result: Value = execute_macro(state, kwargs, "get_relation_last_modified")?;
        let result = result.downcast_object::<ResultObject>().unwrap();

        let table = result.table.as_ref().expect("AgateTable exists");
        let record_batch = table.original_record_batch();

        let identifier_column_values =
            get_column_values::<StringArray>(&record_batch, "IDENTIFIER")?;
        let schema_column_values = get_column_values::<StringArray>(&record_batch, "SCHEMA")?;
        let last_modified_column_values =
            get_column_values::<TimestampMillisecondArray>(&record_batch, "LAST_MODIFIED")?;

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

    /// Convert an Arrow [Schema] to a [Vec] of [Column]s.
    ///
    /// This is not part of the Jinja adapter API.
    ///
    /// NOTE(jason): This schema might come directly out of the driver and is not
    /// a sdf frontend schema - this function might not format types perfectly yet
    ///
    /// NOTE(felipecrv): we are working on making it easy to not confuse
    /// driver-generated schemas versus canonicalized sdf frontend schemas
    fn schema_to_columns(
        &self,
        _original: Option<&Arc<Schema>>,
        schema: &Arc<Schema>,
    ) -> AdapterResult<Vec<Column>> {
        let engine = self.engine();
        let type_formatter = engine.type_ops();
        let builder = ColumnBuilder::new(self.adapter_type());

        let fields = schema.fields();
        let mut columns = Vec::<Column>::with_capacity(fields.len());
        for field in fields {
            let column = builder.build(field, type_formatter)?;
            columns.push(column);
        }
        Ok(columns)
    }

    /// Get column schema from query
    fn get_column_schema_from_query(
        &self,
        state: &State,
        conn: &mut dyn Connection,
        ctx: &QueryCtx,
        sql: &str,
    ) -> AdapterResult<Vec<Column>> {
        if let Some(replay_adapter) = self.as_replay() {
            return replay_adapter.replay_get_column_schema_from_query(state, conn, ctx);
        }
        let batch = self.engine().execute(Some(state), conn, ctx, sql)?;
        let original_schema = Some(batch.schema());
        let sdf_arrow_schema = batch.schema(); // XXX: this is not a SDF schema
        self.schema_to_columns(original_schema.as_ref(), &sdf_arrow_schema)
    }

    /// Get columns in select sql
    fn get_columns_in_select_sql(
        &self,
        _conn: &'_ mut dyn Connection,
        _sql: &str,
    ) -> AdapterResult<Vec<Column>> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// Used by redshift and postgres to check if the database string is consistent with what's in the project `config`
    fn verify_database(&self, database: String) -> AdapterResult<Value> {
        match self.adapter_type() {
            AdapterType::Postgres => {
                let configured_database = self.engine().get_configured_database_name();
                if let Some(configured_database) = configured_database {
                    if database != configured_database {
                        Err(AdapterError::new(
                            AdapterErrorKind::UnexpectedDbReference,
                            format!(
                                "Cross-db references not allowed in the {} adapter ({} vs {})",
                                self.adapter_type(),
                                database,
                                configured_database
                            ),
                        ))
                    } else {
                        Ok(Value::from(()))
                    }
                } else {
                    // Replay engine does not have a configured database
                    Ok(Value::from(()))
                }
            }
            AdapterType::Redshift => {
                let ra3_node = self
                    .engine()
                    .config("ra3_node")
                    .unwrap_or(Cow::Borrowed("false"));

                // We have no guarantees that `database` is unquoted, but we do know that `configured_database` will be unquoted.
                // For the Redshift adapter, we can just trim the `"` character per `self.quote`.
                let database = database.trim_matches('\"');
                let configured_database = self.engine().config("database");

                if let Some(configured_database) = configured_database {
                    let ra3_node: bool = FromStr::from_str(&ra3_node).map_err(|_| {
                        AdapterError::new(
                            AdapterErrorKind::Configuration,
                            r#"Failed to parse ra3_node, expected "true" or "false""#,
                        )
                    })?;
                    if !database.eq_ignore_ascii_case(&configured_database) && !ra3_node {
                        return Err(AdapterError::new(
                            AdapterErrorKind::UnexpectedDbReference,
                            format!(
                                "Cross-db references allowed only in RA3.* node ({database} vs {configured_database})"
                            ),
                        ));
                    }
                }

                Ok(Value::from(()))
            }
            AdapterType::Snowflake
            | AdapterType::Bigquery
            | AdapterType::Databricks
            | AdapterType::Salesforce => {
                unimplemented!("only available with either Postgres or Redshift adapter")
            }
        }
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
    fn parse_partition_by(&self, partition_by: Value) -> AdapterResult<Value> {
        if self.adapter_type() == AdapterType::Bigquery {
            // Pure config parse; safe for both BigQuery and Replay (when adapter type is BigQuery)
            return crate::bigquery::adapter::parse_partition_by_value(partition_by);
        }
        unimplemented!("only available with BigQuery adapter")
    }

    /// get_table_options
    fn get_table_options(
        &self,
        state: &State,
        config: ModelConfig,
        node: &InternalDbtNodeWrapper,
        temporary: bool,
    ) -> AdapterResult<BTreeMap<String, Value>> {
        if self.adapter_type() == AdapterType::Bigquery {
            return crate::bigquery::adapter::get_table_options_value(
                state,
                config,
                node,
                temporary,
                self.adapter_type(),
            );
        }
        unimplemented!("only available with BigQuery adapter")
    }

    /// get_view_options
    fn get_view_options(
        &self,
        _state: &State,
        _config: ModelConfig,
        _node: &CommonAttributes,
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

    /// Lists all relations in the provided [CatalogAndSchema]
    fn list_relations(
        &self,
        query_ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        db_schema: &CatalogAndSchema,
    ) -> AdapterResult<Vec<Arc<dyn BaseRelation>>> {
        use crate::metadata::*;
        use dbt_common::adapter::AdapterType::*;

        if let Some(replay_adapter) = self.as_replay() {
            return replay_adapter.replay_list_relations(query_ctx, conn, db_schema);
        }

        let adapter = self.as_typed_base_adapter();
        match self.adapter_type() {
            Snowflake => snowflake::list_relations(adapter, query_ctx, conn, db_schema),
            Bigquery => bigquery::list_relations(adapter, query_ctx, conn, db_schema),
            Databricks => databricks::list_relations(adapter, query_ctx, conn, db_schema),
            Redshift => redshift::list_relations(adapter, query_ctx, conn, db_schema),
            Postgres | Salesforce => {
                let err = AdapterError::new(
                    AdapterErrorKind::Internal,
                    format!(
                        "list_relations_without_caching is not implemented for this adapter: {}",
                        self.adapter_type()
                    ),
                );
                Err(err)
            }
        }
    }

    /// Behavior (flags)
    fn behavior(&self) -> Vec<BehaviorFlag> {
        match self.adapter_type() {
            AdapterType::Snowflake => {
                // https://github.com/dbt-labs/dbt-adapters/blob/917301379d4ece300d32a3366c71daf0c4ac44aa/dbt-snowflake/src/dbt/adapters/snowflake/impl.py#L87
                let flag = BehaviorFlag::new(
                    "enable_iceberg_materializations",
                    false,
                    Some(
                        "Enabling Iceberg materializations introduces latency to metadata queries, specifically within the list_relations_without_caching macro. Since Iceberg benefits only those actively using it, we've made this behavior opt-in to prevent unnecessary latency for other users.",
                    ),
                    Some(
                        r#"Enabling Iceberg materializations introduces latency to metadata queries,
specifically within the list_relations_without_caching macro. Since Iceberg
benefits only those actively using it, we've made this behavior opt-in to
prevent unnecessary latency for other users."#,
                    ),
                    Some(
                        "https://docs.getdbt.com/reference/resource-configs/snowflake-configs#iceberg-table-format",
                    ),
                );
                vec![flag]
            }
            AdapterType::Databricks => {
                // https://github.com/databricks/dbt-databricks/blob/822b105b15e644676d9e1f47cbfd765cd4c1541f/dbt/adapters/databricks/impl.py#L87
                let use_info_schema_for_columns = BehaviorFlag::new(
                    "use_info_schema_for_columns",
                    false,
                    Some(
                        "Use info schema to gather column information to ensure complex types are not truncated. Incurs some overhead, so disabled by default.",
                    ),
                    None,
                    None,
                );

                let use_user_folder_for_python = BehaviorFlag::new(
                    "use_user_folder_for_python",
                    false,
                    Some(
                        "Use the user's home folder for uploading python notebooks. Shared folder use is deprecated due to governance concerns.",
                    ),
                    None,
                    None,
                );

                let use_materialization_v2 = BehaviorFlag::new(
                    "use_materialization_v2",
                    false,
                    Some(
                        "Use revamped materializations based on separating create and insert. This allows more performant column comments, as well as new column features.",
                    ),
                    None,
                    None,
                );

                vec![
                    use_info_schema_for_columns,
                    use_user_folder_for_python,
                    use_materialization_v2,
                ]
            }
            AdapterType::Bigquery
            | AdapterType::Postgres
            | AdapterType::Redshift
            | AdapterType::Salesforce => vec![],
        }
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
        _config: ModelConfig,
        _node: &dyn InternalDbtNodeAttributes,
        _is_incremental: bool,
    ) -> AdapterResult<String> {
        unimplemented!("only available with Databricks adapter")
    }

    /// update_tblproperties_for_uniform_iceberg
    fn update_tblproperties_for_uniform_iceberg(
        &self,
        _state: &State,
        _conn: &mut dyn Connection,
        _config: ModelConfig,
        _node: &InternalDbtNodeWrapper,
        _tblproperties: &mut BTreeMap<String, Value>,
    ) -> AdapterResult<()> {
        unimplemented!("only available with Databricks adapter")
    }

    /// is_uniform
    fn is_uniform(
        &self,
        _state: &State,
        _conn: &mut dyn Connection,
        _config: ModelConfig,
        _node: &InternalDbtNodeWrapper,
    ) -> AdapterResult<bool> {
        unimplemented!("only available with Databricks adapter")
    }

    /// get_relation_config
    fn get_relation_config(
        &self,
        _state: &State,
        _conn: &mut dyn Connection,
        _relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<Arc<dyn BaseRelationConfig>> {
        unimplemented!("only available with Databricks adapter")
    }

    /// get_config_from_model
    fn get_config_from_model(
        &self,
        _model: &dyn InternalDbtNodeAttributes,
    ) -> AdapterResult<Value> {
        unimplemented!("only available with Databricks adapter")
    }

    fn get_column_tags_from_model(
        &self,
        _model: &dyn InternalDbtNodeAttributes,
    ) -> AdapterResult<Value> {
        unimplemented!("only available with Databricks adapter")
    }

    /// clean_sql
    fn clean_sql(&self, _args: &str) -> AdapterResult<String> {
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
        let names_in_relation: Vec<String> =
            columns.iter().map(|c| c.name().to_lowercase()).collect();

        // missing columns
        let mut missing: Vec<String> = Vec::new();

        // Note: we're not checking dbt_updated_at or dbt_is_deleted
        // here because they aren't always present.
        let mut hardcoded_columns = vec!["dbt_scd_id", "dbt_valid_from", "dbt_valid_to"];

        if let Some(ref s) = strategy.hard_deletes
            && s == "new_record"
        {
            hardcoded_columns.push("dbt_is_deleted");
        }

        for column in hardcoded_columns {
            let desired = match column_names {
                Some(ref tree) => match tree.get(column) {
                    Some(v) => v.to_string(),
                    None => {
                        return Err(AdapterError::new(
                            AdapterErrorKind::Configuration,
                            format!("Could not find key {column}"),
                        ));
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
                format!("There are missing columns: {missing:?}"),
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
                "You cannot set both the invalidate_hard_deletes and hard_deletes config properties on the same snapshot.",
            ));
        }

        if invalidate_hard_deletes.is_some() {
            return Ok("invalidate".to_string());
        }

        match hard_deletes {
            None => Ok("ignore".to_string()),
            Some(val) => {
                // Treat null values same as missing (None)
                if val.is_none() {
                    return Ok("ignore".to_string());
                }
                match val.as_str() {
                    Some("invalidate") => Ok("invalidate".to_string()),
                    Some("new_record") => Ok("new_record".to_string()),
                    Some("ignore") => Ok("ignore".to_string()),
                    Some(_) => Err(AdapterError::new(
                        AdapterErrorKind::Configuration,
                        "Invalid string value for property hard_deletes.",
                    )),
                    None => Err(AdapterError::new(
                        AdapterErrorKind::Configuration,
                        "Invalid type for property hard_deletes (expected string).",
                    )),
                }
            }
        }
    }

    /// Optional fast-path for replay adapters: return schema existence from the trace
    /// when available. Default is None for non-replay adapters.
    fn schema_exists_from_trace(&self, _database: &str, _schema: &str) -> Option<bool> {
        None
    }

    /// Get the default ADBC statement options
    fn get_adbc_execute_options(&self, _state: &State) -> ExecuteOptions {
        match self.adapter_type() {
            AdapterType::Bigquery => vec![(
                QUERY_LINK_FAILED_JOB.to_string(),
                OptionValue::String("true".to_string()),
            )],
            _ => Vec::new(),
        }
    }
}

/// Abstract interface for the concrete replay adapter implementation.
///
/// NOTE: this is a growing interface that is currently growing.
pub trait ReplayAdapter: TypedBaseAdapter {
    fn replay_use_warehouse(
        &self,
        conn: &'_ mut dyn Connection,
        warehouse: String,
        node_id: &str,
    ) -> FsResult<()>;

    fn replay_new_connection(
        &self,
        state: Option<&State>,
        node_id: Option<String>,
    ) -> AdapterResult<Box<dyn Connection>>;

    #[allow(clippy::too_many_arguments)]
    fn replay_execute(
        &self,
        state: Option<&State>,
        conn: &'_ mut dyn Connection,
        ctx: &QueryCtx,
        sql: &str,
        auto_begin: bool,
        fetch: bool,
        limit: Option<i64>,
        options: Option<HashMap<String, String>>,
    ) -> AdapterResult<(AdapterResponse, AgateTable)>;

    fn replay_add_query(
        &self,
        ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        sql: &str,
        auto_begin: bool,
        bindings: Option<&Value>,
        abridge_sql_log: bool,
    ) -> AdapterResult<()>;

    fn replay_get_relation(
        &self,
        state: &State,
        query_ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        database: &str,
        schema: &str,
        identifier: &str,
    ) -> AdapterResult<Option<Arc<dyn BaseRelation>>>;

    fn replay_quote(&self, state: &State, identifier: &str) -> AdapterResult<String>;

    fn replay_quote_seed_column(
        &self,
        state: &State,
        column: &str,
        quote_config: Option<bool>,
    ) -> AdapterResult<String>;

    fn replay_convert_type(&self, state: &State, data_type: &DataType) -> AdapterResult<String>;

    fn replay_list_relations(
        &self,
        query_ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        db_schema: &CatalogAndSchema,
    ) -> AdapterResult<Vec<Arc<dyn BaseRelation>>>;

    fn replay_get_column_schema_from_query(
        &self,
        state: &State,
        _conn: &mut dyn Connection,
        _query_ctx: &QueryCtx,
    ) -> AdapterResult<Vec<Column>>;

    fn replay_get_columns_in_relation(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
        cache_result: Option<Vec<Column>>,
    ) -> Result<Value, minijinja::Error>;

    fn replay_render_raw_columns_constraints(
        &self,
        _state: &State,
        _columns_map: IndexMap<String, DbtColumn>,
    ) -> AdapterResult<Vec<String>>;

    fn replay_render_raw_model_constraints(
        &self,
        _state: &State,
        _raw_constraints: &[ModelConstraint],
    ) -> Result<Value, minijinja::Error>;
}
