use crate::metadata::MetadataAdapter;
use crate::sql_engine::SqlEngine;
use crate::typed_adapter::TypedBaseAdapter;
use crate::{AdapterResponse, AdapterResult};

use dbt_agate::AgateTable;
use dbt_common::FsResult;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::relations::base::ComponentName;
use dbt_xdbc::Connection;
use minijinja::arg_utils::ArgParser;
use minijinja::dispatch_object::DispatchObject;
use minijinja::{Error as MinijinjaError, ErrorKind as MinijinjaErrorKind, State, Value};

use std::fmt;
use std::sync::Arc;

/// The type of the adapter.
///
/// Used to identify the specific database adapter being used.
pub type AdapterType = dbt_common::adapter::AdapterType;

/// Type queries to be implemented for every [BaseAdapter]
pub trait AdapterTyping {
    /// Get name/type of this adapter
    fn adapter_type(&self) -> AdapterType;

    /// Get a reference to the metadata adapter if supported.
    fn as_metadata_adapter(&self) -> Option<&dyn MetadataAdapter>;

    /// Get a reference to the typed base adapter if supported.
    fn as_typed_base_adapter(&self) -> &dyn TypedBaseAdapter;

    /// Get column type instance
    fn column_type(&self) -> Option<Value>;

    /// Get the [SqlEngine], if available
    fn engine(&self) -> Option<&Arc<SqlEngine>>;

    /// Get the [ResolvedQuoting]
    fn quoting(&self) -> ResolvedQuoting;

    /// Quote a component of a relation
    fn quote_component(&self, identifier: &str, component: ComponentName) -> String {
        let quoted = match component {
            ComponentName::Database => self.quoting().database,
            ComponentName::Schema => self.quoting().schema,
            ComponentName::Identifier => self.quoting().identifier,
        };
        if quoted {
            let adapter = self.as_typed_base_adapter();
            adapter.quote(identifier)
        } else {
            identifier.to_string()
        }
    }
}

/// Base adapter
pub trait BaseAdapter: fmt::Display + fmt::Debug + AdapterTyping + Send + Sync {
    /// Commit
    fn commit(&self, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(Value::from(true))
    }

    /// Create a new connection
    fn new_connection(&self) -> Result<Box<dyn Connection>, MinijinjaError>;

    /// Cache added
    fn cache_added(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("cache_added")
    }

    /// Cache dropped
    fn cache_dropped(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("cache_dropped")
    }

    /// Cache renamed
    fn cache_renamed(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("cache_renamed")
    }

    /// Standardize grants dict
    fn standardize_grants_dict(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError>;

    /// Encloses identifier in the correct quotes for the adapter when escaping reserved column names etc.
    fn quote(&self, state: &State, _args: &[Value]) -> Result<Value, MinijinjaError>;

    /// Quote as configured.
    fn quote_as_configured(&self, state: &State, _args: &[Value]) -> Result<Value, MinijinjaError>;

    /// Quote seed column.
    fn quote_seed_column(&self, state: &State, _args: &[Value]) -> Result<Value, MinijinjaError>;

    /// Convert type.
    fn convert_type(&self, state: &State, _args: &[Value]) -> Result<Value, MinijinjaError>;

    /// Render raw model constants.
    fn render_raw_model_constraints(
        &self,
        state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError>;

    /// TODO: this is a stub (used in postgres__list_schemas macro and maybe others)
    fn verify_database(&self, state: &State, _args: &[Value]) -> Result<Value, MinijinjaError>;

    /// Dispatch.
    fn dispatch(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        let macro_name = parser.get::<String>("macro_name")?;
        let package_name: Option<String> = parser.get_optional::<String>("macro_namespace");

        if macro_name.contains('.') {
            let parts: Vec<&str> = macro_name.split('.').collect();
            return Err(MinijinjaError::new(
                MinijinjaErrorKind::InvalidOperation,
                format!(
                    "In adapter.dispatch, got a macro name of \"{}\", but \".\" is not a valid macro name component. Did you mean `adapter.dispatch(\"{}\", macro_namespace=\"{}\")`?",
                    macro_name, parts[1], parts[0]
                ),
            ));
        }

        Ok(Value::from_object(DispatchObject {
            macro_name,
            package_name,
            strict: false,
            auto_execute: false,
            context: Some(state.get_base_context()),
        }))
    }

    /// Gets the macro for the given incremental strategy.
    ///
    /// Additionally some validations are done:
    /// 1. Assert that if the given strategy is a "builtin" strategy, then it must
    ///    also be defined as a "valid" strategy for the associated adapter
    /// 2. Assert that the incremental strategy exists in the model context
    fn get_incremental_strategy_macro(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError>;

    /// Execute the given SQL. This is a thin wrapper around [SqlEngine.execute].
    ///
    /// ```python
    /// def execute(
    ///     self,
    ///     sql: str,
    ///     auto_begin: bool = False,
    ///     fetch: bool = False,
    ///     limit: Optional[int] = None,
    /// ) -> Tuple[AdapterResponse, "agate.Table"]:
    ///     """
    ///     :param str sql: The sql to execute.
    ///     :param bool auto_begin: If set, and dbt is not currently inside a transaction,
    ///                             automatically begin one.
    ///     :param bool fetch: If set, fetch results.
    ///     :param Optional[int] limit: If set, only fetch n number of rows
    ///     :return: A tuple of the query status and results (empty if fetch=False).
    ///     :rtype: Tuple[AdapterResponse, "agate.Table"]
    ///     """
    /// ```
    fn execute(
        &self,
        state: &State,
        sql: &str,
        auto_begin: bool,
        fetch: bool,
        limit: Option<i64>,
    ) -> AdapterResult<(AdapterResponse, AgateTable)>;

    /// Execute a statement, expect no results.
    fn exec_stmt(
        &self,
        state: &State,
        sql: &str,
        auto_begin: bool,
    ) -> AdapterResult<AdapterResponse> {
        let (response, _) = self.execute(
            state, sql, auto_begin, false, // fetch
            None,  // limit
        )?;
        Ok(response)
    }

    /// Execute a query and get results in an [AgateTable].
    fn query(
        &self,
        state: &State,
        sql: &str,
        limit: Option<i64>,
    ) -> AdapterResult<(AdapterResponse, AgateTable)> {
        self.execute(state, sql, false, true, limit)
    }

    /// Add Query
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/9f39ba3d94b02eeb3aef40fe161af844e15944e4/dbt-adapters/src/dbt/adapters/sql/connections.py#L69
    ///
    /// ```python
    /// def add_query(
    ///    self,
    ///    sql: str,
    ///    auto_begin: bool = True,
    ///    bindings: Optional[Any] = None,
    ///    abridge_sql_log: bool = False,
    ///    retryable_exceptions: Tuple[Type[Exception], ...] = tuple(),
    ///    retry_limit: int = 1,
    /// ) -> Tuple[Connection, Any]:
    /// ```
    fn add_query(
        &self,
        state: &State,
        sql: &str,
        auto_begin: bool,
        bindings: Option<&Value>,
        abridge_sql_log: bool,
    ) -> AdapterResult<()>;

    /// Drop relation.
    fn drop_relation(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError>;

    /// Truncate relation.
    fn truncate_relation(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError>;

    /// Rename relation.
    fn rename_relation(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError>;

    /// Expand target column types.
    fn expand_target_column_types(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError>;

    /// List schemas.
    fn list_schemas(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError>;

    /// List relations without caching.
    fn list_relations_without_caching(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        unimplemented!("Only available with BigQuery adapter")
    }

    /// Create schema.
    fn create_schema(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError>;

    /// Drop schema.
    fn drop_schema(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError>;

    /// Valid snapshot target.
    fn valid_snapshot_target(&self, state: &State, args: &[Value])
    -> Result<Value, MinijinjaError>;

    /// Assert valid snapshot target given strategy.
    fn assert_valid_snapshot_target_given_strategy(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError>;

    /// Get hard deletes behavior.
    fn get_hard_deletes_behavior(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError>;

    /// Get relation.
    fn get_relation(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError>;

    /// Returns a list of columns.
    fn get_missing_columns(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError>;

    /// Get columns in relation.
    fn get_columns_in_relation(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError>;

    /// Render raw columns constants.
    fn render_raw_columns_constraints(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError>;

    /// Check if schema exists
    fn check_schema_exists(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError>;

    /// Get relations by pattern
    fn get_relations_by_pattern(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError>;

    /// Get column schema from query
    fn get_column_schema_from_query(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError>;

    /// Get columns in select sql
    fn get_columns_in_select_sql(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError>;

    /// list_relations_without_caching
    fn add_time_ingestion_partition_column(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// parse_partition_by
    fn parse_partition_by(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// is_replaceable
    fn is_replaceable(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// nest_column_data_types
    fn nest_column_data_types(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// copy_table
    fn copy_table(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError>;

    /// update_columns
    fn update_columns(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError>;

    /// update_table_description
    fn update_table_description(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError>;

    /// alter_table_add_columns
    fn alter_table_add_columns(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError>;

    /// load_dataframe
    fn load_dataframe(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// upload_file
    fn upload_file(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// get_common_options
    fn get_common_options(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// get_table_options
    fn get_table_options(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// get_view_options
    fn get_view_options(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// get_bq_table
    fn get_bq_table(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// describe_relation
    fn describe_relation(&self, _args: &[Value]) -> Result<Value, MinijinjaError>;

    /// grant_access_to
    fn grant_access_to(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError>;

    /// get_dataset_location
    fn get_dataset_location(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// compare_dbr_version
    fn compare_dbr_version(&self, _state: &State, _args: &[Value])
    -> Result<Value, MinijinjaError>;

    /// compute_external_path
    fn compute_external_path(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError>;

    /// update_tblproperties_for_iceberg
    fn update_tblproperties_for_iceberg(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError>;

    /// generate_unique_temporary_table_suffix
    fn generate_unique_temporary_table_suffix(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError>;

    /// parse_columns_and_constraints
    fn parse_columns_and_constraints(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        unimplemented!("only available with Databricks adapter")
    }

    /// valid_incremental_strategies
    fn valid_incremental_strategies(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError>;

    /// get_partitions_metadata
    fn get_partitions_metadata(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError>;

    /// get_persist_doc_columns
    fn get_persist_doc_columns(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError>;

    /// clean_sql
    fn clean_sql(&self, _args: &[Value]) -> Result<Value, MinijinjaError>;

    /// get_relation_config
    fn get_relation_config(&self, _state: &State, _args: &[Value])
    -> Result<Value, MinijinjaError>;

    /// get_config_from_model
    fn get_config_from_model(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError>;

    /// get_relations_without_caching
    fn get_relations_without_caching(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError>;

    /// parse_index
    fn parse_index(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError>;

    /// redact_credentials
    fn redact_credentials(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError>;

    /// Behavior (flags)
    fn behavior(&self) -> Value;

    /// This adapter as a Value
    fn as_value(&self) -> Value;

    /// Used internally to attempt executing a Snowflake `use warehouse [name]` statement from BridgeAdapter
    /// For other BaseAdapter types, this is noop
    ///
    /// # Returns
    ///
    /// Returns true if the warehouse was overridden, false otherwise
    fn use_warehouse(&self, _warehouse: Option<String>, _node_id: &str) -> FsResult<bool> {
        Ok(false)
    }

    /// Used internally to attempt executing a Snowflake `use warehouse [name]` statement from BridgeAdapter
    ///
    /// To restore to the warehouse configured in profiles.yml
    /// For other BaseAdapter types, this is noop
    fn restore_warehouse(&self, _node_id: &str) -> FsResult<()> {
        Ok(())
    }
}
