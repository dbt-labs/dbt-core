use crate::adapter_engine::AdapterEngine;
use crate::cache::RelationCache;
use crate::column::ColumnStatic;
use crate::metadata::*;
use crate::query_cache::QueryCache;
use crate::snapshots::SnapshotStrategy;
use crate::typed_adapter::{ReplayAdapter, TypedBaseAdapter};
use crate::{AdapterResponse, AdapterResult};

use dbt_agate::AgateTable;
use dbt_common::FsResult;
use dbt_common::cancellation::CancellationToken;
use dbt_common::io_args::ReplayMode;
use dbt_schema_store::SchemaStoreTrait;
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::dbt_column::DbtColumn;
use dbt_schemas::schemas::project::QueryComment;
use dbt_schemas::schemas::properties::ModelConstraint;
use dbt_schemas::schemas::relations::base::{BaseRelation, ComponentName};
use dbt_xdbc::{Backend, Connection};
use indexmap::IndexMap;
use minijinja::dispatch_object::DispatchObject;
use minijinja::{Error as MinijinjaError, ErrorKind as MinijinjaErrorKind, State, Value};

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;

/// The type of the adapter. Used to identify the specific database adapter being used.
pub type AdapterType = dbt_common::adapter::AdapterType;

pub fn backend_of(adapter_type: AdapterType) -> Backend {
    match adapter_type {
        AdapterType::Postgres => Backend::Postgres,
        AdapterType::Snowflake => Backend::Snowflake,
        AdapterType::Bigquery => Backend::BigQuery,
        AdapterType::Databricks => Backend::Databricks,
        AdapterType::Redshift => Backend::Redshift,
        AdapterType::Salesforce => Backend::Salesforce,
    }
}

/// Type queries to be implemented for every [BaseAdapter]
pub trait AdapterTyping {
    /// Get name/type of this adapter
    fn adapter_type(&self) -> AdapterType {
        self.engine().adapter_type()
    }

    /// Get a reference to the metadata adapter if supported.
    fn as_metadata_adapter(&self) -> Option<&dyn MetadataAdapter>;

    /// Get a reference to the typed base adapter if supported.
    fn as_typed_base_adapter(&self) -> &dyn TypedBaseAdapter;

    /// True if called on the [ParseAdapter].
    fn is_parse(&self) -> bool {
        false
    }

    /// This adapter as the replay adapter if it is one, None otherwise.
    fn as_replay(&self) -> Option<&dyn ReplayAdapter> {
        None
    }

    /// Get column type instance
    fn column_type(&self) -> Option<Value> {
        let value = Value::from_object(ColumnStatic::new(self.adapter_type()));
        Some(value)
    }

    /// Get the [SqlEngine]
    fn engine(&self) -> &Arc<AdapterEngine>;

    /// Get the [ResolvedQuoting]
    fn quoting(&self) -> ResolvedQuoting {
        self.engine().quoting()
    }

    /// Quote a component of a relation
    fn quote_component(
        &self,
        state: &State,
        identifier: &str,
        component: ComponentName,
    ) -> AdapterResult<String> {
        let quoted = match component {
            ComponentName::Database => self.quoting().database,
            ComponentName::Schema => self.quoting().schema,
            ComponentName::Identifier => self.quoting().identifier,
        };
        if quoted {
            let adapter = self.as_typed_base_adapter();
            adapter.quote(state, identifier)
        } else {
            Ok(identifier.to_string())
        }
    }

    fn cancellation_token(&self) -> CancellationToken {
        self.engine().cancellation_token()
    }
}

/// Base adapter
pub trait BaseAdapter: fmt::Debug + AdapterTyping + Send + Sync {
    /// Commit
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L000
    ///
    /// ```python
    /// def commit(self) -> None
    /// ```
    fn commit(&self) -> Result<Value, MinijinjaError> {
        Ok(Value::from(true))
    }

    /// Create a new connection
    fn new_connection(
        &self,
        state: Option<&State>,
        node_id: Option<String>,
    ) -> Result<Box<dyn Connection>, MinijinjaError>;

    /// Cache added
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L644
    ///
    /// ```python
    /// def cache_added(
    ///     self,
    ///     relation: Optional[BaseRelation]
    /// ) -> None
    /// ```
    fn cache_added(
        &self,
        _state: &State,
        _relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        unimplemented!("cache_added")
    }

    /// Cache dropped
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L655
    ///
    /// ```python
    /// def cache_dropped(
    ///     self,
    ///     relation: Optional[BaseRelation]
    /// ) -> None
    /// ```
    fn cache_dropped(
        &self,
        _state: &State,
        _relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        unimplemented!("cache_dropped")
    }

    /// Cache renamed
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L667
    ///
    /// ```python
    /// def cache_renamed(
    ///     self,
    ///     from_relation: Optional[BaseRelation],
    ///     to_relation: Optional[BaseRelation]
    /// ) -> None
    /// ```
    fn cache_renamed(
        &self,
        _state: &State,
        _from_relation: Arc<dyn BaseRelation>,
        _to_relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        unimplemented!("cache_renamed")
    }

    /// Standardize grants dict
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L823
    ///
    /// ```python
    /// def standardize_grants_dict(
    ///     self,
    ///     grants_table: "agate.Table"
    /// ) -> dict
    /// ```
    fn standardize_grants_dict(
        &self,
        _state: &State,
        _grants_table: &Arc<AgateTable>,
    ) -> Result<Value, MinijinjaError>;

    /// Encloses identifier in the correct quotes for the adapter when escaping reserved column names etc.
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/5fba80c621c3f0f732dba71aa6cf9055792b6495/dbt-adapters/src/dbt/adapters/base/impl.py#L1064
    ///
    /// ```python
    /// @classmethod
    /// def quote(
    ///     cls,
    ///     identifier: str
    /// ) -> str
    /// ```
    fn quote(&self, state: &State, identifier: &str) -> Result<Value, MinijinjaError>;

    /// Quote as configured.
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/5fba80c621c3f0f732dba71aa6cf9055792b6495/dbt-adapters/src/dbt/adapters/base/impl.py#L1070C5-L1070C75
    ///
    /// ```python
    /// def quote_as_configured(
    ///     self,
    ///     identifier: str,
    ///     quote_key: str
    /// ) -> str
    /// ```
    fn quote_as_configured(
        &self,
        state: &State,
        identifier: &str,
        quote_key: &str,
    ) -> Result<Value, MinijinjaError>;

    /// Quote seed column.
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/5fba80c621c3f0f732dba71aa6cf9055792b6495/dbt-adapters/src/dbt/adapters/base/impl.py#L1091
    ///
    /// ```python
    /// def quote_seed_column(
    ///     self,
    ///     column: str,
    ///     quote_config: Optional[bool]
    /// ) -> str
    /// ```
    fn quote_seed_column(
        &self,
        state: &State,
        column: &str,
        quote_config: Option<bool>,
    ) -> Result<Value, MinijinjaError>;

    /// Convert type.
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L1221
    ///
    /// ```python
    /// def convert_type(
    ///     cls,
    ///     agate_table: "agate.Table",
    ///     col_idx: int
    /// ) -> Optional[str]
    /// ```
    fn convert_type(
        &self,
        state: &State,
        _table: &Arc<AgateTable>,
        _col_idx: i64,
    ) -> Result<Value, MinijinjaError>;

    /// Render raw model constraints.
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L1891
    ///
    /// ```python
    /// def render_raw_model_constraints(
    ///     cls,
    ///     raw_constraints: List[Dict[str, Any]]
    /// ) -> List[str]
    ///
    /// ```
    fn render_raw_model_constraints(
        &self,
        state: &State,
        _raw_constraints: &[ModelConstraint],
    ) -> Result<Value, MinijinjaError>;

    /// Verify database.
    /// ```
    fn verify_database(&self, state: &State, _database: String) -> Result<Value, MinijinjaError>;

    /// Dispatch.
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L226
    ///
    /// ```python
    /// def dispatch(
    ///     self,
    ///     macro_name: str,
    ///     macro_namespace: Optional[str] = None
    /// ) -> DispatchObject
    /// ```
    fn dispatch(
        &self,
        state: &State,
        macro_name: &str,
        macro_namespace: Option<&str>,
    ) -> Result<Value, MinijinjaError> {
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
            macro_name: macro_name.to_string(),
            package_name: macro_namespace.map(|s| s.to_string()),
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
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L1704
    ///
    /// ```python
    /// def get_incremental_strategy_macro(
    ///     self,
    ///     context: dict,
    ///     strategy: str
    /// ) -> DispatchObject
    /// ```
    fn get_incremental_strategy_macro(
        &self,
        state: &State,
        strategy: &str,
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
    ///     options: Optional[Dict[str, str]],
    /// ) -> Tuple[AdapterResponse, "agate.Table"]:
    ///     """
    ///     :param str sql: The sql to execute.
    ///     :param bool auto_begin: If set, and dbt is not currently inside a transaction,
    ///                             automatically begin one.
    ///     :param bool fetch: If set, fetch results.
    ///     :param Optional[int] limit: If set, only fetch n number of rows
    ///     :param Optional[Dict[str, str]] options: If set, pass ADBC options to the execute call
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
        options: Option<HashMap<String, String>>,
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
            None,  // options
        )?;
        Ok(response)
    }

    /// Execute a query and get results in an [AgateTable].
    fn exec_query(
        &self,
        state: &State,
        sql: &str,
        limit: Option<i64>,
    ) -> AdapterResult<(AdapterResponse, AgateTable)> {
        self.execute(state, sql, false, true, limit, None)
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

    /// Submit Python job
    ///
    /// Executes Python code in the warehouse's Python runtime.
    /// For Snowflake, this wraps the Python code in a stored procedure.
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L1603
    ///
    /// ```python
    /// def submit_python_job(self, parsed_model: dict, compiled_code: str) -> AdapterResponse:
    /// ```
    fn submit_python_job(
        &self,
        state: &State,
        model: &Value,
        compiled_code: &str,
    ) -> AdapterResult<AdapterResponse>;

    /// Drop relation.
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/sql/impl.py#L145
    ///
    /// ```python
    /// def drop_relation(
    ///     self,
    ///     relation: BaseRelation
    /// ) -> None
    /// ```
    fn drop_relation(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError>;

    /// Truncate relation.
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/sql/impl.py#L152
    ///
    /// ```python
    /// def truncate_relation(
    ///     self,
    ///     relation: BaseRelation
    /// ) -> None
    /// ```
    fn truncate_relation(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError>;

    /// Rename relation.
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/sql/impl.py#L155
    ///
    /// ```python
    /// def rename_relation(
    ///     self,
    ///     from_relation: BaseRelation,
    ///     to_relation: BaseRelation
    /// ) -> None
    /// ```
    fn rename_relation(
        &self,
        state: &State,
        from_relation: Arc<dyn BaseRelation>,
        to_relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError>;

    /// Expand target column types.
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L764
    ///
    /// ```python
    /// def expand_column_types(
    ///     self,
    ///     goal: BaseRelation,
    ///     current: BaseRelation
    /// ) -> None
    /// ```
    fn expand_target_column_types(
        &self,
        state: &State,
        from_relation: Arc<dyn BaseRelation>,
        to_relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError>;

    /// List schemas.
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/sql/impl.py#L217
    ///
    /// ```python
    /// def list_schemas(
    ///     self,
    ///     database: str
    /// ) -> List[str]
    /// ```
    fn list_schemas(&self, state: &State, database: &str) -> Result<Value, MinijinjaError>;

    /// List relations without caching.
    fn list_relations_without_caching(
        &self,
        state: &State,
        schema_relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError>;

    /// Create schema.
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/sql/impl.py#L166
    ///
    /// ```python
    /// def create_schema(
    ///     self,
    ///     relation: BaseRelation
    /// ) -> None
    /// ```
    fn create_schema(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError>;

    /// Drop schema.
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/sql/impl.py#L177
    ///
    /// ```python
    /// def drop_schema(
    ///     self,
    ///     relation: BaseRelation
    /// ) -> None
    /// ```
    fn drop_schema(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError>;

    /// Valid snapshot target.
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L884
    ///
    /// ```python
    /// def valid_snapshot_target(
    ///     relation: BaseRelation,
    ///     column_names: Optional[Dict[str, str]] = None
    /// ) -> None
    /// ```
    fn valid_snapshot_target(
        &self,
        state: &State,
        _relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError>;

    /// Assert valid snapshot target given strategy.
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L917
    ///
    /// ```python
    /// def assert_valid_snapshot_target_given_strategy(
    ///     relation: BaseRelation,
    ///     column_names: Dict[str, str],
    ///     strategy: SnapshotStrategy
    /// ) -> None
    /// ```
    fn assert_valid_snapshot_target_given_strategy(
        &self,
        state: &State,
        _relation: Arc<dyn BaseRelation>,
        _column_names: Option<&BTreeMap<String, String>>,
        _strategy: &Arc<SnapshotStrategy>,
    ) -> Result<Value, MinijinjaError>;

    /// Get hard deletes behavior.
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L1964
    ///
    /// ```python
    /// def get_hard_deletes_behavior(
    ///     cls,
    ///     config: Dict[str, str]
    /// ) -> str
    /// ```
    fn get_hard_deletes_behavior(
        &self,
        state: &State,
        config: BTreeMap<String, Value>,
    ) -> Result<Value, MinijinjaError>;

    /// Get relation.
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/5fba80c621c3f0f732dba71aa6cf9055792b6495/dbt-adapters/src/dbt/adapters/base/impl.py#L1014
    ///
    /// ```python
    /// def get_relation(
    ///     self,
    ///     database: str,
    ///     schema: str,
    ///     identifier: str
    /// )  -> Optional[BaseRelation]
    /// ```
    ///
    fn get_relation(
        &self,
        state: &State,
        database: &str,
        schema: &str,
        identifier: &str,
    ) -> Result<Value, MinijinjaError>;

    /// Get a catalog relation object.
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/c16cc7047e8678f8bb88ae294f43da2c68e9f5cc/dbt-adapters/src/dbt/adapters/base/impl.py#L338
    ///
    /// ```python
    /// def build_catalog_relation(
    ///     self,
    ///     model: RelationConfig
    /// )  -> Optional[CatalogRelation]
    /// ```
    ///
    /// In Core, there are numerous derived flavors of CatalogRelation.
    /// We handle this in Fusion as a piecemeal instantiated flat object
    /// and push down validation to the DDL level.
    fn build_catalog_relation(&self, model: &Value) -> Result<Value, MinijinjaError>;

    /// Get all relevant metadata about a dynamic table to return as a dict to Agate Table row
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/703180a871f2960cd0c91765ffc4b1dc111d615b/dbt-snowflake/src/dbt/adapters/snowflake/impl.py#L510
    ///
    /// ```python
    /// def describe_dynamic_table(self, relation: SnowflakeRelation) -> Dict[str, Any]
    /// ```
    fn describe_dynamic_table(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError>;

    /// Get a catalog integration object.
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/c16cc7047e8678f8bb88ae294f43da2c68e9f5cc/dbt-adapters/src/dbt/adapters/base/impl.py#L334
    ///
    /// ```python
    /// def get_catalog_integration(
    ///     self,
    ///     name: str,
    /// )  -> Optional[CatalogRelation]
    /// ```
    fn get_catalog_integration(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        unimplemented!(
            "get_catalog_integration is unavailable in Fusion. Access catalogs metadata directly from a catalog relation obtained using adapter.build_catalog_relation(model: RelationConfig)"
        )
    }

    /// Get missing columns.
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L852
    ///
    /// ```python
    /// def get_missing_columns(
    ///     from_relation: BaseRelation,
    ///     to_relation: BaseRelation
    /// ) -> List[BaseColumn]
    /// ```
    fn get_missing_columns(
        &self,
        state: &State,
        _from_relation: Arc<dyn BaseRelation>,
        _to_relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError>;

    /// Get columns in relation.
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L741
    ///
    /// ```python
    /// def get_columns_in_relation(
    ///     self,
    ///     relation: BaseRelation
    /// ) -> List[Column]
    /// ```
    fn get_columns_in_relation(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError>;

    /// Render raw columns constants.
    fn render_raw_columns_constraints(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError>;

    /// Check if schema exists
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L849
    ///
    /// ```python
    /// def check_schema_exists(
    ///     self,
    ///     database: str,
    ///     schema: str
    /// ) -> bool
    /// ```
    fn check_schema_exists(
        &self,
        state: &State,
        database: &str,
        schema: &str,
    ) -> Result<Value, MinijinjaError>;

    /// Get relations by pattern
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L858
    ///
    /// ```python
    /// def get_relations_by_pattern(
    ///     self,
    ///     schema_pattern: str,
    ///     table_pattern: str,
    ///     exclude: Optional[str] = None,
    ///     database: Optional[str] = None,
    ///     quote_table: Optional[bool] = None,
    ///     excluded_schemas: Optional[List[str]] = None
    /// ) -> List[BaseRelation]
    /// ```
    #[allow(clippy::too_many_arguments)]
    fn get_relations_by_pattern(
        &self,
        state: &State,
        schema_pattern: &str,
        table_pattern: &str,
        exclude: Option<&str>,
        database: Option<&str>,
        quote_table: Option<bool>,
        excluded_schemas: Option<Value>,
    ) -> Result<Value, MinijinjaError>;

    /// Get column schema from query
    fn get_column_schema_from_query(
        &self,
        state: &State,
        sql: &str,
    ) -> Result<Value, MinijinjaError>;

    /// Get columns in select sql
    fn get_columns_in_select_sql(&self, state: &State, sql: &str) -> Result<Value, MinijinjaError>;

    /// Add time ingestion partition column
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L259
    ///
    /// ```python
    /// @available.parse(lambda *a, **k: [])
    /// def add_time_ingestion_partition_column(
    ///     self,
    ///     partition_by,
    ///     columns
    /// ) -> List[BigQueryColumn]
    /// ```
    fn add_time_ingestion_partition_column(
        &self,
        _state: &State,
        _columns: &Value,
        _partition_config: dbt_schemas::schemas::manifest::BigqueryPartitionConfig,
    ) -> Result<Value, MinijinjaError> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// Parse partition by
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L581
    ///
    /// ```python
    /// @available
    /// def parse_partition_by(
    ///     self,
    ///     raw_partition_by: Any
    /// ) -> Optional[PartitionConfig]
    /// ```
    fn parse_partition_by(
        &self,
        _state: &State,
        _raw_partition_by: &Value,
    ) -> Result<Value, MinijinjaError> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// Is replaceable
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L541
    ///
    /// ```python
    /// def is_replaceable(
    ///     self,
    ///     relation: Optional[BaseRelation],
    ///     partition_by: Optional[dict],
    ///     cluster_by: Optional[dict]
    /// ) -> bool
    /// ```
    fn is_replaceable(
        &self,
        _state: &State,
        _relation: Option<Arc<dyn BaseRelation>>,
        _partition_by: Option<dbt_schemas::schemas::manifest::BigqueryPartitionConfig>,
        _cluster_by: Option<dbt_schemas::schemas::manifest::BigqueryClusterConfig>,
    ) -> Result<Value, MinijinjaError> {
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
    fn copy_table(
        &self,
        state: &State,
        tmp_relation_partitioned: Arc<dyn BaseRelation>,
        target_relation_partitioned: Arc<dyn BaseRelation>,
        materialization: &str,
    ) -> Result<Value, MinijinjaError>;

    /// update_columns
    fn update_columns(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
        columns: IndexMap<String, DbtColumn>,
    ) -> Result<Value, MinijinjaError>;

    /// update_table_description
    fn update_table_description(
        &self,
        state: &State,
        database: &str,
        schema: &str,
        identifier: &str,
        description: &str,
    ) -> Result<Value, MinijinjaError>;

    /// alter_table_add_columns
    fn alter_table_add_columns(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
        columns: &Value,
    ) -> Result<Value, MinijinjaError>;

    /// load_dataframe
    #[allow(clippy::too_many_arguments)]
    fn load_dataframe(
        &self,
        _state: &State,
        _database: &str,
        _schema: &str,
        _table_name: &str,
        _agate_table: Arc<AgateTable>,
        _file_path: &str,
        _field_delimiter: &str,
    ) -> Result<Value, MinijinjaError>;

    /// upload_file
    fn upload_file(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// get_common_options
    fn get_common_options(
        &self,
        _state: &State,
        _config: dbt_schemas::schemas::project::ModelConfig,
        _node: &dbt_schemas::schemas::InternalDbtNodeWrapper,
        _temporary: bool,
    ) -> Result<Value, MinijinjaError> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// Get table options
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/57b131a11ea24b79cfebda003c15456972892427/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L793
    ///
    /// ```python
    /// def get_table_options(
    ///     self, config: Dict[str, Any], node: Dict[str, Any], temporary: bool
    /// ) -> Dict[str, Any]:
    /// ```
    fn get_table_options(
        &self,
        _state: &State,
        _config: dbt_schemas::schemas::project::ModelConfig,
        _node: &dbt_schemas::schemas::InternalDbtNodeWrapper,
        _temporary: bool,
    ) -> Result<Value, MinijinjaError> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// get_view_options
    fn get_view_options(
        &self,
        _state: &State,
        _config: dbt_schemas::schemas::project::ModelConfig,
        _node: &dbt_schemas::schemas::InternalDbtNodeWrapper,
    ) -> Result<Value, MinijinjaError> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// get_bq_table
    fn get_bq_table(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError>;

    /// describe_relation
    fn describe_relation(
        &self,
        _state: &State,
        _relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError>;

    /// grant_access_to
    fn grant_access_to(
        &self,
        state: &State,
        entity: Arc<dyn BaseRelation>,
        entity_type: &str,
        role: Option<&str>,
        database: &str,
        schema: &str,
    ) -> Result<Value, MinijinjaError>;

    /// get_dataset_location
    fn get_dataset_location(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError>;

    /// compare_dbr_version
    fn compare_dbr_version(&self, _state: &State, _args: &[Value])
    -> Result<Value, MinijinjaError>;

    /// compute_external_path
    fn compute_external_path(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError>;

    /// Add UniForm Iceberg table properties.
    ///
    /// https://github.com/databricks/dbt-databricks/blob/bfcb5c7c7714e97e67023119f674d2938b04acb0/dbt/adapters/databricks/impl.py#L280
    ///
    /// ```python
    /// def update_tblproperties_for_uniform_iceberg(
    ///     self, config: BaseConfig, tblproperties: Optional[dict[str, str]] = None
    /// )  -> dict[str, str]
    /// ```
    fn update_tblproperties_for_uniform_iceberg(
        &self,
        _state: &State,
        _config: dbt_schemas::schemas::project::ModelConfig,
        _node: &dbt_schemas::schemas::InternalDbtNodeWrapper,
        _tblproperties: Option<Value>,
    ) -> Result<Value, MinijinjaError>;

    /// Is table UniForm Iceberg
    ///
    /// https://github.com/databricks/dbt-databricks/blob/bfcb5c7c7714e97e67023119f674d2938b04acb0/dbt/adapters/databricks/impl.py#L256C6-L256C7
    ///
    /// ```python
    /// def is_uniform(self, config: BaseConfig) -> bool:
    /// ```
    fn is_uniform(
        &self,
        _state: &State,
        _config: dbt_schemas::schemas::project::ModelConfig,
        _node: &dbt_schemas::schemas::InternalDbtNodeWrapper,
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
        _relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError>;

    /// get_persist_doc_columns
    fn get_persist_doc_columns(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError>;

    fn get_column_tags_from_model(
        &self,
        _state: &State,
        _node: &dyn InternalDbtNodeAttributes,
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
        _relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError>;

    /// parse_index
    fn parse_index(&self, _state: &State, _raw_index: &Value) -> Result<Value, MinijinjaError>;

    /// redact_credentials
    fn redact_credentials(&self, _state: &State, _sql: &str) -> Result<Value, MinijinjaError>;

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

    /// Used internally to hydrate the relation cache with the given schema -> relation map
    ///
    /// This operation should be additive and not reset the cache.
    fn update_relation_cache(
        &self,
        _schema_to_relations_map: BTreeMap<CatalogAndSchema, Vec<Arc<dyn BaseRelation>>>,
    ) -> FsResult<()> {
        Ok(())
    }

    /// Used internally to identify if a schema is already cached
    fn is_already_fully_cached(&self, _schema: &CatalogAndSchema) -> bool {
        false
    }

    /// Used internally to identify if a relation is already cached
    fn is_cached(&self, _relation: &Arc<dyn BaseRelation>) -> bool {
        false
    }
}

impl fmt::Display for dyn BaseAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}({})",
            if self.is_parse() {
                "ParseAdapter"
            } else if self.as_replay().is_some() {
                "DbtReplayAdapter"
            } else {
                "Adapter"
            },
            self.adapter_type()
        )
    }
}

/// A factory for adapters, relations and columns.
///
/// It can create adapters wrapped in a boxed `dyn BaseAdapter`
/// objects. Similarly, it can create boxed `dyn BaseRelation`
/// and `Column` objects.
pub trait AdapterFactory: Send + Sync {
    #[allow(clippy::too_many_arguments)]
    fn create_adapter(
        &self,
        adapter_type: AdapterType,
        config: dbt_serde_yaml::Mapping,
        replay_mode: Option<ReplayMode>,
        flags: BTreeMap<String, Value>,
        schema_cache: Option<Arc<dyn SchemaStoreTrait>>,
        query_cache: Option<Arc<dyn QueryCache>>,
        quoting: ResolvedQuoting,
        query_comment: Option<QueryComment>,
        token: CancellationToken,
    ) -> FsResult<Arc<dyn BaseAdapter>>;

    /// Create a relation from a InternalDbtNode
    fn create_relation_from_node(
        &self,
        node: &dyn InternalDbtNodeAttributes,
        adapter_type: AdapterType,
    ) -> Result<Arc<dyn BaseRelation>, minijinja::Error>;

    /// Return a new instance of the factory with a different relation cache.
    fn with_relation_cache(&self, relation_cache: Arc<RelationCache>) -> Arc<dyn AdapterFactory>;
}

/// Check if the adapter type is supported
///
/// XXX: the definition of "supported" is lost here
pub fn is_supported_dialect(adapter_type: AdapterType) -> bool {
    matches!(
        adapter_type,
        AdapterType::Snowflake
            | AdapterType::Bigquery
            | AdapterType::Redshift
            | AdapterType::Databricks
    )
}
