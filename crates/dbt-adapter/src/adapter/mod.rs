use crate::cache::RelationCache;
use crate::cast_util::downcast_value_to_dyn_base_relation;
use crate::catalog_relation::CatalogRelation;
use crate::engine::XdbcEngine;
use crate::engine::query_comment::QueryCommentConfig;
use crate::errors::into_fs_error;
use crate::metadata::*;
use crate::parse::adapter::ParseAdapterState;
use crate::query_ctx::{node_id_from_state, query_ctx_from_state};
use crate::relation::databricks::DEFAULT_DATABRICKS_DATABASE;
use crate::relation::factory::create_static_relation;
use crate::relation::{Relation, RelationObject};
use crate::render_constraint::render_model_constraint;
use crate::snapshots::SnapshotStrategy;
use crate::sql_types::TypeOps;
use crate::stmt_splitter::DefaultStmtSplitter;
use crate::time_machine::TimeMachine;
use crate::value::*;
use crate::{AdapterResponse, AdapterResult};

use dbt_adapter_core::AdapterType;
use dbt_agate::AgateTable;
use dbt_auth::{AdapterConfig, Auth, auth_for_backend};
use dbt_common::behavior_flags::Behavior;
use dbt_common::cancellation::{CancellationToken, never_cancels};
use dbt_common::{AdapterError, AdapterErrorKind, FsResult};
use dbt_schemas::schemas::InternalDbtNodeWrapper;
use dbt_schemas::schemas::common::{ClusterConfig, DbtQuoting, PartitionConfig};
use dbt_schemas::schemas::dbt_catalogs::DbtCatalogs;
use dbt_schemas::schemas::dbt_column::DbtColumn;
use dbt_schemas::schemas::manifest::{BigqueryPartitionConfig, GrantAccessToTarget};
use dbt_schemas::schemas::project::ModelConfig;
use dbt_schemas::schemas::properties::ModelConstraint;
use dbt_schemas::schemas::relations::base::{BaseRelation, ComponentName, TableFormat};
use dbt_schemas::schemas::serde::{minijinja_value_to_typed_struct, yml_value_to_minijinja};
use dbt_xdbc::QueryCtx;
use indexmap::IndexMap;
use minijinja::arg_utils::ArgsIter;
use minijinja::constants::TARGET_UNIQUE_ID;
use minijinja::dispatch_object::DispatchObject;
use minijinja::listener::RenderingEventListener;
use minijinja::value::{Object, ValueKind};
use minijinja::{State, Value};
use serde::Deserialize;
use tracing;

use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;

pub mod adapter_factory;
pub mod adapter_impl;
pub use adapter_factory::*;
pub use adapter_impl::{AdapterImpl, quote_component, quote_ident};
#[cfg(test)]
mod tests;

// Thread-local counter to track adapter call depth.
//
// Used to avoid recording/replaying nested adapter calls (e.g. truncate_relation
// calling execute). Only the outermost call is recorded/replayed.
thread_local! {
    static ADAPTER_CALL_DEPTH: Cell<usize> = const { Cell::new(0) };
}

/// The inner adapter implementation inside a [Adapter].
#[derive(Clone)]
enum InnerAdapter {
    /// The state necessary to perform operation in a shallow way during the parsing phase.
    Parse(Box<ParseAdapterState>),
    /// The actual implementation for all phases except parsing.
    /// The relation cache is now stored in the engine, not here.
    Typed { adapter: Arc<AdapterImpl> },
}

use InnerAdapter::*;

/// Type bridge adapter
///
/// This adapter converts untyped method calls (those that use Value)
/// to typed method calls, which we expect most adapters to implement.
/// As inseperable part of this process, this adapter also checks
/// arguments of all methods, their numbers, and types.
///
/// This adapter also takes care of what method annotations would do
/// in the dbt Core Python implementation. Things like returning
/// simple values during the parsing phase.
///
/// # Connection Management
///
/// This adapter caches the database connection used by the thread in a
/// thread-local. This allows Jinja code to use the connection without
/// explicitly referring to database connections.
///
/// Use the [ConcreateAdapter::borrow_tlocal_connection] function, which returns
/// a guard that can be dereferenced into a mutable [Box<dyn Connection>]. When
/// the guard instance is destroyed, the connection returns to the thread-local
/// variable.
///
/// # Relation Cache
///
/// The relation cache is now managed by the engine. Access via `engine().relation_cache()`.
#[derive(Clone)]
pub struct Adapter {
    inner: InnerAdapter,
    /// Time-machine for cross-version snapshot testing (optional)
    time_machine: Option<TimeMachine>,
    /// Global CLI cancellation token
    cancellation_token: CancellationToken,
}

impl fmt::Debug for Adapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            Typed { adapter, .. } => adapter.fmt(f),
            Parse(parse_adapter_state) => parse_adapter_state.debug_fmt(f),
        }
    }
}

impl dbt_handles::AdapterHandle for Adapter {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl fmt::Display for Adapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match &self.inner {
            Typed { adapter, .. } => match adapter.inner_adapter() {
                adapter_impl::InnerAdapter::Impl(_, _) => "Adapter",
                adapter_impl::InnerAdapter::Replay(_, _) => "DbtReplayAdapter",
            },
            Parse(_) => "ParseAdapter",
        };
        write!(f, "{}({})", name, self.adapter_type())
    }
}

impl Adapter {
    pub fn new(
        adapter: Arc<AdapterImpl>,
        time_machine: Option<TimeMachine>,
        cancellation_token: CancellationToken,
    ) -> Self {
        let inner = Typed { adapter };
        Self {
            inner,
            time_machine,
            cancellation_token,
        }
    }

    /// Create an instance of [Adapter] that operates in parse phase mode.
    pub fn new_parse_phase_adapter(
        adapter_type: AdapterType,
        config: dbt_yaml::Mapping,
        package_quoting: DbtQuoting,
        type_ops: Arc<dyn TypeOps>,
        catalogs: Option<Arc<DbtCatalogs>>,
    ) -> Adapter {
        let state = Self::make_parse_adapter_state(
            adapter_type,
            config,
            package_quoting,
            type_ops,
            Arc::new(RelationCache::default()),
            catalogs,
        );
        Adapter {
            inner: Parse(state),
            time_machine: None,
            cancellation_token: never_cancels(),
        }
    }

    pub(crate) fn make_parse_adapter_state(
        adapter_type: AdapterType,
        config: dbt_yaml::Mapping,
        package_quoting: DbtQuoting,
        type_ops: Arc<dyn TypeOps>,
        relation_cache: Arc<RelationCache>,
        catalogs: Option<Arc<DbtCatalogs>>,
    ) -> Box<ParseAdapterState> {
        let backend = backend_of(adapter_type);

        let auth: Arc<dyn Auth> = auth_for_backend(backend).into();
        let adapter_config = AdapterConfig::new(config);
        let quoting = package_quoting
            .try_into()
            .expect("Failed to convert quoting to resolved quoting");
        let stmt_splitter = Arc::new(DefaultStmtSplitter {});
        // No cloud config needed — bridge adapter is used for internal operations, not user-facing queries.
        let query_comment = QueryCommentConfig::from_query_comment(None, adapter_type, false, None);

        let engine = XdbcEngine::new(
            adapter_type,
            auth,
            adapter_config,
            quoting,
            query_comment,
            type_ops,
            stmt_splitter,
            relation_cache,
            BTreeMap::new(),
            None,
        );

        Box::new(ParseAdapterState::new(
            adapter_type,
            Arc::new(engine),
            catalogs,
        ))
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    /// Get a reference to the time machine, if enabled.
    pub fn time_machine(&self) -> Option<&TimeMachine> {
        self.time_machine.as_ref()
    }

    pub fn parse_adapter_state(&self) -> Option<&ParseAdapterState> {
        match &self.inner {
            Typed { .. } => None,
            Parse(state) => Some(state),
        }
    }

    /// Commit
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L000
    ///
    /// ```python
    /// def commit(self) -> None
    /// ```
    pub fn commit(&self) -> Result<Value, minijinja::Error> {
        Ok(Value::from(true))
    }

    /// Execute a statement, expect no results.
    pub fn exec_stmt(
        &self,
        state: &State,
        sql: &str,
        auto_begin: bool,
    ) -> AdapterResult<AdapterResponse> {
        let (response, _) = self.execute(
            state, None, // query_ctx
            sql, auto_begin, false, // fetch
            None,  // limit
            None,  // options
        )?;
        Ok(response)
    }

    /// Execute a query and get results in an [AgateTable].
    pub fn exec_query(
        &self,
        state: &State,
        sql: &str,
        limit: Option<i64>,
    ) -> AdapterResult<(AdapterResponse, AgateTable)> {
        self.execute(state, None, sql, false, true, limit, None)
    }
}

impl Adapter {
    #[inline]
    pub fn adapter_type(&self) -> AdapterType {
        match &self.inner {
            Typed { adapter, .. } => adapter.adapter_type(),
            Parse(state) => state.adapter_type,
        }
    }

    pub fn engine(&self) -> &Arc<dyn crate::AdapterEngine> {
        match &self.inner {
            Typed { adapter, .. } => adapter.engine(),
            Parse(state) => &state.engine,
        }
    }

    #[inline]
    pub fn is_parse(&self) -> bool {
        matches!(&self.inner, Parse(_))
    }

    pub fn as_replay(&self) -> Option<&dyn adapter_impl::Replayer> {
        match &self.inner {
            Typed { adapter, .. } => match adapter.inner_adapter() {
                adapter_impl::InnerAdapter::Replay(_, replay) => Some(replay),
                adapter_impl::InnerAdapter::Impl(..) => None,
            },
            Parse(_) => None,
        }
    }

    /// Execute a SQL query without requiring a Jinja [State].
    ///
    /// Used for lightweight operations like `dbt debug` connection tests
    /// where no Jinja environment is available.
    pub fn execute_without_state(
        &self,
        ctx: Option<&QueryCtx>,
        sql: &str,
        fetch: bool,
    ) -> AdapterResult<(AdapterResponse, AgateTable)> {
        match &self.inner {
            Typed { adapter, .. } => {
                let mut conn = adapter.borrow_tlocal_connection(None, None)?;
                adapter.execute(
                    None,
                    conn.as_mut(),
                    ctx,
                    sql,
                    false,
                    fetch,
                    None,
                    None,
                    self.cancellation_token.clone(),
                )
            }
            Parse(_) => Ok((AdapterResponse::default(), AgateTable::default())),
        }
    }

    /// Build an instance of the metadata adapter if supported.
    pub fn metadata_adapter(&self) -> Option<Box<dyn MetadataAdapter>> {
        match &self.inner {
            Typed { adapter, .. } => adapter.metadata_adapter(),
            Parse(_) => None, // TODO: implement metadata_adapter() for ParseAdapter
        }
    }

    pub async fn hydrate_relation_cache(&self, db_schemas: &[CatalogAndSchema]) -> FsResult<()> {
        let collected_relations = if let Some(metadata_adapter) = self.metadata_adapter() {
            metadata_adapter
                .list_relations_in_parallel(db_schemas, self.cancellation_token())
                .await
                .map_err(into_fs_error)
                .map(|r| {
                    r.into_iter()
                        .filter_map(|(k, v)| {
                            if let Ok(relations) = v {
                                Some((k, relations))
                            } else {
                                // XXX: Warnings are not shown right now since this is purely for performance
                                None
                            }
                        })
                        .collect::<BTreeMap<CatalogAndSchema, Vec<Arc<dyn BaseRelation>>>>()
                })?
        } else {
            // No metadata adapter available
            Default::default()
        };

        self.engine()
            .relation_cache()
            .insert_many(collected_relations.into_iter());
        Ok(())
    }

    /// This adapter as a Value
    pub fn as_value(&self) -> Value {
        Value::from_object(self.clone())
    }

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
    #[tracing::instrument(skip_all, level = "trace")]
    pub fn cache_added(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => adapter.cache_added(state, relation),
            // TODO(jason): We should probably capture any manual user engagement with the cache
            // and use this knowledge for our cache hydration
            Parse(_) => Ok(none_value()),
        }
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
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn cache_dropped(&self, state: &State, args: &[Value]) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("cache_dropped", &["relation"], args);
                let relation_val = iter.next_arg::<&Value>()?;
                let relation = downcast_value_to_dyn_base_relation(relation_val)?;
                iter.finish()?;

                adapter.cache_dropped(state, &relation)
            }
            Parse(_) => Ok(none_value()),
        }
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
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn cache_renamed(&self, state: &State, args: &[Value]) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("cache_renamed", &["from_relation", "to_relation"], args);
                let from_relation_val = iter.next_arg::<&Value>()?;
                let from_relation = downcast_value_to_dyn_base_relation(from_relation_val)?;
                let to_relation_val = iter.next_arg::<&Value>()?;
                let to_relation = downcast_value_to_dyn_base_relation(to_relation_val)?;
                iter.finish()?;

                adapter.cache_renamed(state, &from_relation, &to_relation)
            }
            Parse(_) => Ok(none_value()),
        }
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
    #[tracing::instrument(skip_all, level = "trace")]
    pub fn standardize_grants_dict(
        &self,
        _state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("standardize_grants_dict", &["grants_table"], args);
                let grants_table = iter
                    .next_arg::<&Value>()?
                    .downcast_object::<AgateTable>()
                    .ok_or_else(|| {
                        minijinja::Error::new(
                            minijinja::ErrorKind::InvalidOperation,
                            "grants_table must be an AgateTable",
                        )
                    })?;

                Ok(Value::from_serialize(
                    &adapter.standardize_grants_dict(grants_table)?,
                ))
            }
            // This method is typically called after show grants SQL + run_query.
            // During parse phase, run_query returns Undefined since queries don't execute,
            // so we don't have an actual AgateTable. Return an empty grants dict to avoid
            // downcast errors on Undefined values.
            Parse(_) => Ok(Value::from(BTreeMap::<Value, Vec<Value>>::new())),
        }
    }

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
    #[tracing::instrument(skip_all, level = "trace")]
    pub fn quote(&self, _state: &State, args: &[Value]) -> Result<Value, minijinja::Error> {
        let iter = ArgsIter::new("quote", &["identifier"], args);
        let identifier = iter.next_arg::<&str>()?;
        iter.finish()?;

        let quoted = quote_ident(self.adapter_type(), identifier);
        Ok(Value::from(quoted))
    }

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
    #[tracing::instrument(skip_all, level = "trace")]
    pub fn quote_as_configured(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("quote_as_configured", &["identifier", "quote_key"], args);
                let identifier = iter.next_arg::<&str>()?;
                let quote_key = iter.next_arg::<&str>()?;
                iter.finish()?;

                let quote_key = quote_key.parse::<ComponentName>().map_err(|_| {
                    minijinja::Error::new(
                        minijinja::ErrorKind::InvalidArgument,
                        "quote_key must be one of: database, schema, identifier",
                    )
                })?;

                let result = adapter.quote_as_configured(state, identifier, &quote_key)?;

                Ok(Value::from(result))
            }
            Parse(_) => Ok(empty_string_value()),
        }
    }

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
    #[tracing::instrument(skip_all, level = "trace")]
    pub fn quote_seed_column(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("quote_seed_column", &["column", "quote_config"], args);
                let column = iter.next_arg::<&str>()?;
                let quote_config = iter.next_kwarg::<Option<bool>>("quote_config")?;
                iter.finish()?;

                let result = adapter.quote_seed_column(state, column, quote_config)?;
                Ok(Value::from(result))
            }
            Parse(_) => Ok(empty_string_value()),
        }
    }

    /// Render the equality comparison SQL for two expressions.
    ///
    /// When `enable_truthy_nulls_equals_macro` is enabled the output uses
    /// the most idiomatic null-safe form for each adapter family
    /// When the flag is off (or in parse mode) a plain `(a = b)` is returned.
    pub fn render_equals(
        &self,
        _state: &State,
        expr1: &str,
        expr2: &str,
    ) -> Result<Value, minijinja::Error> {
        let flag_enabled = match &self.inner {
            Typed { adapter, .. } => adapter
                .behavior_object()
                .get_value(&Value::from("enable_truthy_nulls_equals_macro"))
                .is_some_and(|flag| flag.is_true()),
            Parse(_) => false,
        };
        let sql = if !flag_enabled {
            format!("({expr1} = {expr2})")
        } else {
            match self.adapter_type() {
                AdapterType::Snowflake
                | AdapterType::Bigquery
                | AdapterType::Postgres
                | AdapterType::Redshift
                | AdapterType::Spark
                | AdapterType::Databricks
                | AdapterType::DuckDB => {
                    format!("({expr1} IS NOT DISTINCT FROM {expr2})")
                }
                _ => format!(
                    "case when (({expr1} = {expr2}) or ({expr1} is null and {expr2} is null)) \
                     then 0 else 1 end = 0"
                ),
            }
        };
        Ok(Value::from(sql))
    }

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
    #[tracing::instrument(skip_all, level = "trace")]
    pub fn convert_type(&self, state: &State, args: &[Value]) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("convert_type", &["agate_table", "col_idx"], args);
                let table = iter
                    .next_arg::<&Value>()?
                    .downcast_object::<AgateTable>()
                    .ok_or_else(|| {
                        minijinja::Error::new(
                            minijinja::ErrorKind::InvalidOperation,
                            "agate_table must be an AgateTable",
                        )
                    })?;
                let col_idx = iter.next_arg::<i64>()?;
                iter.finish()?;

                let result = adapter.convert_type(state, table, col_idx)?;
                Ok(Value::from(result))
            }
            Parse(_) => Ok(empty_string_value()),
        }
    }

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
    #[tracing::instrument(skip_all, level = "trace")]
    pub fn render_raw_model_constraints(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter =
                    ArgsIter::new("render_raw_model_constraints", &["raw_constraints"], args);
                let raw_constraints_val = iter.next_arg::<&Value>()?;
                let raw_constraints = minijinja_value_to_typed_struct::<Vec<ModelConstraint>>(
                    raw_constraints_val.clone(),
                )
                .map_err(|e| {
                    minijinja::Error::new(
                        minijinja::ErrorKind::SerdeDeserializeError,
                        e.to_string(),
                    )
                })?;
                iter.finish()?;

                if let Some(replay_adapter) = adapter.as_replay() {
                    return replay_adapter
                        .replay_render_raw_model_constraints(state, &raw_constraints);
                }
                let mut result = vec![];
                for constraint in &raw_constraints {
                    let rendered =
                        render_model_constraint(adapter.adapter_type(), constraint.clone());
                    if let Some(rendered) = rendered {
                        result.push(rendered)
                    }
                }
                Ok(Value::from(result))
            }
            Parse(_) => Ok(empty_vec_value()),
        }
    }

    /// Render raw columns constraints.
    ///
    /// Used by BigQuery adapter to render column constraints.
    #[tracing::instrument(skip_all, level = "trace")]
    pub fn render_raw_columns_constraints(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("render_raw_columns_constraints", &["raw_columns"], args);
                let raw_columns = iter.next_arg::<&Value>()?;
                iter.finish()?;

                let columns = minijinja_value_to_typed_struct::<IndexMap<String, DbtColumn>>(
                    raw_columns.clone(),
                )
                .map_err(|e| {
                    minijinja::Error::new(
                        minijinja::ErrorKind::SerdeDeserializeError,
                        e.to_string(),
                    )
                })?;

                if let Some(replay_adapter) = adapter.as_replay() {
                    return Ok(Value::from(
                        replay_adapter.replay_render_raw_columns_constraints(state, columns)?,
                    ));
                }
                let result = adapter.render_raw_columns_constraints(columns)?;

                Ok(Value::from(result))
            }
            Parse(_) => Ok(empty_vec_value()),
        }
    }

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
    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(skip(self, state, ctx), level = "trace")]
    pub fn execute(
        &self,
        state: &State,
        ctx: Option<&QueryCtx>,
        sql: &str,
        auto_begin: bool,
        fetch: bool,
        limit: Option<i64>,
        options: Option<HashMap<String, String>>,
    ) -> AdapterResult<(AdapterResponse, AgateTable)> {
        match &self.inner {
            Typed { adapter, .. } => {
                let mut conn =
                    adapter.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let (response, table) = adapter.execute(
                    Some(state),
                    conn.as_mut(),
                    ctx,
                    sql,
                    auto_begin,
                    fetch,
                    limit,
                    options,
                    self.cancellation_token.clone(),
                )?;
                Ok((response, table))
            }
            Parse(parse_state) => {
                let response = AdapterResponse::default();
                let table = AgateTable::default();

                if state.is_execute() {
                    if let Some(unique_id) = state.lookup(TARGET_UNIQUE_ID, &[]) {
                        parse_state.unsafe_nodes.insert(
                            unique_id
                                .as_str()
                                .expect("unique_id must be a string")
                                .to_string(),
                        );
                    }
                    parse_state.execute_sqls.insert(sql.to_string());
                }

                Ok((response, table))
            }
        }
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
    #[tracing::instrument(skip(self, state, args), level = "trace")]
    pub fn add_query(&self, state: &State, args: &[Value]) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("add_query", &["sql"], args);
                let sql = iter.next_arg::<&str>()?;
                let auto_begin = iter
                    .next_kwarg::<Option<bool>>("auto_begin")?
                    .unwrap_or(true);
                let bindings = iter.next_kwarg::<Option<&Value>>("bindings")?;
                let abridge_sql_log = iter
                    .next_kwarg::<Option<bool>>("abridge_sql_log")?
                    .unwrap_or(false);
                let _retryable_exceptions =
                    iter.next_kwarg::<Option<&Value>>("retryable_exceptions")?;
                let _retry_limit = iter.next_kwarg::<Option<i64>>("retry_limit")?.unwrap_or(1);
                // TODO(harry): add iter.finish() and fix the tests

                let mut conn =
                    adapter.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                adapter.add_query(
                    state,
                    conn.as_mut(),
                    sql,
                    auto_begin,
                    bindings,
                    abridge_sql_log,
                    self.cancellation_token.clone(),
                )?;
                Ok(Value::from(()))
            }
            Parse(_) => Ok(Value::from(())),
        }
    }

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
    pub fn submit_python_job(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("submit_python_job", &["model", "compiled_code"], args);
                let model = iter.next_arg::<&Value>()?;
                let compiled_code = iter.next_arg::<&str>()?;
                iter.finish()?;

                let mut conn =
                    adapter.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let ctx = query_ctx_from_state(state)?.with_desc("submit_python_job adapter call");

                let response = adapter.submit_python_job(
                    &ctx,
                    conn.as_mut(),
                    state,
                    model,
                    compiled_code,
                    self.cancellation_token.clone(),
                )?;
                Ok(Value::from_object(response))
            }
            Parse(_) => {
                // Python models cannot be executed during parse phase
                Err(AdapterError::new(
                    AdapterErrorKind::NotSupported,
                    "submit_python_job can only be called in materialization macros",
                )
                .into())
            }
        }
    }

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
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn drop_relation(&self, state: &State, args: &[Value]) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("drop_relation", &["relation"], args);
                let relation_val = iter.next_arg::<&Value>()?;
                let relation = downcast_value_to_dyn_base_relation(relation_val)?;
                iter.finish()?;

                adapter
                    .engine()
                    .relation_cache()
                    .evict_relation(relation.as_ref() as &dyn BaseRelation);
                Ok(adapter.drop_relation(state, &relation)?)
            }
            Parse(_) => Ok(none_value()),
        }
    }

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
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn truncate_relation(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("truncate_relation", &["relation"], args);
                let relation_val = iter.next_arg::<&Value>()?;
                let relation = downcast_value_to_dyn_base_relation(relation_val)?;
                iter.finish()?;

                Ok(adapter.truncate_relation(state, &relation)?)
            }
            Parse(_) => Ok(none_value()),
        }
    }

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
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn rename_relation(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter =
                    ArgsIter::new("rename_relation", &["from_relation", "to_relation"], args);
                let from_relation_val = iter.next_arg::<&Value>()?;
                let from_relation = downcast_value_to_dyn_base_relation(from_relation_val)?;
                let to_relation_val = iter.next_arg::<&Value>()?;
                let to_relation = downcast_value_to_dyn_base_relation(to_relation_val)?;
                iter.finish()?;

                // Update cache (call the typed AdapterImpl directly since we already have refs)
                adapter.cache_renamed(state, &from_relation, &to_relation)?;

                adapter.rename_relation(state, &from_relation, &to_relation)?;
                Ok(Value::from(()))
            }
            Parse(_) => Ok(none_value()),
        }
    }

    /// Expand the to_relation table's column types to match the schema of from_relation.
    /// https://docs.getdbt.com/reference/dbt-jinja-functions/adapter#expand_target_column_types
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn expand_target_column_types(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new(
                    "expand_target_column_types",
                    &["from_relation", "to_relation"],
                    args,
                );
                let from_relation_val = iter.next_arg::<&Value>()?;
                let from_relation = downcast_value_to_dyn_base_relation(from_relation_val)?;
                let to_relation_val = iter.next_arg::<&Value>()?;
                let to_relation = downcast_value_to_dyn_base_relation(to_relation_val)?;
                iter.finish()?;

                let result =
                    adapter.expand_target_column_types(state, &from_relation, &to_relation)?;
                Ok(result)
            }
            Parse(_) => Ok(none_value()),
        }
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/sql/impl.py#L212-L213
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn list_schemas(&self, state: &State, database: &str) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let result = adapter.list_schemas(state, database)?;

                Ok(Value::from_iter(result))
            }
            Parse(_) => Ok(empty_vec_value()),
        }
    }

    /// Typed variant of [`Adapter::list_schemas`] that returns the underlying
    /// [`AdapterResult`] so callers can inspect the [`AdapterError`] kind
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn list_schemas_typed(&self, state: &State, database: &str) -> AdapterResult<Vec<String>> {
        match &self.inner {
            Typed { adapter, .. } => adapter.list_schemas(state, database),
            Parse(_) => Ok(Vec::new()),
        }
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/sql/impl.py#L161
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn create_schema(&self, state: &State, args: &[Value]) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("create_schema", &["relation"], args);
                let relation_val = iter.next_arg::<&Value>()?;
                let relation = downcast_value_to_dyn_base_relation(relation_val)?;
                iter.finish()?;

                adapter.create_schema(state, &relation)
            }
            Parse(_) => Ok(none_value()),
        }
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/sql/impl.py#L172-L173
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn drop_schema(&self, state: &State, args: &[Value]) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("drop_schema", &["relation"], args);
                let relation_val = iter.next_arg::<&Value>()?;
                let relation = downcast_value_to_dyn_base_relation(relation_val)?;
                iter.finish()?;

                adapter.drop_schema(state, &relation)
            }
            Parse(_) => Ok(none_value()),
        }
    }

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
    #[tracing::instrument(skip(self, state), level = "trace")]
    #[allow(clippy::used_underscore_binding)]
    pub fn valid_snapshot_target(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("valid_snapshot_target", &["relation"], args);
                let relation_val = iter.next_arg::<&Value>()?;
                let relation = downcast_value_to_dyn_base_relation(relation_val)?;
                iter.finish()?;

                adapter.valid_snapshot_target(state, &relation)
            }
            Parse(_) => Ok(none_value()),
        }
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
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn get_incremental_strategy_macro(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new(
                    "get_incremental_strategy_macro",
                    &["context", "strategy"],
                    args,
                );
                let _context = iter.next_arg::<Value>()?; // unused, for backward compat
                let strategy = iter.next_arg::<&str>()?;
                iter.finish()?;

                adapter.get_incremental_strategy_macro(state, strategy)
            }
            Parse(_) => Ok(none_value()),
        }
    }

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
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn assert_valid_snapshot_target_given_strategy(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new(
                    "assert_valid_snapshot_target_given_strategy",
                    &["relation", "column_names", "strategy"],
                    args,
                );
                let relation_val = iter.next_arg::<&Value>()?;
                let relation = downcast_value_to_dyn_base_relation(relation_val)?;
                let column_names_val = iter.next_arg::<&Value>()?;
                let column_names = if column_names_val.is_none() || column_names_val.is_undefined()
                {
                    None
                } else {
                    Some(
                        minijinja_value_to_typed_struct::<BTreeMap<String, String>>(
                            column_names_val.clone(),
                        )
                        .map_err(|e| {
                            minijinja::Error::new(
                                minijinja::ErrorKind::SerdeDeserializeError,
                                e.to_string(),
                            )
                        })?,
                    )
                };
                let strategy_val = iter.next_arg::<&Value>()?;
                let strategy =
                    minijinja_value_to_typed_struct::<SnapshotStrategy>(strategy_val.clone())
                        .map_err(|e| {
                            minijinja::Error::new(
                                minijinja::ErrorKind::SerdeDeserializeError,
                                e.to_string(),
                            )
                        })?;
                iter.finish()?;

                adapter.assert_valid_snapshot_target_given_strategy(
                    state,
                    &relation,
                    column_names,
                    Arc::new(strategy),
                )?;
                Ok(none_value())
            }
            Parse(_) => Ok(none_value()),
        }
    }

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
    #[tracing::instrument(skip(self, _state), level = "trace")]
    pub fn get_hard_deletes_behavior(
        &self,
        _state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("get_hard_deletes_behavior", &["config"], args);
                let config_val = iter.next_arg::<&Value>()?;
                iter.finish()?;

                let hard_deletes = config_val.get_item(&Value::from("hard_deletes")).ok();
                let invalidate_hard_deletes = config_val
                    .get_item(&Value::from("invalidate_hard_deletes"))
                    .ok();

                let mut config = BTreeMap::<String, Value>::new();
                if let Some(hard_deletes) = hard_deletes
                    && !hard_deletes.is_undefined()
                    && !hard_deletes.is_none()
                {
                    config.insert("hard_deletes".to_string(), hard_deletes);
                }
                if let Some(invalidate_hard_deletes) = invalidate_hard_deletes
                    && !invalidate_hard_deletes.is_undefined()
                    && !invalidate_hard_deletes.is_none()
                {
                    config.insert(
                        "invalidate_hard_deletes".to_string(),
                        invalidate_hard_deletes,
                    );
                }

                Ok(Value::from(adapter.get_hard_deletes_behavior(config)?))
            }
            // For parse adapter, always return "ignore" as default behavior
            Parse(_) => Ok(none_value()),
        }
    }

    /// Get relation.
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/5fba80c621c3f0f732dba71aa6cf9055792b6495/dbt-adapters/src/dbt/adapters/base/impl.py#L1014
    ///
    /// ```python
    /// def get_relation(
    ///     self,
    ///     database: str,
    ///     schema: str,
    ///     identifier: str,
    ///     needs_information: bool = False
    /// )  -> Optional[BaseRelation]
    /// ```
    ///
    /// When `needs_information` is false (default): returns cached relation only; no extra
    /// database call. When true: guarantees the relation has catalog metadata (Provider, Owner,
    /// Statistics, etc.), running DESCRIBE EXTENDED if needed (Databricks).
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn get_relation(
        &self,
        state: &State,
        database: &str,
        schema: &str,
        identifier: &str,
        needs_information: bool,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                // Skip cache in replay mode
                let is_replay = adapter.as_replay().is_some();

                let temp_relation = crate::relation::do_create_relation(
                    adapter.adapter_type(),
                    database.to_string(),
                    schema.to_string(),
                    Some(identifier.to_string()),
                    None,
                    adapter.quoting(),
                )?;

                let maybe_cache_result = if is_replay {
                    None
                } else {
                    // Cache hit
                    if let Some(cache_result) =
                        self.get_relation_value_from_cache(temp_relation.as_ref())
                    {
                        Some(cache_result)
                    } else {
                        // Cache miss: execute list_relations
                        // Skip when relation has neither catalog nor schema (e.g. temporary view)
                        // - list_relations cannot query without schema
                        // - CatalogAndSchema::from would panic
                        let resolved_catalog =
                            temp_relation.database_as_resolved_str().unwrap_or_default();
                        let resolved_schema =
                            temp_relation.schema_as_resolved_str().unwrap_or_default();
                        let has_schema =
                            !resolved_catalog.is_empty() || !resolved_schema.is_empty();

                        if has_schema {
                            let mut conn = adapter
                                .borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                            let db_schema = CatalogAndSchema::from(temp_relation.as_ref());
                            let query_ctx = query_ctx_from_state(state)?
                                .with_desc("get_relation > list_relations call");
                            let maybe_relations_list = adapter.list_relations(
                                &query_ctx,
                                conn.as_mut(),
                                &db_schema,
                                self.cancellation_token.clone(),
                            );

                            if let Ok(relations_list) = maybe_relations_list {
                                let to_insert = Vec::from([(db_schema, relations_list)]);
                                adapter
                                    .engine()
                                    .relation_cache()
                                    .insert_many(to_insert.into_iter());

                                self.get_relation_value_from_cache(temp_relation.as_ref())
                                    .or(Some(none_value()))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                };

                // Return early when cache is sufficient:
                // - Relation doesn't exist (contains_full_schema): return none_value
                // - Cache hit with relation: return cached value unless needs_information && !has_information
                if let Some(cache_result) = maybe_cache_result {
                    if let Some(cached_entry) = adapter
                        .engine()
                        .relation_cache()
                        .get_relation(temp_relation.as_ref())
                    {
                        let can_use_cache =
                            !needs_information || cached_entry.relation().has_information();
                        if can_use_cache {
                            return Ok(RelationObject::new(cached_entry.relation()).into_value());
                        }
                    } else {
                        return Ok(cache_result);
                    }
                }

                // Execute get_relation when: cache miss, list_relations failed, or needs_information && !has_information
                let mut conn =
                    adapter.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let query_ctx = query_ctx_from_state(state)?.with_desc("get_relation adapter call");
                let relation = adapter.get_relation(
                    state,
                    &query_ctx,
                    conn.as_mut(),
                    database,
                    schema,
                    identifier,
                    self.cancellation_token.clone(),
                )?;
                match relation {
                    Some(relation) => {
                        // cache found relation
                        adapter
                            .engine()
                            .relation_cache()
                            .insert_relation(Arc::clone(&relation), None);
                        Ok(RelationObject::new(relation).into_value())
                    }
                    None => Ok(none_value()),
                }
            }
            Parse(adapter_parse_state) => {
                // TODO(jason): record needs_information calls in parse phase for prefetc
                let adapter_type = adapter_parse_state.adapter_type;
                adapter_parse_state
                    .record_get_relation_call(state, database, schema, identifier)?;
                Ok(
                    RelationObject::new(Arc::new(Relation::new_parse_time(adapter_type)))
                        .into_value(),
                )
            }
        }
    }

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
    #[tracing::instrument(skip(self), level = "trace")]
    pub fn build_catalog_relation(&self, model: &Value) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let relation = adapter.build_catalog_relation(model)?;
                Ok(Value::from_object(relation))
            }
            Parse(parse_adapter_state) => {
                let relation = CatalogRelation::from_model_config_and_catalogs(
                    parse_adapter_state.adapter_type,
                    model,
                    parse_adapter_state.catalogs.clone(),
                )?;
                Ok(Value::from_object(relation))
            }
        }
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
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn get_missing_columns(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new(
                    "get_missing_columns",
                    &["from_relation", "to_relation"],
                    args,
                );
                let from_relation_val = iter.next_arg::<&Value>()?;
                let from_relation = downcast_value_to_dyn_base_relation(from_relation_val)?;
                let to_relation_val = iter.next_arg::<&Value>()?;
                let to_relation = downcast_value_to_dyn_base_relation(to_relation_val)?;
                iter.finish()?;

                let result = adapter.get_missing_columns(state, &from_relation, &to_relation)?;
                Ok(Value::from_object(result))
            }
            Parse(_) => Ok(empty_vec_value()),
        }
    }

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
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn get_columns_in_relation(
        &self,
        state: &State,
        relation: &dyn BaseRelation,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                // Replay Mode: Re-use recordings and compare with cache result
                if let Some(replay_adapter) = adapter.as_replay() {
                    let cached = adapter.get_columns_in_relation_via_cache(state, relation)?;
                    replay_adapter.replay_get_columns_in_relation(
                        state,
                        &relation.to_owned(),
                        cached,
                    )
                } else {
                    Ok(Value::from(
                        adapter.get_columns_in_relation(state, relation)?,
                    ))
                }
            }
            Parse(parse_adapter_state) => {
                parse_adapter_state.record_get_columns_in_relation_call(state, relation)?;
                Ok(empty_vec_value())
            }
        }
    }

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
    #[tracing::instrument(skip_all, level = "trace")]
    pub fn check_schema_exists(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("check_schema_exists", &["database", "schema"], args);
                let database = iter.next_arg::<&str>()?;
                let schema = iter.next_arg::<&str>()?;
                iter.finish()?;

                adapter.check_schema_exists(state, database, schema)
            }
            Parse(_) => Ok(Value::from(true)),
        }
    }

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
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn get_relations_by_pattern(
        &self,
        state: &State,
        schema_pattern: &str,
        table_pattern: &str,
        exclude: Option<&str>,
        database: Option<&str>,
        quote_table: Option<bool>,
        excluded_schemas: Option<Value>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => adapter.get_relations_by_pattern(
                state,
                schema_pattern,
                table_pattern,
                exclude,
                database,
                quote_table,
                excluded_schemas,
            ),
            Parse(parse_adapter_state) => parse_adapter_state.get_relations_by_pattern(
                state,
                schema_pattern,
                table_pattern,
                exclude,
                database,
                quote_table,
                excluded_schemas,
            ),
        }
    }

    /// Get column schema from query
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn get_column_schema_from_query(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("get_column_schema_from_query", &["sql"], args);
                let sql = iter.next_arg::<&str>()?;
                iter.finish()?;

                let ctx = query_ctx_from_state(state)?
                    .with_desc("get_column_schema_from_query adapter call");
                let mut conn =
                    adapter.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let result = adapter.get_column_schema_from_query(
                    state,
                    conn.as_mut(),
                    &ctx,
                    sql,
                    self.cancellation_token.clone(),
                )?;
                Ok(Value::from(result))
            }
            Parse(_) => Ok(empty_map_value()),
        }
    }

    /// Get columns in select sql
    ///
    /// reference: https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L443-L444
    /// Shares the same input and output as get_column_schema_from_query.
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn get_columns_in_select_sql(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("get_columns_in_select_sql", &["sql"], args);
                let sql = iter.next_arg::<&str>()?;
                iter.finish()?;

                let ctx = query_ctx_from_state(state)?
                    .with_desc("get_column_schema_from_query adapter call");
                let mut conn =
                    adapter.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let result = adapter.get_column_schema_from_query(
                    state,
                    conn.as_mut(),
                    &ctx,
                    sql,
                    self.cancellation_token.clone(),
                )?;
                Ok(Value::from(result))
            }
            Parse(_) => Ok(empty_map_value()),
        }
    }

    /// Verify database.
    #[tracing::instrument(skip(self, _state), level = "trace")]
    pub fn verify_database(
        &self,
        _state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("verify_database", &["database"], args);
                let database = iter.next_arg::<String>()?;
                iter.finish()?;

                let result = adapter.verify_database(database);
                Ok(result?)
            }
            Parse(_) => Ok(Value::from(false)),
        }
    }

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
    pub fn dispatch(&self, state: &State, args: &[Value]) -> Result<Value, minijinja::Error> {
        let iter = ArgsIter::new("dispatch", &["macro_name"], args);
        let macro_name = iter.next_arg::<&str>()?;
        let macro_namespace = iter.next_kwarg::<Option<&str>>("macro_namespace")?;
        iter.finish()?;

        if macro_name.contains('.') {
            let parts: Vec<&str> = macro_name.split('.').collect();
            return Err(minijinja::Error::new(
                minijinja::ErrorKind::InvalidOperation,
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

    /// Nest column data types for BigQuery STRUCT/ARRAY types.
    ///
    /// Converts flat column definitions into nested structures.
    /// Only available with BigQuery adapter.
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn nest_column_data_types(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("nest_column_data_types", &["columns"], args);
                let columns = iter.next_arg::<&Value>()?;
                iter.finish()?;

                adapter.nest_column_data_types(state, columns)
            }
            Parse(_) => Ok(empty_map_value()),
        }
    }

    /// Re-project a column's STRUCT fields in the declared YAML order.
    ///
    /// Only available with BigQuery adapter.
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn get_struct_select_expression(
        &self,
        state: &State,
        col_name: &str,
        data_type: &str,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                adapter.get_struct_select_expression(state, col_name, data_type)
            }
            Parse(_) => Ok(Value::from(col_name.to_string())),
        }
    }

    #[tracing::instrument(skip(self), level = "trace")]
    #[allow(clippy::used_underscore_binding)]
    pub fn get_bq_table(&self, state: &State, args: &[Value]) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("get_bq_table", &["relation"], args);
                let relation_val = iter.next_arg::<&Value>()?;
                let relation = downcast_value_to_dyn_base_relation(relation_val)?;
                iter.finish()?;

                adapter.get_bq_table(state, &relation)
            }
            Parse(_) => Ok(none_value()),
        }
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
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn is_replaceable(&self, state: &State, args: &[Value]) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("is_replaceable", &["relation"], args);
                let relation_val = iter.next_arg::<&Value>()?;
                let relation = if relation_val.is_none() {
                    None
                } else {
                    Some(downcast_value_to_dyn_base_relation(relation_val)?)
                };
                let partition_by_val = iter.next_kwarg::<Option<&Value>>("partition_by")?;
                let cluster_by_val = iter.next_kwarg::<Option<&Value>>("cluster_by")?;
                iter.finish()?;

                let partition_by = if let Some(pb) = partition_by_val {
                    // Match original behavior: check is_none() only, then deserialize
                    if pb.is_none() {
                        None
                    } else {
                        Some(
                            minijinja_value_to_typed_struct::<BigqueryPartitionConfig>(pb.clone())
                                .map_err(|e| {
                                    minijinja::Error::new(
                                        minijinja::ErrorKind::SerdeDeserializeError,
                                        e.to_string(),
                                    )
                                })?,
                        )
                    }
                } else {
                    None
                };

                let cluster_by = if let Some(cb) = cluster_by_val {
                    if cb.is_none() {
                        None
                    } else {
                        Some(
                            minijinja_value_to_typed_struct::<ClusterConfig>(cb.clone()).map_err(
                                |e| {
                                    minijinja::Error::new(
                                        minijinja::ErrorKind::SerdeDeserializeError,
                                        e.to_string(),
                                    )
                                },
                            )?,
                        )
                    }
                } else {
                    None
                };

                let relation = match relation.as_ref() {
                    None => {
                        // Replay compatibility: Mantle recordings may include an is_replaceable call even
                        // when relation=None (dbt-bigquery passes None when get_relation returns None).
                        //
                        // Our typed adapter short-circuits relation=None to true, but in replay mode
                        // we must optionally consume a recorded is_replaceable to keep the stream aligned.
                        if adapter.adapter_type() == AdapterType::Bigquery {
                            if let Some(replay_adapter) = adapter.as_replay() {
                                if replay_adapter
                                    .replay_peek_is_replaceable_next(state)
                                    .map_err(|e| {
                                        minijinja::Error::new(
                                            minijinja::ErrorKind::UndefinedError,
                                            e.to_string(),
                                        )
                                    })?
                                {
                                    let val = replay_adapter.replay_is_replaceable(state).map_err(
                                        |e| {
                                            minijinja::Error::new(
                                                minijinja::ErrorKind::UndefinedError,
                                                e.to_string(),
                                            )
                                        },
                                    )?;
                                    return Ok(Value::from(val));
                                }
                            }
                        }
                        return Ok(Value::from(true));
                    }
                    Some(r) => r,
                };

                let mut conn =
                    adapter.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let result = adapter.is_replaceable(
                    conn.as_mut(),
                    relation,
                    partition_by,
                    cluster_by,
                    Some(state),
                )?;
                Ok(Value::from(result))
            }
            // In parse mode, return stub value early without validation
            Parse(_) => Ok(Value::from(false)),
        }
    }

    pub fn upload_file(&self, state: &State, args: &[Value]) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => adapter.upload_file(state, args),
            Parse(_) => Ok(none_value()),
        }
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L579-L586
    ///
    /// # Panics
    /// This method will panic if called on a non-BigQuery adapter
    #[tracing::instrument(skip_all, level = "trace")]
    pub fn parse_partition_by(
        &self,
        _state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("parse_partition_by", &["raw_partition_by"], args);
                let raw_partition_by = iter.next_arg::<&Value>()?;
                iter.finish()?;

                let result = adapter.parse_partition_by(raw_partition_by.clone())?;
                Ok(result)
            }
            // In parse mode, return stub value early without validation
            Parse(_) => Ok(none_value()),
        }
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
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn get_table_options(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("get_table_options", &["config", "node"], args);
                let config_val = iter.next_arg::<&Value>()?;
                let node_val = iter.next_arg::<&Value>()?;
                let temporary = iter
                    .next_kwarg::<Option<bool>>("temporary")?
                    .unwrap_or_default();
                iter.finish()?;

                let config = minijinja_value_to_typed_struct::<ModelConfig>(config_val.clone())
                    .map_err(|e| {
                        minijinja::Error::new(
                            minijinja::ErrorKind::SerdeDeserializeError,
                            format!("get_table_options: Failed to deserialize config: {e}"),
                        )
                    })?;
                let node = minijinja_value_to_typed_struct::<InternalDbtNodeWrapper>(
                    node_val.clone(),
                )
                .map_err(|e| {
                    minijinja::Error::new(
                        minijinja::ErrorKind::SerdeDeserializeError,
                        format!(
                            "get_table_options: Failed to deserialize InternalDbtNodeWrapper: {e}"
                        ),
                    )
                })?;

                let options = adapter.get_table_options(state, config, &node, temporary)?;
                Ok(Value::from_serialize(options))
            }
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn get_view_options(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("get_view_options", &["config", "node"], args);
                let config_val = iter.next_arg::<&Value>()?;
                let node_val = iter.next_arg::<&Value>()?;
                iter.finish()?;

                let config = minijinja_value_to_typed_struct::<ModelConfig>(config_val.clone())
                    .map_err(|e| {
                        minijinja::Error::new(
                            minijinja::ErrorKind::SerdeDeserializeError,
                            format!("get_view_options: Failed to deserialize config: {e}"),
                        )
                    })?;
                let node = minijinja_value_to_typed_struct::<InternalDbtNodeWrapper>(
                    node_val.clone(),
                )
                .map_err(|e| {
                    minijinja::Error::new(
                        minijinja::ErrorKind::SerdeDeserializeError,
                        format!(
                            "get_view_options: Failed to deserialize InternalDbtNodeWrapper: {e}"
                        ),
                    )
                })?;

                let inner_node = node.as_internal_node();
                let options = adapter.get_view_options(state, config, inner_node.common())?;
                Ok(Value::from_serialize(options))
            }
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn get_common_options(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("get_common_options", &["config", "node"], args);
                let config_val = iter.next_arg::<&Value>()?;
                let node_val = iter.next_arg::<&Value>()?;
                let temporary = iter
                    .next_kwarg::<Option<bool>>("temporary")?
                    .unwrap_or(false);
                iter.finish()?;

                let config = minijinja_value_to_typed_struct::<ModelConfig>(config_val.clone())
                    .map_err(|e| {
                        minijinja::Error::new(
                            minijinja::ErrorKind::SerdeDeserializeError,
                            format!("get_common_options: Failed to deserialize config: {e}"),
                        )
                    })?;
                let node = minijinja_value_to_typed_struct::<InternalDbtNodeWrapper>(
                    node_val.clone(),
                )
                .map_err(|e| {
                    minijinja::Error::new(
                        minijinja::ErrorKind::SerdeDeserializeError,
                        format!(
                            "get_common_options: Failed to deserialize InternalDbtNodeWrapper: {e}"
                        ),
                    )
                })?;

                let options = adapter.get_common_options(state, config, &node, temporary)?;
                Ok(options)
            }
            Parse(_) => Ok(none_value()),
        }
    }

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
    #[tracing::instrument(skip(self, _state), level = "trace")]
    pub fn add_time_ingestion_partition_column(
        &self,
        _state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new(
                    "add_time_ingestion_partition_column",
                    &["partition_by", "columns"],
                    args,
                );
                let partition_by = iter.next_arg::<&Value>()?;
                let columns = iter.next_arg::<&Value>()?;
                iter.finish()?;

                // Match original behavior: try to deserialize directly, let deserialization handle errors
                let partition_by =
                    minijinja_value_to_typed_struct::<PartitionConfig>(partition_by.clone())
                        .map_err(|e| {
                            minijinja::Error::new(
                                minijinja::ErrorKind::SerdeDeserializeError,
                                format!(
                                    "adapter.add_time_ingestion_partition_column failed on partition_by {partition_by:?}: {e}"
                                ),
                            )
                        })?;

                let partition_config = partition_by.into_bigquery().ok_or_else(|| {
                    minijinja::Error::new(
                        minijinja::ErrorKind::InvalidArgument,
                        "Expect a BigqueryPartitionConfigStruct",
                    )
                })?;

                let result = adapter
                    .add_time_ingestion_partition_column(columns.clone(), partition_config)?;
                Ok(result)
            }
            // In parse mode, return stub value early without validation
            Parse(_) => Ok(empty_vec_value()),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn grant_access_to(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new(
                    "grant_access_to",
                    &["entity", "entity_type", "role", "grant_target_dict"],
                    args,
                );
                let entity_val = iter.next_arg::<&Value>()?;
                let entity_type = iter.next_arg::<&str>()?;
                let role_val = iter.next_arg::<&Value>()?;
                let grant_target_dict = iter.next_arg::<&Value>()?;
                let grant_target = minijinja_value_to_typed_struct::<GrantAccessToTarget>(
                    grant_target_dict.clone(),
                )
                .map_err(|e| {
                    minijinja::Error::new(
                        minijinja::ErrorKind::SerdeDeserializeError,
                        e.to_string(),
                    )
                })?;
                iter.finish()?;

                let database = grant_target.project.as_deref().ok_or_else(|| {
                    minijinja::Error::new(
                        minijinja::ErrorKind::InvalidOperation,
                        "project in a GrantAccessToTarget cannot be empty",
                    )
                })?;
                let schema = grant_target.dataset.as_deref().ok_or_else(|| {
                    minijinja::Error::new(
                        minijinja::ErrorKind::InvalidOperation,
                        "dataset in a GrantAccessToTarget cannot be empty",
                    )
                })?;

                let role = if role_val.is_none() || role_val.is_undefined() {
                    None
                } else {
                    Some(role_val.as_str().ok_or_else(|| {
                        minijinja::Error::new(
                            minijinja::ErrorKind::InvalidOperation,
                            "role must be a string",
                        )
                    })?)
                };

                let entity = downcast_value_to_dyn_base_relation(entity_val)?;

                let mut conn =
                    adapter.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let result = adapter.grant_access_to(
                    state,
                    conn.as_mut(),
                    &entity,
                    entity_type,
                    role,
                    database,
                    schema,
                    self.cancellation_token.clone(),
                )?;
                Ok(result)
            }
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn get_dataset_location(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("get_dataset_location", &["relation"], args);
                let relation_val = iter.next_arg::<&Value>()?;
                let relation = downcast_value_to_dyn_base_relation(relation_val)?;
                iter.finish()?;

                let mut conn =
                    adapter.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let result = adapter.get_dataset_location(
                    state,
                    conn.as_mut(),
                    relation.as_ref(),
                    self.cancellation_token.clone(),
                )?;
                Ok(Value::from(result))
            }
            Parse(_) => Ok(Value::from("")),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn update_table_description(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new(
                    "update_table_description",
                    &["database", "schema", "identifier", "description"],
                    args,
                );
                let database = iter.next_arg::<&str>()?;
                let schema = iter.next_arg::<&str>()?;
                let identifier = iter.next_arg::<&str>()?;
                let description = iter.next_arg::<&str>()?;
                iter.finish()?;

                let mut conn =
                    adapter.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let result = adapter.update_table_description(
                    state,
                    conn.as_mut(),
                    database,
                    schema,
                    identifier,
                    description,
                    self.cancellation_token.clone(),
                )?;
                Ok(result)
            }
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn alter_table_add_columns(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("alter_table_add_columns", &["relation", "columns"], args);
                let relation_val = iter.next_arg::<&Value>()?;
                let relation = downcast_value_to_dyn_base_relation(relation_val)?;
                let columns = iter.next_arg::<&Value>()?;
                iter.finish()?;

                let mut conn =
                    adapter.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let result = adapter.alter_table_add_columns(
                    state,
                    conn.as_mut(),
                    &relation,
                    columns.clone(),
                    self.cancellation_token.clone(),
                )?;
                Ok(result)
            }
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn update_columns(&self, state: &State, args: &[Value]) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("update_columns", &["relation", "columns"], args);
                let relation_val = iter.next_arg::<&Value>()?;
                let relation = downcast_value_to_dyn_base_relation(relation_val)?;
                let columns_val = iter.next_arg::<&Value>()?;
                let columns = minijinja_value_to_typed_struct::<IndexMap<String, DbtColumn>>(
                    columns_val.clone(),
                )
                .map_err(|e| {
                    minijinja::Error::new(
                        minijinja::ErrorKind::SerdeDeserializeError,
                        e.to_string(),
                    )
                })?;
                iter.finish()?;

                let mut conn =
                    adapter.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let result = adapter.update_columns_descriptions(
                    state,
                    conn.as_mut(),
                    &relation,
                    columns,
                    self.cancellation_token.clone(),
                )?;
                Ok(result)
            }
            Parse(_) => Ok(none_value()),
        }
    }

    /// Behavior (flags)
    #[tracing::instrument(skip_all, level = "trace")]
    pub fn behavior(&self) -> Value {
        match &self.inner {
            Typed { adapter, .. } => Value::from_object((**adapter.behavior_object()).clone()),
            Parse(_) => Value::from_object(Behavior::new(vec![], &BTreeMap::new())),
        }
    }

    /// List relations without caching.
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn list_relations_without_caching(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter =
                    ArgsIter::new("list_relations_without_caching", &["schema_relation"], args);
                let schema_relation_val = iter.next_arg::<&Value>()?;
                let schema_relation = downcast_value_to_dyn_base_relation(schema_relation_val)?;
                iter.finish()?;

                let resolved_catalog = schema_relation
                    .database_as_resolved_str()
                    .unwrap_or_default();
                let resolved_schema = schema_relation.schema_as_resolved_str().unwrap_or_default();
                if resolved_catalog.is_empty() && resolved_schema.is_empty() {
                    return Ok(empty_vec_value());
                }

                let query_ctx = query_ctx_from_state(state)?
                    .with_desc("list_relations_without_caching adapter call");
                let mut conn =
                    adapter.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let result = adapter.list_relations(
                    &query_ctx,
                    conn.as_mut(),
                    &CatalogAndSchema::from(schema_relation.as_ref()),
                    self.cancellation_token.clone(),
                )?;

                Ok(Value::from_object(
                    result
                        .into_iter()
                        .map(|r| RelationObject::new(r).into_value())
                        .collect::<Vec<_>>(),
                ))
            }
            Parse(_) => Ok(empty_vec_value()),
        }
    }

    /// Check if a DBR capability is available for current compute.
    ///
    /// Accepts capability names as strings (e.g. 'replace_on', 'insert_by_name').
    ///
    /// https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/impl.py#L336-L354
    ///
    /// DEPRECATED: in favor of [`AdapterImpl::has_feature`]
    /// Use `has_feature(capability_name)` instead.
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn has_dbr_capability(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("has_dbr_capability", &["capability_name"], args);
                let capability_name = iter.next_arg::<&str>()?;
                iter.finish()?;

                match adapter.adapter_type() {
                    AdapterType::Databricks => {
                        let has_feature = adapter.has_feature(state, capability_name, self.cancellation_token.clone())?;
                        Ok(Value::from(has_feature.unwrap_or(false)))
                    }
                    _ => Err(AdapterError::new(
                        AdapterErrorKind::NotSupported,
                        format!("has_dbr_capability is only supported by the Databricks adapter. Use the portable adapter.has_feature(\"{}\") instead.", capability_name),
                    )
                    .into()),
                }
            }
            Parse(_) => Ok(Value::from(false)),
        }
    }

    /// Compare Databricks Runtime version.
    ///
    /// https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/connections.py#L226-L227
    ///
    /// Returns:
    /// - 1 if current version > expected
    /// - 0 if current version == expected
    /// - -1 if current version < expected
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn compare_dbr_version(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("compare_dbr_version", &["major", "minor"], args);
                let major = iter.next_arg::<i64>()?;
                let minor = iter.next_arg::<i64>()?;
                iter.finish()?;

                let mut conn =
                    adapter.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let result = adapter.compare_dbr_version(
                    state,
                    conn.as_mut(),
                    major,
                    minor,
                    self.cancellation_token.clone(),
                )?;
                Ok(result)
            }
            Parse(_) => Ok(Value::from(0)),
        }
    }

    /// Returns true if the adapter supports the given feature.
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn has_feature(&self, state: &State, args: &[Value]) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("has_feature", &["feature_name"], args);
                let name = iter.next_arg::<&str>()?;
                iter.finish()?;

                let result = adapter.has_feature(state, name, self.cancellation_token.clone())?;
                Ok(Value::from(result))
            }
            Parse(_) => Ok(Value::from(None::<bool>)),
        }
    }

    /// Extract the database name from a Jinja relation Value and look up its table format.
    fn table_format(&self, relation_val: &Value) -> Option<TableFormat> {
        let database = relation_val.get_attr("database").ok().and_then(|v| {
            if v.is_undefined() || v.is_none() {
                None
            } else {
                v.as_str().map(|s| s.to_owned())
            }
        })?;
        match &self.inner {
            Typed { adapter, .. } => Some(adapter.table_format_for_database(&database)),
            Parse(_) => None,
        }
    }

    /// DEPRECATED: in favor of [`AdapterImpl::has_feature`]
    /// Use `has_feature("motherduck")` instead.
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn is_motherduck(&self, state: &State) -> AdapterResult<Value> {
        match self.adapter_type() {
            AdapterType::DuckDB => self
                .has_feature(state, &[Value::from("motherduck")])
                .map_err(|e| AdapterError::new(AdapterErrorKind::UnexpectedResult, e.to_string())),
            _ => Err(AdapterError::new(
                AdapterErrorKind::NotSupported,
                "is_motherduck() is only available for the DuckDB adapter. Use the portable adapter.has_feature(\"motherduck\") instead.",
            )),
        }
    }

    /// DEPRECATED: in favor of [`AdapterImpl::has_feature`]
    /// Use `!has_feature("transactions")` instead.
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn disable_transactions(&self, state: &State) -> AdapterResult<Value> {
        let result = match &self.inner {
            Typed { adapter, .. } => match adapter.adapter_type() {
                AdapterType::DuckDB => {
                    let transactions_enabled = adapter.has_feature(
                        state,
                        "transactions",
                        self.cancellation_token.clone(),
                    )?;
                    Ok(!transactions_enabled.unwrap_or(true))
                }
                _ => Err(AdapterError::new(
                    AdapterErrorKind::NotSupported,
                    "disable_transactions() is only available for the DuckDB adapter. Use the portable !has_feature(\"transactions\") instead.",
                )),
            },
            // Assume transactions are enabled (!disable_transactions) during parse phase.
            // It's unlikelye that parse phase code would depend on this result for anything.
            Parse(_) => Ok(false),
        };
        Ok(Value::from(result?))
    }

    #[tracing::instrument(skip(self), level = "trace")]
    pub fn get_temp_relation_path(
        &self,
        database: &str,
        identifier: &str,
        batch_id: &str,
    ) -> AdapterResult<BTreeMap<String, Value>> {
        match &self.inner {
            Typed { adapter, .. } => adapter.get_temp_relation_path(database, identifier, batch_id),
            Parse(_) => Err(AdapterError::new(
                AdapterErrorKind::NotSupported,
                "get_temp_relation_path is not available during parsing",
            )),
        }
    }

    #[tracing::instrument(skip_all, level = "trace")]
    pub fn external_root(&self, _state: &State) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => Ok(Value::from(adapter.external_root())),
            Parse(_) => Ok(Value::from(".")),
        }
    }

    #[tracing::instrument(skip_all, level = "trace")]
    pub fn external_write_options(
        &self,
        _state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new(
                    "external_write_options",
                    &["write_location", "rendered_options"],
                    args,
                );
                let write_location = iter.next_arg::<&str>()?;
                let rendered_options = iter.next_arg::<&Value>()?;
                iter.finish()?;

                Ok(Value::from(
                    adapter.external_write_options(write_location, rendered_options),
                ))
            }
            Parse(_) => Ok(empty_string_value()),
        }
    }

    #[tracing::instrument(skip_all, level = "trace")]
    pub fn external_read_location(
        &self,
        _state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        let iter = ArgsIter::new(
            "external_read_location",
            &["write_location", "rendered_options"],
            args,
        );
        let write_location = iter.next_arg::<&str>()?;
        match &self.inner {
            Typed { adapter, .. } => {
                let rendered_options = iter.next_arg::<&Value>()?;
                iter.finish()?;
                Ok(Value::from(
                    adapter.external_read_location(write_location, rendered_options),
                ))
            }
            Parse(_) => Ok(Value::from(write_location)),
        }
    }

    #[tracing::instrument(skip_all, level = "trace")]
    pub fn location_exists(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { .. } => {
                let iter = ArgsIter::new("location_exists", &["location"], args);
                let location = iter.next_arg::<&str>()?;
                iter.finish()?;

                let sql = format!("select 1 from '{}' where 1=0", location);
                let result = self.execute(state, None, &sql, false, false, None, None);
                Ok(Value::from(result.is_ok()))
            }
            Parse(_) => Ok(Value::from(false)),
        }
    }

    /// Compute external path for Databricks external tables.
    ///
    /// https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/impl.py#L208-L209
    #[tracing::instrument(skip_all, level = "trace")]
    pub fn compute_external_path(
        &self,
        _state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("compute_external_path", &["config", "model"], args);
                let config_val = iter.next_arg::<&Value>()?;
                let model_val = iter.next_arg::<&Value>()?;
                let is_incremental = iter
                    .next_kwarg::<Option<bool>>("is_incremental")?
                    .unwrap_or(false);
                iter.finish()?;

                let config = minijinja_value_to_typed_struct::<ModelConfig>(config_val.clone())
                    .map_err(|e| {
                        minijinja::Error::new(
                            minijinja::ErrorKind::SerdeDeserializeError,
                            format!("compute_external_path: Failed to deserialize config: {e}"),
                        )
                    })?;

                let node = minijinja_value_to_typed_struct::<InternalDbtNodeWrapper>(
                    model_val.clone(),
                )
                .map_err(|e| {
                    minijinja::Error::new(
                        minijinja::ErrorKind::SerdeDeserializeError,
                        format!(
                            "compute_external_path: Failed to deserialize InternalDbtNodeWrapper: {e}"
                        ),
                    )
                })?;

                let result = adapter.compute_external_path(
                    config,
                    node.as_internal_node(),
                    is_incremental,
                )?;
                Ok(Value::from(result))
            }
            Parse(_) => Ok(empty_string_value()),
        }
    }

    /// Add UniForm Iceberg table properties.
    ///
    /// https://github.com/databricks/dbt-databricks/blob/bfcb5c7c7714e97e67023119f674d2938b04acb0/dbt/adapters/databricks/impl.py#L280
    ///
    /// ```python
    /// def update_tblproperties_for_uniform_iceberg(
    ///     self, config: BaseConfig, tblproperties: Optional[dict[str, str]] = None
    /// )  -> dict[str, str]
    /// ```
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn update_tblproperties_for_uniform_iceberg(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                if adapter.adapter_type() != AdapterType::Databricks {
                    unimplemented!(
                        "update_tblproperties_for_uniform_iceberg is only supported in Databricks"
                    )
                }

                let iter = ArgsIter::new(
                    "update_tblproperties_for_uniform_iceberg",
                    &["config"],
                    args,
                );
                let config_val = iter.next_arg::<&Value>()?;
                let tblproperties_val = iter.next_kwarg::<Option<Value>>("tblproperties")?;
                iter.finish()?;

                let model_val = config_val.get_attr("model").map_err(|e| {
                    minijinja::Error::new(
                        minijinja::ErrorKind::InvalidArgument,
                        format!(
                            "update_tblproperties_for_uniform_iceberg: config.model is required: {e}"
                        ),
                    )
                })?;

                let config = minijinja_value_to_typed_struct::<ModelConfig>(config_val.clone())
                    .map_err(|e| {
                        minijinja::Error::new(
                            minijinja::ErrorKind::SerdeDeserializeError,
                            e.to_string(),
                        )
                    })?;
                let node = minijinja_value_to_typed_struct::<InternalDbtNodeWrapper>(model_val)
                    .map_err(|e| {
                        minijinja::Error::new(
                            minijinja::ErrorKind::SerdeDeserializeError,
                            e.to_string(),
                        )
                    })?;

                let mut tblproperties = match tblproperties_val {
                    Some(v) if !v.is_none() => minijinja_value_to_typed_struct::<
                        BTreeMap<String, Value>,
                    >(v)
                    .map_err(|e| {
                        minijinja::Error::new(
                            minijinja::ErrorKind::SerdeDeserializeError,
                            e.to_string(),
                        )
                    })?,
                    _ => config
                        .__warehouse_specific_config__
                        .tblproperties
                        .clone()
                        .unwrap_or_default()
                        .into_iter()
                        .map(|(k, v)| (k, yml_value_to_minijinja(v)))
                        .collect(),
                };

                let mut conn =
                    adapter.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                adapter.update_tblproperties_for_uniform_iceberg(
                    state,
                    conn.as_mut(),
                    config,
                    &node,
                    &mut tblproperties,
                    self.cancellation_token.clone(),
                )?;
                Ok(Value::from_serialize(&tblproperties))
            }
            Parse(_) => Ok(empty_map_value()),
        }
    }

    /// Is table UniForm Iceberg
    ///
    /// https://github.com/databricks/dbt-databricks/blob/bfcb5c7c7714e97e67023119f674d2938b04acb0/dbt/adapters/databricks/impl.py#L256C6-L256C7
    ///
    /// ```python
    /// def is_uniform(self, config: BaseConfig) -> bool:
    /// ```
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn is_uniform(&self, state: &State, args: &[Value]) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                if adapter.adapter_type() != AdapterType::Databricks {
                    unimplemented!("is_uniform is only supported in Databricks")
                }

                let iter = ArgsIter::new("is_uniform", &["config"], args);
                let config_val = iter.next_arg::<&Value>()?;
                iter.finish()?;

                let model_val = config_val.get_attr("model").map_err(|e| {
                    minijinja::Error::new(
                        minijinja::ErrorKind::InvalidArgument,
                        format!("is_uniform: config.model is required: {e}"),
                    )
                })?;
                let config = minijinja_value_to_typed_struct::<ModelConfig>(config_val.clone())
                    .map_err(|e| {
                        minijinja::Error::new(
                            minijinja::ErrorKind::SerdeDeserializeError,
                            e.to_string(),
                        )
                    })?;
                let node = minijinja_value_to_typed_struct::<InternalDbtNodeWrapper>(model_val)
                    .map_err(|e| {
                        minijinja::Error::new(
                            minijinja::ErrorKind::SerdeDeserializeError,
                            e.to_string(),
                        )
                    })?;

                let mut conn =
                    adapter.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let result = adapter.is_uniform(
                    state,
                    conn.as_mut(),
                    config,
                    &node,
                    self.cancellation_token.clone(),
                )?;
                Ok(Value::from(result))
            }
            Parse(_) => Ok(Value::from(false)),
        }
    }

    /// Resolve file format from model config.
    ///
    /// Returns the file_format from config, or adapter-specific default.
    /// Databricks default: "delta". Used by clone materialization.
    ///
    /// https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/impl.py
    /// DatabricksConfig has file_format: str = "delta"
    #[tracing::instrument(skip(self, args), level = "trace")]
    pub fn resolve_file_format(
        &self,
        _: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("resolve_file_format", &["config"], args);
                let config_val = iter.next_arg::<&Value>()?;
                iter.finish()?;

                let config = minijinja_value_to_typed_struct::<ModelConfig>(config_val.clone())
                    .map_err(|e| {
                        minijinja::Error::new(
                            minijinja::ErrorKind::SerdeDeserializeError,
                            e.to_string(),
                        )
                    })?;

                let file_format = adapter.resolve_file_format(config)?;
                Ok(Value::from(file_format))
            }
            Parse(_) => Ok(Value::from("delta")),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn copy_table(&self, state: &State, args: &[Value]) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new(
                    "copy_table",
                    &[
                        "tmp_relation_partitioned",
                        "target_relation_partitioned",
                        "materialization",
                    ],
                    args,
                );
                let tmp_relation_val = iter.next_arg::<&Value>()?;
                let tmp_relation_partitioned =
                    downcast_value_to_dyn_base_relation(tmp_relation_val)?;
                let target_relation_val = iter.next_arg::<&Value>()?;
                let target_relation_partitioned =
                    downcast_value_to_dyn_base_relation(target_relation_val)?;
                let materialization = iter.next_arg::<&str>()?;
                iter.finish()?;

                adapter
                    .engine()
                    .relation_cache()
                    .insert_relation(target_relation_partitioned.clone(), None);

                let mut conn =
                    adapter.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                adapter.copy_table(
                    state,
                    conn.as_mut(),
                    &tmp_relation_partitioned,
                    &target_relation_partitioned,
                    materialization.to_string(),
                    self.cancellation_token.clone(),
                )?;
                Ok(none_value())
            }
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip(self), level = "trace")]
    pub fn describe_relation(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("describe_relation", &["relation"], args);
                let relation_val = iter.next_arg::<&Value>()?;
                let relation = downcast_value_to_dyn_base_relation(relation_val)?;
                iter.finish()?;

                let mut conn =
                    adapter.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                Ok(adapter
                    .describe_relation(conn.as_mut(), &relation, Some(state))?
                    .map(Value::from_object)
                    .unwrap_or_else(none_value))
            }
            Parse(_) => Ok(none_value()),
        }
    }

    /// Generate a unique temporary table suffix.
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/4dc395b42dae78e895adf9c66ad6811534e879a6/dbt-athena/src/dbt/adapters/athena/impl.py#L445
    #[tracing::instrument(skip(self, _state), level = "trace")]
    pub fn generate_unique_temporary_table_suffix(
        &self,
        _state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("generate_unique_temporary_table_suffix", &[], args);
                let suffix_initial = iter
                    .next_kwarg::<Option<String>>("suffix_initial")?
                    .or(None);
                iter.finish()?;

                let suffix = adapter.generate_unique_temporary_table_suffix(suffix_initial)?;

                Ok(Value::from(suffix))
            }
            Parse(_) => Ok(Value::from("")),
        }
    }

    /// Get the list of valid incremental strategies for this adapter.
    #[tracing::instrument(skip(self, _state), level = "trace")]
    pub fn valid_incremental_strategies(&self, _state: &State) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => Ok(Value::from_serialize(
                adapter.valid_incremental_strategies(),
            )),
            Parse(_) => Ok(empty_vec_value()),
        }
    }

    pub fn is_cluster(&self) -> Result<Value, minijinja::Error> {
        let is_cluster = match &self.inner {
            Typed { adapter, .. } => adapter.is_cluster().map_err(minijinja::Error::from)?,
            Parse(_) => false,
        };
        Ok(Value::from(is_cluster))
    }

    #[tracing::instrument(skip(self, _state), level = "trace")]
    pub fn redact_credentials(
        &self,
        _state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("redact_credentials", &["sql"], args);
                let sql = iter.next_arg::<&str>()?;
                iter.finish()?;

                let sql_redacted = adapter.redact_credentials(sql)?;
                Ok(Value::from(sql_redacted))
            }
            Parse(_) => Ok(Value::from("")),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn get_partitions_metadata(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("get_partitions_metadata", &["table"], args);
                let relation_val = iter.next_arg::<&Value>()?;
                let relation = downcast_value_to_dyn_base_relation(relation_val)?;
                iter.finish()?;

                adapter.get_partitions_metadata(state, relation.as_ref())
            }
            Parse(_) => Ok(none_value()),
        }
    }

    /// Get columns to persist documentation for.
    ///
    /// Given existing columns and columns from the model, determines which columns
    /// to update and persist docs for. Only supported by Databricks.
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn get_persist_doc_columns(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new(
                    "get_persist_doc_columns",
                    &["existing_columns", "model_columns"],
                    args,
                );
                let existing_columns = iter.next_arg::<&Value>()?;
                let model_columns = iter.next_arg::<&Value>()?;
                iter.finish()?;

                adapter.get_persist_doc_columns(state, existing_columns, model_columns)
            }
            Parse(_) => Ok(none_value()),
        }
    }

    pub fn get_column_tags_from_model(
        &self,
        _state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("get_column_tags_from_model", &["model"], args);
                let model_val = iter.next_arg::<&Value>()?;
                iter.finish()?;

                let node =
                    minijinja_value_to_typed_struct::<InternalDbtNodeWrapper>(model_val.clone())
                        .map_err(|e| {
                            minijinja::Error::new(
                                minijinja::ErrorKind::SerdeDeserializeError,
                                e.to_string(),
                            )
                        })?;

                let result = adapter.get_column_tags_from_model(node.as_internal_node())?;
                Ok(result)
            }
            Parse(_) => Ok(none_value()),
        }
    }

    /// Parse columns and constraints for table creation.
    ///
    /// Used by Databricks adapter for table creation with constraints.
    #[tracing::instrument(skip_all, level = "trace")]
    pub fn parse_columns_and_constraints(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new(
                    "parse_columns_and_constraints",
                    &["existing_columns", "model_columns", "model_constraints"],
                    args,
                );
                let existing_columns = iter.next_arg::<&Value>()?;
                let model_columns = iter.next_arg::<&Value>()?;
                let model_constraints = iter.next_arg::<&Value>()?;
                iter.finish()?;

                adapter.parse_columns_and_constraints(
                    state,
                    existing_columns,
                    model_columns,
                    model_constraints,
                )
            }
            Parse(_) => Ok(Value::from(vec![
                Value::from(Vec::<Value>::new()),
                Value::from(Vec::<Value>::new()),
            ])),
        }
    }

    /// Get the configuration of an existing relation from the remote data warehouse.
    ///
    /// https://github.com/databricks/dbt-databricks/blob/13686739eb59566c7a90ee3c357d12fe52ec02ea/dbt/adapters/databricks/impl.py#L797
    #[tracing::instrument(skip_all, level = "trace")]
    pub fn get_relation_config(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("get_relation_config", &["relation"], args);
                let relation_val = iter.next_arg::<&Value>()?;
                let relation = downcast_value_to_dyn_base_relation(relation_val)?;
                iter.finish()?;

                let mut conn =
                    adapter.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let config = adapter.get_relation_config(
                    state,
                    conn.as_mut(),
                    &relation,
                    self.cancellation_token.clone(),
                )?;
                Ok(Value::from_object(config))
            }
            Parse(_) => Ok(none_value()),
        }
    }

    /// Get configuration from a model node.
    ///
    /// Given a model, parse and build its configurations.
    /// https://github.com/databricks/dbt-databricks/blob/13686739eb59566c7a90ee3c357d12fe52ec02ea/dbt/adapters/databricks/impl.py#L810
    #[tracing::instrument(skip_all, level = "trace")]
    pub fn get_config_from_model(
        &self,
        _state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("get_config_from_model", &["model"], args);
                let model_val = iter.next_arg::<&Value>()?;
                iter.finish()?;

                let node =
                    minijinja_value_to_typed_struct::<InternalDbtNodeWrapper>(model_val.clone())
                        .map_err(|e| {
                            minijinja::Error::new(
                                minijinja::ErrorKind::SerdeDeserializeError,
                                e.to_string(),
                            )
                        })?;

                Ok(adapter.get_config_from_model(&node)?)
            }
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip_all, level = "trace")]
    pub fn get_relations_without_caching(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter =
                    ArgsIter::new("get_relations_without_caching", &["schema_relation"], args);
                let relation_val = iter.next_arg::<&Value>()?;
                let relation = downcast_value_to_dyn_base_relation(relation_val)?;
                iter.finish()?;

                adapter.get_relations_without_caching(state, &relation)
            }
            Parse(_) => Ok(empty_vec_value()),
        }
    }

    #[tracing::instrument(skip_all, level = "trace")]
    pub fn parse_index(&self, state: &State, args: &[Value]) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("parse_index", &["raw_index"], args);
                let raw_index = iter.next_arg::<&Value>()?;
                iter.finish()?;

                adapter.parse_index(state, raw_index)
            }
            Parse(_) => Ok(none_value()),
        }
    }

    /// Clean SQL by removing extra whitespace and normalizing format.
    ///
    /// Only available with Databricks adapter.
    #[tracing::instrument(skip_all, level = "trace")]
    pub fn clean_sql(&self, _state: &State, args: &[Value]) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("clean_sql", &["sql"], args);
                let sql = iter.next_arg::<&str>()?;
                iter.finish()?;

                Ok(Value::from(adapter.clean_sql(sql)?))
            }
            Parse(_) => unimplemented!("clean_sql"),
        }
    }

    /// Used internally to attempt executing a Snowflake `use warehouse [name]` statement.
    ///
    /// # Returns
    ///
    /// Returns true if the warehouse was overridden, false otherwise
    #[tracing::instrument(skip(self), level = "trace")]
    pub fn use_warehouse(&self, warehouse: Option<String>, node_id: &str) -> FsResult<bool> {
        // TODO(jason): Record/replay non-jinja internal calls non-invasively
        // https://github.com/dbt-labs/fs/issues/7736
        if let Some(tm) = self.time_machine()
            && tm.is_replaying()
        {
            return Ok(false);
        }

        match &self.inner {
            Typed { adapter, .. } => {
                if warehouse.is_none() {
                    return Ok(false);
                }

                let mut conn = adapter.borrow_tlocal_connection(None, Some(node_id.to_string()))?;
                adapter.use_warehouse(
                    conn.as_mut(),
                    warehouse.unwrap(),
                    node_id,
                    self.cancellation_token.clone(),
                )?;
                Ok(true)
            }
            Parse(_) => Ok(false),
        }
    }

    /// Used internally to attempt executing a Snowflake `use warehouse [name]` statement.
    ///
    /// To restore to the warehouse configured in profiles.yml
    #[tracing::instrument(skip(self), level = "trace")]
    pub fn restore_warehouse(&self, node_id: &str) -> FsResult<()> {
        // TODO(jason): Record/replay non-jinja internal calls non-invasively
        // https://github.com/dbt-labs/fs/issues/7736
        if let Some(tm) = self.time_machine()
            && tm.is_replaying()
        {
            return Ok(());
        }

        match &self.inner {
            Typed { adapter, .. } => {
                let mut conn = adapter.borrow_tlocal_connection(None, Some(node_id.to_string()))?;
                adapter.restore_warehouse(
                    conn.as_mut(),
                    node_id,
                    self.cancellation_token.clone(),
                )?;
                Ok(())
            }
            Parse(_) => Ok(()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn load_dataframe(&self, state: &State, args: &[Value]) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new(
                    "load_dataframe",
                    &[
                        "database",
                        "schema",
                        "table_name",
                        "file_path",
                        "agate_table",
                        "column_overrides",
                        "field_delimiter",
                    ],
                    args,
                );
                let database = iter.next_arg::<&str>()?;
                let schema = iter.next_arg::<&str>()?;
                let table_name = iter.next_arg::<&str>()?;
                let file_path = iter.next_arg::<&str>()?;
                let agate_table = iter
                    .next_arg::<&Value>()?
                    .downcast_object::<AgateTable>()
                    .ok_or_else(|| {
                        minijinja::Error::new(
                            minijinja::ErrorKind::InvalidOperation,
                            "agate_table must be an agate.Table",
                        )
                    })?;
                let column_overrides_val = iter.next_arg::<Value>()?;
                let column_overrides = minijinja_value_to_typed_struct::<IndexMap<String, String>>(
                    column_overrides_val,
                )
                .map_err(|e| {
                    minijinja::Error::new(
                        minijinja::ErrorKind::SerdeDeserializeError,
                        e.to_string(),
                    )
                })?;
                let field_delimiter = iter.next_arg::<&str>()?;
                iter.finish()?;

                let mut conn =
                    adapter.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let ctx = query_ctx_from_state(state)?.with_desc("load_dataframe");
                let sql = "";
                let result = adapter.load_dataframe(
                    &ctx,
                    conn.as_mut(),
                    sql,
                    database,
                    schema,
                    table_name,
                    agate_table,
                    file_path,
                    column_overrides,
                    field_delimiter,
                    self.cancellation_token.clone(),
                )?;
                Ok(result)
            }
            Parse(_) => Ok(none_value()),
        }
    }

    /// Get all relevant metadata about a dynamic table to return as a dict to Agate Table row
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/703180a871f2960cd0c91765ffc4b1dc111d615b/dbt-snowflake/src/dbt/adapters/snowflake/impl.py#L510
    ///
    /// ```python
    /// def describe_dynamic_table(self, relation: SnowflakeRelation) -> Dict[str, Any]
    /// ```
    #[tracing::instrument(skip(self, state), level = "trace")]
    pub fn describe_dynamic_table(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let iter = ArgsIter::new("describe_dynamic_table", &["relation"], args);
                let relation_val = iter.next_arg::<&Value>()?;
                let relation = downcast_value_to_dyn_base_relation(relation_val)?;
                let include_transient = iter
                    .next_kwarg::<Option<bool>>("include_transient")?
                    .unwrap_or(false);
                iter.finish()?;

                let mut conn =
                    adapter.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                adapter.describe_dynamic_table(
                    state,
                    conn.as_mut(),
                    &relation,
                    include_transient,
                    self.cancellation_token.clone(),
                )
            }
            Parse(_) => {
                let map = [("dynamic_table", none_value())]
                    .into_iter()
                    .collect::<HashMap<_, _>>();
                Ok(Value::from_serialize(map))
            }
        }
    }

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
    pub fn get_catalog_integration(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        unimplemented!(
            "get_catalog_integration is unavailable in Fusion. Access catalogs metadata directly from a catalog relation obtained using adapter.build_catalog_relation(model: RelationConfig)"
        )
    }
}

impl Adapter {
    pub fn call_method_impl(
        self: &Arc<Self>,
        state: &State,
        name: &str,
        args: &[Value],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, minijinja::Error> {
        match name {
            // macro_name: str, macro_namespace: Optional[str] = None
            "dispatch" => self.dispatch(state, args),
            "execute" => {
                // sql: str, auto_begin: bool = False, fetch: bool = False, limit: Optional[int] = None
                let iter = ArgsIter::new(name, &["sql"], args);
                let sql = iter.next_arg::<&str>()?;
                let auto_begin = iter
                    .next_kwarg::<Option<bool>>("auto_begin")?
                    .unwrap_or(false);
                let mut fetch = iter.next_kwarg::<Option<bool>>("fetch")?.unwrap_or(false);
                let limit = iter.next_kwarg::<Option<i64>>("limit")?;
                let options = if let Some(value) = iter.next_kwarg::<Option<Value>>("options")? {
                    Some(HashMap::<String, String>::deserialize(value).map_err(|e| {
                        minijinja::Error::new(
                            minijinja::ErrorKind::SerdeDeserializeError,
                            e.to_string(),
                        )
                    })?)
                } else {
                    None
                };
                // TODO(harry): add iter.finish() and fix the tests

                // NOTE(serramatutu): this is a hacky fix for: https://github.com/dbt-labs/dbt-fusion/issues/1332
                // It is possible this still fails for other things that return large result sets
                // unnecessarily. Without users explicitly running with `fetch=False`, we'd need full SQL
                // parsing to determine whether to fetch or not.
                if self.adapter_type() == AdapterType::Bigquery
                    && sql.trim().to_lowercase().starts_with("alter table")
                {
                    fetch = false;
                }

                let (response, table) =
                    self.execute(state, None, sql, auto_begin, fetch, limit, options)?;
                Ok(Value::from_iter([
                    Value::from_object(response),
                    Value::from_object(table),
                ]))
            }
            // sql: str,
            // auto_begin: bool = True,
            // bindings: Optional[Any] = None,
            // abridge_sql_log: bool = False,
            // retryable_exceptions: Tuple[Type[Exception], ...] = tuple(),
            // retry_limit: int = 1,
            "add_query" => self.add_query(state, args),
            // model: dict, compiled_code: str
            "submit_python_job" => self.submit_python_job(state, args),
            "get_relation" => {
                // database: str
                // schema: str
                // identifier: str
                // needs_information: bool = False
                let iter = ArgsIter::new(name, &["database", "schema", "identifier"], args);

                let database = iter.next_arg::<&str>().or_else(|e| {
                    if self.adapter_type() == AdapterType::Databricks {
                        Ok(DEFAULT_DATABRICKS_DATABASE)
                    } else {
                        Err(e)
                    }
                })?;
                let schema = iter.next_arg::<&str>()?;
                let identifier = iter.next_arg::<&str>()?;
                let needs_information = iter
                    .next_kwarg::<Option<bool>>("needs_information")?
                    .unwrap_or(false);
                iter.finish()?;

                self.get_relation(state, database, schema, identifier, needs_information)
            }
            "get_columns_in_relation" => {
                // relation: BaseRelation
                let iter = ArgsIter::new(name, &["relation"], args);
                let relation = iter.next_arg::<&Value>()?;
                let relation = downcast_value_to_dyn_base_relation(relation)?;
                iter.finish()?;

                self.get_columns_in_relation(state, relation.as_ref())
            }
            "build_catalog_relation" => {
                let iter = ArgsIter::new(name, &["model"], args);
                let model = iter.next_arg::<&Value>()?;
                iter.finish()?;

                // Case 1: caller passed a plain string (CLD name) -- this is a hack for
                // incremental polaris model. TODO: remove this when catalog_relation is
                // fully engineered to no longer require runtime attribute shim-based solutions
                if model.kind() == ValueKind::String {
                    return self.build_catalog_relation(model);
                }

                // Case 2: caller passed a model object
                // TODO: When we remove case 1, we can serialize this as an InternalDbtNode (and add the necessary attributes to their respective resource-type specific attribute structs)
                // Ex: minijinja_value_to_typed_struct::<DbtModel>(model.clone()).is_ok();

                self.build_catalog_relation(model)
            }
            // relation: BaseRelation, include_transient: bool = False
            "describe_dynamic_table" => self.describe_dynamic_table(state, args),
            "get_catalog_integration" => self.get_catalog_integration(state, args),
            "type" => Ok(Value::from(self.adapter_type().to_string())),
            // config: dict
            "get_hard_deletes_behavior" => self.get_hard_deletes_behavior(state, args),
            "cache_added" => {
                // relation: BaseRelation
                let iter = ArgsIter::new(name, &["relation"], args);
                let relation = iter.next_arg::<&Value>()?;
                let relation = downcast_value_to_dyn_base_relation(relation)?;
                iter.finish()?;

                self.cache_added(state, relation)
            }
            // relation: BaseRelation
            "cache_dropped" => self.cache_dropped(state, args),
            // from_relation: BaseRelation, to_relation: BaseRelation
            "cache_renamed" => self.cache_renamed(state, args),
            // identifier: str
            "quote" => self.quote(state, args),
            // identifier: str, quote_key: str
            "quote_as_configured" => self.quote_as_configured(state, args),
            // column: str, quote_config: Optional[bool]
            "quote_seed_column" => self.quote_seed_column(state, args),
            // relation: BaseRelation
            "drop_relation" => self.drop_relation(state, args),
            // relation: BaseRelation
            "truncate_relation" => self.truncate_relation(state, args),
            // from_relation: BaseRelation, to_relation: BaseRelation
            "rename_relation" => self.rename_relation(state, args),
            // from_relation: BaseRelation, to_relation: BaseRelation
            "expand_target_column_types" => self.expand_target_column_types(state, args),
            "list_schemas" => {
                // database: str
                let iter = ArgsIter::new(name, &["database"], args);
                let database = iter.next_arg::<&str>()?;
                iter.finish()?;

                self.list_schemas(state, database)
            }
            // relation: BaseRelation
            "create_schema" => self.create_schema(state, args),
            // relation: BaseRelation
            "drop_schema" => self.drop_schema(state, args),
            // relation: BaseRelation
            "valid_snapshot_target" => self.valid_snapshot_target(state, args),
            // relation: BaseRelation, column_names: Optional[Dict[str, str]], strategy: SnapshotStrategy
            "assert_valid_snapshot_target_given_strategy" => {
                self.assert_valid_snapshot_target_given_strategy(state, args)
            }
            // from_relation: BaseRelation, to_relation: BaseRelation
            "get_missing_columns" => self.get_missing_columns(state, args),
            // raw_constraints: List[ModelConstraint]
            "render_raw_model_constraints" => self.render_raw_model_constraints(state, args),
            // grants_table: AgateTable
            "standardize_grants_dict" => self.standardize_grants_dict(state, args),
            // agate_table: AgateTable, col_idx: int
            "convert_type" => self.convert_type(state, args),
            // raw_columns: dict
            "render_raw_columns_constraints" => self.render_raw_columns_constraints(state, args),
            // database: str
            "verify_database" => self.verify_database(state, args),
            "commit" => self.commit(),
            // context: dict, strategy: str
            "get_incremental_strategy_macro" => self.get_incremental_strategy_macro(state, args),
            // database: str, schema: str
            "check_schema_exists" => self.check_schema_exists(state, args),
            "get_relations_by_pattern" => {
                // schema_pattern: str, table_pattern: str, exclude: Optional[str] = None,
                // database: Optional[str] = None, quote_table: Optional[bool] = None,
                // excluded_schemas: Optional[List[str]] = None
                let iter = ArgsIter::new(name, &["schema_pattern", "table_pattern"], args);
                let schema_pattern = iter.next_arg::<&str>()?;
                let table_pattern = iter.next_arg::<&str>()?;
                let exclude = iter.next_kwarg::<Option<&str>>("exclude")?;
                let database = iter.next_kwarg::<Option<&str>>("database")?;
                let quote_table = iter.next_kwarg::<Option<bool>>("quote_table")?;
                let excluded_schemas = iter.next_kwarg::<Option<Value>>("excluded_schemas")?;
                iter.finish()?;

                self.get_relations_by_pattern(
                    state,
                    schema_pattern,
                    table_pattern,
                    exclude,
                    database,
                    quote_table,
                    excluded_schemas,
                )
            }
            // only available for BigQuery
            // columns: dict
            "nest_column_data_types" => self.nest_column_data_types(state, args),
            // partition_by: dict, columns: List[Column]
            // only available for BigQuery
            "get_struct_select_expression" => {
                // col_name: string, data_type: string
                let iter = ArgsIter::new(name, &["col_name", "data_type"], args);
                let col_name = iter.next_arg::<&str>()?;
                let data_type = iter.next_arg::<&str>()?;
                iter.finish()?;

                self.get_struct_select_expression(state, col_name, data_type)
            }
            "add_time_ingestion_partition_column" => {
                self.add_time_ingestion_partition_column(state, args)
            }
            // raw_partition_by: Optional[dict]
            "parse_partition_by" => self.parse_partition_by(state, args),
            // relation: Optional[BaseRelation], partition_by: Optional[dict], cluster_by: Optional[dict]
            "is_replaceable" => self.is_replaceable(state, args),
            // schema_relation: BaseRelation
            "list_relations_without_caching" => self.list_relations_without_caching(state, args),
            // tmp_relation_partitioned: BaseRelation, target_relation_partitioned: BaseRelation, materialization: str
            "copy_table" => self.copy_table(state, args),
            // relation: BaseRelation, columns: Dict[str, DbtColumn]
            "update_columns" => self.update_columns(state, args),
            // database: str, schema: str, identifier: str, description: str
            "update_table_description" => self.update_table_description(state, args),
            // relation: BaseRelation, columns: Value
            "alter_table_add_columns" => self.alter_table_add_columns(state, args),
            // database: str, schema: str, table_name: str, file_path: str,
            // agate_table: AgateTable, column_overrides: dict, field_delimiter: str
            "load_dataframe" => self.load_dataframe(state, args),
            "upload_file" => self.upload_file(state, args),
            // relation: BaseRelation
            "get_bq_table" => self.get_bq_table(state, args),
            // relation: BaseRelation
            "describe_relation" => self.describe_relation(state, args),
            // entity: BaseRelation, entity_type: str, role: Optional[str], grant_target_dict: GrantAccessToTarget
            "grant_access_to" => self.grant_access_to(state, args),
            // relation: BaseRelation
            "get_dataset_location" => self.get_dataset_location(state, args),
            // sql: str
            "get_column_schema_from_query" => self.get_column_schema_from_query(state, args),
            // sql: str
            "get_columns_in_select_sql" => self.get_columns_in_select_sql(state, args),
            // config: dict, node: dict, temporary: Optional[bool] = False
            "get_common_options" => self.get_common_options(state, args),
            // config: dict, node: dict, temporary: Optional[bool] = False
            "get_table_options" => self.get_table_options(state, args),
            // config: dict, node: dict
            "get_view_options" => self.get_view_options(state, args),
            // table: BaseRelation
            "get_partitions_metadata" => self.get_partitions_metadata(state, args),
            // schema_relation: BaseRelation
            "get_relations_without_caching" => self.get_relations_without_caching(state, args),
            // raw_index: dict
            "parse_index" => self.parse_index(state, args),
            // sql: str
            "redact_credentials" => self.redact_credentials(state, args),
            "is_cluster" => self.is_cluster(),
            // capability_name: str
            "has_dbr_capability" => self.has_dbr_capability(state, args),
            "table_format" => {
                // Returns the table format for a relation's database (e.g. "ducklake", "iceberg", "default").
                // relation: Relation
                let iter = ArgsIter::new(name, &["relation"], args);
                let relation_val = iter.next_arg::<&Value>()?;
                iter.finish()?;
                let format_str = self
                    .table_format(relation_val)
                    .map(|f| f.as_str(self.adapter_type()))
                    .unwrap_or("default");
                Ok(Value::from(format_str))
            }
            "is_ducklake" => {
                // Backwards-compatible alias: adapter.is_ducklake(relation) == (table_format(relation) == "ducklake")
                // relation: Relation
                let iter = ArgsIter::new(name, &["relation"], args);
                let relation_val = iter.next_arg::<&Value>()?;
                iter.finish()?;
                Ok(Value::from(
                    self.table_format(relation_val) == Some(TableFormat::DuckLake),
                ))
            }
            // DEPRECATED: in favor of "has_feature"
            "is_motherduck" => Ok(self.is_motherduck(state).map_err(minijinja::Error::from)?),
            // DEPRECATED: in favor of "has_feature"
            "disable_transactions" => Ok(self
                .disable_transactions(state)
                .map_err(minijinja::Error::from)?),
            // feature_name: str
            "has_feature" => self.has_feature(state, args),
            "get_temp_relation_path" => {
                // model: Any, batch_id: str = ""
                let iter = ArgsIter::new(name, &["relation"], args);
                let relation_val = iter.next_arg::<&Value>()?;
                let batch_id = iter.next_kwarg::<Option<&str>>("batch_id")?.unwrap_or("");
                iter.finish()?;
                let database = relation_val
                    .get_attr("database")
                    .ok()
                    .and_then(|v| {
                        if v.is_undefined() || v.is_none() {
                            None
                        } else {
                            v.as_str().map(|s| s.to_owned())
                        }
                    })
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        minijinja::Error::new(
                            minijinja::ErrorKind::InvalidOperation,
                            "get_temp_relation_path: relation.database is required",
                        )
                    })?;
                let identifier = relation_val
                    .get_attr("identifier")
                    .ok()
                    .and_then(|v| {
                        if v.is_undefined() || v.is_none() {
                            None
                        } else {
                            v.as_str().map(|s| s.to_owned())
                        }
                    })
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        minijinja::Error::new(
                            minijinja::ErrorKind::InvalidOperation,
                            "get_temp_relation_path: relation.identifier is required",
                        )
                    })?;
                let path = self
                    .get_temp_relation_path(&database, &identifier, batch_id)
                    .map_err(minijinja::Error::from)?;
                Ok(Value::from_object(path))
            }
            // major: int, minor: int
            "compare_dbr_version" => self.compare_dbr_version(state, args),
            // config: dict, model: dict, is_incremental: Optional[bool] = False
            "compute_external_path" => self.compute_external_path(state, args),
            // config: dict, tblproperties: Optional[dict] = None
            "update_tblproperties_for_uniform_iceberg" => {
                self.update_tblproperties_for_uniform_iceberg(state, args)
            }
            // config: dict
            "is_uniform" => self.is_uniform(state, args),
            // config: dict
            "resolve_file_format" => self.resolve_file_format(state, args),
            "valid_incremental_strategies" => {
                // No arguments required
                self.valid_incremental_strategies(state)
            }
            // relation: BaseRelation
            "get_relation_config" => self.get_relation_config(state, args),
            // model: dict
            "get_config_from_model" => self.get_config_from_model(state, args),
            // existing_columns: List[Column], model_columns: dict
            "get_persist_doc_columns" => self.get_persist_doc_columns(state, args),
            // model: dict
            "get_column_tags_from_model" => self.get_column_tags_from_model(state, args),
            // suffix_initial: Optional[str] = None
            "generate_unique_temporary_table_suffix" => {
                self.generate_unique_temporary_table_suffix(state, args)
            }
            // existing_columns: List[Column], model_columns: dict, model_constraints: List[dict]
            "parse_columns_and_constraints" => self.parse_columns_and_constraints(state, args),
            // sql: str
            "clean_sql" => self.clean_sql(state, args),
            "get_seed_file_path" => {
                // model: dict (seed node)
                let iter = ArgsIter::new(name, &["model"], args);
                let model = iter.next_arg::<Value>()?;
                iter.finish()?;

                // Extract seed file path from the model
                // The seed file path is root_path + original_file_path
                let seed =
                    minijinja_value_to_typed_struct::<dbt_schemas::schemas::nodes::DbtSeed>(model)
                        .map_err(|e| {
                            minijinja::Error::new(
                                minijinja::ErrorKind::SerdeDeserializeError,
                                format!("get_seed_file_path: Failed to deserialize DbtSeed: {e}"),
                            )
                        })?;

                let root_path = seed.__seed_attr__.root_path.unwrap_or_default();
                let original_file_path = &seed.__common_attr__.original_file_path;
                let full_path = root_path.join(original_file_path);
                Ok(Value::from(full_path.display().to_string()))
            }
            "external_root" => {
                // (no args)
                let iter = ArgsIter::new(name, &[], args);
                iter.finish()?;
                self.external_root(state)
            }
            "external_write_options" => self.external_write_options(state, args),
            "external_read_location" => self.external_read_location(state, args),
            "location_exists" => self.location_exists(state, args),
            // ---- ClickHouse adapter method stubs (MVP) ----
            // These methods are called from ClickHouse Jinja macros. They are
            // registered for all adapters (they only run when the dispatch
            // hits a ClickHouse macro), but they return conservative defaults
            // so behavior matches "no cluster, no special engine, current
            // server version, cannot exchange tables atomically".
            "clickhouse_db_engine_clause" => {
                // (no args) -> "" (skip "ENGINE = ..." in CREATE DATABASE)
                Ok(Value::from(""))
            }
            "get_clickhouse_cluster_name" => {
                // (no args) -> None (no ON CLUSTER usage)
                Ok(Value::from(()))
            }
            "get_model_settings" => {
                // model: dict, engine: str = "MergeTree"  -> "" (no settings)
                Ok(Value::from(""))
            }
            "get_model_query_settings" => {
                // model: dict -> SETTINGS clause appended to CREATE TABLE ... AS (SELECT ...)
                // Default join_use_nulls=1 makes unmatched LEFT JOIN rows produce NULL
                // instead of ClickHouse's default type-zero values (0 for Int64, etc.),
                // restoring standard SQL semantics.
                // Users can override via model config `query_settings`.
                Ok(Value::from("SETTINGS join_use_nulls = 1"))
            }
            "is_before_version" => {
                // version: str -> false (assume modern server)
                Ok(Value::from(false))
            }
            "can_exchange" => {
                // schema: str, type: str -> false (don't use EXCHANGE TABLES)
                Ok(Value::from(false))
            }
            "should_on_cluster" => {
                // materialized: str, engine_clause: str -> false
                Ok(Value::from(false))
            }
            "calculate_incremental_strategy" => {
                // strategy: Optional[str] -> str (default to "append" if not set)
                let strategy = args
                    .first()
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .unwrap_or("append");
                Ok(Value::from(strategy))
            }
            "validate_incremental_strategy" => {
                // strategy: str, predicates: list, unique_key: ?, partition_by: ? -> None
                // Stub: all strategies accepted for MVP
                Ok(Value::from(()))
            }
            "check_incremental_schema_changes" => {
                // on_schema_change: str, existing_relation: Relation, sql: str -> None
                // Stub: return None (no schema changes tracked); only reached when on_schema_change != 'ignore'
                Ok(Value::from(()))
            }
            "filter_settings_by_engine" => {
                // model: dict, settings: str -> str
                Ok(args.get(1).cloned().unwrap_or_else(|| Value::from("")))
            }
            "get_ch_database" => {
                // schema: str -> str (CH database = schema in 2-part naming)
                Ok(args.first().cloned().unwrap_or_else(|| Value::from("")))
            }
            "get_csv_data" => {
                let table = args
                    .first()
                    .ok_or_else(|| {
                        minijinja::Error::new(
                            minijinja::ErrorKind::MissingArgument,
                            "get_csv_data requires agate_table argument",
                        )
                    })?
                    .downcast_object::<AgateTable>()
                    .ok_or_else(|| {
                        minijinja::Error::new(
                            minijinja::ErrorKind::InvalidOperation,
                            "get_csv_data: argument must be an AgateTable",
                        )
                    })?;
                self.get_csv_data(table)
            }
            "render_equals" => {
                let iter = ArgsIter::new(name, &["expr1", "expr2"], args);
                let expr1 = iter.next_arg::<&str>()?;
                let expr2 = iter.next_arg::<&str>()?;
                iter.finish()?;
                self.render_equals(state, expr1, expr2)
            }
            _ => Err(minijinja::Error::new(
                minijinja::ErrorKind::UnknownMethod,
                format!("Unknown method on adapter object: '{name}'"),
            )),
        }
    }

    pub fn get_csv_data(&self, table: Arc<AgateTable>) -> Result<Value, minijinja::Error> {
        let batch = table.original_record_batch();
        let mut buf: Vec<u8> = Vec::new();
        let mut writer = arrow::csv::WriterBuilder::new()
            .with_header(false)
            .build(&mut buf);
        writer.write(&batch).map_err(|e| {
            minijinja::Error::new(
                minijinja::ErrorKind::InvalidOperation,
                format!("get_csv_data: failed to format CSV: {e}"),
            )
        })?;
        drop(writer);
        Ok(Value::from(String::from_utf8_lossy(&buf).into_owned()))
    }
}

impl Object for Adapter {
    fn call_method(
        self: &Arc<Self>,
        state: &State,
        name: &str,
        args: &[Value],
        listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, minijinja::Error> {
        if let Parse(_) = &self.inner {
            return self.call_method_impl(state, name, args, listeners);
        }
        // NOTE(jason): This function uses the time machine - cross version Fusion snapshot tests
        // not to be confused with conformance ReplayAdapter or Adapter Record/Replay modes
        let node_id = node_id_from_state(state).unwrap_or_else(|| "global".to_string());

        // Determine the semantic category of this call for time machine handling.
        // Pure categories are not recorded and do not increment the call depth tracker.
        let call_category = crate::time_machine::SemanticCategory::from_adapter_method(name);
        let is_pure_or_cache = matches!(
            call_category,
            crate::time_machine::SemanticCategory::Pure
                | crate::time_machine::SemanticCategory::Cache
        );

        // Track call depth for handling nested adapter calls in time machine record mode.
        // Methods might internally call execute via macros,
        // which would cause the inner call to be recorded before the outer one.
        // Only the outermost call should be recorded/replayed.
        // Pure/Cache operations don't increment depth since they may dispatch.
        let (depth, _guard) = if is_pure_or_cache {
            (0, None)
        } else {
            let depth = ADAPTER_CALL_DEPTH.with(|d| {
                let current = d.get();
                d.set(current + 1);
                current
            });

            // RAII guard to decrement depth on exit
            struct DepthGuard;
            impl Drop for DepthGuard {
                fn drop(&mut self) {
                    ADAPTER_CALL_DEPTH.with(|d| d.set(d.get().saturating_sub(1)));
                }
            }
            (depth, Some(DepthGuard))
        };
        let is_outermost = depth == 0;

        // Check if we should replay instead of executing.
        if !is_pure_or_cache
            && let Some(ref tm) = self.time_machine
            && let Some(replay_result) = tm.try_replay(&node_id, name, args)
        {
            return replay_result.map_err(|e| {
                // Reconstruct an AdapterError so that from_jinja_err can downcast it
                // and classify the error code correctly (matching recording behavior).
                let error_msg = e.recorded_error.unwrap_or(e.message);
                // Old recordings used e.to_string() which includes the ErrorKind prefix
                // (e.g. "execution error: ..."). Strip it dynamically so we stay in
                // sync with minijinja. New recordings store only the detail, so this
                // is a no-op for them.
                let prefix = format!("{}: ", minijinja::ErrorKind::Execution);
                let error_msg = error_msg
                    .strip_prefix(&prefix)
                    .unwrap_or(&error_msg)
                    .to_string();
                let adapter_err = AdapterError::new(AdapterErrorKind::Driver, error_msg);
                minijinja::Error::from(adapter_err)
            });
        }

        // Execute the actual adapter call.
        //
        // Pre-condition: In Replay mode leaked calls are safe because this
        // adapter should not have an actual connection to the warehouse.
        //
        // If replaying, assert the engine is mock (for safety)
        if let Some(ref tm) = self.time_machine
            && tm.is_replaying()
        {
            let is_mock = self.engine().is_mock();
            assert!(
                is_mock,
                "Replay mode requires mock engine; attempted on non-mock engine which risks leaking queries"
            );
        }
        let result = self.call_method_impl(state, name, args, listeners);

        // Record if time machine is in recording mode
        if !is_pure_or_cache
            && is_outermost
            && let Some(ref tm) = self.time_machine
        {
            let (result_json, success, error) = match &result {
                Ok(value) => (crate::time_machine::serialize_value(value), true, None),
                Err(e) => {
                    // Record only the detail (raw error message) without the
                    // ErrorKind prefix or stack trace that to_string() includes.
                    let error_msg = e
                        .detail()
                        .map(|d| d.to_string())
                        .unwrap_or_else(|| e.kind().to_string());
                    (serde_json::Value::Null, false, Some(error_msg))
                }
            };

            tm.record_call(
                node_id,
                name,
                crate::time_machine::serialize_args(args),
                result_json,
                success,
                error,
            );
        }

        result
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str() {
            Some("behavior") => Some(self.behavior()),
            // NOTE(serramatutu): BigQuery adapter calls `Relation` from `adapter.Relation`
            // instead of `api.Relation` when executing materialized views
            Some("Relation") => {
                create_static_relation(self.adapter_type(), self.engine().quoting())
            }
            _ => None,
        }
    }
}

impl Adapter {
    fn get_relation_value_from_cache(&self, temp_relation: &dyn BaseRelation) -> Option<Value> {
        let relation_cache = self.engine().relation_cache();
        if let Some(cached_entry) = relation_cache.get_relation(temp_relation) {
            Some(RelationObject::new(cached_entry.relation()).into_value())
        }
        // If we have captured the entire schema previously, we can check for non-existence
        // In these cases, return early with a None value
        else if relation_cache.contains_full_schema_for_relation(temp_relation) {
            Some(none_value())
        } else {
            None
        }
    }
}
