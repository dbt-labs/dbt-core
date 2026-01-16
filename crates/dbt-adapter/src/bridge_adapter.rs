use crate::base_adapter::*;
use crate::cache::RelationCache;
use crate::catalog_relation::CatalogRelation;
use crate::column::{Column, ColumnStatic};
use crate::funcs::*;
use crate::metadata::*;
use crate::parse::adapter::ParseAdapterState;
use crate::query_comment::QueryCommentConfig;
use crate::query_ctx::{node_id_from_state, query_ctx_from_state};
use crate::relation::RelationObject;
use crate::relation::parse::EmptyRelation;
use crate::render_constraint::render_model_constraint;
use crate::snapshots::SnapshotStrategy;
use crate::sql_types::TypeOps;
use crate::stmt_splitter::NaiveStmtSplitter;
use crate::time_machine::TimeMachine;
use crate::typed_adapter::{ReplayAdapter, TypedBaseAdapter};
use crate::{AdapterEngine, AdapterResponse, AdapterResult, BaseAdapter};

use dbt_agate::AgateTable;
use dbt_auth::{AdapterConfig, Auth, auth_for_backend};
use dbt_common::behavior_flags::{Behavior, BehaviorFlag};
use dbt_common::cancellation::CancellationToken;
use dbt_common::{AdapterError, AdapterErrorKind, FsError, FsResult};
use dbt_schema_store::{SchemaEntry, SchemaStoreTrait};
use dbt_schemas::schemas::common::{DbtQuoting, ResolvedQuoting};
use dbt_schemas::schemas::dbt_catalogs::DbtCatalogs;
use dbt_schemas::schemas::dbt_column::DbtColumn;
use dbt_schemas::schemas::manifest::{BigqueryClusterConfig, BigqueryPartitionConfig};
use dbt_schemas::schemas::project::ModelConfig;
use dbt_schemas::schemas::properties::ModelConstraint;
use dbt_schemas::schemas::relations::base::{BaseRelation, ComponentName};
use dbt_schemas::schemas::serde::{minijinja_value_to_typed_struct, yml_value_to_minijinja};
use dbt_schemas::schemas::{InternalDbtNodeAttributes, InternalDbtNodeWrapper};
use dbt_xdbc::Connection;
use indexmap::IndexMap;
use minijinja::constants::TARGET_UNIQUE_ID;
use minijinja::listener::RenderingEventListener;
use minijinja::value::{Kwargs, Object};
use minijinja::{State, Value};
use tracing;
use tracy_client::span;

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::rc::Rc;
use std::sync::{Arc, LazyLock};

// Thread-local counter to track adapter call depth.
// Used to avoid recording/replaying nested adapter calls (e.g., truncate_relation calling execute).
// Only the outermost call is recorded/replayed.
thread_local! {
    static ADAPTER_CALL_DEPTH: Cell<usize> = const { Cell::new(0) };
}

// Thread-local connection.
//
// This implementation provides an efficient connection management strategy:
// 1. Each thread maintains its own connection instance
// 2. Connections are reused across multiple operations within the same thread
// 3. This approach ensures proper transaction management within a DAG node
// 4. The ConnectionGuard wrapper ensures connections are returned to the thread-local
thread_local! {
    static CONNECTION: pri::TlsConnectionContainer = pri::TlsConnectionContainer::new();
}

// https://github.com/dbt-labs/dbt-adapters/blob/3ed165d452a0045887a5032c621e605fd5c57447/dbt-adapters/src/dbt/adapters/base/impl.py#L117
static DEFAULT_BASE_BEHAVIOR_FLAGS: LazyLock<[BehaviorFlag; 2]> = LazyLock::new(|| {
    [
        BehaviorFlag::new(
            "require_batched_execution_for_custom_microbatch_strategy",
            false,
            Some("https://docs.getdbt.com/docs/build/incremental-microbatch"),
            None,
            None,
        ),
        BehaviorFlag::new("enable_truthy_nulls_equals_macro", false, None, None, None),
    ]
});

/// A connection wrapper that automatically returns the connection to the thread local when dropped
/// This ensures that for a single thread, a connection is reused across multiple operations
pub struct ConnectionGuard<'a> {
    conn: Option<Box<dyn Connection>>,
    _phantom: PhantomData<&'a ()>,
}
impl ConnectionGuard<'_> {
    fn new(conn: Box<dyn Connection>) -> Self {
        Self {
            conn: Some(conn),
            _phantom: PhantomData,
        }
    }
}
impl Deref for ConnectionGuard<'_> {
    type Target = Box<dyn Connection>;

    fn deref(&self) -> &Self::Target {
        self.conn.as_ref().unwrap()
    }
}
impl DerefMut for ConnectionGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn.as_mut().unwrap()
    }
}
impl Drop for ConnectionGuard<'_> {
    fn drop(&mut self) {
        let conn = self.conn.take();
        CONNECTION.with(|c| c.replace(conn));
    }
}

/// The inner adapter implementation inside a [BridgeAdapter].
#[derive(Clone)]
enum InnerAdapter {
    /// The actual implementation for all phases except parsing.
    /// The relation cache is now stored in the engine, not here.
    Typed {
        adapter: Arc<dyn TypedBaseAdapter>,
        schema_store: Option<Arc<dyn SchemaStoreTrait>>,
    },
    /// The state necessary to perform operation in a shallow way during the parsing phase.
    Parse(Box<ParseAdapterState>),
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
/// Use the `borrow_tlocal_connection` method, which returns a guard that
/// can be dereferenced into a mutable [Box<dyn Connection>]. When the
/// guard instance is destroyed, the connection returns to the thread-local
/// variable.
///
/// # Relation Cache
///
/// The relation cache is now managed by the engine. Access via `self.engine().relation_cache()`.
#[derive(Clone)]
pub struct BridgeAdapter {
    inner: InnerAdapter,
    /// Time-machine for cross-version snapshot testing (optional)
    time_machine: Option<TimeMachine>,
}

impl fmt::Debug for BridgeAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            Typed { adapter, .. } => adapter.fmt(f),
            Parse(parse_adapter_state) => parse_adapter_state.debug_fmt(f),
        }
    }
}

impl BridgeAdapter {
    /// Create a new bridge adapter.
    ///
    /// The relation cache is obtained from the engine. No longer needs to be passed explicitly.
    pub fn new(
        adapter: Arc<dyn TypedBaseAdapter>,
        schema_store: Option<Arc<dyn SchemaStoreTrait>>,
        time_machine: Option<TimeMachine>,
    ) -> Self {
        let inner = Typed {
            adapter,
            schema_store,
        };
        Self {
            inner,
            time_machine,
        }
    }

    /// Create an instance of [BridgeAdapter] that operates in parse phase mode.
    pub fn new_parse_phase_adapter(
        adapter_type: AdapterType,
        config: dbt_serde_yaml::Mapping,
        package_quoting: DbtQuoting,
        type_ops: Box<dyn TypeOps>,
        token: CancellationToken,
        catalogs: Option<Arc<DbtCatalogs>>,
    ) -> BridgeAdapter {
        let state = Self::make_parse_adapter_state(
            adapter_type,
            config,
            package_quoting,
            type_ops,
            Arc::new(RelationCache::default()),
            token,
            catalogs,
        );
        BridgeAdapter {
            inner: Parse(state),
            time_machine: None,
        }
    }

    pub(crate) fn make_parse_adapter_state(
        adapter_type: AdapterType,
        config: dbt_serde_yaml::Mapping,
        package_quoting: DbtQuoting,
        type_ops: Box<dyn TypeOps>,
        relation_cache: Arc<RelationCache>,
        token: CancellationToken,
        catalogs: Option<Arc<DbtCatalogs>>,
    ) -> Box<ParseAdapterState> {
        let backend = backend_of(adapter_type);

        let auth: Arc<dyn Auth> = auth_for_backend(backend).into();
        let adapter_config = AdapterConfig::new(config);
        let quoting = package_quoting
            .try_into()
            .expect("Failed to convert quoting to resolved quoting");
        let stmt_splitter = Arc::new(NaiveStmtSplitter {});
        let query_comment = QueryCommentConfig::from_query_comment(None, adapter_type, false);

        let engine = AdapterEngine::new(
            adapter_type,
            auth,
            adapter_config,
            quoting,
            stmt_splitter,
            None,
            query_comment,
            type_ops,
            relation_cache,
            token,
        );

        Box::new(ParseAdapterState::new(adapter_type, engine, catalogs))
    }

    /// Get a reference to the time machine, if enabled.
    pub fn time_machine(&self) -> Option<&TimeMachine> {
        self.time_machine.as_ref()
    }

    /// Borrow the current thread-local connection or create one if it's not set yet.
    ///
    /// A guard is returned. When destroyed, the guard returns the connection to
    /// the thread-local variable. If another connection became the thread-local
    /// in the mean time, that connection is dropped and the return proceeds as
    /// normal.
    pub(crate) fn borrow_tlocal_connection(
        &self,
        state: Option<&State>,
        node_id: Option<String>,
    ) -> Result<ConnectionGuard<'_>, minijinja::Error> {
        let _span = span!("BridgeAdapter::borrow_thread_local_connection");
        let mut conn = CONNECTION.with(|c| c.take());
        if conn.is_none() {
            self.new_connection(state, node_id)
                .map(|new_conn| conn.replace(new_conn))?;
        } else if let Some(c) = conn.as_mut() {
            c.update_node_id(node_id);
        }
        let guard = ConnectionGuard::new(conn.unwrap());
        Ok(guard)
    }

    /// Checks if the given [BaseRelation] matches the node currently being rendered
    fn matches_current_relation(&self, state: &State, relation: &Arc<dyn BaseRelation>) -> bool {
        if let Some((database, schema, alias)) = state.database_schema_alias_from_state() {
            // Lowercase name comparison because relation names from the local project
            // are user specified, whereas the input relation may have been a normalized name
            // from the warehouse
            relation
                .database_as_str()
                .is_ok_and(|s| s.eq_ignore_ascii_case(&database))
                && relation
                    .schema_as_str()
                    .is_ok_and(|s| s.eq_ignore_ascii_case(&schema))
                && relation
                    .identifier_as_str()
                    .is_ok_and(|s| s.eq_ignore_ascii_case(&alias))
        } else {
            false
        }
    }

    /// Loads a schema from the schema cache
    fn get_schema_from_cache(&self, relation: &Arc<dyn BaseRelation>) -> Option<SchemaEntry> {
        match &self.inner {
            Typed { schema_store, .. } => schema_store
                .as_ref()
                .and_then(|ss| ss.get_schema(&relation.get_canonical_fqn().unwrap_or_default())),
            Parse(_) => None,
        }
    }

    pub fn parse_adapter_state(&self) -> Option<&ParseAdapterState> {
        match &self.inner {
            Typed { .. } => None,
            Parse(state) => Some(state),
        }
    }
}

impl AdapterTyping for BridgeAdapter {
    fn adapter_type(&self) -> AdapterType {
        match &self.inner {
            Typed { adapter, .. } => adapter.adapter_type(),
            Parse(state) => state.adapter_type,
        }
    }

    fn metadata_adapter(&self) -> Option<Box<dyn MetadataAdapter>> {
        match &self.inner {
            Typed { adapter, .. } => adapter.metadata_adapter(),
            Parse(_) => None, // TODO: implement metadata_adapter() for ParseAdapter
        }
    }

    fn as_typed_base_adapter(&self) -> &dyn TypedBaseAdapter {
        match &self.inner {
            Typed { adapter, .. } => adapter.as_ref(),
            Parse(_) => unimplemented!("as_typed_base_adapter"),
        }
    }

    fn is_parse(&self) -> bool {
        matches!(&self.inner, Parse(_))
    }

    fn as_replay(&self) -> Option<&dyn ReplayAdapter> {
        match &self.inner {
            Typed { adapter, .. } => adapter.as_replay(),
            Parse(_) => None,
        }
    }

    fn column_type(&self) -> Option<Value> {
        match &self.inner {
            Typed { adapter, .. } => adapter.column_type(),
            Parse(_) => {
                let value = Value::from_object(ColumnStatic::new(self.adapter_type()));
                Some(value)
            }
        }
    }

    fn engine(&self) -> &Arc<AdapterEngine> {
        match &self.inner {
            Typed { adapter, .. } => adapter.engine(),
            Parse(state) => &state.engine,
        }
    }

    fn quoting(&self) -> ResolvedQuoting {
        match &self.inner {
            Typed { adapter, .. } => adapter.quoting(),
            Parse(state) => state.engine.quoting(),
        }
    }

    fn cancellation_token(&self) -> CancellationToken {
        self.engine().cancellation_token()
    }
}

impl BaseAdapter for BridgeAdapter {
    fn as_value(&self) -> Value {
        Value::from_object(self.clone())
    }

    fn new_connection(
        &self,
        state: Option<&State>,
        node_id: Option<String>,
    ) -> Result<Box<dyn Connection>, minijinja::Error> {
        let _span = span!("BrideAdapter::new_connection");
        match &self.inner {
            Typed { adapter, .. } => {
                let conn = adapter.new_connection(state, node_id)?;
                Ok(conn)
            }
            Parse(_) => unimplemented!("new_connection is not implemented for ParseAdapter"),
        }
    }

    /// Used internally to hydrate the relation cache with the given schema -> relation map
    ///
    /// This operation should be additive and not reset the cache.
    fn update_relation_cache(
        &self,
        schema_to_relations_map: BTreeMap<CatalogAndSchema, RelationVec>,
    ) -> FsResult<()> {
        match &self.inner {
            Typed { .. } => {
                schema_to_relations_map
                    .into_iter()
                    .for_each(|(schema, relations)| {
                        self.engine()
                            .relation_cache()
                            .insert_schema(schema, relations)
                    });
                Ok(())
            }
            Parse(_) => Ok(()),
        }
    }

    fn is_cached(&self, relation: &Arc<dyn BaseRelation>) -> bool {
        match &self.inner {
            Typed { .. } => self.engine().relation_cache().contains_relation(relation),
            Parse(_) => false,
        }
    }

    fn is_already_fully_cached(&self, schema: &CatalogAndSchema) -> bool {
        match &self.inner {
            Typed { .. } => self.engine().relation_cache().contains_full_schema(schema),
            Parse(_) => false,
        }
    }

    #[tracing::instrument(skip_all, level = "trace")]
    fn cache_added(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => adapter.cache_added(state, relation.clone()),
            // TODO(jason): We should probably capture any manual user engagement with the cache
            // and use this knowledge for our cache hydration
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn cache_dropped(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => adapter.cache_dropped(state, relation),
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn cache_renamed(
        &self,
        state: &State,
        from_relation: Arc<dyn BaseRelation>,
        to_relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => adapter.cache_renamed(state, from_relation, to_relation),
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip_all, level = "trace")]
    fn standardize_grants_dict(
        &self,
        _state: &State,
        grants_table: &Arc<AgateTable>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => Ok(Value::from(
                adapter.standardize_grants_dict(grants_table.clone())?,
            )),
            Parse(_) => unreachable!(
                "standardize_grants_dict should be handled in dispatch for ParseAdapter"
            ),
        }
    }

    #[tracing::instrument(skip_all, level = "trace")]
    fn quote(&self, _state: &State, identifier: &str) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let quoted_identifier = adapter.quote(identifier);
                Ok(Value::from(quoted_identifier))
            }
            Parse(_) => Ok(empty_vec_value()),
        }
    }

    #[tracing::instrument(skip_all, level = "trace")]
    fn quote_as_configured(
        &self,
        state: &State,
        identifier: &str,
        quote_key: &str,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
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

    #[tracing::instrument(skip_all, level = "trace")]
    fn quote_seed_column(
        &self,
        state: &State,
        column: &str,
        quote_config: Option<bool>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let result = adapter.quote_seed_column(state, column, quote_config)?;
                Ok(Value::from(result))
            }
            Parse(_) => Ok(empty_string_value()),
        }
    }

    #[tracing::instrument(skip_all, level = "trace")]
    fn convert_type(
        &self,
        state: &State,
        table: &Arc<AgateTable>,
        col_idx: i64,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let result = adapter.convert_type(state, table.clone(), col_idx)?;
                Ok(Value::from(result))
            }
            Parse(_) => Ok(empty_string_value()),
        }
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L1839-L1840
    #[tracing::instrument(skip_all, level = "trace")]
    fn render_raw_model_constraints(
        &self,
        state: &State,
        raw_constraints: &[ModelConstraint],
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                if let Some(replay_adapter) = adapter.as_replay() {
                    return replay_adapter
                        .replay_render_raw_model_constraints(state, raw_constraints);
                }
                let mut result = vec![];
                for constraint in raw_constraints {
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

    #[tracing::instrument(skip_all, level = "trace")]
    fn render_raw_columns_constraints(
        &self,
        state: &State,
        raw_columns: &Value,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
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

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn execute(
        &self,
        state: &State,
        sql: &str,
        auto_begin: bool,
        fetch: bool,
        limit: Option<i64>,
        options: Option<HashMap<String, String>>,
    ) -> AdapterResult<(AdapterResponse, AgateTable)> {
        match &self.inner {
            Typed { adapter, .. } => {
                let mut conn =
                    self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let ctx = query_ctx_from_state(state)?.with_desc("execute adapter call");
                let (response, table) = adapter.execute(
                    Some(state),
                    conn.as_mut(),
                    &ctx,
                    sql,
                    auto_begin,
                    fetch,
                    limit,
                    options,
                )?;
                Ok((response, table))
            }
            Parse(parse_state) => {
                let response = AdapterResponse::default();
                let table = AgateTable::default();

                if state.is_execute() {
                    if let Some(unique_id) = state.lookup(TARGET_UNIQUE_ID) {
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

    #[tracing::instrument(skip(self, state, bindings), level = "trace")]
    fn add_query(
        &self,
        state: &State,
        sql: &str,
        auto_begin: bool,
        bindings: Option<&Value>,
        abridge_sql_log: bool,
    ) -> AdapterResult<()> {
        match &self.inner {
            Typed { adapter, .. } => {
                let adapter_type = adapter.adapter_type();
                let formatted_sql = if let Some(bindings) = bindings {
                    format_sql_with_bindings(adapter_type, sql, bindings)?
                } else {
                    sql.to_string()
                };

                let mut conn =
                    self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let ctx = query_ctx_from_state(state)?.with_desc("add_query adapter call");

                adapter.add_query(
                    &ctx,
                    conn.as_mut(),
                    &formatted_sql,
                    auto_begin,
                    bindings,
                    abridge_sql_log,
                )?;
                Ok(())
            }
            Parse(_) => Ok(()),
        }
    }

    fn submit_python_job(
        &self,
        state: &State,
        model: &Value,
        compiled_code: &str,
    ) -> AdapterResult<AdapterResponse> {
        match &self.inner {
            Typed { adapter, .. } => {
                let mut conn =
                    self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let ctx = query_ctx_from_state(state)?.with_desc("submit_python_job adapter call");

                adapter.submit_python_job(&ctx, conn.as_mut(), state, model, compiled_code)
            }
            Parse(_) => {
                // Python models cannot be executed during parse phase
                Err(AdapterError::new(
                    AdapterErrorKind::NotSupported,
                    "submit_python_job can only be called in materialization macros",
                ))
            }
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn drop_relation(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                self.engine().relation_cache().evict_relation(&relation);
                Ok(adapter.drop_relation(state, relation)?)
            }
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn truncate_relation(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => Ok(adapter.truncate_relation(state, relation)?),
            Parse(_) => Ok(none_value()),
        }
    }

    // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/include/global_project/macros/relations/rename.sql
    #[tracing::instrument(skip(self, state), level = "trace")]
    fn rename_relation(
        &self,
        state: &State,
        from_relation: Arc<dyn BaseRelation>,
        to_relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                // Update cache
                self.cache_renamed(state, from_relation.clone(), to_relation.clone())?;

                adapter.rename_relation(state, from_relation.clone(), to_relation.clone())?;
                Ok(Value::from(()))
            }
            Parse(_) => Ok(none_value()),
        }
    }

    /// Expand the to_relation table's column types to match the schema of from_relation.
    /// https://docs.getdbt.com/reference/dbt-jinja-functions/adapter#expand_target_column_types
    #[tracing::instrument(skip(self, state), level = "trace")]
    fn expand_target_column_types(
        &self,
        state: &State,
        from_relation: Arc<dyn BaseRelation>,
        to_relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let result =
                    adapter.expand_target_column_types(state, from_relation, to_relation)?;
                Ok(result)
            }
            Parse(_) => Ok(none_value()),
        }
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/sql/impl.py#L212-L213
    #[tracing::instrument(skip(self, state), level = "trace")]
    fn list_schemas(&self, state: &State, database: &str) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let kwargs = Kwargs::from_iter([("database", Value::from(database))]);

                let result = execute_macro_wrapper(state, &[Value::from(kwargs)], "list_schemas")?;
                let result = adapter.list_schemas(result)?;

                Ok(Value::from_iter(result))
            }
            Parse(_) => Ok(empty_vec_value()),
        }
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/sql/impl.py#L161
    #[tracing::instrument(skip(self, state), level = "trace")]
    fn create_schema(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => adapter.create_schema(state, relation),
            Parse(_) => Ok(none_value()),
        }
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/sql/impl.py#L172-L173
    #[tracing::instrument(skip(self, state), level = "trace")]
    fn drop_schema(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => adapter.drop_schema(state, relation),
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    #[allow(clippy::used_underscore_binding)]
    fn valid_snapshot_target(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => adapter.valid_snapshot_target(state, relation),
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn get_incremental_strategy_macro(
        &self,
        state: &State,
        strategy: &str,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => adapter.get_incremental_strategy_macro(state, strategy),
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn assert_valid_snapshot_target_given_strategy(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
        column_names: Option<&BTreeMap<String, String>>,
        strategy: &Arc<SnapshotStrategy>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                adapter.assert_valid_snapshot_target_given_strategy(
                    state,
                    relation,
                    column_names.cloned(),
                    strategy.clone(),
                )?;
                Ok(none_value())
            }
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip(self, _state), level = "trace")]
    fn get_hard_deletes_behavior(
        &self,
        _state: &State,
        config: BTreeMap<String, Value>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => Ok(Value::from(adapter.get_hard_deletes_behavior(config)?)),
            // For parse adapter, always return "ignore" as default behavior
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn get_relation(
        &self,
        state: &State,
        database: &str,
        schema: &str,
        identifier: &str,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                // Skip cache in replay mode
                let is_replay = adapter.as_replay().is_some();
                if !is_replay {
                    let temp_relation = crate::relation::do_create_relation(
                        adapter.adapter_type(),
                        database.to_string(),
                        schema.to_string(),
                        Some(identifier.to_string()),
                        None,
                        adapter.quoting(),
                    )?;

                    if let Some(cached_entry) =
                        self.engine().relation_cache().get_relation(&temp_relation)
                    {
                        return Ok(cached_entry.relation().as_value());
                    }
                    // If we have captured the entire schema previously, we can check for non-existence
                    // In these cases, return early with a None value
                    else if self
                        .engine()
                        .relation_cache()
                        .contains_full_schema_for_relation(&temp_relation)
                    {
                        return Ok(none_value());
                    }

                    let mut conn =
                        self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                    let db_schema = CatalogAndSchema::from(&temp_relation);
                    let query_ctx = query_ctx_from_state(state)?
                        .with_desc("get_relation > list_relations call");
                    let maybe_relations_list =
                        adapter.list_relations(&query_ctx, conn.as_mut(), &db_schema);

                    // TODO(jason): We are ignoring this optimization in the logging
                    // this needs to be reported somewhere
                    if let Ok(relations_list) = maybe_relations_list {
                        let _ = self
                            .update_relation_cache(BTreeMap::from([(db_schema, relations_list)]));

                        // After calling list_relations_without_caching, the cache should be populated
                        // with the full schema.
                        if let Some(cached_entry) =
                            self.engine().relation_cache().get_relation(&temp_relation)
                        {
                            return Ok(cached_entry.relation().as_value());
                        } else {
                            return Ok(none_value());
                        }
                    }
                }

                // TODO(jason): Adjust replay mode to be integrated with the cache
                // Move on to query against the remote when we have:
                // 1. A cache miss and we failed to execute list_relations
                // 2. The schema was not previously cached
                let mut conn =
                    self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let query_ctx = query_ctx_from_state(state)?.with_desc("get_relation adapter call");
                let relation = adapter.get_relation(
                    state,
                    &query_ctx,
                    conn.as_mut(),
                    database,
                    schema,
                    identifier,
                )?;
                match relation {
                    Some(relation) => {
                        // cache found relation
                        self.engine()
                            .relation_cache()
                            .insert_relation(relation.clone(), None);
                        Ok(relation.as_value())
                    }
                    None => Ok(none_value()),
                }
            }
            Parse(adapter_parse_state) => {
                adapter_parse_state
                    .record_get_relation_call(state, database, schema, identifier)?;
                Ok(RelationObject::new(Arc::new(EmptyRelation {})).into_value())
            }
        }
    }

    #[tracing::instrument(skip(self), level = "trace")]
    fn build_catalog_relation(&self, model: &Value) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let relation = adapter.build_catalog_relation(model)?;
                Ok(Value::from_object(relation))
            }
            Parse(parse_adapter_state) => {
                let relation = CatalogRelation::from_model_config_and_catalogs(
                    &parse_adapter_state.adapter_type,
                    model,
                    parse_adapter_state.catalogs.clone(),
                )?;
                Ok(Value::from_object(relation))
            }
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn get_missing_columns(
        &self,
        state: &State,
        from_relation: Arc<dyn BaseRelation>,
        to_relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let result = adapter.get_missing_columns(state, from_relation, to_relation)?;
                Ok(Value::from_object(result))
            }
            Parse(_) => Ok(empty_vec_value()),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn get_columns_in_relation(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                // Check if the relation being queried is the same as the one currently being rendered
                // Skip local compilation results for the current relation since the compiled sql
                // may represent a schema that the model will have when the run is done, not the current state
                let is_current_relation = self.matches_current_relation(state, &relation);

                let maybe_from_cache = if !is_current_relation {
                    self.get_schema_from_cache(&relation)
                } else {
                    None
                };

                // Convert Arrow schemas to dbt Columns
                let maybe_from_local = if let Some(schema) = &maybe_from_cache {
                    let from_local =
                        adapter.schema_to_columns(schema.original(), schema.inner())?;

                    #[cfg(debug_assertions)]
                    debug_compare_column_types(
                        state,
                        relation.clone(),
                        adapter.as_ref(),
                        from_local.clone(),
                    );

                    Some(from_local)
                } else {
                    None
                };

                // Replay Mode: Re-use recordings and compare with cache result
                if let Some(replay_adapter) = adapter.as_replay() {
                    return replay_adapter.replay_get_columns_in_relation(
                        state,
                        relation,
                        maybe_from_local,
                    );
                }

                // Cache Hit: Re-use values
                if let Some(from_local) = maybe_from_local {
                    return Ok(Value::from(from_local));
                }

                // Cache Miss: Issue warehouse specific behavior to fetch columns
                let from_remote = adapter.get_columns_in_relation(state, relation)?;

                Ok(Value::from(from_remote))
            }
            Parse(parse_adapter_state) => {
                parse_adapter_state.record_get_columns_in_relation_call(state, relation)?;
                Ok(empty_vec_value())
            }
        }
    }

    #[tracing::instrument(skip_all, level = "trace")]
    fn check_schema_exists(
        &self,
        state: &State,
        database: &str,
        schema: &str,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => adapter.check_schema_exists(state, database, schema),
            Parse(_) => Ok(Value::from(true)),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn get_relations_by_pattern(
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

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn get_column_schema_from_query(
        &self,
        state: &State,
        sql: &str,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let ctx = query_ctx_from_state(state)?
                    .with_desc("get_column_schema_from_query adapter call");
                let mut conn =
                    self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let result =
                    adapter.get_column_schema_from_query(state, conn.as_mut(), &ctx, sql)?;
                Ok(Value::from(result))
            }
            Parse(_) => Ok(empty_map_value()),
        }
    }

    /// reference: https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L443-L444
    /// Shares the same input and output as get_column_schema_from_query.
    /// FIXME(harry): unlike get_column_schema_from_query which only works when returning a non-empty result
    /// get_columns_in_select_sql returns a schema using the BigQuery Job and GetTable APIs
    #[tracing::instrument(skip(self, state), level = "trace")]
    fn get_columns_in_select_sql(
        &self,
        state: &State,
        sql: &str,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let ctx = query_ctx_from_state(state)?
                    .with_desc("get_column_schema_from_query adapter call");
                let mut conn =
                    self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let result =
                    adapter.get_column_schema_from_query(state, conn.as_mut(), &ctx, sql)?;
                Ok(Value::from(result))
            }
            Parse(_) => Ok(empty_map_value()),
        }
    }

    #[tracing::instrument(skip(self, _state), level = "trace")]
    fn verify_database(&self, _state: &State, database: String) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let result = adapter.verify_database(database);
                Ok(result?)
            }
            Parse(_) => Ok(Value::from(false)),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn nest_column_data_types(
        &self,
        state: &State,
        columns: &Value,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => adapter.nest_column_data_types(state, columns),
            Parse(_) => Ok(empty_map_value()),
        }
    }

    #[tracing::instrument(skip(self), level = "trace")]
    #[allow(clippy::used_underscore_binding)]
    fn get_bq_table(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => adapter.get_bq_table(state, relation),
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn is_replaceable(
        &self,
        state: &State,
        relation: Option<Arc<dyn BaseRelation>>,
        partition_by: Option<BigqueryPartitionConfig>,
        cluster_by: Option<BigqueryClusterConfig>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let relation = match relation {
                    None => return Ok(Value::from(true)),
                    Some(r) => r,
                };

                let mut conn =
                    self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let result = adapter.is_replaceable(
                    conn.as_mut(),
                    relation,
                    partition_by,
                    cluster_by,
                    Some(state),
                )?;
                Ok(Value::from(result))
            }
            Parse(_) => Ok(Value::from(false)),
        }
    }

    fn upload_file(&self, state: &State, args: &[Value]) -> Result<Value, minijinja::Error> {
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
    fn parse_partition_by(
        &self,
        _state: &State,
        raw_partition_by: &Value,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let result = adapter.parse_partition_by(raw_partition_by.clone())?;
                Ok(result)
            }
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn get_table_options(
        &self,
        state: &State,
        config: ModelConfig,
        node: &InternalDbtNodeWrapper,
        temporary: bool,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let options = adapter.get_table_options(state, config, node, temporary)?;
                Ok(Value::from_serialize(options))
            }
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn get_view_options(
        &self,
        state: &State,
        config: ModelConfig,
        node: &InternalDbtNodeWrapper,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let node = node.as_internal_node();
                let options = adapter.get_view_options(state, config, node.common())?;
                Ok(Value::from_serialize(options))
            }
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn get_common_options(
        &self,
        state: &State,
        config: ModelConfig,
        node: &InternalDbtNodeWrapper,
        temporary: bool,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let options = adapter.get_common_options(state, config, node, temporary)?;
                Ok(options)
            }
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip(self, _state), level = "trace")]
    fn add_time_ingestion_partition_column(
        &self,
        _state: &State,
        columns: &Value,
        partition_config: BigqueryPartitionConfig,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let result = adapter
                    .add_time_ingestion_partition_column(columns.clone(), partition_config)?;
                Ok(result)
            }
            Parse(_) => Ok(empty_vec_value()),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn grant_access_to(
        &self,
        state: &State,
        entity: Arc<dyn BaseRelation>,
        entity_type: &str,
        role: Option<&str>,
        database: &str,
        schema: &str,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let mut conn =
                    self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let result = adapter.grant_access_to(
                    state,
                    conn.as_mut(),
                    entity,
                    entity_type,
                    role,
                    database,
                    schema,
                )?;
                Ok(result)
            }
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn get_dataset_location(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let mut conn =
                    self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let result = adapter.get_dataset_location(state, conn.as_mut(), relation)?;
                Ok(Value::from(result))
            }
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn update_table_description(
        &self,
        state: &State,
        database: &str,
        schema: &str,
        identifier: &str,
        description: &str,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let mut conn =
                    self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let result = adapter.update_table_description(
                    state,
                    conn.as_mut(),
                    database,
                    schema,
                    identifier,
                    description,
                )?;
                Ok(result)
            }
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn alter_table_add_columns(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
        columns: &Value,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let mut conn =
                    self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let result = adapter.alter_table_add_columns(
                    state,
                    conn.as_mut(),
                    relation,
                    columns.clone(),
                )?;
                Ok(result)
            }
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn update_columns(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
        columns: IndexMap<String, DbtColumn>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let mut conn =
                    self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let result =
                    adapter.update_columns_descriptions(state, conn.as_mut(), relation, columns)?;
                Ok(result)
            }
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip_all, level = "trace")]
    fn behavior(&self) -> Value {
        match &self.inner {
            Typed { adapter, .. } => {
                let mut behavior_flags = adapter.behavior();
                for flag in DEFAULT_BASE_BEHAVIOR_FLAGS.iter() {
                    behavior_flags.push(flag.clone());
                }
                // TODO: support user overrides (using flags from RuntimeConfig)
                // https://github.com/dbt-labs/dbt-adapters/blob/3ed165d452a0045887a5032c621e605fd5c57447/dbt-adapters/src/dbt/adapters/base/impl.py#L360
                Value::from_object(Behavior::new(&behavior_flags))
            }
            Parse(_) => Value::from_object(Behavior::new(&[])),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn list_relations_without_caching(
        &self,
        state: &State,
        schema_relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let query_ctx = query_ctx_from_state(state)?
                    .with_desc("list_relations_without_caching adapter call");
                let mut conn =
                    self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let result = adapter.list_relations(
                    &query_ctx,
                    conn.as_mut(),
                    &CatalogAndSchema::from(&schema_relation),
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

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn compare_dbr_version(
        &self,
        state: &State,
        major: i64,
        minor: i64,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let mut conn =
                    self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let result = adapter.compare_dbr_version(state, conn.as_mut(), major, minor)?;
                Ok(result)
            }
            Parse(_) => Ok(Value::from(0)),
        }
    }

    #[tracing::instrument(skip_all, level = "trace")]
    fn compute_external_path(
        &self,
        _state: &State,
        config: ModelConfig,
        node: &InternalDbtNodeWrapper,
        is_incremental: bool,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
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

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn update_tblproperties_for_uniform_iceberg(
        &self,
        state: &State,
        config: ModelConfig,
        node: &InternalDbtNodeWrapper,
        tblproperties: Option<Value>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                if adapter.adapter_type() != AdapterType::Databricks {
                    unimplemented!(
                        "update_tblproperties_for_uniform_iceberg is only supported in Databricks"
                    )
                }

                let mut tblproperties = match tblproperties {
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
                    self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                adapter.update_tblproperties_for_uniform_iceberg(
                    state,
                    conn.as_mut(),
                    config,
                    node,
                    &mut tblproperties,
                )?;
                Ok(Value::from_serialize(&tblproperties))
            }
            Parse(_) => Ok(empty_map_value()),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn is_uniform(
        &self,
        state: &State,
        config: ModelConfig,
        node: &InternalDbtNodeWrapper,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                if adapter.adapter_type() != AdapterType::Databricks {
                    unimplemented!("is_uniform is only supported in Databricks")
                }
                let mut conn =
                    self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let result = adapter.is_uniform(state, conn.as_mut(), config, node)?;
                Ok(Value::from(result))
            }
            Parse(_) => Ok(Value::from(false)),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn copy_table(
        &self,
        state: &State,
        tmp_relation_partitioned: Arc<dyn BaseRelation>,
        target_relation_partitioned: Arc<dyn BaseRelation>,
        materialization: &str,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                self.engine()
                    .relation_cache()
                    .insert_relation(target_relation_partitioned.clone(), None);

                let mut conn =
                    self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                adapter.copy_table(
                    state,
                    conn.as_mut(),
                    tmp_relation_partitioned,
                    target_relation_partitioned,
                    materialization.to_string(),
                )?;
                Ok(none_value())
            }
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip(self), level = "trace")]
    fn describe_relation(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let mut conn =
                    self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let result = adapter.describe_relation(conn.as_mut(), relation, Some(state))?;
                Ok(result.map_or_else(none_value, Value::from_serialize))
            }
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip(self, _state), level = "trace")]
    fn generate_unique_temporary_table_suffix(
        &self,
        _state: &State,
        suffix_initial: Option<String>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let suffix = adapter.generate_unique_temporary_table_suffix(suffix_initial)?;

                Ok(Value::from(suffix))
            }
            Parse(_) => Ok(Value::from("")),
        }
    }

    #[tracing::instrument(skip(self, _state), level = "trace")]
    fn valid_incremental_strategies(&self, _state: &State) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => Ok(Value::from_serialize(
                adapter.valid_incremental_strategies(),
            )),
            Parse(_) => Ok(empty_vec_value()),
        }
    }

    #[tracing::instrument(skip(self, _state), level = "trace")]
    fn redact_credentials(&self, _state: &State, sql: &str) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let sql_redacted = adapter.redact_credentials(sql)?;
                Ok(Value::from(sql_redacted))
            }
            Parse(_) => Ok(Value::from("")),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn get_partitions_metadata(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => adapter.get_partitions_metadata(state, relation),
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn get_persist_doc_columns(
        &self,
        state: &State,
        existing_columns: &Value,
        model_columns: &Value,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                adapter.get_persist_doc_columns(state, existing_columns, model_columns)
            }
            Parse(_) => Ok(none_value()),
        }
    }

    fn get_column_tags_from_model(
        &self,
        _state: &State,
        node: &dyn InternalDbtNodeAttributes,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let result = adapter.get_column_tags_from_model(node)?;
                Ok(result)
            }
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip_all, level = "trace")]
    fn get_relation_config(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let mut conn =
                    self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let config = adapter.get_relation_config(state, conn.as_mut(), relation.clone())?;
                Ok(Value::from_object(config))
            }
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip_all, level = "trace")]
    fn get_config_from_model(
        &self,
        _state: &State,
        node: &InternalDbtNodeWrapper,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => Ok(adapter.get_config_from_model(node)?),
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip_all, level = "trace")]
    fn get_relations_without_caching(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => adapter.get_relations_without_caching(state, relation),
            Parse(_) => Ok(empty_vec_value()),
        }
    }

    #[tracing::instrument(skip_all, level = "trace")]
    fn parse_index(&self, state: &State, raw_index: &Value) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => adapter.parse_index(state, raw_index),
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip_all, level = "trace")]
    fn clean_sql(&self, sql: &str) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => Ok(Value::from(adapter.clean_sql(sql)?)),
            Parse(_) => unimplemented!("clean_sql"),
        }
    }

    /// Used internally to attempt executing a Snowflake `use warehouse [name]` statement.
    ///
    /// # Returns
    ///
    /// Returns true if the warehouse was overridden, false otherwise
    #[tracing::instrument(skip(self), level = "trace")]
    fn use_warehouse(&self, warehouse: Option<String>, node_id: &str) -> FsResult<bool> {
        match &self.inner {
            Typed { adapter, .. } => {
                if warehouse.is_none() {
                    return Ok(false);
                }

                let mut conn = self
                    .borrow_tlocal_connection(None, Some(node_id.to_string()))
                    .map_err(|e| FsError::from_jinja_err(e, "Failed to create a connection"))?;
                adapter.use_warehouse(conn.as_mut(), warehouse.unwrap(), node_id)?;
                Ok(true)
            }
            Parse(_) => Ok(false),
        }
    }

    /// Used internally to attempt executing a Snowflake `use warehouse [name]` statement.
    ///
    /// To restore to the warehouse configured in profiles.yml
    #[tracing::instrument(skip(self), level = "trace")]
    fn restore_warehouse(&self, node_id: &str) -> FsResult<()> {
        match &self.inner {
            Typed { adapter, .. } => {
                let mut conn = self
                    .borrow_tlocal_connection(None, Some(node_id.to_string()))
                    .map_err(|e| FsError::from_jinja_err(e, "Failed to create a connection"))?;
                adapter.restore_warehouse(conn.as_mut(), node_id)?;
                Ok(())
            }
            Parse(_) => Ok(()),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn load_dataframe(
        &self,
        state: &State,
        database: &str,
        schema: &str,
        table_name: &str,
        agate_table: Arc<AgateTable>,
        file_path: &str,
        field_delimiter: &str,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let mut conn =
                    self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
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
                    field_delimiter,
                )?;
                Ok(result)
            }
            Parse(_) => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn describe_dynamic_table(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        match &self.inner {
            Typed { adapter, .. } => {
                let mut conn =
                    self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                adapter.describe_dynamic_table(state, conn.as_mut(), relation)
            }
            Parse(_) => {
                let map = [("dynamic_table", none_value())]
                    .into_iter()
                    .collect::<HashMap<_, _>>();
                Ok(Value::from_serialize(map))
            }
        }
    }
}

impl Object for BridgeAdapter {
    fn call_method(
        self: &Arc<Self>,
        state: &State,
        name: &str,
        args: &[Value],
        listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, minijinja::Error> {
        if let Parse(_) = &self.inner {
            return dispatch_adapter_calls(&**self, state, name, args, listeners);
        }
        // NOTE(jason): This function uses the time machine - cross version Fusion snapshot tests
        // not to be confused with conformance ReplayAdapter or RecordEngine/ReplayEngine
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
                minijinja::Error::new(minijinja::ErrorKind::InvalidOperation, e.to_string())
            });
        }

        // Execute the actual adapter call
        // Pre-condition: In Replay mode leaked calls are safe because this adapter should not have an actual connection
        // to the warehouse.
        // If replaying, assert the engine is mock (for safety)
        if let Some(ref tm) = self.time_machine
            && tm.is_replaying()
        {
            assert!(
                self.engine().is_mock(),
                "Replay mode requires mock engine; attempted on non-mock engine which risks leaking queries"
            );
        }
        let result = dispatch_adapter_calls(&**self, state, name, args, listeners);

        // Record if time machine is in recording mode
        if !is_pure_or_cache
            && is_outermost
            && let Some(ref tm) = self.time_machine
        {
            let (result_json, success, error) = match &result {
                Ok(value) => (crate::time_machine::serialize_value(value), true, None),
                Err(e) => (serde_json::Value::Null, false, Some(e.to_string())),
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
        dispatch_adapter_get_value(&**self, key)
    }
}

#[cfg(debug_assertions)]
fn debug_compare_column_types(
    state: &State,
    relation: Arc<dyn BaseRelation>,
    typed_adapter: &dyn TypedBaseAdapter,
    mut from_local: Vec<Column>,
) {
    if std::env::var("DEBUG_COMPARE_LOCAL_REMOTE_COLUMNS_TYPES").is_ok() {
        match typed_adapter.get_columns_in_relation(state, relation.clone()) {
            Ok(mut from_remote) => {
                from_remote.sort_by(|a, b| a.name().cmp(b.name()));

                from_local.sort_by(|a, b| a.name().cmp(b.name()));

                println!("local  remote mismatches");
                if !from_remote.is_empty() {
                    assert_eq!(from_local.len(), from_remote.len());
                    for (local, remote) in from_local.iter().zip(from_remote.iter()) {
                        let mismatch =
                            (local.dtype() != remote.dtype()) || (local.name() != remote.name());
                        if mismatch {
                            println!(
                                "adapter.get_columns_in_relation for {}",
                                relation.semantic_fqn()
                            );
                            println!(
                                "{}:{}  {}:{}",
                                local.name(),
                                local.dtype(),
                                remote.name(),
                                remote.dtype()
                            );
                        }
                    }
                } else {
                    println!("WARNING: from_remote is empty");
                }
            }
            Err(e) => {
                println!("Error getting columns in relation from remote: {e}");
            }
        }
    }
}

mod pri {
    use super::*;

    /// A wrapper around a [Connection] stored in thread-local storage
    ///
    /// The point of this struct is to avoid calling the `Drop` destructor on
    /// the wrapped [Connection] during process exit, which dead locks on
    /// Windows.
    pub(super) struct TlsConnectionContainer(RefCell<Option<Box<dyn Connection>>>);

    impl TlsConnectionContainer {
        pub(super) fn new() -> Self {
            TlsConnectionContainer(RefCell::new(None))
        }

        pub(super) fn replace(&self, conn: Option<Box<dyn Connection>>) {
            let prev = self.take();
            *self.0.borrow_mut() = conn;
            if prev.is_some() {
                // We should avoid nested borrows because they mean we are creating more
                // than one connection when one would be sufficient. But if we reached
                // this branch, we did exactly that (!).
                //
                //     {
                //       let outer_guard = adapter.borrow_tlocal_connection()?;
                //       f(outer_guard.as_mut());  // Pass the conn as ref. GOOD.
                //       {
                //         // We tried to borrow, but a new connection had to
                //         // be created. BAD.
                //         let inner_guard = adapter.borrow_tlocal_connection()?;
                //         ...
                //       }  // Connection from inner_guard returns to CONNECTION.
                //     }  // Connection from outer_guard is returning to CONNECTION,
                //        // but one was already there -- the one from inner_guard.
                //
                // The right choice is to simply drop the innermost connection.
                drop(prev);
                // An assert could be added here to help finding code that creates
                // a connection instead of taking one as a parameter so that the
                // outermost caller can pass the thread-local one by reference.
            }
        }

        pub(super) fn take(&self) -> Option<Box<dyn Connection>> {
            self.0.borrow_mut().take()
        }
    }

    impl Drop for TlsConnectionContainer {
        fn drop(&mut self) {
            std::mem::forget(self.take());
        }
    }
}
