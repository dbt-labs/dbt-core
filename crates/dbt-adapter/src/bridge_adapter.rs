use crate::base_adapter::{AdapterType, AdapterTyping};
use crate::cache::RelationCache;
use crate::cast_util::downcast_value_to_dyn_base_relation;
use crate::column::Column;
use crate::funcs::{
    dispatch_adapter_calls, dispatch_adapter_get_value, execute_macro, execute_macro_wrapper,
    format_sql_with_bindings, none_value,
};
use crate::metadata::*;
use crate::query_ctx::{node_id_from_state, query_ctx_from_state};
use crate::relation_object::RelationObject;
use crate::render_constraint::render_model_constraint;
use crate::snapshots::SnapshotStrategy;
use crate::typed_adapter::TypedBaseAdapter;
use crate::{AdapterResponse, AdapterResult, BaseAdapter, SqlEngine, relation_object};

use dbt_agate::AgateTable;
use dbt_common::behavior_flags::{Behavior, BehaviorFlag};
use dbt_common::cancellation::CancellationToken;
use dbt_common::{FsError, FsResult, current_function_name};
use dbt_schema_store::SchemaStoreTrait;
use dbt_schemas::schemas::common::{DbtIncrementalStrategy, ResolvedQuoting};
use dbt_schemas::schemas::dbt_column::{DbtColumn, DbtColumnRef};
use dbt_schemas::schemas::manifest::{
    BigqueryClusterConfig, BigqueryPartitionConfig, GrantAccessToTarget,
};
use dbt_schemas::schemas::project::ModelConfig;
use dbt_schemas::schemas::properties::ModelConstraint;
use dbt_schemas::schemas::relations::base::{BaseRelation, ComponentName};
use dbt_schemas::schemas::serde::{minijinja_value_to_typed_struct, yml_value_to_minijinja};
use dbt_schemas::schemas::{InternalDbtNodeAttributes, InternalDbtNodeWrapper};
use dbt_xdbc::Connection;
use indexmap::IndexMap;
use minijinja::arg_utils::{ArgParser, check_num_args};
use minijinja::dispatch_object::DispatchObject;
use minijinja::listener::RenderingEventListener;
use minijinja::value::{Kwargs, Object};
use minijinja::{Error as MinijinjaError, ErrorKind as MinijinjaErrorKind, State};
use minijinja::{Value, invalid_argument, invalid_argument_inner};
use tracing;
use tracy_client::span;

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::rc::Rc;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

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

/// Type bridge adapter
///
/// This adapter converts untyped method calls (those that use Value)
/// to typed method calls, which we expect most adapters to implement.
/// As inseperable part of this process, this adapter also checks
/// arguments of all methods, their numbers, and types. This file
/// could be auto generated from a simple specification of each
/// method, but considering that the set of methods is small and
/// limited, such an approach was not taken.
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
#[derive(Clone)]
pub struct BridgeAdapter {
    pub(crate) typed_adapter: Arc<dyn TypedBaseAdapter>,
    #[allow(dead_code)]
    schema_store: Option<Arc<dyn SchemaStoreTrait>>,
    relation_cache: Arc<RelationCache>,
}

impl fmt::Debug for BridgeAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.typed_adapter.fmt(f)
    }
}

impl BridgeAdapter {
    /// Create a new bridge adapter
    pub fn new(
        typed_adapter: Arc<dyn TypedBaseAdapter>,
        db: Option<Arc<dyn SchemaStoreTrait>>,
        relation_cache: Arc<RelationCache>,
    ) -> Self {
        Self {
            typed_adapter,
            schema_store: db,
            relation_cache,
        }
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
    ) -> Result<ConnectionGuard<'_>, MinijinjaError> {
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

    /// Get a reference to the [TypedBaseAdapter]
    pub fn typed_adapter(&self) -> &dyn TypedBaseAdapter {
        self.typed_adapter.as_ref()
    }
}

impl AdapterTyping for BridgeAdapter {
    fn adapter_type(&self) -> AdapterType {
        self.typed_adapter.adapter_type()
    }

    fn as_metadata_adapter(&self) -> Option<&dyn MetadataAdapter> {
        self.typed_adapter.as_metadata_adapter()
    }

    fn as_typed_base_adapter(&self) -> &dyn TypedBaseAdapter {
        self.typed_adapter.as_ref()
    }

    fn column_type(&self) -> Option<Value> {
        self.typed_adapter.column_type()
    }

    fn engine(&self) -> &Arc<SqlEngine> {
        self.typed_adapter.engine()
    }

    fn quoting(&self) -> ResolvedQuoting {
        self.typed_adapter.quoting()
    }

    fn cancellation_token(&self) -> CancellationToken {
        self.typed_adapter.cancellation_token()
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
    ) -> Result<Box<dyn Connection>, MinijinjaError> {
        let _span = span!("BrideAdapter::new_connection");
        let conn = self.typed_adapter.new_connection(state, node_id)?;
        Ok(conn)
    }

    fn update_relation_cache(
        &self,
        schema_to_relations_map: BTreeMap<CatalogAndSchema, RelationVec>,
    ) -> FsResult<()> {
        schema_to_relations_map
            .into_iter()
            .for_each(|(schema, relations)| self.relation_cache.insert_schema(schema, relations));
        Ok(())
    }

    fn is_cached(&self, relation: &Arc<dyn BaseRelation>) -> bool {
        self.relation_cache.contains_relation(relation)
    }

    fn is_already_fully_cached(&self, schema: &CatalogAndSchema) -> bool {
        self.relation_cache.contains_full_schema(schema)
    }

    #[tracing::instrument(skip_all, level = "trace")]
    fn cache_added(
        &self,
        _state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        self.relation_cache.insert_relation(relation, None);
        Ok(none_value())
    }

    #[tracing::instrument(skip(self, _state))]
    fn cache_dropped(
        &self,
        _state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        self.relation_cache.evict_relation(&relation);
        Ok(none_value())
    }

    #[tracing::instrument(skip(self, _state))]
    fn cache_renamed(
        &self,
        _state: &State,
        from_relation: Arc<dyn BaseRelation>,
        to_relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        self.relation_cache
            .rename_relation(&from_relation, to_relation);
        Ok(none_value())
    }

    #[tracing::instrument(skip_all, level = "trace")]
    fn standardize_grants_dict(
        &self,
        _state: &State,
        grants_table: &Arc<AgateTable>,
    ) -> Result<Value, MinijinjaError> {
        Ok(Value::from(
            self.typed_adapter
                .standardize_grants_dict(grants_table.clone())?,
        ))
    }

    #[tracing::instrument(skip_all, level = "trace")]
    fn quote(&self, state: &State, identifier: &str) -> Result<Value, MinijinjaError> {
        let quoted_identifier = self.typed_adapter.quote(state, identifier)?;
        Ok(Value::from(quoted_identifier))
    }

    #[tracing::instrument(skip_all, level = "trace")]
    fn quote_as_configured(
        &self,
        state: &State,
        identifier: &str,
        quote_key: &str,
    ) -> Result<Value, MinijinjaError> {
        let quote_key = quote_key.parse::<ComponentName>().map_err(|_| {
            MinijinjaError::new(
                MinijinjaErrorKind::InvalidArgument,
                "quote_key must be one of: database, schema, identifier",
            )
        })?;

        let result = self
            .typed_adapter
            .quote_as_configured(state, identifier, &quote_key)?;

        Ok(Value::from(result))
    }

    #[tracing::instrument(skip_all, level = "trace")]
    fn quote_seed_column(
        &self,
        state: &State,
        column: &str,
        quote_config: Option<bool>,
    ) -> Result<Value, MinijinjaError> {
        let result = self
            .typed_adapter
            .quote_seed_column(state, column, quote_config)?;
        Ok(Value::from(result))
    }

    #[tracing::instrument(skip_all, level = "trace")]
    fn convert_type(
        &self,
        state: &State,
        table: &Arc<AgateTable>,
        col_idx: i64,
    ) -> Result<Value, MinijinjaError> {
        let result = self
            .typed_adapter
            .convert_type(state, table.clone(), col_idx)?;
        Ok(Value::from(result))
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L1839-L1840
    #[tracing::instrument(skip_all, level = "trace")]
    fn render_raw_model_constraints(
        &self,
        _state: &State,
        raw_constraints: &[ModelConstraint],
    ) -> Result<Value, MinijinjaError> {
        let mut result = vec![];
        for constraint in raw_constraints {
            let rendered = render_model_constraint(self.adapter_type(), constraint.clone());
            if let Some(rendered) = rendered {
                result.push(rendered)
            }
        }
        Ok(Value::from(result))
    }

    #[tracing::instrument(skip_all, level = "trace")]
    fn render_raw_columns_constraints(
        &self,
        _state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;

        let raw_columns = parser.get::<Value>("raw_columns")?;

        let columns = minijinja_value_to_typed_struct::<IndexMap<String, DbtColumn>>(raw_columns)
            .map_err(|e| {
            MinijinjaError::new(MinijinjaErrorKind::SerdeDeserializeError, e.to_string())
        })?;

        let result = self.typed_adapter.render_raw_columns_constraints(columns)?;

        Ok(Value::from(result))
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
        let mut conn = self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
        let ctx = query_ctx_from_state(state)?.with_desc("execute adapter call");
        let (response, table) = self.typed_adapter.execute(
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

    #[tracing::instrument(skip(self, state, bindings), level = "trace")]
    fn add_query(
        &self,
        state: &State,
        sql: &str,
        auto_begin: bool,
        bindings: Option<&Value>,
        abridge_sql_log: bool,
    ) -> AdapterResult<()> {
        let adapter_type = self.typed_adapter.adapter_type();
        let formatted_sql = if let Some(bindings) = bindings {
            format_sql_with_bindings(adapter_type, sql, bindings)?
        } else {
            sql.to_string()
        };

        let mut conn = self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
        let ctx = query_ctx_from_state(state)?.with_desc("add_query adapter call");

        self.typed_adapter.add_query(
            &ctx,
            conn.as_mut(),
            &formatted_sql,
            auto_begin,
            bindings,
            abridge_sql_log,
        )?;
        Ok(())
    }

    fn submit_python_job(
        &self,
        state: &State,
        model: &Value,
        compiled_code: &str,
    ) -> AdapterResult<AdapterResponse> {
        let mut conn = self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
        let ctx = query_ctx_from_state(state)?.with_desc("submit_python_job adapter call");

        self.typed_adapter
            .submit_python_job(&ctx, conn.as_mut(), state, model, compiled_code)
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn drop_relation(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        self.relation_cache.evict_relation(&relation);
        Ok(self.typed_adapter.drop_relation(state, relation)?)
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn truncate_relation(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        Ok(self.typed_adapter.truncate_relation(state, relation)?)
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn rename_relation(
        &self,
        state: &State,
        from_relation: Arc<dyn BaseRelation>,
        to_relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        // Update cache
        self.cache_renamed(state, from_relation.clone(), to_relation.clone())?;
        if self.typed_adapter.as_replay().is_some() {
            // TODO: move this logic to the [ReplayAdapter]
            let mut conn = self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
            self.typed_adapter.rename_relation(
                conn.as_mut(),
                from_relation.clone(),
                to_relation.clone(),
            )?;
        }
        // Execute the macro with the relation objects
        let args = vec![
            RelationObject::new(from_relation).as_value(),
            RelationObject::new(to_relation).as_value(),
        ];
        execute_macro(state, &args, "rename_relation")?;
        Ok(none_value())
    }

    /// Expand the to_relation table's column types to match the schema of from_relation.
    /// https://docs.getdbt.com/reference/dbt-jinja-functions/adapter#expand_target_column_types
    #[tracing::instrument(skip(self, state), level = "trace")]
    fn expand_target_column_types(
        &self,
        state: &State,
        from_relation: Arc<dyn BaseRelation>,
        to_relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        let result =
            self.typed_adapter
                .expand_target_column_types(state, from_relation, to_relation)?;
        Ok(result)
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/sql/impl.py#L212-L213
    #[tracing::instrument(skip(self, state), level = "trace")]
    fn list_schemas(&self, state: &State, database: &str) -> Result<Value, MinijinjaError> {
        let kwargs = Kwargs::from_iter([("database", Value::from(database))]);

        let result = execute_macro_wrapper(state, &[Value::from(kwargs)], "list_schemas")?;
        let result = self.typed_adapter.list_schemas(result)?;

        Ok(Value::from_iter(result))
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/sql/impl.py#L161
    #[tracing::instrument(skip(self, state), level = "trace")]
    fn create_schema(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        let args = [RelationObject::new(relation).into_value()];
        execute_macro(state, &args, "create_schema")?;
        Ok(none_value())
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/sql/impl.py#L172-L173
    #[tracing::instrument(skip(self, state), level = "trace")]
    fn drop_schema(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        self.relation_cache.evict_schema_for_relation(&relation);
        let args = [RelationObject::new(relation).into_value()];
        execute_macro(state, &args, "drop_schema")?;
        Ok(none_value())
    }

    #[tracing::instrument(skip(self, _state), level = "trace")]
    #[allow(clippy::used_underscore_binding)]
    fn valid_snapshot_target(
        &self,
        _state: &State,
        _relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        unimplemented!("valid_snapshot_target")
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn get_incremental_strategy_macro(
        &self,
        state: &State,
        strategy: &str,
    ) -> Result<Value, MinijinjaError> {
        if strategy != "default" {
            let strategy_ = DbtIncrementalStrategy::from_str(strategy)
                .map_err(|e| invalid_argument_inner!("Invalid strategy value {}", e))?;
            if !self
                .typed_adapter()
                .valid_incremental_strategies()
                .contains(&strategy_)
                && builtin_incremental_strategies(false).contains(&strategy_)
            {
                return invalid_argument!(
                    "The incremental strategy '{}' is not valid for this adapter",
                    strategy
                );
            }
        }

        let strategy = strategy.replace("+", "_");
        let macro_name = format!("get_incremental_{strategy}_sql");

        // Return the macro
        Ok(Value::from_object(DispatchObject {
            macro_name,
            package_name: None,
            strict: false,
            auto_execute: false,
            context: Some(state.get_base_context()),
        }))
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn assert_valid_snapshot_target_given_strategy(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
        column_names: Option<&BTreeMap<String, String>>,
        strategy: &Arc<SnapshotStrategy>,
    ) -> Result<Value, MinijinjaError> {
        self.typed_adapter
            .assert_valid_snapshot_target_given_strategy(
                state,
                relation,
                column_names.cloned(),
                strategy.clone(),
            )?;
        Ok(none_value())
    }

    #[tracing::instrument(skip(self, _state), level = "trace")]
    fn get_hard_deletes_behavior(
        &self,
        _state: &State,
        config: BTreeMap<String, Value>,
    ) -> Result<Value, MinijinjaError> {
        Ok(Value::from(
            self.typed_adapter.get_hard_deletes_behavior(config)?,
        ))
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn get_relation(
        &self,
        state: &State,
        database: &str,
        schema: &str,
        identifier: &str,
    ) -> Result<Value, MinijinjaError> {
        // Skip cache in replay mode
        let is_replay = self.typed_adapter.as_replay().is_some();
        if !is_replay {
            let temp_relation = relation_object::create_relation(
                self.typed_adapter.adapter_type(),
                database.to_string(),
                schema.to_string(),
                Some(identifier.to_string()),
                None,
                self.typed_adapter().quoting(),
            )?;

            if let Some(cached_entry) = self.relation_cache.get_relation(&temp_relation) {
                return Ok(cached_entry.relation().as_value());
            }
            // If we have captured the entire schema previously, we can check for non-existence
            // In these cases, return early with a None value
            else if self
                .relation_cache
                .contains_full_schema_for_relation(&temp_relation)
            {
                return Ok(none_value());
            }

            let mut conn = self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
            let db_schema = CatalogAndSchema::from(&temp_relation);
            let query_ctx =
                query_ctx_from_state(state)?.with_desc("get_relation > list_relations call");
            let maybe_relations_list =
                self.typed_adapter
                    .list_relations(&query_ctx, conn.as_mut(), &db_schema);

            // TODO(jason): We are ignoring this optimization in the logging
            // this needs to be reported somewhere
            if let Ok(relations_list) = maybe_relations_list {
                let _ = self.update_relation_cache(BTreeMap::from([(db_schema, relations_list)]));

                // After calling list_relations_without_caching, the cache should be populated
                // with the full schema.
                if let Some(cached_entry) = self.relation_cache.get_relation(&temp_relation) {
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
        let mut conn = self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
        let query_ctx = query_ctx_from_state(state)?.with_desc("get_relation adapter call");
        let relation = self.typed_adapter.get_relation(
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
                self.relation_cache.insert_relation(relation.clone(), None);
                Ok(relation.as_value())
            }
            None => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip(self), level = "trace")]
    fn build_catalog_relation(&self, model: &Value) -> Result<Value, MinijinjaError> {
        Ok(self.typed_adapter.build_catalog_relation(model)?)
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn get_missing_columns(
        &self,
        state: &State,
        from_relation: Arc<dyn BaseRelation>,
        to_relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        let result = self
            .typed_adapter
            .get_missing_columns(state, from_relation, to_relation)?;
        Ok(Value::from_object(result))
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn get_columns_in_relation(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        let maybe_from_local = if let Some(schema_store) = &self.schema_store {
            // Check if the relation being queried is the same as the one currently being rendered
            // Skip local compilation results for the current relation since the compiled sql
            // may represent a schema that the model will have when the run is done, not the current state
            let is_current_relation =
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
                };

            if !is_current_relation {
                let schema =
                    schema_store.get_schema(&relation.get_canonical_fqn().unwrap_or_default());
                if let Some(schema) = &schema {
                    let from_local = self
                        .typed_adapter
                        .schema_to_columns(schema.original(), schema.inner())?;

                    #[cfg(debug_assertions)]
                    {
                        if std::env::var("DEBUG_COMPARE_LOCAL_REMOTE_COLUMNS_TYPES").is_ok() {
                            match self
                                .typed_adapter
                                .get_columns_in_relation(state, relation.clone())
                            {
                                Ok(mut from_remote) => {
                                    from_remote.sort_by(|a, b| a.name().cmp(b.name()));

                                    let mut from_local = from_local.clone();
                                    from_local.sort_by(|a, b| a.name().cmp(b.name()));

                                    println!("local  remote mismatches");
                                    if !from_remote.is_empty() {
                                        assert_eq!(from_local.len(), from_remote.len());
                                        for (local, remote) in
                                            from_local.iter().zip(from_remote.iter())
                                        {
                                            let mismatch = (local.dtype() != remote.dtype())
                                                || (local.name() != remote.name());
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
                    Some(from_local)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        if let Some(replay_adapter) = self.typed_adapter.as_replay() {
            return replay_adapter.replay_get_columns_in_relation(
                state,
                relation,
                maybe_from_local,
            );
        }

        if let Some(from_local) = maybe_from_local {
            return Ok(Value::from(from_local));
        }

        let from_remote = self
            .typed_adapter
            .get_columns_in_relation(state, relation)?;
        Ok(Value::from(from_remote))
    }

    #[tracing::instrument(skip_all, level = "trace")]
    fn check_schema_exists(
        &self,
        state: &State,
        database: &str,
        schema: &str,
    ) -> Result<Value, MinijinjaError> {
        self.typed_adapter
            .check_schema_exists(state, database, schema)
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
    ) -> Result<Value, MinijinjaError> {
        // Validate excluded_schemas if provided
        if let Some(ref schemas) = excluded_schemas {
            let _ =
                minijinja_value_to_typed_struct::<Vec<String>>(schemas.clone()).map_err(|e| {
                    MinijinjaError::new(MinijinjaErrorKind::SerdeDeserializeError, e.to_string())
                })?;
        }

        // Get default database from state if not provided
        let database_str = if let Some(db) = database {
            db.to_string()
        } else {
            let target = state.lookup("target").ok_or_else(|| {
                MinijinjaError::new(
                    MinijinjaErrorKind::InvalidOperation,
                    "target is not set in state",
                )
            })?;
            let db_value = target.get_attr("database").unwrap_or_default();
            db_value.as_str().unwrap_or_default().to_string()
        };

        // Build args array for macro call
        // Note: For optional string parameters like 'exclude', we pass empty string instead of None
        // because the macro expects a string and None gets converted to "none" string
        let args = vec![
            Value::from(schema_pattern),
            Value::from(table_pattern),
            exclude.map(Value::from).unwrap_or_else(|| Value::from("")),
            Value::from(database_str.as_str()),
            quote_table
                .map(Value::from)
                .unwrap_or_else(|| Value::from(false)),
            excluded_schemas.unwrap_or_else(|| Value::from_iter::<Vec<String>>(vec![])),
        ];

        let result = execute_macro(state, &args, "get_relations_by_pattern_internal")?;
        Ok(result)
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn get_column_schema_from_query(
        &self,
        state: &State,
        sql: &str,
    ) -> Result<Value, MinijinjaError> {
        let ctx =
            query_ctx_from_state(state)?.with_desc("get_column_schema_from_query adapter call");
        let mut conn = self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
        let result =
            self.typed_adapter
                .get_column_schema_from_query(state, conn.as_mut(), &ctx, sql)?;
        Ok(Value::from(result))
    }

    /// reference: https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L443-L444
    /// Shares the same input and output as get_column_schema_from_query, simply delegate to the other for now
    /// FIXME(harry): unlike get_column_schema_from_query which only works when returning a non-empty result
    /// get_columns_in_select_sql returns a schema using the BigQuery Job and GetTable APIs
    #[tracing::instrument(skip(self, state), level = "trace")]
    fn get_columns_in_select_sql(&self, state: &State, sql: &str) -> Result<Value, MinijinjaError> {
        self.get_column_schema_from_query(state, sql)
    }

    #[tracing::instrument(skip(self, _state), level = "trace")]
    fn verify_database(&self, _state: &State, database: String) -> Result<Value, MinijinjaError> {
        let result = self.typed_adapter.verify_database(database);
        Ok(result?)
    }

    #[tracing::instrument(skip(self, _state), level = "trace")]
    fn nest_column_data_types(
        &self,
        _state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 2)?;

        // TODO: 'constraints' arg are ignored; didn't find an usage example, implement later
        let columns = parser.get::<Value>("columns")?;

        let columns = minijinja_value_to_typed_struct::<IndexMap<String, DbtColumn>>(columns)
            .map_err(|e| {
                MinijinjaError::new(MinijinjaErrorKind::SerdeDeserializeError, e.to_string())
            })?;

        let nested_columns = self.typed_adapter.nest_column_data_types(columns, None)?;
        let result = IndexMap::<String, Value>::from_iter(
            nested_columns
                .into_iter()
                .map(|(col_name, col)| (col_name, Value::from_serialize(col))),
        );

        Ok(Value::from_object(result))
    }

    #[tracing::instrument(skip(self, _state), level = "trace")]
    fn get_bq_table(&self, _state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;
        unimplemented!("get_bq_table")
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn is_replaceable(
        &self,
        state: &State,
        relation: Option<Arc<dyn BaseRelation>>,
        partition_by: Option<BigqueryPartitionConfig>,
        cluster_by: Option<BigqueryClusterConfig>,
    ) -> Result<Value, MinijinjaError> {
        let relation = match relation {
            None => return Ok(Value::from(true)),
            Some(r) => r,
        };

        let mut conn = self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
        let result =
            self.typed_adapter
                .is_replaceable(conn.as_mut(), relation, partition_by, cluster_by)?;
        Ok(Value::from(result))
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
    ) -> Result<Value, MinijinjaError> {
        let result = self
            .typed_adapter
            .parse_partition_by(raw_partition_by.clone())?;
        Ok(result)
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn get_table_options(
        &self,
        state: &State,
        config: ModelConfig,
        node: &InternalDbtNodeWrapper,
        temporary: bool,
    ) -> Result<Value, MinijinjaError> {
        let options = self
            .typed_adapter
            .get_table_options(state, config, node, temporary)?;
        Ok(Value::from_serialize(options))
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn get_view_options(
        &self,
        state: &State,
        config: ModelConfig,
        node: &InternalDbtNodeWrapper,
    ) -> Result<Value, MinijinjaError> {
        let node = node.as_internal_node();
        let options = self
            .typed_adapter
            .get_view_options(state, config, node.common())?;
        Ok(Value::from_serialize(options))
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn get_common_options(
        &self,
        state: &State,
        config: ModelConfig,
        node: &InternalDbtNodeWrapper,
        temporary: bool,
    ) -> Result<Value, MinijinjaError> {
        use crate::bigquery::adapter::get_common_table_options_value;
        use dbt_common::adapter::AdapterType::Bigquery;

        if self.typed_adapter().adapter_type() == Bigquery {
            let node = node.as_internal_node();
            let options = get_common_table_options_value(state, config, node.common(), temporary);
            Ok(Value::from_serialize(options))
        } else {
            Err(MinijinjaError::new(
                MinijinjaErrorKind::InvalidOperation,
                "get_common_options is only available with BigQuery adapter",
            ))
        }
    }

    #[tracing::instrument(skip(self, _state), level = "trace")]
    fn add_time_ingestion_partition_column(
        &self,
        _state: &State,
        columns: &Value,
        partition_config: BigqueryPartitionConfig,
    ) -> Result<Value, MinijinjaError> {
        let result = self
            .typed_adapter
            .add_time_ingestion_partition_column(columns.clone(), partition_config)?;
        Ok(result)
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn grant_access_to(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 4, 4)?;

        let entity = parser.get::<Value>("entity")?;
        let entity_type = parser.get::<String>("entity_type")?;
        let role = parser.get::<Value>("role")?;
        let grant_target = minijinja_value_to_typed_struct::<GrantAccessToTarget>(
            parser.get::<Value>("grant_target_dict")?,
        )
        .map_err(|e| {
            MinijinjaError::new(MinijinjaErrorKind::SerdeDeserializeError, e.to_string())
        })?;

        let (database, schema) = (
            grant_target.project.as_deref().ok_or_else(|| {
                invalid_argument_inner!("project in a GrantAccessToTarget cannot be empty")
            })?,
            grant_target.dataset.as_deref().ok_or_else(|| {
                invalid_argument_inner!("dataset in a GrantAccessToTarget cannot be empty")
            })?,
        );

        let role = if role.is_none() {
            None
        } else {
            Some(
                role.as_str()
                    .ok_or_else(|| invalid_argument_inner!("role must be a string"))?,
            )
        };
        let mut conn = self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
        let relation = downcast_value_to_dyn_base_relation(&entity)?;
        let result = self.typed_adapter.grant_access_to(
            state,
            conn.as_mut(),
            relation,
            &entity_type,
            role,
            database,
            schema,
        )?;
        Ok(result)
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn get_dataset_location(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;

        let relation = parser.get::<Value>("relation")?;
        let mut conn = self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
        let result = self
            .typed_adapter
            .get_dataset_location(state, conn.as_mut(), relation)?;
        Ok(Value::from(result))
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn update_table_description(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 4, 4)?;

        let database = parser.get::<String>("database")?;
        let schema = parser.get::<String>("schema")?;
        let identifier = parser.get::<String>("identifier")?;
        let description = parser.get::<String>("description")?;

        let mut conn = self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
        let result = self.typed_adapter.update_table_description(
            state,
            conn.as_mut(),
            &database,
            &schema,
            &identifier,
            &description,
        )?;
        Ok(result)
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn alter_table_add_columns(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 2, 2)?;

        let relation = parser.get::<Value>("relation")?;
        let columns = parser.get::<Value>("columns")?;

        let mut conn = self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
        let result =
            self.typed_adapter
                .alter_table_add_columns(state, conn.as_mut(), relation, columns)?;
        Ok(result)
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn update_columns(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 2, 2)?;

        let relation = parser.get::<Value>("relation")?;
        let columns = parser.get::<Value>("columns")?;
        let columns = minijinja_value_to_typed_struct::<IndexMap<String, DbtColumn>>(columns)
            .map_err(|e| {
                MinijinjaError::new(MinijinjaErrorKind::SerdeDeserializeError, e.to_string())
            })?;

        let mut conn = self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
        let result = self.typed_adapter.update_columns_descriptions(
            state,
            conn.as_mut(),
            relation,
            columns,
        )?;
        Ok(result)
    }

    #[tracing::instrument(skip_all, level = "trace")]
    fn behavior(&self) -> Value {
        let mut behavior_flags = self.typed_adapter.behavior();
        for flag in DEFAULT_BASE_BEHAVIOR_FLAGS.iter() {
            behavior_flags.push(flag.clone());
        }
        // TODO: support user overrides (using flags from RuntimeConfig)
        // https://github.com/dbt-labs/dbt-adapters/blob/3ed165d452a0045887a5032c621e605fd5c57447/dbt-adapters/src/dbt/adapters/base/impl.py#L360
        Value::from_object(Behavior::new(&behavior_flags))
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn list_relations_without_caching(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;

        let relation = parser.get::<Value>("schema_relation")?;
        let relation = downcast_value_to_dyn_base_relation(&relation)?;

        let query_ctx =
            query_ctx_from_state(state)?.with_desc("list_relations_without_caching adapter call");
        let mut conn = self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
        let result = self.typed_adapter.list_relations(
            &query_ctx,
            conn.as_mut(),
            &CatalogAndSchema::from(&relation),
        )?;

        Ok(Value::from_object(
            result
                .into_iter()
                .map(|r| RelationObject::new(r).into_value())
                .collect::<Vec<_>>(),
        ))
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn compare_dbr_version(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 2, 2)?;

        let major = parser.get::<i64>("major")?;
        let minor = parser.get::<i64>("minor")?;

        let mut conn = self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
        let result = self
            .typed_adapter
            .compare_dbr_version(state, conn.as_mut(), major, minor)?;
        Ok(result)
    }

    #[tracing::instrument(skip_all, level = "trace")]
    fn compute_external_path(
        &self,
        _state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 2, 3)?;

        let config = parser.get::<Value>("config")?;

        let model = parser.get::<Value>("model")?;
        let is_incremental = parser
            .get_optional::<bool>("is_incremental")
            .unwrap_or_default();

        let config = minijinja_value_to_typed_struct::<ModelConfig>(config).map_err(|e| {
            MinijinjaError::new(MinijinjaErrorKind::SerdeDeserializeError, e.to_string())
        })?;

        let node =
            minijinja_value_to_typed_struct::<InternalDbtNodeWrapper>(model).map_err(|e| {
                MinijinjaError::new(
                    MinijinjaErrorKind::SerdeDeserializeError,
                    format!(
                        "adapter.compute_external_path expected an InternalDbtNodeWrapper: {e}"
                    ),
                )
            })?;

        let result = self.typed_adapter.compute_external_path(
            config,
            node.as_internal_node(),
            is_incremental,
        )?;
        Ok(Value::from(result))
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn update_tblproperties_for_uniform_iceberg(
        &self,
        state: &State,
        config: ModelConfig,
        node: &InternalDbtNodeWrapper,
        tblproperties: Option<Value>,
    ) -> Result<Value, MinijinjaError> {
        if self.adapter_type() != AdapterType::Databricks {
            unimplemented!(
                "update_tblproperties_for_uniform_iceberg is only supported in Databricks"
            )
        }

        let mut tblproperties = match tblproperties {
            Some(v) if !v.is_none() => {
                minijinja_value_to_typed_struct::<BTreeMap<String, Value>>(v).map_err(|e| {
                    MinijinjaError::new(MinijinjaErrorKind::SerdeDeserializeError, e.to_string())
                })?
            }
            _ => config
                .__warehouse_specific_config__
                .tblproperties
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(|(k, v)| (k, yml_value_to_minijinja(v)))
                .collect(),
        };

        let mut conn = self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
        self.typed_adapter
            .update_tblproperties_for_uniform_iceberg(
                state,
                conn.as_mut(),
                config,
                node,
                &mut tblproperties,
            )?;
        Ok(Value::from_serialize(&tblproperties))
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn is_uniform(
        &self,
        state: &State,
        config: ModelConfig,
        node: &InternalDbtNodeWrapper,
    ) -> Result<Value, MinijinjaError> {
        if self.adapter_type() != AdapterType::Databricks {
            unimplemented!("is_uniform is only supported in Databricks")
        }
        let mut conn = self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
        let result = self
            .typed_adapter
            .is_uniform(state, conn.as_mut(), config, node)?;
        Ok(Value::from(result))
    }

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn copy_table(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 3, 3)?;

        // (tmp_relation_partitioned, target_relation_partitioned, "materialization")
        let source = parser.get::<Value>("tmp_relation_partitioned")?;
        let dest = parser.get::<Value>("target_relation_partitioned")?;
        let materialization = parser.get::<String>("materialization")?;

        let source = downcast_value_to_dyn_base_relation(&source)?;
        let dest = downcast_value_to_dyn_base_relation(&dest)?;

        self.relation_cache.insert_relation(dest.clone(), None);

        let mut conn = self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
        self.typed_adapter
            .copy_table(state, conn.as_mut(), source, dest, materialization)?;

        Ok(none_value())
    }

    #[tracing::instrument(skip(self), level = "trace")]
    fn describe_relation(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        let mut conn = self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
        let result = self
            .typed_adapter
            .describe_relation(conn.as_mut(), relation)?;
        Ok(result.map_or_else(none_value, Value::from_serialize))
    }

    #[tracing::instrument(skip(self, _state), level = "trace")]
    fn generate_unique_temporary_table_suffix(
        &self,
        _state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 0, 1)?;

        let suffix_initial = parser.get_optional::<String>("suffix_initial");

        let suffix = self
            .typed_adapter()
            .generate_unique_temporary_table_suffix(suffix_initial)?;

        Ok(Value::from(suffix))
    }

    #[tracing::instrument(skip(self, _state), level = "trace")]
    fn valid_incremental_strategies(
        &self,
        _state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(Value::from_serialize(
            self.typed_adapter.valid_incremental_strategies(),
        ))
    }

    #[tracing::instrument(skip(self, _state), level = "trace")]
    fn redact_credentials(&self, _state: &State, sql: &str) -> Result<Value, MinijinjaError> {
        let sql_redacted = self.typed_adapter().redact_credentials(sql)?;
        Ok(Value::from(sql_redacted))
    }

    #[tracing::instrument(skip(self, _state), level = "trace")]
    fn get_partitions_metadata(
        &self,
        _state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        let _ = relation;
        unimplemented!("get_partitions_metadata")
    }

    #[tracing::instrument(skip(self, _state), level = "trace")]
    fn get_persist_doc_columns(
        &self,
        _state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 2, 2)?;

        let existing_columns = parser.get::<Value>("existing_columns")?;

        let model_columns = parser.get::<Value>("model_columns")?;

        let existing_columns =
            Column::vec_from_jinja_value(AdapterType::Databricks, existing_columns).map_err(
                |e| MinijinjaError::new(MinijinjaErrorKind::SerdeDeserializeError, e.to_string()),
            )?;
        let model_columns =
            minijinja_value_to_typed_struct::<IndexMap<String, DbtColumnRef>>(model_columns)
                .map_err(|e| {
                    MinijinjaError::new(MinijinjaErrorKind::SerdeDeserializeError, e.to_string())
                })?;

        let persist_doc_columns = self
            .typed_adapter
            .get_persist_doc_columns(existing_columns, model_columns)?;

        let result = IndexMap::from_iter(
            persist_doc_columns
                .into_iter()
                .map(|(col_name, col)| (col_name, Value::from_serialize(col))),
        );

        Ok(Value::from_object(result))
    }

    fn get_column_tags_from_model(
        &self,
        _state: &State,
        node: &dyn InternalDbtNodeAttributes,
    ) -> Result<Value, MinijinjaError> {
        let result = self.typed_adapter.get_column_tags_from_model(node)?;
        Ok(result)
    }

    #[tracing::instrument(skip_all, level = "trace")]
    fn get_relation_config(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;

        let relation = parser.get::<Value>("relation")?;
        let relation = downcast_value_to_dyn_base_relation(&relation)?;

        let mut conn = self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
        let config =
            self.typed_adapter
                .get_relation_config(state, conn.as_mut(), relation.clone())?;
        let value = config.to_value();
        Ok(value)
    }

    #[tracing::instrument(skip_all, level = "trace")]
    fn get_config_from_model(
        &self,
        _state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;

        let model = parser.get::<Value>("model")?;

        let deserialized_node = minijinja_value_to_typed_struct::<InternalDbtNodeWrapper>(model)
            .map_err(|e| {
                MinijinjaError::new(
                    MinijinjaErrorKind::SerdeDeserializeError,
                    format!(
                        "adapter.get_config_from_model expected an InternalDbtNodeWrapper: {e}"
                    ),
                )
            })?;

        Ok(self
            .typed_adapter
            .get_config_from_model(deserialized_node.as_internal_node())?)
    }

    #[tracing::instrument(skip_all, level = "trace")]
    fn get_relations_without_caching(
        &self,
        _state: &State,
        _relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        unimplemented!("get_relations_without_caching")
    }

    #[tracing::instrument(skip_all, level = "trace")]
    fn parse_index(&self, _state: &State, _raw_index: &Value) -> Result<Value, MinijinjaError> {
        unimplemented!("parse_index")
    }

    #[tracing::instrument(skip_all, level = "trace")]
    fn clean_sql(&self, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;

        let sql = parser.get::<String>("sql")?;

        Ok(Value::from(self.typed_adapter.clean_sql(&sql)?))
    }

    #[tracing::instrument(skip(self), level = "trace")]
    fn use_warehouse(&self, warehouse: Option<String>, node_id: &str) -> FsResult<bool> {
        if warehouse.is_none() {
            return Ok(false);
        }

        let mut conn = self
            .borrow_tlocal_connection(None, Some(node_id.to_string()))
            .map_err(|e| FsError::from_jinja_err(e, "Failed to create a connection"))?;
        self.typed_adapter
            .use_warehouse(conn.as_mut(), warehouse.unwrap(), node_id)?;
        Ok(true)
    }

    #[tracing::instrument(skip(self), level = "trace")]
    fn restore_warehouse(&self, node_id: &str) -> FsResult<()> {
        let mut conn = self
            .borrow_tlocal_connection(None, Some(node_id.to_string()))
            .map_err(|e| FsError::from_jinja_err(e, "Failed to create a connection"))?;
        self.typed_adapter
            .restore_warehouse(conn.as_mut(), node_id)?;
        Ok(())
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
    ) -> Result<Value, MinijinjaError> {
        let mut conn = self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
        let ctx = query_ctx_from_state(state)?.with_desc("load_dataframe");
        let sql = "";
        let result = self.typed_adapter.load_dataframe(
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

    #[tracing::instrument(skip(self, state), level = "trace")]
    fn describe_dynamic_table(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        let mut conn = self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
        self.typed_adapter
            .describe_dynamic_table(state, conn.as_mut(), relation)
    }
}

impl fmt::Display for BridgeAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BridgeAdapter({})", self.adapter_type())
    }
}

impl Object for BridgeAdapter {
    fn call_method(
        self: &Arc<Self>,
        state: &State,
        name: &str,
        args: &[Value],
        listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, MinijinjaError> {
        dispatch_adapter_calls(&**self, state, name, args, listeners)
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        dispatch_adapter_get_value(&**self, key)
    }
}

/// List of possible builtin strategies for adapters
/// Microbatch is added by _default_. It is only not added when the behavior flag
/// `require_batched_execution_for_custom_microbatch_strategy` is True.
/// TODO: come back when Behavior is implemented
/// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L1690-L1691
fn builtin_incremental_strategies(
    require_batched_execution_for_custom_microbatch_strategy: bool,
) -> Vec<DbtIncrementalStrategy> {
    let mut result = vec![
        DbtIncrementalStrategy::Append,
        DbtIncrementalStrategy::DeleteInsert,
        DbtIncrementalStrategy::Merge,
        DbtIncrementalStrategy::InsertOverwrite,
    ];
    if require_batched_execution_for_custom_microbatch_strategy {
        result.push(DbtIncrementalStrategy::Microbatch)
    }
    result
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
