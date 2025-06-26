use crate::base_adapter::{AdapterType, AdapterTyping};
use crate::cast_util::{downcast_value_to_dyn_base_relation, dyn_base_columns_to_value};
use crate::databricks::relation::DEFAULT_DATABRICKS_DATABASE;
use crate::formatter::create_sql_literal_formatter;
use crate::funcs::{
    dispatch_adapter_calls, dispatch_adapter_get_value, execute_macro, execute_macro_wrapper,
    none_value,
};
use crate::funcs::{execute_macro_wrapper_with_package, format_sql_with_bindings};
use crate::information_schema::InformationSchema;
use crate::metadata::MetadataAdapter;
use crate::query_ctx::{query_ctx_from_state, query_ctx_from_state_with_sql};
use crate::record_batch_utils::extract_first_value_as_i64;
use crate::render_constraint::render_model_constraint;
use crate::snapshots::SnapshotStrategy;
use crate::typed_adapter::TypedBaseAdapter;
use crate::{BaseAdapter, SqlEngine};

use dbt_agate::AgateTable;
use dbt_common::adapter::SchemaRegistry;
use dbt_common::behavior_flags::{Behavior, BehaviorFlag};
use dbt_common::{current_function_name, FsError, FsResult};
use dbt_schemas::schemas::columns::base::StdColumn;
use dbt_schemas::schemas::common::{DbtIncrementalStrategy, ResolvedQuoting};
use dbt_schemas::schemas::dbt_column::DbtColumn;
use dbt_schemas::schemas::manifest::{
    BigqueryClusterConfig, BigqueryPartitionConfig, BigqueryPartitionConfigLegacy,
    GrantAccessToTarget,
};
use dbt_schemas::schemas::project::ModelConfig;
use dbt_schemas::schemas::properties::ModelConstraint;
use dbt_schemas::schemas::relations::base::{BaseRelation, ComponentName};
use dbt_schemas::schemas::InternalDbtNodeWrapper;
use dbt_xdbc::Connection;
use minijinja::arg_utils::{check_num_args, ArgParser};
use minijinja::dispatch_object::DispatchObject;
use minijinja::listener::RenderingEventListener;
use minijinja::value::{Kwargs, Object};
use minijinja::{invalid_argument, invalid_argument_inner, jinja_err, Value};
use minijinja::{Error as MinijinjaError, ErrorKind as MinijinjaErrorKind, State};
use serde::Deserialize;
use tracing;
use tracy_client::span;

use std::cell::RefCell;
use std::collections::BTreeMap;
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
    static CONNECTION: RefCell<Option<Box<dyn Connection>>> = RefCell::new(None);
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
        let prev = CONNECTION.replace(conn);
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
    db: Option<Arc<dyn SchemaRegistry>>,
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
        db: Option<Arc<dyn SchemaRegistry>>,
    ) -> Self {
        Self { typed_adapter, db }
    }

    /// Borrow the current thread-local connection or create one if it's not set yet.
    ///
    /// A guard is returned. When destroyed, the guard returns the connection to
    /// the thread-local variable. If another connection became the thread-local
    /// in the mean time, that connection is dropped and the return proceeds as
    /// normal.
    pub(crate) fn borrow_tlocal_connection(&self) -> Result<ConnectionGuard<'_>, MinijinjaError> {
        let _span = span!("BridgeAdapter::borrow_thread_local_connection");
        let mut conn = CONNECTION.take();
        if conn.is_none() {
            self.new_connection()
                .map(|new_conn| conn.replace(new_conn))?;
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

    fn engine(&self) -> Option<&Arc<SqlEngine>> {
        self.typed_adapter.engine()
    }

    fn quoting(&self) -> ResolvedQuoting {
        self.typed_adapter.quoting()
    }
}

impl BaseAdapter for BridgeAdapter {
    fn as_value(&self) -> Value {
        Value::from_object(self.clone())
    }

    fn new_connection(&self) -> Result<Box<dyn Connection>, MinijinjaError> {
        let _span = span!("BrideAdapter::new_connection");
        let conn = self.typed_adapter.new_connection()?;
        Ok(conn)
    }

    /// TODO (alex): THIS IS NOT AN ACTUAL IMPLEMENTATION
    #[tracing::instrument(skip(self, _state))]
    fn cache_added(&self, _state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser: ArgParser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;
        let relation = parser.get::<Value>("relation")?;
        if relation.as_object().is_some() {
            let _relation = downcast_value_to_dyn_base_relation(relation)?;
            // TODO (alex): Determine where the cache should live
            return Ok(Value::from(""));
        }
        invalid_argument!("cache_added expects a relation")
    }

    #[tracing::instrument(skip(self, _state, _args))]
    fn cache_dropped(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("cache_dropped")
    }

    #[tracing::instrument(skip(self, _state, _args))]
    fn cache_renamed(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("cache_renamed")
    }

    #[tracing::instrument(skip(self, _state))]
    fn standardize_grants_dict(
        &self,
        _state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;

        let grants_table = parser.get::<Value>("grants_table")?;

        if let Some(grants_table) = grants_table.downcast_object::<AgateTable>() {
            Ok(Value::from(
                self.typed_adapter.standardize_grants_dict(grants_table)?,
            ))
        } else {
            invalid_argument!("grants_table must be of type AgateTable")
        }
    }

    #[tracing::instrument(skip(self, _state))]
    fn quote(&self, _state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;

        let identifier = args
            .first()
            .expect("quote requires exactly one argument")
            .to_string();

        let quoted_identifier = self.typed_adapter.quote(&identifier);
        Ok(Value::from(quoted_identifier))
    }

    #[tracing::instrument(skip(self, _state))]
    fn quote_as_configured(&self, _state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 2, 2)?;

        let identifier = args
            .first()
            .expect("quote_as_configured requires two arguments")
            .as_str()
            .unwrap();

        let quote_key = args
            .last()
            .expect("quote_as_configured requires two arguments")
            .as_str()
            .unwrap();

        let quote_key = quote_key.parse::<ComponentName>().map_err(|_| {
            MinijinjaError::new(
                MinijinjaErrorKind::InvalidArgument,
                "quote_key must be one of: database, schema, identifier",
            )
        })?;

        let result = self
            .typed_adapter
            .quote_as_configured(identifier, &quote_key);

        Ok(Value::from(result))
    }

    #[tracing::instrument(skip(self, _state))]
    fn quote_seed_column(&self, _state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        // column: str, quote_config: Optional[bool]
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 2)?;

        let column = parser.get::<String>("column")?;
        let quote_config = parser.get_optional::<bool>("quote_config");

        let result = self.typed_adapter.quote_seed_column(&column, quote_config);
        Ok(Value::from(result))
    }

    #[tracing::instrument(skip(self, _state))]
    fn convert_type(&self, _state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 2, 2)?;

        let table = args.first().expect("agate_table");
        let table = table.downcast_object::<AgateTable>().unwrap();

        let col_idx = args.last().expect("col_idx");
        let col_idx = col_idx.as_i64().unwrap();

        let result = self.typed_adapter.convert_type(table, col_idx)?;

        Ok(Value::from(result))
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L1839-L1840
    #[tracing::instrument(skip(self, _state))]
    fn render_raw_model_constraints(
        &self,
        _state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;

        let raw_constraints = parser.get::<Value>("raw_constraints")?;

        let constraints = <Vec<ModelConstraint>>::deserialize(raw_constraints)?;
        let mut result = vec![];
        for constraint in constraints {
            let rendered = render_model_constraint(self.adapter_type(), constraint);
            if let Some(rendered) = rendered {
                result.push(rendered)
            }
        }
        Ok(Value::from(result))
    }

    #[tracing::instrument(skip(self, _state))]
    fn render_raw_columns_constraints(
        &self,
        _state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;

        let raw_columns = parser.get::<Value>("raw_columns")?;

        let columns_map = BTreeMap::<String, DbtColumn>::deserialize(raw_columns)?;
        let result = self
            .typed_adapter
            .render_raw_columns_constraints(columns_map)?;
        Ok(Value::from(result))
    }

    #[tracing::instrument(skip(self, state))]
    fn execute(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 4)?;

        let sql = parser.get::<String>("sql")?;
        let auto_begin = parser.get_optional::<bool>("auto_begin");
        let fetch = parser.get_optional::<bool>("fetch");
        let limit = parser.get_optional::<u32>("limit");

        let mut conn = self.borrow_tlocal_connection()?;
        let query_ctx =
            query_ctx_from_state_with_sql(state, sql)?.with_desc("execute adapter call");
        let (response, table) =
            self.typed_adapter
                .execute(conn.as_mut(), &query_ctx, auto_begin, fetch, limit)?;
        Ok(Value::from_iter([
            Value::from_object(response),
            Value::from_object(table),
        ]))
    }

    #[tracing::instrument(skip(self, state))]
    fn add_query(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);

        let sql = parser.get::<String>("sql")?;
        let auto_begin = parser.get_optional::<bool>("auto_begin");
        let bindings = parser.get_optional::<Value>("bindings");
        let abridge_sql_log = parser.get_optional::<bool>("abridge_sql_log");

        let adapter_type = self.typed_adapter.adapter_type();
        let formatter = create_sql_literal_formatter(adapter_type);

        let formatted_sql = if let Some(bindings) = bindings {
            format_sql_with_bindings(&sql, &bindings, formatter)?
        } else {
            sql
        };

        let mut conn = self.borrow_tlocal_connection()?;
        let query_ctx = query_ctx_from_state_with_sql(state, formatted_sql)?
            .with_desc("add_query adapter call");

        self.typed_adapter.add_query(
            conn.as_mut(),
            &query_ctx,
            auto_begin.unwrap_or(true),
            abridge_sql_log.unwrap_or(false),
        )?;
        Ok(Value::UNDEFINED)
    }

    #[tracing::instrument(skip(self, state))]
    fn drop_relation(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;

        let relation = parser.get::<Value>("relation")?;
        let relation = downcast_value_to_dyn_base_relation(relation)?;
        Ok(self.typed_adapter.drop_relation(state, relation)?)
    }

    #[tracing::instrument(skip(self, state))]
    fn truncate_relation(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;

        let relation = parser.get::<Value>("relation")?;
        let relation = downcast_value_to_dyn_base_relation(relation)?;
        Ok(self.typed_adapter.truncate_relation(state, relation)?)
    }

    #[tracing::instrument(skip(self, state))]
    fn rename_relation(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        execute_macro(state, args, "rename_relation")?;
        Ok(none_value())
    }

    /// Expand the to_relation table's column types to match the schema of from_relation.
    /// https://docs.getdbt.com/reference/dbt-jinja-functions/adapter#expand_target_column_types
    #[tracing::instrument(skip(self, state))]
    fn expand_target_column_types(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 2, 2)?;

        let from_relation = parser.get::<Value>("from_relation")?;
        let to_relation = parser.get::<Value>("to_relation")?;

        let from_relation = downcast_value_to_dyn_base_relation(from_relation)?;
        let to_relation = downcast_value_to_dyn_base_relation(to_relation)?;
        let result =
            self.typed_adapter
                .expand_target_column_types(state, from_relation, to_relation)?;
        Ok(result)
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/sql/impl.py#L212-L213
    #[tracing::instrument(skip(self, state))]
    fn list_schemas(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;

        let database = parser.get::<String>("database")?;
        let kwargs = Kwargs::from_iter([("database", Value::from(database))]);

        let result = execute_macro_wrapper(state, &[Value::from(kwargs)], "list_schemas")?;
        let result = self.typed_adapter.list_schemas(result);

        Ok(Value::from_iter(result))
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/sql/impl.py#L161
    #[tracing::instrument(skip(self, state))]
    fn create_schema(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        execute_macro(state, args, "create_schema")?;
        Ok(none_value())
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/sql/impl.py#L172-L173
    #[tracing::instrument(skip(self, state))]
    fn drop_schema(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        execute_macro(state, args, "drop_schema")?;
        Ok(none_value())
    }

    #[tracing::instrument(skip(self, _state, _args))]
    fn valid_snapshot_target(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        unimplemented!("valid_snapshot_target")
    }

    #[tracing::instrument(skip(self, state))]
    fn get_incremental_strategy_macro(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 2, 2)?;

        let _ = parser.get::<String>("context")?; // unnecessary, parse for backward compat; the existing dbt requires it to execute a macro
        let strategy = parser.get::<String>("strategy")?;

        if strategy != "default" {
            let strategy_ = DbtIncrementalStrategy::from_str(&strategy)
                .map_err(|e| invalid_argument_inner!("Invalid strategy value {}", e))?;
            if !self
                .typed_adapter
                .valid_incremental_strategies()
                .contains(&strategy_)
                && !builtin_incremental_strategies(false).contains(&strategy_)
            {
                return invalid_argument!(
                    "The incremental strategy '{}' is not valid for this adapter",
                    strategy
                );
            }
        }

        let strategy = strategy.replace("+", "_");
        let macro_name = format!("get_incremental_{}_sql", strategy);

        // Return the macro
        Ok(Value::from_object(DispatchObject {
            macro_name,
            package_name: None,
            strict: false,
            auto_execute: false,
            context: Some(state.get_base_context()),
        }))
    }

    #[tracing::instrument(skip(self, state))]
    fn assert_valid_snapshot_target_given_strategy(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 3, 3)?;

        let relation = parser.get::<Value>("relation")?;
        let relation = downcast_value_to_dyn_base_relation(relation)?;

        let column_names = parser.get::<Value>("column_names")?;
        let column_names = if column_names.is_none() {
            None
        } else {
            Some(BTreeMap::<String, String>::deserialize(column_names)?)
        };

        let strategy = parser.get::<Value>("strategy")?;
        let strategy = SnapshotStrategy::deserialize(strategy)?;

        self.typed_adapter
            .assert_valid_snapshot_target_given_strategy(
                state,
                relation,
                column_names,
                Arc::new(strategy),
            )?;

        Ok(none_value())
    }

    #[tracing::instrument(skip(self, _state))]
    fn get_hard_deletes_behavior(
        &self,
        _state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;

        let config: Value = parser.next_positional()?;
        let hard_deletes = config.get_item(&Value::from("hard_deletes")).ok();
        let invalidate_hard_deletes = config
            .get_item(&Value::from("invalidate_hard_deletes"))
            .ok();

        let mut config = BTreeMap::<String, Value>::new();
        if let Some(hard_deletes) = hard_deletes {
            if !hard_deletes.is_undefined() {
                config.insert("hard_deletes".to_string(), hard_deletes);
            }
        }
        if let Some(invalidate_hard_deletes) = invalidate_hard_deletes {
            if !invalidate_hard_deletes.is_undefined() {
                config.insert(
                    "invalidate_hard_deletes".to_string(),
                    invalidate_hard_deletes,
                );
            }
        }

        Ok(Value::from(
            self.typed_adapter.get_hard_deletes_behavior(config)?,
        ))
    }

    #[tracing::instrument(skip(self, state))]
    fn get_relation(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 3, 4)?;

        let database = parser.get_optional::<String>("database");
        let database = if let Some(database) = database {
            database
        } else if self.typed_adapter.adapter_type() == AdapterType::Databricks {
            DEFAULT_DATABRICKS_DATABASE.to_string()
        } else {
            return jinja_err!(MinijinjaErrorKind::InvalidOperation, "database is required");
        };

        let schema = parser.get::<String>("schema")?;
        let identifier = parser.get::<String>("identifier")?;
        let needs_information = parser.get_optional::<bool>("needs_information");

        let mut conn = self.borrow_tlocal_connection()?;
        let query_ctx = query_ctx_from_state(state)?.with_desc("get_relation adapter call");
        let relation = self.typed_adapter.get_relation(
            &query_ctx,
            conn.as_mut(),
            &database,
            &schema,
            &identifier,
            needs_information,
        )?;
        match relation {
            Some(relation) => Ok(relation.as_value()),
            None => Ok(none_value()),
        }
    }

    #[tracing::instrument(skip(self, state))]
    fn get_missing_columns(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 2, 2)?;

        let from_relation = parser.get::<Value>("from_relation")?;
        let to_relation = parser.get::<Value>("to_relation")?;

        let from_relation = downcast_value_to_dyn_base_relation(from_relation)?;
        let to_relation = downcast_value_to_dyn_base_relation(to_relation)?;
        let result = self
            .typed_adapter
            .get_missing_columns(state, from_relation, to_relation)?;

        Ok(result)
    }

    #[tracing::instrument(skip(self, state))]
    fn get_columns_in_relation(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;

        let relation = parser.get::<Value>("relation")?;
        let relation = downcast_value_to_dyn_base_relation(relation)?;

        if let Some(db) = &self.db {
            // see example at crates/dbt-adapter-tests/tests/data/repros/incremental_simple
            // if a model is incremental, always query the remote state
            // since the compiled sql in incremental run may represent a schema of which the model that will have when the run is done
            if !state.is_run_incremental() {
                let schema = db.get_schema_by_fqn(&relation.semantic_fqn());
                if let Some(schema) = schema {
                    let result = self.typed_adapter.arrow_schema_to_dbt_columns(schema)?;
                    return Ok(Value::from_iter(result));
                }
            }
        }

        let result = self
            .typed_adapter
            .get_columns_in_relation(state, relation)?;
        let result = dyn_base_columns_to_value(result)?;
        Ok(result)
    }

    #[tracing::instrument(skip(self, state))]
    fn check_schema_exists(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 2, 2)?;

        let database = parser.get::<String>("database")?;
        let schema = parser.get::<String>("schema")?;

        let information_schema = InformationSchema {
            database: Some(database),
            schema: "INFORMATION_SCHEMA".to_string(),
            identifier: None,
        };

        let (package_name, macro_name) =
            self.typed_adapter.check_schema_exists_macro(state, args)?;
        let batch: Arc<arrow::array::RecordBatch> = execute_macro_wrapper_with_package(
            state,
            &[information_schema.as_value(), Value::from(schema)],
            &macro_name,
            &package_name,
        )?;

        match extract_first_value_as_i64(&batch) {
            Some(0) => Ok(Value::from(false)),
            Some(1) => Ok(Value::from(true)),
            _ => jinja_err!(MinijinjaErrorKind::ReturnValue, "invalid return value"),
        }
    }

    #[tracing::instrument(skip(self, state))]
    fn get_relations_by_pattern(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 2, 6)?;

        let _ = parser.get::<String>("schema_pattern")?;
        let _ = parser.get::<String>("table_pattern")?;
        let _ = parser.get_optional::<String>("exclude").unwrap_or_default();
        let _ = parser
            .get_optional::<String>("database")
            .unwrap_or_default();
        let _ = parser
            .get_optional::<bool>("quote_table")
            .unwrap_or_default();
        let excluded_schemas = parser
            .get_optional::<Value>("excluded_schemas")
            .unwrap_or(Value::from_iter::<Vec<String>>(vec![]));
        let _ = Vec::<String>::deserialize(excluded_schemas).map_err(|e| {
            MinijinjaError::new(MinijinjaErrorKind::SerdeDeserializeError, e.to_string())
        })?;

        let result = execute_macro(state, args, "get_relations_by_pattern_internal")?;
        Ok(result)
    }

    #[tracing::instrument(skip(self, state))]
    fn get_column_schema_from_query(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;
        let sql = parser.get::<String>("sql")?;

        let query_ctx = query_ctx_from_state_with_sql(state, sql)?
            .with_desc("get_column_schema_from_query adapter call");
        let mut conn = self.borrow_tlocal_connection()?;
        let result = self
            .typed_adapter
            .get_column_schema_from_query(conn.as_mut(), &query_ctx)?;
        let result = dyn_base_columns_to_value(result)?;
        Ok(result)
    }

    /// reference: https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L443-L444
    /// Shares the same input and output as get_column_schema_from_query, simply delegate to the other for now
    /// TODO: but it's implemented in a different way, investigate if this matters.
    #[tracing::instrument(skip(self, state))]
    fn get_columns_in_select_sql(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        self.get_column_schema_from_query(state, args)
    }

    #[tracing::instrument(skip(self, _state))]
    fn verify_database(&self, _state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;
        let database = parser.get::<String>("database")?;
        let result = self.typed_adapter.verify_database(database);
        Ok(result?)
    }

    #[tracing::instrument(skip(self, _state))]
    fn nest_column_data_types(
        &self,
        _state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 2)?;

        // TODO: 'constraints' arg are ignored; didn't find an usage example, implement later
        let columns = parser.get::<Value>("columns")?;
        let columns_map = BTreeMap::<String, DbtColumn>::deserialize(columns).map_err(|e| {
            MinijinjaError::new(MinijinjaErrorKind::SerdeDeserializeError, e.to_string())
        })?;

        let result = self
            .typed_adapter
            .nest_column_data_types(columns_map, None)?;
        Ok(Value::from_serialize(&result))
    }

    #[tracing::instrument(skip(self, _state))]
    fn get_bq_table(&self, _state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;
        unimplemented!("get_bq_table")
    }

    #[tracing::instrument(skip(self, _state))]
    fn is_replaceable(&self, _state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 3)?;

        let relation_as_val = parser.get::<Value>("relation")?;
        let relation = if relation_as_val.is_none() {
            return Ok(Value::from(true));
        } else {
            downcast_value_to_dyn_base_relation(relation_as_val)?
        };
        let partition_by = parser.get::<Value>("partition_by")?;
        let partition_by = if partition_by.is_none() {
            None
        } else {
            Some(BigqueryPartitionConfig::deserialize(partition_by)?)
        };

        let cluster_by = parser.get::<Value>("cluster_by")?;
        let cluster_by = if cluster_by.is_none() {
            None
        } else {
            Some(BigqueryClusterConfig::deserialize(cluster_by)?)
        };

        let mut conn = self.borrow_tlocal_connection()?;
        let result =
            self.typed_adapter
                .is_replaceable(conn.as_mut(), relation, partition_by, cluster_by)?;
        Ok(Value::from(result))
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L566-L567
    /// In dbt, this methods converts a python dict to a BigqueryPartitionConfig
    /// this is unnecessary here since the input arg should be fetched from value of ManifestModelConfig type
    /// and the validation is done via a Minijinja value's downcast_object
    ///
    /// # Panics
    /// This method will panic if called on a non-BigQuery adapter
    #[tracing::instrument(skip(self, _state))]
    fn parse_partition_by(&self, _state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        parser.check_num_args(current_function_name!(), 1, 1)?;

        let raw_partition_by = parser.get::<Value>("raw_partition_by")?;

        if !raw_partition_by.is_none() {
            let partition_by = BigqueryPartitionConfigLegacy::deserialize(raw_partition_by.clone())
                .map_err(|e| {
                    MinijinjaError::new(MinijinjaErrorKind::SerdeDeserializeError, e.to_string())
                })?;
            self.typed_adapter
                .parse_partition_by(partition_by.validate()?)?;
        }
        Ok(raw_partition_by)
    }

    #[tracing::instrument(skip(self, _state))]
    fn get_table_options(&self, _state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        parser.check_num_args(current_function_name!(), 2, 3)?;
        let config = parser.get::<Value>("config")?;
        let node = parser.get::<Value>("node")?;
        let temporary = parser
            .get_optional::<Value>("node")
            .unwrap_or_default()
            .is_true();

        let config = ModelConfig::deserialize(config).map_err(|e| {
            MinijinjaError::new(
                MinijinjaErrorKind::SerdeDeserializeError,
                format!("get_table_options: Failed to deserialize config: {e}"),
            )
        })?;

        let node_wrapper = InternalDbtNodeWrapper::deserialize(node).map_err(|e| {
            MinijinjaError::new(
                MinijinjaErrorKind::SerdeDeserializeError,
                format!("get_table_options: Failed to deserialize InternalDbtNodeWrapper: {e}"),
            )
        })?;
        let node = node_wrapper.as_internal_node();

        let options = self
            .typed_adapter
            .get_table_options(config, node.common(), temporary)?;
        Ok(Value::from_serialize(options))
    }

    #[tracing::instrument(skip(self, _state))]
    fn get_view_options(&self, _state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        parser.check_num_args(current_function_name!(), 2, 3)?;

        let config = parser.get::<Value>("config")?;
        let node = parser.get::<Value>("node")?;

        let config = ModelConfig::deserialize(config).map_err(|e| {
            MinijinjaError::new(
                MinijinjaErrorKind::SerdeDeserializeError,
                format!("get_view_options: Failed to deserialize config: {e}"),
            )
        })?;

        let node_wrapper = InternalDbtNodeWrapper::deserialize(node).map_err(|e| {
            MinijinjaError::new(
                MinijinjaErrorKind::SerdeDeserializeError,
                format!("get_table_options: Failed to deserialize InternalDbtNodeWrapper: {e}"),
            )
        })?;
        let node = node_wrapper.as_internal_node();

        let options = self.typed_adapter.get_view_options(config, node.common())?;
        Ok(Value::from_serialize(options))
    }

    #[tracing::instrument(skip(self, _state))]
    fn add_time_ingestion_partition_column(
        &self,
        _state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        parser.check_num_args(current_function_name!(), 2, 3)?;

        let partition_by = parser.get::<Value>("partition_by")?;
        let columns = parser.get::<Value>("columns")?;

        let partition_by =
            BigqueryPartitionConfigLegacy::deserialize(partition_by).map_err(|e| {
                MinijinjaError::new(MinijinjaErrorKind::SerdeDeserializeError, e.to_string())
            })?;

        let result = self
            .typed_adapter
            .add_time_ingestion_partition_column(columns, partition_by.validate()?)?;
        Ok(result)
    }

    #[tracing::instrument(skip(self, state))]
    fn grant_access_to(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 4, 4)?;

        let entity = parser.get::<String>("entity")?;
        let entity_type = parser.get::<String>("entity_type")?;
        let role = parser.get::<Value>("role")?;
        let grant_target =
            GrantAccessToTarget::deserialize(parser.get::<Value>("grant_target_dict")?)?;

        let (database, schema) = (
            grant_target
                .project
                .as_deref()
                .ok_or(invalid_argument_inner!(
                    "project in a GrantAccessToTarget cannot be empty"
                ))?,
            grant_target
                .dataset
                .as_deref()
                .ok_or(invalid_argument_inner!(
                    "dataset in a GrantAccessToTarget cannot be empty"
                ))?,
        );

        let role = if role.is_none() {
            None
        } else {
            Some(
                role.as_str()
                    .ok_or(invalid_argument_inner!("role must be a string"))?,
            )
        };
        let mut conn = self.borrow_tlocal_connection()?;
        let result = self.typed_adapter.grant_access_to(
            state,
            conn.as_mut(),
            &entity,
            &entity_type,
            role,
            database,
            schema,
        )?;
        Ok(result)
    }

    #[tracing::instrument(skip(self, state))]
    fn get_dataset_location(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;

        let relation = parser.get::<Value>("relation")?;
        let mut conn = self.borrow_tlocal_connection()?;
        let result = self
            .typed_adapter
            .get_dataset_location(state, conn.as_mut(), relation)?;
        Ok(Value::from(result))
    }

    #[tracing::instrument(skip(self, state))]
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

        let mut conn = self.borrow_tlocal_connection()?;
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

    #[tracing::instrument(skip(self, state))]
    fn alter_table_add_columns(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 2, 2)?;

        let relation = parser.get::<Value>("relation")?;
        let columns = parser.get::<Value>("columns")?;

        let mut conn = self.borrow_tlocal_connection()?;
        let result =
            self.typed_adapter
                .alter_table_add_columns(state, conn.as_mut(), relation, columns)?;
        Ok(result)
    }

    #[tracing::instrument(skip(self, state))]
    fn update_columns(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 2, 2)?;

        let relation = parser.get::<Value>("relation")?;
        let columns = parser.get::<Value>("columns")?;
        let columns_map = BTreeMap::<String, DbtColumn>::deserialize(columns).map_err(|e| {
            MinijinjaError::new(MinijinjaErrorKind::SerdeDeserializeError, e.to_string())
        })?;

        let mut conn = self.borrow_tlocal_connection()?;
        let result = self.typed_adapter.update_columns_descriptions(
            state,
            conn.as_mut(),
            relation,
            columns_map,
        )?;
        Ok(result)
    }

    #[tracing::instrument(skip(self))]
    fn behavior(&self) -> Value {
        let mut behavior_flags = self.typed_adapter.behavior();
        for flag in DEFAULT_BASE_BEHAVIOR_FLAGS.iter() {
            behavior_flags.push(flag.clone());
        }
        // TODO: support user overrides (using flags from RuntimeConfig)
        // https://github.com/dbt-labs/dbt-adapters/blob/3ed165d452a0045887a5032c621e605fd5c57447/dbt-adapters/src/dbt/adapters/base/impl.py#L360
        Value::from_object(Behavior::new(&behavior_flags))
    }

    #[tracing::instrument(skip(self, state))]
    fn list_relations_without_caching(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;

        let relation = parser.get::<Value>("schema_relation")?;

        let mut conn = self.borrow_tlocal_connection()?;
        let result =
            self.typed_adapter
                .list_relations_without_caching(state, conn.as_mut(), relation)?;
        Ok(result)
    }

    #[tracing::instrument(skip(self, state))]
    fn compare_dbr_version(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 2, 2)?;

        let major = parser.get::<i64>("major")?;
        let minor = parser.get::<i64>("minor")?;

        let mut conn = self.borrow_tlocal_connection()?;
        let result = self
            .typed_adapter
            .compare_dbr_version(state, conn.as_mut(), major, minor)?;
        Ok(result)
    }

    #[tracing::instrument(skip(self, _state))]
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

        let config = ModelConfig::deserialize(config).map_err(|e| {
            MinijinjaError::new(MinijinjaErrorKind::SerdeDeserializeError, e.to_string())
        })?;

        let node = InternalDbtNodeWrapper::deserialize(model).map_err(|e| {
            MinijinjaError::new(
                MinijinjaErrorKind::SerdeDeserializeError,
                format!(
                    "adapter.compute_external_path expected an InternalDbtNodeWrapper: {}",
                    e
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

    #[tracing::instrument(skip(self, state))]
    fn update_tblproperties_for_iceberg(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 2)?;

        let config = parser.get::<Value>("config")?;
        let config = ModelConfig::deserialize(config).map_err(|e| {
            MinijinjaError::new(MinijinjaErrorKind::SerdeDeserializeError, e.to_string())
        })?;

        let mut tblproperties = match parser.get_optional::<Value>("tblproperties") {
            Some(v) if !v.is_none() => BTreeMap::<String, Value>::deserialize(v).map_err(|e| {
                MinijinjaError::new(MinijinjaErrorKind::SerdeDeserializeError, e.to_string())
            })?,
            _ => BTreeMap::new(),
        };

        let mut conn = self.borrow_tlocal_connection()?;
        self.typed_adapter.update_tblproperties_for_iceberg(
            state,
            conn.as_mut(),
            config,
            &mut tblproperties,
        )?;
        Ok(Value::from_serialize(&tblproperties))
    }

    #[tracing::instrument(skip(self, state))]
    fn copy_table(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 3, 3)?;

        // (tmp_relation_partitioned, target_relation_partitioned, "materialization")
        let source = parser.get::<Value>("tmp_relation_partitioned")?;
        let dest = parser.get::<Value>("target_relation_partitioned")?;
        let materialization = parser.get::<String>("materialization")?;

        let source = downcast_value_to_dyn_base_relation(source)?;
        let dest = downcast_value_to_dyn_base_relation(dest)?;

        let mut conn = self.borrow_tlocal_connection()?;
        self.typed_adapter
            .copy_table(state, conn.as_mut(), source, dest, materialization)?;

        Ok(none_value())
    }

    #[tracing::instrument(skip(self))]
    fn describe_relation(&self, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;

        let relation = parser.get::<Value>("relation")?;
        let relation = downcast_value_to_dyn_base_relation(relation)?;

        let mut conn = self.borrow_tlocal_connection()?;
        let result = self
            .typed_adapter
            .describe_relation(conn.as_mut(), relation)?;
        Ok(result.map_or(none_value(), Value::from_serialize))
    }

    #[tracing::instrument(skip(self, _state))]
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

    #[tracing::instrument(skip(self, _state, _args))]
    fn valid_incremental_strategies(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(self.typed_adapter.valid_incremental_strategies_as_values())
    }

    #[tracing::instrument(skip(self, _state))]
    fn redact_credentials(&self, _state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;

        let sql = parser.next_positional::<String>()?;

        let sql_redacted = self.typed_adapter().redact_credentials(&sql)?;

        Ok(Value::from(sql_redacted))
    }

    #[tracing::instrument(skip(self, _state, _args))]
    fn get_partitions_metadata(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        unimplemented!("get_partitions_metadata")
    }

    #[tracing::instrument(skip(self, _state, args))]
    fn get_persist_doc_columns(
        &self,
        _state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 2, 2)?;

        let existing_columns = parser.get::<Value>("existing_columns")?;

        let model_columns = parser.get::<Value>("model_columns")?;

        let existing_columns = Vec::<StdColumn>::deserialize(existing_columns)?;
        let model_columns = BTreeMap::<String, DbtColumn>::deserialize(model_columns)?;

        Ok(Value::from_serialize(
            self.typed_adapter
                .get_persist_doc_columns(existing_columns, model_columns)?,
        ))
    }

    #[tracing::instrument(skip(self, state, args))]
    fn get_relation_config(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;

        let relation = parser.get::<Value>("relation")?;
        let relation = downcast_value_to_dyn_base_relation(relation)?;

        Ok(self.typed_adapter.get_relation_config(state, relation)?)
    }

    #[tracing::instrument(skip(self, _state, args))]
    fn get_config_from_model(
        &self,
        _state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;

        let model = parser.get::<Value>("model")?;

        let deserialized_node = InternalDbtNodeWrapper::deserialize(model).map_err(|e| {
            MinijinjaError::new(
                MinijinjaErrorKind::SerdeDeserializeError,
                format!(
                    "adapter.get_config_from_model expected an InternalDbtNodeWrapper: {}",
                    e
                ),
            )
        })?;

        let model = match deserialized_node {
            InternalDbtNodeWrapper::Model(model) => model,
            _ => {
                return Err(MinijinjaError::new(
                    MinijinjaErrorKind::InvalidOperation,
                    "adapter.get_config_from_model expected a DbtModel node".to_string(),
                ))
            }
        };

        Ok(self.typed_adapter.get_config_from_model(&model)?)
    }

    #[tracing::instrument(skip(self, _state, _args))]
    fn get_relations_without_caching(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        unimplemented!("get_relations_without_caching")
    }

    #[tracing::instrument(skip(self, _state, _args))]
    fn parse_index(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("parse_index")
    }

    #[tracing::instrument(skip(self, args))]
    fn clean_sql(&self, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;

        let sql = parser.get::<String>("sql")?;

        Ok(Value::from(self.typed_adapter.clean_sql(&sql)?))
    }

    #[tracing::instrument(skip(self, warehouse, node_id))]
    fn use_warehouse(&self, warehouse: Option<String>, node_id: &str) -> FsResult<bool> {
        if warehouse.is_none() {
            return Ok(false);
        }

        let mut conn = self
            .borrow_tlocal_connection()
            .map_err(|e| FsError::from_jinja_err(e, "Failed to create a connection"))?;
        self.typed_adapter
            .use_warehouse(conn.as_mut(), warehouse.unwrap(), node_id)?;
        Ok(true)
    }

    #[tracing::instrument(skip(self, node_id))]
    fn restore_warehouse(&self, node_id: &str) -> FsResult<()> {
        let mut conn = self
            .borrow_tlocal_connection()
            .map_err(|e| FsError::from_jinja_err(e, "Failed to create a connection"))?;
        self.typed_adapter
            .restore_warehouse(conn.as_mut(), node_id)?;
        Ok(())
    }

    #[tracing::instrument(skip(self, state))]
    fn load_dataframe(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut conn = self.borrow_tlocal_connection()?;
        let query_ctx = query_ctx_from_state(state)?.with_desc("load_dataframe");
        let result = self
            .typed_adapter
            .load_dataframe(&query_ctx, conn.as_mut(), args)?;
        Ok(result)
    }
}

impl fmt::Display for BridgeAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Adapter({})", self.adapter_type())
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
