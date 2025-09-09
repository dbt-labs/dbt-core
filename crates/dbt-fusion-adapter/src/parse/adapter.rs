use crate::base_adapter::{AdapterFactory, AdapterType, AdapterTyping, BaseAdapter, backend_of};
use crate::cast_util::downcast_value_to_dyn_base_relation;
use crate::funcs::{
    dispatch_adapter_calls, empty_map_value, empty_mutable_vec_value, empty_string_value,
    empty_vec_value, none_value,
};
use crate::metadata::MetadataAdapter;
use crate::parse::relation::EmptyRelation;
use crate::relation_object::{RelationObject, create_relation};
use crate::response::AdapterResponse;
use crate::stmt_splitter::NaiveStmtSplitter;
use crate::typed_adapter::TypedBaseAdapter;
use crate::{AdapterResult, SqlEngine};

use dashmap::{DashMap, DashSet};
use dbt_agate::AgateTable;
use dbt_auth::{AdapterConfig, Auth, auth_for_backend};
use dbt_common::adapter::SchemaRegistry;
use dbt_common::behavior_flags::Behavior;
use dbt_common::cancellation::CancellationToken;
use dbt_common::io_args::ReplayMode;
use dbt_common::{FsError, FsResult, current_function_name};
use dbt_schemas::schemas::columns::base::StdColumnType;
use dbt_schemas::schemas::common::{DbtQuoting, ResolvedQuoting};
use dbt_schemas::schemas::relations::base::{BaseRelation, RelationPattern};
use dbt_xdbc::Connection;
use minijinja::Value;
use minijinja::arg_utils::{ArgParser, check_num_args};
use minijinja::constants::TARGET_UNIQUE_ID;
use minijinja::listener::RenderingEventListener;
use minijinja::value::Object;
use minijinja::{Error as MinijinjaError, ErrorKind as MinijinjaErrorKind, State};
use serde::Deserialize;

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;

/// Parse adapter for Jinja templates.
///
/// Returns stub values to enable the parsing phase.
#[derive(Clone)]
pub struct ParseAdapter {
    adapter_type: AdapterType,
    /// The SQL engine for the ParseAdapter
    ///
    /// Not actually used to run SQL queries during parse, but needed since
    /// this object carries useful dependencies.
    engine: Arc<SqlEngine>,
    /// The call_get_relation method calls found during parse
    call_get_relation: DashMap<String, Vec<Value>>,
    /// The call_get_columns_in_relation method calls found during parse
    call_get_columns_in_relation: DashMap<String, Vec<Value>>,
    /// A patterned relation may turn to many dangling sources
    patterned_dangling_sources: DashMap<String, Vec<RelationPattern>>,
    /// A list of unsafe nodes detected during parse (unsafe nodes are nodes that have introspection qualities that make them non-deterministic / stateful)
    unsafe_nodes: DashSet<String>,
    /// SQLs that are found passed in to adapter.execute in the hidden Parse phase
    execute_sqls: DashSet<String>,
    /// The quoting policy for the adapter
    quoting: ResolvedQuoting,
    /// The global CLI cancellation token
    cancellation_token: CancellationToken,
}

impl fmt::Debug for ParseAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParseAdapter")
            .field("adapter_type", &self.adapter_type)
            .field("call_get_relation", &self.call_get_relation)
            .field(
                "call_get_columns_in_relation",
                &self.call_get_columns_in_relation,
            )
            .field(
                "patterned_dangling_sources",
                &self.patterned_dangling_sources,
            )
            .field("unsafe_nodes", &self.unsafe_nodes)
            .field("execute_sqls", &self.execute_sqls)
            .field("quoting", &self.quoting)
            .finish()
    }
}

type RelationsToFetch = (
    Result<BTreeMap<String, Vec<Arc<dyn BaseRelation>>>, FsError>,
    Result<BTreeMap<String, Vec<Arc<dyn BaseRelation>>>, FsError>,
    BTreeMap<String, Vec<RelationPattern>>,
);

impl ParseAdapter {
    /// Make a new adapter
    pub fn new(
        adapter_type: AdapterType,
        config: dbt_serde_yaml::Mapping,
        package_quoting: DbtQuoting,
        token: CancellationToken,
    ) -> Self {
        let backend = backend_of(adapter_type);

        let auth: Arc<dyn Auth> = auth_for_backend(backend).into();
        let adapter_config = AdapterConfig::new(config);
        let adapter_factory = Arc::new(AdapterFactoryForParse {});
        let stmt_splitter = Arc::new(NaiveStmtSplitter {});

        let engine = SqlEngine::new(
            auth,
            adapter_config,
            adapter_factory,
            stmt_splitter,
            token.clone(),
        );

        Self {
            adapter_type,
            engine,
            call_get_relation: DashMap::new(),
            call_get_columns_in_relation: DashMap::new(),
            patterned_dangling_sources: DashMap::new(),
            unsafe_nodes: DashSet::new(),
            execute_sqls: DashSet::new(),
            quoting: package_quoting
                .try_into()
                .expect("Failed to convert quoting to resolved quoting"),
            cancellation_token: token,
        }
    }

    /// Record a get_relation call for tracking
    pub fn record_get_relation_call(
        &self,
        state: &State,
        database: &str,
        schema: &str,
        identifier: &str,
    ) -> Result<(), MinijinjaError> {
        let relation = create_relation(
            self.adapter_type,
            database.to_string(),
            schema.to_string(),
            Some(identifier.to_string()),
            None,
            self.quoting,
        )?
        .as_value();

        if state.is_execute() {
            if let Some(unique_id) = state.lookup(TARGET_UNIQUE_ID) {
                self.call_get_relation
                    .entry(unique_id.to_string())
                    .or_default()
                    .push(relation);
            } else {
                println!("'TARGET_UNIQUE_ID' while get_relation is unset");
            }
        }
        Ok(())
    }

    /// Record a get_columns_in_relation call for tracking
    pub fn record_get_columns_in_relation_call(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<(), MinijinjaError> {
        let parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;

        let relation = args
            .first()
            .expect("get_columns_in_relation requires one argument");

        let base_relation = downcast_value_to_dyn_base_relation(relation)?;
        if !base_relation.is_database_relation() {
            return Ok(());
        }
        if state.is_execute() {
            if let Some(unique_id) = state.lookup(TARGET_UNIQUE_ID) {
                self.call_get_columns_in_relation
                    .entry(unique_id.to_string())
                    .or_default()
                    .push(relation.to_owned());
            } else {
                println!("'TARGET_UNIQUE_ID' while get_columns_in_relation is unset");
            }
        }
        Ok(())
    }

    /// Returns a tuple of (dangling_sources, patterned_dangling_sources)
    /// dangling_sources is a vector of dangling source relations
    /// patterned_dangling_sources is a vector of patterned dangling source relations
    #[allow(clippy::type_complexity)]
    pub fn relations_to_fetch(&self) -> RelationsToFetch {
        let relations_to_fetch = self
            .call_get_relation
            .iter()
            .map(|v| {
                Ok((
                    v.key().to_owned(),
                    v.value()
                        .iter()
                        .map(|v| downcast_value_to_dyn_base_relation(v))
                        .collect::<Result<Vec<Arc<dyn BaseRelation>>, MinijinjaError>>()?,
                ))
            })
            .collect::<Result<BTreeMap<String, Vec<Arc<dyn BaseRelation>>>, MinijinjaError>>()
            .map_err(|e| FsError::from_jinja_err(e, "Failed to collect get_relation"));

        let relations_to_fetch_columns = self
            .call_get_columns_in_relation
            .iter()
            .map(|v| {
                Ok((
                    v.key().to_owned(),
                    v.value()
                        .iter()
                        .map(|v| downcast_value_to_dyn_base_relation(v))
                        .collect::<Result<Vec<Arc<dyn BaseRelation>>, MinijinjaError>>()?,
                ))
            })
            .collect::<Result<BTreeMap<String, Vec<Arc<dyn BaseRelation>>>, MinijinjaError>>()
            .map_err(|e| FsError::from_jinja_err(e, "Failed to collect get_columns_in_relation"));

        let patterned_dangling_sources: BTreeMap<String, Vec<RelationPattern>> = self
            .patterned_dangling_sources
            .iter()
            .map(|r| (r.key().to_owned(), r.value().to_owned()))
            .collect();
        (
            relations_to_fetch,
            relations_to_fetch_columns,
            patterned_dangling_sources,
        )
    }

    /// Returns a DashSet of unsafe nodes
    pub fn unsafe_nodes(&self) -> &DashSet<String> {
        &self.unsafe_nodes
    }
}

impl AdapterTyping for ParseAdapter {
    fn adapter_type(&self) -> AdapterType {
        self.adapter_type
    }

    fn as_metadata_adapter(&self) -> Option<&dyn MetadataAdapter> {
        None // TODO: implement metadata_adapter() for ParseAdapter
    }

    fn as_typed_base_adapter(&self) -> &dyn TypedBaseAdapter {
        unimplemented!("as_typed_base_adapter")
    }

    fn column_type(&self) -> Option<Value> {
        let value = Value::from_object(StdColumnType);
        Some(value)
    }

    fn engine(&self) -> &Arc<SqlEngine> {
        &self.engine
    }

    fn quoting(&self) -> ResolvedQuoting {
        self.quoting
    }

    fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }
}

impl BaseAdapter for ParseAdapter {
    fn new_connection(
        &self,
        _node_id: Option<String>,
    ) -> Result<Box<dyn Connection>, MinijinjaError> {
        unimplemented!("new_connection is not implemented for ParseAdapter")
    }

    fn execute(
        &self,
        state: &State,
        sql: &str,
        _auto_begin: bool,
        _fetch: bool,
        _limit: Option<i64>,
        _options: Option<HashMap<String, String>>,
    ) -> AdapterResult<(AdapterResponse, AgateTable)> {
        let response = AdapterResponse::default();
        let table = AgateTable::default();

        if state.is_execute() {
            if let Some(unique_id) = state.lookup(TARGET_UNIQUE_ID) {
                self.unsafe_nodes.insert(
                    unique_id
                        .as_str()
                        .expect("unique_id must be a string")
                        .to_string(),
                );
            }
            self.execute_sqls.insert(sql.to_string());
        }

        Ok((response, table))
    }

    fn add_query(
        &self,
        _state: &State,
        _sql: &str,
        _auto_begin: bool,
        _bindings: Option<&Value>,
        _abridge_sql_log: bool,
    ) -> AdapterResult<()> {
        Ok(())
    }

    fn get_relation(
        &self,
        state: &State,
        database: &str,
        schema: &str,
        identifier: &str,
    ) -> Result<Value, MinijinjaError> {
        self.record_get_relation_call(state, database, schema, identifier)?;
        Ok(RelationObject::new(Arc::new(EmptyRelation {})).into_value())
    }

    fn get_columns_in_relation(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        self.record_get_columns_in_relation_call(state, args)?;
        Ok(empty_vec_value())
    }

    fn get_hard_deletes_behavior(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        // For parse adapter, always return "ignore" as default behavior
        Ok(none_value())
    }

    fn truncate_relation(&self, _state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;

        let _ = args
            .first()
            .expect("truncate_relation requires one argument");
        // TODO: check that the argument is a relation

        Ok(none_value())
    }

    // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/include/global_project/macros/relations/rename.sql
    fn rename_relation(&self, _state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 2, 2)?;

        let _ = args
            .first()
            .expect("rename_relation requires two arguments");
        // TODO: check that the argument is actually a relation

        let _ = args.last().expect("rename_relation requires two arguments");
        // TODO: check that the argument is actually a relation

        Ok(none_value())
    }

    fn expand_target_column_types(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn list_schemas(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(empty_vec_value())
    }

    fn create_schema(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn drop_schema(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn drop_relation(&self, _state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;

        let _ = args.first().expect("drop_relation requires one argument");
        // TODO: check that the argument is a relation

        Ok(none_value())
    }

    fn valid_snapshot_target(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn assert_valid_snapshot_target_given_strategy(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn get_missing_columns(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(empty_vec_value())
    }

    fn quote(&self, _state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 1, 1)?;

        let _ = args
            .first()
            .expect("quote requires exactly one argument")
            .to_string();

        Ok(empty_vec_value())
    }

    fn check_schema_exists(&self, _state: &State, args: &[Value]) -> Result<Value, MinijinjaError> {
        let parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 2, 2)?;

        Ok(Value::from(true))
    }

    fn get_relations_by_pattern(
        &self,
        state: &State,
        args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &parser, 2, 6)?;

        let schema_pattern = parser.get::<String>("schema_pattern")?;
        let table_pattern = parser.get::<String>("table_pattern")?;
        let _ = parser.get_optional::<String>("exclude").unwrap_or_default();

        let target = state
            .lookup("target")
            .expect("target is set in parse")
            .get_attr("database")
            .unwrap_or_default();
        let default_database = target.as_str().unwrap_or_default();
        let database = parser
            .get_optional::<String>("database")
            .unwrap_or(default_database.to_string());
        let _ = parser
            .get_optional::<bool>("quote_table")
            .unwrap_or_default();
        let excluded_schemas = parser
            .get_optional::<Value>("excluded_schemas")
            .unwrap_or(Value::from_iter::<Vec<String>>(vec![]));
        let _: Vec<String> = Vec::<String>::deserialize(excluded_schemas).map_err(|e| {
            MinijinjaError::new(MinijinjaErrorKind::SerdeDeserializeError, e.to_string())
        })?;

        let patterned_relation = RelationPattern::new(database, schema_pattern, table_pattern);

        if state.is_execute() {
            if let Some(unique_id) = state.lookup(TARGET_UNIQUE_ID) {
                self.patterned_dangling_sources
                    .entry(unique_id.to_string())
                    .or_default()
                    .push(patterned_relation);
            } else {
                println!("'TARGET_UNIQUE_ID' while get_relations_by_pattern is unset");
            }
        }

        // Seen methods like 'append' being used on the result in internaly-analytics
        Ok(empty_mutable_vec_value())
    }

    fn standardize_grants_dict(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(Value::from(BTreeMap::<Value, Vec<Value>>::new()))
    }

    fn get_column_schema_from_query(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(empty_vec_value())
    }

    fn render_raw_columns_constraints(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(empty_vec_value())
    }

    fn get_columns_in_select_sql(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(empty_vec_value())
    }

    fn add_time_ingestion_partition_column(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(empty_vec_value())
    }

    fn parse_partition_by(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn is_replaceable(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(Value::from(false))
    }

    fn nest_column_data_types(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(empty_map_value())
    }

    fn update_columns(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn list_relations_without_caching(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(empty_vec_value())
    }

    fn copy_table(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn update_table_description(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn alter_table_add_columns(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn load_dataframe(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn upload_file(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn get_common_options(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn get_table_options(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn get_view_options(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn get_bq_table(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn describe_relation(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn grant_access_to(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn get_dataset_location(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn quote_as_configured(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(empty_string_value())
    }

    fn quote_seed_column(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(empty_string_value())
    }

    fn convert_type(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(empty_string_value())
    }

    fn render_raw_model_constraints(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(empty_vec_value())
    }

    fn verify_database(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(Value::from(false))
    }

    fn compare_dbr_version(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(Value::from(0))
    }

    fn compute_external_path(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(empty_string_value())
    }

    fn update_tblproperties_for_iceberg(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(empty_map_value())
    }

    fn get_incremental_strategy_macro(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn behavior(&self) -> Value {
        Value::from_object(Behavior::new(&[]))
    }

    fn as_value(&self) -> Value {
        Value::from_object(self.clone())
    }

    fn generate_unique_temporary_table_suffix(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(Value::from(""))
    }

    fn get_config_from_model(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn get_partitions_metadata(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn get_persist_doc_columns(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn get_relation_config(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn get_relations_without_caching(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn parse_index(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn redact_credentials(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn valid_incremental_strategies(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(empty_string_value())
    }

    fn clean_sql(&self, _args: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("clean_sql")
    }

    // TODO(jason): We should probably capture any manual user engagement with the cache
    // and use this knowledge for our cache hydration
    fn cache_added(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn cache_dropped(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn cache_renamed(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }
}

impl fmt::Display for ParseAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ParseAdapter({})", self.adapter_type)
    }
}

impl Object for ParseAdapter {
    fn call_method(
        self: &Arc<Self>,
        state: &State,
        name: &str,
        args: &[Value],
        listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, MinijinjaError> {
        dispatch_adapter_calls(&**self, state, name, args, listeners)
    }
}

struct AdapterFactoryForParse;

impl AdapterFactory for AdapterFactoryForParse {
    fn create_adapter(
        &self,
        _adapter_type: AdapterType,
        _config: dbt_serde_yaml::Mapping,
        _replay_mode: Option<ReplayMode>,
        _flags: BTreeMap<String, Value>,
        _db: Option<Arc<dyn SchemaRegistry>>,
        _quoting: ResolvedQuoting,
        _token: CancellationToken,
    ) -> FsResult<Arc<dyn BaseAdapter>> {
        unreachable!("AdapterFactoryForParse should not be used to create more adapters")
    }

    fn create_relation_from_node(
        &self,
        _node: &dyn dbt_schemas::schemas::InternalDbtNodeAttributes,
        _adapter_type: AdapterType,
    ) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        unreachable!("AdapterFactoryForParse should not be used to create relations")
    }

    fn create_column(
        &self,
        _adapter_type: AdapterType,
        _name: String,
        _dtype: String,
        _char_size: Option<u32>,
        _numeric_precision: Option<u64>,
        _numeric_scale: Option<u64>,
        _mode: Option<String>,
    ) -> Result<Arc<dyn dbt_schemas::schemas::columns::base::BaseColumn>, minijinja::Error> {
        unreachable!("AdapterFactoryForParse should not be used to create columns")
    }

    fn with_relation_cache(
        &self,
        _relation_cache: Arc<crate::cache::RelationCache>,
    ) -> Arc<dyn AdapterFactory> {
        unreachable!("AdapterFactoryForParse should not be used to create more adapters")
    }

    fn to_owned(&self) -> Arc<dyn AdapterFactory> {
        Arc::new(AdapterFactoryForParse)
    }
}
