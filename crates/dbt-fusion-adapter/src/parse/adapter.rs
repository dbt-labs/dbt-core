use crate::base_adapter::{AdapterType, AdapterTyping, BaseAdapter};
use crate::cast_util::downcast_value_to_dyn_base_relation;
use crate::funcs::{
    dispatch_adapter_calls, empty_map_value, empty_mutable_vec_value, empty_string_value,
    empty_vec_value, none_value,
};
use crate::metadata::MetadataAdapter;
use crate::parse::relation::EmptyRelation;
use crate::relation_object::{RelationObject, create_relation};
use crate::response::AdapterResponse;
use crate::typed_adapter::TypedBaseAdapter;
use crate::{AdapterResult, SqlEngine};

use dashmap::{DashMap, DashSet};
use dbt_agate::AgateTable;
use dbt_common::behavior_flags::Behavior;
use dbt_common::cancellation::CancellationToken;
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

use std::collections::BTreeMap;
use std::fmt;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;

/// Parse adapter for Jinja templates.
///
/// Returns stub values to enable the parsing phase.
#[derive(Debug, Clone)]
pub struct ParseAdapter {
    /// The type of database adapter (e.g. "snowflake", "postgres", etc.)
    adapter_type: String,
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

type RelationsToFetch = (
    Result<BTreeMap<String, Vec<Arc<dyn BaseRelation>>>, FsError>,
    Result<BTreeMap<String, Vec<Arc<dyn BaseRelation>>>, FsError>,
    BTreeMap<String, Vec<RelationPattern>>,
);

impl ParseAdapter {
    /// Make a new adapter
    pub fn new(
        adapter_type: impl Into<String>,
        package_quoting: DbtQuoting,
        token: CancellationToken,
    ) -> Self {
        let adapter_type = adapter_type.into();
        AdapterType::from_str(&adapter_type).expect("adapter_type is valid");
        Self {
            adapter_type,
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
            self.adapter_type.clone(),
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

        let base_relation = downcast_value_to_dyn_base_relation(relation.clone())?;
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
                        .cloned()
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
                        .cloned()
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
        // TODO: check if we need adapterType::Parse
        // since even ParseAdapter should be a specific <dialect> type
        debug_assert!(
            self.adapter_type
                .parse::<AdapterType>()
                .expect("adapter_type is valid")
                != AdapterType::Parse,
            "ParseAdapter should be a specific <dialect> type"
        );
        self.adapter_type.parse().unwrap_or(AdapterType::Parse)
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

    fn engine(&self) -> Option<&Arc<SqlEngine>> {
        None
    }

    fn quoting(&self) -> ResolvedQuoting {
        self.quoting
    }

    fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }
}

impl BaseAdapter for ParseAdapter {
    fn new_connection(&self) -> Result<Box<dyn Connection>, MinijinjaError> {
        unimplemented!("new_connection is not implemented for ParseAdapter")
    }

    fn execute(
        &self,
        state: &State,
        sql: &str,
        _auto_begin: bool,
        _fetch: bool,
        _limit: Option<i64>,
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

    fn describe_relation(&self, _args: &[Value]) -> Result<Value, MinijinjaError> {
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
}

impl fmt::Display for ParseAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Adapter({})", self.adapter_type)
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

/// Make parse factory
pub fn create_parse_adapter(
    adapter_type: impl Into<String>,
    package_quoting: DbtQuoting,
    token: CancellationToken,
) -> FsResult<Arc<dyn BaseAdapter>> {
    let adapter_type: String = adapter_type.into();
    Ok(Arc::new(ParseAdapter::new(
        adapter_type,
        package_quoting,
        token,
    )))
}
