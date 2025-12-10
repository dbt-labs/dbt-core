use crate::base_adapter::{AdapterType, AdapterTyping, BaseAdapter, backend_of};
use crate::cast_util::downcast_value_to_dyn_base_relation;
use crate::catalog_relation::CatalogRelation;
use crate::errors::{AdapterError, AdapterErrorKind};
use crate::funcs::{
    dispatch_adapter_calls, empty_map_value, empty_mutable_vec_value, empty_string_value,
    empty_vec_value, none_value,
};
use crate::metadata::MetadataAdapter;
use crate::query_comment::QueryCommentConfig;
use crate::relation::parse::EmptyRelation;
use crate::relation::{RelationObject, create_relation};
use crate::response::AdapterResponse;
use crate::snapshots::SnapshotStrategy;
use crate::sql_types::TypeOps;
use crate::stmt_splitter::NaiveStmtSplitter;
use crate::typed_adapter::TypedBaseAdapter;
use crate::{AdapterEngine, AdapterResult};

use dashmap::{DashMap, DashSet};
use dbt_agate::AgateTable;
use dbt_auth::{AdapterConfig, Auth, auth_for_backend};
use dbt_common::FsError;
use dbt_common::behavior_flags::Behavior;
use dbt_common::cancellation::CancellationToken;
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use dbt_schemas::schemas::common::{DbtQuoting, ResolvedQuoting};
use dbt_schemas::schemas::dbt_catalogs::DbtCatalogs;
use dbt_schemas::schemas::dbt_column::DbtColumn;
use dbt_schemas::schemas::properties::ModelConstraint;
use dbt_schemas::schemas::relations::base::{BaseRelation, RelationPattern};
use dbt_xdbc::Connection;
use indexmap::IndexMap;
use minijinja::Value;
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
    /// The engine for the ParseAdapter
    ///
    /// Not actually used to run SQL queries during parse, but needed since
    /// this object carries useful dependencies.
    engine: Arc<AdapterEngine>,
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
    /// The global CLI cancellation token
    cancellation_token: CancellationToken,
    /// catalogs.yml stored when found and loaded
    catalogs: Option<Arc<DbtCatalogs>>,
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
            .field("quoting", &self.engine.quoting())
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
        type_ops: Box<dyn TypeOps>,
        token: CancellationToken,
        catalogs: Option<Arc<DbtCatalogs>>,
    ) -> Self {
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
            cancellation_token: token,
            catalogs,
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
            self.engine().quoting(),
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
        relation: Arc<dyn BaseRelation>,
    ) -> Result<(), MinijinjaError> {
        if !relation.is_database_relation() {
            return Ok(());
        }
        if state.is_execute() {
            if let Some(unique_id) = state.lookup(TARGET_UNIQUE_ID) {
                let relation_value = RelationObject::new(relation).into_value();
                self.call_get_columns_in_relation
                    .entry(unique_id.to_string())
                    .or_default()
                    .push(relation_value);
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

    fn is_parse(&self) -> bool {
        true
    }

    fn engine(&self) -> &Arc<AdapterEngine> {
        &self.engine
    }

    fn quoting(&self) -> ResolvedQuoting {
        self.engine.quoting()
    }

    fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }
}

impl BaseAdapter for ParseAdapter {
    fn new_connection(
        &self,
        _state: Option<&State>,
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

    fn submit_python_job(
        &self,
        _state: &State,
        _model: &Value,
        _compiled_code: &str,
    ) -> AdapterResult<AdapterResponse> {
        // Python models cannot be executed during parse phase
        Err(AdapterError::new(
            AdapterErrorKind::NotSupported,
            "submit_python_job can only be called in materialization macros",
        ))
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

    fn build_catalog_relation(&self, model: &Value) -> Result<Value, MinijinjaError> {
        let relation = CatalogRelation::from_model_config_and_catalogs(
            &self.adapter_type,
            model,
            self.catalogs.clone(),
        )?;
        Ok(Value::from_object(relation))
    }

    fn get_columns_in_relation(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        self.record_get_columns_in_relation_call(state, relation)?;
        Ok(empty_vec_value())
    }

    fn get_hard_deletes_behavior(
        &self,
        _state: &State,
        _config: BTreeMap<String, Value>,
    ) -> Result<Value, MinijinjaError> {
        // For parse adapter, always return "ignore" as default behavior
        Ok(none_value())
    }

    fn truncate_relation(
        &self,
        _state: &State,
        _relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/include/global_project/macros/relations/rename.sql
    fn rename_relation(
        &self,
        _state: &State,
        _from_relation: Arc<dyn BaseRelation>,
        _to_relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn expand_target_column_types(
        &self,
        _state: &State,
        _from_relation: Arc<dyn BaseRelation>,
        _to_relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn list_schemas(&self, _state: &State, _database: &str) -> Result<Value, MinijinjaError> {
        Ok(empty_vec_value())
    }

    fn create_schema(
        &self,
        _state: &State,
        _relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn drop_schema(
        &self,
        _state: &State,
        _relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn drop_relation(
        &self,
        _state: &State,
        _relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn valid_snapshot_target(
        &self,
        _state: &State,
        _relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn assert_valid_snapshot_target_given_strategy(
        &self,
        _state: &State,
        _relation: Arc<dyn BaseRelation>,
        _column_names: Option<&BTreeMap<String, String>>,
        _strategy: &Arc<SnapshotStrategy>,
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn get_missing_columns(
        &self,
        _state: &State,
        _from_relation: Arc<dyn BaseRelation>,
        _to_relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        Ok(empty_vec_value())
    }

    fn quote(&self, _state: &State, _identifier: &str) -> Result<Value, MinijinjaError> {
        Ok(empty_vec_value())
    }

    fn check_schema_exists(
        &self,
        _state: &State,
        _database: &str,
        _schema: &str,
    ) -> Result<Value, MinijinjaError> {
        Ok(Value::from(true))
    }

    fn get_relations_by_pattern(
        &self,
        state: &State,
        schema_pattern: &str,
        table_pattern: &str,
        _exclude: Option<&str>,
        database: Option<&str>,
        _quote_table: Option<bool>,
        excluded_schemas: Option<Value>,
    ) -> Result<Value, MinijinjaError> {
        // Validate excluded_schemas if provided
        if let Some(ref schemas) = excluded_schemas {
            let _: Vec<String> = Vec::<String>::deserialize(schemas.clone()).map_err(|e| {
                MinijinjaError::new(MinijinjaErrorKind::SerdeDeserializeError, e.to_string())
            })?;
        }

        let target = state
            .lookup("target")
            .expect("target is set in parse")
            .get_attr("database")
            .unwrap_or_default();
        let default_database = target.as_str().unwrap_or_default();
        let database = database.unwrap_or(default_database);

        let patterned_relation = RelationPattern::new(
            database.to_string(),
            schema_pattern.to_string(),
            table_pattern.to_string(),
        );

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
        _grants_table: &Arc<AgateTable>,
    ) -> Result<Value, MinijinjaError> {
        unreachable!("standardize_grants_dict should be handled in dispatch for ParseAdapter")
    }

    fn get_column_schema_from_query(
        &self,
        _state: &State,
        _sql: &str,
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
        _sql: &str,
    ) -> Result<Value, MinijinjaError> {
        Ok(empty_vec_value())
    }

    fn add_time_ingestion_partition_column(
        &self,
        _state: &State,
        _columns: &Value,
        _partition_config: dbt_schemas::schemas::manifest::BigqueryPartitionConfig,
    ) -> Result<Value, MinijinjaError> {
        Ok(empty_vec_value())
    }

    fn parse_partition_by(
        &self,
        _state: &State,
        _raw_partition_by: &Value,
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn is_replaceable(
        &self,
        _state: &State,
        _relation: Option<Arc<dyn BaseRelation>>,
        _partition_by: Option<dbt_schemas::schemas::manifest::BigqueryPartitionConfig>,
        _cluster_by: Option<dbt_schemas::schemas::manifest::BigqueryClusterConfig>,
    ) -> Result<Value, MinijinjaError> {
        Ok(Value::from(false))
    }

    fn nest_column_data_types(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> Result<Value, MinijinjaError> {
        Ok(empty_map_value())
    }

    fn update_columns(
        &self,
        _state: &State,
        _relation: Arc<dyn BaseRelation>,
        _columns: IndexMap<String, DbtColumn>,
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn list_relations_without_caching(
        &self,
        _state: &State,
        _schema_relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        Ok(empty_vec_value())
    }

    fn copy_table(
        &self,
        _state: &State,
        _tmp_relation_partitioned: Arc<dyn BaseRelation>,
        _target_relation_partitioned: Arc<dyn BaseRelation>,
        _materialization: &str,
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn update_table_description(
        &self,
        _state: &State,
        _database: &str,
        _schema: &str,
        _identifier: &str,
        _description: &str,
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn alter_table_add_columns(
        &self,
        _state: &State,
        _relation: Arc<dyn BaseRelation>,
        _columns: &Value,
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn load_dataframe(
        &self,
        _state: &State,
        _database: &str,
        _schema: &str,
        _table_name: &str,
        _agate_table: Arc<AgateTable>,
        _file_path: &str,
        _field_delimiter: &str,
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn upload_file(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn get_common_options(
        &self,
        _state: &State,
        _config: dbt_schemas::schemas::project::ModelConfig,
        _node: &dbt_schemas::schemas::InternalDbtNodeWrapper,
        _temporary: bool,
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn get_table_options(
        &self,
        _state: &State,
        _config: dbt_schemas::schemas::project::ModelConfig,
        _node: &dbt_schemas::schemas::InternalDbtNodeWrapper,
        _temporary: bool,
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn get_view_options(
        &self,
        _state: &State,
        _config: dbt_schemas::schemas::project::ModelConfig,
        _node: &dbt_schemas::schemas::InternalDbtNodeWrapper,
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn get_bq_table(
        &self,
        _state: &State,
        _relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn describe_relation(
        &self,
        _state: &State,
        _relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn grant_access_to(
        &self,
        _state: &State,
        _entity: Arc<dyn BaseRelation>,
        _entity_type: &str,
        _role: Option<&str>,
        _database: &str,
        _schema: &str,
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn get_dataset_location(
        &self,
        _state: &State,
        _relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn quote_as_configured(
        &self,
        _state: &State,
        _identifier: &str,
        _quote_key: &str,
    ) -> Result<Value, MinijinjaError> {
        Ok(empty_string_value())
    }

    fn quote_seed_column(
        &self,
        _state: &State,
        _column: &str,
        _quote_config: Option<bool>,
    ) -> Result<Value, MinijinjaError> {
        Ok(empty_string_value())
    }

    fn convert_type(
        &self,
        _state: &State,
        _table: &Arc<AgateTable>,
        _col_idx: i64,
    ) -> Result<Value, MinijinjaError> {
        Ok(empty_string_value())
    }

    fn render_raw_model_constraints(
        &self,
        _state: &State,
        _raw_constraints: &[ModelConstraint],
    ) -> Result<Value, MinijinjaError> {
        Ok(empty_vec_value())
    }

    fn verify_database(&self, _state: &State, _database: String) -> Result<Value, MinijinjaError> {
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

    fn update_tblproperties_for_uniform_iceberg(
        &self,
        _state: &State,
        _config: dbt_schemas::schemas::project::ModelConfig,
        _node: &dbt_schemas::schemas::InternalDbtNodeWrapper,
        _tblproperties: Option<Value>,
    ) -> Result<Value, MinijinjaError> {
        Ok(empty_map_value())
    }

    fn is_uniform(
        &self,
        _state: &State,
        _config: dbt_schemas::schemas::project::ModelConfig,
        _node: &dbt_schemas::schemas::InternalDbtNodeWrapper,
    ) -> Result<Value, MinijinjaError> {
        Ok(Value::from(false))
    }

    fn get_incremental_strategy_macro(
        &self,
        _state: &State,
        _strategy: &str,
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
        _relation: Arc<dyn BaseRelation>,
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

    fn get_column_tags_from_model(
        &self,
        _state: &State,
        _node: &dyn InternalDbtNodeAttributes,
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
        _relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        Ok(empty_vec_value())
    }

    fn parse_index(&self, _state: &State, _raw_index: &Value) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn redact_credentials(&self, _state: &State, _sql: &str) -> Result<Value, MinijinjaError> {
        Ok(Value::from(""))
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
    fn cache_added(
        &self,
        _state: &State,
        _relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn cache_dropped(
        &self,
        _state: &State,
        _relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn cache_renamed(
        &self,
        _state: &State,
        _from_relation: Arc<dyn BaseRelation>,
        _to_relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        Ok(none_value())
    }

    fn describe_dynamic_table(
        &self,
        _state: &State,
        _relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, MinijinjaError> {
        let map = [("dynamic_table", none_value())]
            .into_iter()
            .collect::<HashMap<_, _>>();
        Ok(Value::from_serialize(map))
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
