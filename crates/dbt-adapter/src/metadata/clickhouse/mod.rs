use crate::AdapterEngine;
use crate::adapter::adapter_impl::{AdapterImpl, InnerAdapter};
use crate::connection::AdapterConnectionFactory;
use crate::query_ctx::{node_id_from_state, query_ctx_from_state};
use crate::record_batch::RecordBatchExt;
use crate::relation::do_create_relation;
use crate::sql_types::{TypeOps, make_arrow_field_v2};
use crate::{AdapterResult, errors::AsyncAdapterResult, metadata::*};
use arrow_schema::Schema;

use arrow_array::{Array, RecordBatch, StringArray, UInt64Array};

use dbt_adapter_core::ExecutionPhase;
use dbt_adapter_engine::MapReduce;
use dbt_adbc::{Connection, QueryCtx};
use dbt_common::cancellation::Cancellable;
use dbt_common::cancellation::CancellationToken;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::{
    legacy_catalog::{CatalogNodeStats, CatalogTable, ColumnMetadata, TableMetadata},
    relations::base::{BaseRelation, RelationPattern},
};
use indexmap::IndexMap;
use minijinja::State;
use minijinja::Value;
use std::collections::btree_map::Entry;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

const MAX_CONNECTIONS: usize = 4;

/// Escape a value to be safely interpolated inside a single-quoted ClickHouse
/// string literal. ClickHouse uses backslash escaping for `\` and `'` within
/// string literals (see <https://clickhouse.com/docs/en/sql-reference/syntax#string>).
pub(crate) fn escape_clickhouse_string_literal(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}

pub(crate) fn build_get_relation_sql(schema: &str, identifier: &str) -> String {
    let escaped_database = escape_clickhouse_string_literal(schema);
    let escaped_identifier = escape_clickhouse_string_literal(identifier);
    format!(
        "SELECT engine, name \
         FROM system.tables \
         WHERE database = '{escaped_database}' \
           AND name = '{escaped_identifier}'",
    )
}

pub struct ClickHouseMetadataAdapter {
    adapter: AdapterImpl,
}

impl ClickHouseMetadataAdapter {
    pub fn new(engine: Arc<dyn AdapterEngine>) -> Self {
        let adapter = AdapterImpl::new(engine, None);
        Self { adapter }
    }
}

impl MetadataAdapter for ClickHouseMetadataAdapter {
    fn adapter_type(&self) -> AdapterType {
        self.adapter.adapter_type()
    }

    /// Parse the record batch produced by the `clickhouse__get_catalog*` macros
    /// into per-relation table metadata.
    fn build_schemas_from_stats_sql(
        &self,
        stats_sql_result: Arc<RecordBatch>,
    ) -> AdapterResult<BTreeMap<String, CatalogTable>> {
        if stats_sql_result.num_rows() == 0 {
            return Ok(BTreeMap::new());
        }

        let table_catalogs = stats_sql_result.column_values::<StringArray>("table_database")?;
        let table_schemas = stats_sql_result.column_values::<StringArray>("table_schema")?;
        let table_names = stats_sql_result.column_values::<StringArray>("table_name")?;
        let data_types = stats_sql_result.column_values::<StringArray>("table_type")?;
        let comments = stats_sql_result.column_values::<StringArray>("table_comment")?;
        let table_owners = stats_sql_result.column_values::<StringArray>("table_owner")?;

        let mut result = BTreeMap::<String, CatalogTable>::new();

        for i in 0..table_catalogs.len() {
            let catalog = table_catalogs.value(i);
            let schema = table_schemas.value(i);
            let table = table_names.value(i);
            let data_type = data_types.value(i);
            let comment = comments.value(i);
            let owner = table_owners.value(i);

            let fully_qualified_name = format!("{catalog}.{schema}.{table}").to_lowercase();

            let entry = result.entry(fully_qualified_name.clone());

            if matches!(entry, Entry::Vacant(_)) {
                let node_metadata = TableMetadata {
                    materialization_type: data_type.to_string(),
                    schema: schema.to_string(),
                    name: table.to_string(),
                    database: Some(catalog.to_string()),
                    comment: match comment {
                        "" => None,
                        _ => Some(comment.to_string()),
                    },
                    owner: Some(owner.to_string()),
                };

                let no_stats = CatalogNodeStats {
                    id: "has_stats".to_string(),
                    label: "Has Stats?".to_string(),
                    value: serde_json::Value::Bool(false),
                    description: Some(
                        "Indicates whether there are statistics for this table".to_string(),
                    ),
                    include: false,
                };

                let node = CatalogTable {
                    metadata: node_metadata,
                    columns: IndexMap::new(),
                    stats: BTreeMap::from([("has_stats".to_string(), no_stats)]),
                    unique_id: None,
                };
                result.insert(fully_qualified_name.clone(), node);
            }
        }
        Ok(result)
    }

    /// Parse the record batch produced by the `clickhouse__get_catalog*` macros
    /// into per-relation column metadata.
    fn build_columns_from_get_columns(
        &self,
        stats_sql_result: Arc<RecordBatch>,
    ) -> AdapterResult<BTreeMap<String, BTreeMap<String, ColumnMetadata>>> {
        if stats_sql_result.num_rows() == 0 {
            return Ok(BTreeMap::new());
        }

        let table_catalogs = stats_sql_result.column_values::<StringArray>("table_database")?;
        let table_schemas = stats_sql_result.column_values::<StringArray>("table_schema")?;
        let table_names = stats_sql_result.column_values::<StringArray>("table_name")?;

        let column_names = stats_sql_result.column_values::<StringArray>("column_name")?;
        let column_indices = stats_sql_result.column_values::<UInt64Array>("column_index")?;
        let column_types = stats_sql_result.column_values::<StringArray>("column_type")?;
        let column_comments = stats_sql_result.column_values::<StringArray>("column_comment")?;

        let mut columns_by_relation = BTreeMap::new();

        for i in 0..table_catalogs.len() {
            let catalog = table_catalogs.value(i);
            let schema = table_schemas.value(i);
            let table = table_names.value(i);

            let fully_qualified_name = format!("{catalog}.{schema}.{table}").to_lowercase();

            let column_name = column_names.value(i);
            let column_index = column_indices.value(i);
            let column_type = column_types.value(i);
            let column_comment = column_comments.value(i);

            let column = ColumnMetadata {
                name: column_name.to_string(),
                index: column_index as i128,
                data_type: column_type.to_string(),
                comment: match column_comment {
                    "" => None,
                    _ => Some(column_comment.to_string()),
                },
            };

            columns_by_relation
                .entry(fully_qualified_name.clone())
                .or_insert(BTreeMap::new())
                .insert(column_name.to_string(), column);
        }
        Ok(columns_by_relation)
    }

    fn list_relations_schemas_inner(
        &self,
        unique_id: Option<String>,
        phase: Option<ExecutionPhase>,
        relations: &[Arc<dyn BaseRelation>],
        token: CancellationToken,
    ) -> AsyncAdapterResult<'_, HashMap<String, AdapterResult<Arc<Schema>>>> {
        type Acc = HashMap<String, AdapterResult<Arc<Schema>>>;

        // ClickHouse is a 2-part name system: dbt `schema` maps to CH `database`,
        // and the dbt `database` field is unused/empty. The DESCRIBE TABLE SQL must
        // use `schema.identifier` (unquoted), but the HashMap key that callers look
        // up via `schemas.get(&semantic_fqn)` must match `relation.semantic_fqn()`.
        // These two are different strings, so we carry both through MapReduce as a
        // tuple `(semantic_fqn, sql_name)`.
        let keys: Vec<(String, String)> = relations
            .iter()
            .map(|relation| {
                let semantic_fqn = relation.semantic_fqn();
                let schema = relation.schema_as_str().unwrap_or_default();
                let identifier = relation.identifier_as_str().unwrap_or_default();
                let sql_name = if schema.is_empty() {
                    identifier
                } else {
                    format!("{schema}.{identifier}")
                };
                (semantic_fqn, sql_name)
            })
            .collect();

        let factory = Box::new(AdapterConnectionFactory::new(
            self.adapter.engine().clone(),
            Some(MAX_CONNECTIONS),
        ));

        let adapter = self.adapter.clone();
        let token_clone = token.clone();
        let map_f = move |conn: &'_ mut dyn Connection,
                          key: &(String, String)|
              -> AdapterResult<Arc<Schema>> {
            let (_semantic_fqn, sql_name) = key;
            // ClickHouse DESCRIBE TABLE returns: name, type, default_type, default_expression,
            // comment, codec_expression, ttl_expression
            let sql = format!("DESCRIBE TABLE {};", sql_name);
            let mut ctx = QueryCtx::default().with_desc("Get table schema");
            if let Some(node_id) = unique_id.clone() {
                ctx = ctx.with_node_id(&node_id);
            }
            if let Some(phase) = phase {
                ctx = ctx.with_phase(phase.as_str());
            }
            let (_, table) = adapter.query(&ctx, conn, &sql, None, token_clone.clone())?;
            let batch = table.original_record_batch();
            let schema =
                build_schema_from_clickhouse_describe(batch, adapter.engine().type_ops().as_ref())?;
            Ok(schema)
        };

        let reduce_f = |acc: &mut Acc,
                        key: (String, String),
                        schema: AdapterResult<Arc<Schema>>|
         -> Result<(), Cancellable<AdapterError>> {
            let (semantic_fqn, _sql_name) = key;
            // Insert under the semantic FQN so callers using `schemas.get(&relation.semantic_fqn())`
            // can find the entry.
            acc.insert(semantic_fqn, schema);
            Ok(())
        };

        let map_reduce = MapReduce::new(factory, Box::new(map_f), Box::new(reduce_f), None);
        map_reduce.run(Arc::new(keys), token)
    }

    fn list_relations_schemas_by_patterns_inner(
        &self,
        _patterns: &[RelationPattern],
        _token: CancellationToken,
    ) -> AsyncAdapterResult<'_, Vec<(String, AdapterResult<RelationSchemaPair>)>> {
        todo!("ClickHouseAdapter::list_relations_schemas_by_patterns")
    }

    fn freshness_inner(
        &self,
        _relations: &[Arc<dyn BaseRelation>],
        _token: CancellationToken,
    ) -> AsyncAdapterResult<'_, BTreeMap<String, MetadataFreshness>> {
        todo!("ClickHouseAdapter::freshness")
    }

    fn create_schemas_if_not_exists(
        &self,
        state: &State<'_, '_>,
        catalog_schemas: Vec<(String, String, String)>,
    ) -> AdapterResult<Vec<(String, String, String, AdapterResult<()>)>> {
        create_schemas_if_not_exists(&self.adapter, self, state, catalog_schemas)
    }

    fn list_relations_in_parallel_inner(
        &self,
        db_schemas: &[CatalogAndSchema],
        token: CancellationToken,
    ) -> AsyncAdapterResult<'_, BTreeMap<CatalogAndSchema, AdapterResult<RelationVec>>> {
        type Acc = BTreeMap<CatalogAndSchema, AdapterResult<RelationVec>>;

        let factory = Box::new(AdapterConnectionFactory::new(
            self.adapter.engine().clone(),
            Some(MAX_CONNECTIONS),
        ));

        let adapter = self.adapter.clone();
        let token_clone = token.clone();
        let map_f = move |conn: &'_ mut dyn Connection,
                          db_schema: &CatalogAndSchema|
              -> AdapterResult<Vec<Arc<dyn BaseRelation>>> {
            let ctx = QueryCtx::default().with_desc("list_relations_in_parallel");
            list_relations(
                adapter.engine().as_ref(),
                &ctx,
                conn,
                db_schema,
                token_clone.clone(),
            )
        };

        let reduce_f = move |acc: &mut Acc,
                             db_schema: CatalogAndSchema,
                             relations: AdapterResult<Vec<Arc<dyn BaseRelation>>>|
              -> Result<(), Cancellable<AdapterError>> {
            match &relations {
                Ok(_) => {
                    acc.insert(db_schema, relations);
                }
                Err(e) => {
                    // Treat missing database as empty so callers can hydrate caches
                    // without erroring out on schemas that haven't been created yet.
                    if e.message().contains("doesn't exist")
                        || e.message().contains("does not exist")
                    {
                        acc.insert(db_schema, Ok(Vec::new()));
                    } else {
                        return Err(Cancellable::Error(AdapterError::new(
                            AdapterErrorKind::Internal,
                            e.message(),
                        )));
                    }
                }
            }
            Ok(())
        };

        let map_reduce = MapReduce::new(factory, Box::new(map_f), Box::new(reduce_f), None);
        map_reduce.run(Arc::new(db_schemas.to_vec()), token)
    }
}

/// List all relations (tables, views, materialized views, dictionaries) in a given database.
///
/// Queries ClickHouse's `system.tables` and maps the engine name to a [`RelationType`].
pub fn list_relations(
    engine: &dyn AdapterEngine,
    ctx: &QueryCtx,
    conn: &'_ mut dyn Connection,
    db_schema: &CatalogAndSchema,
    token: CancellationToken,
) -> AdapterResult<Vec<Arc<dyn BaseRelation>>> {
    // ClickHouse only has databases, no schemas — we map dbt `schema` to CH `database`.
    let escaped_database = escape_clickhouse_string_literal(&db_schema.resolved_schema);
    let sql = format!(
        "SELECT database AS table_database, \
                name AS table_name, \
                engine AS table_type \
         FROM system.tables \
         WHERE database = '{escaped_database}'"
    );

    let batch = engine.execute(None, conn, ctx, &sql, token)?;

    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }

    let table_databases = batch.column_values::<StringArray>("table_database")?;
    let table_names = batch.column_values::<StringArray>("table_name")?;
    let table_types = batch.column_values::<StringArray>("table_type")?;

    let mut relations = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        let database = table_databases.value(i);
        let name = table_names.value(i);
        let engine_name = table_types.value(i);
        let relation_type = relation_type_from_engine(engine_name);

        let relation = do_create_relation(
            engine.adapter_type(),
            db_schema.resolved_catalog.clone(),
            database.to_string(),
            Some(name.to_string()),
            Some(relation_type),
            engine.quoting(),
        )
        .map_err(|e| AdapterError::new(AdapterErrorKind::Internal, e.to_string()))?;

        relations.push(Arc::from(relation));
    }

    Ok(relations)
}

/// Map a ClickHouse `system.tables.engine` value to a dbt [`RelationType`].
///
/// MergeTree-family engines (and their Replicated/Versioned variants) are tables;
/// `View` and `LiveView` are views; `MaterializedView` and `Dictionary` get their
/// own kinds.
pub fn relation_type_from_engine(engine_name: &str) -> RelationType {
    match engine_name {
        "View" | "LiveView" => RelationType::View,
        "MaterializedView" => RelationType::MaterializedView,
        // Anything else (MergeTree, ReplacingMergeTree, AggregatingMergeTree,
        // SummingMergeTree, CollapsingMergeTree, VersionedCollapsingMergeTree,
        // GraphiteMergeTree, Replicated*, Distributed, Memory, Log, etc.) is a table.
        _ => RelationType::Table,
    }
}

/// The `(name, type)` pairs from a `DESCRIBE TABLE` result batch (which also
/// carries default/comment/codec/ttl columns; only name/type are consumed).
fn describe_name_type_pairs(batch: &RecordBatch) -> AdapterResult<Vec<(String, String)>> {
    let names = batch.column_values::<StringArray>("name")?;
    let types = batch.column_values::<StringArray>("type")?;
    Ok((0..batch.num_rows())
        .map(|i| (names.value(i).to_string(), types.value(i).to_string()))
        .collect())
}

/// Column names and ClickHouse type text of an arbitrary SELECT, via
/// `DESCRIBE TABLE (<sql>)`. DESCRIBE preserves the server's own type text —
/// the ADBC/Arrow result schema marks every column Nullable, which breaks
/// downstream DDL and contract/type comparisons. `settings_clause` (rendered
/// by [`query_settings_clause`]) makes introspected types match runtime
/// settings such as `join_use_nulls`.
pub fn describe_query_columns(
    engine: &dyn AdapterEngine,
    state: Option<&State>,
    conn: &mut dyn Connection,
    ctx: &QueryCtx,
    sql: &str,
    settings_clause: &str,
    token: CancellationToken,
) -> AdapterResult<Vec<(String, String)>> {
    // The closing paren goes on its own line: model SQL routinely ends with a
    // `-- line comment` (no trailing newline), which would otherwise swallow
    // the `)` and break the query.
    let batch = engine.execute(
        state,
        conn,
        ctx,
        &format!("DESCRIBE TABLE ({sql}\n){settings_clause}"),
        token,
    )?;
    describe_name_type_pairs(&batch)
}

/// Build an Arrow Schema from ClickHouse's `DESCRIBE TABLE` output.
fn build_schema_from_clickhouse_describe(
    describe_result: Arc<RecordBatch>,
    type_ops: &dyn TypeOps,
) -> AdapterResult<Arc<Schema>> {
    let mut fields = vec![];
    for (name, text_data_type) in describe_name_type_pairs(&describe_result)? {
        // ClickHouse encodes nullability via the `Nullable(...)` wrapper in the
        // type text; the type parser handles that and the Arrow nullable bit is
        // derived from there.
        let field = make_arrow_field_v2(type_ops, name, &text_data_type, None, None)?;
        fields.push(field);
    }

    let schema = Schema::new(fields);
    Ok(Arc::new(schema))
}

/// ClickHouse server capabilities, probed once per process and reused for every
/// model/thread afterwards (as the Python client does at connection init).
/// Later capability probes (atomic exchange, lightweight deletes) extend this
/// struct.
#[derive(Debug, Clone)]
pub struct ClickHouseCapabilities {
    /// `SELECT version()` result; empty when the probe failed (callers treat
    /// unknown as "modern server").
    pub server_version: String,
}

impl ClickHouseCapabilities {
    /// Whether the connected server is older than `version`; an unknown
    /// server version is treated as modern (false).
    pub fn is_before(&self, version: &str) -> AdapterResult<bool> {
        if self.server_version.is_empty() {
            return Ok(false);
        }
        Ok(compare_versions(version, &self.server_version)? > 0)
    }

    /// Inclusive counterpart of [`ClickHouseCapabilities::is_before`]; an
    /// unknown server version is treated as modern (true).
    pub fn is_at_or_after(&self, version: &str) -> AdapterResult<bool> {
        if self.server_version.is_empty() {
            return Ok(true);
        }
        Ok(compare_versions(&self.server_version, version)? >= 0)
    }
}

/// Mirrors dbt-clickhouse util.py `compare_versions`: 1/-1/0 comparing dotted
/// numeric versions segment by segment; errors on non-numeric segments.
fn compare_versions(v1: &str, v2: &str) -> AdapterResult<i32> {
    for (part1, part2) in v1.split('.').zip(v2.split('.')) {
        match (part1.parse::<i64>(), part2.parse::<i64>()) {
            (Ok(a), Ok(b)) => {
                if a != b {
                    return Ok(if a > b { 1 } else { -1 });
                }
            }
            _ => {
                return Err(AdapterError::new(
                    AdapterErrorKind::Configuration,
                    "Version must consist of only numbers separated by '.'",
                ));
            }
        }
    }
    Ok(0)
}

/// Read a dict-valued key (e.g. `settings`, `query_settings`) from
/// `model['config']`, preserving iteration order. Missing/undefined -> empty.
pub(crate) fn model_config_map(model: &Value, key: &str) -> Vec<(String, Value)> {
    let Ok(config) = model.get_attr("config") else {
        return Vec::new();
    };
    let Ok(map) = config.get_attr(key) else {
        return Vec::new();
    };
    if map.is_none() || map.is_undefined() {
        return Vec::new();
    }
    let Ok(keys) = map.try_iter() else {
        return Vec::new();
    };
    keys.filter_map(|k| {
        let name = k.as_str()?.to_string();
        let value = map.get_item(&k).ok()?;
        Some((name, value))
    })
    .collect()
}

/// Mirrors dbt-clickhouse impl.py `_build_settings_str`: string values not
/// already single-quoted get single-quoted, other values are emitted verbatim;
/// '' for no entries, otherwise newline-terminated.
pub(crate) fn build_settings_str(settings: &[(String, Value)]) -> String {
    let res: Vec<String> = settings
        .iter()
        .map(|(key, value)| match value.as_str() {
            Some(s) if !s.starts_with('\'') => format!("{key}='{s}'"),
            _ => format!("{key}={value}"),
        })
        .collect();
    if res.is_empty() {
        String::new()
    } else {
        format!("SETTINGS {}\n", res.join(", "))
    }
}

/// Mirrors ClickHouse impl.py `format_columns`:
/// `[{'name': c.name, 'data_type': c.data_type}, ...]`.
pub(crate) fn format_columns(columns: &Value) -> Value {
    let mut out: Vec<Value> = Vec::new();
    if let Ok(items) = columns.try_iter() {
        for column in items {
            let mut map: BTreeMap<String, Value> = BTreeMap::new();
            map.insert(
                "name".to_string(),
                column.get_attr("name").unwrap_or_default(),
            );
            map.insert(
                "data_type".to_string(),
                column.get_attr("data_type").unwrap_or_default(),
            );
            out.push(Value::from(map));
        }
    }
    Value::from(out)
}

/// Render a `query_settings` map (from macro kwargs) into a `" SETTINGS k=v, ..."`
/// suffix for introspection queries; empty string when absent or empty.
pub(crate) fn query_settings_clause(query_settings: Option<&Value>) -> String {
    let Some(qs) = query_settings else {
        return String::new();
    };
    let mut pairs: Vec<(String, Value)> = Vec::new();
    if let Ok(keys) = qs.try_iter() {
        for key in keys {
            if let (Some(name), Ok(value)) = (key.as_str(), qs.get_item(&key)) {
                pairs.push((name.to_string(), value));
            }
        }
    }
    let rendered = build_settings_str(&pairs);
    if rendered.is_empty() {
        String::new()
    } else {
        format!(" {}", rendered.trim_end())
    }
}

static CLICKHOUSE_CAPABILITIES: std::sync::OnceLock<ClickHouseCapabilities> =
    std::sync::OnceLock::new();

/// Return the server capabilities, probing only on the very first call
/// process-wide (concurrent callers block until it completes).
pub fn server_capabilities(
    adapter: &AdapterImpl,
    state: &State,
    token: CancellationToken,
) -> &'static ClickHouseCapabilities {
    CLICKHOUSE_CAPABILITIES.get_or_init(|| ClickHouseCapabilities {
        server_version: probe_server_version(adapter, state, token).unwrap_or_default(),
    })
}

/// `SELECT version()`; None when the probe cannot run (replay has no live
/// connection) or fails, leaving the version unknown -> "assume modern server".
fn probe_server_version(
    adapter: &AdapterImpl,
    state: &State,
    token: CancellationToken,
) -> Option<String> {
    let InnerAdapter::Impl(_, engine) = adapter.inner_adapter() else {
        return None;
    };
    let engine = Arc::clone(engine);
    let ctx = query_ctx_from_state(state)
        .ok()?
        .with_desc("clickhouse capability probe");
    let mut conn = adapter
        .borrow_tlocal_connection(Some(state), node_id_from_state(state))
        .ok()?;
    let batch = engine
        .execute(None, conn.as_mut(), &ctx, "SELECT version() AS v", token)
        .ok()?;
    if batch.num_rows() == 0 {
        return None;
    }
    let values = batch.column_values::<StringArray>("v").ok()?;
    Some(values.value(0).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql_types::DefaultTypeOps;
    use crate::stmt_splitter::DefaultStmtSplitter;
    use arrow_schema::{DataType, Field};
    use dbt_schemas::schemas::relations::DEFAULT_RESOLVED_QUOTING;

    #[test]
    fn test_build_settings_str_mirrors_python() {
        // Mirrors dbt-clickhouse _build_settings_str: unquoted strings get
        // single-quoted, other values verbatim; trailing newline; '' when empty.
        assert_eq!(build_settings_str(&[]), "");
        assert_eq!(
            build_settings_str(&[
                ("index_granularity".to_string(), Value::from(4096)),
                (
                    "replicated_deduplication_window".to_string(),
                    Value::from("0")
                ),
                ("prequoted".to_string(), Value::from("'x'")),
            ]),
            "SETTINGS index_granularity=4096, replicated_deduplication_window='0', prequoted='x'\n"
        );
    }

    #[test]
    fn test_model_config_map_reads_model_config() {
        let model = Value::from_serialize(serde_json::json!({
            "config": {
                "settings": {"index_granularity": 4096},
                "query_settings": {"join_use_nulls": 1},
            }
        }));
        let settings = model_config_map(&model, "settings");
        assert_eq!(settings.len(), 1);
        assert_eq!(settings[0].0, "index_granularity");

        // missing key -> empty
        assert!(model_config_map(&model, "not_there").is_empty());
        // model without config -> empty
        assert!(model_config_map(&Value::from(()), "settings").is_empty());
    }

    #[test]
    fn test_compare_versions_mirrors_python() {
        assert_eq!(compare_versions("26.6", "26.3.12.3").unwrap(), 1);
        assert_eq!(compare_versions("22.7.1.2484", "26.3.12.3").unwrap(), -1);
        assert_eq!(compare_versions("26.3", "26.3.12.3").unwrap(), 0); // zip stops at shorter
        assert!(compare_versions("26.x", "26.3").is_err());
    }

    /// A ClickHouse metadata adapter backed by a mock engine (no live
    /// connection); the catalog batch parsers never touch the engine.
    fn make_metadata_adapter() -> ClickHouseMetadataAdapter {
        ClickHouseMetadataAdapter {
            adapter: AdapterImpl::new_mock(
                AdapterType::ClickHouse,
                BTreeMap::new(),
                DEFAULT_RESOLVED_QUOTING,
                Arc::new(DefaultTypeOps::new(AdapterType::ClickHouse)),
                Arc::new(DefaultStmtSplitter),
            ),
        }
    }

    /// Build a catalog record batch with the same Arrow types the ClickHouse
    /// ADBC driver produces for the `clickhouse__get_catalog*` macro SQL:
    /// strings everywhere except `column_index`, which is a `UInt64`
    /// (`system.columns.position`). Using other types here (e.g. Decimal128
    /// for `column_index`) must fail, so these tests pin the wire format.
    #[allow(clippy::type_complexity)]
    fn catalog_batch(rows: &[(&str, &str, &str, &str, &str, u64, &str, &str)]) -> Arc<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_database", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
            Field::new("table_comment", DataType::Utf8, true),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("column_index", DataType::UInt64, false),
            Field::new("column_type", DataType::Utf8, false),
            Field::new("column_comment", DataType::Utf8, true),
            Field::new("table_owner", DataType::Utf8, true),
        ]));
        Arc::new(
            RecordBatch::try_new(
                schema,
                vec![
                    // table_database is always '' for ClickHouse (2-part naming)
                    Arc::new(StringArray::from(vec![""; rows.len()])),
                    Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0))),
                    Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
                    Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.2))),
                    Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.3))),
                    Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.4))),
                    Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.5))),
                    Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.6))),
                    Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.7))),
                    // table_owner is `cast(null as Nullable(String))` in the macro
                    Arc::new(StringArray::from(vec![None::<&str>; rows.len()])),
                ],
            )
            .expect("catalog_batch test fixture should build a valid RecordBatch"),
        )
    }

    fn sample_batch() -> Arc<RecordBatch> {
        catalog_batch(&[
            (
                "db1",
                "orders",
                "table",
                "fact table",
                "id",
                1,
                "UInt64",
                "",
            ),
            (
                "db1",
                "orders",
                "table",
                "fact table",
                "amount",
                2,
                "Decimal(18, 2)",
                "in cents",
            ),
            ("db1", "orders_v", "view", "", "id", 1, "UInt64", ""),
        ])
    }

    #[test]
    fn build_schemas_from_stats_sql_groups_rows_per_relation() {
        let adapter = make_metadata_adapter();
        let result = adapter
            .build_schemas_from_stats_sql(sample_batch())
            .unwrap();

        assert_eq!(
            result.keys().collect::<Vec<_>>(),
            vec![".db1.orders", ".db1.orders_v"]
        );

        let orders = &result[".db1.orders"].metadata;
        assert_eq!(orders.materialization_type, "table");
        assert_eq!(orders.schema, "db1");
        assert_eq!(orders.name, "orders");
        assert_eq!(orders.database, Some("".to_string()));
        assert_eq!(orders.comment, Some("fact table".to_string()));
        // null table_owner reads back as an empty string
        assert_eq!(orders.owner, Some("".to_string()));

        let view = &result[".db1.orders_v"].metadata;
        assert_eq!(view.materialization_type, "view");
        assert_eq!(view.comment, None);
    }

    #[test]
    fn build_columns_from_get_columns_reads_uint64_positions() {
        let adapter = make_metadata_adapter();
        let result = adapter
            .build_columns_from_get_columns(sample_batch())
            .unwrap();

        let orders = &result[".db1.orders"];
        assert_eq!(orders.len(), 2);
        assert_eq!(orders["id"].index, 1);
        assert_eq!(orders["id"].data_type, "UInt64");
        assert_eq!(orders["id"].comment, None);
        assert_eq!(orders["amount"].index, 2);
        assert_eq!(orders["amount"].data_type, "Decimal(18, 2)");
        assert_eq!(orders["amount"].comment, Some("in cents".to_string()));

        let view = &result[".db1.orders_v"];
        assert_eq!(view.len(), 1);
        assert_eq!(view["id"].index, 1);
    }

    #[test]
    fn catalog_batch_parsers_accept_empty_batches() {
        let adapter = make_metadata_adapter();
        let batch = catalog_batch(&[]);
        assert!(
            adapter
                .build_schemas_from_stats_sql(batch.clone())
                .unwrap()
                .is_empty()
        );
        assert!(
            adapter
                .build_columns_from_get_columns(batch)
                .unwrap()
                .is_empty()
        );
    }

    /// ClickHouse's `system.tables` is case-sensitive on `database` and `name`
    #[test]
    fn build_get_relation_sql_passes_names_through_verbatim() {
        assert_eq!(
            build_get_relation_sql("Mixed_Case", "Stg_Customers"),
            "SELECT engine, name FROM system.tables \
             WHERE database = 'Mixed_Case' AND name = 'Stg_Customers'",
        );
    }

    /// Embedded `'` and `\` must be backslash-escaped per
    /// <https://clickhouse.com/docs/en/sql-reference/syntax#string>, otherwise
    /// the literal terminates early or trails an unbalanced backslash.
    #[test]
    fn build_get_relation_sql_escapes_quotes_and_backslashes() {
        assert_eq!(
            build_get_relation_sql("a'b", r"c\d"),
            r"SELECT engine, name FROM system.tables WHERE database = 'a\'b' AND name = 'c\\d'",
        );
    }
}
