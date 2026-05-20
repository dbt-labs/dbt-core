use crate::AdapterEngine;
use crate::adapter::adapter_impl::AdapterImpl;
use crate::connection::AdapterConnectionFactory;
use crate::record_batch::RecordBatchExt;
use crate::relation::do_create_relation;
use crate::sql_types::{TypeOps, make_arrow_field_v2};
use crate::{AdapterResult, errors::AsyncAdapterResult, metadata::*};
use arrow_schema::Schema;

use arrow_array::{Array, Decimal128Array, RecordBatch, StringArray};

use dbt_adapter_core::ExecutionPhase;
use dbt_common::cancellation::Cancellable;
use dbt_common::cancellation::CancellationToken;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::{
    legacy_catalog::{CatalogNodeStats, CatalogTable, ColumnMetadata, TableMetadata},
    relations::base::{BaseRelation, RelationPattern},
};
use dbt_xdbc::{Connection, MapReduce, QueryCtx};
use indexmap::IndexMap;
use minijinja::State;
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
        let column_indices = stats_sql_result.column_values::<Decimal128Array>("column_index")?;
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
                index: column_index,
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
    let query_database = if engine.quoting().schema {
        db_schema.resolved_schema.clone()
    } else {
        db_schema.resolved_schema.to_lowercase()
    };

    let escaped_database = escape_clickhouse_string_literal(&query_database);
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

/// Build an Arrow Schema from ClickHouse's `DESCRIBE TABLE` output.
///
/// ClickHouse `DESCRIBE TABLE` returns columns: name, type, default_type,
/// default_expression, comment, codec_expression, ttl_expression.
fn build_schema_from_clickhouse_describe(
    describe_result: Arc<RecordBatch>,
    type_ops: &dyn TypeOps,
) -> AdapterResult<Arc<Schema>> {
    let column_names = describe_result.column_values::<StringArray>("name")?;
    let data_types = describe_result.column_values::<StringArray>("type")?;

    let mut fields = vec![];
    for i in 0..describe_result.num_rows() {
        let name = column_names.value(i);
        let text_data_type = data_types.value(i);
        // ClickHouse encodes nullability via the `Nullable(...)` wrapper in the
        // type text; the type parser handles that and the Arrow nullable bit is
        // derived from there.
        let field = make_arrow_field_v2(type_ops, name.to_string(), text_data_type, None, None)?;
        fields.push(field);
    }

    let schema = Schema::new(fields);
    Ok(Arc::new(schema))
}
