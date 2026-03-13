use crate::relation::do_create_relation;
use crate::sql_types::{TypeOps, make_arrow_field_v2};
use crate::typed_adapter::ConcreteAdapter;
use crate::{AdapterEngine, AdapterTyping};
use crate::{
    AdapterResult, errors::AsyncAdapterResult, metadata::*, record_batch_utils::get_column_values,
};
use arrow_schema::Schema;

use arrow_array::{Array, Decimal128Array, RecordBatch, StringArray};

use dbt_common::adapter::ExecutionPhase;
use dbt_common::cancellation::Cancellable;
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

/// Maximum number of concurrent connections for schema introspection.
const MAX_CONNECTIONS: usize = 4;

pub struct DuckDBMetadataAdapter {
    adapter: ConcreteAdapter,
}

impl DuckDBMetadataAdapter {
    pub fn new(engine: Arc<dyn AdapterEngine>) -> Self {
        let adapter = ConcreteAdapter::new(engine);
        Self { adapter }
    }
}

impl MetadataAdapter for DuckDBMetadataAdapter {
    fn build_schemas_from_stats_sql(
        &self,
        stats_sql_result: Arc<RecordBatch>,
    ) -> AdapterResult<BTreeMap<String, CatalogTable>> {
        if stats_sql_result.num_rows() == 0 {
            return Ok(BTreeMap::new());
        }

        let table_catalogs = get_column_values::<StringArray>(&stats_sql_result, "table_database")?;
        let table_schemas = get_column_values::<StringArray>(&stats_sql_result, "table_schema")?;
        let table_names = get_column_values::<StringArray>(&stats_sql_result, "table_name")?;
        let data_types = get_column_values::<StringArray>(&stats_sql_result, "table_type")?;
        let comments = get_column_values::<StringArray>(&stats_sql_result, "table_comment")?;
        let table_owners = get_column_values::<StringArray>(&stats_sql_result, "table_owner")?;

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

        let table_catalogs = get_column_values::<StringArray>(&stats_sql_result, "table_database")?;
        let table_schemas = get_column_values::<StringArray>(&stats_sql_result, "table_schema")?;
        let table_names = get_column_values::<StringArray>(&stats_sql_result, "table_name")?;

        let column_names = get_column_values::<StringArray>(&stats_sql_result, "column_name")?;
        let column_indices =
            get_column_values::<Decimal128Array>(&stats_sql_result, "column_index")?;
        let column_types = get_column_values::<StringArray>(&stats_sql_result, "column_type")?;
        let column_comments =
            get_column_values::<StringArray>(&stats_sql_result, "column_comment")?;

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
    ) -> AsyncAdapterResult<'_, HashMap<String, AdapterResult<Arc<Schema>>>> {
        type Acc = HashMap<String, AdapterResult<Arc<Schema>>>;

        let table_names = relations
            .iter()
            .map(|relation| relation.semantic_fqn())
            .collect::<Vec<_>>();

        let adapter = self.adapter.clone();
        let new_connection_f = Box::new(move || {
            adapter
                .engine()
                .new_connection(None, None)
                .map_err(Cancellable::Error)
        });

        let adapter = self.adapter.clone();
        let map_f = move |conn: &'_ mut dyn Connection,
                          table_name: &String|
              -> AdapterResult<Arc<Schema>> {
            // Use DESCRIBE to get table schema
            // DuckDB's DESCRIBE returns: column_name, column_type, null, key, default, extra
            let sql = format!("DESCRIBE {};", &table_name);
            let mut ctx = QueryCtx::default().with_desc("Get table schema");
            if let Some(node_id) = unique_id.clone() {
                ctx = ctx.with_node_id(&node_id);
            }
            if let Some(phase) = phase {
                ctx = ctx.with_phase(phase.as_str());
            }
            let (_, table) = adapter.query(&ctx, conn, &sql, None)?;
            let batch = table.original_record_batch();
            let schema = build_schema_from_duckdb_describe(batch, adapter.engine().type_ops())?;
            Ok(schema)
        };

        let reduce_f = |acc: &mut Acc,
                        table_name: String,
                        schema: AdapterResult<Arc<Schema>>|
         -> Result<(), Cancellable<AdapterError>> {
            acc.insert(table_name, schema);
            Ok(())
        };

        let map_reduce = MapReduce::new(
            Box::new(new_connection_f),
            Box::new(map_f),
            Box::new(reduce_f),
            MAX_CONNECTIONS,
        );
        let token = self.adapter.cancellation_token();
        map_reduce.run(Arc::new(table_names), token)
    }

    fn list_relations_schemas_by_patterns_inner(
        &self,
        _patterns: &[RelationPattern],
    ) -> AsyncAdapterResult<'_, Vec<(String, AdapterResult<RelationSchemaPair>)>> {
        todo!("DuckDBAdapter::list_relations_schemas_by_patterns")
    }

    fn freshness_inner(
        &self,
        _relations: &[Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<'_, BTreeMap<String, MetadataFreshness>> {
        todo!("DuckDBAdapter::freshness")
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
    ) -> AsyncAdapterResult<'_, BTreeMap<CatalogAndSchema, AdapterResult<RelationVec>>> {
        type Acc = BTreeMap<CatalogAndSchema, AdapterResult<RelationVec>>;

        let adapter = self.adapter.clone();
        let new_connection_f = move || {
            adapter
                .engine()
                .new_connection(None, None)
                .map_err(Cancellable::Error)
        };

        let adapter = self.adapter.clone();
        let map_f = move |conn: &'_ mut dyn Connection,
                          db_schema: &CatalogAndSchema|
              -> AdapterResult<Vec<Arc<dyn BaseRelation>>> {
            let ctx = QueryCtx::default().with_desc("list_relations_in_parallel");
            list_relations(adapter.engine().as_ref(), &ctx, conn, db_schema)
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
                    // If the schema doesn't exist, treat as empty (no relations).
                    // DuckDB raises "Catalog Error: Schema with name <x> does not exist"
                    if e.message().contains("does not exist") {
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

        let map_reduce = MapReduce::new(
            Box::new(new_connection_f),
            Box::new(map_f),
            Box::new(reduce_f),
            MAX_CONNECTIONS,
        );
        let token = self.adapter.cancellation_token();
        map_reduce.run(Arc::new(db_schemas.to_vec()), token)
    }
}

/// List all relations (tables, views) in a given schema.
///
/// Queries DuckDB's `information_schema.tables` and maps the results to
/// `BaseRelation` objects suitable for populating the adapter relation cache.
pub fn list_relations(
    engine: &dyn AdapterEngine,
    ctx: &QueryCtx,
    conn: &'_ mut dyn Connection,
    db_schema: &CatalogAndSchema,
) -> AdapterResult<Vec<Arc<dyn BaseRelation>>> {
    let query_schema = if engine.quoting().schema {
        db_schema.resolved_schema.clone()
    } else {
        db_schema.resolved_schema.to_lowercase()
    };

    let sql = format!(
        "SELECT table_catalog, table_schema, table_name, table_type \
         FROM information_schema.tables \
         WHERE table_schema = '{query_schema}'"
    );

    let batch = engine.execute(None, conn, ctx, &sql)?;

    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }

    let table_catalogs = get_column_values::<StringArray>(&batch, "table_catalog")?;
    let table_schemas = get_column_values::<StringArray>(&batch, "table_schema")?;
    let table_names = get_column_values::<StringArray>(&batch, "table_name")?;
    let table_types = get_column_values::<StringArray>(&batch, "table_type")?;

    let mut relations = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        let database = table_catalogs.value(i);
        let schema = table_schemas.value(i);
        let name = table_names.value(i);
        // DuckDB table_type values: "BASE TABLE", "VIEW", "LOCAL TEMPORARY"
        let relation_type = match table_types.value(i) {
            "BASE TABLE" => RelationType::Table,
            "VIEW" => RelationType::View,
            "LOCAL TEMPORARY" => RelationType::Table,
            other => RelationType::from_adapter_type(engine.adapter_type(), other),
        };

        let relation = do_create_relation(
            engine.adapter_type(),
            database.to_string(),
            schema.to_string(),
            Some(name.to_string()),
            Some(relation_type),
            engine.quoting(),
        )
        .map_err(|e| AdapterError::new(AdapterErrorKind::Internal, e.to_string()))?;

        relations.push(Arc::from(relation));
    }

    Ok(relations)
}

/// Build an Arrow Schema from DuckDB's DESCRIBE output.
///
/// DuckDB's DESCRIBE returns columns: column_name, column_type, null, key, default, extra
fn build_schema_from_duckdb_describe(
    describe_result: Arc<RecordBatch>,
    type_ops: &dyn TypeOps,
) -> AdapterResult<Arc<Schema>> {
    let column_names = get_column_values::<StringArray>(&describe_result, "column_name")?;
    let data_types = get_column_values::<StringArray>(&describe_result, "column_type")?;
    let nullability = get_column_values::<StringArray>(&describe_result, "null")?;

    let mut fields = vec![];
    for i in 0..describe_result.num_rows() {
        let name = column_names.value(i);
        // DuckDB returns "YES" or "NO" for nullability
        let nullable = nullability.value(i).to_uppercase() == "YES";
        let text_data_type = data_types.value(i);

        let field = make_arrow_field_v2(
            type_ops,
            name.to_string(),
            text_data_type,
            Some(nullable),
            None, // No comment from DESCRIBE
        )?;
        fields.push(field);
    }

    let schema = Schema::new(fields);
    Ok(Arc::new(schema))
}
