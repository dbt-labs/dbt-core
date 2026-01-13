use crate::AdapterTyping;
use crate::relation::postgres::PostgresRelation;
use crate::typed_adapter::ConcreteAdapter;
use crate::{AdapterEngine, TypedBaseAdapter};
use crate::{
    AdapterResult, errors::AsyncAdapterResult, metadata::*, record_batch_utils::get_column_values,
};
use arrow_array::{Array, Decimal128Array, RecordBatch, StringArray};
use arrow_schema::Schema;
use dbt_common::adapter::ExecutionPhase;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::{
    legacy_catalog::{CatalogNodeStats, CatalogTable, ColumnMetadata, TableMetadata},
    relations::base::{BaseRelation, RelationPattern},
};
use dbt_xdbc::{Connection, QueryCtx};
use minijinja::State;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

/// DuckDB metadata adapter implementation
///
/// DuckDB uses INFORMATION_SCHEMA for metadata queries (similar to Postgres/MySQL).
/// Key differences from cloud warehouses:
/// - Uses `database.schema.table` naming (database = catalog)
/// - DESCRIBE TABLE returns: column_name, column_type, null, key, default, extra
/// - INFORMATION_SCHEMA.TABLES provides table/view metadata
/// - Case-sensitive identifiers require double quotes
#[warn(dead_code)]
pub struct DuckDBMetadataAdapter {
    adapter: ConcreteAdapter,
}

#[warn(dead_code)]
impl DuckDBMetadataAdapter {
    #[allow(dead_code)]
    pub fn new(engine: Arc<AdapterEngine>) -> Self {
        let adapter = ConcreteAdapter::new(engine);
        Self { adapter }
    }
}

impl MetadataAdapter for DuckDBMetadataAdapter {
    fn adapter(&self) -> &dyn TypedBaseAdapter {
        &self.adapter
    }

    /// Parse table metadata from DuckDB INFORMATION_SCHEMA.TABLES query result
    ///
    /// Expected columns:
    /// - table_database: catalog/database name
    /// - table_schema: schema name
    /// - table_name: table/view name
    /// - table_type: BASE TABLE, VIEW, LOCAL TEMPORARY, etc.
    /// - table_comment: comment/description (may be empty)
    /// - table_owner: owner (may not be available in DuckDB)
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

        // DuckDB's INFORMATION_SCHEMA may not have comments or owners
        // Use empty strings as fallback
        let comments = get_column_values::<StringArray>(&stats_sql_result, "table_comment")
            .unwrap_or_else(|_| {
                let mut builder = arrow::array::StringBuilder::new();
                for _ in 0..stats_sql_result.num_rows() {
                    builder.append_value("");
                }
                builder.finish()
            });

        let table_owners = get_column_values::<StringArray>(&stats_sql_result, "table_owner")
            .unwrap_or_else(|_| {
                let mut builder = arrow::array::StringBuilder::new();
                for _ in 0..stats_sql_result.num_rows() {
                    builder.append_value("");
                }
                builder.finish()
            });

        let mut result = BTreeMap::<String, CatalogTable>::new();

        for i in 0..table_catalogs.len() {
            let catalog = table_catalogs.value(i);
            let schema = table_schemas.value(i);
            let table = table_names.value(i);
            let data_type = data_types.value(i);
            let comment = comments.value(i);
            let owner = table_owners.value(i);

            // DuckDB is case-preserving, so maintain original case
            let fully_qualified_name = format!("{catalog}.{schema}.{table}");

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
                    owner: if owner.is_empty() {
                        None
                    } else {
                        Some(owner.to_string())
                    },
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
                    columns: BTreeMap::new(),
                    stats: BTreeMap::from([("has_stats".to_string(), no_stats)]),
                    unique_id: None,
                };
                result.insert(fully_qualified_name.clone(), node);
            }
        }
        Ok(result)
    }

    /// Parse column metadata from DuckDB query result
    ///
    /// Can be populated from:
    /// 1. INFORMATION_SCHEMA.COLUMNS
    /// 2. DESCRIBE TABLE output
    ///
    /// Expected columns:
    /// - table_database, table_schema, table_name: relation identifiers
    /// - column_name: column name
    /// - column_index: ordinal position
    /// - column_type: data type string
    /// - column_comment: comment (may be empty)
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

        // Comments may not be available
        let column_comments = get_column_values::<StringArray>(&stats_sql_result, "column_comment")
            .unwrap_or_else(|_| {
                let mut builder = arrow::array::StringBuilder::new();
                for _ in 0..stats_sql_result.num_rows() {
                    builder.append_value("");
                }
                builder.finish()
            });

        let mut columns_by_relation = BTreeMap::new();

        for i in 0..table_catalogs.len() {
            let catalog = table_catalogs.value(i);
            let schema = table_schemas.value(i);
            let table = table_names.value(i);

            // Maintain case sensitivity
            let fully_qualified_name = format!("{catalog}.{schema}.{table}");

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

    /// List relation schemas (Arrow schemas) for multiple relations
    ///
    /// Not yet implemented for DuckDB - requires schema introspection
    fn list_relations_schemas_inner(
        &self,
        _unique_id: Option<String>,
        _phase: Option<ExecutionPhase>,
        _relations: &[Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<'_, HashMap<String, AdapterResult<Arc<Schema>>>> {
        let future = async move { todo!("DuckDB's list_relations_schemas") };
        Box::pin(future)
    }

    /// List relation schemas by pattern matching
    ///
    /// Not yet implemented for DuckDB
    fn list_relations_schemas_by_patterns_inner(
        &self,
        _patterns: &[RelationPattern],
    ) -> AsyncAdapterResult<'_, Vec<(String, AdapterResult<RelationSchemaPair>)>> {
        todo!("DuckDBAdapter::list_relations_schemas_by_patterns")
    }

    /// Get freshness information (last modified timestamp)
    ///
    /// Not yet implemented for DuckDB - would need to query system tables
    fn freshness_inner(
        &self,
        _relations: &[Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<'_, BTreeMap<String, MetadataFreshness>> {
        todo!("DuckDBAdapter::freshness")
    }

    /// Create schemas if they don't exist
    ///
    /// DuckDB supports CREATE SCHEMA IF NOT EXISTS
    fn create_schemas_if_not_exists(
        &self,
        _state: &State<'_, '_>,
        _catalog_schemas: &BTreeMap<String, BTreeSet<String>>,
    ) -> AdapterResult<Vec<(String, String, AdapterResult<()>)>> {
        todo!("DuckDBAdapter::create_schemas_if_not_exists")
    }

    /// List relations in parallel for multiple schemas
    ///
    /// Not yet implemented - cache hydration pending
    fn list_relations_in_parallel_inner(
        &self,
        _db_schemas: &[CatalogAndSchema],
    ) -> AsyncAdapterResult<'_, BTreeMap<CatalogAndSchema, AdapterResult<RelationVec>>> {
        // FIXME: Implement cache hydration
        let future = async move { Ok(BTreeMap::new()) };
        Box::pin(future)
    }
}

/// Lists all relations in a DuckDB schema
///
/// Queries INFORMATION_SCHEMA.TABLES to find all tables and views in the specified schema.
/// DuckDB uses standard SQL INFORMATION_SCHEMA similar to Postgres.
///
/// Reference: Similar to Redshift's implementation but adapted for DuckDB's schema
#[allow(dead_code)]
pub fn list_relations(
    adapter: &dyn AdapterTyping,
    ctx: &QueryCtx,
    conn: &'_ mut dyn Connection,
    db_schema: &CatalogAndSchema,
) -> AdapterResult<Vec<Arc<dyn BaseRelation>>> {
    // DuckDB INFORMATION_SCHEMA query
    // table_type values: BASE TABLE, VIEW, LOCAL TEMPORARY
    let sql = format!(
        "SELECT
    table_catalog as database,
    table_name as name,
    table_schema as schema,
    CASE 
        WHEN table_type = 'BASE TABLE' THEN 'table'
        WHEN table_type = 'VIEW' THEN 'view'
        WHEN table_type = 'LOCAL TEMPORARY' THEN 'table'
        ELSE 'table'
    END as type
FROM information_schema.tables
WHERE table_schema = '{}'",
        &db_schema.resolved_schema
    );

    let batch = adapter.engine().execute(None, conn, ctx, &sql)?;

    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }

    let mut relations = Vec::new();

    let table_name = get_column_values::<StringArray>(&batch, "name")?;
    let database_name = get_column_values::<StringArray>(&batch, "database")?;
    let schema_name = get_column_values::<StringArray>(&batch, "schema")?;
    let table_type = get_column_values::<StringArray>(&batch, "type")?;

    for i in 0..batch.num_rows() {
        let table_name = table_name.value(i);
        let database_name = database_name.value(i);
        let schema_name = schema_name.value(i);
        let table_type = table_type.value(i);

        // Use PostgresRelation for DuckDB (they share similar SQL semantics)
        let relation = Arc::new(PostgresRelation::try_new(
            Some(database_name.to_string()),
            Some(schema_name.to_string()),
            Some(table_name.to_string()),
            Some(RelationType::from(table_type)),
            adapter.quoting(),
        )?) as Arc<dyn BaseRelation>;
        relations.push(relation);
    }

    Ok(relations)
}
