use crate::snowflake::adapter::SnowflakeAdapter;

use crate::errors::{AdapterError, AdapterResult, AsyncAdapterResult};
use crate::metadata::*;
use crate::record_batch_utils::get_column_values;
use crate::sql_types::{TypeOps, make_arrow_field};
use crate::typed_adapter::TypedBaseAdapter;
use crate::{AdapterType, AdapterTyping};
use arrow_array::{
    Array, BooleanArray, Decimal128Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use dbt_common::adapter::ExecutionPhase;
use dbt_common::cancellation::Cancellable;

use arrow_schema::Schema;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::legacy_catalog::{
    CatalogNodeStats, CatalogTable, ColumnMetadata, TableMetadata,
};
use dbt_schemas::schemas::relations::base::{BaseRelation, RelationPattern};
use dbt_xdbc::{Connection, MapReduce, QueryCtx};

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

impl MetadataAdapter for SnowflakeAdapter {
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

        let clustering_key_label =
            get_column_values::<StringArray>(&stats_sql_result, "stats:clustering_key:label")?;
        let clustering_key_value =
            get_column_values::<StringArray>(&stats_sql_result, "stats:clustering_key:value")?;
        let clustering_key_description = get_column_values::<StringArray>(
            &stats_sql_result,
            "stats:clustering_key:description",
        )?;
        let clustering_key_include =
            get_column_values::<BooleanArray>(&stats_sql_result, "stats:clustering_key:include")?;

        let row_count_label =
            get_column_values::<StringArray>(&stats_sql_result, "stats:row_count:label")?;
        let row_count_value =
            get_column_values::<Decimal128Array>(&stats_sql_result, "stats:row_count:value")?;
        let row_count_description =
            get_column_values::<StringArray>(&stats_sql_result, "stats:row_count:description")?;
        let row_count_include =
            get_column_values::<BooleanArray>(&stats_sql_result, "stats:row_count:include")?;

        let bytes_label = get_column_values::<StringArray>(&stats_sql_result, "stats:bytes:label")?;
        let bytes_value =
            get_column_values::<Decimal128Array>(&stats_sql_result, "stats:bytes:value")?;
        let bytes_description =
            get_column_values::<StringArray>(&stats_sql_result, "stats:bytes:description")?;
        let bytes_include =
            get_column_values::<BooleanArray>(&stats_sql_result, "stats:bytes:include")?;

        let last_modified_label =
            get_column_values::<StringArray>(&stats_sql_result, "stats:last_modified:label")?;
        let last_modified_value =
            get_column_values::<StringArray>(&stats_sql_result, "stats:last_modified:value")?;
        let last_modified_description =
            get_column_values::<StringArray>(&stats_sql_result, "stats:last_modified:description")?;
        let last_modified_include =
            get_column_values::<BooleanArray>(&stats_sql_result, "stats:last_modified:include")?;

        let mut result = BTreeMap::<String, CatalogTable>::new();

        for i in 0..table_catalogs.len() {
            let catalog = table_catalogs.value(i).to_string();
            let schema = table_schemas.value(i).to_string();
            let table = table_names.value(i).to_string();
            let data_type = data_types.value(i).to_string();
            let comment = comments.value(i);
            let owner = table_owners.value(i).to_string();

            let fully_qualified_name = format!("{catalog}.{schema}.{table}").to_lowercase();

            if !result.contains_key(&fully_qualified_name) {
                let clustering_key_label_i = clustering_key_label.value(i);
                let clustering_key_value_i = clustering_key_value.value(i);
                let clustering_key_description_i = clustering_key_description.value(i);
                let clustering_key_include_i = clustering_key_include.value(i);

                let row_count_label_i = row_count_label.value(i);
                let row_count_value_i = row_count_value.value(i);
                let row_count_description_i = row_count_description.value(i);
                let row_count_include_i = row_count_include.value(i);

                let bytes_label_i = bytes_label.value(i);
                let bytes_value_i = bytes_value.value(i);
                let bytes_description_i = bytes_description.value(i);
                let bytes_include_i = bytes_include.value(i);

                let last_modified_label_i = last_modified_label.value(i);
                let last_modified_value_i = last_modified_value.value(i);
                let last_modified_description_i = last_modified_description.value(i);
                let last_modified_include_i = last_modified_include.value(i);

                let mut stats = BTreeMap::new();
                if clustering_key_include_i {
                    stats.insert(
                        "clustering_key".to_string(),
                        CatalogNodeStats {
                            id: "clustering_key".to_string(),
                            label: clustering_key_label_i.to_string(),
                            value: serde_json::Value::String(clustering_key_value_i.to_string()),
                            description: Some(clustering_key_description_i.to_string()),
                            include: clustering_key_include_i,
                        },
                    );
                }
                if bytes_include_i {
                    stats.insert(
                        "bytes".to_string(),
                        CatalogNodeStats {
                            id: "bytes".to_string(),
                            label: bytes_label_i.to_string(),
                            value: serde_json::Number::from_i128(bytes_value_i).into(),
                            description: Some(bytes_description_i.to_string()),
                            include: bytes_include_i,
                        },
                    );
                }
                if row_count_include_i {
                    stats.insert(
                        "row_count".to_string(),
                        CatalogNodeStats {
                            id: "row_count".to_string(),
                            label: row_count_label_i.to_string(),
                            value: serde_json::Number::from_i128(row_count_value_i).into(),
                            description: Some(row_count_description_i.to_string()),
                            include: row_count_include_i,
                        },
                    );
                }
                if last_modified_include_i {
                    stats.insert(
                        "last_modified".to_string(),
                        CatalogNodeStats {
                            id: "last_modified".to_string(),
                            label: last_modified_label_i.to_string(),
                            value: serde_json::Value::String(last_modified_value_i.to_string()),
                            description: Some(last_modified_description_i.to_string()),
                            include: last_modified_include_i,
                        },
                    );
                }

                stats.insert(
                    "has_stats".to_string(),
                    CatalogNodeStats {
                        id: "has_stats".to_string(),
                        label: "Has Stats?".to_string(),
                        value: serde_json::Value::Bool(!stats.is_empty()),
                        description: Some(
                            "Indicates whether there are statistics for this table".to_string(),
                        ),
                        include: false,
                    },
                );

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

                let node = CatalogTable {
                    metadata: node_metadata,
                    columns: BTreeMap::new(),
                    stats,
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

        // Can probably zip these into a table metadata tuple array
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

            let column_name_i = column_names.value(i);
            let column_index_i = column_indices.value(i);
            let column_type_i = column_types.value(i);
            let column_comment_i = column_comments.value(i);

            let column = ColumnMetadata {
                name: column_name_i.to_string(),
                index: column_index_i,
                data_type: column_type_i.to_string(),
                comment: match column_comment_i {
                    "" => None,
                    _ => Some(column_comment_i.to_string()),
                },
            };

            columns_by_relation
                .entry(fully_qualified_name.clone())
                .or_insert(BTreeMap::new())
                .insert(column_name_i.to_string(), column);
        }
        Ok(columns_by_relation)
    }

    fn list_user_defined_functions_inner(
        &self,
        catalog_schemas: &BTreeMap<String, BTreeSet<String>>,
    ) -> AsyncAdapterResult<'_, Vec<UDF>> {
        type Acc = Vec<UDF>;

        // https://docs.snowflake.com/en/sql-reference/sql/show-user-functions
        // this is chosen over `information_schema.views` because the latter takes tens of seconds to complete
        // when running against the `ska67070` account
        let queries = catalog_schemas
            .iter()
            .flat_map(|(catalog, schemas)| {
                schemas
                    .iter()
                    .map(move |schema| format!("SHOW USER FUNCTIONS IN SCHEMA {catalog}.{schema}"))
            })
            .collect::<Vec<_>>();

        let adapter = self.clone();
        let new_connection_f = move || {
            adapter
                .new_connection(None, None)
                .map_err(Cancellable::Error)
        };

        let adapter = self.clone();
        let map_f =
            move |conn: &'_ mut dyn Connection, sql: &String| -> AdapterResult<Arc<RecordBatch>> {
                let ctx = QueryCtx::default().with_desc("List user functions");
                let (_, table) = adapter.query(&ctx, conn, sql, None)?;
                let batch = table.original_record_batch();
                Ok(batch)
            };

        let reduce_f = |acc: &mut Acc,
                        _sql: String,
                        batch_res: AdapterResult<Arc<RecordBatch>>|
         -> Result<(), Cancellable<AdapterError>> {
            let batch = batch_res?;

            if batch.num_rows() == 0 {
                return Ok(());
            }

            let catalog_names = get_column_values::<StringArray>(&batch, "catalog_name")?;
            let schema_names = get_column_values::<StringArray>(&batch, "schema_name")?;
            let names = get_column_values::<StringArray>(&batch, "name")?;
            let descriptions = get_column_values::<StringArray>(&batch, "description")?;
            let is_table = get_column_values::<StringArray>(&batch, "is_table_function")?;
            let is_aggregate = get_column_values::<StringArray>(&batch, "is_aggregate")?;
            let language = get_column_values::<StringArray>(&batch, "language")?;
            let arguments = get_column_values::<StringArray>(&batch, "arguments")?;

            // possible values are either "Y" or "N"
            let is_true = |s: &str| s.to_uppercase() == "Y";
            for i in 0..batch.num_rows() {
                let language = language.value(i).to_string();
                if language.to_uppercase() != "SQL" {
                    continue;
                }

                let catalog = catalog_names.value(i).to_string();
                let schema = schema_names.value(i).to_string();
                let name = names.value(i).to_string();

                let description = descriptions.value(i).to_string();
                let is_table = is_true(is_table.value(i));
                let is_aggregate = is_true(is_aggregate.value(i));
                // Data types of the arguments and return value.
                let signature = arguments.value(i).to_string();

                // Snowflake doesn't tell if a function is a window function or not in either
                // `show functions` or `information_schema.functions view`
                let kind = if is_aggregate {
                    UDFKind::Aggregate
                } else if is_table {
                    UDFKind::Table
                } else {
                    UDFKind::Scalar
                };

                let fqn = format!("{catalog}.{schema}.{name}");
                acc.push(UDF {
                    name: fqn,
                    description,
                    signature,
                    adapter_type: AdapterType::Snowflake,
                    kind,
                });
            }

            Ok(())
        };

        let map_reduce = MapReduce::new(
            Box::new(new_connection_f),
            Box::new(map_f),
            Box::new(reduce_f),
            MAX_CONNECTIONS,
        );
        let token = self.cancellation_token();
        map_reduce.run(Arc::new(queries), token)
    }

    fn list_relations_schemas_inner(
        &self,
        unique_id: Option<String>,
        phase: Option<ExecutionPhase>,
        relations: &[Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<'_, HashMap<String, AdapterResult<Arc<Schema>>>> {
        // All results are accumulated in an unordered map
        type Acc = HashMap<String, AdapterResult<Arc<Schema>>>;

        let table_names = relations
            .iter()
            .map(|relation| relation.semantic_fqn())
            .collect::<Vec<_>>();

        let adapter = self.clone(); // clone needed to move it into lambda
        let new_connection_f = Box::new(move || {
            adapter
                .new_connection(None, None)
                .map_err(Cancellable::Error)
        });

        let adapter = self.clone();
        let map_f = move |conn: &'_ mut dyn Connection,
                          table_name: &String|
              -> AdapterResult<Arc<Schema>> {
            let sql = format!("describe table {};", &table_name);
            let mut ctx = QueryCtx::default().with_desc("Get table schema");
            if let Some(node_id) = unique_id.clone() {
                ctx = ctx.with_node_id(&node_id);
            }
            if let Some(phase) = phase {
                ctx = ctx.with_phase(phase.as_str());
            }
            let (_, table) = adapter.query(&ctx, conn, &sql, None)?;
            let batch = table.original_record_batch();
            let schema = build_schema_from_desc_table(batch, adapter.engine().type_ops())?;
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
        let token = self.cancellation_token();
        map_reduce.run(Arc::new(table_names), token)
    }

    /// List relations schemas by patterns (use information schema query)
    fn list_relations_schemas_by_patterns_inner(
        &self,
        relations_pattern: &[RelationPattern],
    ) -> AsyncAdapterResult<'_, Vec<(String, AdapterResult<RelationSchemaPair>)>> {
        // All results are accumulated in a Vec of pairs
        type Acc = Vec<(String, AdapterResult<RelationSchemaPair>)>;

        // Group patterns by database to minimize queries needed
        let mut patterns_by_database = BTreeMap::new();
        for pat in relations_pattern {
            patterns_by_database
                .entry(pat.database.clone())
                .or_insert_with(Vec::new)
                .push(pat);
        }

        let queries = patterns_by_database
            .into_iter()
            .map(|(database, patterns)| {
                // Build the query for all relations in this database
                let predicates = patterns
                    .iter()
                    .map(|pat| {
                        format!(
                            "(TABLE_SCHEMA ILIKE '{}' AND TABLE_NAME ILIKE '{}')",
                            pat.schema_pattern, pat.table_pattern
                        )
                    })
                    .collect::<Vec<_>>();
                let predicates_union = predicates.join(" OR ");
                format!(
                    "SELECT
    TABLE_CATALOG,
    TABLE_SCHEMA,
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE,
    COMMENT
FROM {database}.INFORMATION_SCHEMA.COLUMNS
WHERE {predicates_union}
ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION"
                )
            });

        let adapter = self.clone();
        let new_connection_f = move || {
            adapter
                .new_connection(None, None)
                .map_err(Cancellable::Error)
        };

        // map_f runs the queries, reduce_f decodes the result set and builds the schemas
        let adapter = self.clone();
        let map_f =
            move |conn: &'_ mut dyn Connection, sql: &String| -> AdapterResult<Arc<RecordBatch>> {
                let ctx = QueryCtx::default().with_desc("Get schema by pattern");
                let (_, table) = adapter.query(&ctx, conn, sql, None)?;
                let batch = table.original_record_batch();
                Ok(batch)
            };

        let quoting = self.quoting();

        let adapter = self.clone();
        let reduce_f = move |acc: &mut Acc,
                             _sql: String,
                             batch_res: AdapterResult<Arc<RecordBatch>>|
              -> Result<(), Cancellable<AdapterError>> {
            let batch = batch_res?;
            let mut schemas_from_batch =
                build_schemas_from_information_schema(batch, quoting, adapter.engine().type_ops())?;
            acc.append(&mut schemas_from_batch);
            Ok(())
        };
        let map_reduce = MapReduce::new(
            Box::new(new_connection_f),
            Box::new(map_f),
            Box::new(reduce_f),
            MAX_CONNECTIONS,
        );
        let keys = queries.collect::<Vec<_>>();
        let token = self.cancellation_token();
        map_reduce.run(Arc::new(keys), token)
    }

    fn create_schemas_if_not_exists(
        &self,
        state: &minijinja::State<'_, '_>,
        catalog_schemas: &BTreeMap<String, BTreeSet<String>>,
    ) -> AdapterResult<Vec<(String, String, AdapterResult<()>)>> {
        create_schemas_if_not_exists(Arc::new(self.clone()), state, catalog_schemas)
    }

    fn freshness_inner(
        &self,
        relations: &[Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<'_, BTreeMap<String, MetadataFreshness>> {
        // Build the where clause for all relations grouped by databases
        let (where_clauses_by_database, relations_by_database) =
            match build_relation_clauses(relations) {
                Ok(result) => result,
                Err(e) => {
                    let future = async move { Err(Cancellable::Error(e)) };
                    return Box::pin(future);
                }
            };

        type Acc = BTreeMap<String, MetadataFreshness>;

        let adapter = self.clone();
        let new_connection_f = move || {
            adapter
                .new_connection(None, None)
                .map_err(Cancellable::Error)
        };

        let adapter = self.clone();
        let map_f = move |conn: &'_ mut dyn Connection,
                          database_and_where_clauses: &(String, Vec<String>)|
              -> AdapterResult<Arc<RecordBatch>> {
            let (database, where_clauses) = &database_and_where_clauses;
            // Query to get last modified times
            let sql = format!(
                "SELECT
                table_schema,
                table_name,
                last_altered,
                (table_type = 'VIEW' OR table_type = 'MATERIALIZED VIEW') AS is_view
             FROM {}.INFORMATION_SCHEMA.TABLES
             WHERE {}",
                database,
                where_clauses.join(" OR ")
            );

            let ctx = QueryCtx::default().with_desc("Extracting freshness from information schema");
            let (_adapter_response, agate_table) = adapter.query(&ctx, &mut *conn, &sql, None)?;
            let batch = agate_table.original_record_batch();
            Ok(batch)
        };

        let reduce_f = move |acc: &mut Acc,
                             database_and_where_clauses: (String, Vec<String>),
                             batch_res: AdapterResult<Arc<RecordBatch>>|
              -> Result<(), Cancellable<AdapterError>> {
            let batch = batch_res?;
            let schemas = get_column_values::<StringArray>(&batch, "TABLE_SCHEMA")?;
            let tables = get_column_values::<StringArray>(&batch, "TABLE_NAME")?;
            let timestamps =
                get_column_values::<TimestampMillisecondArray>(&batch, "LAST_ALTERED")?;
            let is_views = get_column_values::<BooleanArray>(&batch, "IS_VIEW")?;

            let (database, _where_clauses) = &database_and_where_clauses;
            for i in 0..batch.num_rows() {
                let schema = schemas.value(i);
                let table = tables.value(i);
                let timestamp = timestamps.value(i);
                let relations = &relations_by_database[database];
                let is_view = is_views.value(i);

                for table_name in find_matching_relation(schema, table, relations)? {
                    acc.insert(
                        table_name,
                        MetadataFreshness::from_millis(timestamp, is_view)?,
                    );
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
        let keys = where_clauses_by_database.into_iter().collect::<Vec<_>>();
        let token = self.cancellation_token();
        map_reduce.run(Arc::new(keys), token)
    }

    /// Reference: https://github.com/dbt-labs/dbt-adapters/blob/f492c919d3bd415bf5065b3cd8cd1af23562feb0/dbt-snowflake/src/dbt/include/snowflake/macros/metadata/list_relations_without_caching.sql
    fn list_relations_in_parallel_inner(
        &self,
        db_schemas: &[CatalogAndSchema],
    ) -> AsyncAdapterResult<'_, BTreeMap<CatalogAndSchema, AdapterResult<RelationVec>>> {
        type Acc = BTreeMap<CatalogAndSchema, AdapterResult<RelationVec>>;
        let adapter = self.clone();
        let new_connection_f = move || {
            adapter
                .new_connection(None, None)
                .map_err(Cancellable::Error)
        };

        let adapter = self.clone();

        let map_f = move |conn: &'_ mut dyn Connection,
                          db_schema: &CatalogAndSchema|
              -> AdapterResult<Vec<Arc<dyn BaseRelation>>> {
            let query_ctx = QueryCtx::default().with_desc("list_relations_in_parallel");
            adapter.list_relations(&query_ctx, conn, db_schema)
        };

        let reduce_f = move |acc: &mut Acc,
                             db_schema: CatalogAndSchema,
                             relations: AdapterResult<Vec<Arc<dyn BaseRelation>>>|
              -> Result<(), Cancellable<AdapterError>> {
            match relations {
                Ok(relations) => {
                    acc.insert(db_schema, Ok(relations));
                    Ok(())
                }
                Err(e) => {
                    // Empty schema error code - no relations in this schema
                    // XXX: The AdapterError struct is not properly being built at the moment, rely on string search for now
                    if e.message().contains("Object does not exist") {
                        acc.insert(db_schema, Ok(Vec::new()));
                        Ok(())
                    } else {
                        // Other errors should be propagated
                        Err(Cancellable::Error(e))
                    }
                }
            }
        };

        let map_reduce = MapReduce::new(
            Box::new(new_connection_f),
            Box::new(map_f),
            Box::new(reduce_f),
            MAX_CONNECTIONS,
        );
        let token = self.cancellation_token();
        map_reduce.run(Arc::new(db_schemas.to_vec()), token)
    }

    fn is_permission_error(&self, e: &AdapterError) -> bool {
        // this is supposed to be using/extended from ANSI SQL standard but I didn't find any Snowflake documentation
        // the magic strings here are from inspecting the results from fs run on a project with a new database,
        // and a weak role that lack permissions to create a database

        // 42501: insufficient privileges
        // 02000: does not exist or not authorized error
        e.sqlstate() == "42501" || e.sqlstate() == "02000"
    }
}

/// reference: https://github.com/sdf-labs/sdf/blob/main/crates/sdf-cli/src/providers/database/snowflake.rs#L177-L178
fn build_schema_from_desc_table(
    show_columns_result: Arc<RecordBatch>,
    type_ops: &dyn TypeOps,
) -> AdapterResult<Arc<Schema>> {
    let column_names = get_column_values::<StringArray>(&show_columns_result, "name")?;
    let data_types = get_column_values::<StringArray>(&show_columns_result, "type")?;
    let comments = get_column_values::<StringArray>(&show_columns_result, "comment")?;
    let nullability = get_column_values::<StringArray>(&show_columns_result, "null?")?;

    let mut fields = vec![];
    for i in 0..show_columns_result.num_rows() {
        let name = column_names.value(i);
        let nullable = nullability.value(i).to_uppercase() == "Y";
        let text_data_type = data_types.value(i);
        let comment = match comments.value(i) {
            "" => None,
            c => Some(c.to_string()),
        };

        let field = make_arrow_field(
            type_ops,
            name.to_string(),
            text_data_type,
            Some(nullable),
            comment,
        )?;
        fields.push(field);
    }

    let schema = Schema::new(fields);
    Ok(Arc::new(schema))
}

#[allow(clippy::type_complexity)]
fn build_schemas_from_information_schema(
    information_schema_result: Arc<RecordBatch>,
    quoting: ResolvedQuoting,
    type_ops: &dyn TypeOps,
) -> AdapterResult<Vec<(String, AdapterResult<RelationSchemaPair>)>> {
    if information_schema_result.num_rows() == 0 {
        return Ok(Vec::new());
    }

    let table_catalogs =
        get_column_values::<StringArray>(&information_schema_result, "TABLE_CATALOG")?;
    let table_schemas =
        get_column_values::<StringArray>(&information_schema_result, "TABLE_SCHEMA")?;
    let table_names = get_column_values::<StringArray>(&information_schema_result, "TABLE_NAME")?;
    let column_names = get_column_values::<StringArray>(&information_schema_result, "COLUMN_NAME")?;
    let data_types = get_column_values::<StringArray>(&information_schema_result, "DATA_TYPE")?;
    let is_nullable = get_column_values::<StringArray>(&information_schema_result, "IS_NULLABLE")?;
    let numeric_precision =
        get_column_values::<Decimal128Array>(&information_schema_result, "NUMERIC_PRECISION")?;
    let numeric_scale =
        get_column_values::<Decimal128Array>(&information_schema_result, "NUMERIC_SCALE")?;
    let comments = get_column_values::<StringArray>(&information_schema_result, "COMMENT")?;

    let mut result = Vec::<(String, AdapterResult<RelationSchemaPair>)>::new();
    let mut current_table = String::new();
    let mut current_fields = Vec::new();
    let mut current_relation: Option<Arc<dyn BaseRelation>> = None;

    for i in 0..information_schema_result.num_rows() {
        let catalog = table_catalogs.value(i);
        let schema = table_schemas.value(i);
        let table = table_names.value(i);
        let fully_qualified_name = format!("{catalog}.{schema}.{table}");

        // If we're starting a new table, save the previous one and start fresh
        if fully_qualified_name != current_table {
            if !current_table.is_empty() {
                let relation_schema: RelationSchemaPair = (
                    current_relation.expect("current_relation should not be None"),
                    Arc::new(Schema::new(current_fields.clone())),
                );
                result.push((current_table.clone(), Ok(relation_schema)));
            }
            current_table = fully_qualified_name;
            current_fields = Vec::new();

            let relation = match crate::relation::create_relation(
                type_ops.adapter_type(),
                catalog.to_string(),
                schema.to_string(),
                Some(table.to_string()),
                None,
                quoting,
            ) {
                Ok(relation) => relation,
                Err(e) => {
                    result.push((current_table, Err(e.into())));
                    return Ok(result);
                }
            };
            current_relation = Some(relation);
        }

        let name = column_names.value(i);
        let data_type = data_types.value(i);
        let nullable = is_nullable.value(i).to_uppercase() == "YES";
        let comment = match comments.value(i) {
            "" => None,
            c => Some(c.to_string()),
        };

        // Handle numeric types
        let data_type = if data_type == "NUMBER" || data_type == "DECIMAL" {
            let (precision, scale) = (
                numeric_precision.value(i).to_string(),
                numeric_scale.value(i).to_string(),
            );
            format!("decimal({precision},{scale})")
        } else {
            data_type.to_string()
        };

        // Add a Schema Field
        let field = match make_arrow_field(
            type_ops,
            name.to_string(),
            &data_type,
            Some(nullable),
            comment,
        ) {
            Ok(field) => field,
            Err(e) => {
                // Place the error in the accumulator output and return immediately
                // instead of trying to read more tables. Progress on the previously
                // read tables is not lost.
                result.push((current_table, Err(e)));
                return Ok(result);
            }
        };
        current_fields.push(field);
    }

    // If there is only 1 table in the query result set, it won't be captured in the loop, so save it at the end
    if !current_table.is_empty() {
        let relation_schema = (
            current_relation.expect("current_relation should not be None"),
            Arc::new(Schema::new(current_fields.clone())),
        );
        result.push((current_table, Ok(relation_schema)));
    }

    Ok(result)
}
