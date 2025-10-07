use crate::redshift::adapter::RedshiftAdapter;

use crate::metadata::{
    CatalogAndSchema, RelationVec, RelationsByDb, WhereClausesByDb, find_matching_relation,
    get_input_schema_database_and_table,
};
use crate::{
    AdapterResult, AdapterTyping, TypedBaseAdapter,
    errors::{AdapterError, AdapterErrorKind, AsyncAdapterResult},
    metadata::{
        MAX_CONNECTIONS, MetadataAdapter, MetadataFreshness, RelationSchemaPair,
        create_schemas_if_not_exists,
    },
    record_batch_utils::get_column_values,
};
use arrow::array::{RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::GenericStringType;
use arrow_array::{Array, BooleanArray, Decimal128Array, GenericByteArray, Int32Array, Int64Array};
use arrow_schema::{Field, Schema};
use dbt_common::cancellation::Cancellable;
use dbt_schemas::schemas::legacy_catalog::{
    CatalogNodeStats, CatalogTable, ColumnMetadata, TableMetadata,
};
use dbt_schemas::schemas::relations::base::{BaseRelation, RelationPattern};
use dbt_xdbc::{Connection, MapReduce, QueryCtx};

use std::collections::btree_map::Entry;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

// TODO: see if this can be shared for other adapters

/// Builds and returns ([WhereClausesByDb], [RelationsByDb]) from a list of [BaseRelation]
/// [WhereClausesByDb] maps databases to statements that select their schema+tables in the relation
/// [RelationsByDb] keys the database to the cloned [BaseRelation]
/// We expect a fqn from the relation in format <database>.<schema>.<table>
fn build_relation_clauses_redshift(
    relations: &[Arc<dyn BaseRelation>],
) -> AdapterResult<(WhereClausesByDb, RelationsByDb)> {
    // Build the where clause for all relations grouped by databases
    let mut where_clauses_by_database = BTreeMap::new();
    let mut relations_by_database = BTreeMap::new();
    for relation in relations {
        let (input_schema, database, input_table) = get_input_schema_database_and_table(relation)?;

        where_clauses_by_database
            .entry(database.to_owned())
            .or_insert_with(Vec::new)
            .push(format!(
                "(
                    upper(ns.nspname) = upper('{input_schema}')
                and upper(c.relname) = upper('{input_table}')
                )"
            ));
        relations_by_database
            .entry(database.to_owned())
            .or_insert_with(Vec::new)
            .push(relation.clone());
    }
    Ok((where_clauses_by_database, relations_by_database))
}

impl MetadataAdapter for RedshiftAdapter {
    fn build_schemas_from_stats_sql(
        &self,
        stats_sql_result: Arc<RecordBatch>,
    ) -> AdapterResult<BTreeMap<String, CatalogTable>> {
        if stats_sql_result.num_rows() == 0 {
            return Ok(BTreeMap::new());
        }

        let schema = stats_sql_result.schema();
        let columns = schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<&str>>();
        let contains_stats = columns.contains(&"stats:encoded:label");

        let table_catalogs = get_column_values::<StringArray>(&stats_sql_result, "table_database")?;
        let table_schemas = get_column_values::<StringArray>(&stats_sql_result, "table_schema")?;
        let table_names = get_column_values::<StringArray>(&stats_sql_result, "table_name")?;
        let data_types = get_column_values::<StringArray>(&stats_sql_result, "table_type")?;
        let comments = get_column_values::<StringArray>(&stats_sql_result, "table_comment")?;
        let table_owners = get_column_values::<StringArray>(&stats_sql_result, "table_owner")?;

        if !contains_stats {
            return build_schema_from_stats_sql_without_stats(
                table_catalogs,
                table_schemas,
                table_names,
                data_types,
                comments,
                table_owners,
            );
        }

        let encoded_label =
            get_column_values::<StringArray>(&stats_sql_result, "stats:encoded:label")?;
        let encoded_value =
            get_column_values::<StringArray>(&stats_sql_result, "stats:encoded:value")?;
        let encoded_description =
            get_column_values::<StringArray>(&stats_sql_result, "stats:encoded:description")?;
        let encoded_include =
            get_column_values::<BooleanArray>(&stats_sql_result, "stats:encoded:include")?;

        let diststyle_label =
            get_column_values::<StringArray>(&stats_sql_result, "`stats:diststyle:label")?;
        let diststyle_value =
            get_column_values::<Decimal128Array>(&stats_sql_result, "`stats:diststyle:value")?;
        let diststyle_description =
            get_column_values::<StringArray>(&stats_sql_result, "`stats:diststyle:description")?;
        let diststyle_include =
            get_column_values::<BooleanArray>(&stats_sql_result, "`stats:diststyle:include")?;

        let sortkey1_label =
            get_column_values::<StringArray>(&stats_sql_result, "stats:sortkey1:label")?;
        let sortkey1_value =
            get_column_values::<Decimal128Array>(&stats_sql_result, "stats:sortkey1:value")?;
        let sortkey1_description =
            get_column_values::<StringArray>(&stats_sql_result, "stats:sortkey1:description")?;
        let sortkey1_include =
            get_column_values::<BooleanArray>(&stats_sql_result, "stats:sortkey1:include")?;

        let max_varchar_label =
            get_column_values::<StringArray>(&stats_sql_result, "stats:max_varchar:label")?;
        let max_varchar_value =
            get_column_values::<Int32Array>(&stats_sql_result, "stats:max_varchar:value")?;
        let max_varchar_description =
            get_column_values::<StringArray>(&stats_sql_result, "stats:max_varchar:description")?;
        let max_varchar_include =
            get_column_values::<BooleanArray>(&stats_sql_result, "stats:max_varchar:include")?;

        let sortkey1_enc_label =
            get_column_values::<StringArray>(&stats_sql_result, "stats:sortkey1_enc:label")?;
        let sortkey1_enc_value =
            get_column_values::<StringArray>(&stats_sql_result, "stats:sortkey1_enc:value")?;
        let sortkey1_enc_description =
            get_column_values::<StringArray>(&stats_sql_result, "stats:sortkey1_enc:description")?;
        let sortkey1_enc_include =
            get_column_values::<BooleanArray>(&stats_sql_result, "stats:sortkey1_enc:include")?;

        let sortkey_num_label =
            get_column_values::<StringArray>(&stats_sql_result, "stats:sortkey_num:label")?;
        let sortkey_num_value =
            get_column_values::<Int32Array>(&stats_sql_result, "stats:sortkey_num:value")?;
        let sortkey_num_description =
            get_column_values::<StringArray>(&stats_sql_result, "stats:sortkey_num:description")?;
        let sortkey_num_include =
            get_column_values::<BooleanArray>(&stats_sql_result, "stats:sortkey_num:include")?;

        let size_label = get_column_values::<StringArray>(&stats_sql_result, "stats:size:label")?;
        let size_value = get_column_values::<Int64Array>(&stats_sql_result, "stats:size:value")?;
        let size_description =
            get_column_values::<StringArray>(&stats_sql_result, "stats:size:description")?;
        let size_include =
            get_column_values::<BooleanArray>(&stats_sql_result, "stats:size:include")?;

        let pct_used_label =
            get_column_values::<StringArray>(&stats_sql_result, "stats:pct_used:label")?;
        let pct_used_value =
            get_column_values::<Decimal128Array>(&stats_sql_result, "stats:pct_used:value")?;
        let pct_used_description =
            get_column_values::<StringArray>(&stats_sql_result, "stats:pct_used:description")?;
        let pct_used_include =
            get_column_values::<BooleanArray>(&stats_sql_result, "stats:pct_used:include")?;

        let unsorted_label =
            get_column_values::<StringArray>(&stats_sql_result, "stats:unsorted:label")?;
        let unsorted_value =
            get_column_values::<Decimal128Array>(&stats_sql_result, "stats:unsorted:value")?;
        let unsorted_description =
            get_column_values::<StringArray>(&stats_sql_result, "stats:unsorted:description")?;
        let unsorted_include =
            get_column_values::<BooleanArray>(&stats_sql_result, "stats:unsorted:include")?;

        let stats_off_label =
            get_column_values::<StringArray>(&stats_sql_result, "stats:stats_off:label")?;
        let stats_off_value =
            get_column_values::<Decimal128Array>(&stats_sql_result, "stats:stats_off:value")?;
        let stats_off_description =
            get_column_values::<StringArray>(&stats_sql_result, "stats:stats_off:description")?;
        let stats_off_include =
            get_column_values::<BooleanArray>(&stats_sql_result, "stats:stats_off:include")?;

        let rows_label = get_column_values::<StringArray>(&stats_sql_result, "stats:rows:label")?;
        let rows_value =
            get_column_values::<Decimal128Array>(&stats_sql_result, "stats:rows:value")?;
        let rows_description =
            get_column_values::<StringArray>(&stats_sql_result, "stats:rows:description")?;
        let rows_include =
            get_column_values::<BooleanArray>(&stats_sql_result, "stats:rows:include")?;

        let skew_sortkey1_label =
            get_column_values::<StringArray>(&stats_sql_result, "stats:skew_sortkey1:label")?;
        let skew_sortkey1_value =
            get_column_values::<Decimal128Array>(&stats_sql_result, "stats:skew_sortkey1:value")?;
        let skew_sortkey1_description =
            get_column_values::<StringArray>(&stats_sql_result, "stats:skew_sortkey1:description")?;
        let skew_sortkey1_include =
            get_column_values::<BooleanArray>(&stats_sql_result, "stats:skew_sortkey1:include")?;

        let skew_rows_label =
            get_column_values::<StringArray>(&stats_sql_result, "stats:skew_rows:label")?;
        let skew_rows_value =
            get_column_values::<Decimal128Array>(&stats_sql_result, "stats:skew_rows:value")?;
        let skew_rows_description =
            get_column_values::<StringArray>(&stats_sql_result, "stats:skew_rows:description")?;
        let skew_rows_include =
            get_column_values::<BooleanArray>(&stats_sql_result, "stats:skew_rows:include")?;

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
                let encoded_label_i = encoded_label.value(i);
                let encoded_value_i = encoded_value.value(i);
                let encoded_description_i = encoded_description.value(i);
                let encoded_include_i = encoded_include.value(i);

                let diststyle_label_i = diststyle_label.value(i);
                let diststyle_value_i = diststyle_value.value(i);
                let diststyle_description_i = diststyle_description.value(i);
                let diststyle_include_i = diststyle_include.value(i);

                let sortkey1_label_i = sortkey1_label.value(i);
                let sortkey1_value_i = sortkey1_value.value(i);
                let sortkey1_description_i = sortkey1_description.value(i);
                let sortkey1_include_i = sortkey1_include.value(i);

                let max_varchar_label_i = max_varchar_label.value(i);
                let max_varchar_value_i = max_varchar_value.value(i);
                let max_varchar_description_i = max_varchar_description.value(i);
                let max_varchar_include_i = max_varchar_include.value(i);

                let sortkey1_enc_label_i = sortkey1_enc_label.value(i);
                let sortkey1_enc_value_i = sortkey1_enc_value.value(i);
                let sortkey1_enc_description_i = sortkey1_enc_description.value(i);
                let sortkey1_enc_include_i = sortkey1_enc_include.value(i);

                let sortkey_num_label_i = sortkey_num_label.value(i);
                let sortkey_num_value_i = sortkey_num_value.value(i);
                let sortkey_num_description_i = sortkey_num_description.value(i);
                let sortkey_num_include_i = sortkey_num_include.value(i);

                let size_label_i = size_label.value(i);
                let size_value_i = size_value.value(i);
                let size_description_i = size_description.value(i);
                let size_include_i = size_include.value(i);

                let pct_used_label_i = pct_used_label.value(i);
                let pct_used_value_i = pct_used_value.value(i);
                let pct_used_description_i = pct_used_description.value(i);
                let pct_used_include_i = pct_used_include.value(i);

                let unsorted_label_i = unsorted_label.value(i);
                let unsorted_value_i = unsorted_value.value(i);
                let unsorted_description_i = unsorted_description.value(i);
                let unsorted_include_i = unsorted_include.value(i);

                let stats_off_label_i = stats_off_label.value(i);
                let stats_off_value_i = stats_off_value.value(i);
                let stats_off_description_i = stats_off_description.value(i);
                let stats_off_include_i = stats_off_include.value(i);

                let rows_label_i = rows_label.value(i);
                let rows_value_i = rows_value.value(i);
                let rows_description_i = rows_description.value(i);
                let rows_include_i = rows_include.value(i);

                let skew_sortkey1_label_i = skew_sortkey1_label.value(i);
                let skew_sortkey1_value_i = skew_sortkey1_value.value(i);
                let skew_sortkey1_description_i = skew_sortkey1_description.value(i);
                let skew_sortkey1_include_i = skew_sortkey1_include.value(i);

                let skew_rows_label_i = skew_rows_label.value(i);
                let skew_rows_value_i = skew_rows_value.value(i);
                let skew_rows_description_i = skew_rows_description.value(i);
                let skew_rows_include_i = skew_rows_include.value(i);

                let mut stats = BTreeMap::new();

                if encoded_include_i {
                    stats.insert(
                        "encoded".to_string(),
                        CatalogNodeStats {
                            id: "encoded".to_string(),
                            label: encoded_label_i.to_string(),
                            value: serde_json::Value::String(encoded_value_i.to_string()),
                            description: Some(encoded_description_i.to_string()),
                            include: encoded_include_i,
                        },
                    );
                }

                if diststyle_include_i {
                    stats.insert(
                        "diststyle".to_string(),
                        CatalogNodeStats {
                            id: "diststyle".to_string(),
                            label: diststyle_label_i.to_string(),
                            value: serde_json::Value::String(diststyle_value_i.to_string()),
                            description: Some(diststyle_description_i.to_string()),
                            include: diststyle_include_i,
                        },
                    );
                }

                if sortkey1_include_i {
                    stats.insert(
                        "sortkey1".to_string(),
                        CatalogNodeStats {
                            id: "sortkey1".to_string(),
                            label: sortkey1_label_i.to_string(),
                            value: serde_json::Value::String(sortkey1_value_i.to_string()),
                            description: Some(sortkey1_description_i.to_string()),
                            include: sortkey1_include_i,
                        },
                    );
                }

                if max_varchar_include_i {
                    stats.insert(
                        "max_varchar".to_string(),
                        CatalogNodeStats {
                            id: "max_varchar".to_string(),
                            label: max_varchar_label_i.to_string(),
                            value: serde_json::Value::String(max_varchar_value_i.to_string()),
                            description: Some(max_varchar_description_i.to_string()),
                            include: max_varchar_include_i,
                        },
                    );
                }

                if sortkey1_enc_include_i {
                    stats.insert(
                        "sortkey1_enc".to_string(),
                        CatalogNodeStats {
                            id: "sortkey1_enc".to_string(),
                            label: sortkey1_enc_label_i.to_string(),
                            value: serde_json::Value::String(sortkey1_enc_value_i.to_string()),
                            description: Some(sortkey1_enc_description_i.to_string()),
                            include: sortkey1_enc_include_i,
                        },
                    );
                }

                if sortkey_num_include_i {
                    stats.insert(
                        "sortkey_num".to_string(),
                        CatalogNodeStats {
                            id: "sortkey_num".to_string(),
                            label: sortkey_num_label_i.to_string(),
                            value: serde_json::Value::String(sortkey_num_value_i.to_string()),
                            description: Some(sortkey_num_description_i.to_string()),
                            include: sortkey_num_include_i,
                        },
                    );
                }

                if size_include_i {
                    stats.insert(
                        "size".to_string(),
                        CatalogNodeStats {
                            id: "size".to_string(),
                            label: size_label_i.to_string(),
                            value: serde_json::Value::String(size_value_i.to_string()),
                            description: Some(size_description_i.to_string()),
                            include: size_include_i,
                        },
                    );
                }

                if pct_used_include_i {
                    stats.insert(
                        "pct_used".to_string(),
                        CatalogNodeStats {
                            id: "pct_used".to_string(),
                            label: pct_used_label_i.to_string(),
                            value: serde_json::Value::String(pct_used_value_i.to_string()),
                            description: Some(pct_used_description_i.to_string()),
                            include: pct_used_include_i,
                        },
                    );
                }

                if unsorted_include_i {
                    stats.insert(
                        "unsorted".to_string(),
                        CatalogNodeStats {
                            id: "unsorted".to_string(),
                            label: unsorted_label_i.to_string(),
                            value: serde_json::Value::String(unsorted_value_i.to_string()),
                            description: Some(unsorted_description_i.to_string()),
                            include: unsorted_include_i,
                        },
                    );
                }

                if stats_off_include_i {
                    stats.insert(
                        "stats_off".to_string(),
                        CatalogNodeStats {
                            id: "stats_off".to_string(),
                            label: stats_off_label_i.to_string(),
                            value: serde_json::Value::String(stats_off_value_i.to_string()),
                            description: Some(stats_off_description_i.to_string()),
                            include: stats_off_include_i,
                        },
                    );
                }

                if rows_include_i {
                    stats.insert(
                        "rows".to_string(),
                        CatalogNodeStats {
                            id: "rows".to_string(),
                            label: rows_label_i.to_string(),
                            value: serde_json::Value::String(rows_value_i.to_string()),
                            description: Some(rows_description_i.to_string()),
                            include: rows_include_i,
                        },
                    );
                }

                if skew_sortkey1_include_i {
                    stats.insert(
                        "skew_sortkey1".to_string(),
                        CatalogNodeStats {
                            id: "skew_sortkey1".to_string(),
                            label: skew_sortkey1_label_i.to_string(),
                            value: serde_json::Value::String(skew_sortkey1_value_i.to_string()),
                            description: Some(skew_sortkey1_description_i.to_string()),
                            include: skew_sortkey1_include_i,
                        },
                    );
                }

                if skew_rows_include_i {
                    stats.insert(
                        "skew_rows".to_string(),
                        CatalogNodeStats {
                            id: "skew_rows".to_string(),
                            label: skew_rows_label_i.to_string(),
                            value: serde_json::Value::String(skew_rows_value_i.to_string()),
                            description: Some(skew_rows_description_i.to_string()),
                            include: skew_rows_include_i,
                        },
                    );
                }

                if stats.is_empty() {
                    stats.insert(
                        "has_stats".to_string(),
                        CatalogNodeStats {
                            id: "has_stats".to_string(),
                            label: "has_stats".to_string(),
                            value: serde_json::Value::Bool(false),
                            description: Some(
                                "Indicates whether there are any statistics for this table"
                                    .to_string(),
                            ),
                            include: false,
                        },
                    );
                }

                let node_metadata = TableMetadata {
                    materialization_type: data_type.to_string(),
                    schema: schema.to_string(),
                    name: table.to_string(),
                    database: Some(catalog.to_string()),
                    comment: Some(comment.to_string()),
                    owner: Some(owner.to_string()),
                };

                let node = CatalogTable {
                    metadata: node_metadata,
                    columns: BTreeMap::new(),
                    stats,
                    unique_id: None,
                };
                result.insert(fully_qualified_name, node);
            }
        }

        Ok(result)
    }

    fn build_columns_from_get_columns(
        &self,
        catalog_sql_result: Arc<RecordBatch>,
    ) -> AdapterResult<BTreeMap<String, BTreeMap<String, ColumnMetadata>>> {
        if catalog_sql_result.num_rows() == 0 {
            return Ok(BTreeMap::new());
        }

        let table_catalogs =
            get_column_values::<StringArray>(&catalog_sql_result, "table_database")?;
        let table_schemas = get_column_values::<StringArray>(&catalog_sql_result, "table_schema")?;
        let table_names = get_column_values::<StringArray>(&catalog_sql_result, "table_name")?;

        let column_names = get_column_values::<StringArray>(&catalog_sql_result, "column_name")?;
        let column_indices = get_column_values::<Int32Array>(&catalog_sql_result, "column_index")?;
        let column_types = get_column_values::<StringArray>(&catalog_sql_result, "column_type")?;
        let column_comments =
            get_column_values::<StringArray>(&catalog_sql_result, "column_comment")?;

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
                comment: Some(column_comment.to_string()),
            };

            columns_by_relation
                .entry(fully_qualified_name.clone())
                .or_insert(BTreeMap::new())
                .insert(column_name.to_string(), column);
        }
        Ok(columns_by_relation)
    }

    fn list_relations_schemas(
        &self,
        _unique_id: Option<String>,
        relations: &[Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<'_, HashMap<String, AdapterResult<Arc<Schema>>>> {
        type Acc = HashMap<String, AdapterResult<Arc<Schema>>>;

        let adapter = self.clone();
        let new_connection_f = Box::new(move || {
            adapter
                .new_connection(None, None)
                .map_err(Cancellable::Error)
        });

        let adapter = self.clone();
        let map_f = move |conn: &'_ mut dyn Connection,
                          relation: &Arc<dyn BaseRelation>|
              -> AdapterResult<Arc<Schema>> {
            let catalog = relation.database_as_str()?;
            let schema = relation.schema_as_str()?;
            let identifier = relation.identifier_as_str()?;

            let sql = format!(
                "SELECT column_name, data_type, is_nullable
    FROM SVV_ALL_COLUMNS
    WHERE database_name = '{catalog}'
    AND schema_name = '{schema}'
    AND table_name = '{identifier}'"
            );

            let query_ctx = QueryCtx::new(adapter.adapter_type().to_string())
                .with_sql(sql)
                .with_desc("Get table schema");
            let (_, table) = adapter.query(&mut *conn, &query_ctx, None)?;
            let batch = table.original_record_batch();
            // Build fields from the response
            let mut fields = Vec::new();
            let column_names = get_column_values::<StringArray>(&batch, "column_name")?;
            let data_types = get_column_values::<StringArray>(&batch, "data_type")?;
            let is_nullables = get_column_values::<StringArray>(&batch, "is_nullable")?;

            let table_schema = {
                for i in 0..batch.num_rows() {
                    let name = column_names.value(i);
                    let data_type = data_types.value(i);
                    let is_nullable = is_nullables.value(i) == "YES";

                    let arrow_type = adapter
                        .engine()
                        .type_ops()
                        .parse_into_arrow_type(data_type)?;

                    fields.push(Field::new(name, arrow_type, is_nullable));
                }
                Arc::new(Schema::new(fields))
            };

            if table_schema.fields().is_empty() {
                Err(AdapterError::new(
                    AdapterErrorKind::UnexpectedResult,
                    format!("No schema in SVV_COLUMNS for {catalog}.{schema}.{identifier}"),
                ))
            } else {
                Ok(table_schema)
            }
        };
        let reduce_f = |acc: &mut Acc,
                        relation: Arc<dyn BaseRelation>,
                        schema: AdapterResult<Arc<Schema>>|
         -> Result<(), Cancellable<AdapterError>> {
            acc.insert(relation.semantic_fqn(), schema);
            Ok(())
        };
        let map_reduce = MapReduce::new(
            Box::new(new_connection_f),
            Box::new(map_f),
            Box::new(reduce_f),
            MAX_CONNECTIONS,
        );
        let token = self.cancellation_token();
        map_reduce.run(Arc::new(relations.to_vec()), token)
    }

    fn list_relations_schemas_by_patterns(
        &self,
        _patterns: &[RelationPattern],
    ) -> AsyncAdapterResult<'_, Vec<(String, AdapterResult<RelationSchemaPair>)>> {
        todo!("RedshiftAdapter::list_relations_schemas_by_patterns")
    }

    fn create_schemas_if_not_exists(
        &self,
        state: &minijinja::State<'_, '_>,
        catalog_schemas: &BTreeMap<String, BTreeSet<String>>,
    ) -> AdapterResult<Vec<(String, String, AdapterResult<()>)>> {
        create_schemas_if_not_exists(Arc::new(self.clone()), state, catalog_schemas)
    }

    fn freshness(
        &self,
        relations: &[Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<'_, BTreeMap<String, MetadataFreshness>> {
        let (where_clauses_by_database, relations_by_database) =
            match build_relation_clauses_redshift(relations) {
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
            let (_, where_clauses) = &database_and_where_clauses;
            // Query to get last modified times.
            // Sourced from dbt-core at https://github.com/dbt-labs/dbt-adapters/blob/0d2ad15e54bbb0da1ecadcea661e3338e148a558/dbt-redshift/src/dbt/include/redshift/macros/metadata/relation_last_modified.sql
            // Note that the trim is necessary to remove
            // the trailing whitespace from the schema and identifier columns. During testing
            // it was discovered that these fields are of type `name` which is a kind of padded
            // char type. See the Special Character Types section of the PostgreSQL documentation
            // for more details: https://www.postgresql.org/docs/current/datatype-character.html
            // With this padding, the function find_matching_relation was unable to
            // find the correct relation and ended up causing a "no freshness data available" error.
            let sql = format!(
                "select
                 trim(trailing from ns.nspname) as schema,
                 trim(trailing from c.relname) as identifier,
                 max(qd.start_time) as last_modified,
                 getdate() as snapshotted_at
                 from pg_class c
                 join pg_namespace ns
                     on ns.oid = c.relnamespace
                 join sys_query_detail qd
                     on qd.table_id = c.oid
                 where qd.step_name = 'insert'
                 and (
                 {}
                 )
                 group by 1, 2, 4",
                where_clauses.join(" OR ")
            );

            let query_ctx = QueryCtx::new(adapter.adapter_type().to_string())
                .with_sql(sql)
                .with_desc("Extracting freshness from information schema");
            let (_adapter_response, agate_table) = adapter.query(&mut *conn, &query_ctx, None)?;
            let batch = agate_table.original_record_batch();
            Ok(batch)
        };

        let reduce_f = move |acc: &mut Acc,
                             database_and_where_clauses: (String, Vec<String>),
                             batch_res: AdapterResult<Arc<RecordBatch>>|
              -> Result<(), Cancellable<AdapterError>> {
            let batch = batch_res?;
            let schemas = get_column_values::<StringArray>(&batch, "schema")?;
            let tables = get_column_values::<StringArray>(&batch, "identifier")?;
            let timestamps =
                get_column_values::<TimestampMicrosecondArray>(&batch, "last_modified")?;

            let (database, _where_clauses) = &database_and_where_clauses;
            for i in 0..batch.num_rows() {
                let schema = schemas.value(i);
                let table = tables.value(i);
                let timestamp = timestamps.value(i);
                let relations = &relations_by_database[database];
                let is_view = false; // TODO: figure out if we can get this from the query

                for table_name in find_matching_relation(schema, table, relations)? {
                    acc.insert(
                        table_name,
                        MetadataFreshness::from_micros(timestamp, is_view)?,
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

    fn list_relations_in_parallel(
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
            let query_ctx = QueryCtx::new(adapter.adapter_type().to_string())
                .with_desc("list_relations_in_parallel");
            adapter.list_relations(&query_ctx, conn, db_schema)
        };

        let reduce_f = move |acc: &mut Acc,
                             db_schema: CatalogAndSchema,
                             relations: AdapterResult<Vec<Arc<dyn BaseRelation>>>|
              -> Result<(), Cancellable<AdapterError>> {
            acc.insert(db_schema, relations);
            Ok(())
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
}

fn build_schema_from_stats_sql_without_stats(
    table_catalogs: GenericByteArray<GenericStringType<i32>>,
    table_schemas: GenericByteArray<GenericStringType<i32>>,
    table_names: GenericByteArray<GenericStringType<i32>>,
    data_types: GenericByteArray<GenericStringType<i32>>,
    comments: GenericByteArray<GenericStringType<i32>>,
    table_owners: GenericByteArray<GenericStringType<i32>>,
) -> AdapterResult<BTreeMap<String, CatalogTable>> {
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
                comment: Some(comment.to_string()),
                owner: Some(owner.to_string()),
            };

            let node = CatalogTable {
                metadata: node_metadata,
                columns: BTreeMap::new(),
                stats: BTreeMap::from([(
                    "has_stats".to_string(),
                    CatalogNodeStats {
                        id: "has_stats".to_string(),
                        label: "Has Stats?".to_string(),
                        value: serde_json::Value::Bool(false),
                        description: Some(
                            "Indicates whether there are statistics for this table".to_string(),
                        ),
                        include: false,
                    },
                )]),
                unique_id: None,
            };
            result.insert(fully_qualified_name, node);
        }
    }

    Ok(result)
}
