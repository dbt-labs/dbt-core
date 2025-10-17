use crate::bigquery::adapter::BigqueryAdapter;

use crate::errors::{
    AdapterError, AdapterErrorKind, AdapterResult, AsyncAdapterResult, adbc_error_to_adapter_error,
};
use crate::metadata::*;
use crate::record_batch_utils::get_column_values;
use crate::{AdapterTyping, TypedBaseAdapter};
use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaBuilder, TimeUnit};
use dbt_common::cancellation::Cancellable;
use dbt_schemas::schemas::legacy_catalog::{
    CatalogNodeStats, CatalogTable, ColumnMetadata, TableMetadata,
};
use dbt_schemas::schemas::relations::base::{BaseRelation, RelationPattern};
use dbt_xdbc::query_ctx::ExecutionPhase;
use dbt_xdbc::{Connection, MapReduce, QueryCtx};

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

// The following views always need to be qualified with a dataset or a region (but not both!)
//
// See: https://cloud.google.com/bigquery/docs/information-schema-intro#dataset_qualifier
const DATASET_OR_REGION_VIEWS: &[&str] = &[
    "COLUMNS",
    "COLUMN_FIELD_PATHS",
    "MATERIALIZED_VIEWS",
    "PARAMETERS",
    "PARTITIONS",
    "ROUTINES",
    "ROUTINE_OPTIONS",
    "TABLES",
    "TABLE_OPTIONS",
    "VIEWS",
];

// The following views always need to be qualified with a dataset but not region
//
// See: https://cloud.google.com/bigquery/docs/information-schema-intro#region_qualifier
const DATASET_ONLY_VIEWS: &[&str] = &[
    "PARTITIONS",
    "SEARCH_INDEXES",
    "SEARCH_INDEX_COLUMNS",
    "SEARCH_INDEX_OPTIONS",
];

// Generate the fully qualified name of a BigQuery INFORMATION_SCHEMA table.
//
// FIXME(serramatutu): This logic can (and will) fail, in certain edge cases when
// the user provides FQN like `project.region.INFORMATION_SCHEMA.VIEW` or
// `dataset.INFORMATION_SCHEMA.VIEW`. This is perfectly legal in BigQuery, but our
// relation parsing upstream it freaks out in some edge cases.
// See: https://github.com/dbt-labs/fs/issues/4917
//
// NOTE: On the necessity of the `region` qualifier, per BigQuery's docs:
// - You MUST specify a region to query _some_ views in `INFORMATION_SCHEMA` [1]
// - Some other views (like `TABLES`) either need region or dataset [2]
// - Generally, if you don't specify a region, the engine defaults to
// the US macro location (which might be routed to any region within the US) [3]
//
// [1] https://cloud.google.com/bigquery/docs/information-schema-intro#syntax
// [2] https://cloud.google.com/bigquery/docs/information-schema-intro#dataset_qualifier
// [3] https://cloud.google.com/bigquery/docs/locations#specify_locations
fn generate_system_table_fqn(
    project: &str,
    table: &str,
    user_preferred_region: Option<&str>,
) -> String {
    let sys_identifier = table.to_uppercase();

    if DATASET_ONLY_VIEWS.contains(&sys_identifier.as_ref()) {
        format!("{project}.INFORMATION_SCHEMA.{sys_identifier}")
    } else if DATASET_OR_REGION_VIEWS.contains(&sys_identifier.as_ref()) {
        // respect user's location preferences by querying the region directly if
        // possible
        match user_preferred_region {
            None => format!("{project}.INFORMATION_SCHEMA.{sys_identifier}"),
            Some(region) => format!("`region-{region}`.INFORMATION_SCHEMA.{sys_identifier}"),
        }
    } else {
        // All other tables NEED to be qualified with the region otherwise the query will fail
        let region = user_preferred_region.unwrap_or("us");
        format!("`region-{region}`.INFORMATION_SCHEMA.{sys_identifier}")
    }
}

impl MetadataAdapter for BigqueryAdapter {
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

        let date_shards_label =
            get_column_values::<StringArray>(&stats_sql_result, "stats__date_shards__label")?;
        let date_shards_value =
            get_column_values::<Int64Array>(&stats_sql_result, "stats__date_shards__value")?;
        let date_shards_description =
            get_column_values::<StringArray>(&stats_sql_result, "stats__date_shards__description")?;
        let date_shards_include =
            get_column_values::<BooleanArray>(&stats_sql_result, "stats__date_shards__include")?;

        let date_shard_min_label =
            get_column_values::<StringArray>(&stats_sql_result, "stats__date_shard_min__label")?;
        let date_shard_min_value =
            get_column_values::<StringArray>(&stats_sql_result, "stats__date_shard_min__value")?;
        let date_shard_min_description = get_column_values::<StringArray>(
            &stats_sql_result,
            "stats__date_shard_min__description",
        )?;
        let date_shard_min_include =
            get_column_values::<BooleanArray>(&stats_sql_result, "stats__date_shard_min__include")?;

        let date_shard_max_label =
            get_column_values::<StringArray>(&stats_sql_result, "stats__date_shard_max__label")?;
        let date_shard_max_value =
            get_column_values::<StringArray>(&stats_sql_result, "stats__date_shard_max__value")?;
        let date_shard_max_description = get_column_values::<StringArray>(
            &stats_sql_result,
            "stats__date_shard_max__description",
        )?;
        let date_shard_max_include =
            get_column_values::<BooleanArray>(&stats_sql_result, "stats__date_shard_max__include")?;

        let num_rows_label =
            get_column_values::<StringArray>(&stats_sql_result, "stats__num_rows__label")?;
        let num_rows_value =
            get_column_values::<Int64Array>(&stats_sql_result, "stats__num_rows__value")?;
        let num_rows_description =
            get_column_values::<StringArray>(&stats_sql_result, "stats__num_rows__description")?;
        let num_rows_include =
            get_column_values::<BooleanArray>(&stats_sql_result, "stats__num_rows__include")?;

        let bytes_label =
            get_column_values::<StringArray>(&stats_sql_result, "stats__num_bytes__label")?;
        let bytes_value =
            get_column_values::<Int64Array>(&stats_sql_result, "stats__num_bytes__value")?;
        let bytes_description =
            get_column_values::<StringArray>(&stats_sql_result, "stats__num_bytes__description")?;
        let bytes_include =
            get_column_values::<BooleanArray>(&stats_sql_result, "stats__num_bytes__include")?;

        let partition_type_label =
            get_column_values::<StringArray>(&stats_sql_result, "stats__partitioning_type__label")?;
        let partition_type_value =
            get_column_values::<StringArray>(&stats_sql_result, "stats__partitioning_type__value")?;
        let partition_type_description = get_column_values::<StringArray>(
            &stats_sql_result,
            "stats__partitioning_type__description",
        )?;
        let partition_type_include = get_column_values::<BooleanArray>(
            &stats_sql_result,
            "stats__partitioning_type__include",
        )?;

        let clustering_fields_label =
            get_column_values::<StringArray>(&stats_sql_result, "stats__clustering_fields__label")?;
        let clustering_fields_value =
            get_column_values::<StringArray>(&stats_sql_result, "stats__clustering_fields__value")?;
        let clustering_fields_description = get_column_values::<StringArray>(
            &stats_sql_result,
            "stats__clustering_fields__description",
        )?;
        let clustering_fields_include = get_column_values::<BooleanArray>(
            &stats_sql_result,
            "stats__clustering_fields__include",
        )?;

        let mut result = BTreeMap::<String, CatalogTable>::new();

        for i in 0..table_catalogs.len() {
            let catalog = table_catalogs.value(i);
            let schema = table_schemas.value(i);
            let table = table_names.value(i);
            let data_type = data_types.value(i);
            let comment = comments.value(i);

            let fully_qualified_name = format!("{catalog}.{schema}.{table}").to_lowercase();

            let entry = result.entry(fully_qualified_name.clone());

            if matches!(entry, Entry::Vacant(_)) {
                let date_shards_label_i = date_shards_label.value(i);
                let date_shards_value_i = date_shards_value.value(i);
                let date_shards_description_i = date_shards_description.value(i);
                let date_shards_include_i = date_shards_include.value(i);

                let date_shard_min_label_i = date_shard_min_label.value(i);
                let date_shard_min_value_i = date_shard_min_value.value(i);
                let date_shard_min_description_i = date_shard_min_description.value(i);
                let date_shard_min_include_i = date_shard_min_include.value(i);

                let date_shard_max_label_i = date_shard_max_label.value(i);
                let date_shard_max_value_i = date_shard_max_value.value(i);
                let date_shard_max_description_i = date_shard_max_description.value(i);
                let date_shard_max_include_i = date_shard_max_include.value(i);

                let num_rows_label_i = num_rows_label.value(i);
                let num_rows_value_i = num_rows_value.value(i);
                let num_rows_description_i = num_rows_description.value(i);
                let num_rows_include_i = num_rows_include.value(i);

                let bytes_label_i = bytes_label.value(i);
                let bytes_value_i = bytes_value.value(i);
                let bytes_description_i = bytes_description.value(i);
                let bytes_include_i = bytes_include.value(i);

                let partition_type_label_i = partition_type_label.value(i);
                let partition_type_value_i = partition_type_value.value(i);
                let partition_type_description_i = partition_type_description.value(i);
                let partition_type_include_i = partition_type_include.value(i);

                let clustering_fields_label_i = clustering_fields_label.value(i);
                let clustering_fields_value_i = clustering_fields_value.value(i);
                let clustering_fields_description_i = clustering_fields_description.value(i);
                let clustering_fields_include_i = clustering_fields_include.value(i);

                let mut stats = BTreeMap::new();

                if date_shards_include_i {
                    stats.insert(
                        "date_shards".to_string(),
                        CatalogNodeStats {
                            id: "date_shards".to_string(),
                            label: date_shards_label_i.to_string(),
                            value: serde_json::Value::String(date_shards_value_i.to_string()),
                            description: Some(date_shards_description_i.to_string()),
                            include: date_shards_include_i,
                        },
                    );
                }
                if date_shard_min_include_i {
                    stats.insert(
                        "date_shard_min".to_string(),
                        CatalogNodeStats {
                            id: "date_shard_min".to_string(),
                            label: date_shard_min_label_i.to_string(),
                            value: serde_json::Value::String(date_shard_min_value_i.to_string()),
                            description: Some(date_shard_min_description_i.to_string()),
                            include: date_shard_min_include_i,
                        },
                    );
                }
                if date_shard_max_include_i {
                    stats.insert(
                        "date_shard_max".to_string(),
                        CatalogNodeStats {
                            id: "date_shard_max".to_string(),
                            label: date_shard_max_label_i.to_string(),
                            value: serde_json::Value::String(date_shard_max_value_i.to_string()),
                            description: Some(date_shard_max_description_i.to_string()),
                            include: date_shard_max_include_i,
                        },
                    );
                }
                if num_rows_include_i {
                    stats.insert(
                        "num_rows".to_string(),
                        CatalogNodeStats {
                            id: "num_rows".to_string(),
                            label: num_rows_label_i.to_string(),
                            value: serde_json::Value::String(num_rows_value_i.to_string()),
                            description: Some(num_rows_description_i.to_string()),
                            include: num_rows_include_i,
                        },
                    );
                }
                if bytes_include_i {
                    stats.insert(
                        "bytes".to_string(),
                        CatalogNodeStats {
                            id: "bytes".to_string(),
                            label: bytes_label_i.to_string(),
                            value: serde_json::Value::String(bytes_value_i.to_string()),
                            description: Some(bytes_description_i.to_string()),
                            include: bytes_include_i,
                        },
                    );
                }
                if partition_type_include_i {
                    stats.insert(
                        "partition_type".to_string(),
                        CatalogNodeStats {
                            id: "partition_type".to_string(),
                            label: partition_type_label_i.to_string(),
                            value: serde_json::Value::String(partition_type_value_i.to_string()),
                            description: Some(partition_type_description_i.to_string()),
                            include: partition_type_include_i,
                        },
                    );
                }
                if clustering_fields_include_i {
                    stats.insert(
                        "clustering_fields".to_string(),
                        CatalogNodeStats {
                            id: "clustering_fields".to_string(),
                            label: clustering_fields_label_i.to_string(),
                            value: serde_json::Value::String(clustering_fields_value_i.to_string()),
                            description: Some(clustering_fields_description_i.to_string()),
                            include: clustering_fields_include_i,
                        },
                    );
                }
                if stats.is_empty() {
                    stats.insert(
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
                    );
                }

                let node_metadata = TableMetadata {
                    materialization_type: data_type.to_string(),
                    schema: schema.to_string(),
                    name: table.to_string(),
                    database: Some(catalog.to_string()),
                    comment: Some(comment.to_string()),
                    owner: None,
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
        let column_indices = get_column_values::<Int64Array>(&catalog_sql_result, "column_index")?;
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
        unique_id: Option<String>,
        _phase: Option<ExecutionPhase>,
        relations: &[Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<'_, HashMap<String, AdapterResult<Arc<Schema>>>> {
        // All results are accumulated in an unordered map
        type Acc = HashMap<String, AdapterResult<Arc<Schema>>>;

        let adapter: BigqueryAdapter = self.clone(); // clone needed to move it into lambda
        let new_connection_f = Box::new(move || {
            // FIXME(harry): this is not taking into account that we will have multiple connections open
            // when relations are more than one, do we want to enforce single connection in record and replay tests?
            let node_id = unique_id.clone().unwrap_or_else(|| "sources".to_string());
            adapter
                .new_connection(None, Some(node_id))
                .map_err(Cancellable::Error)
        });

        let adapter = self.clone();
        let map_f = move |conn: &'_ mut dyn Connection,
                          relation: &Arc<dyn BaseRelation>|
              -> AdapterResult<Arc<Schema>> {
            let project = relation.database_as_resolved_str()?;
            let dataset = relation.schema_as_resolved_str()?;
            let table = relation.identifier_as_resolved_str()?;

            // To download the schemas of the Information schema tables
            // we cannot use `get_table_schema` (since the adbc connection, via the googleapi doesn't support this)
            // and we cannot query a the COLUMNS INFORMATION_SCHEMA view either
            // The workaround is to issue a query that returns the minimum data, then use returns the Arrow schema of the batch
            // TODO(jason): This needs to be resolved within the driver itself - querying this way returns IPC directly from the
            // storage API within the driver where it's currently not annotated with the original type text
            if relation.is_system() {
                let project = relation.database_as_quoted_str()?;

                let user_preferred_region = adapter
                    .engine()
                    .config("location")
                    .map(|cfg| cfg.to_lowercase());

                let table_fqn =
                    generate_system_table_fqn(&project, &table, user_preferred_region.as_deref());
                let sql = format!("SELECT * FROM {table_fqn} LIMIT 0");

                let ctx = QueryCtx::default().with_desc("Get table schema");
                let (_, agate_table) = adapter.query(&ctx, &mut *conn, &sql, None)?;
                let batch = agate_table.original_record_batch();

                let schema = batch.schema();
                if schema.fields().is_empty() {
                    Err(AdapterError::new(
                        AdapterErrorKind::UnexpectedResult,
                        format!("BigQuery driver returned no schema for {table_fqn}"),
                    ))
                } else {
                    Ok(schema)
                }
            } else {
                let schema = conn
                    .get_table_schema(Some(&project), Some(&dataset), &table)
                    .map_err(adbc_error_to_adapter_error)?;
                let mut schema_builder = SchemaBuilder::from(schema.fields());

                if let Some(time_partitioning_type) = schema.metadata().get("TimePartitioning.Type")
                {
                    schema_builder.push(Field::new(
                        "_PARTITIONTIME",
                        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                        true,
                    ));
                    if time_partitioning_type == "DAY" {
                        schema_builder.push(Field::new("_PARTITIONDATE", DataType::Date32, true));
                    }
                }

                Ok(Arc::new(schema_builder.finish()))
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
        todo!("BigqueryAdapter::list_relations_schemas_by_patterns")
    }

    fn freshness(
        &self,
        _relations: &[Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<'_, BTreeMap<String, MetadataFreshness>> {
        // FIXME: implement this
        println!("WARNING: BigqueryAdapter::freshness is not implemented");
        let future = async move { Ok(BTreeMap::new()) };
        Box::pin(future)
    }

    fn create_schemas_if_not_exists(
        &self,
        state: &minijinja::State<'_, '_>,
        catalog_schemas: &BTreeMap<String, BTreeSet<String>>,
    ) -> AdapterResult<Vec<(String, String, AdapterResult<()>)>> {
        create_schemas_if_not_exists(Arc::new(self.clone()), state, catalog_schemas)
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
            // Deviation from core: we cannot use `list_tables` as this is not supported from ADBC
            // Pagination is handled in the ADBC driver
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
                    // Empty schema error code
                    // XXX: The AdapterError struct is not properly being built at the moment, rely on string search for now
                    if e.message().contains("Error 404: Not found:") {
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

    /// Check if the returned error is due to insufficient permissions.
    fn is_permission_error(&self, _e: &AdapterError) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_system_table_fqn_always_dataset_only() {
        let dataset_only_view = "PARTITIONS";
        assert_eq!(
            generate_system_table_fqn("`my-project`", dataset_only_view, None),
            "`my-project`.INFORMATION_SCHEMA.PARTITIONS"
        );
        assert_eq!(
            generate_system_table_fqn("`my-project`", dataset_only_view, Some("eu")),
            "`my-project`.INFORMATION_SCHEMA.PARTITIONS"
        );
    }

    #[test]
    fn test_generate_system_table_fqn_dataset_or_region() {
        // FIXME: sometimes the actual dataset reaches this method as if it were a part of
        // the project due to our upstream relation parsing.
        //
        // See: https://github.com/dbt-labs/fs/issues/4917

        let dataset_or_region_view = "TABLES";

        assert_eq!(
            generate_system_table_fqn("`my_dataset`", dataset_or_region_view, None),
            "`my_dataset`.INFORMATION_SCHEMA.TABLES"
        );
        // prefer user's region settings if specified
        assert_eq!(
            generate_system_table_fqn("`my_dataset`", dataset_or_region_view, Some("eu")),
            "`region-eu`.INFORMATION_SCHEMA.TABLES"
        );
    }

    #[test]
    fn test_generate_system_table_fqn_region_only() {
        // FIXME: sometimes the actual dataset reaches this method as if it were a part of
        // the project due to our upstream relation parsing.
        //
        // See: https://github.com/dbt-labs/fs/issues/4917

        let region_only_view = "JOBS";

        // use US as the default region if the user hasn't specified one
        assert_eq!(
            generate_system_table_fqn("`my_dataset`", region_only_view, None),
            "`region-us`.INFORMATION_SCHEMA.JOBS"
        );
        // prefer user's region settings if specified
        assert_eq!(
            generate_system_table_fqn("`my_dataset`", region_only_view, Some("eu")),
            "`region-eu`.INFORMATION_SCHEMA.JOBS"
        );
    }
}
