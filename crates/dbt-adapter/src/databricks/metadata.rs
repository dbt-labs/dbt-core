use crate::databricks::adapter::DatabricksAdapter;
use crate::databricks::describe_table::DatabricksTableMetadata;
use crate::databricks::version::DbrVersion;

use crate::errors::{AdapterError, AdapterResult, AsyncAdapterResult};
use crate::metadata::*;
use crate::metadata::{build_relation_clauses, find_matching_relation};
use crate::record_batch_utils::get_column_values;
use crate::sql_types::{TypeOps, make_arrow_field_v2};
use crate::{AdapterTyping, TypedBaseAdapter};
use arrow_array::{
    Array, BooleanArray, Int32Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{Field, Schema};
use dbt_common::adapter::ExecutionPhase;
use dbt_common::cancellation::Cancellable;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::legacy_catalog::{
    CatalogNodeStats, CatalogTable, ColumnMetadata, TableMetadata,
};
use dbt_schemas::schemas::relations::base::{BaseRelation, RelationPattern};
use dbt_xdbc::{Connection, MapReduce, QueryCtx};

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future;
use std::sync::Arc;

fn get_relation_with_quote_policy(
    relation: &Arc<dyn BaseRelation>,
) -> AdapterResult<(String, String, String)> {
    let database = relation.database_as_str()?;
    let schema = relation.schema_as_str()?;
    let identifier = relation.identifier_as_str()?;

    let quote_char = relation.quote_character();

    let quoted_database = if relation.quote_policy().database {
        format!("{quote_char}{database}{quote_char}")
    } else {
        database
    };
    let quoted_schema = if relation.quote_policy().schema {
        format!("{quote_char}{schema}{quote_char}")
    } else {
        schema
    };
    let quoted_identifier = if relation.quote_policy().identifier {
        format!("{quote_char}{identifier}{quote_char}")
    } else {
        identifier
    };

    Ok((quoted_database, quoted_schema, quoted_identifier))
}

impl MetadataAdapter for DatabricksAdapter {
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

        let last_modified_label =
            get_column_values::<StringArray>(&stats_sql_result, "stats:last_modified:label")?;
        let last_modified_value = get_column_values::<TimestampMicrosecondArray>(
            &stats_sql_result,
            "stats:last_modified:value",
        )?;
        let last_modified_description =
            get_column_values::<StringArray>(&stats_sql_result, "stats:last_modified:description")?;
        let last_modified_include =
            get_column_values::<BooleanArray>(&stats_sql_result, "stats:last_modified:include")?;
        let mut result = BTreeMap::<String, CatalogTable>::new();

        for i in 0..table_catalogs.len() {
            let catalog = table_catalogs.value(i);
            let schema = table_schemas.value(i);
            let table = table_names.value(i);
            let data_type = data_types.value(i);
            let comment = comments.value(i);
            let owner = table_owners.value(i);

            let fully_qualified_name = format!("{catalog}.{schema}.{table}").to_lowercase();

            if !result.contains_key(&fully_qualified_name) {
                let mut stats = BTreeMap::new();
                if last_modified_include.value(i) {
                    stats.insert(
                        "last_modified".to_string(),
                        CatalogNodeStats {
                            id: "last_modified".to_string(),
                            label: last_modified_label.value(i).to_string(),
                            value: serde_json::Value::String(
                                last_modified_value.value(i).to_string(),
                            ),
                            description: Some(last_modified_description.value(i).to_string()),
                            include: last_modified_include.value(i),
                        },
                    );
                }

                stats.insert(
                    "has_stats".to_string(),
                    CatalogNodeStats {
                        id: "has_stats".to_string(),
                        label: "has_stats".to_string(),
                        value: serde_json::Value::Bool(stats.is_empty()),
                        description: Some("Has stats".to_string()),
                        include: last_modified_include.value(i),
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

        let table_catalogs = get_column_values::<StringArray>(&stats_sql_result, "table_database")?;
        let table_schemas = get_column_values::<StringArray>(&stats_sql_result, "table_schema")?;
        let table_names = get_column_values::<StringArray>(&stats_sql_result, "table_name")?;

        let column_names = get_column_values::<StringArray>(&stats_sql_result, "column_name")?;
        let column_indices = get_column_values::<Int32Array>(&stats_sql_result, "column_index")?;
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

    fn list_relations_schemas(
        &self,
        _unique_id: Option<String>,
        _phase: Option<ExecutionPhase>,
        relations: &[Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<'_, HashMap<String, AdapterResult<Arc<Schema>>>> {
        type Acc = HashMap<String, AdapterResult<Arc<Schema>>>;

        let dbr_version = match self.dbr_version() {
            Ok(version) => version,
            Err(e) => {
                return Box::pin(future::ready(Err(Cancellable::Error(e))));
            }
        };

        let adapter = self.clone(); // clone needed to move it into lambda
        let new_connection_f = Box::new(move || {
            adapter
                .new_connection(None, None)
                .map_err(Cancellable::Error)
        });

        let adapter = self.clone();
        let map_f = move |conn: &'_ mut dyn Connection,
                          relation: &Arc<dyn BaseRelation>|
              -> AdapterResult<Arc<Schema>> {
            let (database, schema, identifier) = get_relation_with_quote_policy(relation)?;

            // Databricks system tables doesn't support `DESCRIBE TABLE EXTENDED .. AS JSON` :(
            // More details refer to the test adapter::repros::databricks_use_system_relations

            // NB(cwalden):
            //  It appears that specifically EXTERNAL system tables do not support AS JSON
            //  `is_system()` is actually a bit too broad and unnecessarily excludes some system tables
            //  that do support AS JSON.
            //
            //  Checking the relation_type will be sufficient enough to fix bug raised in `dbt-fusion#543`
            //  but we will need to revisit this when we add cluster support (see `fs#5135`).

            let relation_type = relation.relation_type().or_else(|| {
                // system.information_schema tables are known to be External types
                // This fallback is necessary since schema hydration is not guaranteed to be called
                // by fully resolved relations
                let d = relation.database_as_str().ok()?;
                let s = relation.schema_as_str().ok()?;
                (d.to_lowercase() == "system" && s.to_lowercase() == "information_schema")
                    .then_some(RelationType::External)
            });

            let is_external_system =
                relation.is_system() && matches!(relation_type, Some(RelationType::External));

            let as_json_unsupported = is_external_system || dbr_version < DbrVersion::Full(16, 2);

            let sql = if as_json_unsupported {
                format!("DESCRIBE TABLE {database}.{schema}.{identifier};")
            } else {
                format!("DESCRIBE TABLE EXTENDED {database}.{schema}.{identifier} AS JSON;")
            };

            let ctx = QueryCtx::default().with_desc("Get table schema");
            let (_, table) = adapter.query(&ctx, conn, &sql, None)?;
            let batch = table.original_record_batch();

            let schema = if as_json_unsupported {
                build_schema_from_basic_describe_table(batch, adapter.engine().type_ops())?
            } else {
                let json_metadata = DatabricksTableMetadata::from_record_batch(batch)?;
                json_metadata.to_arrow_schema(adapter.engine().type_ops())?
            };
            Ok(schema)
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
        todo!("DatabricksAdapter::list_relations_schemas_by_patterns")
    }

    fn freshness(
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
                (table_type = 'VIEW' OR table_type = 'MATERIALIZED_VIEW') AS is_view
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
            let schemas = get_column_values::<StringArray>(&batch, "table_schema")?;
            let tables = get_column_values::<StringArray>(&batch, "table_name")?;
            let timestamps =
                get_column_values::<TimestampMicrosecondArray>(&batch, "last_altered")?;
            let is_views = get_column_values::<BooleanArray>(&batch, "is_view")?;

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
            let query_ctx = QueryCtx::default().with_desc("list_relations_in_parallel (UC)");
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

    fn is_permission_error(&self, e: &AdapterError) -> bool {
        // 42501: insufficient privileges
        // Databricks doesn't provide an explicit enough SQLSTATE, noticed most of their errors' SQLSTATE is HY000
        // so we have to match on the error message below.
        // By the time of writing down this note, it is a problem from their backend thus not something we can fix on the SDK or driver layer
        // check out data/repros/databricks_create_schema_no_catalog_access on how to repro this error
        e.sqlstate() == "42501" || e.message().contains("PERMISSION_DENIED")
    }
}

/// Build a schema from `describe table [table]` (without extended ... as json)
fn build_schema_from_basic_describe_table(
    batch: Arc<RecordBatch>,
    type_ops: &dyn TypeOps,
) -> AdapterResult<Arc<Schema>> {
    let col_name = get_column_values::<StringArray>(&batch, "col_name")?;
    let data_type = get_column_values::<StringArray>(&batch, "data_type")?;
    let comments = get_column_values::<StringArray>(&batch, "comment")?;

    let mut fields: Vec<Field> = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        let name = col_name.value(i).to_string();
        let type_str = data_type.value(i).to_string();
        let comment = comments.value(i).to_string();

        let field = make_arrow_field_v2(type_ops, name, &type_str, None, Some(comment))?;
        fields.push(field);
    }

    Ok(Arc::new(Schema::new(fields)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relation::databricks::DatabricksRelation;
    use dbt_schemas::dbt_types::RelationType;
    use dbt_schemas::schemas::common::ResolvedQuoting;
    use dbt_schemas::schemas::relations::base::BaseRelation;
    use std::sync::Arc;

    // Helper function to create a test relation with specific quoting policies
    fn create_test_relation(
        database: &str,
        schema: &str,
        identifier: &str,
        quote_database: bool,
        quote_schema: bool,
        quote_identifier: bool,
    ) -> Arc<dyn BaseRelation> {
        let quote_policy = ResolvedQuoting {
            database: quote_database,
            schema: quote_schema,
            identifier: quote_identifier,
        };

        Arc::new(DatabricksRelation::new(
            Some(database.to_string()),
            Some(schema.to_string()),
            Some(identifier.to_string()),
            Some(RelationType::Table),
            None,
            quote_policy,
            None,
            false,
        ))
    }

    #[test]
    fn test_get_relation_with_quote_policy_no_quoting() {
        let relation =
            create_test_relation("test_db", "test_schema", "test_table", false, false, false);

        let (database, schema, identifier) = get_relation_with_quote_policy(&relation).unwrap();

        assert_eq!(database, "test_db");
        assert_eq!(schema, "test_schema");
        assert_eq!(identifier, "test_table");
    }

    #[test]
    fn test_get_relation_with_quote_policy_identifier_only() {
        let relation =
            create_test_relation("test_db", "test_schema", "test_table", false, false, true);

        let (database, schema, identifier) = get_relation_with_quote_policy(&relation).unwrap();

        assert_eq!(database, "test_db");
        assert_eq!(schema, "test_schema");
        assert_eq!(identifier, "`test_table`");
    }

    #[test]
    fn test_get_relation_with_quote_policy_all_parts() {
        let relation =
            create_test_relation("test_db", "test_schema", "test_table", true, true, true);

        let (database, schema, identifier) = get_relation_with_quote_policy(&relation).unwrap();

        assert_eq!(database, "`test_db`");
        assert_eq!(schema, "`test_schema`");
        assert_eq!(identifier, "`test_table`");
    }

    #[test]
    fn test_get_relation_with_quote_policy_mixed_scenario() {
        let relation =
            create_test_relation("test_db", "test_schema", "test_table", true, false, true);

        let (database, schema, identifier) = get_relation_with_quote_policy(&relation).unwrap();

        assert_eq!(database, "`test_db`");
        assert_eq!(schema, "test_schema");
        assert_eq!(identifier, "`test_table`");
    }

    #[test]
    fn test_get_relation_with_quote_policy_with_special_characters() {
        let relation =
            create_test_relation("test-db", "test-schema", "test-table", false, false, true);

        let (database, schema, identifier) = get_relation_with_quote_policy(&relation).unwrap();

        assert_eq!(database, "test-db");
        assert_eq!(schema, "test-schema");
        assert_eq!(identifier, "`test-table`");
    }

    #[test]
    fn test_get_relation_with_quote_policy_with_reserved_keywords() {
        let relation = create_test_relation("order", "select", "table", true, true, true);

        let (database, schema, identifier) = get_relation_with_quote_policy(&relation).unwrap();

        assert_eq!(database, "`order`");
        assert_eq!(schema, "`select`");
        assert_eq!(identifier, "`table`");
    }

    #[test]
    fn test_get_relation_with_quote_policy_database_schema_only() {
        let relation =
            create_test_relation("test_db", "test_schema", "test_table", true, true, false);

        let (database, schema, identifier) = get_relation_with_quote_policy(&relation).unwrap();

        assert_eq!(database, "`test_db`");
        assert_eq!(schema, "`test_schema`");
        assert_eq!(identifier, "test_table");
    }
}
