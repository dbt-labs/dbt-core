use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::OnceLock;

use arrow_array::*;
use arrow_schema::{Field, Schema};
use dbt_agate::AgateTable;
use dbt_common::adapter::AdapterType;
use dbt_common::adapter::ExecutionPhase;
use dbt_common::cancellation::Cancellable;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::legacy_catalog::{
    CatalogNodeStats, CatalogTable, ColumnMetadata, TableMetadata,
};
use dbt_schemas::schemas::relations::base::{BaseRelation, RelationPattern};
use dbt_xdbc::{Connection, MapReduce, QueryCtx};
use indexmap::IndexMap;
use minijinja::State;
use once_cell::sync::Lazy;
use regex::Regex;

use crate::errors::{AdapterError, AdapterResult, AsyncAdapterResult};
use crate::metadata::CatalogAndSchema;
use crate::metadata::databricks::describe_table::DatabricksTableMetadata;
use crate::metadata::databricks::version::DbrVersion;
use crate::query_ctx::query_ctx_from_state;
use crate::record_batch_utils::get_column_values;
use crate::relation::databricks::DatabricksRelation;
use crate::relation::databricks::config_v2::{
    DatabricksRelationMetadata, DatabricksRelationMetadataKey,
};
use crate::sql_types::{TypeOps, make_arrow_field_v2};
use crate::typed_adapter::ConcreteAdapter;
use crate::{AdapterEngine, AdapterResponse};
use crate::{AdapterTyping, TypedBaseAdapter, metadata::*};

pub mod describe_table;
pub mod schemas;
pub(crate) mod version;

// Reference: https://github.com/databricks/dbt-databricks/blob/92f1442faabe0fce6f0375b95e46ebcbfcea4c67/dbt/include/databricks/macros/adapters/metadata.sql
pub fn list_relations(
    adapter: &dyn AdapterTyping,
    ctx: &QueryCtx,
    conn: &'_ mut dyn Connection,
    db_schema: &CatalogAndSchema,
) -> AdapterResult<Vec<Arc<dyn BaseRelation>>> {
    let sql = format!("
SELECT
    table_name,
    if(table_type IN ('EXTERNAL', 'MANAGED', 'MANAGED_SHALLOW_CLONE', 'EXTERNAL_SHALLOW_CLONE'), 'table', lower(table_type)) AS table_type,
    lower(data_source_format) AS file_format,
    table_schema,
    table_owner,
    table_catalog,
    if(
    table_type IN (
        'EXTERNAL',
        'MANAGED',
        'MANAGED_SHALLOW_CLONE',
        'EXTERNAL_SHALLOW_CLONE'
    ),
    lower(table_type),
    NULL
    ) AS databricks_table_type
FROM `system`.`information_schema`.`tables`
WHERE table_catalog = '{}'
    AND table_schema = '{}'",
                            &db_schema.resolved_catalog,
                            &db_schema.resolved_schema);

    let batch = adapter.engine().execute(None, conn, ctx, &sql)?;

    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }

    let mut relations = Vec::new();

    let names = get_column_values::<StringArray>(&batch, "table_name")?;
    let schemas = get_column_values::<StringArray>(&batch, "table_schema")?;
    let catalogs = get_column_values::<StringArray>(&batch, "table_catalog")?;
    let table_types = get_column_values::<StringArray>(&batch, "table_type")?;
    let file_formats = get_column_values::<StringArray>(&batch, "file_format")?;

    for i in 0..batch.num_rows() {
        let name = names.value(i);
        let schema = schemas.value(i);
        let catalog = catalogs.value(i);
        let table_type = table_types.value(i).to_uppercase();
        let is_delta = file_formats.value(i) == "delta";

        let relation = Arc::new(DatabricksRelation::new(
            Some(catalog.to_string()),
            Some(schema.to_string()),
            Some(name.to_string()),
            Some(RelationType::from_adapter_type(
                AdapterType::Databricks,
                table_type.as_str(),
            )),
            None,
            adapter.quoting(),
            None,
            is_delta,
        )) as Arc<dyn BaseRelation>;
        relations.push(relation);
    }

    Ok(relations)
}

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

pub struct DatabricksMetadataAdapter {
    adapter: ConcreteAdapter,
}

impl DatabricksMetadataAdapter {
    pub fn new(engine: Arc<AdapterEngine>) -> Self {
        let adapter = ConcreteAdapter::new(engine);
        Self { adapter }
    }

    /// Get the Databricks Runtime version, caching the result for subsequent calls.
    ///
    /// To bypass the cache, use [`get_dbr_version()`](Self::get_dbr_version) directly.
    pub(crate) fn dbr_version(&self) -> AdapterResult<DbrVersion> {
        static CACHED_DBR_VERSION: OnceLock<AdapterResult<DbrVersion>> = OnceLock::new();

        CACHED_DBR_VERSION
            .get_or_init(|| {
                let query_ctx = QueryCtx::default().with_desc("get_dbr_version adapter call");
                let mut conn = self.adapter.new_connection(None, None)?;
                Self::get_dbr_version(&self.adapter, &query_ctx, conn.deref_mut())
            })
            .clone()
    }

    /// Get the Databricks Runtime version without caching.
    ///
    /// This follows the dbt-databricks implementation:
    /// - For clusters: queries `SET spark.databricks.clusterUsageTags.sparkVersion`
    /// - For SQL Warehouses: returns `DbrVersion::Unset` (treated as latest/max version)
    ///
    /// See: https://github.com/databricks/dbt-databricks/blob/822b105b15e644676d9e1f47cbfd765cd4c1541f/dbt/adapters/databricks/handle.py#L129
    pub fn get_dbr_version(
        adapter: &dyn TypedBaseAdapter,
        ctx: &QueryCtx,
        conn: &mut dyn Connection,
    ) -> AdapterResult<DbrVersion> {
        let is_cluster = adapter.is_cluster()?;

        if !is_cluster {
            return Ok(DbrVersion::Unset);
        }

        // For clusters, query the spark version tag
        // Returns a row like: (key, value) = ("spark.databricks.clusterUsageTags.sparkVersion", "15.4.x-scala2.12")
        let sql = "SET spark.databricks.clusterUsageTags.sparkVersion";
        let (_response, table) = adapter.execute(None, conn, ctx, sql, false, true, None, None)?;
        let batch = table.original_record_batch();

        // The result has two columns: "key" and "value"
        let values = get_column_values::<StringArray>(&batch, "value")?;
        debug_assert_eq!(values.len(), 1);

        let version_str = values.value(0);
        extract_dbr_version(version_str)
    }

    /// Given the relation, fetch its config from the remote data warehouse
    /// reference: https://github.com/databricks/dbt-databricks/blob/13686739eb59566c7a90ee3c357d12fe52ec02ea/dbt/adapters/databricks/impl.py#L871
    // TODO: use Arrow RecordBatches for this instead of a hashmap of Agate tables, like BigQuery does
    pub(crate) fn fetch_relation_config_from_remote(
        &self,
        state: &State,
        conn: &mut dyn Connection,
        base_relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<(RelationType, DatabricksRelationMetadata)> {
        let relation_type = base_relation.relation_type().ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::Configuration,
                format!("relation_type is required for the input relation of adapter.get_relation_config. Input relation: {}", base_relation.render_self_as_str()),
            )
        })?;

        let database = base_relation.database_as_str()?;
        let schema = base_relation.schema_as_str()?;
        let identifier = base_relation.identifier_as_str()?;
        let rendered_relation = base_relation.render_self_as_str();

        let mut metadata = IndexMap::new();
        metadata.insert(
            DatabricksRelationMetadataKey::DescribeExtended,
            self.describe_extended(&database, &schema, &identifier, state, &mut *conn)?,
        );
        metadata.insert(
            DatabricksRelationMetadataKey::ShowTblProperties,
            self.show_tblproperties(&rendered_relation, state, &mut *conn)?,
        );

        // Add materialization-specific metadata
        // https://github.com/databricks/dbt-databricks/blob/9e2566fdb56318cb7a59a4492f96c7aaa7af73b0/dbt/adapters/databricks/impl.py#L914-L1021
        match relation_type {
            RelationType::MaterializedView => {
                metadata.insert(
                    DatabricksRelationMetadataKey::DescribeExtended,
                    self.get_view_description(&database, &schema, &identifier, state, &mut *conn)?,
                );
            }
            RelationType::View => {
                metadata.insert(
                    DatabricksRelationMetadataKey::InfoSchemaViews,
                    self.get_view_description(&database, &schema, &identifier, state, &mut *conn)?,
                );
                metadata.insert(
                    DatabricksRelationMetadataKey::InfoSchemaRelationTags,
                    self.fetch_tags(&database, &schema, &identifier, state, &mut *conn)?,
                );
                metadata.insert(
                    DatabricksRelationMetadataKey::InfoSchemaColumnTags,
                    self.fetch_column_tags(&database, &schema, &identifier, state, &mut *conn)?,
                );
            }
            RelationType::StreamingTable => {}
            RelationType::Table => {
                let is_hive_metastore =
                    base_relation.is_hive_metastore().try_into().map_err(|_| {
                        AdapterError::new(
                            AdapterErrorKind::Configuration,
                            format!(
                                "Unable to decode is_hive_metastore config for {}",
                                base_relation.render_self_as_str()
                            ),
                        )
                    })?;
                if is_hive_metastore {
                    return Err(AdapterError::new(
                        AdapterErrorKind::NotSupported,
                        format!(
                            "Incremental application of constraints and column masks is not supported for Hive Metastore! Relation: `{database}`.`{schema}`.`{identifier}`"
                        ),
                    ));
                }

                metadata.insert(
                    DatabricksRelationMetadataKey::InfoSchemaRelationTags,
                    self.fetch_tags(&database, &schema, &identifier, state, &mut *conn)?,
                );
                metadata.insert(
                    DatabricksRelationMetadataKey::InfoSchemaColumnTags,
                    self.fetch_column_tags(&database, &schema, &identifier, state, &mut *conn)?,
                );
                metadata.insert(
                    DatabricksRelationMetadataKey::NonNullConstraints,
                    self.fetch_non_null_constraint_columns(
                        &database,
                        &schema,
                        &identifier,
                        state,
                        &mut *conn,
                    )?,
                );
                metadata.insert(
                    DatabricksRelationMetadataKey::PrimaryKeyConstraints,
                    self.fetch_primary_key_constraints(
                        &database,
                        &schema,
                        &identifier,
                        state,
                        &mut *conn,
                    )?,
                );
                metadata.insert(
                    DatabricksRelationMetadataKey::ForeignKeyConstraints,
                    self.fetch_foreign_key_constraints(
                        &database,
                        &schema,
                        &identifier,
                        state,
                        &mut *conn,
                    )?,
                );
                metadata.insert(
                    DatabricksRelationMetadataKey::ColumnMasks,
                    self.fetch_column_masks(&database, &schema, &identifier, state, &mut *conn)?,
                );
            }
            RelationType::CTE
            | RelationType::Ephemeral
            | RelationType::External
            | RelationType::PointerTable
            | RelationType::DynamicTable
            | RelationType::Function => {
                return Err(AdapterError::new(
                    AdapterErrorKind::NotSupported,
                    format!(
                        "Cannot apply incremental config on relation of type {relation_type}. Relation: `{database}`.`{schema}`.`{identifier}`"
                    ),
                ));
            }
        };

        // https://github.com/databricks/dbt-databricks/blob/13686739eb59566c7a90ee3c357d12fe52ec02ea/dbt/adapters/databricks/impl.py#L908
        // TODO: Implement polling for DLT pipeline status
        // we don't have the dbx client here
        // we might need to query internal delta system tables or expose something via ADBC

        Ok((relation_type, metadata))
    }

    // convenience for executing SQL
    fn execute_sql_with_context(
        &self,
        sql: &str,
        state: &State,
        desc: &str,
        conn: &mut dyn Connection,
    ) -> AdapterResult<(AdapterResponse, AgateTable)> {
        let ctx = query_ctx_from_state(state)?.with_desc(desc);
        self.adapter.execute(
            Some(state),
            conn,
            &ctx,
            sql,
            false, // auto_begin
            true,  // fetch
            None,  // limit
            None,  // options
        )
    }

    // https://github.com/dbt-labs/dbt-adapters/blob/6f2aae13e39c5df1c93e5d514678914142d71768/dbt-spark/src/dbt/include/spark/macros/adapters.sql#L314
    fn describe_extended(
        &self,
        database: &str,
        schema: &str,
        identifier: &str,
        state: &State,
        conn: &mut dyn Connection,
    ) -> AdapterResult<AgateTable> {
        let sql = format!("DESCRIBE EXTENDED `{database}`.`{schema}`.`{identifier}`;");
        let (_, result) =
            self.execute_sql_with_context(&sql, state, "Describe table extended", conn)?;
        Ok(result)
    }

    // https://github.com/databricks/dbt-databricks/blob/9e2566fdb56318cb7a59a4492f96c7aaa7af73b0/dbt/include/databricks/macros/adapters/metadata.sql#L78
    fn get_view_description(
        &self,
        database: &str,
        schema: &str,
        identifier: &str,
        state: &State,
        conn: &mut dyn Connection,
    ) -> AdapterResult<AgateTable> {
        let sql = format!(
            "SELECT * 
            FROM `SYSTEM`.`INFORMATION_SCHEMA`.`VIEWS`
            WHERE TABLE_CATALOG = '{}'
                AND TABLE_SCHEMA = '{}'
                AND TABLE_NAME = '{}';",
            database.to_lowercase(),
            schema.to_lowercase(),
            identifier.to_lowercase()
        );
        let (_, result) = self.execute_sql_with_context(&sql, state, "Query for view", conn)?;
        Ok(result)
    }

    // https://github.com/dbt-labs/dbt-adapters/blob/6f2aae13e39c5df1c93e5d514678914142d71768/dbt-spark/src/dbt/include/spark/macros/adapters.sql#L127
    fn show_tblproperties(
        &self,
        relation_str: &str,
        state: &State,
        conn: &mut dyn Connection,
    ) -> AdapterResult<AgateTable> {
        let sql = format!("SHOW TBLPROPERTIES {relation_str}");
        let (_, result) =
            self.execute_sql_with_context(&sql, state, "Show table properties", conn)?;
        Ok(result)
    }

    // https://github.com/databricks/dbt-databricks/blob/9e2566fdb56318cb7a59a4492f96c7aaa7af73b0/dbt/include/databricks/macros/relations/components/constraints.sql#L1
    fn fetch_non_null_constraint_columns(
        &self,
        database: &str,
        schema: &str,
        identifier: &str,
        state: &State,
        conn: &mut dyn Connection,
    ) -> AdapterResult<AgateTable> {
        let sql = format!(
            "SELECT column_name
            FROM `{database}`.`information_schema`.`columns`
            WHERE table_catalog = '{database}' 
              AND table_schema = '{schema}'
              AND table_name = '{identifier}'
              AND is_nullable = 'NO';"
        );
        let (_, result) =
            self.execute_sql_with_context(&sql, state, "Fetch non null constraint columns", conn)?;
        Ok(result)
    }

    // https://github.com/databricks/dbt-databricks/blob/9e2566fdb56318cb7a59a4492f96c7aaa7af73b0/dbt/include/databricks/macros/relations/components/constraints.sql#L20
    fn fetch_primary_key_constraints(
        &self,
        database: &str,
        schema: &str,
        identifier: &str,
        state: &State,
        conn: &mut dyn Connection,
    ) -> AdapterResult<AgateTable> {
        let sql = format!(
            "SELECT kcu.constraint_name, kcu.column_name
            FROM `{database}`.information_schema.key_column_usage kcu
            WHERE kcu.table_catalog = '{database}' 
                AND kcu.table_schema = '{schema}'
                AND kcu.table_name = '{identifier}' 
                AND kcu.constraint_name = (
                SELECT constraint_name
                FROM `{database}`.information_schema.table_constraints
                WHERE table_catalog = '{database}'
                    AND table_schema = '{schema}'
                    AND table_name = '{identifier}' 
                    AND constraint_type = 'PRIMARY KEY'
                )
            ORDER BY kcu.ordinal_position;"
        );
        let (_, result) =
            self.execute_sql_with_context(&sql, state, "Fetch PK constraints", conn)?;
        Ok(result)
    }

    // https://github.com/databricks/dbt-databricks/blob/9e2566fdb56318cb7a59a4492f96c7aaa7af73b0/dbt/include/databricks/macros/relations/components/column_mask.sql#L11
    fn fetch_column_masks(
        &self,
        database: &str,
        schema: &str,
        identifier: &str,
        state: &State,
        conn: &mut dyn Connection,
    ) -> AdapterResult<AgateTable> {
        let sql = format!(
            "SELECT 
                column_name,
                mask_name,
                using_columns
            FROM `system`.`information_schema`.`column_masks`
            WHERE table_catalog = '{database}'
                AND table_schema = '{schema}'
                AND table_name = '{identifier}';"
        );
        let (_, result) = self.execute_sql_with_context(&sql, state, "Fetch column masks", conn)?;
        Ok(result)
    }

    // https://github.com/databricks/dbt-databricks/blob/9e2566fdb56318cb7a59a4492f96c7aaa7af73b0/dbt/include/databricks/macros/relations/components/constraints.sql#L47
    fn fetch_foreign_key_constraints(
        &self,
        database: &str,
        schema: &str,
        identifier: &str,
        state: &State,
        conn: &mut dyn Connection,
    ) -> AdapterResult<AgateTable> {
        let sql = format!(
            "SELECT
                kcu.constraint_name,
                kcu.column_name AS from_column,
                ukcu.table_catalog AS to_catalog,
                ukcu.table_schema AS to_schema,
                ukcu.table_name AS to_table,
                ukcu.column_name AS to_column
            FROM `{database}`.information_schema.key_column_usage kcu
            JOIN `{database}`.information_schema.referential_constraints rc
                ON kcu.constraint_name = rc.constraint_name
            JOIN `{database}`.information_schema.key_column_usage ukcu
                ON rc.unique_constraint_name = ukcu.constraint_name
                AND kcu.ordinal_position = ukcu.ordinal_position
            WHERE kcu.table_catalog = '{database}'
                AND kcu.table_schema = '{schema}'
                AND kcu.table_name = '{identifier}'
                AND kcu.constraint_name IN (
                SELECT constraint_name
                FROM `{database}`.information_schema.table_constraints
                WHERE table_catalog = '{database}'
                    AND table_schema = '{schema}'
                    AND table_name = '{identifier}'
                    AND constraint_type = 'FOREIGN KEY'
                )
            ORDER BY kcu.ordinal_position;"
        );
        let (_, result) =
            self.execute_sql_with_context(&sql, state, "Fetch FK constraints", conn)?;
        Ok(result)
    }

    // https://github.com/databricks/dbt-databricks/blob/9e2566fdb56318cb7a59a4492f96c7aaa7af73b0/dbt/include/databricks/macros/relations/tags.sql#L11
    fn fetch_tags(
        &self,
        database: &str,
        schema: &str,
        identifier: &str,
        state: &State,
        conn: &mut dyn Connection,
    ) -> AdapterResult<AgateTable> {
        let sql = format!(
            "SELECT tag_name, tag_value
            FROM `system`.`information_schema`.`table_tags`
            WHERE catalog_name = '{database}' 
                AND schema_name = '{schema}'
                AND table_name = '{identifier}'"
        );
        let (_, result) = self.execute_sql_with_context(&sql, state, "Fetch tags", conn)?;
        Ok(result)
    }

    fn fetch_column_tags(
        &self,
        database: &str,
        schema: &str,
        identifier: &str,
        state: &State,
        conn: &mut dyn Connection,
    ) -> AdapterResult<AgateTable> {
        let sql = format!(
            "SELECT column_name, tag_name, tag_value
            FROM `system`.`information_schema`.`column_tags`
            WHERE catalog_name = '{database}' 
                AND schema_name = '{schema}'
                AND table_name = '{identifier}'"
        );
        let (_, result) = self.execute_sql_with_context(&sql, state, "Fetch column tags", conn)?;
        Ok(result)
    }
}

impl MetadataAdapter for DatabricksMetadataAdapter {
    fn adapter(&self) -> &dyn TypedBaseAdapter {
        &self.adapter
    }

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

    fn list_relations_schemas_inner(
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

        let adapter = self.adapter.clone(); // clone needed to move it into lambda
        let new_connection_f = Box::new(move || {
            adapter
                .new_connection(None, None)
                .map_err(Cancellable::Error)
        });

        let adapter = self.adapter.clone();
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
        let token = self.adapter.cancellation_token();
        map_reduce.run(Arc::new(relations.to_vec()), token)
    }

    fn list_relations_schemas_by_patterns_inner(
        &self,
        _patterns: &[RelationPattern],
    ) -> AsyncAdapterResult<'_, Vec<(String, AdapterResult<RelationSchemaPair>)>> {
        todo!("DatabricksAdapter::list_relations_schemas_by_patterns")
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

        let adapter = self.adapter.clone();
        let new_connection_f = move || {
            adapter
                .new_connection(None, None)
                .map_err(Cancellable::Error)
        };

        let adapter = self.adapter.clone();
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
        let token = self.adapter.cancellation_token();
        map_reduce.run(Arc::new(keys), token)
    }

    fn create_schemas_if_not_exists(
        &self,
        state: &State<'_, '_>,
        catalog_schemas: &BTreeMap<String, BTreeSet<String>>,
    ) -> AdapterResult<Vec<(String, String, AdapterResult<()>)>> {
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
                .new_connection(None, None)
                .map_err(Cancellable::Error)
        };

        let adapter = self.adapter.clone();
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
        let token = self.adapter.cancellation_token();
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

static DBR_VERSION_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"([1-9][0-9]*)\.(x|0|[1-9][0-9]*)").unwrap());

/// Extract DBR version from a spark version string using regex.
///
/// See: https://github.com/databricks/dbt-databricks/blob/822b105b15e644676d9e1f47cbfd765cd4c1541f/dbt/adapters/databricks/handle.py#L273
fn extract_dbr_version(version_str: &str) -> AdapterResult<DbrVersion> {
    let caps = DBR_VERSION_REGEX.captures(version_str).ok_or_else(|| {
        AdapterError::new(
            AdapterErrorKind::Internal,
            format!("Failed to detect DBR version from: {version_str}"),
        )
    })?;

    let major: i64 = caps[1].parse().map_err(|_| {
        AdapterError::new(AdapterErrorKind::Internal, "Major version is not a number")
    })?;

    let minor_str = &caps[2];
    if minor_str == "x" {
        Ok(DbrVersion::Full(major, i64::MAX))
    } else {
        let minor: i64 = minor_str.parse().map_err(|_| {
            AdapterError::new(AdapterErrorKind::Internal, "Minor version is not a number")
        })?;
        Ok(DbrVersion::Full(major, minor))
    }
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

    #[test]
    fn test_extract_dbr_version_with_scala() {
        // Format: "15.4.x-scala2.12" → regex finds "15.4" first → (15, 4)
        let result = extract_dbr_version("15.4.x-scala2.12").unwrap();
        assert_eq!(result, DbrVersion::Full(15, 4));
    }

    #[test]
    fn test_extract_dbr_version_with_gpu_ml() {
        // Format: "15.4.x-gpu-ml-scala2.12" → regex finds "15.4" first → (15, 4)
        let result = extract_dbr_version("15.4.x-gpu-ml-scala2.12").unwrap();
        assert_eq!(result, DbrVersion::Full(15, 4));
    }

    #[test]
    fn test_extract_dbr_version_full_version() {
        // Format: "16.2-scala2.12" → (16, 2)
        let result = extract_dbr_version("16.2-scala2.12").unwrap();
        assert_eq!(result, DbrVersion::Full(16, 2));
    }

    #[test]
    fn test_extract_dbr_version_simple() {
        // Simple format without suffix
        let result = extract_dbr_version("16.2").unwrap();
        assert_eq!(result, DbrVersion::Full(16, 2));
    }

    #[test]
    fn test_extract_dbr_version_with_x_minor() {
        // Format: "16.x-scala2.12" → minor is "x" → (16, i64::MAX)
        // This matches Python's (16, sys.maxsize) behavior
        let result = extract_dbr_version("16.x-scala2.12").unwrap();
        assert_eq!(result, DbrVersion::Full(16, i64::MAX));
    }
}
