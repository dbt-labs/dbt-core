use crate::errors::*;
use crate::metadata::CatalogAndSchema;
use crate::metadata::*;
use crate::record_batch_utils::get_column_values;
use crate::relation::bigquery::BigqueryRelation;
use crate::typed_adapter::ConcreteAdapter;
use crate::{AdapterEngine, AdapterResult, AdapterTyping, TypedBaseAdapter};

use arrow_array::*;
use arrow_schema::*;
use dbt_common::adapter::AdapterType;
use dbt_common::adapter::ExecutionPhase;
use dbt_common::cancellation::Cancellable;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::dbt_column::DbtColumn;
use dbt_schemas::schemas::legacy_catalog::*;
use dbt_schemas::schemas::relations::base::*;
use dbt_xdbc::*;
use indexmap::IndexMap;
use minijinja::State;

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

pub mod object_options;

pub fn list_relations(
    adapter: &dyn AdapterTyping,
    ctx: &QueryCtx,
    conn: &'_ mut dyn Connection,
    db_schema: &CatalogAndSchema,
) -> AdapterResult<Vec<Arc<dyn BaseRelation>>> {
    let sql = format!(
        "SELECT
    table_catalog,
    table_schema,
    table_name,
    table_type
FROM 
    {db_schema}.INFORMATION_SCHEMA.TABLES"
    );

    let batch = adapter.engine().execute(None, conn, ctx, &sql)?;
    let table_names = get_column_values::<StringArray>(&batch, "table_name")?;
    let table_schemas = get_column_values::<StringArray>(&batch, "table_schema")?;
    let table_catalogs = get_column_values::<StringArray>(&batch, "table_catalog")?;
    let table_types = get_column_values::<StringArray>(&batch, "table_type")?;

    let mut result = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        let database = table_catalogs.value(i);
        let schema = table_schemas.value(i);
        let identifier = table_names.value(i);
        let relation_type =
            RelationType::from_adapter_type(AdapterType::Bigquery, table_types.value(i));

        result.push(Arc::new(BigqueryRelation::new(
            Some(database.to_string()),
            Some(schema.to_string()),
            Some(identifier.to_string()),
            Some(relation_type),
            None,
            adapter.quoting(),
        )) as Arc<dyn BaseRelation>);
    }
    Ok(result)
}

/// Represent nested data types (struct/array) for BigQuery
/// Leaf nodes are primitive types
/// For example column names "a.b", "a.c", "a.c.d" will be
///  a (struct)
///  /\
/// b  c (struct)
///     \
///      d
#[derive(Debug, Default)]
struct NestedColumnDataTypes {
    root: TrieNode,
}

#[derive(Debug, Default)]
struct TrieNode {
    pub children: IndexMap<String, TrieNode>,
    pub data_type: Option<String>,
}

impl NestedColumnDataTypes {
    pub fn insert(&mut self, column_name: &str, column_type: Option<&String>) {
        let names = column_name.split(".");
        let mut node = &mut self.root;
        for name in names {
            node = node.children.entry(name.to_owned()).or_default();
        }
        node.data_type = column_type.map(String::from);
    }

    pub fn format_top_level_columns_data_types(&self) -> IndexMap<String, String> {
        let mut result = IndexMap::new();
        for (column_name, node) in &self.root.children {
            let data_type = match &node.data_type {
                None => {
                    let inner_data_type = node.format_data_type();
                    format!("struct<{inner_data_type}>")
                }
                Some(data_type) => match data_type.as_str() {
                    "struct" => {
                        let inner_data_type = node.format_data_type();
                        format!("struct<{inner_data_type}>")
                    }
                    "array" => {
                        let inner_data_type = node.format_data_type();
                        format!("array<struct<{inner_data_type}>>")
                    }
                    // assume any struct or array type is a primitive type
                    _ => {
                        // ensure no sub fields
                        if node.children.is_empty() {
                            data_type.to_owned()
                        }
                        // sub fields exist -> it's actually not a primitive type -> default to struct
                        // this is to be consistent with dbt compile behavior
                        else {
                            let inner_data_type = node.format_data_type();
                            format!("struct<{inner_data_type}>")
                        }
                    }
                },
            };
            result.insert(column_name.to_owned(), data_type);
        }
        result
    }
}

impl TrieNode {
    // TODO: refactor since this method is very much overlapped with `format_top_level_columns_data_types`
    fn format_data_type(&self) -> String {
        let mut result = vec![];
        for (column_name, node) in &self.children {
            let data_type = match &node.data_type {
                None => {
                    let inner_data_type = node.format_data_type();
                    if inner_data_type.is_empty() {
                        column_name.to_owned()
                    } else {
                        format!("{column_name} struct<{inner_data_type}>")
                    }
                }
                Some(data_type) => match data_type.as_str() {
                    "struct" => {
                        let inner_data_type = node.format_data_type();
                        format!("{column_name} struct<{inner_data_type}>")
                    }
                    "array" => {
                        let inner_data_type = node.format_data_type();
                        format!("{column_name} array<struct<{inner_data_type}>>")
                    }
                    _ => {
                        if node.children.is_empty() {
                            format!("{column_name} {data_type}")
                        } else {
                            let inner_data_type = node.format_data_type();
                            format!("{column_name} struct<{inner_data_type}>")
                        }
                    }
                },
            };
            result.push(data_type);
        }
        result.join(", ")
    }
}

/// Example:
///
///     columns: {
///         "a": {"name": "a", "data_type": "string", "description": ...},
///         "b.nested": {"name": "b.nested", "data_type": "string"},
///         "b.nested2": {"name": "b.nested2", "data_type": "string"}
///     }
///     returns: {
///         "a": {"name": "a", "data_type": "string"},
///         "b": {"name": "b": "data_type": "struct<nested string, nested2 string>}
///     }
///
/// arbitrarily nested struct/array types are allowed, for more details check out the
/// tests/data/nest_column_data_types example
/// reference: https://github.com/dbt-labs/dbt-core/blob/main/env/lib/python3.12/site-packages/dbt/adapters/bigquery/column.py#L131-L132
/// The implementation is purely based on the pydoc and the limited observations of how dbt
/// compile behehaves on the test example so there probably exist corner cases not handled
/// properly
/// TODO: support constraints
pub fn nest_column_data_types(
    columns: IndexMap<String, DbtColumn>,
    _constraints: Option<BTreeMap<String, String>>,
) -> AdapterResult<IndexMap<String, DbtColumn>> {
    let mut result = NestedColumnDataTypes::default();
    for (column_name, column) in &columns {
        result.insert(column_name, column.data_type.as_ref())
    }
    let column_to_data_type = result.format_top_level_columns_data_types();
    let mut result = IndexMap::new();
    for (column_name, data_type) in &column_to_data_type {
        match columns.get(column_name) {
            Some(column) => result.insert(
                column_name.clone(),
                DbtColumn {
                    name: column.name.clone(),
                    data_type: Some(data_type.clone()),
                    description: column.description.clone(),
                    constraints: column.constraints.clone(),
                    meta: column.meta.clone(),
                    tags: column.tags.clone(),
                    policy_tags: column.policy_tags.clone(),
                    databricks_tags: column.databricks_tags.clone(),
                    quote: column.quote,
                    deprecated_config: column.deprecated_config.clone(),
                },
            ),
            None => result.insert(
                column_name.clone(),
                DbtColumn {
                    name: column_name.to_owned(),
                    data_type: Some(data_type.to_owned()),
                    description: None,
                    constraints: vec![],
                    meta: IndexMap::new(),
                    tags: vec![],
                    policy_tags: None,
                    databricks_tags: None,
                    quote: None,
                    deprecated_config: Default::default(),
                },
            ),
        };
    }
    Ok(result)
}

#[derive(Debug)]
pub enum QualifierRequirement {
    DatasetOnly,
    RegionOnly,
    DatasetOrRegion,
}

// TODO: currently not using _optional_include_project and _optional_exclude_region, but could be useful so capturing that info here
#[derive(Debug)]
pub struct QualifierOptions {
    // Most views allow passing an optional project qualifier. Will use default project when not specified
    pub _optional_include_project: bool,
    // Some RegionOnly views allow excluding the region and will default to US
    pub _optional_exclude_region: bool,
    pub requirement: QualifierRequirement,
}

impl QualifierOptions {
    pub const fn new(
        optional_include_project: bool,
        optional_exclude_region: bool,
        requirement: QualifierRequirement,
    ) -> Self {
        Self {
            _optional_include_project: optional_include_project,
            _optional_exclude_region: optional_exclude_region,
            requirement,
        }
    }
}

// Shared QualifierOptions singletons for match-based lookup
static QO_REGION_TF: QualifierOptions =
    QualifierOptions::new(true, false, QualifierRequirement::RegionOnly);
static QO_REGION_FF: QualifierOptions =
    QualifierOptions::new(false, false, QualifierRequirement::RegionOnly);
static QO_REGION_TT: QualifierOptions =
    QualifierOptions::new(true, true, QualifierRequirement::RegionOnly);
static QO_DATASET_TF: QualifierOptions =
    QualifierOptions::new(true, false, QualifierRequirement::DatasetOnly);
static QO_DS_OR_REGION_TF: QualifierOptions =
    QualifierOptions::new(true, false, QualifierRequirement::DatasetOrRegion);

/// Find the qualifier options and requirements for a known view in info schema.
///
/// This should be an exhaustive list of all known views in BQ's INFO SCHEMA. They
/// are organized in the same order as the documentation to make it easier to find
/// any new, missing views that need to be accounted for.
///
/// NOTE: BY_PROJECT views have an alias stripping that suffix.
///
/// NOTE: On the necessity of the `region` qualifier, per BigQuery's docs:
/// - You MUST specify a region to query _some_ views in `INFORMATION_SCHEMA` [1]
/// - Some other views (like `TABLES`) either need region or dataset [2]
/// - Generally, if you don't specify a region, the engine defaults to
///   the US macro location (which might be routed to any region within the US) [3]
///
/// On the ability to specify a project
/// [1] https://cloud.google.com/bigquery/docs/information-schema-intro#syntax
/// [2] https://cloud.google.com/bigquery/docs/information-schema-intro#dataset_qualifier
/// [3] https://cloud.google.com/bigquery/docs/locations#specify_locations
///
/// See https://cloud.google.com/bigquery/docs/information-schema-intro
fn qualifier_options_for_info_schema_view(
    sys_identifier: &str,
) -> Option<&'static QualifierOptions> {
    match sys_identifier {
        // Access control
        "OBJECT_PRIVILEGES" => Some(&QO_REGION_TF),

        // BI Engine
        "BI_CAPACITIES" | "BI_CAPACITY_CHANGES" => Some(&QO_REGION_TF),

        // Configurations
        "EFFECTIVE_PROJECT_OPTIONS"
        | "ORGANIZATION_OPTIONS"
        | "ORGANIZATION_OPTIONS_CHANGES"
        | "PROJECT_OPTIONS"
        | "PROJECT_OPTIONS_CHANGES" => Some(&QO_REGION_FF),

        // Datasets
        "SCHEMATA" | "SCHEMATA_LINKS" | "SCHEMATA_OPTIONS" | "SHARED_DATASET_USAGE" => {
            Some(&QO_REGION_TT)
        }
        "SCHEMATA_REPLICAS" | "SCHEMATA_REPLICAS_BY_FAILOVER_RESERVATION" => Some(&QO_REGION_TF),

        // Jobs
        "JOBS" | "JOBS_BY_PROJECT" | "JOBS_BY_USER" | "JOBS_BY_FOLDER" | "JOBS_BY_ORGANIZATION" => {
            Some(&QO_REGION_TF)
        }

        // Jobs by timeslice
        "JOBS_TIMELINE"
        | "JOBS_TIMELINE_BY_PROJECT"
        | "JOBS_TIMELINE_BY_USER"
        | "JOBS_TIMELINE_BY_FOLDER"
        | "JOBS_TIMELINE_BY_ORGANIZATION" => Some(&QO_REGION_TF),

        // Recommendations and insights
        "INSIGHTS"
        | "INSIGHTS_BY_PROJECT"
        | "RECOMMENDATIONS"
        | "RECOMMENDATIONS_BY_PROJECT"
        | "RECOMMENDATIONS_BY_ORGANIZATION" => Some(&QO_REGION_TF),

        // Reservations
        "ASSIGNMENTS"
        | "ASSIGNMENTS_BY_PROJECT"
        | "ASSIGNMENT_CHANGES"
        | "ASSIGNMENT_CHANGES_BY_PROJECT"
        | "CAPACITY_COMMITMENTS"
        | "CAPACITY_COMMITMENTS_BY_PROJECT"
        | "CAPACITY_COMMITMENT_CHANGES"
        | "CAPACITY_COMMITMENT_CHANGES_BY_PROJECT"
        | "RESERVATIONS"
        | "RESERVATIONS_BY_PROJECT"
        | "RESERVATION_CHANGES"
        | "RESERVATION_CHANGES_BY_PROJECT"
        | "RESERVATIONS_TIMELINE"
        | "RESERVATIONS_TIMELINE_BY_PROJECT" => Some(&QO_REGION_TF),

        // Routines
        "PARAMETERS" | "ROUTINES" | "ROUTINE_OPTIONS" => Some(&QO_DS_OR_REGION_TF),

        // Search indexes
        "SEARCH_INDEXES"
        | "SEARCH_INDEX_COLUMNS"
        | "SEARCH_INDEX_COLUMN_OPTIONS"
        | "SEARCH_INDEX_OPTIONS" => Some(&QO_DATASET_TF),
        "SEARCH_INDEXES_BY_ORGANIZATION" => Some(&QO_REGION_TF),

        // Sessions
        "SESSIONS" | "SESSIONS_BY_PROJECT" | "SESSIONS_BY_USER" => Some(&QO_REGION_TF),

        // Streaming
        "STREAMING_TIMELINE"
        | "STREAMING_TIMELINE_BY_PROJECT"
        | "STREAMING_TIMELINE_BY_FOLDER"
        | "STREAMING_TIMELINE_BY_ORGANIZATION" => Some(&QO_REGION_TF),

        // Tables
        "COLUMNS" | "COLUMN_FIELD_PATHS" | "TABLES" | "TABLE_OPTIONS" => Some(&QO_DS_OR_REGION_TF),
        "CONSTRAINT_COLUMN_USAGE"
        | "KEY_COLUMN_USAGE"
        | "PARTITIONS"
        | "TABLE_CONSTRAINTS"
        | "TABLE_SNAPSHOTS" => Some(&QO_DATASET_TF),
        "TABLE_STORAGE"
        | "TABLE_STORAGE_BY_PROJECT"
        | "TABLE_STORAGE_BY_FOLDER"
        | "TABLE_STORAGE_BY_ORGANIZATION"
        | "TABLE_STORAGE_USAGE_TIMELINE"
        | "TABLE_STORAGE_USAGE_TIMELINE_BY_FOLDER"
        | "TABLE_STORAGE_USAGE_TIMELINE_BY_ORGANIZATION" => Some(&QO_REGION_TF),

        // Vector indexes
        "VECTOR_INDEXES" | "VECTOR_INDEX_COLUMNS" | "VECTOR_INDEX_OPTIONS" => Some(&QO_DATASET_TF),

        // Views
        "VIEWS" | "MATERIALIZED_VIEWS" => Some(&QO_DS_OR_REGION_TF),

        // Write API
        "WRITE_API_TIMELINE"
        | "WRITE_API_TIMELINE_BY_PROJECT"
        | "WRITE_API_TIMELINE_BY_FOLDER"
        | "WRITE_API_TIMELINE_BY_ORGANIZATION" => Some(&QO_REGION_TF),

        _ => None,
    }
}

// Generate the fully qualified name of a BigQuery INFORMATION_SCHEMA table.
//
// BQ's info schema tables have unique way of handling qualifiers. Instead of a
// "database", the qualifier is something like [<project_id>.]<region_or_dataset_id>.
// but in some cases the region is also optional.
//
// See `qualifier_options_for_info_schema_view` for specific view requirements..
//
// TODO: We're currently ignoring any differences between the provided qualifier and
//       the spericific requirements for the given view name since this is only used to
//       fetch the view schema, so the specific location doesn't really matter.
fn generate_system_table_fqn(
    qualifier: &str,
    table: &str,
    user_preferred_region: Option<&str>,
) -> String {
    let sys_identifier = table.to_uppercase();

    match qualifier_options_for_info_schema_view(&sys_identifier) {
        Some(qualifier_option) => match qualifier_option.requirement {
            QualifierRequirement::RegionOnly => {
                let region = user_preferred_region.unwrap_or("us");
                format!("`region-{region}`.INFORMATION_SCHEMA.{sys_identifier}")
            }
            QualifierRequirement::DatasetOnly => {
                format!("{qualifier}.INFORMATION_SCHEMA.{sys_identifier}")
            }
            QualifierRequirement::DatasetOrRegion => {
                // respect user's location preferences by querying the region directly if
                // possible
                match user_preferred_region {
                    None => format!("{qualifier}.INFORMATION_SCHEMA.{sys_identifier}"),
                    Some(region) => {
                        format!("`region-{region}`.INFORMATION_SCHEMA.{sys_identifier}")
                    }
                }
            }
        },
        // This is technically an error, but we'll just let it fail when querying BQ
        None => format!("INFORMATION_SCHEMA.{sys_identifier}"),
    }
}

pub fn build_relation_clauses_bigquery(
    relations: &[Arc<dyn BaseRelation>],
) -> AdapterResult<(WhereClausesByDb, RelationsByDb)> {
    let mut where_by_db = BTreeMap::<String, Vec<String>>::new();
    let mut rels_by_db = BTreeMap::<String, Vec<Arc<dyn BaseRelation>>>::new();

    for rel in relations {
        // Semantic FQN: <project>.<dataset>.<table>
        let fqn = rel.semantic_fqn();
        let parts: Vec<&str> = fqn.split('.').collect();
        if parts.len() != 3 {
            return Err(AdapterError::new(
                AdapterErrorKind::UnexpectedResult,
                format!("Invalid BigQuery FQN: {}", rel.semantic_fqn()),
            ));
        }
        let (project, dataset_raw, table_raw) = (parts[0], parts[1], parts[2]);

        let dataset = dataset_raw.trim_matches('`');
        let table = table_raw.trim_matches('`');
        let db_key = format!("{project}.{dataset}");

        where_by_db
            .entry(db_key.clone())
            .or_default()
            .push(format!("table_id = '{table}'"));

        rels_by_db.entry(db_key).or_default().push(rel.clone());
    }

    Ok((where_by_db, rels_by_db))
}

fn make_map_f(
    relations: Vec<Arc<dyn BaseRelation>>,
    adapter: ConcreteAdapter,
) -> impl Fn(&mut dyn Connection, &(String, Vec<String>)) -> AdapterResult<Arc<RecordBatch>>
+ Send
+ Sync
+ 'static {
    move |conn: &mut dyn Connection, database_and_where_clauses: &(String, Vec<String>)| {
        let (database, where_clauses) = &database_and_where_clauses;
        // Query to get last modified times from BigQuery's __TABLES__ metadata table
        let table_list = relations
            .iter()
            .map(|relation| format!("'{}'", relation.identifier()))
            .collect::<Vec<_>>()
            .join(", ");

        let or_block = where_clauses.join(" OR ");

        let table_filter = format!("table_id IN ({})", table_list);

        let joined_where_clauses = if or_block.is_empty() {
            table_filter
        } else {
            format!("({}) AND {}", or_block, table_filter)
        };

        // __TABLES__ is officially deprecated in favor of TABLES and
        // PARTITIONS, but neither has last_modified_time. Bigquery's API
        // has get_table. But for customers with larger source freshness
        // workloads fanning out over all individual relations can trigger
        // API limiting errors or run up larger bills.
        //
        // reference: https://discuss.google.dev/t/information-schema-tables-monitoring-last-modified-time/125698
        let sql = format!(
            "SELECT
                 dataset_id AS table_schema,
                 table_id AS table_name,
                 TIMESTAMP_MILLIS(last_modified_time) AS last_altered,
                 (type = 2) AS is_view
             FROM {db}.__TABLES__
             WHERE {joined_where_clauses}",
            db = database,
            joined_where_clauses = joined_where_clauses,
        );

        let ctx = QueryCtx::default().with_desc("Extracting freshness from information schema");
        let (_, agate_table) = adapter.query(&ctx, &mut *conn, &sql, None)?;
        let batch = agate_table.original_record_batch();
        Ok(batch)
    }
}

pub struct BigqueryMetadataAdapter {
    adapter: ConcreteAdapter,
}

impl BigqueryMetadataAdapter {
    pub fn new(engine: Arc<dyn AdapterEngine>) -> Self {
        let adapter = ConcreteAdapter::new(engine);
        Self { adapter }
    }
}

impl MetadataAdapter for BigqueryMetadataAdapter {
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
                            value: serde_json::Value::Number(num_rows_value_i.into()),
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
                            value: serde_json::Value::Number(bytes_value_i.into()),
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

                stats.insert(
                    "has_stats".to_string(),
                    CatalogNodeStats {
                        id: "has_stats".to_string(),
                        label: "Has Stats?".to_string(),
                        value: serde_json::Value::Bool(stats.is_empty()),
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
        _phase: Option<ExecutionPhase>,
        relations: &[Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<'_, HashMap<String, AdapterResult<Arc<Schema>>>> {
        // All results are accumulated in an unordered map
        type Acc = HashMap<String, AdapterResult<Arc<Schema>>>;

        let adapter: ConcreteAdapter = self.adapter.clone(); // clone needed to move it into lambda
        let new_connection_f = Box::new(move || {
            // FIXME(harry): this is not taking into account that we will have multiple connections open
            // when relations are more than one, do we want to enforce single connection in record and replay tests?
            let node_id = unique_id.clone().unwrap_or_else(|| "sources".to_string());
            adapter
                .new_connection(None, Some(node_id))
                .map_err(Cancellable::Error)
        });

        let adapter = self.adapter.clone();
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
                let qualifier = relation.database_as_quoted_str()?;

                let user_preferred_region = adapter
                    .engine()
                    .config("location")
                    .map(|cfg| cfg.to_lowercase());

                let table_fqn =
                    generate_system_table_fqn(&qualifier, &table, user_preferred_region.as_deref());
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

                if let Some(schema_type) = schema.metadata().get("Type") {
                    if schema_type == "EXTERNAL" {
                        schema_builder.push(Field::new("_FILE_NAME", DataType::Utf8, true));
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
        let token = self.adapter.cancellation_token();
        map_reduce.run(Arc::new(relations.to_vec()), token)
    }

    fn list_relations_schemas_by_patterns_inner(
        &self,
        _patterns: &[RelationPattern],
    ) -> AsyncAdapterResult<'_, Vec<(String, AdapterResult<RelationSchemaPair>)>> {
        todo!("list_relations_schemas_by_patterns for BigQuery")
    }

    fn freshness_inner(
        &self,
        relations: &[Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<'_, BTreeMap<String, MetadataFreshness>> {
        // Build the where clause for all relations grouped by databases
        let (where_clauses_by_database, relations_by_database) =
            match build_relation_clauses_bigquery(relations) {
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
        let map_f = make_map_f(relations.to_vec(), adapter);

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
                let is_view = is_views.value(i);
                let relations = &relations_by_database[database];

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
                .new_connection(None, None)
                .map_err(Cancellable::Error)
        };

        let adapter = self.adapter.clone();

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
        let token = self.adapter.cancellation_token();
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

    #[test]
    fn test_format_top_level_columns_data_types() {
        // Test case 1: Simple primitive types
        {
            let mut nested = NestedColumnDataTypes::default();
            nested.insert("id", Some(&"integer".to_string()));
            nested.insert("name", Some(&"string".to_string()));

            let result = nested.format_top_level_columns_data_types();
            assert_eq!(result.get("id").unwrap(), "integer");
            assert_eq!(result.get("name").unwrap(), "string");
        }

        // Test case 2: Nested struct
        {
            let mut nested = NestedColumnDataTypes::default();
            nested.insert("user.id", Some(&"integer".to_string()));
            nested.insert("user.name", Some(&"string".to_string()));

            let result = nested.format_top_level_columns_data_types();
            assert_eq!(
                result.get("user").unwrap(),
                "struct<id integer, name string>"
            );
        }

        // Test case 3: Array of structs
        {
            let mut nested = NestedColumnDataTypes::default();
            nested.insert("addresses", Some(&"array".to_string()));
            nested.insert("addresses.street", Some(&"string".to_string()));
            nested.insert("addresses.city", Some(&"string".to_string()));

            let result = nested.format_top_level_columns_data_types();
            assert_eq!(
                result.get("addresses").unwrap(),
                "array<struct<street string, city string>>"
            );
        }

        // Test case 4: Mixed types with deep nesting
        {
            let mut nested = NestedColumnDataTypes::default();
            nested.insert("id", Some(&"integer".to_string()));
            nested.insert("user.name", Some(&"string".to_string()));
            nested.insert("user.contact.email", Some(&"string".to_string()));
            nested.insert("user.contact.phone", Some(&"string".to_string()));

            let result = nested.format_top_level_columns_data_types();
            assert_eq!(result.get("id").unwrap(), "integer");
            assert_eq!(
                result.get("user").unwrap(),
                "struct<name string, contact struct<email string, phone string>>"
            );
        }

        // Test case 5: Empty struct (no data type)
        {
            let mut nested = NestedColumnDataTypes::default();
            nested.insert("empty_struct", None);
            nested.insert("empty_struct.field1", Some(&"string".to_string()));

            let result = nested.format_top_level_columns_data_types();
            assert_eq!(result.get("empty_struct").unwrap(), "struct<field1 string>");
        }

        // Test case 6: Struct marked as primitive but has children
        {
            let mut nested = NestedColumnDataTypes::default();
            nested.insert("metadata", Some(&"json".to_string()));
            nested.insert("metadata.key1", Some(&"string".to_string()));
            nested.insert("metadata.key2", Some(&"integer".to_string()));

            let result = nested.format_top_level_columns_data_types();
            assert_eq!(
                result.get("metadata").unwrap(),
                "struct<key1 string, key2 integer>"
            );
        }
    }
}
