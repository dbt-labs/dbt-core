use crate::AdapterEngine;
use crate::adapter::adapter_impl::AdapterImpl;
use crate::connection::AdapterConnectionFactory;
use crate::load_catalogs;
use crate::relation::do_create_relation;
use crate::sql_types::{TypeOps, make_arrow_field_v2};
use crate::{AdapterResult, errors::AsyncAdapterResult, metadata::*, record_batch::RecordBatchExt};
use arrow_schema::Schema;

use arrow_array::{Array, Decimal128Array, RecordBatch, StringArray};

use dbt_adapter_core::ExecutionPhase;
use dbt_common::cancellation::Cancellable;
use dbt_common::cancellation::CancellationToken;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::dbt_catalogs_v2::{
    CatalogSpecV2View, DbtCatalogsV2View, V2CatalogType, V2TableFormat,
};
use dbt_schemas::schemas::{
    common::ResolvedQuoting,
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
    adapter: AdapterImpl,
}

impl DuckDBMetadataAdapter {
    pub fn new(engine: Arc<dyn AdapterEngine>) -> Self {
        let adapter = AdapterImpl::new(engine, None);
        Self { adapter }
    }
}

impl MetadataAdapter for DuckDBMetadataAdapter {
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

        let table_names = relations
            .iter()
            .map(|relation| relation.semantic_fqn())
            .collect::<Vec<_>>();

        let factory = Box::new(AdapterConnectionFactory::new(
            self.adapter.engine().clone(),
            Some(MAX_CONNECTIONS),
        ));

        let adapter = self.adapter.clone();
        let token_clone = token.clone();
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
            let (_, table) = adapter.query(&ctx, conn, &sql, None, token_clone.clone())?;
            let batch = table.original_record_batch();
            let schema =
                build_schema_from_duckdb_describe(batch, adapter.engine().type_ops().as_ref())?;
            Ok(schema)
        };

        let reduce_f = |acc: &mut Acc,
                        table_name: String,
                        schema: AdapterResult<Arc<Schema>>|
         -> Result<(), Cancellable<AdapterError>> {
            acc.insert(table_name, schema);
            Ok(())
        };

        let map_reduce = MapReduce::new(factory, Box::new(map_f), Box::new(reduce_f), None);
        map_reduce.run(Arc::new(table_names), token)
    }

    fn list_relations_schemas_by_patterns_inner(
        &self,
        _patterns: &[RelationPattern],
        _token: CancellationToken,
    ) -> AsyncAdapterResult<'_, Vec<(String, AdapterResult<RelationSchemaPair>)>> {
        todo!("DuckDBAdapter::list_relations_schemas_by_patterns")
    }

    fn freshness_inner(
        &self,
        _relations: &[Arc<dyn BaseRelation>],
        _token: CancellationToken,
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

        let map_reduce = MapReduce::new(factory, Box::new(map_f), Box::new(reduce_f), None);
        map_reduce.run(Arc::new(db_schemas.to_vec()), token)
    }
}

/// Build the `information_schema.tables` query used to list relations in a
/// schema.
///
/// `information_schema.tables` unions every attached database, so the query is
/// constrained to the target catalog. This keeps the result correct (no rows
/// leak in from other attached catalogs, including misreported Iceberg REST
/// ones) and matches the per-catalog scope dbt expects when warming the relation
/// cache. When the catalog is unknown (empty), the catalog predicate is omitted.
fn list_relations_sql(quoting: ResolvedQuoting, db_schema: &CatalogAndSchema) -> String {
    let query_schema = if quoting.schema {
        db_schema.resolved_schema.clone()
    } else {
        db_schema.resolved_schema.to_lowercase()
    };

    let catalog_predicate = if db_schema.resolved_catalog.is_empty() {
        String::new()
    } else {
        let query_catalog = if quoting.database {
            db_schema.resolved_catalog.clone()
        } else {
            db_schema.resolved_catalog.to_lowercase()
        };
        format!(
            " AND lower(table_catalog) = lower('{}')",
            query_catalog.replace('\'', "''"),
        )
    };

    format!(
        "SELECT table_catalog, table_schema, table_name, table_type \
         FROM information_schema.tables \
         WHERE table_schema = '{}'{}",
        query_schema.replace('\'', "''"),
        catalog_predicate,
    )
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
    token: CancellationToken,
) -> AdapterResult<Vec<Arc<dyn BaseRelation>>> {
    // Schema-wide listing is only unreliable for external Iceberg REST catalogs
    // (their information_schema coverage is incomplete), so bail to the targeted
    // DESCRIBE-based get_relation path only when the *target* catalog is one of
    // them. A regular DuckDB catalog is still listed even while Iceberg catalogs
    // are attached — see the catalog scoping below.
    if is_duckdb_v2_external_iceberg_catalog_database(&db_schema.resolved_catalog) {
        return Err(AdapterError::new(
            AdapterErrorKind::NotSupported,
            format!(
                "DuckDB schema-wide relation listing is disabled while v2 Iceberg REST catalogs are attached; use targeted get_relation introspection instead for '{}'",
                db_schema.resolved_catalog
            ),
        ));
    }

    let sql = list_relations_sql(engine.quoting(), db_schema);

    let batch = engine.execute(None, conn, ctx, &sql, token)?;

    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }

    let table_catalogs = batch.column_values::<StringArray>("table_catalog")?;
    let table_schemas = batch.column_values::<StringArray>("table_schema")?;
    let table_names = batch.column_values::<StringArray>("table_name")?;
    let table_types = batch.column_values::<StringArray>("table_type")?;

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

pub(crate) fn is_duckdb_v2_external_iceberg_catalog_database(database: &str) -> bool {
    if database.is_empty() {
        return false;
    }

    with_duckdb_v2_external_iceberg_catalogs(|view| {
        is_duckdb_v2_external_iceberg_catalog_database_in_view(database, view)
    })
}

fn with_duckdb_v2_external_iceberg_catalogs(
    f: impl FnOnce(&DbtCatalogsV2View<'_>) -> bool,
) -> bool {
    if !load_catalogs::fetch_use_catalogs_v2() {
        return false;
    }

    let Some(catalogs) = load_catalogs::fetch_catalogs() else {
        return false;
    };
    let Ok(view) = catalogs.view_v2() else {
        return false;
    };

    f(&view)
}

#[cfg(test)]
fn has_duckdb_v2_external_iceberg_catalogs_in_view(view: &DbtCatalogsV2View<'_>) -> bool {
    view.catalogs
        .iter()
        .any(is_duckdb_v2_external_iceberg_catalog)
}

fn is_duckdb_v2_external_iceberg_catalog_database_in_view(
    database: &str,
    view: &DbtCatalogsV2View<'_>,
) -> bool {
    if database.is_empty() {
        return false;
    }

    view.catalogs.iter().any(|catalog| {
        if !is_duckdb_v2_external_iceberg_catalog(catalog) {
            return false;
        }
        let Some(duckdb_block) = catalog.config_block("duckdb") else {
            return false;
        };

        let alias = duckdb_block
            .get(dbt_yaml::Value::from("attach_as"))
            .and_then(|value| value.as_str())
            .unwrap_or(catalog.name);
        let alias = crate::catalog_relation::sanitize_duckdb_identifier(alias);
        alias.eq_ignore_ascii_case(database)
    })
}

fn is_duckdb_v2_external_iceberg_catalog(catalog: &CatalogSpecV2View<'_>) -> bool {
    // DuckDB exposes these catalogs through Iceberg REST-style attachments.
    // Their information_schema coverage is incomplete, so schema-wide listing
    // is avoided while targeted DESCRIBE-based get_relation remains available.
    matches!(
        catalog.catalog_type,
        V2CatalogType::Horizon | V2CatalogType::Glue | V2CatalogType::IcebergRest
    ) && matches!(catalog.table_format, V2TableFormat::Iceberg)
        && catalog.config_block("duckdb").is_some()
}

/// Build an Arrow Schema from DuckDB's DESCRIBE output.
///
/// DuckDB's DESCRIBE returns columns: column_name, column_type, null, key, default, extra
fn build_schema_from_duckdb_describe(
    describe_result: Arc<RecordBatch>,
    type_ops: &dyn TypeOps,
) -> AdapterResult<Arc<Schema>> {
    let column_names = describe_result.column_values::<StringArray>("column_name")?;
    let data_types = describe_result.column_values::<StringArray>("column_type")?;
    let nullability = describe_result.column_values::<StringArray>("null")?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_schemas::schemas::dbt_catalogs::DbtCatalogs;

    fn catalog_and_schema(catalog: &str, schema: &str) -> CatalogAndSchema {
        CatalogAndSchema {
            rendered_catalog: catalog.to_string(),
            rendered_schema: schema.to_string(),
            resolved_catalog: catalog.to_string(),
            resolved_schema: schema.to_string(),
        }
    }

    #[test]
    fn list_relations_sql_scopes_to_target_catalog() {
        // With a catalog present the listing must be constrained to it, so rows
        // from other attached databases (e.g. remote Iceberg REST catalogs that
        // `information_schema.tables` would otherwise union in) cannot leak.
        let sql = list_relations_sql(
            ResolvedQuoting::trues(),
            &catalog_and_schema("aws_cloud_cost_demo", "aws_cloud_cost"),
        );
        assert_eq!(
            sql,
            "SELECT table_catalog, table_schema, table_name, table_type \
             FROM information_schema.tables \
             WHERE table_schema = 'aws_cloud_cost' \
             AND lower(table_catalog) = lower('aws_cloud_cost_demo')",
        );
    }

    #[test]
    fn list_relations_sql_lowercases_unquoted_identifiers() {
        // Unquoted identifiers fold to lowercase in DuckDB, so the predicates
        // must be lowercased to match.
        let sql = list_relations_sql(
            ResolvedQuoting::falses(),
            &catalog_and_schema("My_Catalog", "My_Schema"),
        );
        assert!(sql.contains("table_schema = 'my_schema'"));
        assert!(sql.contains("lower(table_catalog) = lower('my_catalog')"));
    }

    #[test]
    fn list_relations_sql_omits_catalog_predicate_when_unknown() {
        // No catalog means we cannot scope; fall back to the schema-only filter
        // rather than emitting an empty/incorrect catalog predicate.
        let sql = list_relations_sql(ResolvedQuoting::trues(), &catalog_and_schema("", "main"));
        assert!(sql.ends_with("WHERE table_schema = 'main'"));
        assert!(!sql.contains("lower(table_catalog)"));
        assert!(!sql.contains(" AND "));
    }

    fn with_v2_view(yaml: &str, test: impl FnOnce(&DbtCatalogsV2View<'_>)) {
        let parsed: dbt_yaml::Value = dbt_yaml::from_str(yaml).expect("valid YAML");
        let dbt_yaml::Value::Mapping(repr, span) = parsed else {
            panic!("expected YAML mapping");
        };
        let catalogs = DbtCatalogs::new(repr, span);
        let view = catalogs.view_v2().expect("valid catalogs v2 view");
        test(&view);
    }

    #[test]
    fn duckdb_v2_external_iceberg_catalogs_disable_schema_listing() {
        with_v2_view(
            r#"
catalogs:
  - name: lakekeeper
    type: iceberg_rest
    table_format: iceberg
    config:
      duckdb:
        endpoint: http://localhost:8181/catalog
        warehouse: demo
        attach_as: iceberg_demo
  - name: horizon
    type: horizon
    table_format: iceberg
    config:
      duckdb:
        endpoint: https://snowflake.example.com/polaris/api/catalog
        warehouse: HORIZON_CATALOG
  - name: files
    type: local_filesystem
    table_format: default
    config:
      duckdb:
        root_path: local_files
        file_format: csv
"#,
            |view| {
                assert!(has_duckdb_v2_external_iceberg_catalogs_in_view(view));
                assert!(is_duckdb_v2_external_iceberg_catalog_database_in_view(
                    "iceberg_demo",
                    view
                ));
                assert!(is_duckdb_v2_external_iceberg_catalog_database_in_view(
                    "horizon", view
                ));
                assert!(!is_duckdb_v2_external_iceberg_catalog_database_in_view(
                    "local_files",
                    view
                ));
                assert!(!is_duckdb_v2_external_iceberg_catalog_database_in_view(
                    "", view
                ));
            },
        );
    }
}
