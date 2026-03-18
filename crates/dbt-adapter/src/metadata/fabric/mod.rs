use crate::metadata::{
    CatalogAndSchema, MAX_CONNECTIONS, MetadataAdapter, MetadataFreshness, RelationSchemaPair,
    RelationVec, create_schemas_if_not_exists,
};
use crate::record_batch_utils::get_column_values;
use crate::typed_adapter::*;
use crate::{AdapterEngine, AdapterTyping};
use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
use arrow_schema::Schema;
use dbt_common::Cancellable;
use dbt_common::cancellation::CancellationToken;
use dbt_common::{AdapterResult, AsyncAdapterResult, adapter::ExecutionPhase};
use dbt_schemas::schemas::legacy_catalog::{CatalogNodeStats, TableMetadata};
use dbt_schemas::schemas::{
    legacy_catalog::{CatalogTable, ColumnMetadata},
    relations::base::{BaseRelation, RelationPattern},
};
use dbt_xdbc::{Connection, MapReduce, QueryCtx};
use minijinja::State;
use std::collections::HashMap;
use std::{collections::BTreeMap, sync::Arc};

pub struct FabricMetadataAdapter {
    #[allow(dead_code, reason = "TODO implement FabricMetadataAdapter")]
    pub adapter: ConcreteAdapter,
}

impl FabricMetadataAdapter {
    pub fn new(engine: Arc<dyn AdapterEngine>) -> Self {
        let adapter = ConcreteAdapter::new(engine);
        Self { adapter }
    }
}

impl MetadataAdapter for FabricMetadataAdapter {
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
        let table_owners = get_column_values::<StringArray>(&stats_sql_result, "table_owner")?;

        let mut result = BTreeMap::<String, CatalogTable>::new();

        for i in 0..table_catalogs.len() {
            let catalog = table_catalogs.value(i);
            let schema = table_schemas.value(i);
            let table = table_names.value(i);
            let data_type = data_types.value(i);
            let owner = table_owners.value(i);

            let fully_qualified_name = format!("{catalog}.{schema}.{table}").to_lowercase();

            if !result.contains_key(&fully_qualified_name) {
                let node_metadata = TableMetadata {
                    materialization_type: data_type.to_string(),
                    schema: schema.to_string(),
                    name: table.to_string(),
                    database: Some(catalog.to_string()),
                    comment: None,
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
                    columns: Default::default(),
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
        let column_indices = get_column_values::<Int32Array>(&stats_sql_result, "column_index")?;
        let column_types = get_column_values::<StringArray>(&stats_sql_result, "column_type")?;

        let mut columns_by_relation = BTreeMap::new();

        for i in 0..table_catalogs.len() {
            let catalog = table_catalogs.value(i);
            let schema = table_schemas.value(i);
            let table = table_names.value(i);

            let fully_qualified_name = format!("{catalog}.{schema}.{table}").to_lowercase();

            let column_name_i = column_names.value(i);
            let column_index_i = column_indices.value(i);
            let column_type_i = column_types.value(i);

            let column = ColumnMetadata {
                name: column_name_i.to_string(),
                index: column_index_i.into(),
                data_type: column_type_i.to_string(),
                comment: None,
            };

            columns_by_relation
                .entry(fully_qualified_name.clone())
                .or_insert(BTreeMap::new())
                .insert(column_name_i.to_string(), column);
        }
        Ok(columns_by_relation)
    }

    fn create_schemas_if_not_exists(
        &self,
        state: &State<'_, '_>,
        catalog_schemas: Vec<(String, String, String)>,
    ) -> AdapterResult<Vec<(String, String, String, AdapterResult<()>)>> {
        create_schemas_if_not_exists(&self.adapter, self, state, catalog_schemas)
    }

    fn list_relations_schemas_inner(
        &self,
        unique_id: Option<String>,
        phase: Option<ExecutionPhase>,
        relations: &[Arc<dyn BaseRelation>],
        token: CancellationToken,
    ) -> AsyncAdapterResult<'_, HashMap<String, AdapterResult<Arc<Schema>>>> {
        let new_conn_f = Box::new({
            let adapter = self.adapter.clone();
            move || {
                adapter
                    .engine()
                    .new_connection(None, None)
                    .map_err(Cancellable::Error)
            }
        });

        let map_f = Box::new({
            let adapter = self.adapter.clone();
            let token_clone = token.clone();
            move |conn: &mut dyn Connection, relation: &Arc<dyn BaseRelation>| {
                // TODO: we need to get the actual schema
                let sql = format!(
                    "EXEC sp_columns {}, {}, {}",
                    relation.identifier_as_str()?,
                    relation.schema_as_str()?,
                    relation.database_as_str()?,
                );
                if false {
                    let ctx = QueryCtx::new_metadata().with_desc("Get table schema");

                    let ctx = unique_id
                        .iter()
                        .fold(ctx, |ctx, id| ctx.with_node_id(id.clone()));

                    let ctx = phase
                        .iter()
                        .fold(ctx, |ctx, phase| ctx.with_phase(phase.as_str()));

                    let (_, table) = adapter.query(&ctx, conn, &sql, None, token_clone.clone())?;
                    let _batch = table.original_record_batch();

                    // TODO: convert the RecordBatch into a Schema and return it
                }
                let schema = Schema::new(Vec::<arrow_schema::Field>::default());

                Ok(Arc::new(schema))
            }
        });

        let reduce_f = Box::new(
            move |acc: &mut HashMap<_, _>, relation: Arc<dyn BaseRelation>, schema| {
                acc.insert(relation.semantic_fqn(), schema);
                Ok(())
            },
        );

        MapReduce::new(new_conn_f, map_f, reduce_f, MAX_CONNECTIONS)
            .run(Arc::new(relations.to_vec()), token)
    }

    fn list_relations_schemas_by_patterns_inner(
        &self,
        patterns: &[RelationPattern],
        _token: CancellationToken,
    ) -> AsyncAdapterResult<'_, Vec<(String, AdapterResult<RelationSchemaPair>)>> {
        let _ = patterns;

        todo!()
    }

    fn freshness_inner(
        &self,
        relations: &[Arc<dyn BaseRelation>],
        _token: CancellationToken,
    ) -> AsyncAdapterResult<'_, BTreeMap<String, MetadataFreshness>> {
        let _ = relations;

        todo!()
    }

    fn list_relations_in_parallel_inner(
        &self,
        db_schemas: &[CatalogAndSchema],
        _token: CancellationToken,
    ) -> AsyncAdapterResult<'_, BTreeMap<CatalogAndSchema, AdapterResult<RelationVec>>> {
        let _ = db_schemas;

        todo!()
    }
}
