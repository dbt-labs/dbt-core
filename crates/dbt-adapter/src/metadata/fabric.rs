use crate::AdapterEngine;
use crate::metadata::{
    CatalogAndSchema, MetadataAdapter, MetadataFreshness, RelationSchemaPair, RelationVec,
};
use crate::typed_adapter::*;
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use dbt_common::{AdapterResult, AsyncAdapterResult, adapter::ExecutionPhase};
use dbt_schemas::schemas::{
    legacy_catalog::{CatalogTable, ColumnMetadata},
    relations::base::{BaseRelation, RelationPattern},
};
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
        stats_sql: Arc<RecordBatch>,
    ) -> AdapterResult<BTreeMap<String, CatalogTable>> {
        let _ = stats_sql;

        todo!()
    }

    fn build_columns_from_get_columns(
        &self,
        columns: Arc<RecordBatch>,
    ) -> AdapterResult<BTreeMap<String, BTreeMap<String, ColumnMetadata>>> {
        let _ = columns;

        todo!()
    }

    fn create_schemas_if_not_exists(
        &self,
        state: &State<'_, '_>,
        catalog_schemas: Vec<(String, String, String)>,
    ) -> AdapterResult<Vec<(String, String, String, AdapterResult<()>)>> {
        let _ = (state, catalog_schemas);

        todo!()
    }

    fn list_relations_schemas_inner(
        &self,
        unique_id: Option<String>,
        phase: Option<ExecutionPhase>,
        relations: &[Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<'_, HashMap<String, AdapterResult<Arc<Schema>>>> {
        let _ = (unique_id, phase, relations);

        todo!()
    }

    fn list_relations_schemas_by_patterns_inner(
        &self,
        patterns: &[RelationPattern],
    ) -> AsyncAdapterResult<'_, Vec<(String, AdapterResult<RelationSchemaPair>)>> {
        let _ = patterns;

        todo!()
    }

    fn freshness_inner(
        &self,
        relations: &[Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<'_, BTreeMap<String, MetadataFreshness>> {
        let _ = relations;

        todo!()
    }

    fn list_relations_in_parallel_inner(
        &self,
        db_schemas: &[CatalogAndSchema],
    ) -> AsyncAdapterResult<'_, BTreeMap<CatalogAndSchema, AdapterResult<RelationVec>>> {
        let _ = db_schemas;

        todo!()
    }
}
