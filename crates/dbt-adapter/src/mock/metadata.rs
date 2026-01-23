use crate::errors::{AdapterResult, AsyncAdapterResult};
use crate::mock::adapter::MockAdapter;
use crate::{TypedBaseAdapter, metadata::*};
use arrow_array::RecordBatch;
use arrow_schema::Schema;

use dbt_common::adapter::ExecutionPhase;
use dbt_schemas::schemas::legacy_catalog::{CatalogTable, ColumnMetadata};
use dbt_schemas::schemas::relations::base::{BaseRelation, RelationPattern};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

impl MetadataAdapter for MockAdapter {
    fn adapter(&self) -> &dyn TypedBaseAdapter {
        self
    }

    fn build_schemas_from_stats_sql(
        &self,
        _: Arc<RecordBatch>,
    ) -> AdapterResult<BTreeMap<String, CatalogTable>> {
        unimplemented!("build_schemas_from_stats_sql in MockAdapter")
    }

    fn build_columns_from_get_columns(
        &self,
        _: Arc<RecordBatch>,
    ) -> AdapterResult<BTreeMap<String, BTreeMap<String, ColumnMetadata>>> {
        unimplemented!("build_columns_from_get_columns in MockAdapter")
    }

    fn list_relations_schemas_inner(
        &self,
        _unique_id: Option<String>,
        _phase: Option<ExecutionPhase>,
        _relations: &[Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<'_, HashMap<String, AdapterResult<Arc<Schema>>>> {
        let schemas = HashMap::new();
        let future = async move { Ok(schemas) };
        Box::pin(future)
    }

    fn list_relations_schemas_by_patterns_inner(
        &self,
        _patterns: &[RelationPattern],
    ) -> AsyncAdapterResult<'_, Vec<(String, AdapterResult<RelationSchemaPair>)>> {
        let future = async move { Ok(Vec::new()) };
        Box::pin(future)
    }

    fn freshness_inner(
        &self,
        _relations: &[Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<'_, BTreeMap<String, MetadataFreshness>> {
        let future = async move { Ok(BTreeMap::new()) };
        Box::pin(future)
    }

    fn create_schemas_if_not_exists(
        &self,
        _state: &minijinja::State<'_, '_>,
        _catalog_schemas: Vec<(String, String, String)>,
    ) -> AdapterResult<Vec<(String, String, String, AdapterResult<()>)>> {
        Ok(Vec::new())
    }

    fn list_relations_in_parallel_inner(
        &self,
        _db_schemas: &[CatalogAndSchema],
    ) -> AsyncAdapterResult<'_, BTreeMap<CatalogAndSchema, AdapterResult<RelationVec>>> {
        let future = async move { Ok(BTreeMap::new()) };
        Box::pin(future)
    }
}
