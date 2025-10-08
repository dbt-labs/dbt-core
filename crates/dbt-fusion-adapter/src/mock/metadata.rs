use crate::errors::{AdapterResult, AsyncAdapterResult};
use crate::metadata::*;
use crate::mock::adapter::MockAdapter;
use arrow_schema::Schema;

use dbt_schemas::schemas::relations::base::{BaseRelation, RelationPattern};
use dbt_xdbc::query_ctx::ExecutionPhase;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

impl MetadataAdapter for MockAdapter {
    fn list_relations_schemas(
        &self,
        _unique_id: Option<String>,
        _phase: Option<ExecutionPhase>,
        _relations: &[Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<'_, HashMap<String, AdapterResult<Arc<Schema>>>> {
        let schemas = HashMap::new();
        let future = async move { Ok(schemas) };
        Box::pin(future)
    }

    fn list_relations_schemas_by_patterns(
        &self,
        _patterns: &[RelationPattern],
    ) -> AsyncAdapterResult<'_, Vec<(String, AdapterResult<RelationSchemaPair>)>> {
        let future = async move { Ok(Vec::new()) };
        Box::pin(future)
    }

    fn freshness(
        &self,
        _relations: &[Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<'_, BTreeMap<String, MetadataFreshness>> {
        let future = async move { Ok(BTreeMap::new()) };
        Box::pin(future)
    }

    fn create_schemas_if_not_exists(
        &self,
        _state: &minijinja::State<'_, '_>,
        _catalog_schemas: Vec<Arc<dyn BaseRelation>>,
    ) -> AdapterResult<Vec<(String, String, AdapterResult<()>)>> {
        Ok(Vec::new())
    }

    fn list_relations_in_parallel(
        &self,
        _db_schemas: &[CatalogAndSchema],
    ) -> AsyncAdapterResult<'_, BTreeMap<CatalogAndSchema, AdapterResult<RelationVec>>> {
        let future = async move { Ok(BTreeMap::new()) };
        Box::pin(future)
    }
}
