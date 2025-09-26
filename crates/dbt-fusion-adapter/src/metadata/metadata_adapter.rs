use crate::errors::{AdapterError, AdapterResult, AsyncAdapterResult};
use crate::funcs::execute_macro;
use crate::metadata::*;
use crate::relation_object::{create_relation, create_relation_internal};
use crate::sql_types::SdfSchema;
use crate::typed_adapter::TypedBaseAdapter;

use arrow::array::RecordBatch;
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use dbt_schemas::schemas::{
    legacy_catalog::{CatalogTable, ColumnMetadata},
    relations::base::{BaseRelation, RelationPattern},
};
use dbt_schemas::state::ResolverState;
use dbt_schemas::stats::Stats;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

/// Maximum number of connections
pub const MAX_CONNECTIONS: usize = 128;

// XXX: we should unify relation representation as Arrow schemas across the codebase

/// Adapter that supports metadata query
pub trait MetadataAdapter: TypedBaseAdapter + Send + Sync {
    fn build_schemas_from_stats_sql(
        &self,
        _: Arc<RecordBatch>,
    ) -> AdapterResult<BTreeMap<String, CatalogTable>> {
        unimplemented!()
    }

    fn build_columns_from_get_columns(
        &self,
        _: Arc<RecordBatch>,
    ) -> AdapterResult<BTreeMap<String, BTreeMap<String, ColumnMetadata>>> {
        unimplemented!()
    }

    /// List UDFs under a given set of catalog and schemas
    fn list_user_defined_functions(
        &self,
        _catalog_schemas: &BTreeMap<String, BTreeSet<String>>,
    ) -> AsyncAdapterResult<'_, Vec<UDF>> {
        let future = async move { Ok(vec![]) };
        Box::pin(future)
    }

    /// List relations and their schemas
    fn list_relations_schemas(
        &self,
        unique_id: Option<String>,
        relations: &[Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<'_, HashMap<String, AdapterResult<Arc<Schema>>>>;

    /// Convert Arrow schema to SDF schema. See [dbt_adapter::arrow_schema_to_sdf_schema].
    fn arrow_schema_to_sdf_schema(&self, schema: Arc<Schema>) -> AdapterResult<SdfSchema>;

    fn list_relations_sdf_schemas<'a>(
        &'a self,
        unique_id: Option<String>,
        relations: &'a [Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<'a, HashMap<String, AdapterResult<SdfSchema>>> {
        let future = async move {
            self.list_relations_schemas(unique_id, relations)
                .await
                .map(|map| {
                    map.into_iter()
                        .map(|(k, v)| {
                            // TODO: make this a direct call when we move `arrow_schema_to_sdf_schema` to `fs/sa`
                            let v = v.and_then(|schema| self.arrow_schema_to_sdf_schema(schema));
                            (k, v)
                        })
                        .collect()
                })
        };
        Box::pin(future)
    }

    /// List relations and their schemas by patterns
    #[allow(clippy::type_complexity)]
    fn list_relations_schemas_by_patterns(
        &self,
        patterns: &[RelationPattern],
    ) -> AsyncAdapterResult<'_, Vec<(String, AdapterResult<RelationSchemaPair>)>>;

    /// Create schemas if they don't exist
    #[allow(clippy::type_complexity)]
    fn create_schemas_if_not_exists(
        &self,
        state: &State<'_, '_>,
        catalog_schemas: &BTreeMap<String, BTreeSet<String>>,
    ) -> AdapterResult<Vec<(String, String, AdapterResult<()>)>>;

    /// Get freshness of relations
    fn freshness(
        &self,
        relations: &[Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<'_, BTreeMap<String, MetadataFreshness>>;

    /// List relations in the specified [CatalogAndSchema] in parallel
    ///
    /// # Arguments
    /// * `db_schemas` - List of (catalog, schema) pairs to discover relations in
    ///
    fn list_relations_in_parallel(
        &self,
        _db_schemas: &[CatalogAndSchema],
    ) -> AsyncAdapterResult<'_, BTreeMap<CatalogAndSchema, AdapterResult<RelationVec>>>;

    fn create_relations_from_executed_nodes(
        &self,
        resolved_state: &ResolverState,
        run_stats: &Stats,
        parent_map: &BTreeMap<String, Vec<String>>,
    ) -> Vec<Arc<dyn BaseRelation>> {
        let adapter_type = resolved_state.adapter_type;
        let mut relations: Vec<Arc<dyn BaseRelation>> = Vec::new();
        let mut executed_unique_ids = run_stats
            .stats
            .iter()
            .map(|stat| stat.unique_id.clone())
            .collect::<Vec<String>>();
        let mut nodes_to_add = Vec::new();
        nodes_to_add.extend(executed_unique_ids.clone());
        // Add all nodes that are dependencies of the executed nodes
        while let Some(unique_id) = nodes_to_add.pop() {
            if !executed_unique_ids.contains(&unique_id) {
                executed_unique_ids.push(unique_id.clone());
            }
            if let Some(parents) = parent_map.get(&unique_id) {
                nodes_to_add.extend(parents.clone());
            }
        }
        let nodes = match run_stats.nodes.as_ref() {
            Some(nodes) => nodes,
            None => return relations,
        };
        for (unique_id, node) in nodes.models.iter() {
            if executed_unique_ids.contains(unique_id) {
                let relation = create_relation_internal(
                    adapter_type,
                    node.database(),
                    node.schema(),
                    Some(node.alias()),
                    None,
                    node.quoting(),
                )
                .expect("Failed to create relations from nodes");
                relations.push(relation);
            }
        }

        for (unique_id, node) in nodes.snapshots.iter() {
            if executed_unique_ids.contains(unique_id) {
                let relation = create_relation_internal(
                    adapter_type,
                    node.database(),
                    node.schema(),
                    Some(node.alias()),
                    None,
                    node.quoting(),
                )
                .expect("Failed to create relations from nodes");
                relations.push(relation);
            }
        }

        for (unique_id, node) in nodes.seeds.iter() {
            if executed_unique_ids.contains(unique_id) {
                let relation = create_relation_internal(
                    adapter_type,
                    node.database(),
                    node.schema(),
                    Some(node.alias()),
                    None,
                    node.quoting(),
                )
                .expect("Failed to create relations from nodes");
                relations.push(relation);
            }
        }

        for (unique_id, node) in nodes.sources.iter() {
            if executed_unique_ids.contains(unique_id) {
                let relation = create_relation_internal(
                    adapter_type,
                    node.database(),
                    node.schema(),
                    Some(node.alias()),
                    None,
                    node.quoting(),
                )
                .expect("Failed to create relations from nodes");
                relations.push(relation);
            }
        }

        relations
    }

    /// Check if the returned error is due to insufficient permissions.
    fn is_permission_error(&self, e: &AdapterError) -> bool {
        #[cfg(debug_assertions)]
        {
            println!("is_permission_error: {:?}: {}", e, e.sqlstate());
        }
        false
    }
}

/// Create schemas if they don't exist
///
/// Caveat: you'll want to first use this helper to create catalogs for the schemas you're going to create
/// before using it to create schemas
#[allow(clippy::type_complexity)]
pub fn create_schemas_if_not_exists(
    adapter: Arc<dyn MetadataAdapter>,
    state: &State,
    catalog_schemas: &BTreeMap<String, BTreeSet<String>>,
) -> AdapterResult<Vec<(String, String, AdapterResult<()>)>> {
    let catalog_schemas = flatten_catalog_schemas(catalog_schemas);

    let map_f = |(catalog, schema): (String, String)| -> AdapterResult<(String, String, AdapterResult<()>)> {
        let mock_relation = create_relation(
            adapter.adapter_type(),
            catalog.clone(),
            schema.clone(),
            None,
            None,
            adapter.get_resolved_quoting(),
        )?;
        let res =
        match execute_macro(state, &[mock_relation.as_value()], "create_schema") {
            Ok(_) => Ok(()),
            Err(e) => {
                if adapter.is_permission_error(&e) {
                    Ok(())
                } else if adapter.adapter_type() == AdapterType::Bigquery {
                    Err(e)
                } else {
                    return Err(e);
                }
            }
        };
        Ok((catalog, schema, res))
    };

    catalog_schemas.into_iter().map(map_f).collect()
}

pub fn flatten_catalog_schemas(
    catalog_schemas: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<(String, String)> {
    catalog_schemas
        .iter()
        .flat_map(|(catalog, schemas)| {
            schemas
                .iter()
                .map(|schema| (catalog.clone(), schema.clone()))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
}
