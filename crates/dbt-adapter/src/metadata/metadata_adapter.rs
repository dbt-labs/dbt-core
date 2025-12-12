use crate::errors::{AdapterError, AdapterResult, AsyncAdapterResult};
use crate::funcs::execute_macro;
use crate::metadata::*;
use crate::relation::{create_relation, create_relation_internal};
use crate::sql_types::{SdfSchema, arrow_schema_to_sdf_schema};
use crate::time_machine::{
    args_freshness, args_list_relations_in_parallel, args_list_relations_schemas,
    args_list_relations_schemas_by_patterns, args_list_udfs, with_time_machine_metadata_wrapper,
};
use crate::typed_adapter::TypedBaseAdapter;

use arrow::array::RecordBatch;
use dbt_common::adapter::ExecutionPhase;
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

/// Adapter that supports metadata query.
///
/// # Recording Pattern
///
/// Methods that perform I/O follow the `*_inner` pattern for transparent recording:
/// - Implementers override `*_inner` methods with the actual implementation
/// - Public methods are provided by the trait and wrap `_inner` with recording
/// - Call sites use the public methods and don't need to know about recording
///
/// Example:
/// ```ignore
/// impl MetadataAdapter for MyAdapter {
///     fn list_relations_schemas_inner(
///         &self,
///         unique_id: Option<String>,
///         phase: Option<ExecutionPhase>,
///         relations: &[Arc<dyn BaseRelation>],
///     ) -> AsyncAdapterResult<'_, HashMap<String, AdapterResult<Arc<Schema>>>> {
///         // Actual implementation here
///     }
/// }
/// ```
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

    /// Check if the returned error is due to insufficient permissions.
    fn is_permission_error(&self, e: &AdapterError) -> bool {
        #[cfg(debug_assertions)]
        {
            dbt_common::tracing::emit::println(format!(
                "is_permission_error: {:?}: {}",
                e,
                e.sqlstate()
            ));
        }
        false
    }

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

    /// Create schemas if they don't exist
    #[allow(clippy::type_complexity)]
    fn create_schemas_if_not_exists(
        &self,
        state: &State<'_, '_>,
        catalog_schemas: &BTreeMap<String, BTreeSet<String>>,
    ) -> AdapterResult<Vec<(String, String, AdapterResult<()>)>>;

    // =========================================================================
    // Async I/O methods - use _inner pattern for recording
    // =========================================================================

    /// List UDFs under a given set of catalog and schemas (implementation).
    ///
    /// Override this method with your adapter's implementation.
    /// Call `list_user_defined_functions` for the recorded version.
    fn list_user_defined_functions_inner(
        &self,
        _catalog_schemas: &BTreeMap<String, BTreeSet<String>>,
    ) -> AsyncAdapterResult<'_, Vec<UDF>> {
        Box::pin(async move { Ok(vec![]) })
    }

    /// List UDFs under a given set of catalog and schemas.
    ///
    /// This is a provided method that wraps `list_user_defined_functions_inner`
    /// with time machine recording.
    fn list_user_defined_functions<'a>(
        &'a self,
        catalog_schemas: &'a BTreeMap<String, BTreeSet<String>>,
    ) -> AsyncAdapterResult<'a, Vec<UDF>> {
        with_time_machine_metadata_wrapper(
            "global",
            "list_user_defined_functions",
            args_list_udfs(catalog_schemas),
            self.list_user_defined_functions_inner(catalog_schemas),
        )
    }

    /// List relations and their schemas (implementation).
    ///
    /// Override this method with your adapter's implementation.
    /// Call `list_relations_schemas` for the recorded version.
    fn list_relations_schemas_inner(
        &self,
        unique_id: Option<String>,
        phase: Option<ExecutionPhase>,
        relations: &[Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<'_, HashMap<String, AdapterResult<Arc<Schema>>>>;

    /// List relations and their schemas.
    ///
    /// This is a provided method that wraps `list_relations_schemas_inner`
    /// with time machine recording.
    fn list_relations_schemas<'a>(
        &'a self,
        unique_id: Option<String>,
        phase: Option<ExecutionPhase>,
        relations: &'a [Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<'a, HashMap<String, AdapterResult<Arc<Schema>>>> {
        let caller_id = unique_id.clone().unwrap_or_else(|| "global".to_string());
        with_time_machine_metadata_wrapper(
            caller_id,
            "list_relations_schemas",
            args_list_relations_schemas(
                unique_id.clone(),
                phase.map(|p| p.as_str().to_string()),
                relations.iter().map(|r| r.semantic_fqn()),
            ),
            self.list_relations_schemas_inner(unique_id, phase, relations),
        )
    }

    /// Convert schemas to SDF schemas.
    ///
    /// This wraps `list_relations_schemas` and converts the result.
    fn list_relations_sdf_schemas<'a>(
        &'a self,
        unique_id: Option<String>,
        phase: Option<ExecutionPhase>,
        relations: &'a [Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<'a, HashMap<String, AdapterResult<SdfSchema>>> {
        let future = async move {
            self.list_relations_schemas(unique_id, phase, relations)
                .await
                .map(|map| {
                    map.into_iter()
                        .map(|(k, v)| {
                            let v = v.and_then(|schema| {
                                arrow_schema_to_sdf_schema(schema, self.engine().type_ops())
                            });
                            (k, v)
                        })
                        .collect()
                })
        };
        Box::pin(future)
    }

    /// List relations and their schemas by patterns (implementation).
    ///
    /// Override this method with your adapter's implementation.
    /// Call `list_relations_schemas_by_patterns` for the recorded version.
    #[allow(clippy::type_complexity)]
    fn list_relations_schemas_by_patterns_inner(
        &self,
        patterns: &[RelationPattern],
    ) -> AsyncAdapterResult<'_, Vec<(String, AdapterResult<RelationSchemaPair>)>>;

    /// List relations and their schemas by patterns.
    ///
    /// This is a provided method that wraps `list_relations_schemas_by_patterns_inner`
    /// with time machine recording.
    #[allow(clippy::type_complexity)]
    fn list_relations_schemas_by_patterns<'a>(
        &'a self,
        patterns: &'a [RelationPattern],
    ) -> AsyncAdapterResult<'a, Vec<(String, AdapterResult<RelationSchemaPair>)>> {
        with_time_machine_metadata_wrapper(
            "global",
            "list_relations_schemas_by_patterns",
            args_list_relations_schemas_by_patterns(
                patterns
                    .iter()
                    .map(|p| format!("{}.{}.{}", p.database, p.schema_pattern, p.table_pattern)),
            ),
            self.list_relations_schemas_by_patterns_inner(patterns),
        )
    }

    /// Get freshness of relations (implementation).
    ///
    /// Override this method with your adapter's implementation.
    /// Call `freshness` for the recorded version.
    fn freshness_inner(
        &self,
        relations: &[Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<'_, BTreeMap<String, MetadataFreshness>>;

    /// Get freshness of relations.
    ///
    /// This is a provided method that wraps `freshness_inner`
    /// with time machine recording.
    fn freshness<'a>(
        &'a self,
        relations: &'a [Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<'a, BTreeMap<String, MetadataFreshness>> {
        with_time_machine_metadata_wrapper(
            "global",
            "freshness",
            args_freshness(relations.iter().map(|r| r.semantic_fqn())),
            self.freshness_inner(relations),
        )
    }

    /// List relations in the specified [CatalogAndSchema] in parallel (implementation).
    ///
    /// Override this method with your adapter's implementation.
    /// Call `list_relations_in_parallel` for the recorded version.
    ///
    /// # Arguments
    /// * `db_schemas` - List of (catalog, schema) pairs to discover relations in
    fn list_relations_in_parallel_inner(
        &self,
        db_schemas: &[CatalogAndSchema],
    ) -> AsyncAdapterResult<'_, BTreeMap<CatalogAndSchema, AdapterResult<RelationVec>>>;

    /// List relations in the specified [CatalogAndSchema] in parallel.
    ///
    /// This is a provided method that wraps `list_relations_in_parallel_inner`
    /// with time machine recording.
    ///
    /// # Arguments
    /// * `db_schemas` - List of (catalog, schema) pairs to discover relations in
    fn list_relations_in_parallel<'a>(
        &'a self,
        db_schemas: &'a [CatalogAndSchema],
    ) -> AsyncAdapterResult<'a, BTreeMap<CatalogAndSchema, AdapterResult<RelationVec>>> {
        with_time_machine_metadata_wrapper(
            "global",
            "list_relations_in_parallel",
            args_list_relations_in_parallel(
                db_schemas
                    .iter()
                    .map(|s| (s.resolved_catalog.clone(), s.resolved_schema.clone())),
            ),
            self.list_relations_in_parallel_inner(db_schemas),
        )
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
            adapter.quoting()
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
