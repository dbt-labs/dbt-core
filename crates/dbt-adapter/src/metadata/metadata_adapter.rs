use crate::adapter::adapter_impl::AdapterImpl;
use crate::errors::{AdapterError, AdapterErrorKind, AdapterResult, AsyncAdapterResult};
use crate::macro_exec::execute_macro;
use crate::relation::{RelationObject, create_relation, do_create_relation};
use crate::sql_types::{SdfSchema, arrow_schema_to_sdf_schema};
use crate::time_machine::{
    args_fetch_view_definitions, args_freshness, args_list_relations_in_parallel,
    args_list_relations_schemas, args_list_relations_schemas_by_patterns, args_list_udfs,
    with_time_machine_metadata_wrapper,
};
use crate::{AdapterEngine, metadata::*};

use arrow::array::RecordBatch;
use dbt_adapter_core::ExecutionPhase;
use dbt_common::cancellation::{Cancellable, CancellationToken};

use dbt_schemas::schemas::{
    legacy_catalog::{CatalogTable, ColumnMetadata},
    relations::base::{BaseRelation, RelationPattern},
};
use dbt_schemas::state::ResolverState;
use dbt_schemas::stats::Stats;
use dbt_telemetry::NodeType;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

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
pub trait MetadataAdapter: Send + Sync {
    /// The adapter type backing this metadata adapter (Snowflake, BigQuery, ...).
    /// Used by callers (e.g. `ViewDefinitionTraverser`) that need to construct
    /// dialect-shaped relations without an external mapping table.
    fn adapter_type(&self) -> AdapterType; // TODO: remove this and pass Arc 
    // into ViewDefinitionTraverser instead

    fn build_schemas_from_stats_sql(
        &self,
        _: Arc<RecordBatch>,
    ) -> AdapterResult<BTreeMap<String, CatalogTable>>;

    fn build_columns_from_get_columns(
        &self,
        _: Arc<RecordBatch>,
    ) -> AdapterResult<BTreeMap<String, BTreeMap<String, ColumnMetadata>>>;

    /// Check if the returned error is due to insufficient permissions.
    #[allow(unused_variables)]
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
    ) -> Vec<Arc<dyn BaseRelation>> {
        let catalog_resource_types = [
            NodeType::Source,
            NodeType::Model,
            NodeType::Snapshot,
            NodeType::Seed,
        ];
        let adapter_type = resolved_state.adapter_type;

        // Collect executed nodes and their direct source dependencies
        let mut relevant_ids = BTreeSet::new();
        for stat in &run_stats.stats {
            let unique_id = &stat.unique_id;
            let Some(node) = resolved_state.nodes.get_node(unique_id) else {
                continue;
            };
            if !catalog_resource_types.contains(&node.resource_type()) {
                continue;
            }

            relevant_ids.insert(unique_id.clone());
            // Include direct source parents from the parent map
            let parents = &node.base().depends_on.nodes;
            relevant_ids.extend(parents.iter().filter(|p| p.starts_with("source.")).cloned());
        }

        relevant_ids
            .iter()
            .filter_map(|uid| resolved_state.nodes.get_node(uid))
            .map(|node| {
                create_relation(
                    adapter_type,
                    node.database(),
                    node.schema(),
                    Some(node.alias()),
                    None,
                    node.quoting(),
                )
                .expect("Failed to create relations from nodes")
                .into()
            })
            .collect()
    }

    /// Create schemas if they don't exist
    #[allow(clippy::type_complexity)]
    fn create_schemas_if_not_exists(
        &self,
        state: &State<'_, '_>,
        catalog_schemas: Vec<(String, String, String)>,
    ) -> AdapterResult<Vec<(String, String, String, AdapterResult<()>)>>;

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
        _token: CancellationToken,
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
        token: CancellationToken,
    ) -> AsyncAdapterResult<'a, Vec<UDF>> {
        with_time_machine_metadata_wrapper(
            "global",
            "list_user_defined_functions",
            args_list_udfs(catalog_schemas),
            self.list_user_defined_functions_inner(catalog_schemas, token),
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
        token: CancellationToken,
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
        token: CancellationToken,
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
            self.list_relations_schemas_inner(unique_id, phase, relations, token),
        )
    }

    /// Convert schemas to SDF schemas.
    ///
    /// This wraps `list_relations_schemas` and converts the result.
    fn list_relations_sdf_schemas<'a>(
        &'a self,
        engine: &'a dyn AdapterEngine,
        unique_id: Option<String>,
        phase: Option<ExecutionPhase>,
        relations: &'a [Arc<dyn BaseRelation>],
        token: CancellationToken,
    ) -> AsyncAdapterResult<'a, HashMap<String, AdapterResult<SdfSchema>>> {
        let future = async move {
            self.list_relations_schemas(unique_id, phase, relations, token)
                .await
                .map(|map| {
                    map.into_iter()
                        .map(|(k, v)| {
                            let v = v.and_then(|schema| {
                                arrow_schema_to_sdf_schema(schema, engine.type_ops().as_ref())
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
        token: CancellationToken,
    ) -> AsyncAdapterResult<'_, Vec<(String, AdapterResult<RelationSchemaPair>)>>;

    /// List relations and their schemas by patterns.
    ///
    /// This is a provided method that wraps `list_relations_schemas_by_patterns_inner`
    /// with time machine recording.
    #[allow(clippy::type_complexity)]
    fn list_relations_schemas_by_patterns<'a>(
        &'a self,
        patterns: &'a [RelationPattern],
        token: CancellationToken,
    ) -> AsyncAdapterResult<'a, Vec<(String, AdapterResult<RelationSchemaPair>)>> {
        with_time_machine_metadata_wrapper(
            "global",
            "list_relations_schemas_by_patterns",
            args_list_relations_schemas_by_patterns(
                patterns
                    .iter()
                    .map(|p| format!("{}.{}.{}", p.database, p.schema_pattern, p.table_pattern)),
            ),
            self.list_relations_schemas_by_patterns_inner(patterns, token),
        )
    }

    /// Get freshness of relations (implementation).
    ///
    /// Override this method with your adapter's implementation.
    /// Call `freshness` for the recorded version.
    fn freshness_inner(
        &self,
        relations: &[Arc<dyn BaseRelation>],
        token: CancellationToken,
    ) -> AsyncAdapterResult<'_, BTreeMap<String, MetadataFreshness>>;

    /// Get freshness of relations.
    ///
    /// This is a provided method that wraps `freshness_inner`
    /// with time machine recording.
    fn freshness<'a>(
        &'a self,
        relations: &'a [Arc<dyn BaseRelation>],
        token: CancellationToken,
    ) -> AsyncAdapterResult<'a, BTreeMap<String, MetadataFreshness>> {
        with_time_machine_metadata_wrapper(
            "global",
            "freshness",
            args_freshness(relations.iter().map(|r| r.semantic_fqn())),
            self.freshness_inner(relations, token),
        )
    }

    /// Get freshness of relations, honoring per-relation overrides
    /// (`loaded_at_field`, `loaded_at_query`).
    ///
    /// Default implementation falls back to the bulk `freshness` path and
    /// silently ignores overrides — adapters that haven't ported the
    /// override path yet retain today's behavior. Adapters that override this
    /// method are expected to partition: bulk INFORMATION_SCHEMA query for the
    /// non-override subset, one targeted query per override (mirroring dbt-core's
    /// run-cache plugin).
    ///
    /// `overrides` is keyed by `BaseRelation::semantic_fqn()` and is a subset of
    /// the relations passed in `relations`.
    fn freshness_with_overrides<'a>(
        &'a self,
        relations: &'a [Arc<dyn BaseRelation>],
        _overrides: &'a BTreeMap<String, FreshnessOverride>,
        token: CancellationToken,
    ) -> AsyncAdapterResult<'a, BTreeMap<String, MetadataFreshness>> {
        self.freshness(relations, token)
    }

    /// Check whether each relation exists, keyed by semantic FQN.
    ///
    /// The default implementation uses `list_relations_in_parallel`, which is
    /// already implemented by supported metadata adapters. Adapters that
    /// cannot list relations should return an adapter error from that method;
    /// callers may treat that as fail-open.
    fn relations_exist_inner<'a>(
        &'a self,
        relations: &'a [Arc<dyn BaseRelation>],
        token: CancellationToken,
    ) -> AsyncAdapterResult<'a, BTreeMap<String, bool>> {
        let db_schemas = relations
            .iter()
            .map(CatalogAndSchema::from)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();

        let future = async move {
            let listed = self.list_relations_in_parallel(&db_schemas, token).await?;
            let mut result = BTreeMap::new();

            for relation in relations {
                let semantic_fqn = relation.semantic_fqn();
                let catalog_schema = CatalogAndSchema::from(relation);
                let Some(schema_relations) = listed.get(&catalog_schema) else {
                    result.insert(semantic_fqn, false);
                    continue;
                };

                let schema_relations = schema_relations.as_ref().map_err(|err| {
                    Cancellable::Error(AdapterError::new(err.kind(), err.message().to_string()))
                })?;
                let exists = schema_relations
                    .iter()
                    .any(|candidate| candidate.semantic_fqn() == semantic_fqn);
                result.insert(semantic_fqn, exists);
            }

            Ok(result)
        };
        Box::pin(future)
    }

    fn relations_exist<'a>(
        &'a self,
        relations: &'a [Arc<dyn BaseRelation>],
        token: CancellationToken,
    ) -> AsyncAdapterResult<'a, BTreeMap<String, bool>> {
        self.relations_exist_inner(relations, token)
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
        token: CancellationToken,
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
        token: CancellationToken,
    ) -> AsyncAdapterResult<'a, BTreeMap<CatalogAndSchema, AdapterResult<RelationVec>>> {
        with_time_machine_metadata_wrapper(
            "global",
            "list_relations_in_parallel",
            args_list_relations_in_parallel(
                db_schemas
                    .iter()
                    .map(|s| (s.resolved_catalog.clone(), s.resolved_schema.clone())),
            ),
            self.list_relations_in_parallel_inner(db_schemas, token),
        )
    }

    /// Fetch view definitions for a batch of fully-qualified table references.
    ///
    /// Implementations are responsible for:
    /// - Issuing a single (or minimal-count) database query for the entire batch.
    /// - Returning a `ViewDefinition` for each input that *is* a view.
    /// - Omitting tables, missing objects, and permission failures from the
    ///   result. The orchestrator caches those omissions so they are not re-fetched.
    ///
    /// This method must be safe to call concurrently from multiple async tasks;
    /// each call acquires its own connection via the engine's connection factory.
    ///
    /// The default implementation returns `NotSupported`; adapters that
    /// support view-definition fetching override it.
    fn fetch_view_definitions_inner<'a>(
        &'a self,
        _relations: &'a [Arc<dyn BaseRelation>],
        _token: CancellationToken,
    ) -> AsyncAdapterResult<'a, Vec<ViewDefinition>> {
        Box::pin(async {
            Err(Cancellable::Error(AdapterError::new(
                AdapterErrorKind::NotSupported,
                "fetch_view_definitions is not supported by this adapter",
            )))
        })
    }

    /// Public, time-machine-recorded wrapper around `fetch_view_definitions_inner`.
    ///
    /// Mirrors the existing `*_inner`/public pattern used by `freshness`,
    /// `list_relations_schemas`, `list_user_defined_functions`, etc.
    fn fetch_view_definitions<'a>(
        &'a self,
        relations: &'a [Arc<dyn BaseRelation>],
        token: CancellationToken,
    ) -> AsyncAdapterResult<'a, Vec<ViewDefinition>> {
        with_time_machine_metadata_wrapper(
            "global",
            "fetch_view_definitions",
            args_fetch_view_definitions(relations.iter().map(|r| r.semantic_fqn())),
            self.fetch_view_definitions_inner(relations, token),
        )
    }
}

/// Create schemas if they don't exist
///
/// Caveat: you'll want to first use this helper to create catalogs for the schemas you're going to create
/// before using it to create schemas
#[allow(clippy::type_complexity)]
pub fn create_schemas_if_not_exists(
    adapter: &AdapterImpl,
    metadata_adapter: &dyn MetadataAdapter,
    state: &State,
    catalog_schemas: Vec<(String, String, String)>,
) -> AdapterResult<Vec<(String, String, String, AdapterResult<()>)>> {
    let map_f = |(catalog, schema, unique_id): (String, String, String)| -> AdapterResult<(String, String, String, AdapterResult<()>)> {
        let mock_relation = do_create_relation(
            adapter.adapter_type(),
            catalog.clone(),
            schema.clone(),
            None,
            None,
            adapter.quoting()
        )?;
        let res =
        match execute_macro(state, &[RelationObject::new(Arc::from(mock_relation)).into_value()], "create_schema") {
            Ok(_) => Ok(()),
            Err(e) => {
                if metadata_adapter.is_permission_error(&e) {
                    Ok(())
                } else if adapter.adapter_type() == AdapterType::Bigquery {
                    Err(e)
                } else {
                    let chars = e.sqlstate().as_bytes();
                    let sqlstate: [u8; 5] = chars[..5].try_into().map_err(|_| e.clone())?;
                    let err_string = format!(
                        "Failed to create schema '{schema}' in database '{catalog}' in remote for {unique_id}: {}", e.message()
                    );
                    return Err(AdapterError::new_with_sqlstate_and_vendor_code(e.kind(), err_string, sqlstate, e.vendor_code()));
                }
            }
        };
        Ok((catalog, schema, unique_id, res))
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
