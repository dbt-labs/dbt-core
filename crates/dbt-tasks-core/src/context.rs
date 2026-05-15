use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use dbt_adapter_core::AdapterType;
use dbt_common::FsResult;
use dbt_common::collections::{DashMap, SccHashMap};
use dbt_common::stats::{NodeStatus, Stat};
use dbt_dag::schedule::Schedule;
use dbt_schemas::materialization_resolver::MaterializationResolver;
use dbt_schemas::schemas::Nodes;
use dbt_schemas::schemas::common::UpdatesOn;
use dbt_schemas::state::{DbtProfile, DbtRuntimeConfig, ResolverState};
use minijinja::Value;

use crate::RunTasksArgs;
use crate::span_manager::SpanManager;
use crate::test_aggregation::GenericTestRelationships;
use crate::visitor::SkipReason;

use dbt_schemas::schemas::common::DbtMaterialization;

/// Information about a rendered node, used for unit test hash computation.
#[derive(Debug, Clone)]
pub struct RenderedNodeInfo {
    pub sql: String,
    pub materialization: DbtMaterialization,
}

pub struct TaskRunnerCtxInner {
    pub arg: Arc<RunTasksArgs>,
    pub worker_id: String,
    pub schedule: Schedule<String>,
    pub runtime_deps: BTreeMap<String, BTreeSet<String>>,
    pub base_context: BTreeMap<String, Value>,
    pub analyze_stats: DashMap<String, Stat>,
    pub run_stats: DashMap<String, Stat>,
    pub node_hashes: DashMap<String, String>,
    pub rendered_sql: DashMap<String, RenderedNodeInfo>,
    pub freshness_seconds: SccHashMap<String, i64>,
    pub updates_on: SccHashMap<String, UpdatesOn>,
    pub execute: dbt_schemas::schemas::profiles::Execute,
    // TODO: Use SIPHash128 for fingerprinting the sets
    pub runnable_set: BTreeSet<String>,
    pub extended_ctx: Box<dyn ExtendedCtx>,
    pub materialization_resolver: Arc<MaterializationResolver>,
    pub root_project_name: String,
    pub adapter_type: AdapterType,
    pub dbt_profile: Arc<DbtProfile>,
    pub runtime_config: Arc<DbtRuntimeConfig>,
    pub generic_test_relationships: GenericTestRelationships,
    span_manager: Arc<SpanManager<FsResult<NodeStatus>, SkipReason>>,
    /// Captured show batches for the LSP preview path; set by run_show, collected after the task loop.
    pub preview_results: parking_lot::Mutex<Option<(Vec<RecordBatch>, SchemaRef)>>,
    /// Error from a failed show query; set by run_show when execution fails, collected after the task loop.
    pub preview_error: parking_lot::Mutex<Option<String>>,
}

impl TaskRunnerCtxInner {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        arg: Arc<RunTasksArgs>,
        worker_id: String,
        schedule: Schedule<String>,
        base_context: BTreeMap<String, Value>,
        node_hashes: DashMap<String, String>,
        extended_ctx: Box<dyn ExtendedCtx>,
        resolver_state: &Arc<ResolverState>,
        generic_test_relationships: GenericTestRelationships,
        span_manager: Arc<SpanManager<FsResult<NodeStatus>, SkipReason>>,
        execute: dbt_schemas::schemas::profiles::Execute,
    ) -> Self {
        let runnable_set = schedule
            .selected_nodes
            .iter()
            .filter_map(|node_id| {
                resolver_state
                    .nodes
                    .get_node(node_id)
                    .map(|node| node.common().unique_id.clone())
            })
            .collect();
        let runtime_deps = compute_runtime_dependencies(&resolver_state.nodes, &schedule);

        let materialization_resolver = MaterializationResolver::new(
            &resolver_state.macros.macros,
            resolver_state.adapter_type,
            &resolver_state.root_project_name,
        );

        TaskRunnerCtxInner {
            arg,
            worker_id,
            schedule,
            runtime_deps,
            base_context,
            analyze_stats: DashMap::default(),
            run_stats: DashMap::default(),
            node_hashes,
            rendered_sql: DashMap::default(),
            freshness_seconds: SccHashMap::default(),
            updates_on: SccHashMap::default(),
            execute,
            runnable_set,
            extended_ctx,
            materialization_resolver: Arc::new(materialization_resolver),
            root_project_name: resolver_state.root_project_name.clone(),
            adapter_type: resolver_state.adapter_type,
            dbt_profile: Arc::new(resolver_state.dbt_profile.clone()),
            runtime_config: resolver_state.runtime_config.clone(),
            generic_test_relationships,
            span_manager,
            preview_results: parking_lot::Mutex::new(None),
            preview_error: parking_lot::Mutex::new(None),
        }
    }

    pub fn span_manager(&self) -> Arc<SpanManager<FsResult<NodeStatus>, SkipReason>> {
        self.span_manager.clone()
    }
}

fn compute_runtime_dependencies(
    nodes: &Nodes,
    schedule: &Schedule<String>,
) -> BTreeMap<String, BTreeSet<String>> {
    let mut deps: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for node_id in schedule.selected_nodes.iter() {
        let node = nodes.get_node(node_id).expect("Node not found");
        let unique_id = node.common().unique_id.clone();
        let mut dependency_set = BTreeSet::new();
        for dep in node.base().depends_on.nodes.iter() {
            if !nodes.contains(dep) {
                continue;
            }
            dependency_set.insert(dep.clone());
        }
        for scheduled_dep in schedule.deps.get(&unique_id).cloned().unwrap_or_default() {
            if scheduled_dep.starts_with("source.") || scheduled_dep.starts_with("seed.") {
                dependency_set.insert(scheduled_dep);
            }
        }
        deps.insert(unique_id, dependency_set);
    }
    deps
}

/// Virtual context that is part of the bigger [`TaskRunnerCtx`] structure.
///
/// It allows carrying additional information without coupling the tasks runner
/// with every piece of information available in the context.
pub trait ExtendedCtx: Send + Sync + Any {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}
