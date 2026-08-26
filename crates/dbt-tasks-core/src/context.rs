use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64};
use std::time::Instant;

use crate::run_cache::run_cache_service::{
    CachedTestExecutionResult, HeuristicClock, TelemetryDispatcher,
};
use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use dbt_adapter::relation::create_relation_from_node;
use dbt_adapter::response::AdapterResponse;
use dbt_adapter_core::AdapterType;
use dbt_common::FsResult;
use dbt_common::collections::{DashMap, SccHashMap};
use dbt_common::io_args::OptimizeTestsOptions;
use dbt_common::path::DbtPath;
use dbt_common::stats::{NodeStatus, Stat};
use dbt_dag::schedule::Schedule;
use dbt_frontend_common::sources_extractor::SourcesExtractor;
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_jinja_utils::phases::compile::{
    DependencyValidationConfig, build_compile_node_context_inner,
};
use dbt_schema_store::{DataStoreTrait, SchemaStoreTrait};
use dbt_schemas::materialization_resolver::MaterializationResolver;
use dbt_schemas::schemas::common::UpdatesOn;
use dbt_schemas::schemas::nodes::TestMetadata;
use dbt_schemas::schemas::relations::base::BaseRelation;
use dbt_schemas::schemas::{BatchResults, InternalDbtNode, InternalDbtNodeAttributes, Nodes};
use dbt_schemas::state::{DbtProfile, DbtRuntimeConfig, NodeResolverTracker, ResolverState};
use dbt_state::explain::StateExplainDevClone;
use dbt_state::metadata_cache::RunCacheMetadataCache;
use dbt_state::service_client::SharedRunCacheServiceClient;
use dbt_state::service_config::RunCacheServiceConfig;
use dbt_state::view_traversal::ViewDefinitionTraverser;
use minijinja::Value;

use crate::RunTasksArgs;
use crate::span_manager::SpanManager;
use crate::task::Task;
use crate::test_aggregation::{GenericTestRelationships, is_data_test_static_analysis_eligible};
use crate::visitor::SkipReason;

use dbt_schemas::schemas::common::DbtMaterialization;

/// Run-cache fields that live on [`TaskRunnerCtxInner`] and are passed in at
/// construction time from the extended-context factory.
pub struct RunCacheCtx {
    pub run_cache_metadata: Arc<RunCacheMetadataCache>,
    pub run_cache_dev_cloned_nodes: DashMap<String, StateExplainDevClone>,
    pub run_cache_deferred_fqns: BTreeSet<String>,
    pub run_cache_service_requested: bool,
    pub run_cache_service_config: Option<RunCacheServiceConfig>,
    pub run_cache_service_client: Option<SharedRunCacheServiceClient>,
    /// Run-scoped dbt State explain log file, when state config is available.
    pub state_explain_log_path: Option<PathBuf>,
    pub view_traverser: Option<Arc<ViewDefinitionTraverser>>,
    /// Run-start warehouse clock, set once in `run_cache_service_before_run`.
    /// When present, `confirm_run_cache_service_execution` uses it to stamp
    /// freshly-executed tables without an additional warehouse round-trip.
    pub heuristic_clock: std::sync::OnceLock<HeuristicClock>,
    /// Tracks the background dependency last-modified prefetch so per-node
    /// submits can observe its progress and await it on demand.
    pub prefetch: RunCachePrefetchState,
    pub telemetry_event_order: AtomicI64,
    pub telemetry_session_start: std::sync::OnceLock<Instant>,
    pub telemetry_session_ended: AtomicBool,
    /// Background telemetry batching worker, started lazily on the first
    /// event so runs with the dbt State service disabled never spawn it.
    pub telemetry_dispatcher: std::sync::OnceLock<TelemetryDispatcher>,
}

/// Lifecycle handle for the background dependency last-modified prefetch.
///
/// The prefetch is started once at run start and runs concurrently with node
/// execution. This handle is `Arc`-shared so the spawned prefetch task and the
/// per-node submit paths observe the same started/done flags and can join the
/// task to completion.
#[derive(Clone, Default)]
pub struct RunCachePrefetchState {
    inner: Arc<RunCachePrefetchStateInner>,
}

#[derive(Default)]
struct RunCachePrefetchStateInner {
    /// Set once the prefetch has been kicked off (or determined to be a no-op).
    started: AtomicBool,
    /// Set once the prefetch has finished (or there was nothing to fetch).
    done: AtomicBool,
    /// Handle to the background prefetch task, taken and joined the first time
    /// the prefetch is awaited. `None` when there was nothing to spawn or it
    /// has already been joined.
    handle: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl RunCachePrefetchState {
    /// Whether the prefetch has been started.
    pub fn is_started(&self) -> bool {
        self.inner
            .started
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Mark the prefetch as started.
    pub fn mark_started(&self) {
        self.inner
            .started
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Whether the prefetch has finished (or there was nothing to fetch).
    pub fn is_done(&self) -> bool {
        self.inner.done.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Mark the prefetch as finished.
    pub fn mark_done(&self) {
        self.inner
            .done
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Store the handle to the background prefetch task.
    pub async fn set_handle(&self, handle: tokio::task::JoinHandle<()>) {
        *self.inner.handle.lock().await = Some(handle);
    }

    /// Await the background prefetch task to completion.
    ///
    /// The join handle is awaited while holding the internal lock so that
    /// concurrent callers serialize and every caller observes completion before
    /// returning (the handle itself can only be joined once). A no-op once the
    /// handle has already been joined or was never set. Returns an error
    /// description if the task did not complete cleanly.
    pub async fn join(&self) -> Result<(), String> {
        let mut guard = self.inner.handle.lock().await;
        if let Some(handle) = guard.take() {
            return handle.await.map_err(|err| err.to_string());
        }
        Ok(())
    }
}

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
    pub data_test_execution_results: DashMap<String, CachedTestExecutionResult>,
    pub batch_results_map: DashMap<String, BatchResults>,
    pub main_adapter_responses: DashMap<String, AdapterResponse>,
    pub node_hashes: DashMap<String, String>,
    pub rendered_sql: DashMap<String, RenderedNodeInfo>,
    pub freshness_seconds: SccHashMap<String, i64>,
    pub updates_on: SccHashMap<String, UpdatesOn>,
    pub execute: dbt_schemas::schemas::profiles::Execute,
    // TODO: Use SIPHash128 for fingerprinting the sets
    pub runnable_set: BTreeSet<String>,
    pub extended_ctx: Box<dyn ExtendedCtx>,
    pub compiled_sql_cache: Arc<dyn crate::CompiledSqlCache>,
    pub adhoc_runner: Arc<dyn crate::AdhocRunner>,
    pub materialization_resolver: Arc<MaterializationResolver>,
    pub root_project_name: String,
    pub adapter_type: AdapterType,
    pub sources_extractor: Arc<dyn SourcesExtractor>,
    pub dbt_profile: Arc<DbtProfile>,
    pub runtime_config: Arc<DbtRuntimeConfig>,
    pub generic_test_relationships: GenericTestRelationships,
    span_manager: Arc<SpanManager<FsResult<NodeStatus>, SkipReason>>,
    /// Captured show batches for the LSP preview path; set by run_show, collected after the task loop.
    pub preview_results: parking_lot::Mutex<Option<(Vec<RecordBatch>, SchemaRef)>>,
    /// Error from a failed show query; set by run_show when execution fails, collected after the task loop.
    pub preview_error: parking_lot::Mutex<Option<String>>,
    // <Start> RunCache-related fields. These are only populated when the RunCache is enabled for the current execution.
    pub run_cache_ctx: RunCacheCtx,
    // <End> RunCache-related fields.
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
        compiled_sql_cache: Arc<dyn crate::CompiledSqlCache>,
        adhoc_runner: Arc<dyn crate::AdhocRunner>,
        resolver_state: &Arc<ResolverState>,
        generic_test_relationships: GenericTestRelationships,
        span_manager: Arc<SpanManager<FsResult<NodeStatus>, SkipReason>>,
        execute: dbt_schemas::schemas::profiles::Execute,
        sources_extractor: Arc<dyn SourcesExtractor>,
        run_cache_ctx: RunCacheCtx,
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
            &resolver_state.root_project_name,
        );

        let batch_results_map: DashMap<String, BatchResults> = {
            let map = DashMap::default();
            for (uid, br) in &arg.previous_batch_results {
                map.insert(uid.clone(), br.clone());
            }
            map
        };

        TaskRunnerCtxInner {
            arg,
            worker_id,
            schedule,
            runtime_deps,
            base_context,
            analyze_stats: DashMap::default(),
            run_stats: DashMap::default(),
            data_test_execution_results: DashMap::default(),
            batch_results_map,
            main_adapter_responses: DashMap::default(),
            node_hashes,
            rendered_sql: DashMap::default(),
            freshness_seconds: SccHashMap::default(),
            updates_on: SccHashMap::default(),
            execute,
            runnable_set,
            extended_ctx,
            compiled_sql_cache,
            adhoc_runner,
            materialization_resolver: Arc::new(materialization_resolver),
            root_project_name: resolver_state.root_project_name.clone(),
            adapter_type: resolver_state.adapter_type,
            sources_extractor,
            dbt_profile: Arc::new(resolver_state.dbt_profile.clone()),
            runtime_config: resolver_state.runtime_config.clone(),
            generic_test_relationships,
            span_manager,
            preview_results: parking_lot::Mutex::new(None),
            preview_error: parking_lot::Mutex::new(None),
            run_cache_ctx,
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

    fn on_test_failure(
        &self,
        ctx: &TaskRunnerCtx,
        node: &Arc<dyn Task>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>;

    fn is_sidecar(&self) -> bool;

    fn should_statically_skip_data_test(
        &self,
        _model_unique_id: &str,
        _test_metadata: &TestMetadata,
        _column_name: &str,
    ) -> bool {
        false
    }
}

#[derive(Clone)]
pub struct TaskRunnerCtx {
    pub inner: Arc<TaskRunnerCtxInner>,
    pub env: Arc<JinjaEnv>,
    pub schema_cache: Arc<dyn SchemaStoreTrait>,
    pub data_store: Arc<dyn DataStoreTrait>,
    pub resolver_state: Arc<ResolverState>, // TODO: make private
    pub rendering_listener_factory:
        Arc<dyn dbt_jinja_utils::listener::RenderingEventListenerFactory>,
    /// Logical worker slot assigned by the visitor before task execution.
    /// Appears in stats as `"Thread-{N}"` and in Jinja as `{{ thread_id }}`.
    pub thread_id: i32,
    // **** DO NOT ADD Arc<> FIELDS HERE ****
    // Tokio spends a lot of time deallocating the TaskRunnerCtx for fast operations.
    // If you need to add a field, add it to TaskRunnerCtxInner instead.
    // (Plain scalars like thread_id are fine — only Arc/heap fields are expensive to drop.)
}

impl TaskRunnerCtx {
    pub async fn is_data_test_statically_skippable(&self, unique_id: &str) -> bool {
        if !self
            .inner
            .arg
            .optimize_tests
            .contains(&OptimizeTestsOptions::TestStaticAnalysis)
        {
            return false;
        }

        let Some(test) = self.nodes().tests.get(unique_id) else {
            return false;
        };
        if !is_data_test_static_analysis_eligible(test) {
            return false;
        }

        let Some(column) = test.__test_attr__.column_name.as_deref() else {
            return false;
        };
        let Some(test_metadata) = test.__test_attr__.test_metadata.as_ref() else {
            return false;
        };
        let Some(model_unique_id) = test.__test_attr__.attached_node.as_deref() else {
            return false;
        };

        self.inner.extended_ctx.should_statically_skip_data_test(
            model_unique_id,
            test_metadata,
            column,
        )
    }
}

impl TaskRunnerCtx {
    pub fn root_project_name(&self) -> &str {
        &self.inner.root_project_name
    }

    pub fn adapter_type(&self) -> AdapterType {
        self.inner.adapter_type
    }

    pub fn extended_ctx<T: ExtendedCtx + 'static>(&self) -> Option<&T> {
        self.inner.extended_ctx.as_any().downcast_ref::<T>()
    }

    pub fn dbt_profile(&self) -> &DbtProfile {
        &self.inner.dbt_profile
    }

    pub fn runtime_config(&self) -> &DbtRuntimeConfig {
        &self.inner.runtime_config
    }

    pub fn generic_test_relationships(&self) -> &GenericTestRelationships {
        &self.inner.generic_test_relationships
    }

    pub fn resolver_state(&self) -> &Arc<ResolverState> {
        &self.resolver_state
    }

    pub fn has_seed(&self, base_db: &str, base_schema: &str, base_identifier: &str) -> bool {
        self.resolver_state.nodes.seeds.values().any(|seed| {
            seed.base().database == base_db
                && seed.base().schema == base_schema
                && seed.base().alias == base_identifier
                && self
                    .inner
                    .runnable_set
                    .contains(seed.common().unique_id.as_str())
        })
    }

    pub fn try_get_model_original_file_path(&self, unique_id: &str) -> Option<&DbtPath> {
        self.resolver_state
            .nodes
            .models
            .get(unique_id)
            .map(|model| &model.__common_attr__.original_file_path)
    }

    pub fn try_get_relation_from_node(&self, unique_id: &str) -> Option<Arc<dyn BaseRelation>> {
        self.resolver_state.nodes.get_node(unique_id).map(|node| {
            create_relation_from_node(self.adapter_type(), node, None)
                .expect("Failed to create relation from node")
                .into()
        })
    }

    pub fn defer_nodes(&self) -> Option<&Nodes> {
        self.resolver_state.defer_nodes.as_ref()
    }

    /// Get the nodes from the resolver state.
    pub fn nodes(&self) -> &Nodes {
        &self.resolver_state.nodes
    }

    /// Get the node resolver for ref/source resolution.
    pub fn node_resolver(&self) -> Arc<dyn NodeResolverTracker> {
        self.resolver_state.node_resolver.clone()
    }

    #[allow(clippy::type_complexity, clippy::too_many_arguments)]
    pub fn build_compile_node_context<T>(
        &self,
        model: &T,
        base_context: &BTreeMap<String, Value>,
        ref_validation_config: DependencyValidationConfig,
    ) -> FsResult<(BTreeMap<String, Value>, Arc<DashMap<String, Value>>)>
    where
        T: InternalDbtNodeAttributes + ?Sized,
    {
        build_compile_node_context_inner(
            model,
            self.adapter_type(),
            base_context,
            self.root_project_name(),
            self.resolver_state.node_resolver.clone(),
            self.inner.runtime_config.clone(),
            ref_validation_config,
        )
    }

    pub fn on_test_failure(
        &self,
        node: &Arc<dyn Task>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.inner.extended_ctx.on_test_failure(self, node)
    }

    pub fn is_sidecar(&self) -> bool {
        self.inner.extended_ctx.is_sidecar()
    }
}
