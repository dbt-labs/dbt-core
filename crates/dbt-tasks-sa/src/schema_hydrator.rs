use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use dbt_adapter::Adapter;
use dbt_adapter::engine::SidecarClient;
use dbt_common::ErrorCode;
use dbt_common::FsResult;
use dbt_common::cancellation::CancellationToken;
use dbt_common::io_args::{EvalArgs, StaticAnalysisKind};
use dbt_common::node_selector::selectors_require_manifest;
use dbt_common::tracing::emit::emit_warn_log_message;
use dbt_compilation::config::CompilationConfig;
use dbt_compilation::schema_hydration::{
    SchemaHydrationState, SchemaHydrator, SchemaHydratorFactory,
};
use dbt_dag::schedule::Schedule;
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_run_cache::run_cache_defer::RunCacheProfileResolver;
use dbt_schema_store::CanonicalFqn;
use dbt_schema_store::store::SchemaStore;
use dbt_schemas::schemas::OnManifestLoadFailure;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::{IntrospectionKind, Nodes, PreviousState, ResolvedCloudConfig};
use dbt_schemas::state::ResolverState;
use dbt_tasks_core::RunTasksArgs;
use dbt_tasks_core::metricflow::MetricflowClient;
use dbt_tasks_core::precompile::StaticAnalysisBuckets;

static EMPTY_DEFERRED: std::sync::LazyLock<HashMap<CanonicalFqn, String>> =
    std::sync::LazyLock::new(HashMap::new);

/// `StaticAnalysisBuckets` implementation that carries the deferred-unique-ids
/// map produced by `defer_common` and returns it for run-cache lenient-dependency
/// matching. All SA classification methods report off/empty — this impl does not
/// rely on Fusion's static analysis infrastructure.
pub struct DeferredOnlyBuckets {
    deferred: HashMap<CanonicalFqn, String>,
}

impl DeferredOnlyBuckets {
    pub fn new(deferred: HashMap<CanonicalFqn, String>) -> Self {
        Self { deferred }
    }
}

impl StaticAnalysisBuckets for DeferredOnlyBuckets {
    fn global_static_analysis(&self) -> Option<StaticAnalysisKind> {
        Some(StaticAnalysisKind::Off)
    }

    fn deferred_unique_ids(&self) -> &HashMap<CanonicalFqn, String> {
        &self.deferred
    }

    fn in_off_closure(&self, _node_id: &str) -> bool {
        true
    }

    fn in_baseline_closure(&self, _node_id: &str) -> bool {
        false
    }

    fn in_dynamic_closure(&self, _node_id: &str) -> bool {
        false
    }

    fn dynamic_node(&self, _node_id: &str) -> Option<IntrospectionKind> {
        None
    }

    fn has_dynamic_closure(&self) -> bool {
        false
    }

    fn will_build_phased_task_graph(&self, _arg: &RunTasksArgs, _task_nodes: &Nodes) {}

    fn did_build_phased_task_graph(
        &self,
        _arg: &RunTasksArgs,
        _nodes_with_no_tasks: &BTreeSet<String>,
    ) {
    }
}

/// A `StaticAnalysisBuckets` that treats all nodes as not part of the static analysis process.
pub struct NoopStaticAnalysisBuckets;

impl StaticAnalysisBuckets for NoopStaticAnalysisBuckets {
    fn global_static_analysis(&self) -> Option<StaticAnalysisKind> {
        Some(StaticAnalysisKind::Off)
    }

    fn deferred_unique_ids(&self) -> &HashMap<CanonicalFqn, String> {
        &EMPTY_DEFERRED
    }

    fn in_off_closure(&self, _node_id: &str) -> bool {
        true
    }

    fn in_baseline_closure(&self, _node_id: &str) -> bool {
        false
    }

    fn in_dynamic_closure(&self, _node_id: &str) -> bool {
        false
    }

    fn dynamic_node(&self, _node_id: &str) -> Option<IntrospectionKind> {
        None
    }

    fn has_dynamic_closure(&self) -> bool {
        false
    }

    fn will_build_phased_task_graph(&self, _arg: &RunTasksArgs, _task_nodes: &Nodes) {}

    fn did_build_phased_task_graph(
        &self,
        _arg: &RunTasksArgs,
        _nodes_with_no_tasks: &BTreeSet<String>,
    ) {
    }
}

/// A `SchemaHydrator` that runs the defer pipeline (synthesize + load state +
/// defer_common + fixup) without performing schema hydration or static analysis.
/// This gives the SA binary the same ref-resolution behaviour as Fusion for
/// run-cache auto-deferral and explicit `--defer`/`--state` flags.
pub struct DeferSchemaHydrator {
    adapter: Arc<Adapter>,
    previous_state: Option<Arc<PreviousState>>,
    root_project_quoting: ResolvedQuoting,
}

#[async_trait::async_trait]
impl SchemaHydrator for DeferSchemaHydrator {
    async fn hydrate_schemas(
        self: Box<Self>,
        arg: &EvalArgs,
        schedule: &Schedule<String>,
        jinja_env: &JinjaEnv,
        resolved_state: &mut ResolverState,
        _schema_hydration_state: &mut SchemaHydrationState,
        _token: CancellationToken,
    ) -> FsResult<Box<dyn StaticAnalysisBuckets>> {
        let DeferSchemaHydrator {
            adapter,
            previous_state,
            root_project_quoting,
        } = *self;

        // Step 1: run-cache auto-deferral (synthesize PROD-schema nodes from service config).
        let run_cache_defer_nodes = match RunCacheProfileResolver::synthesize_defer_nodes(
            arg,
            resolved_state,
            jinja_env,
        ) {
            Ok(Some(nodes)) => Some(nodes),
            Ok(None) => None,
            Err(e) => {
                emit_warn_log_message(
                    ErrorCode::StateServiceWarn,
                    format!(
                        "dbt State auto-deferral setup failed: {e}; continuing without synthesized defer state"
                    ),
                    arg.io.status_reporter.as_ref(),
                );
                None
            }
        };

        // Step 2: load explicit --state/--defer manifest nodes (manifest wins over auto-defer).
        let mut defer_nodes = if arg.defer {
            let on_failure =
                if selectors_require_manifest(arg.select.as_ref(), arg.exclude.as_ref()) {
                    OnManifestLoadFailure::Warn
                } else {
                    OnManifestLoadFailure::Ignore
                };
            let (_, manifest_nodes) = dbt_defer::load_defer_state(
                &arg.io,
                None, // SA loads state via compilation.rs; no cloud downloader needed here
                arg.defer_state.as_ref(),
                previous_state,
                root_project_quoting,
                on_failure,
            )
            .await?;
            manifest_nodes.or(run_cache_defer_nodes)
        } else {
            run_cache_defer_nodes
        };

        // Step 3: remap frontier/selected refs to the deferred (prod) relations.
        // Collect deferred_unique_ids so the run-cache context can mark those
        // relations as lenient dependencies (matched by name, not semantic hash).
        let mut deferred_unique_ids = HashMap::new();
        if let Some(ref mut nodes) = defer_nodes {
            let update =
                dbt_defer::defer_common(arg, resolved_state, nodes, schedule, &adapter).await?;
            dbt_defer::rewrite_recorded_relation_calls_with_deferral(
                resolved_state,
                &update.relation_remap,
            );
            deferred_unique_ids = update.deferred_unique_ids;
        }

        // Step 4: patch NodeResolver for O(1) ref lookups and persist defer_nodes.
        if let Some(nodes) = defer_nodes {
            dbt_defer::set_defer_context_on_resolver(
                resolved_state,
                &schedule.sorted_nodes,
                &schedule.frontier_nodes,
            );
            resolved_state.defer_nodes = Some(nodes);
        }

        Ok(Box::new(DeferredOnlyBuckets::new(deferred_unique_ids)))
    }
}

/// Factory that produces `DeferSchemaHydrator` instances.
#[derive(Default)]
pub struct DeferSchemaHydratorFactory;

impl SchemaHydratorFactory for DeferSchemaHydratorFactory {
    fn create(
        &self,
        adapter: Arc<Adapter>,
        _execute_mode: dbt_schemas::schemas::profiles::Execute,
        _compilation_config: CompilationConfig,
        _cloud_config: Option<&ResolvedCloudConfig>,
        previous_state: Option<Arc<PreviousState>>,
        root_project_quoting: ResolvedQuoting,
        _schema_store: Arc<SchemaStore>,
        _sidecar_client: Option<Arc<dyn SidecarClient>>,
        _metricflow_server_client: Option<Arc<dyn MetricflowClient>>,
    ) -> Box<dyn SchemaHydrator> {
        Box::new(DeferSchemaHydrator {
            adapter,
            previous_state,
            root_project_quoting,
        })
    }
}

/// A `SchemaHydrator` that skips all hydration, deferral, and static analysis.
/// Kept for contexts where neither deferral nor schema hydration is needed.
pub struct NoopSchemaHydrator;

#[async_trait::async_trait]
impl SchemaHydrator for NoopSchemaHydrator {
    async fn hydrate_schemas(
        self: Box<Self>,
        _arg: &EvalArgs,
        _schedule: &Schedule<String>,
        _jinja_env: &JinjaEnv,
        _resolved_state: &mut ResolverState,
        _schema_hydration_state: &mut SchemaHydrationState,
        _token: CancellationToken,
    ) -> FsResult<Box<dyn StaticAnalysisBuckets>> {
        Ok(Box::new(NoopStaticAnalysisBuckets))
    }
}

/// A `SchemaHydratorFactory` that produces `NoopSchemaHydrator` instances.
#[derive(Default)]
pub struct NoopSchemaHydratorFactory;

impl SchemaHydratorFactory for NoopSchemaHydratorFactory {
    fn create(
        &self,
        _adapter: Arc<Adapter>,
        _execute_mode: dbt_schemas::schemas::profiles::Execute,
        _compilation_config: CompilationConfig,
        _cloud_config: Option<&ResolvedCloudConfig>,
        _previous_state: Option<Arc<PreviousState>>,
        _root_project_quoting: ResolvedQuoting,
        _schema_store: Arc<SchemaStore>,
        _sidecar_client: Option<Arc<dyn SidecarClient>>,
        _metricflow_server_client: Option<Arc<dyn MetricflowClient>>,
    ) -> Box<dyn SchemaHydrator> {
        Box::new(NoopSchemaHydrator)
    }
}
