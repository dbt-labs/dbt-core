use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;

use dbt_adapter::Adapter;
use dbt_adapter::relation::create_relation_from_node;
use dbt_adapter_core::AdapterType;
use dbt_common::FsError;
use dbt_common::collections::DashMap;
use dbt_dag::schedule::Schedule;
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_jinja_utils::listener::RenderingEventListenerFactory;
use dbt_run_cache::view_traversal::ViewDefinitionTraverser;
use dbt_schema_store::{DataStoreTrait, SchemaStoreTrait};
use dbt_schemas::schemas::profiles::Execute;
use dbt_schemas::state::ResolverState;
use petgraph::graph::DiGraph;

use crate::CompiledSqlCache;
use crate::PreTaskRunData;
use crate::RunTasksArgs;
use crate::context::{ExtendedCtx, RunCacheCtx, TaskRunnerCtx, TaskRunnerCtxInner};
use crate::run_cache_lifecycle::RunCacheLifecycle;
use crate::span_manager::SpanManager;
use crate::static_analysis_buckets::StaticAnalysisBuckets;
use crate::task::Task;
use crate::task_spans::populate_span_manager;
use crate::test_aggregation::GenericTestRelationships;

/// Abstract [TaskRunnerCtx] factory.
pub trait TaskRunnerCtxFactory: Send + Sync + 'static {
    fn rendering_listener_factory(&self) -> Arc<dyn RenderingEventListenerFactory>;

    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    fn build(
        self: Arc<Self>,
        run_task_args: Arc<RunTasksArgs>,
        worker_id: String,
        resolver_state: Arc<ResolverState>,
        extended_ctx_factory: Box<dyn ExtendedTaskRunnerCtxFactory>,
        generic_test_relationships: GenericTestRelationships,
        graph: &DiGraph<Arc<dyn Task>, ()>,
        schema_store: Arc<dyn SchemaStoreTrait>,
        data_store: Arc<dyn DataStoreTrait>,
        compiled_sql_cache: Arc<dyn CompiledSqlCache>,
        mut base_context: BTreeMap<String, minijinja::Value>,
        schedule: Schedule<String>,
        jinja_env: Arc<JinjaEnv>,
        freshness_results: Option<Box<dyn PreTaskRunData>>,
        static_analysis_buckets: Arc<dyn StaticAnalysisBuckets>,
        adapter: Arc<Adapter>,
    ) -> Pin<Box<dyn Future<Output = Result<TaskRunnerCtx, Box<FsError>>> + Send>> {
        let rendering_listener_factory = self.rendering_listener_factory();
        let span_manager = Arc::new({
            let sm = SpanManager::new_empty();
            let _ = populate_span_manager(
                &sm,
                graph,
                run_task_args.io.in_dir.as_ref(),
                run_task_args.io.out_dir.as_ref(),
                &schedule.selected_nodes,
            );
            sm
        });
        let adhoc_runner = extended_ctx_factory.adhoc_runner(
            Arc::clone(&jinja_env),
            resolver_state.adapter_type,
            Arc::clone(&run_task_args),
            resolver_state.root_project_name.clone(),
        );
        Box::pin(async move {
            let execute = Execute::from_compute_flag(run_task_args.local_execution_backend);
            let run_cache_lifecycle = RunCacheLifecycle::initialize(
                run_task_args.as_ref(),
                execute,
                resolver_state.adapter_type,
            )
            .await;

            let extended_ctx = extended_ctx_factory
                .build(run_cache_lifecycle.is_requested())
                .await?;

            let node_hashes = self
                .build_node_hashes(
                    run_task_args.as_ref(),
                    &schedule,
                    &worker_id,
                    resolver_state.as_ref(),
                    jinja_env.as_ref(),
                    freshness_results.as_deref(),
                    extended_ctx.as_ref(),
                )
                .await?;

            base_context.insert(
                "selected_resources".to_string(),
                minijinja::Value::from_iter(schedule.selected_nodes.iter().cloned()),
            );

            let schema_cache = schema_store as Arc<dyn SchemaStoreTrait>;

            let run_cache_deferred_fqns = static_analysis_buckets
                .deferred_unique_ids()
                .values()
                .filter_map(|unique_id| {
                    let node = resolver_state.get_defer_node_by_id(unique_id)?;
                    let relation =
                        create_relation_from_node(resolver_state.adapter_type, node, None).ok()?;
                    Some(relation.semantic_fqn())
                })
                .collect();

            let RunCacheLifecycle {
                service: run_cache_service,
                metadata: run_cache_metadata,
            } = run_cache_lifecycle;

            let run_cache_ctx = RunCacheCtx {
                run_cache_metadata,
                run_cache_dev_cloned_nodes: DashMap::default(),
                run_cache_deferred_fqns,
                run_cache_service_requested: run_cache_service.requested,
                run_cache_service_config: run_cache_service.config,
                run_cache_service_client: run_cache_service.client,
                view_traverser: adapter.metadata_adapter().map(|metadata_adapter| {
                    Arc::new(ViewDefinitionTraverser::new(
                        Arc::from(metadata_adapter),
                        Arc::clone(adapter.engine().type_ops()),
                    ))
                }),
            };

            Ok(TaskRunnerCtx {
                inner: Arc::new(TaskRunnerCtxInner::new(
                    run_task_args,
                    worker_id,
                    schedule,
                    base_context,
                    node_hashes,
                    extended_ctx,
                    compiled_sql_cache,
                    adhoc_runner,
                    &resolver_state,
                    generic_test_relationships,
                    span_manager,
                    execute,
                    run_cache_ctx,
                )),
                schema_cache,
                data_store,
                resolver_state,
                rendering_listener_factory,
                env: jinja_env,
                thread_id: 0,
            })
        })
    }

    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    fn build_node_hashes<'a>(
        &'a self,
        arg: &'a RunTasksArgs,
        schedule: &'a Schedule<String>,
        worker_id: &'a str,
        resolver_state: &'a ResolverState,
        env: &'a JinjaEnv,
        freshness_results: Option<&'a dyn PreTaskRunData>,
        extended_ctx: &'a dyn ExtendedCtx,
    ) -> Pin<Box<dyn Future<Output = Result<DashMap<String, String>, Box<FsError>>> + Send + 'a>>;
}

/// Abstract factory for building the extended context, which is a component of [TaskRunnerCtx].
pub trait ExtendedTaskRunnerCtxFactory: Send + Sync {
    fn adhoc_runner(
        &self,
        env: Arc<JinjaEnv>,
        adapter_type: AdapterType,
        args: Arc<RunTasksArgs>,
        root_project_name: String,
    ) -> Arc<dyn crate::AdhocRunner>;

    #[allow(clippy::type_complexity)]
    fn build(
        self: Box<Self>,
        run_cache_enabled: bool,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn ExtendedCtx>, Box<FsError>>> + Send>>;
}
