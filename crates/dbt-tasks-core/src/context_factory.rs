use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;

use dbt_adapter_core::AdapterType;
use dbt_common::FsError;
use dbt_common::collections::DashMap;
use dbt_dag::schedule::Schedule;
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_jinja_utils::listener::RenderingEventListenerFactory;
use dbt_schema_store::{DataStoreTrait, SchemaStoreTrait};
use dbt_schemas::schemas::profiles::Execute;
use dbt_schemas::state::ResolverState;
use petgraph::graph::DiGraph;

use crate::PreTaskRunData;
use crate::RunTasksArgs;
use crate::context::{ExtendedCtx, TaskRunnerCtx, TaskRunnerCtxInner};
use crate::span_manager::SpanManager;
use crate::task::Task;
use crate::task_spans::populate_span_manager;
use crate::test_aggregation::GenericTestRelationships;

/// Abstract [TaskRunnerCtx] factory.
pub trait TaskRunnerCtxFactory: Send + Sync + 'static {
    fn rendering_listener_factory(&self) -> Arc<dyn RenderingEventListenerFactory>;
    fn compiled_sql_cache(&self) -> Arc<dyn crate::CompiledSqlCache>;

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
        mut base_context: BTreeMap<String, minijinja::Value>,
        schedule: Schedule<String>,
        jinja_env: Arc<JinjaEnv>,
        freshness_results: Option<Box<dyn PreTaskRunData>>,
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
        let compiled_sql_cache = self.compiled_sql_cache();
        let adhoc_runner = extended_ctx_factory.adhoc_runner(
            Arc::clone(&jinja_env),
            resolver_state.adapter_type,
            Arc::clone(&run_task_args),
            resolver_state.root_project_name.clone(),
        );
        Box::pin(async move {
            let extended_ctx = extended_ctx_factory.build().await?;

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
            let data_store = data_store as Arc<dyn DataStoreTrait>;

            let execute = Execute::from_compute_flag(run_task_args.local_execution_backend);

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
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn ExtendedCtx>, Box<FsError>>> + Send>>;
}
