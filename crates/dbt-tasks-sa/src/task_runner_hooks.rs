use std::sync::Arc;

use async_trait::async_trait;
use dbt_adapter::Adapter;
use dbt_common::FsError;
use dbt_common::FsResult;
use dbt_common::cancellation::CancellationToken;
use dbt_dag::schedule::Schedule;
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_schema_store::DataStoreTrait;
use dbt_schema_store::store::SchemaStore;
use dbt_schemas::schemas::PreviousState;
use dbt_schemas::schemas::ResolvedCloudConfig;
use dbt_schemas::state::ResolverState;
use dbt_tasks_core::CompiledSqlCache;
use dbt_tasks_core::Preview;
use dbt_tasks_core::RunTasksArgs;
use dbt_tasks_core::ShowableResults;
use dbt_tasks_core::StoreableResults;
use dbt_tasks_core::context::TaskRunnerCtx;
use dbt_tasks_core::context_factory::ExtendedTaskRunnerCtxFactory;
use dbt_tasks_core::metricflow::MetricflowClient;
use dbt_tasks_core::precompile::StaticAnalysisBuckets;
use dbt_tasks_core::task::Task;
pub use dbt_tasks_core::task_runner_hooks::{TaskRunnerHooks, TaskRunnerHooksFactory};
use petgraph::Graph;

pub struct DefaultTaskRunnerHooksFactory;

impl TaskRunnerHooksFactory for DefaultTaskRunnerHooksFactory {
    fn create(
        &self,
        _cloud_config: Option<ResolvedCloudConfig>,
        _previous_state: Option<Arc<PreviousState>>,
        _adapter: Arc<Adapter>,
        _resolved_state: Arc<ResolverState>,
        _jinja_env: Arc<JinjaEnv>,
        _schema_store: Arc<SchemaStore>,
        _data_store: Arc<dyn DataStoreTrait>,
        _compiled_sql_cache: Arc<dyn CompiledSqlCache>,
        _metricflow_server_client: Option<Arc<dyn MetricflowClient>>,
        _static_analysis_buckets: Arc<dyn StaticAnalysisBuckets>,
    ) -> Box<dyn TaskRunnerHooks> {
        Box::new(DefaultTaskRunnerHooks)
    }
}

struct DefaultTaskRunnerHooks;

#[async_trait]
impl TaskRunnerHooks for DefaultTaskRunnerHooks {
    fn should_persist_seed_data(&self, _run_task_args: &RunTasksArgs) -> bool {
        false
    }

    async fn create_extended_ctx_factory(
        &self,
        _run_task_args: &Arc<RunTasksArgs>,
    ) -> FsResult<Box<dyn ExtendedTaskRunnerCtxFactory>> {
        todo!("create_extended_ctx_factory")
    }

    fn show_taskgraph(&self, _graph: &Graph<Arc<dyn Task>, ()>) {}

    fn will_run(&self, _run_task_args: &RunTasksArgs, _schedule: &Schedule<String>) {}

    async fn did_register_schemas(
        &self,
        _registered_schemas: bool,
        _run_task_args: &RunTasksArgs,
        _schedule: &Schedule<String>,
        _ctx: &mut TaskRunnerCtx,
    ) -> Result<(), Box<FsError>> {
        Ok(())
    }

    fn collect_showables(&self, _ctx: &mut TaskRunnerCtx) -> Vec<Box<dyn ShowableResults>> {
        vec![]
    }

    fn collect_preview(&self, _ctx: &mut TaskRunnerCtx) -> Option<Result<Preview, String>> {
        None
    }

    fn collect_storeables(
        &mut self,
        _run_task_args: &RunTasksArgs,
        _ctx: &mut TaskRunnerCtx,
    ) -> Vec<Box<dyn StoreableResults>> {
        vec![]
    }

    async fn did_collect_all_run_task_results(
        &self,
        _run_task_args: &RunTasksArgs,
        _ctx: &mut TaskRunnerCtx,
    ) {
    }

    async fn will_visit_taskgraph(
        &self,
        _run_task_args: &Arc<RunTasksArgs>,
        _schedule: &Schedule<String>,
        _has_dynamic_closure: bool,
        _on_run_start_sqls: &[String],
        _graph: &Graph<Arc<dyn Task>, ()>,
        _ctx: &mut TaskRunnerCtx,
        _token: &CancellationToken,
    ) -> FsResult<()> {
        todo!("will_visit_taskgraph")
    }

    async fn did_visit_taskgraph(
        &self,
        _run_task_args: &Arc<RunTasksArgs>,
        _schedule: &Schedule<String>,
        _graph: &Graph<Arc<dyn Task>, ()>,
        _ctx: &mut TaskRunnerCtx,
        _token: &CancellationToken,
    ) -> FsResult<()> {
        todo!("did_visit_taskgraph")
    }
}
