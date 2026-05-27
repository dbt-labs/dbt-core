use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;

use dbt_common::FsError;
use dbt_dag::schedule::Schedule;
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_schema_store::{DataStoreTrait, SchemaStoreTrait};
use dbt_schemas::state::ResolverState;
use petgraph::graph::DiGraph;

use crate::PreTaskRunData;
use crate::RunTasksArgs;
use crate::context::{ExtendedCtx, TaskRunnerCtx};
use crate::task::Task;
use crate::test_aggregation::GenericTestRelationships;

/// Abstract [TaskRunnerCtx] factory.
pub trait TaskRunnerCtxFactory: Send + Sync {
    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    fn build(
        &self,
        run_task_args: Arc<RunTasksArgs>,
        worker_id: String,
        resolver_state: Arc<ResolverState>,
        extended_ctx_factory: Box<dyn ExtendedTaskRunnerCtxFactory>,
        generic_test_relationships: GenericTestRelationships,
        graph: &DiGraph<Arc<dyn Task>, ()>,
        schema_store: Arc<dyn SchemaStoreTrait>,
        data_store: Arc<dyn DataStoreTrait>,
        base_context: BTreeMap<String, minijinja::Value>,
        schedule: Schedule<String>,
        jinja_env: Arc<JinjaEnv>,
        freshness_results: Option<Box<dyn PreTaskRunData>>,
    ) -> Pin<Box<dyn Future<Output = Result<TaskRunnerCtx, Box<FsError>>> + Send>>;
}

/// Abstract factory for building the extended context, which is a component of [TaskRunnerCtx].
pub trait ExtendedTaskRunnerCtxFactory: Send + Sync {
    #[allow(clippy::type_complexity)]
    fn build(
        self: Box<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn ExtendedCtx>, Box<FsError>>> + Send>>;
}
