use std::sync::Arc;

use dbt_compilation::schema_hydration::SchemaHydratorFactory;
use dbt_jinja_utils::listener::RenderingEventListenerFactory;
use dbt_tasks_core::context_factory::TaskRunnerCtxFactory;
use dbt_tasks_core::task::TasksForNodeFactory;
use dbt_tasks_sa::graph::CompareTaskGraphBuilder;

pub struct TaskRunnerFeature {
    pub schema_hydrator_factory: Arc<dyn SchemaHydratorFactory>,
    pub tasks_for_node_factory: Arc<dyn TasksForNodeFactory>,
    pub compare_task_graph_builder: Option<Arc<dyn CompareTaskGraphBuilder>>,
    pub rendering_listener_factory: Arc<dyn RenderingEventListenerFactory>,
    pub task_runner_ctx_factory: Arc<dyn TaskRunnerCtxFactory>,
}
