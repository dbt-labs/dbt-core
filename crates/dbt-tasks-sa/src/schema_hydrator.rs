use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use dbt_adapter::Adapter;
use dbt_adapter::engine::SidecarClient;
use dbt_common::FsResult;
use dbt_common::cancellation::CancellationToken;
use dbt_common::io_args::{EvalArgs, StaticAnalysisKind};
use dbt_compilation::config::CompilationConfig;
use dbt_compilation::schema_hydration::{
    SchemaHydrationState, SchemaHydrator, SchemaHydratorFactory,
};
use dbt_dag::schedule::Schedule;
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_schema_store::CanonicalFqn;
use dbt_schema_store::store::SchemaStore;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::profiles::Execute;
use dbt_schemas::schemas::{IntrospectionKind, Nodes, PreviousState, ResolvedCloudConfig};
use dbt_schemas::state::ResolverState;
use dbt_tasks_core::RunTasksArgs;
use dbt_tasks_core::metricflow::MetricflowClient;
use dbt_tasks_core::precompile::StaticAnalysisBuckets;

/// A `StaticAnalysisBuckets` that treats all nodes as not part of the static analysis process.
pub struct NoopStaticAnalysisBuckets;

static EMPTY_DEFERRED: std::sync::LazyLock<HashMap<CanonicalFqn, String>> =
    std::sync::LazyLock::new(HashMap::new);

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

/// A `SchemaHydrator` that skips all hydration and static analysis.
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
        _execute_mode: Execute,
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
