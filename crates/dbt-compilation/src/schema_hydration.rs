use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use dbt_adapter::Adapter;
use dbt_adapter::engine::SidecarClient;
use dbt_cloud_config::ResolvedCloudConfig;
use dbt_common::FsError;
use dbt_common::FsResult;
use dbt_common::cancellation::CancellationToken;
use dbt_common::io_args::EvalArgs;
use dbt_common::io_args::IoArgs;
use dbt_common::io_args::StaticAnalysisKind;
use dbt_dag::schedule::Schedule;
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_schema_store::store::SchemaStore;
use dbt_schemas::schemas::PreviousState;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::profiles::Execute;
use dbt_schemas::state::ResolverState;
use dbt_tasks_core::metricflow::MetricflowClient;
use dbt_tasks_core::precompile::StaticAnalysisBuckets;

use crate::config::CompilationConfig;

#[derive(Clone)]
pub struct SchemaHydrationArgs<'a> {
    pub io: &'a IoArgs,
    pub global_static_analysis: Option<StaticAnalysisKind>,
    pub execute_mode: Execute,
    pub scope: SchemaHydrationScope,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchemaHydrationScope {
    FrontierOnly,
    Full,
}

#[derive(Clone)]
pub struct SchemaHydrationDownloadWarning {
    pub err: Arc<FsError>,
    pub unique_id: String,
}

#[derive(Clone, Default)]
pub struct SchemaHydrationState {
    pub fetched_schema_fqns: HashSet<String>,
    pub download_warnings_by_fqn: HashMap<String, SchemaHydrationDownloadWarning>,
}

#[async_trait::async_trait]
pub trait SchemaHydrator: Send + Sync {
    async fn hydrate_schemas(
        self: Box<Self>,
        arg: &EvalArgs,
        schedule: &Schedule<String>,
        jinja_env: &JinjaEnv,
        resolved_state: &mut ResolverState,
        schema_hydration_state: &mut SchemaHydrationState,
        token: CancellationToken,
    ) -> FsResult<Box<dyn StaticAnalysisBuckets>>;
}

pub trait SchemaHydratorFactory: Send + Sync {
    fn create(
        &self,
        adapter: Arc<Adapter>,
        execute_mode: Execute,
        compilation_config: CompilationConfig,
        cloud_config: Option<&ResolvedCloudConfig>,
        previous_state: Option<Arc<PreviousState>>,
        root_project_quoting: ResolvedQuoting,
        schema_store: Arc<SchemaStore>,
        sidecar_client: Option<Arc<dyn SidecarClient>>,
        metricflow_server_client: Option<Arc<dyn MetricflowClient>>,
    ) -> Box<dyn SchemaHydrator>;
}
