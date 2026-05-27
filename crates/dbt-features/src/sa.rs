use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use dbt_common::FsResult;
use dbt_common::cancellation::{CancellationToken, CancellationTokenSource};
use dbt_common::fail_fast::FailFast;
use dbt_common::io_args::EvalArgs;
use dbt_tasks_core::context::TaskRunnerCtx;
use minijinja::Value as MinijinjaValue;
use uuid::Uuid;

use crate::adapter::AdapterFeature;
use crate::antlr_parser::AntlrParserFeature;
use crate::cli_extension::{CliExtensionFeature, CliExtensionHooks};
use crate::feature_stack::{FeatureStack, InstrumentationFeature};
use crate::index::IndexFeature;
use crate::index::IndexHooks;
use crate::metricflow::MetricflowFeature;
use crate::sidecar::SidecarFeature;
use crate::task_runner::TaskRunnerFeature;
use crate::tracing::TracingFeature;

struct NoOpExtensionHooks;

#[async_trait]
impl CliExtensionHooks for NoOpExtensionHooks {
    fn will_validate_compilation_cli_args(
        &self,
        _cli: &dbt_clap_core::Cli,
        _eval_arg: &mut Cow<EvalArgs>,
        _dbt_state: &Arc<dbt_schemas::state::DbtState>,
        _config: &dbt_compilation::config::CompilationConfig,
    ) -> FsResult<()> {
        Ok(())
    }

    async fn will_init_project(
        &self,
        _invocation_id: Uuid,
        _cli: &dbt_clap_core::Cli,
        _init_args: &dbt_clap_core::InitArgs,
    ) -> FsResult<()> {
        Ok(())
    }

    async fn will_execute(
        &self,
        _cli: &dbt_clap_core::Cli,
        _eval_arg: &EvalArgs,
        _feature_stack: &Arc<FeatureStack>,
    ) -> FsResult<()> {
        Ok(())
    }

    async fn did_resolve_project(
        &self,
        _arg: &EvalArgs,
        _resolved_state: &dbt_schemas::state::ResolverState,
    ) -> FsResult<()> {
        Ok(())
    }

    fn will_run_tasks(
        &self,
        _cli: &dbt_clap_core::Cli,
        _arg: &EvalArgs,
        _resolved_state: &dbt_schemas::state::ResolverState,
        _token: &CancellationToken,
    ) -> FsResult<()> {
        Ok(())
    }

    async fn did_schedule_and_run_tasks(
        &self,
        _arg: &EvalArgs,
        _cli: &dbt_clap_core::Cli,
        _previous_state: Option<&dbt_schemas::schemas::PreviousState>,
        _run_task_results: &dbt_tasks_core::RunTaskResults,
        _resolved_state: &dbt_schemas::state::ResolverState,
        _token: &CancellationToken,
    ) -> FsResult<()> {
        Ok(())
    }

    async fn did_emit_selected_compile_output(
        &self,
        _arg: &EvalArgs,
        _resolved_state: &dbt_schemas::state::ResolverState,
        _jinja_env: &Arc<dbt_jinja_utils::jinja_environment::JinjaEnv>,
        _task_runner_ctx: Option<&TaskRunnerCtx>,
        _schema_store: &Arc<dyn dbt_schema_store::SchemaStoreTrait>,
        _data_store: &Arc<dyn dbt_schema_store::DataStoreTrait>,
        _map_compiled_sql: &HashMap<String, Option<String>>,
        _feature_stack: &Arc<FeatureStack>,
        _token: &CancellationToken,
    ) -> FsResult<()> {
        Ok(())
    }

    async fn did_compile(
        &self,
        _arg: &EvalArgs,
        _cli: &dbt_clap_core::Cli,
        _resolved_state: &dbt_schemas::state::ResolverState,
        _schedule: &dbt_dag::schedule::Schedule<String>,
        _token: &CancellationToken,
    ) -> FsResult<()> {
        Ok(())
    }

    async fn did_pre_run(
        &self,
        _arg: &EvalArgs,
        _cli: &dbt_clap_core::Cli,
        _jinja_env: Cow<'_, dbt_jinja_utils::jinja_environment::JinjaEnv>,
        _augmented_resolved_state: &dbt_schemas::state::ResolverState,
        _schedule: &dbt_dag::schedule::Schedule<String>,
        _adapter: Arc<dbt_adapter::Adapter>,
        _base_context: &BTreeMap<String, MinijinjaValue>,
        _token: &CancellationToken,
    ) -> FsResult<Option<Box<dyn dbt_tasks_core::PreTaskRunData>>> {
        Ok(None)
    }

    async fn did_handle_defer(
        &self,
        _arg: &EvalArgs,
        _cli: &dbt_clap_core::Cli,
        _jinja_env: Cow<'_, dbt_jinja_utils::jinja_environment::JinjaEnv>,
        _augmented_resolved_state: &dbt_schemas::state::ResolverState,
        _schedule: &dbt_dag::schedule::Schedule<String>,
        _metricflow_client: Option<Arc<dyn crate::metricflow::MetricflowClient>>,
        _token: &CancellationToken,
    ) -> FsResult<()> {
        Ok(())
    }
}

struct NoOpIndexHooks;

#[async_trait]
impl IndexHooks for NoOpIndexHooks {}

pub struct SourceAvailableFeatureStackBuilder {
    send_anonymous_usage_stats: bool,
    tracing: TracingFeature,
    adapter: AdapterFeature,
    antlr_parser: AntlrParserFeature,
    task_runner: TaskRunnerFeature,
}

impl SourceAvailableFeatureStackBuilder {
    pub fn new(
        tracing: TracingFeature,
        adapter: AdapterFeature,
        task_runner: TaskRunnerFeature,
    ) -> Self {
        Self {
            send_anonymous_usage_stats: false,
            tracing,
            adapter,
            antlr_parser: Default::default(),
            task_runner,
        }
    }

    pub fn send_anonymous_usage_stats(mut self, enabled: bool) -> Self {
        self.send_anonymous_usage_stats = enabled;
        self
    }

    pub fn antlr_parser(mut self, feature: AntlrParserFeature) -> Self {
        self.antlr_parser = feature;
        self
    }

    pub fn task_runner(mut self, feature: TaskRunnerFeature) -> Self {
        self.task_runner = feature;
        self
    }

    pub fn build(self) -> Box<FeatureStack> {
        let instrumentation = InstrumentationFeature {
            event_emitter: vortex_events::fusion_sa_event_emitter(self.send_anonymous_usage_stats),
        };
        let cli_extension = CliExtensionFeature {
            hooks: Box::new(NoOpExtensionHooks),
        };
        let index = IndexFeature {
            hooks: Box::new(NoOpIndexHooks),
        };
        let stack = FeatureStack {
            instrumentation,
            cli_extension,
            index,
            tracing: self.tracing,
            adapter: self.adapter,
            antlr_parser: self.antlr_parser,
            sidecar: SidecarFeature::default(),
            metricflow: MetricflowFeature::default(),
            task_runner: self.task_runner,
            cancellation_token_source: CancellationTokenSource::new(),
            fail_fast: FailFast::new(),
        };
        Box::new(stack)
    }
}
