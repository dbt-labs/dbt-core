use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use dbt_adapter::adapter::DefaultAdapterFactory;
use dbt_adapter::sql_types::DefaultTypeOpsFactory;
use dbt_common::FsError;
use dbt_common::collections::DashMap;
use dbt_dag::schedule::Schedule;
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_jinja_utils::listener::{
    DefaultRenderingEventListenerFactory, RenderingEventListenerFactory,
};
use dbt_login::{LicenseFetcher, NoOpLicenseFetcher};
use dbt_schemas::state::ResolverState;
use dbt_tasks_core::context::ExtendedCtx;
use dbt_tasks_core::context_factory::TaskRunnerCtxFactory;
use dbt_tasks_core::{PreTaskRunData, RunTasksArgs};
use dbt_tasks_sa::schema_hydrator::DefaultSchemaHydratorFactory;
use dbt_tasks_sa::task::DefaultTasksForNodeFactory;
use dbt_tasks_sa::task_runner_hooks::DefaultTaskRunnerHooksFactory;

use crate::adapter::AdapterFeature;
use crate::antlr_parser::AntlrParserFeature;
use crate::cli::{CliFeature, CliFeatureBuilder, DefaultCliExtensionHooks};
use crate::feature_stack::{FeatureStack, InstrumentationFeature};
use crate::index::{IndexFeature, IndexHooks};
use crate::loader::LoaderFeature;
use crate::metricflow::MetricflowFeature;
use crate::resolver::ResolverFeature;
use crate::sidecar::SidecarFeature;
use crate::task_runner::TaskRunnerFeature;
use crate::tracing::TracingFeature;

struct DefaultTaskRunnerCtxFactory {
    rendering_listener_factory: Arc<dyn RenderingEventListenerFactory>,
}
impl DefaultTaskRunnerCtxFactory {
    fn new(rendering_listener_factory: Arc<dyn RenderingEventListenerFactory>) -> Self {
        Self {
            rendering_listener_factory,
        }
    }
}

impl TaskRunnerCtxFactory for DefaultTaskRunnerCtxFactory {
    fn rendering_listener_factory(&self) -> Arc<dyn RenderingEventListenerFactory> {
        Arc::clone(&self.rendering_listener_factory)
    }

    fn build_node_hashes<'a>(
        &'a self,
        _arg: &'a RunTasksArgs,
        _schedule: &'a Schedule<String>,
        _worker_id: &'a str,
        _resolver_state: &'a ResolverState,
        _env: &'a JinjaEnv,
        _freshness_results: Option<&'a dyn PreTaskRunData>,
        _extended_ctx: &'a dyn ExtendedCtx,
    ) -> Pin<Box<dyn Future<Output = Result<DashMap<String, String>, Box<FsError>>> + Send + 'a>>
    {
        Box::pin(async move { Ok(DashMap::default()) })
    }
}

struct NoOpIndexHooks;

#[async_trait]
impl IndexHooks for NoOpIndexHooks {}

pub struct FeatureStackBuilder {
    send_anonymous_usage_stats: bool,
    tracing: TracingFeature,
    adapter: AdapterFeature,
    antlr_parser: AntlrParserFeature,
    sidecar: SidecarFeature,
    cli: CliFeature,
    task_runner: TaskRunnerFeature,
    resolver: ResolverFeature,
    loader: LoaderFeature,
    license_fetcher: Arc<dyn LicenseFetcher>,
    dbt_distribution: &'static str,
}

impl FeatureStackBuilder {
    pub fn new(tracing: TracingFeature) -> Self {
        let adapter = {
            let type_ops_factory = Arc::new(DefaultTypeOpsFactory);
            let adapter_factory = Arc::new(DefaultAdapterFactory);

            AdapterFeature {
                type_ops_factory,
                adapter_factory,
            }
        };
        let task_runner = {
            let rendering_listener_factory: Arc<dyn RenderingEventListenerFactory> =
                Arc::new(DefaultRenderingEventListenerFactory::default());

            let task_runner_ctx_factory = Arc::new(DefaultTaskRunnerCtxFactory::new(Arc::clone(
                &rendering_listener_factory,
            ))) as Arc<dyn TaskRunnerCtxFactory>;

            TaskRunnerFeature {
                schema_hydrator_factory: Arc::new(DefaultSchemaHydratorFactory),
                tasks_for_node_factory: Arc::new(DefaultTasksForNodeFactory),
                compare_task_graph_builder: None,
                rendering_listener_factory,
                task_runner_ctx_factory,
                hooks_factory: Arc::new(DefaultTaskRunnerHooksFactory),
            }
        };
        let cli = {
            let hooks = Box::new(DefaultCliExtensionHooks);
            CliFeatureBuilder::with_hooks(hooks).build()
        };
        Self {
            send_anonymous_usage_stats: false,
            tracing,
            adapter,
            antlr_parser: Default::default(),
            sidecar: SidecarFeature::default(),
            cli,
            task_runner,
            resolver: ResolverFeature::default(),
            loader: LoaderFeature::default(),
            license_fetcher: Arc::new(NoOpLicenseFetcher),
            dbt_distribution: "unknown-oss",
        }
    }

    pub fn license_fetcher(mut self, fetcher: Arc<dyn LicenseFetcher>) -> Self {
        self.license_fetcher = fetcher;
        self
    }

    pub fn send_anonymous_usage_stats(mut self, enabled: bool) -> Self {
        self.send_anonymous_usage_stats = enabled;
        self
    }

    pub fn dbt_distribution(mut self, dbt_distribution: &'static str) -> Self {
        self.dbt_distribution = dbt_distribution;
        self
    }

    pub fn adapter(mut self, feature: AdapterFeature) -> Self {
        self.adapter = feature;
        self
    }

    pub fn antlr_parser(mut self, feature: AntlrParserFeature) -> Self {
        self.antlr_parser = feature;
        self
    }

    pub fn cli(mut self, feature: CliFeature) -> Self {
        self.cli = feature;
        self
    }

    pub fn task_runner(mut self, feature: TaskRunnerFeature) -> Self {
        self.task_runner = feature;
        self
    }

    pub fn resolver(mut self, feature: ResolverFeature) -> Self {
        self.resolver = feature;
        self
    }

    pub fn loader(mut self, feature: LoaderFeature) -> Self {
        self.loader = feature;
        self
    }

    pub fn build(self) -> Box<FeatureStack> {
        let instrumentation = InstrumentationFeature {
            event_emitter: vortex_events::fusion_sa_event_emitter(
                self.send_anonymous_usage_stats,
                self.dbt_distribution,
            ),
        };
        let index = IndexFeature {
            hooks: Box::new(NoOpIndexHooks),
            providers_factory: crate::index::default_providers_factory,
        };
        let stack = FeatureStack {
            instrumentation,
            cli: self.cli,
            index,
            tracing: self.tracing,
            adapter: self.adapter,
            antlr_parser: self.antlr_parser,
            sidecar: self.sidecar,
            metricflow: MetricflowFeature::default(),
            task_runner: self.task_runner,
            resolver: self.resolver,
            loader: self.loader,
            license_fetcher: self.license_fetcher,
            version_check_enabled: false,
        };
        Box::new(stack)
    }
}
