use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use dbt_login::{LicenseFetcher, NoOpLicenseFetcher};

use async_trait::async_trait;
use dbt_adapter::adapter::{AdapterFactory, backend_of};
use dbt_adapter::auth::Auth;
use dbt_adapter::cache::RelationCache;
use dbt_adapter::config::AdapterConfig;
use dbt_adapter::engine::XdbcEngine;
use dbt_adapter::engine::query_comment::QueryCommentConfig;
use dbt_adapter::query_cache::QueryCache;
use dbt_adapter::sql_types::{DefaultTypeOps, TypeOps, TypeOpsFactory};
use dbt_adapter::stmt_splitter::{DefaultStmtSplitter, StmtSplitter};
use dbt_adapter::{Adapter, AdapterEngine, AdapterImpl};
use dbt_adapter_core::AdapterType;
use dbt_auth::auth_for_backend;
use dbt_cloud_config::ResolvedCloudConfig;
use dbt_common::cancellation::{CancellationToken, CancellationTokenSource};
use dbt_common::collections::DashMap;
use dbt_common::fail_fast::FailFast;
use dbt_common::io_args::ReplayMode;
use dbt_common::{FsError, FsResult};
use dbt_dag::schedule::Schedule;
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_jinja_utils::listener::{
    DefaultRenderingEventListenerFactory, RenderingEventListenerFactory,
};
use dbt_schema_store::SchemaStoreTrait;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::project::QueryComment;
use dbt_schemas::schemas::relations::base::BaseRelation;
use dbt_schemas::schemas::{
    InternalDbtNodeAttributes, ResolvedCloudConfig as SchemasResolvedCloudConfig,
};
use dbt_schemas::state::ResolverState;
use dbt_tasks_core::context::ExtendedCtx;
use dbt_tasks_core::context_factory::TaskRunnerCtxFactory;
use dbt_tasks_core::{PreTaskRunData, RunTasksArgs};
use dbt_tasks_sa::schema_hydrator::DefaultSchemaHydratorFactory;
use dbt_tasks_sa::task::DefaultTasksForNodeFactory;
use dbt_tasks_sa::task_runner_hooks::DefaultTaskRunnerHooksFactory;
use minijinja::Value as MinijinjaValue;

use crate::adapter::AdapterFeature;
use crate::antlr_parser::AntlrParserFeature;
use crate::cli_extension::{
    CliExtensionFeature, CliExtensionFeatureBuilder, DefaultCliExtensionHooks,
};
use crate::feature_stack::{FeatureStack, InstrumentationFeature};
use crate::index::{IndexFeature, IndexHooks};
use crate::loader::LoaderFeature;
use crate::metricflow::MetricflowFeature;
use crate::resolver::ResolverFeature;
use crate::sidecar::SidecarFeature;
use crate::task_runner::TaskRunnerFeature;
use crate::tracing::TracingFeature;

struct DefaultTypeOpsFactoryImpl;
impl TypeOpsFactory for DefaultTypeOpsFactoryImpl {
    fn create(&self, adapter_type: AdapterType) -> Arc<dyn TypeOps> {
        Arc::new(DefaultTypeOps::new(adapter_type))
    }
}

struct DefaultAdapterFactoryImpl {
    stmt_splitter: Arc<dyn StmtSplitter>,
}

impl DefaultAdapterFactoryImpl {
    fn create_engine(
        &self,
        adapter_type: AdapterType,
        adapter_config: AdapterConfig,
        type_ops_factory: Arc<dyn TypeOpsFactory>,
        quoting: ResolvedQuoting,
        query_comment: Option<QueryComment>,
        behavior_flag_overrides: BTreeMap<String, bool>,
        cloud_config: Option<&ResolvedCloudConfig>,
        threads: Option<usize>,
    ) -> FsResult<Arc<dyn AdapterEngine>> {
        let backend = backend_of(adapter_type);
        let auth: Arc<dyn Auth> = auth_for_backend(backend).into();
        let stmt_splitter = Arc::clone(&self.stmt_splitter);
        let type_ops = type_ops_factory.create(adapter_type);
        let relation_cache = Arc::new(RelationCache::default());

        let query_comment =
            QueryCommentConfig::from_query_comment(query_comment, adapter_type, true, cloud_config);

        let engine = Arc::new(XdbcEngine::new(
            adapter_type,
            auth,
            adapter_config,
            quoting,
            query_comment,
            type_ops,
            stmt_splitter,
            None,
            relation_cache,
            behavior_flag_overrides,
            threads,
        ));
        Ok(engine)
    }
}

impl AdapterFactory for DefaultAdapterFactoryImpl {
    fn create_adapter(
        &self,
        adapter_type: AdapterType,
        config: dbt_yaml::Mapping,
        type_ops_factory: Arc<dyn TypeOpsFactory>,
        _replay_mode: Option<ReplayMode>,
        flags: BTreeMap<String, MinijinjaValue>,
        schema_cache: Option<Arc<dyn SchemaStoreTrait>>,
        _query_cache: Option<Arc<dyn QueryCache>>,
        quoting: ResolvedQuoting,
        query_comment: Option<QueryComment>,
        token: CancellationToken,
        cloud_config: Option<&SchemasResolvedCloudConfig>,
        threads: Option<usize>,
    ) -> FsResult<Arc<Adapter>> {
        let adapter_config = AdapterConfig::new(config);

        let behavior_flag_overrides = flags
            .iter()
            .map(|(key, value)| {
                let bool_val = if value.is_true() {
                    true
                } else if let Some(s) = value.as_str() {
                    s == "true" || s.parse::<bool>().unwrap_or(false)
                } else {
                    false
                };
                (key.clone(), bool_val)
            })
            .collect::<BTreeMap<_, _>>();

        let engine = self.create_engine(
            adapter_type,
            AdapterConfig::new(adapter_config.repr().clone()),
            type_ops_factory,
            quoting,
            query_comment,
            behavior_flag_overrides,
            cloud_config,
            threads,
        )?;

        let adapter_impl = Arc::new(AdapterImpl::new(engine, schema_cache));

        // Create adapter with appropriate time machine mode
        let adapter: Arc<Adapter> = Arc::new(Adapter::new(adapter_impl, None, token));
        Ok(adapter)
    }

    fn stmt_splitter(&self) -> Arc<dyn StmtSplitter> {
        unimplemented!()
    }

    fn create_relation_from_node(
        &self,
        _node: &dyn InternalDbtNodeAttributes,
        _adapter_type: AdapterType,
    ) -> Result<Box<dyn BaseRelation>, minijinja::Error> {
        unimplemented!()
    }
}
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
    cli_extension: CliExtensionFeature,
    task_runner: TaskRunnerFeature,
    resolver: ResolverFeature,
    loader: LoaderFeature,
    license_fetcher: Arc<dyn LicenseFetcher>,
    dbt_distribution: &'static str,
    version_check_disabled: bool,
}

impl FeatureStackBuilder {
    pub fn new(tracing: TracingFeature) -> Self {
        let adapter = {
            let type_ops_factory = Arc::new(DefaultTypeOpsFactoryImpl);
            let adapter_factory: Arc<dyn AdapterFactory> = {
                let stmt_splitter = Arc::new(DefaultStmtSplitter {});
                Arc::new(DefaultAdapterFactoryImpl { stmt_splitter })
            };

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
        let cli_extension = {
            let hooks = Box::new(DefaultCliExtensionHooks);
            CliExtensionFeatureBuilder::with_hooks(hooks).build()
        };
        Self {
            send_anonymous_usage_stats: false,
            tracing,
            adapter,
            antlr_parser: Default::default(),
            sidecar: SidecarFeature::default(),
            cli_extension,
            task_runner,
            resolver: ResolverFeature::default(),
            loader: LoaderFeature::default(),
            license_fetcher: Arc::new(NoOpLicenseFetcher),
            dbt_distribution: "unknown-oss",
            version_check_disabled: false,
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

    pub fn disable_version_check(mut self) -> Self {
        self.version_check_disabled = true;
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

    pub fn cli_extension(mut self, feature: CliExtensionFeature) -> Self {
        self.cli_extension = feature;
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
            cli_extension: self.cli_extension,
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
            version_check_disabled: self.version_check_disabled,
            cancellation_token_source: CancellationTokenSource::new(),
            fail_fast: FailFast::new(),
        };
        Box::new(stack)
    }
}
