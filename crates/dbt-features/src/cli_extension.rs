use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use dbt_adapter::Adapter;
use dbt_clap_core::commands::ExtensionCommandParser;
use dbt_clap_core::{Cli, CliParser, CliParserFactory, InitArgs};
use dbt_cloud_config::ResolvedCloudConfig;
use dbt_common::FsResult;
use dbt_common::cancellation::CancellationToken;
use dbt_common::io_args::{EvalArgs, IoArgs};
use dbt_compilation::config::CompilationConfig;
use dbt_dag::schedule::Schedule;
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_schema_store::{DataStoreTrait, SchemaStoreTrait};
use dbt_schemas::schemas::StateArtifacts;
use dbt_schemas::state::{DbtState, ResolverState};
use dbt_tasks_core::context::TaskRunnerCtx;
use dbt_tasks_core::{PreTaskRunData, RunTaskResults};
use minijinja::Value as MinijinjaValue;
use uuid::Uuid;

use crate::feature_stack::FeatureStack;
use crate::metricflow::MetricflowClient;

pub struct CliExtensionFeature {
    pub hooks: Box<dyn CliExtensionHooks>,
    pub cli_parser_factory: Arc<dyn CliParserFactory>,
}

pub struct CliExtensionFeatureBuilder {
    pub hooks: Box<dyn CliExtensionHooks>,
    pub cli_parser_factory: Arc<dyn CliParserFactory>,
}

impl CliExtensionFeatureBuilder {
    pub fn with_hooks(hooks: Box<dyn CliExtensionHooks>) -> Self {
        Self {
            hooks,
            cli_parser_factory: Arc::new(DefaultCliParserFactory {
                command_name: "dbt-fusion",
            }),
        }
    }

    pub fn cli_parser_factory(mut self, factory: Arc<dyn CliParserFactory>) -> Self {
        self.cli_parser_factory = factory;
        self
    }

    pub fn build(self) -> CliExtensionFeature {
        CliExtensionFeature {
            hooks: self.hooks,
            cli_parser_factory: self.cli_parser_factory,
        }
    }
}

struct DefaultCliParserFactory {
    command_name: &'static str,
}

impl CliParserFactory for DefaultCliParserFactory {
    fn create(&self) -> CliParser {
        CliParser::new(self.command_name, Box::new(NoopExtensionCommandParser))
    }
}

pub fn default_cli_parser_factory(command_name: &'static str) -> Box<dyn CliParserFactory> {
    Box::new(DefaultCliParserFactory { command_name })
}

struct NoopExtensionCommandParser;

impl ExtensionCommandParser for NoopExtensionCommandParser {
    fn has_subcommand(&self, _name: &str) -> bool {
        false
    }
}

#[async_trait]
pub trait CliExtensionHooks: Send + Sync {
    /// Called before CLI compilation argument validation.
    ///
    /// Allowing extensions to inspect or reject arguments before any execution begins.
    fn will_validate_compilation_cli_args(
        &self,
        cli: &Cli,
        eval_arg: &mut Cow<EvalArgs>,
        dbt_state: &Arc<DbtState>,
        config: &CompilationConfig,
    ) -> FsResult<()>;

    /// Called when `dbt init` is invoked, before project initialization begins.
    async fn will_init_project(
        &self,
        invocation_id: Uuid,
        cli: &Cli,
        init_args: &InitArgs,
    ) -> FsResult<()>;

    /// Called early in execution, before any tasks are scheduled or run.
    async fn will_execute(
        &self,
        cli: &Cli,
        eval_arg: &EvalArgs,
        feature_stack: &Arc<FeatureStack>,
    ) -> FsResult<()>;

    /// Called after the project has been resolved, before task scheduling
    /// and execution.
    ///
    /// This is the earliest point where `ResolverState` (including nodes,
    /// groups, and other resolved project data) is available.
    async fn did_resolve_project(
        &self,
        cli: &Cli,
        arg: &EvalArgs,
        resolved_state: &ResolverState,
        jinja_env: &JinjaEnv,
    ) -> FsResult<()>;

    /// Called just before tasks are scheduled and run.
    fn will_run_tasks(
        &self,
        cli: &Cli,
        arg: &EvalArgs,
        resolved_state: &ResolverState,
        token: &CancellationToken,
    ) -> FsResult<()>;

    /// Called after tasks have been scheduled and run, but before manifest
    /// update and further phases.
    ///
    /// Return `Ok(())` if execution was not fully handled by this hook and
    /// should continue normally. To signal that a command was handled and
    /// execution should terminate, return `Err(FsError::exit_with_status(0))`
    /// for success or `Err(FsError::exit_with_status(n))` for failure.
    async fn did_schedule_and_run_tasks(
        &self,
        arg: &EvalArgs,
        cli: &Cli,
        previous_state: Option<&StateArtifacts>,
        run_task_results: &RunTaskResults,
        resolved_state: &ResolverState,
        token: &CancellationToken,
    ) -> FsResult<()>;

    /// Called after compile output has been emitted, providing the full
    /// compilation state for consumers that need post-compile access
    /// (e.g. REPL bootstrap).
    async fn did_emit_selected_compile_output(
        &self,
        arg: &EvalArgs,
        resolved_state: &ResolverState,
        jinja_env: &Arc<JinjaEnv>,
        task_runner_ctx: Option<&TaskRunnerCtx>,
        schema_store: &Arc<dyn SchemaStoreTrait>,
        data_store: &Arc<dyn DataStoreTrait>,
        map_compiled_sql: &HashMap<String, Option<String>>,
        feature_stack: &Arc<FeatureStack>,
        token: &CancellationToken,
    ) -> FsResult<()>;

    /// Called after compilation and manifest update, once the full schedule
    /// and lineage information are available.
    ///
    /// This is called after `did_schedule_and_run_tasks` and compilation
    /// happened without errors.
    ///
    /// Return `Ok(())` if execution was not fully handled by this hook and
    /// should continue normally. To signal that a command was handled and
    /// execution should terminate, return `Err(FsError::exit_with_status(0))`
    /// for success or `Err(FsError::exit_with_status(n))` for failure.
    async fn did_compile(
        &self,
        arg: &EvalArgs,
        cli: &Cli,
        resolved_state: &ResolverState,
        schedule: &Schedule<String>,
        token: &CancellationToken,
    ) -> FsResult<()>;

    /// Called after all compile-time setup (adapter init, defer, schema hydration)
    /// and before the task runner starts.
    ///
    /// Returns per-node data that the task runner consumes, or `None` if this
    /// hook has nothing to contribute. An `Err` with an exit status signals that
    /// the hook handled the command fully and execution should terminate.
    async fn did_pre_run(
        &self,
        arg: &EvalArgs,
        cli: &Cli,
        jinja_env: Cow<'_, JinjaEnv>,
        augmented_resolved_state: &ResolverState,
        schedule: &Schedule<String>,
        adapter: Arc<Adapter>,
        base_context: &BTreeMap<String, MinijinjaValue>,
        token: &CancellationToken,
    ) -> FsResult<Option<Box<dyn PreTaskRunData>>>;

    async fn did_handle_defer(
        &self,
        arg: &EvalArgs,
        cli: &Cli,
        jinja_env: Cow<'_, JinjaEnv>,
        augmented_resolved_state: &ResolverState,
        schedule: &Schedule<String>,
        metricflow_client: Option<Arc<dyn MetricflowClient>>,
        token: &CancellationToken,
    ) -> FsResult<()>;

    /// Called before deferred state is loaded. Implementations may populate
    /// `manifest_path` with a locally cached manifest to use for deferral.
    async fn will_load_deferred_state(
        &self,
        io: &IoArgs,
        cloud_config: Option<&ResolvedCloudConfig>,
        manifest_path: &mut Option<PathBuf>,
    ) -> FsResult<()>;
}

pub(crate) struct DefaultCliExtensionHooks;

#[async_trait]
impl CliExtensionHooks for DefaultCliExtensionHooks {
    fn will_validate_compilation_cli_args(
        &self,
        _cli: &Cli,
        _eval_arg: &mut Cow<EvalArgs>,
        _dbt_state: &Arc<DbtState>,
        _config: &CompilationConfig,
    ) -> FsResult<()> {
        Ok(())
    }

    async fn will_init_project(
        &self,
        _invocation_id: Uuid,
        _cli: &Cli,
        _init_args: &InitArgs,
    ) -> FsResult<()> {
        Ok(())
    }

    async fn will_execute(
        &self,
        _cli: &Cli,
        _eval_arg: &EvalArgs,
        _feature_stack: &Arc<FeatureStack>,
    ) -> FsResult<()> {
        Ok(())
    }

    async fn did_resolve_project(
        &self,
        _cli: &Cli,
        _arg: &EvalArgs,
        _resolved_state: &ResolverState,
        _jinja_env: &JinjaEnv,
    ) -> FsResult<()> {
        Ok(())
    }

    fn will_run_tasks(
        &self,
        _cli: &Cli,
        _arg: &EvalArgs,
        _resolved_state: &ResolverState,
        _token: &CancellationToken,
    ) -> FsResult<()> {
        Ok(())
    }

    async fn did_schedule_and_run_tasks(
        &self,
        _arg: &EvalArgs,
        _cli: &Cli,
        _previous_state: Option<&StateArtifacts>,
        _run_task_results: &RunTaskResults,
        _resolved_state: &ResolverState,
        _token: &CancellationToken,
    ) -> FsResult<()> {
        Ok(())
    }

    async fn did_emit_selected_compile_output(
        &self,
        _arg: &EvalArgs,
        _resolved_state: &ResolverState,
        _jinja_env: &Arc<JinjaEnv>,
        _task_runner_ctx: Option<&TaskRunnerCtx>,
        _schema_store: &Arc<dyn SchemaStoreTrait>,
        _data_store: &Arc<dyn DataStoreTrait>,
        _map_compiled_sql: &HashMap<String, Option<String>>,
        _feature_stack: &Arc<FeatureStack>,
        _token: &CancellationToken,
    ) -> FsResult<()> {
        Ok(())
    }

    async fn did_compile(
        &self,
        _arg: &EvalArgs,
        _cli: &Cli,
        _resolved_state: &ResolverState,
        _schedule: &Schedule<String>,
        _token: &CancellationToken,
    ) -> FsResult<()> {
        Ok(())
    }

    async fn did_pre_run(
        &self,
        _arg: &EvalArgs,
        _cli: &Cli,
        _jinja_env: Cow<'_, JinjaEnv>,
        _augmented_resolved_state: &ResolverState,
        _schedule: &Schedule<String>,
        _adapter: Arc<Adapter>,
        _base_context: &BTreeMap<String, MinijinjaValue>,
        _token: &CancellationToken,
    ) -> FsResult<Option<Box<dyn PreTaskRunData>>> {
        Ok(None)
    }

    async fn did_handle_defer(
        &self,
        _arg: &EvalArgs,
        _cli: &Cli,
        _jinja_env: Cow<'_, JinjaEnv>,
        _augmented_resolved_state: &ResolverState,
        _schedule: &Schedule<String>,
        _metricflow_client: Option<Arc<dyn MetricflowClient>>,
        _token: &CancellationToken,
    ) -> FsResult<()> {
        Ok(())
    }

    async fn will_load_deferred_state(
        &self,
        _io: &IoArgs,
        _cloud_config: Option<&ResolvedCloudConfig>,
        _manifest_path: &mut Option<PathBuf>,
    ) -> FsResult<()> {
        Ok(())
    }
}
