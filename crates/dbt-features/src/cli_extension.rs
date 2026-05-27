use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use dbt_adapter::Adapter;
use dbt_clap_core::{Cli, InitArgs};
use dbt_common::FsResult;
use dbt_common::cancellation::CancellationToken;
use dbt_common::io_args::EvalArgs;
use dbt_compilation::config::CompilationConfig;
use dbt_dag::schedule::Schedule;
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_schema_store::{DataStoreTrait, SchemaStoreTrait};
use dbt_schemas::schemas::PreviousState;
use dbt_schemas::state::{DbtState, ResolverState};
use dbt_tasks_core::context::TaskRunnerCtx;
use dbt_tasks_core::{PreTaskRunData, RunTaskResults};
use minijinja::Value as MinijinjaValue;
use uuid::Uuid;

use crate::feature_stack::FeatureStack;
use crate::metricflow::MetricflowClient;

pub struct CliExtensionFeature {
    pub hooks: Box<dyn CliExtensionHooks>,
}

pub struct CliExtensionFeatureBuilder {
    pub hooks: Box<dyn CliExtensionHooks>,
}

impl CliExtensionFeatureBuilder {
    pub fn with_hooks(hooks: Box<dyn CliExtensionHooks>) -> Self {
        Self { hooks }
    }

    pub fn build(self) -> CliExtensionFeature {
        CliExtensionFeature { hooks: self.hooks }
    }
}

#[async_trait]
#[allow(clippy::too_many_arguments)]
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
        arg: &EvalArgs,
        resolved_state: &ResolverState,
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
        previous_state: Option<&PreviousState>,
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
}
