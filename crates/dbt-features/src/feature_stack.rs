use async_trait::async_trait;
use dbt_clap_core::Cli;
use dbt_common::DiscreteEventEmitter;
use dbt_common::FsResult;
use dbt_common::cancellation::CancellationToken;
use dbt_common::cancellation::CancellationTokenSource;
use dbt_common::fail_fast::FailFast;
use dbt_common::io_args::EvalArgs;
use dbt_common::tracing::TracingFeaturesHandle;
use dbt_dag::schedule::Schedule;
use dbt_schemas::schemas::PreviousState;
use dbt_schemas::state::ResolverState;
// use dbt_tasks::task_runner::RunTasksOk;
use std::fmt;

pub trait CommandHandler: Sync + Send {
    fn process_eval_args(
        &self,
        eval_args: &EvalArgs,
        resolver_state: &ResolverState,
    ) -> FsResult<()>;
}

/// The instrumentation feature. Exposed as a set of instrumentation services.
pub struct InstrumentationFeature {
    pub event_emitter: Box<dyn DiscreteEventEmitter>,
    // TODO: add more instrumentation services here
}

/// The formatter feature. Exposed as a [CommandHandler] implementation.
pub struct FormatterFeature {
    pub command_handler: Box<dyn CommandHandler>,
}

pub struct LinterFeature {
    pub command_handler: Box<dyn CommandHandler>,
}

#[async_trait]
#[allow(clippy::too_many_arguments)]
pub trait CliExtensionHooks: Send + Sync {
    /// Called early in execution, before any tasks are scheduled or run.
    fn will_execute(&self, _cli: &Cli, _arg: &EvalArgs) -> FsResult<()> {
        Ok(())
    }

    /// Called after tasks have been scheduled and run, but before manifest
    /// update and further phases.
    ///
    /// Return `Ok(())` if execution was not fully handled by this hook and
    /// should continue normally. To signal that a command was handled and
    /// execution should terminate, return `Err(FsError::exit_with_status(0))`
    /// for success or `Err(FsError::exit_with_status(n))` for failure.
    async fn did_schedule_and_run_tasks(
        &self,
        _arg: &EvalArgs,
        _cli: &Cli,
        _previous_state: Option<&PreviousState>,
        // _run_tasks_ok: &RunTasksOk,
        _resolved_state: &ResolverState,
        _token: &CancellationToken,
    ) -> FsResult<()> {
        Ok(())
    }

    /// Called after compilation and manifest update, once the full schedule
    /// and lineage information are available.
    ///
    /// Return `Ok(())` if execution was not fully handled by this hook and
    /// should continue normally. To signal that a command was handled and
    /// execution should terminate, return `Err(FsError::exit_with_status(0))`
    /// for success or `Err(FsError::exit_with_status(n))` for failure.
    async fn did_compile(
        &self,
        _arg: &EvalArgs,
        _cli: &Cli,
        // _run_tasks_ok: &RunTasksOk,
        _resolved_state: &ResolverState,
        _schedule: &Schedule<String>,
        _token: &CancellationToken,
    ) -> FsResult<()> {
        Ok(())
    }
}

pub struct CliExtensionFeature {
    pub hooks: Box<dyn CliExtensionHooks>,
}

/// A feature stack is an object that can be initialized with type-erased
/// objects that implement feature-specific services.
pub struct FeatureStack {
    pub instrumentation: InstrumentationFeature,
    pub formatter: FormatterFeature,
    pub linter: LinterFeature,
    pub cli_extension: CliExtensionFeature,
    pub tracing: TracingFeaturesHandle,
    // TODO: add more features here
    /// Global [CancelltionTokenSource] that can be used to signal cancellation to
    /// tasks running in other threads from a signal handler (e.g. Ctrl+C).
    pub cancellation_token_source: CancellationTokenSource,
    /// Per CLI invocation fail-fast signal.
    ///
    /// Each invocation of the CLI (or test) gets its own isolated signal
    /// so concurrent runs don't interfere with each other.
    pub fail_fast: FailFast,
}

impl fmt::Debug for FeatureStack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FeatureStack").finish()
    }
}
