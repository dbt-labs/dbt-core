use std::any::Any;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use clap::{Parser, Subcommand};
use minijinja::Value as MinijinjaValue;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use dbt_adapter::Adapter;
use dbt_clap_core::commands::{AbstractExtensionCommand, ExtensionCommandParser};
use dbt_clap_core::{Cli, CliParser, CliParserFactory, CommonArgs, InitArgs, in_out_dir};
use dbt_cloud_config::ResolvedCloudConfig;
use dbt_common::cancellation::{CancellationToken, CancellationTokenSource};
use dbt_common::fail_fast::FailFast;
use dbt_common::io_args::{EvalArgs, FsCommand, IoArgs, Phases, SystemArgs};
use dbt_common::tracing::dbt_emit::{
    emit_error_log_from_fs_error, emit_error_log_message, println as emit_stdout_line,
};
use dbt_common::{ErrorCode, FsError, FsResult, fs_err};
use dbt_compilation::config::CompilationConfig;
use dbt_dag::schedule::Schedule;
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_schema_store::{DataStoreTrait, SchemaStoreTrait};
use dbt_schemas::schemas::StateArtifacts;
use dbt_schemas::state::{DbtState, ResolverState};
use dbt_tasks_core::context::TaskRunnerCtx;
use dbt_tasks_core::{PreTaskRunData, RunTaskResults};

use crate::feature_stack::FeatureStack;
use crate::metricflow::MetricflowClient;

pub struct CliFeature {
    pub command_name: &'static str,
    pub hooks: Box<dyn CliExtensionHooks>,
    pub cli_parser_factory: Arc<dyn CliParserFactory>,
    /// Global [CancelltionTokenSource] that can be used to signal cancellation to
    /// tasks running in other threads from a signal handler (e.g. Ctrl+C).
    pub cancellation_token_source: CancellationTokenSource,
    /// Per CLI invocation fail-fast signal.
    ///
    /// Each invocation of the CLI (or test) gets its own isolated signal
    /// so concurrent runs don't interfere with each other.
    pub fail_fast: FailFast,
}

pub struct CliFeatureBuilder {
    command_name: &'static str,
    hooks: Option<Box<dyn CliExtensionHooks>>,
    cli_parser_factory: Option<Arc<dyn CliParserFactory>>,
}

impl CliFeatureBuilder {
    pub fn new(command_name: &'static str) -> Self {
        Self {
            command_name,
            hooks: None,
            cli_parser_factory: None,
        }
    }

    pub fn hooks(mut self, hooks: Box<dyn CliExtensionHooks>) -> Self {
        self.hooks = Some(hooks);
        self
    }

    pub fn cli_parser_factory(mut self, factory: Arc<dyn CliParserFactory>) -> Self {
        self.cli_parser_factory = Some(factory);
        self
    }

    pub fn build(self) -> CliFeature {
        let hooks = self
            .hooks
            .unwrap_or_else(|| Box::new(DefaultCliExtensionHooks));

        let cli_parser_factory = self
            .cli_parser_factory
            .unwrap_or_else(|| Arc::new(DefaultCliParserFactory));

        CliFeature {
            command_name: self.command_name,
            hooks,
            cli_parser_factory,
            cancellation_token_source: CancellationTokenSource::new(),
            fail_fast: FailFast::new(),
        }
    }
}

#[derive(clap::Parser, Debug, Clone, Serialize, Deserialize)]
#[command()]
pub enum SystemCommand {
    /// Informative-only update command for dbt Core 2.x.
    #[clap(hide = true)]
    Update,
    /// Informative-only uninstall command for dbt Core 2.x.
    #[clap(hide = true)]
    Uninstall,
    /// Preinstall all supported database drivers into the local cache
    InstallDrivers,
}

#[derive(Parser, Debug, Clone, Serialize, Deserialize)]
pub struct SystemMgmtArgs {
    #[command(subcommand)]
    pub command: SystemCommand,
    // Flattened Common args
    #[clap(flatten)]
    pub common_args: CommonArgs,
}

impl SystemMgmtArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let mut eval_args = self.common_args.to_eval_args(arg, in_dir, out_dir);
        eval_args.phase = Phases::Deps;
        eval_args
    }
}

#[derive(clap::Subcommand, Debug, Clone)]
pub enum OSSExtensionCommand {
    /// dbt Core 2.x system subcommand
    System(SystemMgmtArgs),
    /// Manage Fivetran AI catalog contextsets
    Contextset(ContextsetArgs),
}

#[derive(Parser, Debug, Clone, Serialize, Deserialize)]
pub struct ContextsetArgs {
    #[command(subcommand)]
    pub command: ContextsetCommand,
    #[clap(flatten)]
    pub common_args: CommonArgs,
}

#[derive(clap::Subcommand, Debug, Clone, Serialize, Deserialize)]
pub enum ContextsetCommand {
    /// Set the full definition of a Fivetran AI catalog contextset
    Set(ContextsetSetArgs),
    /// Fetch a Fivetran AI catalog contextset
    Get(ContextsetGetArgs),
    /// Delete a Fivetran AI catalog contextset
    Delete(ContextsetDeleteArgs),
}

#[derive(clap::Args, Clone, Serialize, Deserialize)]
pub struct ContextsetClientArgs {
    /// Base URL for the Fivetran AI MCP service
    #[arg(long, env = "DBT_FIVETRAN_MCP_URL")]
    pub url: String,
    /// Bearer token for the Fivetran AI MCP service
    #[arg(long, env = "DBT_FIVETRAN_MCP_TOKEN")]
    pub token: String,
    /// Fivetran group id that owns the contextset
    #[arg(long, env = "DBT_FIVETRAN_GROUP_ID")]
    pub group_id: String,
}

impl fmt::Debug for ContextsetClientArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ContextsetClientArgs")
            .field("url", &self.url)
            .field("token", &"<redacted>")
            .field("group_id", &self.group_id)
            .finish()
    }
}

#[derive(clap::Args, Debug, Clone, Serialize, Deserialize)]
pub struct ContextsetSetArgs {
    /// Contextset name
    pub name: String,
    /// Fully-qualified schema to allow. Repeat for multiple schemas.
    #[arg(long = "schema-fqn")]
    pub schema_fqns: Vec<String>,
    /// Fully-qualified table to allow. Repeat for multiple tables.
    #[arg(long = "table-fqn")]
    pub table_fqns: Vec<String>,
    #[clap(flatten)]
    pub client: ContextsetClientArgs,
}

#[derive(clap::Args, Debug, Clone, Serialize, Deserialize)]
pub struct ContextsetGetArgs {
    /// Contextset name
    pub name: String,
    #[clap(flatten)]
    pub client: ContextsetClientArgs,
}

#[derive(clap::Args, Debug, Clone, Serialize, Deserialize)]
pub struct ContextsetDeleteArgs {
    /// Contextset name
    pub name: String,
    #[clap(flatten)]
    pub client: ContextsetClientArgs,
}

impl ContextsetArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let mut eval_args = self.common_args.to_eval_args(arg, in_dir, out_dir);
        eval_args.phase = Phases::Deps;
        eval_args
    }
}

impl AbstractExtensionCommand for OSSExtensionCommand {
    fn name(&self) -> &'static str {
        match self {
            OSSExtensionCommand::System(_) => "system",
            OSSExtensionCommand::Contextset(_) => "contextset",
        }
    }

    fn clone_box(&self) -> Box<dyn AbstractExtensionCommand> {
        Box::new(self.clone())
    }

    fn display_fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        <Self as fmt::Debug>::fmt(self, f)
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self as &mut dyn Any
    }
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn is_project_command(&self) -> bool {
        use OSSExtensionCommand::*;
        !matches!(self, System(_) | Contextset(_))
    }

    fn to_eval_args(&self, common_args: &CommonArgs, system_arg: SystemArgs) -> FsResult<EvalArgs> {
        use OSSExtensionCommand::*;
        // Determine the input and output directories based on the command.
        // Some commands operate without project context, while others must be run in a project directory.
        let (in_dir, out_dir) = if self.is_project_command() {
            in_out_dir(common_args)?
        } else {
            (PathBuf::from("."), PathBuf::from("."))
        };

        let from_main = system_arg.from_main;
        let mut arg = match self {
            System(args) => args.to_eval_args(system_arg, &in_dir, &out_dir),
            Contextset(args) => args.to_eval_args(system_arg, &in_dir, &out_dir),
        };
        arg.from_main = from_main;

        Ok(arg)
    }

    fn common_args(&self) -> CommonArgs {
        use OSSExtensionCommand::*;
        match self {
            System(args) => args.common_args.clone(),
            Contextset(args) => args.common_args.clone(),
        }
    }

    fn stage(&self) -> Phases {
        use OSSExtensionCommand::*;
        match self {
            System(_) => unreachable!("System command does not need a phase"),
            Contextset(_) => unreachable!("Contextset command does not need a phase"),
        }
    }

    fn as_command(&self) -> FsCommand {
        match self {
            OSSExtensionCommand::System(_) => FsCommand::System,
            OSSExtensionCommand::Contextset(_) => FsCommand::Extension("contextset"),
        }
    }

    fn extend_cli_options(&self, _options: &mut Vec<String>) {
        // no-op
    }

    fn sample_select(&self) -> Option<Vec<String>> {
        None
    }

    fn sample_exclude(&self) -> Option<Vec<String>> {
        None
    }
}

pub struct DefaultCliParserFactory;

impl CliParserFactory for DefaultCliParserFactory {
    fn create(&self, command_name: &'static str, version: &'static str) -> CliParser {
        CliParser::new(command_name, version, Box::new(OSSExtensionCommandParser))
    }
}

struct OSSExtensionCommandParser;

impl ExtensionCommandParser for OSSExtensionCommandParser {
    fn from_arg_matches_mut(
        &self,
        arg_matches: &mut clap::ArgMatches,
    ) -> Result<Box<dyn AbstractExtensionCommand>, clap::Error> {
        let cmd = <OSSExtensionCommand as clap::FromArgMatches>::from_arg_matches_mut(arg_matches)?;
        Ok(Box::new(cmd) as Box<dyn AbstractExtensionCommand>)
    }

    fn augment_subcommands(&self, app: clap::Command) -> clap::Command {
        OSSExtensionCommand::augment_subcommands(app)
    }

    fn has_subcommand(&self, name: &str) -> bool {
        OSSExtensionCommand::has_subcommand(name)
    }
}

#[derive(Serialize)]
struct ContextsetPayload<'a> {
    schema_fqns: &'a [String],
    table_fqns: &'a [String],
}

async fn execute_contextset_command(command: &ContextsetCommand) -> FsResult<()> {
    match command {
        ContextsetCommand::Set(args) => {
            if args.schema_fqns.is_empty() && args.table_fqns.is_empty() {
                return Err(fs_err!(
                    ErrorCode::Generic,
                    "`dbt contextset set` requires at least one --schema-fqn or --table-fqn"
                ));
            }

            let payload = ContextsetPayload {
                schema_fqns: &args.schema_fqns,
                table_fqns: &args.table_fqns,
            };
            let body = send_contextset_request(
                reqwest::Method::PUT,
                &args.client,
                &args.name,
                Some(&payload),
            )
            .await?;
            emit_response_body(body, "Contextset saved.");
        }
        ContextsetCommand::Get(args) => {
            let body =
                send_contextset_request(reqwest::Method::GET, &args.client, &args.name, None)
                    .await?;
            emit_response_body(body, "Contextset found.");
        }
        ContextsetCommand::Delete(args) => {
            let body =
                send_contextset_request(reqwest::Method::DELETE, &args.client, &args.name, None)
                    .await?;
            emit_response_body(body, "Contextset deleted.");
        }
    }

    Ok(())
}

async fn send_contextset_request(
    method: reqwest::Method,
    client_args: &ContextsetClientArgs,
    name: &str,
    payload: Option<&ContextsetPayload<'_>>,
) -> FsResult<String> {
    let client = reqwest::Client::new();
    let url = contextset_url(&client_args.url, name, &client_args.group_id)?;
    let mut request = client
        .request(method, url)
        .bearer_auth(&client_args.token)
        .header(reqwest::header::ACCEPT, "application/json");

    if let Some(payload) = payload {
        request = request.json(payload);
    }

    let response = request.send().await.map_err(|err| {
        fs_err!(
            ErrorCode::Generic,
            "Failed to call Fivetran AI MCP contextset endpoint: {}",
            err
        )
    })?;

    let status = response.status();
    let body = response.text().await.map_err(|err| {
        fs_err!(
            ErrorCode::Generic,
            "Failed to read Fivetran AI MCP contextset response: {}",
            err
        )
    })?;

    if !status.is_success() {
        return Err(fs_err!(
            ErrorCode::Generic,
            "Fivetran AI MCP contextset endpoint returned HTTP {}: {}",
            status.as_u16(),
            body
        ));
    }

    Ok(body)
}

fn contextset_url(base_url: &str, name: &str, group_id: &str) -> FsResult<reqwest::Url> {
    let mut url = reqwest::Url::parse(base_url)
        .map_err(|err| fs_err!(ErrorCode::Generic, "Invalid Fivetran AI MCP URL: {}", err))?;

    url.path_segments_mut()
        .map_err(|_| {
            fs_err!(
                ErrorCode::Generic,
                "Invalid Fivetran AI MCP URL: cannot append endpoint path"
            )
        })?
        .pop_if_empty()
        .push("contextsets")
        .push(name);
    url.query_pairs_mut().append_pair("group_id", group_id);
    Ok(url)
}

fn emit_response_body(body: String, fallback: &str) {
    if body.trim().is_empty() {
        emit_stdout_line(fallback);
    } else {
        emit_stdout_line(body);
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
        cli: &Cli,
        eval_arg: &EvalArgs,
        _feature_stack: &Arc<FeatureStack>,
    ) -> FsResult<()> {
        use OSSExtensionCommand::*;
        match cli.extension_command::<OSSExtensionCommand>() {
            Some(System(args)) => {
                match &args.command {
                    SystemCommand::Update => {
                        let e = fs_err!(
                            ErrorCode::NotSupported,
                            "`dbt system update` is not supported for this distribution. Upgrade \
             dbt-core with the package manager you installed it with:\
             \n\n    pip install --pre --upgrade dbt-core\
             \n    brew upgrade dbt-core\
             \n    winget upgrade --id dbtLabs.dbt-core --exact"
                        );
                        emit_error_log_from_fs_error(
                            e.as_ref(),
                            eval_arg.io.status_reporter.as_ref(),
                        );
                        Err(FsError::exit_with_status(1))
                    }
                    SystemCommand::Uninstall => {
                        let e = fs_err!(
                            ErrorCode::NotSupported,
                            "`dbt system uninstall` is not supported for this distribution. Remove \
             dbt-core with the package manager you installed it with:\
             \n\n    pip uninstall dbt-core\
             \n    brew uninstall dbt-core\
             \n    winget uninstall --id dbtLabs.dbt-core"
                        );
                        emit_error_log_from_fs_error(
                            e.as_ref(),
                            eval_arg.io.status_reporter.as_ref(),
                        );
                        Err(FsError::exit_with_status(1))
                    }
                    SystemCommand::InstallDrivers => {
                        dbt_adbc::pre_install_all_drivers().map_err(|install_err| {
                            emit_error_log_message(
                                ErrorCode::Generic,
                                format!("Failed to install drivers: {}", install_err).as_str(),
                                eval_arg.io.status_reporter.as_ref(),
                            );
                            FsError::exit_with_status(1)
                        })
                    }
                }?;
                // handled the System command, signal to exit with success
                Err(FsError::exit_with_status(0))
            }
            Some(Contextset(args)) => {
                if let Err(err) = execute_contextset_command(&args.command).await {
                    emit_error_log_from_fs_error(
                        err.as_ref(),
                        eval_arg.io.status_reporter.as_ref(),
                    );
                    return Err(FsError::exit_with_status(1));
                }
                Err(FsError::exit_with_status(0))
            }
            _ => Ok(()), // nothing handled, continue normal execution
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn system_help(parser: &CliParser) -> String {
        parser
            .try_parse_from(["dbt", "system", "--help"])
            .unwrap_err()
            .to_string()
    }

    #[test]
    fn hidden_self_management_commands_are_absent_from_help_but_parseable() {
        let version = "2.x";
        let parser = CliParser::new("dbt-core", version, Box::new(OSSExtensionCommandParser));

        // Hidden from help, but unaffected commands remain.
        let help = system_help(&parser);
        assert!(!help.contains("Update dbt in place"), "got:\n{help}");
        assert!(!help.contains("Uninstall dbt"), "got:\n{help}");
        assert!(help.contains("install-drivers"), "got:\n{help}");

        // Still parseable, so the runtime message fires instead of a parse error.
        let cli = parser.try_parse_from(["dbt", "system", "update"]).unwrap();
        let oss_ext_cmd = cli.extension_command::<OSSExtensionCommand>().unwrap();
        assert!(matches!(
            oss_ext_cmd,
            OSSExtensionCommand::System(SystemMgmtArgs {
                command: SystemCommand::Update,
                ..
            })
        ));
    }

    #[test]
    fn system_is_not_a_project_command() {
        let parser = CliParser::new("dbt-core", "2.x", Box::new(OSSExtensionCommandParser));
        let cli = parser.try_parse_from(["dbt", "system", "update"]).unwrap();

        let oss_ext_cmd = cli.extension_command::<OSSExtensionCommand>().unwrap();
        assert!(!oss_ext_cmd.is_project_command());

        // `Cli::is_project_command` should delegate to the extension command.
        assert!(!cli.is_project_command());
    }

    #[test]
    fn contextset_set_is_parseable() {
        let parser = CliParser::new("dbt-core", "2.x", Box::new(OSSExtensionCommandParser));
        let cli = parser
            .try_parse_from([
                "dbt",
                "contextset",
                "set",
                "customer_support",
                "--url",
                "http://localhost:30820",
                "--token",
                "pat_test",
                "--group-id",
                "group_id",
                "--schema-fqn",
                "WAREHOUSE.ZENDESK",
                "--table-fqn",
                "WAREHOUSE.TRANSFORMS_BI.CUSTOMER_360",
            ])
            .unwrap();

        let oss_ext_cmd = cli.extension_command::<OSSExtensionCommand>().unwrap();
        assert!(matches!(
            oss_ext_cmd,
            OSSExtensionCommand::Contextset(ContextsetArgs {
                command: ContextsetCommand::Set(ContextsetSetArgs {
                    name,
                    schema_fqns,
                    table_fqns,
                    ..
                }),
                ..
            }) if name == "customer_support"
                && schema_fqns == &vec!["WAREHOUSE.ZENDESK".to_string()]
                && table_fqns == &vec!["WAREHOUSE.TRANSFORMS_BI.CUSTOMER_360".to_string()]
        ));
    }

    #[test]
    fn contextset_is_not_a_project_command() {
        let parser = CliParser::new("dbt-core", "2.x", Box::new(OSSExtensionCommandParser));
        let cli = parser
            .try_parse_from([
                "dbt",
                "contextset",
                "get",
                "customer_support",
                "--url",
                "http://localhost:30820",
                "--token",
                "pat_test",
                "--group-id",
                "group_id",
            ])
            .unwrap();

        let oss_ext_cmd = cli.extension_command::<OSSExtensionCommand>().unwrap();
        assert!(!oss_ext_cmd.is_project_command());
        assert!(!cli.is_project_command());
    }

    #[test]
    fn contextset_client_args_debug_redacts_token() {
        let args = ContextsetClientArgs {
            url: "http://localhost:30820".to_string(),
            token: "pat_test".to_string(),
            group_id: "group_id".to_string(),
        };

        let debug = format!("{args:?}");

        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("pat_test"));
    }

    #[test]
    fn contextset_url_appends_encoded_path_and_query() {
        let url = contextset_url(
            "http://localhost:30820/",
            "customer support",
            "sandbox group",
        )
        .unwrap();

        assert_eq!(
            url.as_str(),
            "http://localhost:30820/contextsets/customer%20support?group_id=sandbox+group"
        );
    }
}
