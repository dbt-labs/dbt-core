use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;

use dbt_common::fail_fast::FailFast;
use dbt_common::io_args::FsCommand;
use dbt_common::io_args::IoArgs;
use dbt_common::io_args::LocalExecutionBackendKind;
use dbt_common::io_args::StaticAnalysisKind;
use dbt_common::io_args::{DisplayFormat, EvalArgs, ReplayMode};
use dbt_common::io_args::{Phases, RunCacheMode};
use dbt_common::static_analysis::{
    is_static_analysis_off_or_baseline, normalize_static_analysis_kind,
};
use dbt_common::warn_error_options::WarnErrorOptions;
use dbt_yaml::Spanned;

#[derive(Clone, Default)]
pub struct RunTasksArgs {
    pub command: FsCommand,
    // The io args
    pub io: IoArgs,
    // The profile to use (user input)
    pub profile: Option<String>,
    // The profile directory to load the profiles from
    pub profiles_dir: Option<PathBuf>,
    // The directory to install packages
    pub packages_install_path: Option<PathBuf>,
    // The target within the profile to use for the dbt run (user input)
    pub target: Option<String>,
    // Resolved profile name (from DbtProfile after resolution)
    pub resolved_profile: String,
    // Resolved target name (from DbtProfile after resolution)
    pub resolved_target: String,
    // Resolved database name (from DbtProfile after resolution)
    pub resolved_database: String,
    // Resolved schema name (from DbtProfile after resolution)
    pub resolved_schema: String,
    // Whether to update dependencies
    pub update_deps: bool,
    // Vars to pass to the jinja environment
    pub vars: BTreeMap<String, dbt_yaml::Value>,
    // Display rows in different formats, this is .to_string on DisplayFormat; we use a string here to break dep. cycle
    pub format: DisplayFormat,
    /// Limiting number of shown rows. Run with --limit -1 to remove limit
    pub limit: Option<usize>,
    /// Whether to write json output
    pub write_json: bool,
    /// Whether to maintain the parquet index during command execution.
    pub write_index: bool,
    /// Whether this is the main command or a subcommand
    pub from_main: bool,
    /// Number of threads (connection backpressure + parser rendering). Not
    /// used to force sequential task execution; see `no_parallel`.
    pub num_threads: usize,
    /// When true, the task graph is visited sequentially (one node at a time)
    /// regardless of `num_threads`. Use for deterministic test output.
    pub no_parallel: bool,
    /// Whether to replay from the execution cache
    pub replay: Option<ReplayMode>,
    /// Whether to compile only
    pub static_analysis: Spanned<StaticAnalysisKind>,
    /// Whether to skip the unreferenced tables
    pub skip_unreferenced_table_check: bool,

    /// Whether to favor state over current environment
    pub favor_state: bool,
    /// The mode to use for the run cache
    pub run_cache_mode: RunCacheMode,
    /// The url to use for the run cache
    pub task_cache_url: String,
    /// phase
    pub phase: Phases,
    /// Optional (resolved) sampling plan to locate local sampled data for sources
    pub sample_renaming: BTreeMap<String, (String, String, String)>,
    /// Backend used for local execution of runnable nodes
    pub local_execution_backend: LocalExecutionBackendKind,
    /// Drop into an interactive REPL after executing the command
    pub interactive: bool,
    /// Whether to perform a full refresh (rebuild incremental models from scratch)
    pub full_refresh: bool,
    /// If specified, the end datetime dbt uses to filter microbatch model inputs (exclusive).
    pub event_time_end: Option<String>,
    /// If specified, the start datetime dbt uses to filter microbatch model inputs (inclusive).
    pub event_time_start: Option<String>,
    /// Per-invocation fail-fast signal.
    pub fail_fast: FailFast,
    /// Whether to skip running post hook operations.
    pub skip_post_hooks: bool,
    /// Per-invocation warn-error options resolved before task execution.
    pub warn_error_options: WarnErrorOptions,
}

impl RunTasksArgs {
    pub fn from_eval_args(arg: &EvalArgs, fail_fast: FailFast) -> Self {
        Self {
            command: arg.command,
            io: arg.io.clone(),
            profile: arg.profile.clone(),
            profiles_dir: arg.profiles_dir.clone(),
            packages_install_path: arg.packages_install_path.clone(),
            target: arg.target.clone(),
            resolved_profile: String::new(),
            resolved_target: String::new(),
            resolved_database: String::new(),
            resolved_schema: String::new(),
            update_deps: arg.update_deps,
            vars: arg.vars.clone(),
            format: arg.format,
            limit: arg.limit,
            write_json: arg.write_json,
            write_index: arg.write_index,
            from_main: arg.from_main,
            num_threads: arg.num_threads.unwrap_or(0),
            no_parallel: arg.no_parallel,
            replay: arg.replay.clone(),
            static_analysis: Spanned::new(normalize_static_analysis_kind(
                arg.static_analysis.unwrap_or_default(),
            )),
            skip_unreferenced_table_check: arg.skip_unreferenced_table_check,
            task_cache_url: arg.task_cache_url.clone(),
            favor_state: arg.favor_state,
            run_cache_mode: arg.run_cache_mode.clone(),
            sample_renaming: arg.sample_renaming.clone(),
            phase: arg.phase.clone(),
            local_execution_backend: arg.local_execution_backend,
            interactive: arg.interactive,
            full_refresh: arg.full_refresh,
            event_time_start: arg.event_time_start.clone(),
            event_time_end: arg.event_time_end.clone(),
            fail_fast,
            skip_post_hooks: arg.skip_post_hooks,
            warn_error_options: arg.warn_error_options.clone(),
        }
    }

    /// Populate resolved profile fields from DbtProfile
    pub fn with_resolved_profile(mut self, profile: &dbt_schemas::state::DbtProfile) -> Self {
        self.resolved_profile = profile.profile.clone();
        self.resolved_target = profile.target.clone();
        self.resolved_database = profile.database.clone();
        self.resolved_schema = profile.schema.clone();
        self
    }

    pub fn static_analysis_off(&self) -> Spanned<bool> {
        self.static_analysis
            .clone()
            .map(|static_analysis| static_analysis == StaticAnalysisKind::Off)
    }

    pub fn static_analysis_off_or_baseline(&self) -> Spanned<bool> {
        self.static_analysis
            .clone()
            .map(is_static_analysis_off_or_baseline)
    }

    pub fn is_runnable(&self) -> bool {
        matches!(
            self.command,
            FsCommand::Run
                | FsCommand::Test
                | FsCommand::Build
                | FsCommand::Seed
                | FsCommand::Snapshot
        )
    }
}

impl fmt::Debug for RunTasksArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompilerArgs")
            .field("command", &self.command)
            .field("in_dir", &self.io.in_dir)
            .field("out_dir", &self.io.out_dir)
            .field("profile", &self.profile)
            .field("profiles_dir", &self.profiles_dir)
            .field("packages_install_path", &self.packages_install_path)
            .field("target", &self.target)
            .field("update_deps", &self.update_deps)
            .field("show", &self.io.show)
            .field("vars", &self.vars)
            .field("from_main", &self.from_main)
            .field("invocation_id", &self.io.invocation_id)
            .field("num_threads", &self.num_threads)
            .field("replay", &self.replay)
            .field("format", &self.format)
            .field("limit", &self.limit)
            .field("run_cache_url", &self.task_cache_url)
            .field("run_cache_mode", &self.run_cache_mode)
            .finish()
    }
}
