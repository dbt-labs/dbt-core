use crate::warn_error_options::WarnErrorOptions;
use clap::ValueEnum;
use dbt_adapter_core::AdapterType;
use dbt_base::{HashMap, HashSet};
use dbt_telemetry::NodeType;
use dbt_yaml::{JsonSchema, Value};
use pathdiff::diff_paths;
use serde::{Deserialize, Serialize};
use std::fs;
use std::str::FromStr;
use std::{
    collections::BTreeMap,
    fmt::{self, Display},
    path::{Path, PathBuf},
    sync::Arc,
};
use strum::EnumIter;
use strum_macros::Display;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum LocalExecutionBackendKind {
    #[default]
    /// Execute on the remote warehouse (Snowflake, BigQuery, etc.)
    Remote,
    /// Run models in the current process
    Inline,
    /// Run models in a separate worker process
    Worker,
    /// Run models in a service
    Service,
}

#[derive(
    Debug,
    Copy,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    ValueEnum,
    Display,
    Default,
    JsonSchema,
)]
#[serde(rename_all = "lowercase")]
#[clap(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum ComputeArg {
    #[default]
    /// Execute on the remote warehouse (Snowflake, BigQuery, etc.)
    Remote,
    /// Run computations in-process
    Inline,
    /// Run computations in a separate, ephemeral worker process
    Sidecar,
    /// Run via the remote compute service (persistent workers/cluster).
    Service,
}

impl From<ComputeArg> for LocalExecutionBackendKind {
    fn from(arg: ComputeArg) -> Self {
        match arg {
            ComputeArg::Remote => LocalExecutionBackendKind::Remote,
            ComputeArg::Inline => LocalExecutionBackendKind::Inline,
            ComputeArg::Sidecar => LocalExecutionBackendKind::Worker,
            ComputeArg::Service => LocalExecutionBackendKind::Service,
        }
    }
}

use crate::constants::DBT_TARGET_DIR_NAME;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[clap(rename_all = "lowercase")]
pub enum InternalPackageMode {
    /// Load internal packages from RustEmbed directly, no disk I/O (default).
    #[default]
    Embedded,
    /// Write embedded assets to disk, then load from disk (legacy).
    ForceWrite,
    /// Assume already on disk, just read (skip writing).
    #[value(alias = "read")]
    ReadFromDisk,
}

impl Display for InternalPackageMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.to_possible_value()
            .expect("no values are skipped")
            .get_name()
            .fmt(f)
    }
}
use crate::{
    constants::{DBT_GENERIC_TESTS_DIR_NAME, DBT_SNAPSHOTS_DIR_NAME},
    io_utils::StatusReporter,
    node_selector::{
        IndirectSelection, SelectExpression, SelectionCriteria, conjoin_expression,
        parse_model_specifiers,
    },
    tracing::invocation::with_invocation_mut,
};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, ValueEnum, Serialize, Copy, Default)]
pub enum LogFormat {
    Text,
    Json,
    #[default]
    Default,
    Otel,
}

impl Display for LogFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text => write!(f, "text"),
            Self::Json => write!(f, "json"),
            Self::Default => write!(f, "default"),
            Self::Otel => write!(f, "otel"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, ValueEnum, Serialize, Copy, Default)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Off,
    Error,
    Warn,
    #[default]
    Info,
    Debug,
    Trace,
}

impl Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Off => write!(f, "OFF"),
            Self::Error => write!(f, "ERROR"),
            Self::Warn => write!(f, "WARN"),
            Self::Info => write!(f, "INFO"),
            Self::Debug => write!(f, "DEBUG"),
            Self::Trace => write!(f, "TRACE"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FsCommand {
    /// Special value indicating no command was provided.
    /// Used in places where a command is optional to avoid using Option<>
    #[default]
    Unset,
    /// Standard dbt commands
    Init,
    Deps,
    Parse,
    List, // aka: Ls
    Compile,
    Run,
    RunOperation,
    Test,
    Seed,
    Snapshot,
    Show,
    Build,
    Clean,
    Source,
    Clone,
    System,
    Man,
    Debug,
    Retry,
    Docs,
    Completions,
    /// All other commands provided by private cli's
    Extension(&'static str),
}

impl FsCommand {
    pub const fn as_str(&self) -> &'static str {
        match self {
            FsCommand::Unset => "",
            FsCommand::Init => "init",
            FsCommand::Deps => "deps",
            FsCommand::Parse => "parse",
            FsCommand::List => "list",
            FsCommand::Compile => "compile",
            FsCommand::Run => "run",
            FsCommand::RunOperation => "run-operation",
            FsCommand::Test => "test",
            FsCommand::Seed => "seed",
            FsCommand::Snapshot => "snapshot",
            FsCommand::Show => "show",
            FsCommand::Build => "build",
            FsCommand::Clean => "clean",
            FsCommand::Source => "freshness",
            FsCommand::Clone => "clone",
            FsCommand::System => "system",
            FsCommand::Man => "man",
            FsCommand::Debug => "debug",
            FsCommand::Retry => "retry",
            FsCommand::Docs => "docs",
            FsCommand::Completions => "completions",
            FsCommand::Extension(s) => s,
        }
    }
}

// ----------------------------------------------------------------------------------------------
// IO Args
#[derive(Default, Clone)]
pub struct IoArgs {
    pub invocation_id: uuid::Uuid,
    pub otel_parent_span_id: Option<u64>,
    pub show: HashSet<ShowOptions>,
    pub is_compile: bool,
    pub in_dir: PathBuf,
    pub out_dir: PathBuf,
    /// Root directory for sidecar/DuckDB state. Defaults to out_dir if not set.
    /// Structure: {db_root}/db/state/{catalog}.db (persistent)
    ///            {db_root}/db/sessions/{session-id}/... (ephemeral)
    pub db_root: Option<PathBuf>,
    pub log_path: Option<PathBuf>,
    pub otel_file_name: Option<String>,
    pub otel_parquet_file_name: Option<String>,
    pub export_to_otlp: bool,
    pub log_format: LogFormat,
    pub log_level: Option<LogLevel>,
    pub log_level_file: Option<LogLevel>,
    pub log_file_max_bytes: u64,
    pub debug: bool,

    // Flags influencing error/warning behavior
    pub show_all_deprecations: bool,

    // Flag for deps to use Fusion-compatible downloads from Package Hub
    pub use_v2_compatible_package_downloads: bool,

    /// Optional status reporter for reporting status messages during execution
    pub status_reporter: Option<Arc<dyn StatusReporter>>,
    pub send_anonymous_usage_stats: bool,

    // internal fields
    pub show_timings: bool, // whether to show timings in the status messages
    pub build_cache_url: Option<String>,
    pub build_cache_cas_url: Option<String>,
    pub build_cache_mode: Option<BuildCacheMode>,
    pub beta_use_query_cache: bool,
    pub host: String,
    pub port: u16,
}
impl IoArgs {
    pub fn is_generated_file(&self, rel_path: &Path) -> bool {
        // Get last component of out_dir (as_os_str returns None if out_dir is empty)
        let out_dir_last = self.out_dir.components().next_back();
        let rel_first = rel_path.components().next();
        out_dir_last == rel_first
    }

    // -----------------------------------------------------------------------------------------
    // Sidecar/DuckDB path helpers
    // -----------------------------------------------------------------------------------------

    /// Returns the db_root, defaulting to out_dir if not explicitly set.
    pub fn db_root(&self) -> &Path {
        self.db_root.as_deref().unwrap_or(&self.out_dir)
    }

    /// Path to persistent DuckDB state directory: {db_root}/db/state/
    pub fn db_state_dir(&self) -> PathBuf {
        self.db_root().join("db").join("state")
    }

    /// Path to a specific catalog's DuckDB file: {db_root}/db/state/{catalog}.db
    pub fn db_catalog_path(&self, catalog: &str) -> PathBuf {
        self.db_state_dir()
            .join(format!("{}.db", catalog.to_ascii_lowercase()))
    }

    /// Path to session-scoped directory: {db_root}/db/sessions/{invocation_id}/
    pub fn db_session_dir(&self) -> PathBuf {
        self.db_root()
            .join("db")
            .join("sessions")
            .join(self.invocation_id.to_string())
    }

    /// Path to session logs: {db_root}/db/sessions/{invocation_id}/logs/
    pub fn db_session_logs_dir(&self) -> PathBuf {
        self.db_session_dir().join("logs")
    }

    /// Path to session pull data: {db_root}/db/sessions/{invocation_id}/pull/
    pub fn db_session_pull_dir(&self) -> PathBuf {
        self.db_session_dir().join("pull")
    }

    /// Path to session pull data for a specific table:
    /// {db_root}/db/sessions/{invocation_id}/pull/{catalog}/{schema}/{table}/
    pub fn db_session_table_dir(&self, catalog: &str, schema: &str, table: &str) -> PathBuf {
        self.db_session_pull_dir()
            .join(catalog.to_ascii_lowercase())
            .join(schema.to_ascii_lowercase())
            .join(table.to_ascii_lowercase())
    }
}

impl fmt::Debug for IoArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IoArgs")
            .field("invocation_id", &self.invocation_id)
            .field("show", &self.show)
            .field("in_dir", &self.in_dir)
            .field("out_dir", &self.out_dir)
            .field("db_root", &self.db_root)
            .field("log_path", &self.log_path)
            .field("log_file_max_bytes", &self.log_file_max_bytes)
            .field("otel_file_name", &self.otel_file_name)
            .field("status_reporter", &self.status_reporter.is_some())
            .finish()
    }
}

impl IoArgs {
    /// Given a path, returns a string representation of that path that is
    /// suitable for display in terminal status messages.
    pub fn format_display_path(&self, path: &Path) -> String {
        let in_dir = &self.in_dir;
        let out_dir = &self.out_dir;

        if path.starts_with(in_dir)
            && let Some(relative_path) = diff_paths(path, in_dir)
        {
            return relative_path.to_string_lossy().to_string();
        }
        if path.starts_with(out_dir)
            && let Some(relative_path) = diff_paths(path, out_dir)
        {
            return relative_path.to_string_lossy().to_string();
        }
        if path.is_relative() {
            let target_path = in_dir.join(DBT_TARGET_DIR_NAME).join(path);
            if target_path.exists() {
                return format!("target/{}", path.to_string_lossy());
            }
        }

        path.to_string_lossy().to_string()
    }

    /// This function takes an artifact path, which may either be a workspace
    /// resource, or some generated temp location, and returns a path to its
    /// corresponding location in the workspace
    pub fn map_to_workspace_path(&self, path: &Path, resource_type: NodeType) -> PathBuf {
        if resource_type == NodeType::UnitTest || resource_type == NodeType::Snapshot {
            let special_component_idx = path.components().position(|c| {
                c.as_os_str() == DBT_GENERIC_TESTS_DIR_NAME
                    || c.as_os_str() == DBT_SNAPSHOTS_DIR_NAME
            });
            if let Some(idx) = special_component_idx {
                // FIXME: this is really a hack, the proper thing to do is to have a
                // semantic representation for each artifact that can generate workspace or
                // temporary paths
                self.out_dir
                    .join(path.components().skip(idx).collect::<PathBuf>())
            } else {
                self.out_dir.join(path)
            }
        } else {
            self.in_dir.join(path)
        }
    }

    pub fn should_show(&self, option: ShowOptions) -> bool {
        (self.show.contains(&option) || option == ShowOptions::All)
            // TODO: temporary logic to avoid showing skipped nodes for compile.
            // Should be centralized across all commands, progress message types, and options.
            && (option != ShowOptions::Completed
                || !self.is_compile
                || self.debug)
    }

    /// Returns true if the build cache should be used (read or readwrite mode, or --use-build-cache flag).
    pub fn should_use_build_cache(&self) -> bool {
        self.build_cache_mode
            .map(|c| matches!(c, BuildCacheMode::Read | BuildCacheMode::ReadWrite))
            .unwrap_or_default()
    }

    /// Returns true if the build cache should be saved (write or readwrite mode).
    pub fn should_save_build_cache(&self) -> bool {
        self.build_cache_mode
            .map(|c| matches!(c, BuildCacheMode::Write | BuildCacheMode::ReadWrite))
            .unwrap_or_default()
    }
}
// ----------------------------------------------------------------------------------------------
// System Args
#[derive(Clone, Debug)]
pub struct SystemArgs {
    pub command: FsCommand,
    pub io: IoArgs,
    pub from_main: bool,
    pub num_threads: Option<usize>,
    /// Request sequential task execution, decoupled from `num_threads`.
    /// Drives the binary entrypoint's single-worker tokio runtime and the
    /// task-scheduler's sequential visitor.
    pub no_parallel: bool,
    pub target: Option<String>,
}

// ----------------------------------------------------------------------------------------------
// Eval Args
#[derive(Clone, Default)]
pub struct EvalArgs {
    // The command to run
    pub command: FsCommand,
    // io
    pub io: IoArgs,
    // The profile directory to load the profiles from
    pub profiles_dir: Option<PathBuf>,
    // The directory to install packages
    pub packages_install_path: Option<PathBuf>,
    // A package to add to deps
    pub add_package: Option<String>,
    // Upgrade deps
    pub upgrade: bool,
    // Generate lock file only
    pub lock: bool,
    // The profile to use
    pub profile: Option<String>,
    // The target within the profile to use for the dbt run
    pub target: Option<String>,
    // Vars to pass to the jinja environment
    pub vars: BTreeMap<String, Value>,
    // Stop as soon as this stage is reached
    pub phase: Phases,
    // Display rows in different formats
    pub format: DisplayFormat,
    /// Limiting number of shown rows. None means no limit, run with --limit -1 to remove limit
    pub limit: Option<usize>,
    /// called as bin or as library
    pub from_main: bool,
    /// The number of threads to use. Drives the adapter connection backpressure
    /// high-water-mark and parser rendering parallelism. Not used to force
    /// sequential task execution — use `no_parallel` for that.
    pub num_threads: Option<usize>,
    /// Force sequential task execution and sequential parser rendering without
    /// constraining the connection pool. Set by `--no-parallel`.
    pub no_parallel: bool,
    /// yaml selector
    pub selector: Option<String>,
    /// Select nodes to operate on
    pub select: Option<SelectExpression>,
    /// Select nodes to exclude from selected nodes
    pub exclude: Option<SelectExpression>,
    /// Indirect selection mode
    pub indirect_selection: Option<IndirectSelection>,
    /// Show output keys
    pub output_keys: Vec<String>,
    /// Resource types to filter by
    pub resource_types: Vec<ClapResourceType>,
    /// Exclude nodes of a specific type
    pub exclude_resource_types: Vec<ClapResourceType>,
    /// Debug flag
    pub debug: bool,
    /// Set log file format, overriding the default and --log-format setting.
    pub log_format_file: Option<LogFormat>,
    /// Set logging format
    pub log_format: LogFormat,
    /// Set minimum log file severity, overriding the default and --log-level setting.
    pub log_level_file: Option<LogLevel>,
    /// Set minimum severity for console/log file
    pub log_level: Option<LogLevel>,
    /// Set 'log-path' for the current run, overriding 'DBT_LOG_PATH'.
    pub log_path: Option<PathBuf>,
    /// The output directory for all produced assets
    pub target_path: Option<PathBuf>,
    /// The directory to load the dbt project from
    pub project_dir: Option<PathBuf>,
    /// Suppress all non-error logging to stdout
    pub quiet: bool,
    /// Write JSON artifacts to disk
    pub write_json: bool,
    /// Write a catalog.json file to the target directory
    pub write_catalog: bool,
    /// Show schema on the command line
    pub schema: Vec<JsonSchemaTypes>,

    // -- fields from the private branch
    pub internal_packages_install_path: Option<PathBuf>,
    pub update_deps: bool,
    pub replay: Option<ReplayMode>,
    pub static_analysis: Option<StaticAnalysisKind>,
    pub interactive: bool,
    pub skip_semantic_manifest_validation: bool,
    pub export_saved_queries: bool,
    pub task_cache_url: String,
    pub run_cache_mode: RunCacheMode,
    pub optimize_tests: HashSet<OptimizeTestsOptions>,
    pub show_scans: bool,
    pub max_depth: usize,
    pub use_fqtn: bool,
    pub skip_unreferenced_table_check: bool,
    pub state: Option<PathBuf>,
    pub defer_state: Option<PathBuf>,
    pub connection: bool,
    pub macro_name: String,
    pub macro_args: BTreeMap<String, Value>,
    pub warn_error: Option<bool>,
    pub warn_error_options: WarnErrorOptions,
    pub version_check: bool,
    pub introspect: bool,
    pub defer: bool,
    pub fail_fast: bool,
    pub empty: bool,
    pub sample: Option<String>,
    pub full_refresh: bool,
    pub store_failures: bool,
    pub favor_state: bool,
    pub refresh_sources: bool,
    pub send_anonymous_usage_stats: bool,
    pub check_all: bool,
    // todo: temporary, until Sampling is public, maps (source) unique_id to renamed (database, schema, table)
    pub sample_renaming: BTreeMap<String, (String, String, String)>,
    pub local_execution_backend: LocalExecutionBackendKind,
    /// Does not apply to interactive checkpoints.
    pub skip_checkpoints: bool,
    /// Skip installation of private dependencies (useful for build conformance testing)
    pub skip_private_deps: bool,
    /// Override end datetime when generating microbatches
    pub event_time_end: Option<String>,
    /// Override start datetime when generating microbatches
    pub event_time_start: Option<String>,
    /// How to load internal (embedded) dbt packages
    pub internal_package_mode: InternalPackageMode,
    /// Whether to skip running post hook operations.
    pub skip_post_hooks: bool,
    /// Write parquet index alongside JSON artifacts
    pub write_index: bool,
    /// Directory for the parquet index output (default: <target>/index/)
    pub index_dir: Option<PathBuf>,
    /// Whether to skip creating generic tests
    pub skip_creating_generic_tests: bool,
}
impl fmt::Debug for EvalArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EvalArgs")
            .field("in_dir", &self.io.in_dir)
            .field("out_dir", &self.io.out_dir)
            .field("profiles_dir", &self.profiles_dir)
            .field("packages_install_path", &self.packages_install_path)
            .field("target", &self.target)
            .field("vars", &self.vars)
            .field("show", &self.io.show)
            .field("optimize_tests", &self.optimize_tests)
            .field("store_failures", &self.store_failures)
            .field("stage", &self.phase)
            .field("format", &self.format)
            .field("limit", &self.limit)
            .field("invocation_id", &self.io.invocation_id)
            .field("select", &self.select)
            .field("exclude", &self.exclude)
            .field("command", &self.command)
            .field("from_main", &self.from_main)
            .field("num_threads", &self.num_threads)
            .field("output_keys", &self.output_keys)
            .field("indirect_selection", &self.indirect_selection)
            .finish()
    }
}

pub struct EvalArgsBuilder {
    pub args: EvalArgs,
}

impl EvalArgsBuilder {
    pub fn from_eval_args(args: &EvalArgs) -> Self {
        Self { args: args.clone() }
    }
}

impl EvalArgsBuilder {
    /// Configure additional arguments
    pub fn with_additional(
        self,
        target: String,
        threads: Option<usize>,
        adapter_type: AdapterType,
    ) -> Self {
        self.with_target(target)
            .with_threads(threads)
            .disable_static_analysis_if_not_supported(adapter_type)
    }

    fn with_target(mut self, target: String) -> Self {
        // Update span info as it is used in telemetry & TUI
        with_invocation_mut(|invocation| {
            if let Some(args) = invocation.eval_args.as_mut() {
                args.target = Some(target.clone());
            };
        });

        self.args.target = Some(target);
        self
    }

    pub fn with_threads(mut self, num_threads: Option<usize>) -> Self {
        // Update span info as it is used in telemetry & TUI
        with_invocation_mut(|invocation| {
            if let Some(args) = invocation.eval_args.as_mut() {
                args.num_threads = num_threads.map(|l| l as u64);
            };
        });

        self.args.num_threads = num_threads;
        self
    }

    /// Disable the static analysis for a specific adapter if the relevant dialect is unsupported.
    /// Otherwise, it's a noop
    pub fn disable_static_analysis_if_not_supported(mut self, adapter_type: AdapterType) -> Self {
        let supported = dbt_adapter_core::adapter_type_supports_static_analysis(adapter_type);

        // FIXME(serramatutu): there is a bug in Postgres' frontend parser that makes
        // all our recordings invalid if enable it, but we can't disable it otherwise
        // the recordings will break too. This line should be removed when we fix the
        // following for Postgres:
        // dbt1058: Column 'id' in node 'model.test.a' has a type mismatch. Overriding
        // 'int' with 'integer'.
        let skip = adapter_type == AdapterType::Postgres;

        if !supported && !skip {
            #[cfg(debug_assertions)]
            {
                println!(
                    "debug:warning=static analysis for adapter: {:?} is disabled",
                    adapter_type
                );
            }
            self.args.static_analysis = Some(StaticAnalysisKind::Off);
        }

        self
    }

    pub fn with_show_scans(mut self, show_scans: bool) -> Self {
        self.args.show_scans = show_scans;
        self
    }

    pub fn with_max_depth(mut self, max_depth: usize) -> Self {
        self.args.max_depth = max_depth;
        self
    }

    pub fn with_use_fqtn(mut self, use_fqtn: bool) -> Self {
        self.args.use_fqtn = use_fqtn;
        self
    }

    pub fn with_warn_error_options(
        mut self,
        warn_error: bool,
        warn_error_options: WarnErrorOptions,
    ) -> Self {
        self.args.warn_error = Some(warn_error);
        self.args.warn_error_options = warn_error_options;
        self
    }

    pub fn build(self) -> EvalArgs {
        self.args
    }
}

impl EvalArgs {
    // this could accept a SelectExpression in case we want to join more complex selections together.
    pub fn set_refined_node_selectors(mut self, predicate: Option<SelectionCriteria>) -> EvalArgs {
        // Convert SelectionCriteria to SelectExpression::Atom first
        let predicate_expr = predicate.map(SelectExpression::Atom);

        self.select = conjoin_expression(self.select.clone(), predicate_expr.clone());
        if self.exclude.is_some() {
            self.exclude = conjoin_expression(self.exclude.clone(), predicate_expr);
        }

        // Update span info as it is used in telemetry & TUI
        with_invocation_mut(|invocation| {
            if let Some(args) = invocation.eval_args.as_mut() {
                args.select = self.select.iter().map(|s| s.to_string()).collect();
                args.exclude = self.exclude.iter().map(|s| s.to_string()).collect();
            };
        });

        self
    }

    pub fn set_schema(mut self, schema: Vec<JsonSchemaTypes>) -> Self {
        self.schema = schema;
        self
    }

    pub fn set_connection(mut self, connection: bool) -> Self {
        self.connection = connection;
        self
    }
}

// ----------------------------------------------------------------------------------------------
// Enums

#[derive(
    Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Default, ValueEnum, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
#[clap(rename_all = "snake_case")]
pub enum ClapResourceType {
    #[default]
    Model,
    Source,
    Seed,
    Snapshot,
    Test,
    UnitTest,
    Analysis,
    Function,
    SemanticModel,
    Metric,
    SavedQuery,
}

impl Display for ClapResourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ClapResourceType::Model => "model",
            ClapResourceType::Source => "source",
            ClapResourceType::Seed => "seed",
            ClapResourceType::Snapshot => "snapshot",
            ClapResourceType::Test => "test",
            ClapResourceType::UnitTest => "unit_test",
            ClapResourceType::Analysis => "analysis",
            ClapResourceType::Function => "function",
            ClapResourceType::SemanticModel => "semantic_model",
            ClapResourceType::Metric => "metric",
            ClapResourceType::SavedQuery => "saved_query",
        };
        write!(f, "{s}")
    }
}

impl From<&ClapResourceType> for NodeType {
    fn from(value: &ClapResourceType) -> Self {
        match value {
            ClapResourceType::Model => NodeType::Model,
            ClapResourceType::Source => NodeType::Source,
            ClapResourceType::Seed => NodeType::Seed,
            ClapResourceType::Snapshot => NodeType::Snapshot,
            ClapResourceType::Test => NodeType::Test,
            ClapResourceType::UnitTest => NodeType::UnitTest,
            ClapResourceType::Analysis => NodeType::Analysis,
            ClapResourceType::Function => NodeType::Function,
            ClapResourceType::SemanticModel => NodeType::SemanticModel,
            ClapResourceType::Metric => NodeType::Metric,
            ClapResourceType::SavedQuery => NodeType::SavedQuery,
        }
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    PartialOrd,
    Serialize,
    Deserialize,
    Hash,
    Eq,
    Ord,
    ValueEnum,
    Display,
    Default,
)]
#[serde(rename_all = "lowercase")]
#[clap(rename_all = "lowercase")]
pub enum Phases {
    Debug, // dbt debug
    Deps,  // dbt deps
    Parse, // dbt parse
    Format,
    Lint,
    Schedule,
    List, // dbt list
    Freshness,
    JinjaCheck, // dbt jinja-check
    Compile,    // dbt compile
    Show,       // dbt show
    Compare,    // dbt compare
    Sample,     // dbt sample
    Lineage,
    RunOperation,
    #[default]
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, Hash, Eq, Display, EnumIter)]
#[serde(rename_all = "lowercase")]
pub enum JsonSchemaTypes {
    Selector(bool),
    Schema(bool),
    Project(bool),
    Profile(bool),
    #[serde(rename = "dbt_cloud")]
    #[strum(serialize = "dbt_cloud")]
    DbtCloud(bool),
    Packages(bool),
    Dependencies(bool),
    Telemetry(bool),
}

impl JsonSchemaTypes {
    pub fn is_pre(&self) -> bool {
        match self {
            JsonSchemaTypes::Selector(is_pre)
            | JsonSchemaTypes::Schema(is_pre)
            | JsonSchemaTypes::Project(is_pre)
            | JsonSchemaTypes::Profile(is_pre)
            | JsonSchemaTypes::DbtCloud(is_pre)
            | JsonSchemaTypes::Packages(is_pre)
            | JsonSchemaTypes::Dependencies(is_pre)
            | JsonSchemaTypes::Telemetry(is_pre) => *is_pre,
        }
    }

    pub fn get_schema_settings(&self) -> schemars::r#gen::SchemaSettings {
        match self {
            JsonSchemaTypes::Selector(_)
            | JsonSchemaTypes::Schema(_)
            | JsonSchemaTypes::Project(_)
            | JsonSchemaTypes::Profile(_)
            | JsonSchemaTypes::DbtCloud(_)
            | JsonSchemaTypes::Packages(_)
            | JsonSchemaTypes::Dependencies(_) => schemars::r#gen::SchemaSettings::default(),
            JsonSchemaTypes::Telemetry(_) => schemars::r#gen::SchemaSettings::draft07(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ValueEnum)]
#[clap(rename_all = "lowercase")]
pub enum ClapSchemaTypes {
    Selector,
    Schema,
    Project,
    Profile,
    #[value(name = "dbt_cloud")]
    DbtCloud,
    Packages,
    Dependencies,
    Telemetry,
}

impl ClapSchemaTypes {
    pub fn to_json_schema_types(&self, is_pre: bool) -> JsonSchemaTypes {
        match self {
            ClapSchemaTypes::Selector => JsonSchemaTypes::Selector(is_pre),
            ClapSchemaTypes::Schema => JsonSchemaTypes::Schema(is_pre),
            ClapSchemaTypes::Project => JsonSchemaTypes::Project(is_pre),
            ClapSchemaTypes::Profile => JsonSchemaTypes::Profile(is_pre),
            ClapSchemaTypes::DbtCloud => JsonSchemaTypes::DbtCloud(is_pre),
            ClapSchemaTypes::Packages => JsonSchemaTypes::Packages(is_pre),
            ClapSchemaTypes::Dependencies => JsonSchemaTypes::Dependencies(is_pre),
            ClapSchemaTypes::Telemetry => JsonSchemaTypes::Telemetry(is_pre),
        }
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Serialize,
    Deserialize,
    Hash,
    Eq,
    ValueEnum,
    Default,
    EnumIter,
    Display,
)]
#[serde(rename_all = "lowercase")]
#[clap(rename_all = "lowercase")]
pub enum DisplayFormat {
    #[default]
    Table,
    Csv,
    Tsv,
    Json,
    NdJson,
    Yml,
    /// Output nodes as selector strings (e.g. "source:pkg.source_name.table_name")
    Selector,
    /// Output nodes as search names (node.search_name)
    Name,
    /// Output nodes as file paths (node.original_file_path)
    Path,
}

/// Output format for the list command. This is a subset of DisplayFormat
/// that only includes formats supported by the list command.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Serialize,
    Deserialize,
    Hash,
    Eq,
    ValueEnum,
    Default,
    EnumIter,
    strum_macros::IntoStaticStr,
)]
#[strum(serialize_all = "lowercase")]
#[serde(rename_all = "lowercase")]
#[clap(rename_all = "lowercase")]
pub enum ListOutputFormat {
    /// Output nodes as JSON objects with customizable keys
    Json,
    /// Output nodes as selector strings (e.g. "source:pkg.source_name.table_name")
    #[default]
    Selector,
    /// Output nodes as search names (node.search_name)
    Name,
    /// Output nodes as file paths (node.original_file_path)
    Path,
}

impl From<ListOutputFormat> for DisplayFormat {
    fn from(format: ListOutputFormat) -> Self {
        match format {
            ListOutputFormat::Json => DisplayFormat::Json,
            ListOutputFormat::Selector => DisplayFormat::Selector,
            ListOutputFormat::Name => DisplayFormat::Name,
            ListOutputFormat::Path => DisplayFormat::Path,
        }
    }
}

impl TryFrom<DisplayFormat> for ListOutputFormat {
    type Error = ();

    fn try_from(format: DisplayFormat) -> Result<Self, Self::Error> {
        match format {
            DisplayFormat::Json => Ok(ListOutputFormat::Json),
            DisplayFormat::Selector => Ok(ListOutputFormat::Selector),
            DisplayFormat::Name => Ok(ListOutputFormat::Name),
            DisplayFormat::Path => Ok(ListOutputFormat::Path),
            _ => Err(()),
        }
    }
}

impl From<ListOutputFormat> for dbt_telemetry::ListOutputFormat {
    fn from(format: ListOutputFormat) -> Self {
        match format {
            ListOutputFormat::Json => Self::Json,
            ListOutputFormat::Selector => Self::Selector,
            ListOutputFormat::Name => Self::Name,
            ListOutputFormat::Path => Self::Path,
        }
    }
}

impl ListOutputFormat {
    pub fn supported_formats() -> impl Iterator<Item = &'static str> {
        use strum::IntoEnumIterator;

        Self::iter().map(|f| f.into())
    }

    pub fn supported_formats_display() -> String {
        Self::supported_formats().collect::<Vec<_>>().join(", ")
    }
}

#[derive(Debug, Clone)]
pub enum ReplayMode {
    /// Replay recordings generated from Mantle
    MantleReplay(PathBuf),
    /// Make recordings at the driver level
    FsRecord(PathBuf),
    /// Replay at the driver level
    FsReplay(PathBuf),
    /// Time Machine mode for cross-version compatibility testing
    FsTimeMachine(TimeMachineMode),
}

impl ReplayMode {
    /// Returns the time machine mode if this is a TimeMachine variant.
    pub fn as_time_machine(&self) -> Option<&TimeMachineMode> {
        match self {
            ReplayMode::FsTimeMachine(mode) => Some(mode),
            _ => None,
        }
    }

    /// Returns true when replaying a Time Machine recording.
    pub fn is_time_machine_replay(&self) -> bool {
        matches!(self, ReplayMode::FsTimeMachine(TimeMachineMode::Replay(_)))
    }
}

// -----------------------------------------------------------------------------
// Time Machine Args
// -----------------------------------------------------------------------------

/// Time Machine operating mode for cross-version compatibility testing.
///
/// This system records adapter-level behavior during a run and can replay
/// those recordings against newer versions to detect breaking changes.
#[derive(Debug, Clone)]
pub enum TimeMachineMode {
    /// Record adapter calls to a directory for later replay
    Record(TimeMachineRecordConfig),
    /// Replay adapter calls from a recorded artifact
    Replay(TimeMachineReplayConfig),
}

/// Configuration for Time Machine record mode.
#[derive(Debug, Clone)]
pub struct TimeMachineRecordConfig {
    /// Output directory for the recording.
    /// The CLI defaults this to `<project_target_dir>/time_machine`.
    pub output_path: PathBuf,
    /// The invocation ID for this run (used as the subdirectory name).
    pub invocation_id: uuid::Uuid,
    /// The invocation command that was executed (e.g., "dbt build --select ...")
    /// Stored in header.json for disambiguation when multiple recordings exist.
    pub invocation_command: Option<String>,
}

/// Configuration for Time Machine replay mode.
#[derive(Debug, Clone, Default)]
pub struct TimeMachineReplayConfig {
    /// Path to the recorded artifact directory
    pub artifact_path: PathBuf,
    /// Replay ordering mode (strict vs semantic)
    pub ordering: TimeMachineReplayOrdering,
}

/// Replay ordering mode for Time Machine.
///
/// Controls how recorded events are matched during replay.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum, Display, Serialize, Deserialize,
)]
#[serde(rename_all = "lowercase")]
#[clap(rename_all = "lowercase")]
pub enum TimeMachineReplayOrdering {
    /// Events must match in exact recorded sequence order.
    /// This is the most restrictive mode and ensures deterministic replay.
    #[default]
    Strict,

    /// Events are matched based on semantic constraints.
    /// Write operations must match in sequence
    /// Read operations between writes can match flexibly
    /// This mode tolerates minor ordering variations between code versions.
    Semantic,
}

/// Parse TIME_MACHINE_MODE from string (for env var parsing).
impl FromStr for TimeMachineModeKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "record" => Ok(TimeMachineModeKind::Record),
            "replay" => Ok(TimeMachineModeKind::Replay),
            "off" | "" => Ok(TimeMachineModeKind::Off),
            _ => Err(format!(
                "Invalid TIME_MACHINE_MODE: '{}'. Expected: record, replay, or off",
                s
            )),
        }
    }
}

/// Simple enum for CLI/env var parsing (the full config is built from multiple args).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum, Display, Serialize, Deserialize,
)]
#[serde(rename_all = "lowercase")]
#[clap(rename_all = "lowercase")]
pub enum TimeMachineModeKind {
    #[default]
    Off,
    Record,
    Replay,
}

#[derive(
    Debug,
    Copy,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Default,
    ValueEnum,
    Display,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "lowercase")]
#[clap(rename_all = "lowercase")]
pub enum Runtime {
    #[default]
    Local,
    Remote,
}

#[derive(
    Debug,
    Copy,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Default,
    ValueEnum,
    Display,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[serde(rename_all = "lowercase")]
#[clap(rename_all = "lowercase")]
pub enum StaticAnalysisKind {
    #[value(hide = true)]
    Unsafe,
    Off,
    Strict,
    #[default]
    Baseline,
    #[value(hide = true)]
    On,
}

#[derive(
    Debug,
    Copy,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    ValueEnum,
    Display,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[serde(rename_all = "lowercase")]
#[clap(rename_all = "lowercase")]
pub enum StaticAnalysisOffReason {
    ConfiguredOff,
    UnableToFetchSchema,
    NoDownstream,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum BuildCacheMode {
    Read,
    Write,
    #[default]
    ReadWrite,
}

impl FromStr for StaticAnalysisKind {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "unsafe" => Ok(StaticAnalysisKind::Unsafe),
            "off" => Ok(StaticAnalysisKind::Off),
            "baseline" => Ok(StaticAnalysisKind::Baseline),
            "strict" => Ok(StaticAnalysisKind::Strict),
            "on" => Ok(StaticAnalysisKind::On),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Debug, Display, Serialize, Deserialize, ValueEnum, Default)]
pub enum RunCacheMode {
    #[default]
    Noop,
    ReadWrite,
    WriteOnly,
}

impl RunCacheMode {
    pub fn use_cache(&self) -> bool {
        match self {
            RunCacheMode::ReadWrite => true,
            RunCacheMode::WriteOnly => false,
            RunCacheMode::Noop => false,
        }
    }

    pub fn write_cache(&self) -> bool {
        matches!(self, RunCacheMode::ReadWrite | RunCacheMode::WriteOnly)
    }
}

impl FromStr for RunCacheMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "noop" => Ok(RunCacheMode::Noop),
            "read-write" => Ok(RunCacheMode::ReadWrite),
            "write-only" => Ok(RunCacheMode::WriteOnly),
            _ => Err(format!("Invalid RunCacheMode: {s}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ValueEnum, EnumIter)]
#[serde(rename_all = "lowercase")]
#[clap(rename_all = "lowercase")]
pub enum OptimizeTestsOptions {
    TestAggregation,
    TestReuse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ValueEnum, EnumIter)]
#[serde(rename_all = "lowercase")]
#[clap(rename_all = "lowercase")]
pub enum ShowOptions {
    Progress,
    ProgressHydrate,
    ProgressParse,
    ProgressRender,
    ProgressAnalyze,
    ProgressRun,
    Completed,
    InputFiles,
    Manifest,
    Schedule,
    Nodes,
    Instructions,
    SourcedSchemas,
    Schema,
    Data,
    Verdict,
    Stats,
    Lineage,
    All,
    None,
    // hidden internal-only:
    RawLineage,
    TaskGraph,
}

#[derive(ValueEnum, Clone, Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PersistTarget {
    #[default]
    Warehouse,
    Local,
}
impl Display for PersistTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            PersistTarget::Warehouse => "warehouse",
            PersistTarget::Local => "local",
        };
        write!(f, "{s}")
    }
}
// ----------------------------------------------------------------------------------------------
pub fn check_selector(selector: &str) -> Result<String, String> {
    // Parity with dbt-core: a delimiter-only value (e.g. "," or ",,") must not
    // hard-error at CLI-parse time. Accept it here; parse_model_specifiers
    // handles the assembled list — either skipping the token when real selectors
    // are present, or synthesizing a literal no-match criterion when every
    // token is delimiter-only (runtime then emits dbt1092 + dbt1601).
    if selector.chars().all(|c| c == ',' || c.is_whitespace()) {
        return Ok(selector.to_string());
    }
    let query = vec![selector.to_string()];
    match parse_model_specifiers(&query) {
        Ok(_) => Ok(selector.to_string()),
        Err(e) => Err(e.pretty()),
    }
}

pub fn check_target(filename: &str) -> Result<String, String> {
    let path = Path::new(filename);
    let err = Err(format!(
        "Input file '{filename}' must have .sql, or .yml extension"
    ));
    // TODO check that this test is universal for all inputs...
    if path.is_dir() {
        Ok(filename.to_owned())
    } else if path.is_file() {
        match path.extension().and_then(|ext| ext.to_str()) {
            Some("yml") | Some("sql") => Ok(filename.to_owned()),
            Some(_) => err,
            None => err,
        }
    } else {
        err
    }
}

pub fn check_key_value_cli_arg(value: &str) -> Result<BTreeMap<String, Value>, String> {
    // Handle empty input
    if value.trim().is_empty() {
        return Err("Empty input is not valid".into());
    }

    // Strip outer quotes if present
    let vars = value.trim().trim_matches('\'');

    // Try parsing as YAML first. Both brace-wrapped flow style ({ key: value, ... })
    // and bare block style (key1: value1\nkey2: value2) are valid YAML mappings.
    // We rely on the YAML parser itself to reject non-mapping inputs (bare strings,
    // lists, malformed syntax).
    let yaml_str = vars.to_string();

    match dbt_yaml::from_str::<BTreeMap<String, Value>>(&yaml_str) {
        Ok(btree) => {
            // Disallow the '{key:value}' format for flow-style YAML syntax
            // to prevent key:value: None interpretation: https://stackoverflow.com/a/70909331
            for key in btree.keys() {
                if key.contains(':') {
                    return Err(format!(
                        "Invalid key-value pair: '{key}'. Value must start with a space after colon."
                    ));
                }
            }
            Ok(btree)
        }
        Err(_) => {
            // If YAML parsing fails, try JSON
            match serde_json::from_str(&yaml_str) {
                Ok(btree) => Ok(btree),
                Err(_) => Err(
                    "Invalid YAML/JSON format. Expected format: 'key: value' or '{key: value, ..}'. Note both argument forms must be just one shell token"
                        .to_string(),
                ),
            }
        }
    }
}

pub fn check_env_var(vars: &str) -> Result<HashMap<String, String>, String> {
    let config = vars;
    if config.starts_with('{') {
        let yaml_hashmap: Result<HashMap<String, String>, dbt_yaml::Error> =
            dbt_yaml::from_str(config);

        match yaml_hashmap {
            Ok(x) => Ok(x),
            Err(err) => Err(err.to_string()),
        }
    } else {
        let path = Path::new(config);
        if path.is_file() {
            if path.extension().unwrap() == "yml" {
                match fs::read_to_string(path) {
                    Ok(yaml_data) => {
                        let yaml_hashmap: Result<HashMap<String, String>, dbt_yaml::Error> =
                            dbt_yaml::from_str(&yaml_data);

                        match yaml_hashmap {
                            Ok(x) => Ok(x),
                            Err(err) => Err(err.to_string()),
                        }
                    }
                    Err(err) => Err(err.to_string()),
                }
            } else {
                Err("File must have a .yml extension".into())
            }
        } else {
            Err("Value must be a .yml file or a yml string like so: '{ dialect: trino }'".into())
        }
    }
}

pub fn validate_project_name(name: &str) -> Result<String, String> {
    // Check if the name contains only letters, digits, and underscores
    if name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        Ok(name.to_string())
    } else {
        Err(format!(
            "{name} is not a valid project name. Only letters, digits and underscore are valid characters in a project name."
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_single_var() {
        let result = check_key_value_cli_arg("key: value").unwrap();
        let expected_result =
            BTreeMap::from([("key".to_string(), dbt_yaml::from_str("value").unwrap())]);

        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_check_single_bracket_var() {
        let result = check_key_value_cli_arg("{key: value}").unwrap();
        let expected_result =
            BTreeMap::from([("key".to_string(), dbt_yaml::from_str("value").unwrap())]);

        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_check_multiple_bracket_var() {
        let result = check_key_value_cli_arg("{key: value, key2: value2}").unwrap();
        let expected_result = BTreeMap::from([
            ("key".to_string(), dbt_yaml::from_str("value").unwrap()),
            ("key2".to_string(), dbt_yaml::from_str("value2").unwrap()),
        ]);

        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_check_var_invalid() {
        let invalid_vars = vec![
            "key",         // Missing colon — YAML returns scalar string, not dict
            "key:value",   // No space after colon — YAML returns scalar string, not dict
            "{key:value}", // Flow-style without space — key-contains-colon guard catches it
        ];

        for var in invalid_vars {
            assert!(
                check_key_value_cli_arg(var).is_err(),
                "Should have failed: {var}"
            );
        }
    }

    #[test]
    fn test_check_var_block_yaml_multikey() {
        // The primary bug fix for issue #402: multi-key block YAML without surrounding braces
        let result = check_key_value_cli_arg("key1: value1\nkey2: value2").unwrap();
        let expected = BTreeMap::from([
            ("key1".to_string(), dbt_yaml::from_str("value1").unwrap()),
            ("key2".to_string(), dbt_yaml::from_str("value2").unwrap()),
        ]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_check_var_block_yaml_three_keys() {
        let result = check_key_value_cli_arg("a: 1\nb: 2\nc: 3").unwrap();
        assert_eq!(result.len(), 3);
        assert!(result.contains_key("a"));
        assert!(result.contains_key("b"));
        assert!(result.contains_key("c"));
    }

    #[test]
    fn test_check_var_value_with_colons() {
        // Values containing colons are valid YAML (and valid in dbt-core / PyYAML).
        // The colon-count pre-check that was removed in the fix for issue #402
        // incorrectly rejected these.
        let result = check_key_value_cli_arg("key: value:with:colons").unwrap();
        let expected = BTreeMap::from([(
            "key".to_string(),
            dbt_yaml::from_str("value:with:colons").unwrap(),
        )]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_validate_project_name_valid() {
        let valid_names = vec![
            "my_project",
            "project123",
            "Project_Name",
            "test_project_1",
            "a",
            "project_with_underscores_and_numbers123",
        ];

        for name in valid_names {
            assert_eq!(validate_project_name(name).unwrap(), name);
        }
    }

    #[test]
    fn test_validate_project_name_invalid() {
        let invalid_names = vec![
            "my-cool-project",      // Contains hyphen
            "project with spaces",  // Contains spaces
            "project.with.dots",    // Contains dots
            "project/with/slashes", // Contains slashes
            "project@symbol",       // Contains @ symbol
            "project#hash",         // Contains # symbol
        ];

        for name in invalid_names {
            let result = validate_project_name(name);
            assert!(result.is_err(), "Should have failed: {name}");
            assert_eq!(
                result.unwrap_err(),
                format!(
                    "{name} is not a valid project name. Only letters, digits and underscore are valid characters in a project name."
                )
            );
        }
    }

    // Parity with dbt-core on delimiter-only --select values: the CLI validator
    // must not hard-error on a per-value token like "," or ",,". Downstream
    // selection resolves this to zero matches + dbt1601 "Nothing to do".
    #[test]
    fn test_check_selector_allows_comma_only() {
        let result = check_selector(",,");
        assert!(
            result.is_ok(),
            "check_selector(\",,\") must be Ok to match dbt-core behavior, got: {result:?}"
        );
    }

    // Regression pin: normal selectors must keep passing the validator unchanged.
    #[test]
    fn test_check_selector_preserves_normal_selector() {
        let result = check_selector("tag:foo");
        assert!(
            result.is_ok(),
            "check_selector(\"tag:foo\") must remain Ok, got: {result:?}"
        );
    }
}
