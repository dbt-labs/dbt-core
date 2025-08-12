use clap::{ArgAction, builder::BoolishValueParser};
use console::Style;
use dbt_common::logging::LogFormat;
use dbt_serde_yaml::Value;
use log::LevelFilter;
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;
use std::{
    collections::{BTreeMap, HashSet},
    path::{Path, PathBuf},
};
use strum::IntoEnumIterator;

use dbt_common::io_args::{
    ClapResourceType, DisplayFormat, EvalArgs, IoArgs, JsonSchemaTypes, Phases, ShowOptions,
    SystemArgs, check_selector, check_var,
};
use dbt_common::row_limit::RowLimit;

use clap::arg;
use clap::{Parser, Subcommand};

use dbt_common::node_selector::{IndirectSelection, parse_model_specifiers};

const DEFAULT_LIMIT: &str = "10";
static DEFAULT_FORMAT: LazyLock<String> = LazyLock::new(|| DisplayFormat::Table.to_string());

// defined in pretty string, but copied here to avoid cycle...
static BOLD: LazyLock<Style> = LazyLock::new(|| Style::new().bold());

// ----------------------------------------------------------------------------------------------
// Cli and its subcommands

static ABOUT: LazyLock<String> = LazyLock::new(|| {
    format!(
        "dbt-sa-cli {}: A fast, source-available dbt runner",
        env!("CARGO_PKG_VERSION")
    )
});
static AFTER_HELP: LazyLock<String> = LazyLock::new(|| {
    format!(
        "{}",
        BOLD.apply_to(
            "Use `dbt-sa-cli <COMMAND> --help` to learn more about the options for each command."
        )
    )
});

#[derive(Parser, Debug, Clone)]
#[command(
    author,
    name = "dbt-sa-cli",
    version = env!("CARGO_PKG_VERSION"),
    about = &**ABOUT,
    after_help = &**AFTER_HELP
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    #[command(flatten)]
    pub common_args: CommonArgs,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Commands {
    /// Initialize a new dbt project
    Init(InitArgs),

    /// Install package dependencies
    Deps(DepsArgs),

    /// Parse models
    Parse(ParseArgs),

    /// List selected nodes
    List(ListArgs),

    /// List selected nodes (alias for list)
    Ls(ListArgs),

    /// Remove target directories
    Clean(CleanArgs),

    /// Create reference documentation (json schema for artifacts)
    Man(ManArgs),
}

// ----------------------------------------------------------------------------------------------
// Command Args
#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct InitArgs {
    /// The name of the project to create
    #[arg(long, default_value = "jaffle_shop")]
    pub project_name: String,

    /// Skip interactive profile setup
    #[arg(long, default_value = "false")]
    pub skip_profile_setup: bool,

    // Flattened Common args
    #[clap(flatten)]
    pub common_args: CommonArgs,
}

#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct DepsArgs {
    #[arg(long)]
    pub add_package: Option<String>,

    // Flattened Common args
    #[clap(flatten)]
    pub common_args: CommonArgs,
}

#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct ParseArgs {
    // Flattened Common args
    #[clap(flatten)]
    pub common_args: CommonArgs,
}

#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct ListArgs {
    // Flattened Common args
    #[clap(flatten)]
    pub common_args: CommonArgs,

    /// Limiting number of shown rows. Run with --limit -1 to remove limit [default: 10]
    #[arg(long, default_value=DEFAULT_LIMIT, allow_hyphen_values = true, hide = true)]
    pub limit: RowLimit,

    /// Display rows in different formats. Supports table, json, selector, name, and path formats.
    #[arg(global = true, long, aliases = ["format"])]
    pub output: Option<DisplayFormat>,

    /// Space-separated node properties to include as JSON keys (e.g. --output-keys name type desc)
    #[arg(long, num_args(1..), value_delimiter = ' ')]
    pub output_keys: Vec<String>,

    /// Select nodes of a specific type;
    #[arg(long)]
    pub resource_type: Option<ClapResourceType>,

    /// Exclude nodes of a specific type;
    #[arg(long)]
    pub exclude_resource_type: Option<ClapResourceType>,
}

#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct CleanArgs {
    /// Clean the target directory specified by file or --target-path
    #[arg(value_parser = check_target)]
    pub files: Vec<String>,

    // Flattened Common args
    #[clap(flatten)]
    pub common_args: CommonArgs,
}

#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct ManArgs {
    // Flattened IO args
    #[clap(flatten)]
    pub common_args: CommonArgs,

    /// Show these json schema types on the command line
    #[clap(long, num_args(0..))]
    pub schema: Vec<JsonSchemaTypes>,
}

// ----------------------------------------------------------------------------------------------
// Commmon Command Args
#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct CommonArgs {
    /// The target to execute
    //  has no ENV_VAR euivalent
    #[arg(global = true, long, short = 't')]
    pub target: Option<String>,

    /// The directory to load the dbt project from
    #[arg(global = true, long, env = "DBT_PROJECT_DIR")]
    pub project_dir: Option<PathBuf>,

    /// The profile to use
    #[arg(global = true, long, env = "DBT_PROFILE")]
    pub profile: Option<String>,

    /// The directory to load the profiles from
    #[arg(global = true, long, env = "DBT_PROFILES_DIR")]
    pub profiles_dir: Option<PathBuf>,

    /// The directory to install packages
    #[arg(global = true, long, env = "DBT_PACKAGES_INSTALL_PATH")]
    pub packages_install_path: Option<PathBuf>,

    /// The output directory for all produced assets
    #[arg(global = true, long, env = "DBT_TARGET_PATH")]
    pub target_path: Option<PathBuf>,

    /// Supply var bindings in yml format e.g. '{key: value}' or as separate key: value pairs
    // has no ENV_VAR
    #[arg(global = true, long,value_parser = check_var, )]
    pub vars: Option<BTreeMap<String, Value>>,

    /// Select nodes to run
    // has no ENV_VAR
    #[arg(global = true, long, short = 's', value_parser = check_selector, num_args(1..), value_delimiter = ' ', group = "selector_or_select")]
    pub select: Option<Vec<String>>,

    /// Select nodes to exclude
    // has no ENV_VAR
    #[arg(global = true, long, value_parser = check_selector, num_args(1..), value_delimiter = ' ')]
    pub exclude: Option<Vec<String>>,

    /// The name of the yml defined selector to use
    #[arg(global = true, long, group = "selector_or_select")]
    pub selector: Option<String>,

    /// Choose which tests to select adjacent to resources: eager (most inclusive), cautious (most exclusive), buildable (inbetween) or empty.
    #[arg(global = true, long, env = "DBT_INDIRECT_SELECTION")]
    pub indirect_selection: Option<IndirectSelection>,

    /// Suppress all non-error logging to stdout. Does not affect {{ print() }} macro calls.
    #[arg(global = true, long, env = "DBT_QUIET", short = 'q')]
    pub quiet: bool,

    /// The number of threads to use [Run with --threads 0 to use max_cpu [default: max_cpu]]
    // has no ENV_VAR, but can be set in profiles.yml
    #[arg(global = true, long)]
    pub threads: Option<usize>,

    /// Overrides threads.
    #[arg(global = true, long = "single-threaded", action = ArgAction::SetTrue, env = "DBT_SINGLE_THREADED", value_parser = BoolishValueParser::new())]
    pub single_threaded: bool,

    /// Write JSON artifacts to disk [env: DBT_WRITE_JSON=]. Use --no-write-json to suppress writing JSON artifacts.
    #[arg(global = true, long,  default_value_t=true,  action = ArgAction::SetTrue, env = "DBT_WRITE_JSON", value_parser = BoolishValueParser::new())]
    pub write_json: bool,
    #[arg(global = true,long,action = ArgAction::SetTrue,  default_value_t=false, value_parser = BoolishValueParser::new(),hide = true)]
    pub no_write_json: bool,

    /// Set 'log-path' for the current run, overriding 'DBT_LOG_PATH'.
    #[arg(global = true, long, env = "DBT_LOG_PATH")]
    pub log_path: Option<PathBuf>,

    /// Set 'otm_file_name' for the current run, overriding 'DBT_OTM_FILE_NAME'.
    /// If set, OTEL telemetry will be written to `$log_path/otm_file_name`.
    #[arg(global = true, long, env = "DBT_OTM_FILE_NAME", hide = true)]
    pub otm_file_name: Option<String>,

    /// Set logging format; use --log-format-file to override.
    #[arg(global = true, long, env = "DBT_LOG_FORMAT", default_value_t = LogFormat::Text,)]
    pub log_format: LogFormat,

    /// Set log file format, overriding the default and --log-format setting.
    #[arg(global = true, long, env = "DBT_LOG_FORMAT_FILE")]
    pub log_format_file: Option<LogFormat>,

    /// Set minimum severity for console/log file; use --log-level-file to set log file severity separately.
    #[arg(global = true, long, env = "DBT_LOG_LEVEL")]
    pub log_level: Option<LevelFilter>,
    /// Set minimum log file severity, overriding the default and --log-level setting.
    #[arg(global = true, long, env = "DBT_LOG_LEVEL_FILE")]
    pub log_level_file: Option<LevelFilter>,

    // Send anonymous usage stats to dbt Labs.
    #[arg(global = true, long, default_value_t=true, action = ArgAction::SetTrue, env = "DBT_SEND_ANONYMOUS_USAGE_STATS", value_parser = BoolishValueParser::new())]
    pub send_anonymous_usage_stats: bool,
    #[arg(global = true, long, default_value_t=false, action = ArgAction::SetTrue, value_parser = BoolishValueParser::new())]
    pub no_send_anonymous_usage_stats: bool,

    // TODO: currently only used to avoid suppressing warnings/errors from dependencies
    /// Show all deprecations warnings/errors instead of one per package
    #[arg(global = true, long, default_value = "false", action = ArgAction::SetTrue, env = "DBT_SHOW_ALL_DEPRECATIONS",hide = true, value_parser = BoolishValueParser::new())]
    pub show_all_deprecations: bool,

    /// Debug flag
    #[arg(global = true, long, short = 'd', default_value = "false", action = ArgAction::SetTrue,  env = "DBT_DEBUG", value_parser = BoolishValueParser::new(),hide = true)]
    pub debug: bool,

    /// Show produced artifacts [default: 'progress']
    #[clap(long, num_args(0..), help = "Show produced artifacts [default: 'progress']")]
    pub show: Vec<ShowOptions>,
}

// ------------------------------------------------------------------------------------------------
// Arg processing
impl Cli {
    pub fn to_eval_args(
        &self,
        arg: SystemArgs,
        in_dir: &Path,
        out_dir: &Path,
        from_main: bool,
    ) -> EvalArgs {
        let mut arg = match &self.command {
            Commands::Init(args) => args.to_eval_args(arg, in_dir, out_dir),
            Commands::Deps(args) => args.to_eval_args(arg, in_dir, out_dir),
            Commands::List(args) => args.to_eval_args(arg, in_dir, out_dir),
            Commands::Parse(args) => args.to_eval_args(arg, in_dir, out_dir),
            Commands::Ls(args) => args.to_eval_args(arg, in_dir, out_dir),
            Commands::Clean(args) => args.to_eval_args(arg, in_dir, out_dir),
            Commands::Man(args) => args.to_eval_args(arg, in_dir, out_dir),
        };
        arg.from_main = from_main;
        arg
    }

    pub fn common_args(&self) -> CommonArgs {
        match &self.command {
            Commands::Init(args) => args.common_args.clone(),
            Commands::Deps(args) => args.common_args.clone(),
            Commands::List(args) => args.common_args.clone(),
            Commands::Ls(args) => args.common_args.clone(),
            Commands::Parse(args) => args.common_args.clone(),
            Commands::Clean(args) => args.common_args.clone(),
            Commands::Man(args) => args.common_args.clone(),
        }
    }

    pub fn project_dir(&self) -> Option<PathBuf> {
        self.common_args().project_dir
    }

    pub fn target_path(&self) -> Option<PathBuf> {
        self.common_args().target_path
    }

    pub fn get_command_str(&self) -> &str {
        // generate the command string
        match &self.command {
            Commands::Init(..) => "init",
            Commands::Deps(..) => "deps",
            Commands::Parse(..) => "parse",
            Commands::List(..) => "list",
            Commands::Ls(..) => "ls",
            Commands::Clean(..) => "clean",
            Commands::Man(..) => "man",
        }
    }
}

impl DepsArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let mut eval_args = self.common_args.to_eval_args(arg, in_dir, out_dir);
        eval_args.phase = Phases::Deps;
        eval_args
    }
}

impl CleanArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        self.common_args.to_eval_args(arg, in_dir, out_dir)
    }
}

impl ParseArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let mut eval_args = self.common_args.to_eval_args(arg, in_dir, out_dir);
        eval_args.phase = Phases::Parse;
        eval_args
    }
}

impl ListArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let mut eval_args = self.common_args.to_eval_args(arg, in_dir, out_dir);
        eval_args.phase = Phases::List;
        eval_args.io.show.insert(ShowOptions::Nodes);
        eval_args.output_keys = self.output_keys.clone();
        if let Some(resource_type) = self.resource_type {
            eval_args.resource_types = vec![resource_type];
        }
        if let Some(exclude_resource_type) = self.exclude_resource_type {
            eval_args.exclude_resource_types = vec![exclude_resource_type];
        }
        eval_args.limit = self.limit.into();
        if let Some(output) = &self.output {
            eval_args.format = output.to_string();
        } else {
            eval_args.format = DEFAULT_FORMAT.clone();
        }
        eval_args
    }
}

impl ManArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let eval_args = self.common_args.to_eval_args(arg, in_dir, out_dir);
        eval_args.with_schema(self.schema.clone())
    }
}
impl InitArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let show = if arg.io.show.contains(&ShowOptions::All) {
            ShowOptions::iter().collect()
        } else if arg.io.show.is_empty() {
            HashSet::from([ShowOptions::Progress])
        } else {
            arg.io.show.iter().cloned().collect()
        };
        EvalArgs {
            command: arg.command.clone(),
            from_main: arg.from_main,
            io: IoArgs {
                in_dir: in_dir.to_path_buf(),
                out_dir: out_dir.to_path_buf(),
                show,
                invocation_id: arg.io.invocation_id,
                send_anonymous_usage_stats: self.common_args.send_anonymous_usage_stats,
                status_reporter: arg.io.status_reporter.clone(),
                log_format: self.common_args.log_format,
                log_level: self.common_args.log_level,
                log_level_file: self.common_args.log_level_file,
                log_path: self.common_args.log_path.clone(),
                otm_file_name: self.common_args.otm_file_name.clone(),
                #[cfg(all(debug_assertions, feature = "otlp"))]
                export_to_otlp: false,
                show_all_deprecations: self.common_args.show_all_deprecations,
                show_timings: arg.from_main,
                build_cache_mode: arg.io.build_cache_mode,
                build_cache_url: arg.io.build_cache_url,
                build_cache_cas_url: arg.io.build_cache_cas_url,
            },
            ..Default::default()
        }
    }
}

// ----------------------------------------------------------------------------------------------
// check options

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

impl CommonArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let mut show = if self.show.contains(&ShowOptions::All) {
            ShowOptions::iter().collect()
        } else if self.show.contains(&ShowOptions::None) {
            HashSet::new()
        } else if self.show.is_empty() {
            HashSet::from_iter(vec![
                ShowOptions::Progress,
                ShowOptions::ProgressParse,
                ShowOptions::ProgressRender,
                ShowOptions::ProgressAnalyze,
                ShowOptions::ProgressRun,
            ])
        } else {
            self.show
                .iter()
                .cloned()
                .flat_map(|opt| {
                    if opt == ShowOptions::Progress {
                        vec![
                            ShowOptions::Progress,
                            ShowOptions::ProgressParse,
                            ShowOptions::ProgressRender,
                            ShowOptions::ProgressAnalyze,
                            ShowOptions::ProgressRun,
                        ]
                    } else {
                        vec![opt]
                    }
                })
                .collect()
        };
        // quiet overrules all show options..
        if self.quiet {
            show = HashSet::new();
        }

        EvalArgs {
            command: arg.command.clone(),
            io: IoArgs {
                show,
                invocation_id: arg.io.invocation_id,
                in_dir: in_dir.to_path_buf(),
                out_dir: out_dir.to_path_buf(),
                send_anonymous_usage_stats: arg.io.send_anonymous_usage_stats,
                status_reporter: arg.io.status_reporter.clone(),
                log_format: self.log_format,
                log_level: self.log_level,
                log_level_file: self.log_level_file,
                log_path: self.log_path.clone(),
                otm_file_name: self.otm_file_name.clone(),
                #[cfg(all(debug_assertions, feature = "otlp"))]
                export_to_otlp: false,
                show_all_deprecations: arg.io.show_all_deprecations,
                show_timings: arg.from_main,
                build_cache_mode: arg.io.build_cache_mode,
                build_cache_url: arg.io.build_cache_url,
                build_cache_cas_url: arg.io.build_cache_cas_url,
            },
            profiles_dir: self.profiles_dir.clone(),
            packages_install_path: self.packages_install_path.clone(),
            profile: self.profile.clone(),
            target: self.target.clone(),
            vars: self.vars.clone().unwrap_or_default(),
            phase: Phases::All,
            format: DEFAULT_FORMAT.clone(),
            limit: Some(10),
            debug: self.debug,
            num_threads: if self.single_threaded {
                Some(1)
            } else {
                self.threads
            },
            select: self
                .select
                .clone()
                .map(|s| parse_model_specifiers(&s).unwrap()),
            exclude: self
                .exclude
                .clone()
                .map(|s| parse_model_specifiers(&s).unwrap()),
            indirect_selection: self.indirect_selection,
            selector: self.selector.clone(),
            log_format_file: self.log_format_file,
            log_format: self.log_format,
            log_level_file: match (self.debug, self.log_level_file) {
                (true, Some(LevelFilter::Trace)) => Some(LevelFilter::Trace),
                (true, _) => Some(LevelFilter::Debug),
                (false, _) => self.log_level_file,
            },
            log_level: match (self.debug, self.log_level) {
                (true, Some(LevelFilter::Trace)) => Some(LevelFilter::Trace),
                (true, _) => Some(LevelFilter::Debug),
                (false, _) => self.log_level,
            },
            log_path: self.log_path.clone(),
            project_dir: self.project_dir.clone(),
            quiet: self.quiet,
            write_json: if self.no_write_json {
                false
            } else {
                self.write_json
            },
            target_path: self.target_path.clone(),
            ..Default::default()
        }
    }

    pub fn get_send_anonymous_usage_stats(&self) -> bool {
        if self.no_send_anonymous_usage_stats {
            false
        } else {
            self.send_anonymous_usage_stats
        }
    }
}

pub fn from_main(cli: &Cli) -> SystemArgs {
    SystemArgs {
        command: cli.get_command_str().to_string(),
        io: IoArgs {
            invocation_id: uuid::Uuid::new_v4(),
            show: cli.common_args().show.iter().cloned().collect(),
            in_dir: PathBuf::new(),
            out_dir: PathBuf::new(),
            send_anonymous_usage_stats: cli.common_args().get_send_anonymous_usage_stats(),
            status_reporter: None,
            log_format: cli.common_args().log_format,
            log_level: match (cli.common_args().debug, cli.common_args().log_level) {
                (true, Some(LevelFilter::Trace)) => Some(LevelFilter::Trace),
                (true, _) => Some(LevelFilter::Debug),
                (false, _) => cli.common_args().log_level,
            },
            log_level_file: match (cli.common_args().debug, cli.common_args().log_level_file) {
                (true, Some(LevelFilter::Trace)) => Some(LevelFilter::Trace),
                (true, _) => Some(LevelFilter::Debug),
                (false, _) => cli.common_args().log_level_file,
            },
            log_path: cli.common_args().log_path,
            otm_file_name: cli.common_args().otm_file_name,
            #[cfg(all(debug_assertions, feature = "otlp"))]
            export_to_otlp: false,
            show_all_deprecations: cli.common_args().show_all_deprecations,
            show_timings: true, // always true for main
            build_cache_mode: None,
            build_cache_url: None,
            build_cache_cas_url: None,
        },
        from_main: true,

        target: cli.common_args().target,
        num_threads: cli.common_args().threads,
    }
}

pub fn from_lib(cli: &Cli) -> SystemArgs {
    SystemArgs {
        command: cli.get_command_str().to_string(),
        io: IoArgs {
            invocation_id: uuid::Uuid::new_v4(),
            show: cli.common_args().show.iter().cloned().collect(),
            in_dir: PathBuf::new(),
            out_dir: PathBuf::new(),
            send_anonymous_usage_stats: cli.common_args().get_send_anonymous_usage_stats(),
            status_reporter: None,
            log_format: cli.common_args().log_format,
            log_level: cli.common_args().log_level,
            log_level_file: cli.common_args().log_level_file,
            log_path: cli.common_args().log_path,
            otm_file_name: cli.common_args().otm_file_name,
            #[cfg(all(debug_assertions, feature = "otlp"))]
            export_to_otlp: false,
            show_all_deprecations: cli.common_args().show_all_deprecations,
            show_timings: false, // always false for lib
            build_cache_mode: None,
            build_cache_url: None,
            build_cache_cas_url: None,
        },
        from_main: false,
        target: cli.common_args().target,
        num_threads: cli.common_args().threads,
    }
}
