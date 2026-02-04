use console::Style;
use dbt_common::cli_parser_trait::CliParserTrait;
use dbt_common::io_utils::determine_project_dir;
use dbt_common::{ErrorCode, FsResult, fs_err, stdfs};
use dbt_serde_yaml::Value as YValue;
use log::LevelFilter;
use serde::{Deserialize, Serialize};

use std::any::Any;
use std::ffi::OsString;
use std::fmt;
use std::sync::LazyLock;
use std::{
    collections::{BTreeMap, HashSet},
    path::{Path, PathBuf},
    str::FromStr,
};
use strum::IntoEnumIterator;
use strum_macros::Display;
use uuid::Uuid;

use dbt_common::constants::{DBT_PROJECT_YML, DBT_TARGET_DIR_NAME, NOOP};
use dbt_common::io_args::FsCommand;
use dbt_common::io_args::{BuildCacheMode, DisplayFormat, ListOutputFormat, StaticAnalysisKind};
use dbt_common::io_args::{
    ClapResourceType, EvalArgs, IoArgs, JsonSchemaTypes, LocalExecutionBackendKind,
    OptimizeTestsOptions, Phases, RunCacheMode, ShowOptions, SystemArgs, TimeMachineModeKind,
    TimeMachineReplayOrdering, check_selector, check_target, check_var, validate_project_name,
};
use dbt_common::row_limit::RowLimit;

use clap::{ArgAction, Parser, ValueEnum, arg, builder::BoolishValueParser, command};

use dbt_common::logging::LogFormat;
use dbt_common::node_selector::{
    IndirectSelection, MethodName, SelectionCriteria, parse_model_specifiers,
};

use crate::time_machine::*;
use crate::tracing::*;

pub mod commands;
mod time_machine;
mod tracing;

pub use crate::commands::{Command, CoreCommand};

use self::commands::{CommandParser, ExtensionCommandParser};

pub const DEFAULT_LIMIT: &str = "10";
pub const DEFAULT_FORMAT: DisplayFormat = DisplayFormat::Table;

// defined in pretty string, but copied here to avoid cycle...
static BOLD: LazyLock<Style> = LazyLock::new(|| Style::new().bold());

// ----------------------------------------------------------------------------------------------
// Cli and its subcommands

static ABOUT: LazyLock<String> = LazyLock::new(|| {
    format!(
        "dbt-fusion {}: A fast and enriched dbt compiler and runner",
        env!("CARGO_PKG_VERSION")
    )
});
static AFTER_HELP: LazyLock<String> = LazyLock::new(|| {
    format!(
        "{}",
        BOLD.apply_to(
            "Use `dbt <COMMAND> --help` to learn more about the options for each command."
        )
    )
});

/// Custom help template that hides top-level options (they are shown per-subcommand).
const CLI_HELP_TEMPLATE: &str = "\
{before-help}{name} {version}: {about-with-newline}
{usage-heading} {usage}

{subcommands}{after-help}";

/// An equivalent of [clap::Parser] that produces a [Cli] instead of parsing into itself.
///
/// This allows us to programatically control the clap setup and parsing process as
/// it allows [clap::Parser::try_parse] to be implemented as a method with a receiver
/// that can carry dependencies instead of a static method.
pub struct CliParser {
    command_parser: CommandParser,
}

impl CliParser {
    pub fn new(extension_command_parser: Box<dyn ExtensionCommandParser>) -> Self {
        let command_parser = CommandParser::new(extension_command_parser);
        Self { command_parser }
    }

    /// Instantiate `Cli` from the `ArgMatches` that `clap` generated.
    fn try_parse_from_arg_matches_mut(
        &self,
        arg_matches: &mut clap::ArgMatches,
    ) -> Result<Cli, clap::Error> {
        let command = self.command_parser.from_arg_matches_mut(arg_matches)?;
        let common_args = <CommonArgs as clap::FromArgMatches>::from_arg_matches_mut(arg_matches)?;
        let cli = Cli {
            command,
            common_args,
        };
        Ok(cli)
    }

    /// Build the [clap::Command] for the CLI application.
    fn app(&self) -> clap::Command {
        let app = clap::Command::new("dbt-fusion");

        // -- Augment arguments
        let app = app.group(clap::ArgGroup::new("Cli").multiple(true).args({
            let members: [clap::Id; 0] = [];
            members
        }));
        let app = self
            .command_parser
            .augment_subcommands(app)
            .subcommand_required(true)
            .arg_required_else_help(true);
        let app = <CommonArgs as clap::Args>::augment_args(app);

        // -- Augment metadata
        app.author("dbt Labs <info@getdbt.com>")
            .version(env!("CARGO_PKG_VERSION"))
            .long_about(None)
            .about(&**ABOUT)
            .after_help(&**AFTER_HELP)
            .help_template(CLI_HELP_TEMPLATE)
    }

    fn format_error(&self, err: clap::Error) -> clap::Error {
        let mut cmd = self.app();
        err.format(&mut cmd)
    }
}

impl CliParserTrait for CliParser {
    type CliType = Cli;

    /// Parse from `std::env::args_os()`, [exit][Error::exit] on error.
    fn parse(&self) -> Cli {
        let mut matches = self.app().get_matches();
        let res = self
            .try_parse_from_arg_matches_mut(&mut matches)
            .map_err(|err| self.format_error(err));
        match res {
            Ok(s) => s,
            Err(e) => {
                // Since this is more of a development-time error, we aren't doing as fancy of a quit
                // as `get_matches`
                e.exit()
            }
        }
    }

    /// Parse from `std::env::args_os()`, return Err on error.
    fn try_parse(&self) -> Result<Cli, clap::Error> {
        let mut matches = self.app().try_get_matches()?;
        self.try_parse_from_arg_matches_mut(&mut matches)
            .map_err(|err| self.format_error(err))
    }

    /// Parse from iterator, [exit][clap::Error::exit] on error.
    fn parse_from<I, T>(&self, itr: I) -> Self::CliType
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        let mut matches = self.app().get_matches_from(itr);
        let res = self
            .try_parse_from_arg_matches_mut(&mut matches)
            .map_err(|err| self.format_error(err));
        match res {
            Ok(s) => s,
            Err(e) => {
                // Since this is more of a development-time error, we aren't doing as fancy of a quit
                // as `get_matches_from`
                e.exit()
            }
        }
    }

    /// Parse from iterator, return Err on error.
    fn try_parse_from<I, T>(&self, itr: I) -> Result<Cli, clap::Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        let mut matches = self.app().try_get_matches_from(itr)?;
        self.try_parse_from_arg_matches_mut(&mut matches)
            .map_err(|err| self.format_error(err))
    }
}

#[derive(Debug, Clone)]
pub struct Cli {
    pub command: Command,
    pub common_args: CommonArgs,
}

/// Determine in/out dir assuming the command requires project dir.
///
/// NOTE: Don't call for commands that don't need project directory.
pub fn in_out_dir(common_args: &CommonArgs) -> FsResult<(PathBuf, PathBuf)> {
    let in_dir = if let Some(project_dir) = common_args.project_dir.clone() {
        project_dir
    } else {
        // TODO: the first argument to this function is never used anywhere in the codebase,
        // possibly it should be removed or properly wired
        let node_targets = &[];
        determine_project_dir(node_targets, DBT_PROJECT_YML)?
    };
    let in_dir = stdfs::canonicalize(in_dir)?;

    let out_dir = common_args
        .target_path
        .clone()
        .map(|p| if p.is_relative() { in_dir.join(p) } else { p })
        .unwrap_or_else(|| in_dir.join(DBT_TARGET_DIR_NAME));
    stdfs::create_dir_all(&out_dir).map_err(|e| {
        fs_err!(
            ErrorCode::IoError,
            "Failed to create output directory: {}",
            e
        )
    })?;
    let out_dir = stdfs::canonicalize(out_dir)?;

    Ok((in_dir, out_dir))
}

impl Cli {
    pub fn extension_command<T: Any + fmt::Debug>(&self) -> Option<&T> {
        match &self.command {
            Command::Extension(ext_cmd) => {
                let typed = ext_cmd.as_any().downcast_ref::<T>();
                debug_assert!(
                    typed.is_some(),
                    "failed to downcast extension command to the expected type; \
did you use the correct CliParser? \
got {:?}, expected an instance of {}",
                    ext_cmd.as_ref() as &dyn fmt::Debug,
                    std::any::type_name::<T>()
                );
                typed
            }
            _ => None,
        }
    }

    pub fn to_eval_args(&self, system_arg: SystemArgs) -> FsResult<EvalArgs> {
        use CoreCommand::*;
        let common_args = self.common_args();
        // Determine the input and output directories based on the command.
        // Some commands operate without project context, while others must be run in a project directory.
        let (in_dir, out_dir) = {
            match &self.command {
                Command::Core(System(_)) | Command::Core(Man(_)) | Command::Core(Init(_)) => {
                    // These commands do not require a project directory
                    (PathBuf::from("."), PathBuf::from("."))
                }
                _ => in_out_dir(&common_args)?,
            }
        };

        let from_main = system_arg.from_main;

        let mut arg = match &self.command {
            Command::Core(core_cmd) => match core_cmd {
                Init(args) => args.to_eval_args(system_arg, &in_dir, &out_dir),
                Deps(args) => args.to_eval_args(system_arg, &in_dir, &out_dir),
                List(args) => args.to_eval_args(system_arg, &in_dir, &out_dir),
                Compile(args) => args.to_eval_args(system_arg, &in_dir, &out_dir),
                Parse(args) => args.to_eval_args(system_arg, &in_dir, &out_dir),
                Run(args) => args.to_eval_args(system_arg, &in_dir, &out_dir),
                RunOperation(args) => args.to_eval_args(system_arg, &in_dir, &out_dir),
                Seed(args) => args.to_eval_args(system_arg, &in_dir, &out_dir),
                Snapshot(args) => args.to_eval_args(system_arg, &in_dir, &out_dir),
                Ls(args) => args.to_eval_args(system_arg, &in_dir, &out_dir),
                Test(args) => args.to_eval_args(system_arg, &in_dir, &out_dir),
                Build(args) => args.to_eval_args(system_arg, &in_dir, &out_dir),
                Clone(args) => args.to_eval_args(system_arg, &in_dir, &out_dir),
                Clean(args) => args.to_eval_args(system_arg, &in_dir, &out_dir),
                Source(args) => args.to_eval_args(system_arg, &in_dir, &out_dir),
                System(args) => args.to_eval_args(system_arg, &in_dir, &out_dir),
                Show(args) => args.to_eval_args(system_arg, &in_dir, &out_dir),
                Man(args) => args.to_eval_args(system_arg, &in_dir, &out_dir),
                Debug(args) => args.to_eval_args(system_arg, &in_dir, &out_dir),
                Retry(args) => args.to_eval_args(system_arg, &in_dir, &out_dir),
            },
            Command::Extension(ext_cmd) => ext_cmd.to_eval_args(&common_args, system_arg)?,
        };
        arg.from_main = from_main;
        arg.interactive = self.is_interactive();

        Ok(arg)
    }

    pub fn is_interactive(&self) -> bool {
        use CoreCommand::*;
        match &self.command {
            Command::Core(core_cmd) => match &core_cmd {
                Run(args) => args.interactive,
                Test(args) => args.interactive,
                Seed(args) => args.interactive,
                Snapshot(args) => args.interactive,
                Build(args) => args.interactive,
                Compile(args) => args.interactive,
                Show(args) => args.interactive,
                _ => false,
            },
            Command::Extension(ext_cmd) => ext_cmd.is_interactive(),
        }
    }

    pub fn common_args(&self) -> CommonArgs {
        match &self.command {
            Command::Core(core_cmd) => core_cmd.common_args().clone(),
            Command::Extension(ext_cmd) => ext_cmd.common_args(),
        }
    }

    pub fn project_dir(&self) -> Option<PathBuf> {
        self.common_args().project_dir
    }

    pub fn target_path(&self) -> Option<PathBuf> {
        self.common_args().target_path
    }

    pub fn stage(&self) -> Phases {
        use CoreCommand::*;
        // todo: fix this: should take the minimum of the user selection and the default
        match &self.command {
            Command::Core(core_cmd) => match core_cmd {
                Init(_args) => unreachable!("Init command does not need a phase"),
                Deps(args) => args.common_args.phase.clone().unwrap_or(Phases::Deps),
                Parse(args) => args.common_args.phase.clone().unwrap_or(Phases::Parse),
                Ls(args) => args.common_args.phase.clone().unwrap_or(Phases::List),
                List(args) => args.common_args.phase.clone().unwrap_or(Phases::List),
                Compile(args) => args.common_args.phase.clone().unwrap_or(Phases::Compile),
                Run(args) => args.common_args.phase.clone().unwrap_or(Phases::All),
                RunOperation(args) => args.common_args.phase.clone().unwrap_or(Phases::Parse),
                Seed(args) => args.common_args.phase.clone().unwrap_or(Phases::All),
                Snapshot(args) => args.common_args.phase.clone().unwrap_or(Phases::All),
                Test(args) => args.common_args.phase.clone().unwrap_or(Phases::All),
                Build(args) => args.common_args.phase.clone().unwrap_or(Phases::All),
                Clone(args) => args.common_args.phase.clone().unwrap_or(Phases::All),
                Clean(_args) => unreachable!("Clean command does not need a phase"),
                Source(args) => args
                    .common_args()
                    .phase
                    .clone()
                    .unwrap_or(Phases::Freshness),
                System(_args) => unreachable!("System command does not need a phase"),
                Show(args) => args.common_args.phase.clone().unwrap_or(Phases::Show),
                Man(_args) => unreachable!("Man command does not need a phase"),
                Debug(args) => args.common_args.phase.clone().unwrap_or(Phases::Debug),
                Retry(args) => args.common_args.phase.clone().unwrap_or(Phases::All),
            },
            Command::Extension(ext_cmd) => ext_cmd.stage(),
        }
    }

    pub fn as_command(&self) -> FsCommand {
        match &self.command {
            Command::Core(core_cmd) => core_cmd.as_command(),
            Command::Extension(ext_cmd) => ext_cmd.as_command(),
        }
    }

    pub fn cli_options(&self) -> Vec<String> {
        use CoreCommand::*;
        let mut options = Vec::new();
        // Add global/common args
        options.extend(struct_to_cli_options(&self.common_args));
        // Add subcommand-specific args
        match &self.command {
            Command::Core(core_cmd) => match core_cmd {
                Build(args) => options.extend(struct_to_cli_options(args)),
                Run(args) => options.extend(struct_to_cli_options(args)),
                Test(args) => options.extend(struct_to_cli_options(args)),
                Parse(args) => options.extend(struct_to_cli_options(args)),
                Compile(args) => options.extend(struct_to_cli_options(args)),
                _ => {}
            },
            Command::Extension(ext_cmd) => ext_cmd.extend_cli_options(&mut options),
        }
        options
    }

    pub fn with_sample(&self) -> Option<String> {
        use CoreCommand::*;
        match &self.command {
            Command::Core(core_cmd) => match core_cmd {
                Run(RunArgs { with_sample, .. }) => with_sample.clone(),
                Test(TestArgs { with_sample, .. }) => with_sample.clone(),
                Build(BuildArgs { with_sample, .. }) => with_sample.clone(),
                Compile(CompileArgs { with_sample, .. }) => with_sample.clone(),
                Show(ShowArgs { with_sample, .. }) => with_sample.clone(),
                _ => None,
            },
            Command::Extension(ext_cmd) => ext_cmd.with_sample(),
        }
    }

    pub fn sampled(&self) -> Vec<String> {
        use CoreCommand::*;
        match &self.command {
            Command::Core(core_cmd) => match core_cmd {
                Run(RunArgs { sampled, .. }) => sampled.clone(),
                Test(TestArgs { sampled, .. }) => sampled.clone(),
                Build(BuildArgs { sampled, .. }) => sampled.clone(),
                Compile(CompileArgs { sampled, .. }) => sampled.clone(),
                Show(ShowArgs { sampled, .. }) => sampled.clone(),
                _ => vec![],
            },
            Command::Extension(ext_cmd) => ext_cmd.sampled(),
        }
    }
}

/// Converts a serializable struct to CLI options (e.g. --foo bar --baz qux)
pub fn struct_to_cli_options<T: Serialize>(s: &T) -> Vec<String> {
    let mut options = Vec::new();
    let value = serde_json::to_value(s).unwrap();
    if let serde_json::Value::Object(map) = value {
        for (k, v) in map {
            if k == "vars" {
                if let serde_json::Value::Object(vars_map) = v
                    && !vars_map.is_empty()
                {
                    // Serialize as JSON string for CLI
                    let vars_str = dbt_serde_yaml::to_string(&vars_map).unwrap();
                    options.push("--vars".to_string());
                    options.push(vars_str);
                }
                continue;
            }
            match v {
                serde_json::Value::Bool(true) => options.push(format!("--{}", k.replace('_', "-"))),
                serde_json::Value::Bool(false) => {} // skip false flags
                serde_json::Value::Null => {}
                serde_json::Value::Array(arr) => {
                    for item in arr {
                        options.push(format!("--{}", k.replace('_', "-")));
                        options.push(item.to_string().trim_matches('"').to_string());
                    }
                }
                _ => {
                    options.push(format!("--{}", k.replace('_', "-")));
                    options.push(v.to_string().trim_matches('"').to_string());
                }
            }
        }
    }
    options
}
// ----------------------------------------------------------------------------------------------
// Build, run, test, compile subcommands

#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct CleanArgs {
    /// Clean the target directory specified by file or --target-path
    #[arg(value_parser = check_target)]
    pub files: Vec<String>,

    // Flattened Common args
    #[clap(flatten)]
    pub common_args: CommonArgs,
}

impl CleanArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        self.common_args.to_eval_args(arg, in_dir, out_dir)
    }
}

#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct DepsArgs {
    #[arg(long)]
    pub add_package: Option<String>,
    #[arg(long)]
    pub upgrade: bool,
    #[arg(long)]
    pub lock: bool,

    // Flattened Common args
    #[clap(flatten)]
    pub common_args: CommonArgs,
}
impl DepsArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let mut eval_args = self.common_args.to_eval_args(arg, in_dir, out_dir);
        eval_args.phase = Phases::Deps;
        eval_args.add_package = self.add_package.clone();
        eval_args.upgrade = self.upgrade;
        eval_args.lock = self.lock;
        eval_args
    }
}

#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct ParseArgs {
    // Flattened Common args
    #[clap(flatten)]
    pub common_args: CommonArgs,
}
impl ParseArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let mut eval_args = self.common_args.to_eval_args(arg, in_dir, out_dir);
        eval_args.phase = Phases::Parse;
        eval_args
    }
}

#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct CompileArgs {
    /// Compile the given nodes, identified by paths, and all its upstreams
    #[arg(value_parser = check_target)]
    pub node_targets: Vec<String>,

    // Flattened Common args
    #[clap(flatten)]
    pub common_args: CommonArgs,

    /// Drop into an interactive REPL after executing the command
    #[arg(long, short = 'i', hide = true)]
    pub interactive: bool,

    /// Provide SQL content directly to compile as a temporary model
    #[arg(long, conflicts_with = "select", allow_hyphen_values = true)]
    pub inline: Option<String>,

    /// Select nodes of a specific type;
    #[arg(long, num_args(1..), value_delimiter = ' ', aliases = ["resource-types"])]
    pub resource_type: Option<Vec<ClapResourceType>>,

    /// Exclude nodes of a specific type;
    #[arg(long, num_args(1..), value_delimiter = ' ', aliases = ["exclude-resource-types"])]
    pub exclude_resource_type: Option<Vec<ClapResourceType>>,

    /// Limiting number of shown rows. Run with --limit -1 to remove limit [default: 10]
    #[arg(long, default_value=DEFAULT_LIMIT, allow_hyphen_values = true)]
    pub limit: RowLimit,

    /// Display rows in different formats
    #[arg(global = true, long, aliases = ["format"])]
    pub output: Option<DisplayFormat>,

    /// Flag to enable or disable SQL analysis, or to run SQL in unsafe mode,  enabled by default
    #[arg(global = true, long, default_value = "on", env = "DBT_STATIC_ANALYSIS")]
    pub static_analysis: StaticAnalysisKind,

    /// Drop incremental models and fully recalculate incremental tables.
    #[arg(global = true, long, action = ArgAction::SetTrue, value_parser = BoolishValueParser::new(), short = 'f', env = "DBT_FULL_REFRESH")]
    pub full_refresh: bool,

    /// Use the samples as given in this YAML/JSON file.
    #[arg(long, value_name = "default|FILE", alias = "with-sample")]
    pub with_sample: Option<String>,
    /// Add source selectors to sample (e.g., "source:raw.events"). Repeatable.
    #[arg(long, num_args(1..), value_delimiter = ' ')]
    pub sampled: Vec<String>,
}

impl CompileArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let mut eval_args = self.common_args.to_eval_args(arg, in_dir, out_dir);
        eval_args.phase = Phases::Compile;
        eval_args.introspect = self.common_args.get_introspect();
        eval_args.limit = self.limit.into();
        // If introspection is disabled, set static analysis to off
        eval_args.static_analysis = if eval_args.introspect {
            self.static_analysis
        } else {
            StaticAnalysisKind::Off
        };
        eval_args.full_refresh = self.full_refresh;
        eval_args.format = self.output.unwrap_or(DEFAULT_FORMAT);
        if let Some(resource_type) = &self.resource_type {
            eval_args.resource_types = resource_type.clone();
        }
        if let Some(exclude_resource_type) = &self.exclude_resource_type {
            eval_args.exclude_resource_types = exclude_resource_type.clone();
        }

        eval_args
    }
}

#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct SeedArgs {
    // Flattened Common args
    #[clap(flatten)]
    pub common_args: CommonArgs,

    /// Drop into an interactive REPL after executing the command
    #[arg(long, short = 'i', hide = true)]
    pub interactive: bool,

    /// Force node selection
    #[arg(long, default_value = "false")]
    pub force_node_selection: bool,

    /// The mode to use for the run cache. Cannot be used with --force-node-selection
    #[arg(
        long,
        default_value = "read-write",
        conflicts_with = "force_node_selection"
    )]
    pub run_cache_mode: RunCacheMode,

    /// Disable run cache
    #[arg(long, default_value = "false", conflicts_with = "force_node_selection")]
    pub no_run_cache: bool,

    /// Flag to enable or disable SQL analysis, or to run SQL in unsafe mode, enabled by default
    #[arg(global = true, long, default_value = "on", env = "DBT_STATIC_ANALYSIS")]
    pub static_analysis: StaticAnalysisKind,

    /// Drop incremental models and fully recalculate incremental tables.
    #[arg(global = true, long, action = ArgAction::SetTrue, value_parser = BoolishValueParser::new(), short = 'f', env = "DBT_FULL_REFRESH")]
    pub full_refresh: bool,
}

impl SeedArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let mut eval_args = self.common_args.to_eval_args(arg, in_dir, out_dir);
        eval_args.resource_types = vec![ClapResourceType::Seed];
        if self.common_args.task_cache_url != NOOP && !self.no_run_cache {
            if self.force_node_selection {
                eval_args.run_cache_mode = RunCacheMode::WriteOnly;
            } else {
                eval_args.run_cache_mode = self.run_cache_mode.clone();
            }
        }
        eval_args.static_analysis = self.static_analysis;
        eval_args.full_refresh = self.full_refresh;
        eval_args
    }
}

#[derive(Parser, Debug, Clone, Serialize, Deserialize)]
pub struct SourceArgs {
    #[command(subcommand)]
    pub command: SourceCommand,
}

impl SourceArgs {
    pub fn common_args(&self) -> &CommonArgs {
        match &self.command {
            SourceCommand::Freshness(f) => &f.common_args,
        }
    }

    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let mut eval_args = self.common_args().to_eval_args(arg, in_dir, out_dir);
        let predicate = SelectionCriteria::new(
            MethodName::ResourceType,
            vec![],
            "source".to_string(),
            false,
            None,
            None,
            Some(IndirectSelection::default()),
            None,
        );
        eval_args.check_all = match &self.command {
            SourceCommand::Freshness(f) => f.check_all,
        };
        eval_args.phase = Phases::Freshness;
        eval_args.set_refined_node_selectors(Some(predicate))
    }
}

#[derive(Parser, Debug, Clone, Serialize, Deserialize)]
#[command()]
pub enum SourceCommand {
    /// Check the current freshness of the project's sources
    Freshness(SourceFreshnessArgs),
}

#[derive(Parser, Debug, Clone, Serialize, Deserialize)]
pub struct SourceFreshnessArgs {
    /// Check freshness of all sources
    #[arg(long, default_value = "false")]
    pub check_all: bool,

    // Flattened Common args
    #[clap(flatten)]
    pub common_args: CommonArgs,
}

#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct ShowArgs {
    #[clap(flatten)]
    pub common_args: CommonArgs,

    /// Show the given query
    #[arg(long)]
    pub inline: Option<String>,

    /// Select nodes of a specific type;
    #[arg(long, num_args(1..), value_delimiter = ' ', aliases = ["resource-types"])]
    pub resource_type: Option<Vec<ClapResourceType>>,

    /// Exclude nodes of a specific type;
    #[arg(long, num_args(1..), value_delimiter = ' ', aliases = ["exclude-resource-types"])]
    pub exclude_resource_type: Option<Vec<ClapResourceType>>,

    /// Drop into an interactive REPL after executing the command
    #[arg(long, short = 'i', hide = true)]
    pub interactive: bool,

    /// Limiting number of shown rows. Run with --limit -1 to remove limit [default: 10]
    #[arg(long, default_value=DEFAULT_LIMIT, allow_hyphen_values = true)]
    pub limit: RowLimit,

    /// Display rows in different formats
    #[arg(global = true, long, aliases = ["format"])]
    pub output: Option<DisplayFormat>,

    /// Flag to enable or disable SQL analysis, or to run SQL in unsafe mode,  enabled by default
    #[arg(global = true, long, default_value = "on", env = "DBT_STATIC_ANALYSIS")]
    pub static_analysis: StaticAnalysisKind,

    /// Do not perform any local type checking on the show target
    ///
    /// If this is set, any existing data in the remote warehouse will be
    /// displayed regardless of whether it matches the current state of the
    /// local workspace.
    ///
    /// @deprecated This is now the default behavior, command arg retained for
    /// backwards compatibility, will be removed in a future release.
    #[arg(long)]
    pub unchecked: bool,

    /// Use the samples as given in this YAML/JSON file.
    #[arg(long, value_name = "default|FILE", alias = "with-sample")]
    pub with_sample: Option<String>,
    /// Add source selectors to sample (e.g., "source:raw.events"). Repeatable.
    #[arg(long, num_args(1..), value_delimiter = ' ')]
    pub sampled: Vec<String>,
}

impl ShowArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let mut eval_args = self.common_args.to_eval_args(arg, in_dir, out_dir);
        eval_args.phase = Phases::Show;
        if let Some(resource_type) = &self.resource_type {
            eval_args.resource_types = resource_type.clone();
        } else {
            eval_args.resource_types = vec![
                ClapResourceType::Model,
                ClapResourceType::Snapshot,
                ClapResourceType::Seed,
                ClapResourceType::Source,
                ClapResourceType::Analysis,
            ];
        }
        eval_args.limit = self.limit.into();
        eval_args.static_analysis = self.static_analysis;
        eval_args.format = self.output.unwrap_or(DEFAULT_FORMAT);
        if let Some(exclude_resource_type) = &self.exclude_resource_type {
            eval_args.exclude_resource_types = exclude_resource_type.clone();
        }
        eval_args.interactive = self.interactive;
        eval_args
    }
}

#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct SnapshotArgs {
    /// Snapshot the given nodes; same as --select node_1 ... node_n
    #[arg(value_parser = check_target)]
    pub node_targets: Vec<String>,

    // Flattened Common args
    #[clap(flatten)]
    pub common_args: CommonArgs,

    /// Drop into an interactive REPL after executing the command
    #[arg(long, short = 'i', hide = true)]
    pub interactive: bool,

    /// Force node selection
    #[arg(long, default_value = "false")]
    pub force_node_selection: bool,

    /// The mode to use for the run cache. Cannot be used with --force-node-selection
    #[arg(
        long,
        default_value = "read-write",
        conflicts_with = "force_node_selection"
    )]
    pub run_cache_mode: RunCacheMode,

    /// Disable run cache
    #[arg(long, default_value = "false", conflicts_with = "force_node_selection")]
    pub no_run_cache: bool,

    /// Flag to enable or disable SQL analysis, or to run SQL in unsafe mode, enabled by default
    #[arg(global = true, long, default_value = "on", env = "DBT_STATIC_ANALYSIS")]
    pub static_analysis: StaticAnalysisKind,
}

impl SnapshotArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let mut eval_args = self.common_args.to_eval_args(arg, in_dir, out_dir);

        if self.common_args.task_cache_url != NOOP && !self.no_run_cache {
            if self.force_node_selection {
                eval_args.run_cache_mode = RunCacheMode::WriteOnly;
            } else {
                eval_args.run_cache_mode = self.run_cache_mode.clone();
            }
        }
        eval_args.resource_types = vec![ClapResourceType::Snapshot];
        eval_args.static_analysis = self.static_analysis;
        eval_args
    }
}

#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct TestArgs {
    ///Test the given nodes; same as --select node_1 ... node_n
    #[arg(value_parser = check_target)]
    pub node_targets: Vec<String>,

    // Flattened Common args
    #[clap(flatten)]
    pub common_args: CommonArgs,

    /// Drop into an interactive REPL after executing the command
    #[arg(long, short = 'i', hide = true)]
    pub interactive: bool,

    /// Enable optimizations (testaggregation, testreuse)
    #[arg(long, num_args(0..), hide = true, help = "Enable optimizations [options: testaggregation, testreuse]\n")]
    pub optimize_tests: Vec<OptimizeTestsOptions>,

    /// Force node selection
    #[arg(long, default_value = "false")]
    pub force_node_selection: bool,

    /// The mode to use for the run cache. Cannot be used with --force-node-selection
    #[arg(
        long,
        default_value = "read-write",
        conflicts_with = "force_node_selection"
    )]
    pub run_cache_mode: RunCacheMode,

    /// Disable run cache
    #[arg(long, default_value = "false", conflicts_with = "force_node_selection")]
    pub no_run_cache: bool,

    /// Limiting number of shown rows. Run with --limit -1 to remove limit [default: 10]
    #[arg(long, default_value=DEFAULT_LIMIT, allow_hyphen_values = true)]
    pub limit: RowLimit,

    /// Display rows in different formats
    #[arg(global = true, long, aliases = ["format"])]
    pub output: Option<DisplayFormat>,

    /// Flag to enable or disable SQL analysis, or to run SQL in unsafe mode,  enabled by default
    #[arg(global = true, long, default_value = "on", env = "DBT_STATIC_ANALYSIS")]
    pub static_analysis: StaticAnalysisKind,

    /// Use the samples as given in this YAML/JSON file.
    #[arg(long, value_name = "default|FILE", alias = "with-sample")]
    pub with_sample: Option<String>,
    /// Add source selectors to sample (e.g., "source:raw.events"). Repeatable.
    #[arg(long, num_args(1..), value_delimiter = ' ')]
    pub sampled: Vec<String>,
}

impl TestArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let mut eval_args = self.common_args.to_eval_args(arg, in_dir, out_dir);
        eval_args.optimize_tests = self.optimize_tests.iter().cloned().collect();
        eval_args.resource_types = vec![ClapResourceType::Test, ClapResourceType::UnitTest];
        if self.common_args.task_cache_url != NOOP && !self.no_run_cache {
            if self.force_node_selection {
                eval_args.run_cache_mode = RunCacheMode::WriteOnly;
            } else {
                eval_args.run_cache_mode = self.run_cache_mode.clone();
            }
        }
        eval_args.limit = self.limit.into();
        eval_args.static_analysis = self.static_analysis;
        eval_args.format = self.output.unwrap_or(DEFAULT_FORMAT);
        eval_args
    }
}

#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct BuildArgs {
    // Flattened Common args
    #[clap(flatten)]
    pub common_args: CommonArgs,

    /// Drop into an interactive REPL after executing the command
    #[arg(long, short = 'i', hide = true)]
    pub interactive: bool,

    /// Select nodes of a specific type;
    #[arg(long, num_args(1..), value_delimiter = ' ', aliases = ["resource-types"])]
    pub resource_type: Option<Vec<ClapResourceType>>,

    /// Exclude nodes of a specific type;
    #[arg(long, num_args(1..), value_delimiter = ' ', aliases = ["exclude-resource-types"])]
    pub exclude_resource_type: Option<Vec<ClapResourceType>>,

    /// Enable optimizations (testaggregation, testreuse)
    #[arg(long, num_args(0..), hide = true, help = "Enable optimizations [options: testaggregation, testreuse]\n")]
    pub optimize_tests: Vec<OptimizeTestsOptions>,

    /// Force node selection
    #[arg(long, default_value = "false")]
    pub force_node_selection: bool,

    /// The mode to use for the run cache. Cannot be used with --force-node-selection
    #[arg(
        long,
        default_value = "read-write",
        conflicts_with = "force_node_selection"
    )]
    pub run_cache_mode: RunCacheMode,

    /// Disable run cache
    #[arg(long, default_value = "false", conflicts_with = "force_node_selection")]
    pub no_run_cache: bool,

    /// Drop incremental models and fully recalculate incremental tables.
    #[arg(global = true, long, action = ArgAction::SetTrue, value_parser = BoolishValueParser::new(), short = 'f', env = "DBT_FULL_REFRESH")]
    pub full_refresh: bool,

    /// Limiting number of shown rows. Run with --limit -1 to remove limit [default: 10]
    #[arg(long, default_value=DEFAULT_LIMIT, allow_hyphen_values = true)]
    pub limit: RowLimit,

    /// Display rows in different formats
    #[arg(global = true, long, aliases = ["format"])]
    pub output: Option<DisplayFormat>,

    /// Flag to enable or disable SQL analysis, or to run SQL in unsafe mode,  enabled by default
    #[arg(global = true, long, default_value = "on", env = "DBT_STATIC_ANALYSIS")]
    pub static_analysis: StaticAnalysisKind,

    /// Run models using time-based filters (only applicable to relations created via `ref` or `source`)
    /// reference: https://docs.getdbt.com/docs/build/sample-flag
    #[arg(global = true, long, hide = true, env = "DBT_SAMPLE")]
    pub sample: Option<String>,

    /// Use the samples as given in this YAML/JSON file.
    #[arg(long, value_name = "default|FILE", alias = "with-sample")]
    pub with_sample: Option<String>,
    /// Add source selectors to sample (e.g., "source:raw.events"). Repeatable.
    #[arg(long, num_args(1..), value_delimiter = ' ')]
    pub sampled: Vec<String>,
}

impl BuildArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let mut eval_args = self.common_args.to_eval_args(arg, in_dir, out_dir);
        eval_args.optimize_tests = self.optimize_tests.iter().cloned().collect();
        eval_args.phase = Phases::All;
        // Enable task cache
        if let Some(resource_type) = &self.resource_type {
            eval_args.resource_types = resource_type.clone();
        } else {
            eval_args.resource_types = vec![
                ClapResourceType::Model,
                ClapResourceType::Seed,
                ClapResourceType::Snapshot,
                ClapResourceType::Test,
                ClapResourceType::UnitTest,
                ClapResourceType::Function,
            ];
            if eval_args.export_saved_queries {
                eval_args.resource_types.push(ClapResourceType::SavedQuery);
            }
        }
        if let Some(exclude_resource_type) = &self.exclude_resource_type {
            eval_args.exclude_resource_types = exclude_resource_type.clone();
        }
        if self.common_args.task_cache_url != NOOP && !self.no_run_cache {
            if self.force_node_selection {
                eval_args.run_cache_mode = RunCacheMode::WriteOnly;
            } else {
                eval_args.run_cache_mode = self.run_cache_mode.clone();
            }
        }
        eval_args.full_refresh = self.full_refresh;
        eval_args.static_analysis = self.static_analysis;
        eval_args.empty = self.common_args.empty;
        eval_args.sample = self.sample.clone();
        eval_args
    }
}

#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct ListArgs {
    // Flattened Common args
    #[clap(flatten)]
    pub common_args: CommonArgs,

    /// Limiting number of shown rows. Run with --limit -1 to remove limit [default: 10]
    // todo: still l;eft to be implemented..
    #[arg(long,default_value=DEFAULT_LIMIT, allow_hyphen_values = true, hide = true)]
    pub limit: RowLimit,

    /// Output format: either JSON or a newline-delimited list of selectors, paths, or names
    #[arg(global = true, long, aliases = ["format"], default_value = "selector")]
    pub output: ListOutputFormat,

    /// Space-delimited listing of node properties to include as custom keys for JSON output
    /// (e.g. `--output json --output-keys name resource_type description`)
    #[arg(long, num_args(1..), value_delimiter = ' ')]
    pub output_keys: Vec<String>,

    /// Select nodes of a specific type;
    #[arg(long, num_args(1..), value_delimiter = ' ', aliases = ["resource-types"])]
    pub resource_type: Option<Vec<ClapResourceType>>,

    /// Exclude nodes of a specific type;
    #[arg(long, num_args(1..), value_delimiter = ' ', aliases = ["exclude-resource-types"])]
    pub exclude_resource_type: Option<Vec<ClapResourceType>>,
}

impl ListArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let mut eval_args = self.common_args.to_eval_args(arg, in_dir, out_dir);
        eval_args.phase = Phases::List;
        eval_args.io.show.insert(ShowOptions::Nodes);
        eval_args.output_keys = self.output_keys.clone();
        if let Some(resource_type) = &self.resource_type {
            eval_args.resource_types = resource_type.clone();
        }
        if let Some(exclude_resource_type) = &self.exclude_resource_type {
            eval_args.exclude_resource_types = exclude_resource_type.clone();
        }
        eval_args.limit = self.limit.into();
        // Convert ListOutputFormat to DisplayFormat for EvalArgs
        eval_args.format = DisplayFormat::from(self.output);
        eval_args
    }
}

#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct RunArgs {
    // Flattened IO args
    #[clap(flatten)]
    pub common_args: CommonArgs,

    /// Drop into an interactive REPL after executing the command
    #[arg(long, short = 'i', hide = true)]
    pub interactive: bool,

    /// Force node selection
    #[arg(long, default_value = "false")]
    pub force_node_selection: bool,

    /// The mode to use for the run cache. Cannot be used with --force-node-selection
    #[arg(
        long,
        default_value = "read-write",
        conflicts_with = "force_node_selection"
    )]
    pub run_cache_mode: RunCacheMode,

    /// Disable run cache
    #[arg(long, default_value = "false", conflicts_with = "force_node_selection")]
    pub no_run_cache: bool,

    /// Limiting number of shown rows. Run with --limit -1 to remove limit [default: 10]
    #[arg(long, default_value=DEFAULT_LIMIT, allow_hyphen_values = true)]
    pub limit: RowLimit,

    /// Display rows in different formats
    #[arg(global = true, long, aliases = ["format"])]
    pub output: Option<DisplayFormat>,

    /// Flag to enable or disable SQL analysis, or to run SQL in unsafe mode,  enabled by default
    #[arg(global = true, long, default_value = "on", env = "DBT_STATIC_ANALYSIS")]
    pub static_analysis: StaticAnalysisKind,

    /// Drop incremental models and fully recalculate incremental tables.
    #[arg(global = true, long, action = ArgAction::SetTrue, value_parser = BoolishValueParser::new(), short = 'f', env = "DBT_FULL_REFRESH")]
    pub full_refresh: bool,

    /// Sample mode generates time-based filtered refs and sources
    #[arg(global = true, long, hide = true, env = "DBT_SAMPLE")]
    pub sample: Option<String>,

    /// Use the samples as given in this YAML/JSON file.
    #[arg(long, value_name = "default|FILE", alias = "with-sample")]
    pub with_sample: Option<String>,
    /// Add source selectors to sample (e.g., "source:raw.events"). Repeatable.
    #[arg(long, num_args(1..), value_delimiter = ' ')]
    pub sampled: Vec<String>,
}

impl RunArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let mut eval_args = self.common_args.to_eval_args(arg, in_dir, out_dir);
        eval_args.phase = Phases::All;

        if self.common_args.task_cache_url != NOOP && !self.no_run_cache {
            if self.force_node_selection {
                eval_args.run_cache_mode = RunCacheMode::WriteOnly;
            } else {
                eval_args.run_cache_mode = self.run_cache_mode.clone();
            }
        }

        eval_args.resource_types = vec![ClapResourceType::Model];
        eval_args.limit = self.limit.into();
        eval_args.static_analysis = self.static_analysis;
        eval_args.full_refresh = self.full_refresh;
        eval_args.empty = self.common_args.empty;
        eval_args.sample = self.sample.clone();
        // Optional sampling plan (for local runs to locate sampled data)
        // if let Some(ss) = &self.with_sample {
        //     let plan = normalize_sample_plan_or_exit::<RunArgs>(&Some(ss.clone()));
        //     eval_args.sample_plan = Some(plan.to_macro_json().expect("Sampling plan to be serializable"));
        // } else if !self.sampled.is_empty() {
        //     let entries: Vec<serde_json::Value> = self
        //         .sampled
        //         .iter()
        //         .map(|p| Entry {
        //             select: Some(p.to_string()),
        //             strategy: Strategy::Clone , ..Default::default()
        //             })
        //         .collect();
        //     let plan = Plan { entries };
        //     eval_args.sample_plan = Some(serde_json::json!({ "entries": entries }));
        // }
        eval_args.format = self.output.unwrap_or(DEFAULT_FORMAT);
        eval_args
    }
}

#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct CloneArgs {
    // Flattened IO args
    #[clap(flatten)]
    pub common_args: CommonArgs,

    /// Drop existing models and clone all tables.
    #[arg(global = true, long, action = ArgAction::SetTrue, value_parser = BoolishValueParser::new(), short = 'f', env = "DBT_FULL_REFRESH")]
    pub full_refresh: bool,
}

impl CloneArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let mut eval_args = self.common_args.to_eval_args(arg, in_dir, out_dir);
        eval_args.phase = Phases::All;

        eval_args.full_refresh = self.full_refresh;

        eval_args.run_cache_mode = RunCacheMode::Noop;
        eval_args.resource_types = vec![
            ClapResourceType::Model,
            ClapResourceType::Seed,
            ClapResourceType::Snapshot,
        ];
        eval_args.format = DEFAULT_FORMAT;

        eval_args
    }
}

#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct RunOperationArgs {
    #[arg(id = "MACRO")]
    pub macro_name: String,

    /// Supply arguments to the macro. This dictionary will be mapped to the keyword arguments defined in the selected macro. This argument should be a yml string.
    #[arg(long,value_parser = check_var)]
    pub args: Option<BTreeMap<String, YValue>>,

    // Flattened IO args
    #[clap(flatten)]
    pub common_args: CommonArgs,
}

impl RunOperationArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let mut eval_args = self.common_args.to_eval_args(arg, in_dir, out_dir);
        eval_args.phase = Phases::RunOperation;
        eval_args.macro_name = self.macro_name.clone();
        if self.args.is_some() {
            eval_args.macro_args = self.args.as_ref().unwrap().clone();
        }

        eval_args
    }
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
// dbt man --schema selector --schema project

impl ManArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let eval_args = self.common_args.to_eval_args(arg, in_dir, out_dir);
        eval_args.set_schema(self.schema.clone())
    }
}

#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct DebugArgs {
    // Flattened IO args
    #[clap(flatten)]
    pub common_args: CommonArgs,

    /// When set, skip any non-connection related debug steps
    #[arg(long, default_value_t = false)]
    pub connection: bool,
}

impl DebugArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let mut eval_args = self.common_args.to_eval_args(arg, in_dir, out_dir);
        eval_args.phase = Phases::Debug;
        eval_args.set_connection(self.connection)
    }
}

#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct RetryArgs {
    // Flattened Common args (includes --state global flag)
    #[clap(flatten)]
    pub common_args: CommonArgs,

    /// Override static analysis setting for retry. If not specified, uses the setting from the
    /// original run (if available) or defaults to "on".
    #[arg(long, env = "DBT_STATIC_ANALYSIS")]
    pub static_analysis: Option<StaticAnalysisKind>,
}

impl RetryArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let mut eval_args = self.common_args.to_eval_args(arg, in_dir, out_dir);
        eval_args.phase = Phases::All;
        // Note: static_analysis is applied later in retry handling based on original run settings
        if let Some(sa) = self.static_analysis {
            eval_args.static_analysis = sa;
        }
        eval_args
    }
}

// reference: https://docs.getdbt.com/reference/global-configs/about-global-configs
#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct CommonArgs {
    /// The target to execute
    #[arg(global = true, short = 't', long, env = "DBT_TARGET")]
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
    pub vars: Option<BTreeMap<String, YValue>>,

    /// Select nodes to run
    // has no ENV_VAR
    #[arg(global = true, long, short = 's', value_parser = check_selector, num_args(1..), value_delimiter = ' ', group = "selector_or_select")]
    // This is a deprecated legacy alias for '--select'. It is not visible in the help and should be removed (eventually).
    #[clap(alias("models"), short_alias('m'))]
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
    // has no ENV_VAR
    #[arg(global = true, long)]
    pub threads: Option<usize>,

    /// Overrides threads.
    #[arg(global = true, long = "single-threaded", action = ArgAction::SetTrue, env = "DBT_SINGLE_THREADED", value_parser = BoolishValueParser::new())]
    pub single_threaded: bool,

    /// Execution backend to use for local runs
    #[arg(
        global = true,
        long = "compute",
        value_enum,
        env = "DBT_COMPUTE",
        default_value = "inline"
    )]
    pub compute: ComputeArg,

    /// Host address
    #[arg(long, default_value = "127.0.0.1", value_name = "HOST")]
    pub host: String,

    /// Port number
    #[arg(long, default_value_t = 8000, value_name = "PORT")]
    pub port: u16,

    /// Warn on error (TODO: need to wire this in)
    #[arg(global = true, long, default_value = "false", action = ArgAction::SetTrue, env = "DBT_WARN_ERROR",hide = true, value_parser = BoolishValueParser::new())]
    pub warn_error: bool,
    #[arg(global = true, long, default_value = "false", action = ArgAction::SetTrue,  env = "DBT_WARN_ERROR",hide = true, value_parser = BoolishValueParser::new())]
    pub no_warn_error: bool,

    /// Warning error options
    #[arg(global = true, long,value_parser = check_var,
        env = "DBT_WARN_ERROR_OPTIONS",
        hide = true )]
    pub warn_error_options: Option<BTreeMap<String, YValue>>,

    // TODO: currently only used to avoid suppressing warnings/errors from dependencies
    /// Show all deprecations warnings/errors instead of one per package
    #[arg(global = true, long, default_value = "false", action = ArgAction::SetTrue, env = "DBT_SHOW_ALL_DEPRECATIONS",hide = true, value_parser = BoolishValueParser::new())]
    pub show_all_deprecations: bool,

    /// Debug flag
    #[arg(global = true, long, short = 'd', default_value = "false", action = ArgAction::SetTrue,  env = "DBT_DEBUG", value_parser = BoolishValueParser::new(),hide = true)]
    pub debug: bool,
    #[arg(global = true, long, default_value = "false", action = ArgAction::SetTrue,  env = "DBT_DEBUG", value_parser = BoolishValueParser::new(),hide = true)]
    pub no_debug: bool,

    /// Introspect flag
    #[arg(global = true, long,  default_value = "false", action = ArgAction::SetTrue,  env = "DBT_INTROSPECT", value_parser = BoolishValueParser::new(),hide = true)]
    pub introspect: bool,
    #[arg(global = true, long, default_value = "false", action = ArgAction::SetTrue, value_parser = BoolishValueParser::new(),hide = true)]
    pub no_introspect: bool,

    /// Write JSON artifacts to disk [env: DBT_WRITE_JSON=]. Use --no-write-json to suppress writing JSON artifacts.
    #[arg(global = true, long,  default_value_t=true,  action = ArgAction::SetTrue, env = "DBT_WRITE_JSON", value_parser = BoolishValueParser::new())]
    pub write_json: bool,
    #[arg(global = true,long,action = ArgAction::SetTrue,  default_value_t=false, value_parser = BoolishValueParser::new(),hide = true)]
    pub no_write_json: bool,

    /// Write a catalog.json file to the target directory
    #[arg(global = true, long, default_value_t=false,  action = ArgAction::SetTrue, env = "DBT_WRITE_CATALOG", value_parser = BoolishValueParser::new())]
    pub write_catalog: bool,

    // support of build cache
    //
    /// Enable the build cache with default settings (equivalent to --build-cache-mode=readwrite)
    #[arg(global = true, long, hide = true)]
    pub use_build_cache: bool,
    /// The mode to use for the build cache (read, write, readwrite, or noop) [default: noop]
    #[arg(global = true, long, env = "DBT_BUILD_CACHE_MODE", hide = true)]
    pub build_cache_mode: Option<BuildCacheMode>,
    /// The URL for the build cache nodes Parquet file (e.g., s3://my-bucket/build-cache/nodes.parquet) [default: target/build_cache/nodes.parquet]
    #[arg(global = true, long, env = "DBT_BUILD_CACHE_NODES_URL", hide = true)]
    pub build_cache_nodes_url: Option<String>,
    /// The URL for the build cache CAS Parquet file (e.g., s3://my-bucket/build-cache/cas.parquet) [default: target/build_cache/cas.parquet]
    #[arg(global = true, long, env = "DBT_BUILD_CACHE_CAS_URL", hide = true)]
    pub build_cache_cas_url: Option<String>,

    // Support for query cache
    #[arg(global = true, long, env = "DBT_BETA_USE_QUERY_CACHE", hide = true)]
    pub beta_use_query_cache: bool,

    //
    // NOTE: The arguments below were generated by a script to temporarily fill gaps between fs and
    // dbt cli parsing. They may not actually be implemented, yet. If you implement them, move them
    // above this comment. Script at: https://github.com/dbt-labs/dbt-mantle/blob/7fff1e9b9ed1203454447e68cf298be788255d9f/scripts/cli-click-to-clap.py
    //
    // At start of run, populate relational cache only for schemas containing selected nodes, or for all schemas of interest.
    #[arg(global = true, long, default_value = "false", action = ArgAction::SetTrue, env = "DBT_CACHE_SELECTED_ONLY", value_parser = BoolishValueParser::new(),hide = true)]
    pub cache_selected_only: bool,
    #[arg(global = true, long, default_value = "false", action = ArgAction::SetTrue, env = "DBT_CACHE_ALL_SCHEMAS", value_parser = BoolishValueParser::new(),hide = true)]
    pub no_cache_selected_only: bool,

    /// Skip writing msgpack files if they already exist, deprecated
    #[arg(global = true, long = "skip-write-msgpack-if-exist", action = ArgAction::SetTrue, value_parser = BoolishValueParser::new(), hide = true)]
    pub skip_write_msgpack_if_exist: bool,

    // If set, resolve unselected nodes by deferring to the manifest within the --state directory.
    #[arg(global = true, long = "defer", action = ArgAction::SetTrue, env = "DBT_DEFER", value_parser = BoolishValueParser::new())]
    pub defer: bool,
    #[arg(global = true, long= "no-defer", default_value_t=false, action = ArgAction::SetTrue, value_parser = BoolishValueParser::new(), hide = true)]
    pub no_defer: bool,

    /// Override the state directory for deferral only.
    #[arg(global = true, long, env = "DBT_DEFER_STATE", hide = true)]
    pub defer_state: Option<PathBuf>,

    /// Unless overridden, use this state directory for both state comparison and deferral.
    #[arg(global = true, long, env = "DBT_STATE")]
    pub state: Option<PathBuf>,

    // Stop execution on first failure.
    #[arg(global = true, long, default_value = "false", action = ArgAction::SetTrue, env = "DBT_FAIL_FAST", short = 'x', value_parser = BoolishValueParser::new(),hide = true)]
    pub fail_fast: bool,
    #[arg(global = true, long, default_value = "false", action = ArgAction::SetTrue, env = "DBT_NO_FAIL_FAST", value_parser = BoolishValueParser::new(),hide = true)]
    pub no_fail_fast: bool,

    // If set, defer to the argument provided to the state flag for resolving unselected nodes, even if the node(s) exist as a database object in the current environment.
    #[arg(global = true, long = "favor-state", default_value = "false",action = ArgAction::SetTrue, env = "DBT_FAVOR_STATE", value_parser = BoolishValueParser::new(),hide = true)]
    pub favor_state: bool,
    #[arg(global = true, long, default_value = "false", action = ArgAction::SetTrue, env = "DBT_NO_FAVOR_STATE", value_parser = BoolishValueParser::new(),hide = true)]
    pub no_favor_state: bool,

    // Enable verbose logging for relational cache events to help when debugging.
    #[arg(global = true, long = "log-cache-events", default_value = "false", action = ArgAction::SetTrue, env = "DBT_LOG_CACHE_EVENTS", value_parser = BoolishValueParser::new(),hide = true)]
    pub log_cache_events: bool,
    #[arg(global = true, long, default_value = "false", action = ArgAction::SetTrue, env = "DBT_NO_LOG_CACHE_EVENTS", value_parser = BoolishValueParser::new(),hide = true)]
    pub no_log_cache_events: bool,

    // logging
    //
    /// Set 'log-path' for the current run, overriding 'DBT_LOG_PATH'.
    #[arg(global = true, long, env = "DBT_LOG_PATH")]
    pub log_path: Option<PathBuf>,

    /// Set 'otel-file-name' for the current run, overriding 'DBT_OTEL_FILE_NAME'.
    /// If set, OTEL telemetry will be written to `$log_path/otel-file-name`.
    #[arg(global = true, long = "otel-file-name", env = "DBT_OTEL_FILE_NAME")]
    pub otel_file_name: Option<String>,

    /// Set 'otel-parquet-file-name' for the current run, overriding 'DBT_OTEL_PARQUET_FILE_NAME'.
    /// If set, OTEL telemetry will be written to `$target_path/metadata/otel-parquet-file-name` in Parquet format.
    #[arg(
        global = true,
        long = "otel-parquet-file-name",
        env = "DBT_OTEL_PARQUET_FILE_NAME"
    )]
    pub otel_parquet_file_name: Option<String>,

    /// Configure the max file size in bytes for a single dbt.log file, before rolling over. 0 means no limit.
    #[arg(global = true, long, default_value = "false", env = "DBT_LOG_FILE_MAX_BYTES", value_parser = BoolishValueParser::new(),hide = true)]
    pub log_file_max_bytes: bool,
    #[arg(global = true, long, default_value = "false", env = "DBT_LOG_FILE_MAX_BYTES", value_parser = BoolishValueParser::new(),hide = true)]
    pub no_log_file_max_bytes: bool,

    /// Set logging format; use --log-format-file to override.
    #[arg(global = true, long, env = "DBT_LOG_FORMAT", default_value_t = LogFormat::Default,)]
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

    #[arg(global = true, long, default_value_t = false, action = ArgAction::SetTrue, env = "DBT_MACRO_DEBUGGING", value_parser = BoolishValueParser::new(),hide = true)]
    pub macro_debugging: bool,
    #[arg(global = true, long, default_value_t = false, action = ArgAction::SetTrue, env = "DBT_MACRO_DEBUGGING", value_parser = BoolishValueParser::new(),hide = true)]
    pub no_macro_debugging: bool,

    // Allow for partial parsing by looking for and writing to a pickle file in the target directory. This overrides the user configuration file.
    #[arg(global = true, long , default_value_t = false,  action = ArgAction::SetTrue, env = "DBT_PARTIAL_PARSE", value_parser = BoolishValueParser::new(), hide = true)]
    pub partial_parse: bool,
    #[arg(global = true, long, default_value_t = false,  action = ArgAction::SetTrue, env = "DBT_PARTIAL_PARSE", value_parser = BoolishValueParser::new(), hide = true)]
    pub no_partial_parse: bool,

    #[arg(global = true, long ,default_value_t = false, action = ArgAction::SetTrue, env = "DBT_PARTIAL_PARSE_FILE_DIFF", value_parser = BoolishValueParser::new(), hide = true)]
    pub partial_parse_file_diff: bool,
    #[arg(global = true, long,default_value_t = false, action = ArgAction::SetTrue, env = "DBT_PARTIAL_PARSE_FILE_DIFF", value_parser = BoolishValueParser::new(), hide = true)]
    pub no_partial_parse_file_diff: bool,

    #[arg(global = true, long, env = "DBT_PARTIAL_PARSE_FILE_PATH", hide = true)]
    pub partial_parse_file_path: Option<PathBuf>,

    // At start of run, use `show` or `information_schema` queries to populate a relational cache, which can speed up subsequent materializations.
    #[arg(global = true, long, default_value_t = false, action = ArgAction::SetTrue, env = "DBT_POPULATE_CACHE", value_parser = BoolishValueParser::new(), hide = true)]
    pub populate_cache: bool,
    #[arg(global = true, long, default_value_t = false, action = ArgAction::SetTrue, env = "DBT_POPULATE_CACHE", value_parser = BoolishValueParser::new(), hide = true)]
    pub no_populate_cache: bool,

    // Output all {{ print() }} macro calls.
    #[arg(global = true, long, default_value_t = false, action = ArgAction::SetTrue, env = "DBT_PRINT", value_parser = BoolishValueParser::new(), hide = true)]
    pub print: bool,
    #[arg(global = true, long, default_value_t = false, action = ArgAction::SetTrue, env = "DBT_PRINT", value_parser = BoolishValueParser::new(), hide = true)]
    pub no_print: bool,

    // Sets the width of terminal output
    #[arg(global = true, long, env = "DBT_PRINTER_WIDTH", value_parser = u32::from_str, default_value_t = 120, hide = true)]
    pub printer_width: u32,

    /// When this option is passed, dbt will output low-level timing stats to the specified file. Example: `--record-timing-info output.profile`
    #[arg(global = true, long, short = 'r', hide = true)]
    pub record_timing_info: Option<PathBuf>,

    // Send anonymous usage stats to dbt Labs.
    #[arg(global = true, long, default_value_t=true, action = ArgAction::SetTrue, env = "DBT_SEND_ANONYMOUS_USAGE_STATS", value_parser = BoolishValueParser::new())]
    pub send_anonymous_usage_stats: bool,
    #[arg(global = true, long, default_value_t=false, action = ArgAction::SetTrue, value_parser = BoolishValueParser::new())]
    pub no_send_anonymous_usage_stats: bool,

    // Use the static parser.
    #[arg(global = true, long, default_value_t=false,  action = ArgAction::SetTrue, env = "DBT_STATIC_PARSER", value_parser = BoolishValueParser::new(), hide = true)]
    pub static_parser: bool,
    #[arg(global = true, long, default_value_t=false,  action = ArgAction::SetTrue, env = "DBT_STATIC_PARSER", value_parser = BoolishValueParser::new(), hide = true)]
    pub no_static_parser: bool,

    // Specify whether log output is colorized in the console and the log file. Use --use-colors-file/--no-use-colors-file to colorize the log file differently than the console.
    #[arg(global = true, long, default_value_t=false,  action = ArgAction::SetTrue, env = "DBT_USE_COLORS", value_parser = BoolishValueParser::new(), hide = true)]
    pub use_colors: bool,
    #[arg(global = true, long, default_value_t=false,  action = ArgAction::SetTrue, env = "DBT_USE_COLORS", value_parser = BoolishValueParser::new(), hide = true)]
    pub no_use_colors: bool,

    // Specify whether log file output is colorized by overriding the default value and the general --use-colors/--no-use-colors setting.
    #[arg(global = true, long, default_value_t=false, action = ArgAction::SetTrue, env = "DBT_USE_COLORS_FILE", value_parser = BoolishValueParser::new(), hide = true)]
    pub use_colors_file: bool,
    #[arg(global = true, long, default_value_t=false, action = ArgAction::SetTrue, env = "DBT_USE_COLORS_FILE", value_parser = BoolishValueParser::new(), hide = true)]
    pub no_use_colors_file: bool,

    // Enable experimental parsing features.
    #[arg(global = true, long, default_value_t=false, action = ArgAction::SetTrue, env = "DBT_USE_EXPERIMENTAL_PARSER", value_parser = BoolishValueParser::new(), hide = true)]
    pub use_experimental_parser: bool,
    #[arg(global = true, long, default_value_t=false, action = ArgAction::SetTrue, env = "DBT_USE_EXPERIMENTAL_PARSER", value_parser = BoolishValueParser::new(), hide = true)]
    pub no_use_experimental_parser: bool,

    #[arg(global = true, long, default_value_t=false,  action = ArgAction::SetTrue, env = "DBT_USE_FAST_TEST_EDGES", value_parser = BoolishValueParser::new(), hide = true)]
    pub use_fast_test_edges: bool,
    #[arg(global = true, long, default_value_t=false,  action = ArgAction::SetTrue, env = "DBT_USE_FAST_TEST_EDGES", value_parser = BoolishValueParser::new(), hide = true)]
    pub no_use_fast_test_edges: bool,

    // If set, ensure the installed dbt version matches the require-dbt-version specified in the dbt_project.yml file (if any). Otherwise, allow them to differ.
    #[arg(global = true, long , default_value_t=true,  action = ArgAction::SetTrue, env = "DBT_VERSION_CHECK", value_parser = BoolishValueParser::new(), hide=true)]
    pub version_check: bool,
    /// Disable online version check for dbt-fusion updates
    #[arg(global = true, long = "no-version-check", default_value_t=false,  action = ArgAction::SetTrue, env = "DBT_DISABLE_VERSION_CHECK", value_parser = BoolishValueParser::new())]
    pub no_version_check: bool,

    // If set, run models in a project while building only the schemas
    // reference: https://docs.getdbt.com/docs/build/empty-flag
    #[arg(global = true, long , default_value_t=false,  action = ArgAction::SetTrue, value_parser = BoolishValueParser::new(), hide=true, overrides_with = "_no_empty")]
    pub empty: bool,
    #[arg(global = true, long = "no-empty", default_value_t=false,  action = ArgAction::SetTrue, value_parser = BoolishValueParser::new(), hide=true)]
    pub _no_empty: bool,

    /// Store test failures in the database (equivalent to dbt-core `--store-failures`).
    #[arg(
        global = true,
        long = "store-failures",
        default_value_t = false,
        action = ArgAction::SetTrue,
        env = "DBT_STORE_FAILURES",
        value_parser = BoolishValueParser::new()
    )]
    pub store_failures: bool,

    // --------------------------------------------------------------------------------------------
    // fs specific public options
    #[clap(
    long,
    num_args(0..),
    help = "Show produced artifacts [default: 'progress']\n"
)]
    pub show: Vec<ShowOptions>,

    /// Run the following phases [default: derived from command]
    #[arg(global = true, long, hide = true)]
    pub phase: Option<Phases>,

    // --------------------------------------------------------------------------------------------
    // fs specific public options
    // Task cache coordination URL. Supports:
    // - `noop` for no coordination (single-process only)
    // - `file://<path>` for local file-based cache
    // - `redis://<host>` for shared Redis coordination
    //
    /// Task cache coordination URL. Use `redis://<host>` for shared Redis coordination
    #[clap(long, env = "DBT_TASK_CACHE_URL", default_value = "noop", hide = !cfg!(feature = "task_cache_url"))]
    pub task_cache_url: String,

    // --------------------------------------------------------------------------------------------
    // internal only
    /// The directory to install fs_internal packages
    #[arg(
        global = true,
        long,
        env = "DBT_FS_INTERNAL_PACKAGES_INSTALL_PATH",
        hide = true
    )]
    pub internal_packages_install_path: Option<PathBuf>,

    /// Enable OTLP export tracing. Use OTEL_EXPORTER_OTLP_ENDPOINT to set the endpoint.
    #[arg(global = true, long, default_value = "false", action = ArgAction::SetTrue,  env = "DBT_EXPORT_TO_OTLP", value_parser = BoolishValueParser::new(), hide = true)]
    pub export_to_otlp: bool,

    /// Path for dbt replay functionality (Using _DBT_REPLAY env var for Shadow Scenarios)
    #[arg(
        global = true,
        long,
        group = "replay_mode",
        env = "_DBT_REPLAY",
        hide = true
    )]
    pub dbt_replay: Option<PathBuf>,

    /// Path for recording SQL queries
    #[arg(global = true, long, group = "replay_mode", hide = true)]
    pub fs_record: Option<PathBuf>,

    /// Path for replaying SQL queries
    #[arg(global = true, long, group = "replay_mode", hide = true)]
    pub fs_replay: Option<PathBuf>,

    // -------------------------------------------------------------------------
    // Time Machine arguments
    // -------------------------------------------------------------------------
    /// Time Machine mode: record adapter calls for later replay, or replay from artifact.
    /// Use with --time-machine-path to specify the recording/artifact location.
    #[arg(
        global = true,
        long = "time-machine",
        env = "DBT_TIME_MACHINE_MODE",
        value_enum,
        hide = true
    )]
    pub time_machine_mode: Option<TimeMachineModeKind>,

    /// Path for Time Machine recording output or replay artifact input.
    /// In record mode: directory to write the recording (default: <target>/time_machine/{run_id})
    /// In replay mode: path to the recorded artifact directory (default: <target>/time_machine)
    #[arg(
        global = true,
        long = "time-machine-path",
        env = "DBT_TIME_MACHINE_PATH",
        hide = true
    )]
    pub time_machine_path: Option<PathBuf>,

    /// [Replay mode] Replay ordering mode: strict (exact sequence) or semantic (flexible reads).
    #[arg(
        global = true,
        long = "time-machine-ordering",
        env = "DBT_TIME_MACHINE_ORDERING",
        value_enum,
        default_value = "strict",
        hide = true
    )]
    pub time_machine_ordering: TimeMachineReplayOrdering,

    /// TEMPORARY same as sql-analysis=none.
    #[arg(global = true, long, default_value = "false", action = ArgAction::SetTrue, hide = true)]
    pub legacy_compile: bool,

    /// Flag for compile conformance
    #[arg(global = true, long, default_value = "false", action = ArgAction::SetTrue, hide= true)]
    pub check_conformance: bool,

    /// Flag for semantic manifest validation
    #[arg(global = true, env = "DBT_SKIP_SEMANTIC_MANIFEST_VALIDATION", long, default_value = "false", action = ArgAction::SetTrue, value_parser = BoolishValueParser::new(), hide= true)]
    pub skip_semantic_manifest_validation: bool,

    /// Flag to enable Semantic Layer exports
    #[arg(global = true, env = "DBT_EXPORT_SAVED_QUERIES", long, default_value = "false", action = ArgAction::SetTrue, value_parser = BoolishValueParser::new(), hide= true)]
    pub export_saved_queries: bool,

    #[arg(global = true, long, default_value = "false", action = ArgAction::SetTrue, hide = true)]
    pub skip_unreferenced_table_check: bool,

    /// Override the invocation ID with a UUID string or integer.
    #[arg(global = true, env = "DBT_INVOCATION_ID", long, value_parser = parse_invocation_id)]
    pub invocation_id: Option<Uuid>,

    /// Set the parent span ID for trace correlation (16-character hex or u64).
    #[arg(global = true, env = "DBT_PARENT_SPAN_ID", long, value_parser = parse_parent_span_id)]
    pub parent_span_id: Option<u64>,

    /// Skip installation of private dependencies (useful for build conformance testing)
    #[arg(global = true, long, default_value = "false", action = ArgAction::SetTrue, hide = true)]
    pub skip_private_deps: bool,
}

fn resolve_show_arg(show_arg: &[ShowOptions], quiet: bool) -> HashSet<ShowOptions> {
    let mut show = if show_arg.contains(&ShowOptions::All) {
        ShowOptions::iter().collect()
    } else if show_arg.contains(&ShowOptions::None) {
        HashSet::new()
    } else if show_arg.is_empty() {
        HashSet::from([ShowOptions::Progress, ShowOptions::Completed])
    } else {
        show_arg
            .iter()
            .cloned()
            .flat_map(|opt| {
                if opt == ShowOptions::Progress {
                    vec![
                        ShowOptions::Progress,
                        ShowOptions::ProgressParse,
                        ShowOptions::ProgressHydrate,
                        ShowOptions::ProgressRender,
                        ShowOptions::ProgressAnalyze,
                        ShowOptions::ProgressRun,
                        ShowOptions::Completed,
                    ]
                } else {
                    vec![opt]
                }
            })
            .collect()
    };

    // quiet overrules all show options
    if quiet {
        show = HashSet::new();
    }

    show
}

impl CommonArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let select_option = self.select.clone().map(|selectors| {
            let mut expr = parse_model_specifiers(&selectors).unwrap();
            // Apply indirect selection from CLI if specified (matching dbt-core behavior)
            if let Some(mode) = self.indirect_selection {
                expr.set_indirect_selection(mode);
            }
            expr
        });

        let exclude_option = self.exclude.clone().map(|selectors| {
            let mut expr = parse_model_specifiers(&selectors).unwrap();
            // Apply indirect selection from CLI if specified (matching dbt-core behavior)
            if let Some(mode) = self.indirect_selection {
                expr.set_indirect_selection(mode);
            }
            expr
        });
        let show = resolve_show_arg(self.show.as_slice(), self.quiet);
        let replay = pick_replay_mode(self, arg.io.invocation_id, out_dir);
        let build_cache_mode = if self.use_build_cache {
            Some(BuildCacheMode::ReadWrite)
        } else {
            self.build_cache_mode
        };
        EvalArgs {
            command: arg.command,
            io: IoArgs {
                show,
                is_compile: arg.command == FsCommand::Compile,
                debug: self.debug,
                invocation_id: arg.io.invocation_id,
                otel_parent_span_id: arg.io.otel_parent_span_id,
                in_dir: in_dir.to_path_buf(),
                out_dir: out_dir.to_path_buf(),
                db_root: None,
                send_anonymous_usage_stats: self.get_send_anonymous_usage_stats(),
                status_reporter: arg.io.status_reporter.clone(),
                log_format: self.log_format,
                log_level: self.log_level,
                log_level_file: self.log_level_file,
                log_path: self.log_path.clone(),
                otel_file_name: self.otel_file_name.clone(),
                otel_parquet_file_name: self.otel_parquet_file_name.clone(),
                export_to_otlp: self.export_to_otlp,
                show_all_deprecations: self.show_all_deprecations,
                show_timings: arg.io.show_timings,
                build_cache_mode,
                build_cache_url: arg.io.build_cache_url.clone(),
                build_cache_cas_url: arg.io.build_cache_cas_url.clone(),
                beta_use_query_cache: self.beta_use_query_cache,
                host: self.host.clone(),
                port: self.port,
            },
            profiles_dir: self.profiles_dir.clone(),
            packages_install_path: self.packages_install_path.clone(),
            internal_packages_install_path: self.internal_packages_install_path.clone(),
            add_package: None, // doesn't come from CommonArgs, comes from DepsArgs
            upgrade: false,    // comes from DepsArgs
            lock: false,       // comes from DepsArgs
            profile: self.profile.clone(),
            target: self.target.clone(),
            update_deps: false,
            vars: self.vars.clone().unwrap_or_default(),
            phase: self.phase.clone().unwrap_or(Phases::All),
            format: DEFAULT_FORMAT,
            limit: if arg.command == FsCommand::Extension("sl") {
                Some(100)
            } else {
                Some(10)
            },
            from_main: false,
            // note: we use
            // - 0 for free threading,
            // - 1 for single threading and
            // - > 1 for fixed number of threads
            num_threads: if self.single_threaded {
                Some(1)
            } else {
                self.threads
            },
            select: select_option,
            exclude: exclude_option,
            indirect_selection: self.indirect_selection,
            replay,
            interactive: false,
            check_conformance: self.check_conformance,
            skip_semantic_manifest_validation: self.skip_semantic_manifest_validation,
            export_saved_queries: self.export_saved_queries,
            max_depth: 0,
            show_scans: false,
            use_fqtn: false,
            schema: vec![],
            output_keys: vec![],
            skip_unreferenced_table_check: self.skip_unreferenced_table_check,
            state: self.state.clone(),
            defer_state: self.defer_state.clone(),
            connection: false,
            macro_name: "".to_string(),
            macro_args: BTreeMap::new(),
            selector: self.selector.clone(),
            resource_types: vec![],
            exclude_resource_types: vec![],
            //flags
            warn_error: self.warn_error,
            warn_error_options: self.warn_error_options.clone().unwrap_or_default(),
            version_check: if self.no_version_check {
                false
            } else {
                self.version_check
            },
            introspect: true,
            defer: if self.no_defer {
                false
            } else if self.defer {
                true
            } else {
                // default to defer if neither flag is passed
                true
            },
            debug: self.debug,
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
            send_anonymous_usage_stats: self.get_send_anonymous_usage_stats(),
            write_json: if self.no_write_json {
                false
            } else {
                self.write_json
            },
            write_catalog: self.write_catalog,
            fail_fast: self.fail_fast,
            target_path: self.target_path.clone(),
            empty: self.empty,
            sample: None,
            favor_state: self.favor_state,
            refresh_sources: false,
            run_cache_mode: RunCacheMode::Noop,
            task_cache_url: self.task_cache_url.clone(),
            static_analysis: StaticAnalysisKind::default(),
            full_refresh: false,
            store_failures: self.store_failures,
            check_all: false,
            sample_renaming: BTreeMap::new(),
            optimize_tests: Default::default(),
            local_execution_backend: self.compute.into(),
            skip_checkpoints: false,
            skip_private_deps: self.skip_private_deps,
        }
    }

    pub fn get_send_anonymous_usage_stats(&self) -> bool {
        if self.no_send_anonymous_usage_stats {
            false
        } else {
            self.send_anonymous_usage_stats
        }
    }

    pub fn get_introspect(&self) -> bool {
        !self.no_introspect
    }
}

impl From<ComputeArg> for LocalExecutionBackendKind {
    fn from(arg: ComputeArg) -> Self {
        match arg {
            ComputeArg::Inline => LocalExecutionBackendKind::Inline,
            ComputeArg::Sidecar => LocalExecutionBackendKind::Worker,
            ComputeArg::Service => LocalExecutionBackendKind::Service,
        }
    }
}

#[derive(
    Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, ValueEnum, Display, Default,
)]
#[serde(rename_all = "kebab-case")]
#[clap(rename_all = "kebab-case")]
pub enum ComputeArg {
    #[default]
    /// Run computations in-process
    Inline,
    /// Run computations in a separate, ephemeral worker process
    Sidecar,
    /// Run via the remote compute service (persistent workers/cluster).
    Service,
    // Warehouse is the remote compute, e.g. Snowflake, BigQuery, etc.
    // Warehouse
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
pub enum WarnErrorOptions {
    #[default]
    All,
    InvalidTests,
    Deprecation,
    VersionMismatch,
}

/// Maintain the system: update and uninstall
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

/// Manage system status
#[derive(clap::Parser, Debug, Clone, Serialize, Deserialize)]
#[command()]
pub enum SystemCommand {
    /// Update dbt in place to the latest version
    Update(SystemUpdateArgs),
    /// Uninstall dbt from the system
    Uninstall(SystemUninstallArgs),
    /// Preinstall all supported database drivers into the local cache
    InstallDrivers,
}

#[derive(Parser, Debug, Clone, Serialize, Deserialize)]
pub struct SystemUpdateArgs {
    /// Update dbt to this version (e.g. 1.2.3) [default: latest version]
    #[arg(global = true, long)]
    pub version: Option<String>,

    /// Package to update (e.g. dbt) [default: dbt]
    #[arg(long)]
    pub package: Option<String>,
}

#[derive(Parser, Debug, Clone, Serialize, Deserialize)]
pub struct SystemUninstallArgs {}

#[derive(
    Debug, Copy, Clone, PartialEq, Eq, Default, Display, ValueEnum, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ProjectTemplate {
    #[default]
    JaffleShop,
    MomsFlowerShop,
}

#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct InitArgs {
    /// The name of the project to create
    #[arg(long, default_value = "jaffle_shop", value_parser = validate_project_name)]
    pub project_name: String,

    /// Skip interactive profile setup
    #[arg(long, default_value = "false")]
    pub skip_profile_setup: bool,

    /// Run the Fusion onboarding/upgrade flow
    #[arg(long, default_value = "false")]
    pub fusion_upgrade: bool,

    /// The sample project to initialize with
    #[arg(long, default_value = "jaffle-shop")]
    pub sample: ProjectTemplate,

    // Flattened Common args
    #[clap(flatten)]
    pub common_args: CommonArgs,
}

impl InitArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let show = if arg.io.show.contains(&ShowOptions::All) {
            ShowOptions::iter().collect()
        } else if arg.io.show.is_empty() {
            HashSet::from([ShowOptions::Progress, ShowOptions::Completed])
        } else {
            arg.io.show.iter().cloned().collect()
        };
        EvalArgs {
            command: arg.command,
            from_main: arg.from_main,
            io: IoArgs {
                in_dir: in_dir.to_path_buf(),
                out_dir: out_dir.to_path_buf(),
                db_root: None,
                show,
                is_compile: arg.command == FsCommand::Compile,
                debug: arg.io.debug,
                invocation_id: arg.io.invocation_id,
                otel_parent_span_id: arg.io.otel_parent_span_id,
                send_anonymous_usage_stats: self.common_args.get_send_anonymous_usage_stats(),
                status_reporter: arg.io.status_reporter.clone(),
                log_format: self.common_args.log_format,
                log_level: self.common_args.log_level,
                log_level_file: self.common_args.log_level_file,
                log_path: self.common_args.log_path.clone(),
                otel_file_name: self.common_args.otel_file_name.clone(),
                otel_parquet_file_name: self.common_args.otel_parquet_file_name.clone(),
                show_all_deprecations: self.common_args.show_all_deprecations,
                show_timings: arg.from_main,
                build_cache_mode: self.common_args.build_cache_mode,
                build_cache_url: self.common_args.build_cache_nodes_url.clone(),
                build_cache_cas_url: self.common_args.build_cache_cas_url.clone(),
                export_to_otlp: self.common_args.export_to_otlp,
                beta_use_query_cache: self.common_args.beta_use_query_cache,
                host: self.common_args.host.clone(),
                port: self.common_args.port,
            },
            task_cache_url: "noop".to_string(),
            favor_state: self.common_args.favor_state,
            phase: Phases::Debug,
            send_anonymous_usage_stats: self.common_args.get_send_anonymous_usage_stats(),
            ..Default::default()
        }
    }
}

pub fn from_main(cli: &Cli) -> SystemArgs {
    let common_args = cli.common_args();
    let command = cli.as_command();

    SystemArgs {
        command,
        io: IoArgs {
            invocation_id: common_args.invocation_id.unwrap_or_else(Uuid::now_v7),
            otel_parent_span_id: common_args.parent_span_id,
            show: resolve_show_arg(common_args.show.as_slice(), common_args.quiet),
            is_compile: command == FsCommand::Compile,
            debug: common_args.debug,
            in_dir: PathBuf::new(),
            out_dir: PathBuf::new(),
            db_root: None,
            send_anonymous_usage_stats: common_args.get_send_anonymous_usage_stats(),
            status_reporter: None,
            log_format: common_args.log_format,
            log_level: match (common_args.debug, common_args.log_level) {
                (true, Some(LevelFilter::Trace)) => Some(LevelFilter::Trace),
                (true, _) => Some(LevelFilter::Debug),
                (false, _) => common_args.log_level,
            },
            log_level_file: match (common_args.debug, common_args.log_level_file) {
                (true, Some(LevelFilter::Trace)) => Some(LevelFilter::Trace),
                (true, _) => Some(LevelFilter::Debug),
                (false, _) => common_args.log_level_file,
            },
            log_path: common_args.log_path,
            export_to_otlp: common_args.export_to_otlp,
            otel_file_name: common_args.otel_file_name,
            otel_parquet_file_name: common_args.otel_parquet_file_name,
            show_all_deprecations: common_args.show_all_deprecations,
            show_timings: true,
            build_cache_mode: common_args.build_cache_mode,
            build_cache_url: common_args.build_cache_nodes_url,
            build_cache_cas_url: common_args.build_cache_cas_url,
            beta_use_query_cache: common_args.beta_use_query_cache,
            host: common_args.host,
            port: common_args.port,
        },
        from_main: true,

        target: common_args.target,
        num_threads: common_args.threads,
    }
}

pub fn from_lib(cli: &Cli) -> SystemArgs {
    let common_args = cli.common_args();
    let command = cli.as_command();

    SystemArgs {
        command,
        io: IoArgs {
            invocation_id: common_args.invocation_id.unwrap_or_else(Uuid::now_v7),
            otel_parent_span_id: common_args.parent_span_id,
            show: resolve_show_arg(common_args.show.as_slice(), common_args.quiet),
            is_compile: command == FsCommand::Compile,
            debug: common_args.debug,
            in_dir: PathBuf::new(),
            out_dir: PathBuf::new(),
            db_root: None,
            send_anonymous_usage_stats: common_args.get_send_anonymous_usage_stats(),
            status_reporter: None,
            // should_cancel_compilation: None,
            log_format: common_args.log_format,
            log_level: common_args.log_level,
            log_level_file: common_args.log_level_file,
            log_path: common_args.log_path,
            export_to_otlp: common_args.export_to_otlp,
            otel_file_name: common_args.otel_file_name,
            otel_parquet_file_name: common_args.otel_parquet_file_name,
            show_all_deprecations: common_args.show_all_deprecations,
            show_timings: false,
            build_cache_mode: common_args.build_cache_mode,
            build_cache_url: common_args.build_cache_nodes_url,
            build_cache_cas_url: common_args.build_cache_cas_url,
            beta_use_query_cache: common_args.beta_use_query_cache,
            host: common_args.host,
            port: common_args.port,
        },
        from_main: false,
        target: common_args.target,
        num_threads: common_args.threads,
    }
}
