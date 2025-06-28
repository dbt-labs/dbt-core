use clap::{builder::BoolishValueParser, ArgAction, ValueEnum};
use console::Style;
use dbt_serde_yaml::Value;
use log::LevelFilter;
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;
use std::{
    collections::{BTreeMap, HashSet},
    fmt,
    path::{Path, PathBuf},
    str::FromStr,
};
use strum::IntoEnumIterator;
use strum_macros::Display;

use crate::constants::NOOP;
use crate::io_args::StaticAnalysisKind;
use crate::io_args::{
    check_selector, check_target, check_var, ClapResourceType, EvalArgs, IoArgs, JsonSchemaTypes,
    Phases, ReplayMode, RunCacheMode, ShowOptions, SystemArgs,
};

use clap::arg;
use clap::{Parser, Subcommand};

use crate::logging::LogFormat;
use crate::node_selector::{
    parse_model_specifiers, IndirectSelection, MethodName, SelectionCriteria,
};

const DEFAULT_LIMIT: usize = 10;
static DEFAULT_FORMAT: LazyLock<String> = LazyLock::new(|| DisplayFormat::Table.to_string());

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

#[derive(Parser, Debug, Clone)]
#[command(
    author,
    name = "dbt-fusion",
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

/// dbt-fusion (fs): A fast and enriched dbt compiler and runner
#[derive(Subcommand, Debug, Clone, Display)]
pub enum Commands {
    /// Initialize a new dbt project
    Init(InitArgs),
    /// Install package dependencies
    Deps(DepsArgs),
    /// Parse models
    Parse(ParseArgs),
    /// Format models
    #[clap(hide = true)]
    Format(FormatArgs),
    /// Lint models
    #[clap(hide = true)]
    Lint(LintArgs),
    /// List selected nodes
    List(ListArgs),
    /// List selected nodes (alias for list)
    Ls(ListArgs),
    /// Compile models
    Compile(CompileArgs),
    /// Run models
    Run(RunArgs),
    /// Run the named macro with any supplied arguments
    RunOperation(RunOperationArgs),
    /// Test models
    Test(TestArgs),
    /// Seed models
    Seed(SeedArgs),
    /// Run snapshot models
    Snapshot(SnapshotArgs),
    /// Show a preview of the selected nodes
    Show(ShowArgs),
    /// Build seeds, models and tests
    Build(BuildArgs),
    /// Remove target directories
    Clean(CleanArgs),
    /// Run sources subcommands
    Source(SourceArgs),
    /// Generate lineage information for models
    #[clap(hide = true)]
    Lineage(LineageArgs),
    /// dbt installation configuration
    System(SystemMgmtArgs),
    /// Create reference documentation
    Man(ManArgs),
    /// Profile connection debugging
    Debug(DebugArgs),
}

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
            Commands::Compile(args) => args.to_eval_args(arg, in_dir, out_dir),
            Commands::Parse(args) => args.to_eval_args(arg, in_dir, out_dir),
            Commands::Run(args) => args.to_eval_args(arg, in_dir, out_dir),
            Commands::RunOperation(args) => args.to_eval_args(arg, in_dir, out_dir),
            Commands::Seed(args) => args.to_eval_args(arg, in_dir, out_dir),
            Commands::Snapshot(args) => args.to_eval_args(arg, in_dir, out_dir),
            Commands::Format(args) => args.to_eval_args(arg, in_dir, out_dir),
            Commands::Lint(args) => args.to_eval_args(arg, in_dir, out_dir),
            Commands::Ls(args) => args.to_eval_args(arg, in_dir, out_dir),
            Commands::Test(args) => args.to_eval_args(arg, in_dir, out_dir),
            Commands::Build(args) => args.to_eval_args(arg, in_dir, out_dir),
            Commands::Clean(args) => args.to_eval_args(arg, in_dir, out_dir),
            Commands::Source(args) => args.to_eval_args(arg, in_dir, out_dir),
            Commands::Lineage(args) => args.to_eval_args(arg, in_dir, out_dir),
            Commands::System(args) => args.to_eval_args(arg, in_dir, out_dir),
            Commands::Show(args) => args.to_eval_args(arg, in_dir, out_dir),
            Commands::Man(args) => args.to_eval_args(arg, in_dir, out_dir),
            Commands::Debug(args) => args.to_eval_args(arg, in_dir, out_dir),
        };
        arg.from_main = from_main;
        arg.interactive = self.is_interactive();

        arg
    }

    pub fn is_interactive(&self) -> bool {
        match &self.command {
            Commands::Run(args) => args.interactive,
            Commands::Test(args) => args.interactive,
            Commands::Seed(args) => args.interactive,
            Commands::Snapshot(args) => args.interactive,
            Commands::Build(args) => args.interactive,
            Commands::Compile(args) => args.interactive,
            Commands::Lineage(args) => args.interactive,
            _ => false,
        }
    }

    pub fn common_args(&self) -> CommonArgs {
        match &self.command {
            Commands::Init(args) => args.common_args.clone(),
            Commands::Deps(args) => args.common_args.clone(),
            Commands::List(args) => args.common_args.clone(),
            Commands::Ls(args) => args.common_args.clone(),
            Commands::Parse(args) => args.common_args.clone(),
            Commands::Compile(args) => args.common_args.clone(),
            Commands::Run(args) => args.common_args.clone(),
            Commands::RunOperation(args) => args.common_args.clone(),
            Commands::Seed(args) => args.common_args.clone(),
            Commands::Snapshot(args) => args.common_args.clone(),
            Commands::Format(args) => args.common_args.clone(),
            Commands::Lint(args) => args.common_args.clone(),
            Commands::Test(args) => args.common_args.clone(),
            Commands::Build(args) => args.common_args.clone(),
            Commands::Clean(args) => args.common_args.clone(),
            Commands::Source(args) => args.common_args().clone(),
            Commands::Lineage(args) => args.common_args.clone(),
            Commands::System(args) => args.common_args.clone(),
            Commands::Show(args) => args.common_args.clone(),
            Commands::Man(args) => args.common_args.clone(),
            Commands::Debug(args) => args.common_args.clone(),
        }
    }

    pub fn project_dir(&self) -> Option<PathBuf> {
        self.common_args().project_dir
    }

    pub fn target_path(&self) -> Option<PathBuf> {
        self.common_args().target_path
    }

    pub fn stage(&self) -> Phases {
        // todo: fix this: should take the minimum of the user selection and the default
        match &self.command {
            Commands::Init(_args) => unreachable!("Init command does not need a phase"),
            Commands::Deps(args) => args.common_args.phase.clone().unwrap_or(Phases::Deps),
            Commands::Parse(args) => args.common_args.phase.clone().unwrap_or(Phases::Parse),
            Commands::Format(args) => args.common_args.phase.clone().unwrap_or(Phases::Format),
            Commands::Lint(args) => args.common_args.phase.clone().unwrap_or(Phases::Lint),
            Commands::Ls(args) => args.common_args.phase.clone().unwrap_or(Phases::List),
            Commands::List(args) => args.common_args.phase.clone().unwrap_or(Phases::List),
            Commands::Compile(args) => args.common_args.phase.clone().unwrap_or(Phases::Compile),
            Commands::Run(args) => args.common_args.phase.clone().unwrap_or(Phases::All),
            Commands::RunOperation(args) => args.common_args.phase.clone().unwrap_or(Phases::Parse),
            Commands::Seed(args) => args.common_args.phase.clone().unwrap_or(Phases::All),
            Commands::Snapshot(args) => args.common_args.phase.clone().unwrap_or(Phases::All),
            Commands::Test(args) => args.common_args.phase.clone().unwrap_or(Phases::All),
            Commands::Build(args) => args.common_args.phase.clone().unwrap_or(Phases::All),
            Commands::Clean(_args) => unreachable!("Clean command does not need a phase"),
            Commands::Source(args) => args
                .common_args()
                .phase
                .clone()
                .unwrap_or(Phases::Freshness),
            Commands::Lineage(args) => args.common_args.phase.clone().unwrap_or(Phases::Lineage),
            Commands::System(_args) => unreachable!("System command does not need a phase"),
            Commands::Show(args) => args.common_args.phase.clone().unwrap_or(Phases::Show),
            Commands::Man(_args) => unreachable!("Man command does not need a phase"),
            Commands::Debug(args) => args.common_args.phase.clone().unwrap_or(Phases::Debug),
        }
    }

    pub fn get_command_str(&self) -> &str {
        // generate the command string
        match &self.command {
            Commands::Init(..) => "init",
            Commands::Deps(..) => "deps",
            Commands::Parse(..) => "parse",
            Commands::Format(..) => "format",
            Commands::Lint(..) => "lint",
            Commands::List(..) => "list",
            Commands::Ls(..) => "ls",
            Commands::Compile(..) => "compile",
            Commands::Run(..) => "run",
            Commands::RunOperation(..) => "run-operation",
            Commands::Seed(..) => "seed",
            Commands::Snapshot(..) => "snapshot",
            Commands::Test(..) => "test",
            Commands::Build(..) => "build",
            Commands::Clean(..) => "clean",
            Commands::Source(SourceArgs {
                command: SourceCommand::Freshness(..),
            }) => "freshness",
            Commands::Lineage(..) => "lineage",
            Commands::System(..) => "System",
            Commands::Show(..) => "show",
            Commands::Man(..) => "man",
            Commands::Debug(..) => "debug",
        }
    }
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
    // Flattened Common args
    #[clap(flatten)]
    pub common_args: CommonArgs,
}
impl DepsArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let mut eval_args = self.common_args.to_eval_args(arg, in_dir, out_dir);
        eval_args.phase = Phases::Deps;
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
    #[arg(long, conflicts_with = "select")]
    pub inline: Option<String>,

    /// Display rows in different formats
    #[arg(global = true, long, aliases = ["format"])]
    pub output: Option<DisplayFormat>,

    /// Flag to enable or disable SQL analysis, or to run SQL in unsafe mode,  enabled by default
    #[arg(global = true, long, default_value = "on")]
    pub static_analysis: StaticAnalysisKind,

    /// Drop incremental models and fully recalculate incremental tables.
    #[arg(global = true, long, action = ArgAction::SetTrue, value_parser = BoolishValueParser::new(), short = 'f')]
    pub full_refresh: bool,
}

impl CompileArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let mut eval_args = self.common_args.to_eval_args(arg, in_dir, out_dir);
        eval_args.phase = Phases::Compile;

        // When --inline is used, automatically exclude all project nodes.
        if self.inline.is_some() {
            eval_args.exclude = Some(parse_model_specifiers(&["*".to_string()]).expect(
                "Internal error: Failed to parse wildcard selector '*' for inline compilation.",
            ));
        }
        eval_args.static_analysis = self.static_analysis;
        eval_args.full_refresh = self.full_refresh;
        eval_args.format = self
            .output
            .map(|f| f.to_string())
            .unwrap_or_else(|| DEFAULT_FORMAT.clone());

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

    /// Drop incremental models and fully recalculate incremental tables.
    #[arg(global = true, long, action = ArgAction::SetTrue, value_parser = BoolishValueParser::new(), short = 'f')]
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
        eval_args.phase = Phases::Freshness;
        eval_args.with_refined_node_selectors(Some(predicate))
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
    // Flattened Common args
    #[clap(flatten)]
    pub common_args: CommonArgs,
}

#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct ShowArgs {
    #[clap(flatten)]
    pub common_args: CommonArgs,

    /// Show the given query
    #[arg(long, conflicts_with = "select")]
    pub inline: Option<String>,

    /// Limiting number of shown rows. Run with --limit -1 to remove limit [default: 10]
    #[arg(global = true, long)]
    pub limit: Option<usize>,

    /// Display rows in different formats
    #[arg(global = true, long, aliases = ["format"])]
    pub output: Option<DisplayFormat>,

    /// Flag to enable or disable SQL analysis, or to run SQL in unsafe mode,  enabled by default
    #[arg(global = true, long, default_value = "on")]
    pub static_analysis: StaticAnalysisKind,

    /// Do not perform any local type checking on the show target
    ///
    /// If this is set, any existing data in the remote warehouse will be
    /// displayed regardless of whether it matches the current state of the
    /// local workspace.
    #[arg(long)]
    pub unchecked: bool,
}

impl ShowArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let mut eval_args = self.common_args.to_eval_args(arg, in_dir, out_dir);
        eval_args.phase = Phases::Show;
        eval_args.resource_types = vec![
            ClapResourceType::Model,
            ClapResourceType::Snapshot,
            ClapResourceType::Seed,
        ];
        eval_args.limit = self.limit;
        eval_args.static_analysis = self.static_analysis;
        eval_args.format = self
            .output
            .map(|f| f.to_string())
            .unwrap_or_else(|| DEFAULT_FORMAT.clone());
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
        if eval_args.run_cache_mode.write_cache() {
            eval_args.resource_types = vec![ClapResourceType::Snapshot, ClapResourceType::Source];
        } else {
            eval_args.resource_types = vec![ClapResourceType::Snapshot];
        }
        eval_args
    }
}

#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct FormatArgs {
    // Flattened Common args
    #[clap(flatten)]
    pub common_args: CommonArgs,

    // Layout options
    #[clap(short = 'l', long, num_args(0..), help = layout_help_text())]
    pub layout: Vec<Layout>,
}
impl FormatArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let mut eval_args = self.common_args.to_eval_args(arg, in_dir, out_dir);
        let mut vars = eval_args.vars.clone();
        for layout in self.layout.iter() {
            match layout {
                Layout::Indent(indent) => {
                    vars.insert(
                        "__layout_indent".to_string(),
                        Value::number(indent.to_owned().into()),
                    );
                }
                Layout::Commas(commas) => {
                    vars.insert(
                        "__layout_commas".to_string(),
                        Value::string(match commas {
                            CommaLayout::Leading => "leading".to_string(),
                            CommaLayout::Trailing => "trailing".to_string(),
                        }),
                    );
                }
                Layout::LineLength(line_length) => {
                    vars.insert(
                        "__layout_line_length".to_string(),
                        Value::number(line_length.to_owned().into()),
                    );
                }
            }
        }
        eval_args.phase = Phases::Format;
        eval_args.vars = vars;
        eval_args
    }
}

#[derive(Debug, Clone, Display, PartialEq, Serialize, Deserialize)]
pub enum Layout {
    Indent(usize),
    Commas(CommaLayout),
    LineLength(usize),
}

impl FromStr for Layout {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('=').collect();
        match parts[0] {
            "indent" => Ok(Layout::Indent(parts.get(1).unwrap().parse().unwrap())),
            "commas" => Ok(Layout::Commas(match *parts.get(1).unwrap() {
                "leading" => CommaLayout::Leading,
                "trailing" => CommaLayout::Trailing,
                _ => return Err("Invalid value for commas".into()),
            })),
            "line-length" => Ok(Layout::LineLength(parts.get(1).unwrap().parse().unwrap())),
            _ => Err(format!("Unknown layout option: {s}")),
        }
    }
}

#[derive(Debug, Clone, Display, PartialEq, Serialize, Deserialize)]
pub enum CommaLayout {
    Leading,
    Trailing,
}

fn layout_help_text() -> &'static str {
    r#"
Specify the layout options. Example: `-l layout1 -l layout2`

Available layout options:
* `indent=<number>`: Set the number of spaces to indent.
* `commas=leading|trailing`: Set the comma layout.
* `line-length=<number>`: Set the line length.
"#
}
#[derive(Parser, Debug, Default, Clone)]
pub struct LintArgs {
    // Flattened Common args
    #[clap(flatten)]
    pub common_args: CommonArgs,

    /// Fix the linting issues
    #[arg(long, default_value_t = false)]
    pub fix: bool,

    #[clap(short = 'w', long, num_args(0..), help = lint_warnings_help_text())]
    pub warnings: Vec<LintWarning>,
}
impl LintArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let mut eval_args = self.common_args.to_eval_args(arg, in_dir, out_dir);
        let mut vars = eval_args.vars.clone();
        let linter = translate_lint_warnings(&self.warnings);
        vars.extend(linter.to_btree_map());
        eval_args.phase = Phases::Lint;
        eval_args.vars = vars;
        eval_args
    }
}

pub fn lint_warnings_help_text() -> &'static str {
    r#"
Specify the warnings to apply. Example: `-w warning1 -w warning2`. Later options override former ones.
   Legend: (*) This warning can be automatically fixed by sdf.

Available Warnings
* `all`: Turn on all warnings.
* `none`: Turn off all warnings.
* `error`: Treat warnings as errors.

Capitalization Settings
* `capitalization-keywords=upper|lower|pascal|snake|camel|consistent`: Set capitalization style for keywords. Default: consistent (*)
* `capitalization-literals=upper|lower|pascal|snake|camel|consistent`: Set capitalization style for literals. Default: consistent (*)
* `capitalization-types=upper|lower|pascal|snake|camel|consistent`: Set capitalization style for types. Default: consistent (*)
* `capitalization-functions=upper|lower|pascal|snake|camel|consistent`: Set capitalization style for functions. Default: consistent (*)

Convention and Reference Settings:
* `convention-blocked-words=<word>`: Specify blocked words.
* `convention-terminator`: Warn about terminator conventions. (*)
* `references-keywords`: Warn about keyword references.
* `references-special-chars=<char>`: Warn about special character references.
* `references-quoting`: Warn about quoting references. (*)
* `references-qualification`: Warn about qualification references.
* `references-columns`: Warn about ambiguous column references.

Structure-Specific Warnings:
* `structure-else-null`: Warn about ELSE NULL in structures. (*)
* `structure-simple-case`: Warn about simple CASE structures. (*)
* `structure-unused-cte`: Warn about unused CTEs. (*)
* `structure-nested-case`: Warn about nested CASE structures.
* `structure-distinct`: Warn about DISTINCT usage. (*)
* `structure-subquery`: Warn about subquery structures.
* `structure-join-condition-order`: Warn about join condition order.
* `structure-column-order`: Warn about column order in structures.

More warnings available as part of sdf compile ...
"#
}

#[derive(Debug, Clone, Display, PartialEq, Eq, Hash)]
pub enum LintWarning {
    All,
    None,
    Error,
    CapitalizationKeywords(Capitalization),
    CapitalizationLiterals(Capitalization),
    CapitalizationTypes(Capitalization),
    CapitalizationFunctions(Capitalization),
    ConventionBlockedWords(String),
    ReferencesKeywords(String),
    ReferencesSpecialChars(String),
    ReferencesQuoting,
    ReferencesQualification,
    AmbiguousColumnReferences,
    StructureElseNull,
    StructureSimpleCase,
    StructureUnusedCte,
    StructureNestedCase,
    StructureDistinct,
    ConventionTerminator,
    ConventionComma,
    StructureSubquery(Vec<SubQueryScope>),
    StructureJoinConditionOrder,
    StructureColumnOrder,
}
impl FromStr for LintWarning {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('=').collect();
        match parts[0] {
            "all" => Ok(LintWarning::All),
            "none" => Ok(LintWarning::None),
            "error" => Ok(LintWarning::Error),
            "capitalization-keywords" => Ok(LintWarning::CapitalizationKeywords(
                parse_capitalization(parts.get(1))?,
            )),
            "capitalization-literals" => Ok(LintWarning::CapitalizationLiterals(
                parse_capitalization(parts.get(1))?,
            )),
            "capitalization-types" => Ok(LintWarning::CapitalizationTypes(parse_capitalization(
                parts.get(1),
            )?)),
            "capitalization-functions" => Ok(LintWarning::CapitalizationFunctions(
                parse_capitalization(parts.get(1))?,
            )),
            "convention-blocked-words" => {
                if let Some(words) = parts.get(1) {
                    Ok(LintWarning::ConventionBlockedWords(words.to_string()))
                } else {
                    Err("convention-blocked-words requires a list of words".into())
                }
            }
            "references-keywords" => {
                if let Some(words) = parts.get(1) {
                    Ok(LintWarning::ReferencesKeywords(words.to_string()))
                } else {
                    Ok(LintWarning::ReferencesKeywords("".to_string()))
                }
            }
            "references-special-chars" => {
                if let Some(chars) = parts.get(1) {
                    Ok(LintWarning::ReferencesSpecialChars(chars.to_string()))
                } else {
                    Err("references-special-chars requires a string".into())
                }
            }
            "references-quoting" => Ok(LintWarning::ReferencesQuoting),
            "references-qualification" => Ok(LintWarning::ReferencesQualification),
            "ambiguous-column-references" => Ok(LintWarning::AmbiguousColumnReferences),
            "structure-else-null" => Ok(LintWarning::StructureElseNull),
            "structure-simple-case" => Ok(LintWarning::StructureSimpleCase),
            "structure-unused-cte" => Ok(LintWarning::StructureUnusedCte),
            "structure-nested-case" => Ok(LintWarning::StructureNestedCase),
            "structure-distinct" => Ok(LintWarning::StructureDistinct),
            "convention-terminator" => Ok(LintWarning::ConventionTerminator),
            "convention-commas" => Ok(LintWarning::ConventionComma),
            "structure-subquery" => {
                if let Some(scope) = parts.get(1) {
                    let options = scope
                        .split(',')
                        .map(|s| match s.trim().to_lowercase().as_str() {
                            "from" => Ok(SubQueryScope::From),
                            "join" => Ok(SubQueryScope::Join),
                            _ => Err(format!("Unknown subquery scope: {s}")),
                        })
                        .collect::<Result<_, Self::Err>>()?;

                    Ok(LintWarning::StructureSubquery(options))
                } else {
                    Ok(LintWarning::StructureSubquery(vec![
                        SubQueryScope::From,
                        SubQueryScope::Join,
                    ]))
                }
            }
            "structure-join-condition-order" => Ok(LintWarning::StructureJoinConditionOrder),
            "structure-column-order" => Ok(LintWarning::StructureColumnOrder),
            _ => Err(format!("Unknown warning option: {s}")),
        }
    }
}

fn parse_capitalization(value: Option<&&str>) -> Result<Capitalization, String> {
    match value {
        Some(&"upper") => Ok(Capitalization::Upper),
        Some(&"lower") => Ok(Capitalization::Lower),
        Some(&"pascal") => Ok(Capitalization::Pascal),
        Some(&"snake") => Ok(Capitalization::Snake),
        Some(&"camel") => Ok(Capitalization::Camel),
        Some(&"consistent") => Ok(Capitalization::Consistent),
        _ => Err("Invalid value for capitalization".into()),
    }
}

pub fn translate_lint_warnings(warnings: &[LintWarning]) -> LinterConfig {
    let warnings = warnings
        .iter()
        .filter(|w| **w != LintWarning::Error)
        .cloned()
        .collect::<Vec<LintWarning>>();

    let mut linter = if warnings.contains(&LintWarning::All) {
        LinterConfig::all_lint()
    } else if warnings.is_empty() {
        LinterConfig::default()
    } else {
        LinterConfig::none()
    };

    for warning in warnings {
        match warning {
            LintWarning::CapitalizationKeywords(cap) => {
                linter.capitalization_keywords = Some(cap.to_owned())
            }
            LintWarning::CapitalizationLiterals(cap) => {
                linter.capitalization_literals = Some(cap.to_owned())
            }
            LintWarning::CapitalizationTypes(cap) => {
                linter.capitalization_types = Some(cap.to_owned())
            }
            LintWarning::CapitalizationFunctions(cap) => {
                linter.capitalization_functions = Some(cap.to_owned())
            }
            LintWarning::ConventionBlockedWords(words) => {
                linter.convention_blocked_words =
                    Some(words.split(',').map(|w| w.to_string()).collect());
            }
            LintWarning::ReferencesKeywords(words) => {
                if words.is_empty() {
                    linter.references_keywords = Some(vec![]);
                } else {
                    linter.references_keywords =
                        Some(words.split(',').map(|w| w.to_lowercase()).collect());
                }
            }
            LintWarning::ReferencesSpecialChars(chars) => {
                linter.references_special_chars = Some(chars.to_string())
            }
            LintWarning::ReferencesQuoting => linter.references_quoting = Some(RuleOn::On),
            LintWarning::ReferencesQualification => {
                linter.references_qualification = Some(RuleOn::On)
            }
            LintWarning::AmbiguousColumnReferences => {
                linter.ambiguous_column_references = Some(RuleOn::On)
            }
            LintWarning::StructureElseNull => linter.structure_else_null = Some(RuleOn::On),
            LintWarning::StructureSimpleCase => linter.structure_simple_case = Some(RuleOn::On),
            LintWarning::StructureUnusedCte => linter.structure_unused_cte = Some(RuleOn::On),
            LintWarning::StructureNestedCase => linter.structure_nested_case = Some(RuleOn::On),
            LintWarning::StructureDistinct => linter.structure_distinct = Some(RuleOn::On),
            LintWarning::ConventionTerminator => linter.convention_terminator = Some(RuleOn::On),
            LintWarning::ConventionComma => linter.convention_comma = Some(RuleOn::On),
            LintWarning::StructureSubquery(options) => {
                linter.structure_subquery = Some(options.to_owned());
            }
            LintWarning::StructureJoinConditionOrder => {
                linter.structure_join_condition_order = Some(RuleOn::On)
            }
            LintWarning::StructureColumnOrder => linter.structure_column_order = Some(RuleOn::Off),
            _ => {}
        }
    }

    linter
}

pub trait StatefulPattern: PartialEq + Eq + std::hash::Hash + fmt::Debug + Sized {
    fn init(init: &Self) -> ReduceablePatterns<Self>;
}

#[derive(Debug, Clone)]
pub struct ReduceablePatterns<T: StatefulPattern> {
    pub patterns: HashSet<T>,
}

impl<T> FromIterator<T> for ReduceablePatterns<T>
where
    T: StatefulPattern,
{
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let patterns = iter.into_iter().collect();
        ReduceablePatterns { patterns }
    }
}

#[derive(PartialEq, Debug, Clone, Eq, Hash, PartialOrd, Ord)]
pub enum ImplicitConversionOperators {
    Eq,
    In,
}

impl fmt::Display for ImplicitConversionOperators {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ImplicitConversionOperators::Eq => write!(f, "eq"),
            ImplicitConversionOperators::In => write!(f, "in"),
        }
    }
}

#[derive(PartialEq, Debug, Clone, Eq, Hash, PartialOrd, Ord, Display, Default, ValueEnum)]
#[repr(u8)]
pub enum Capitalization {
    // Example SELECT
    Upper = 1,
    // Example customer
    Lower = 2,
    // Example GetCustomerName
    Pascal = 3,
    // Example customer_id
    Snake = 4,
    // Example getSomeData -- rarely used.
    Camel = 5,
    // Off
    Off = 6,
    //
    #[default]
    Consistent = 0,
}

impl Capitalization {
    pub fn get_possible_patterns(input: &str) -> HashSet<Capitalization> {
        let mut result = HashSet::new();
        if input.to_ascii_uppercase() == input {
            result.insert(Capitalization::Upper);
        }
        if input.to_ascii_lowercase() == input {
            result.insert(Capitalization::Lower);
        }
        let first = input.chars().next().unwrap();
        if !first.is_alphabetic() {
            result.insert(Capitalization::Pascal);
            result.insert(Capitalization::Snake);
            result.insert(Capitalization::Camel);
        } else {
            if first.is_uppercase() && input.chars().skip(1).all(|c| c.is_lowercase()) {
                result.insert(Capitalization::Pascal);
            }
            if input
                .chars()
                .all(|c| c.is_lowercase() || c == '_' || c.is_ascii_digit())
            {
                result.insert(Capitalization::Snake);
            }
            if first.is_lowercase()
                && input
                    .chars()
                    .skip(1)
                    .all(|c| c.is_lowercase() || c.is_uppercase())
            {
                result.insert(Capitalization::Camel);
            }
        }
        result
    }

    pub fn get_best_pattern(patterns: &HashSet<Capitalization>) -> Option<Capitalization> {
        if !patterns.is_empty() {
            patterns.iter().min().cloned()
        } else {
            None
        }
    }

    pub fn fix(&self, text: &str) -> String {
        match self {
            Capitalization::Upper => text.to_uppercase(),
            Capitalization::Lower => text.to_lowercase(),
            Capitalization::Pascal => {
                let mut chars = text.chars();
                let first = chars.next().unwrap().to_uppercase();
                let rest = chars.collect::<String>().to_lowercase();
                format!("{first}{rest}")
            }
            Capitalization::Snake => text.to_lowercase(),
            Capitalization::Camel => {
                let mut chars = text.chars();
                let first = chars.next().unwrap().to_lowercase();
                let rest = chars.collect::<String>().to_lowercase();
                format!("{first}{rest}")
            }
            Capitalization::Consistent => unreachable!(),
            Capitalization::Off => text.to_owned(),
        }
    }
}

impl StatefulPattern for Capitalization {
    fn init(init: &Self) -> ReduceablePatterns<Self> {
        if init == &Capitalization::Consistent {
            vec![
                Capitalization::Upper,
                Capitalization::Lower,
                Capitalization::Pascal,
                Capitalization::Snake,
                Capitalization::Camel,
            ]
            .into_iter()
            .collect()
        } else {
            vec![init.to_owned()].into_iter().collect()
        }
    }
}

impl ReduceablePatterns<Capitalization> {
    pub fn reduce(&self, current: &HashSet<Capitalization>) -> Self {
        self.patterns.intersection(current).cloned().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.patterns.is_empty()
    }
}

#[derive(PartialEq, Debug, Clone, Eq, Hash, PartialOrd, Ord)]

pub enum SubQueryScope {
    Join,
    From,
}

impl fmt::Display for SubQueryScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SubQueryScope::Join => write!(f, "join"),
            SubQueryScope::From => write!(f, "from"),
        }
    }
}

#[derive(PartialEq, Debug, Clone, Eq, Hash, PartialOrd, Ord, Default)]

pub enum RuleOn {
    On,
    #[default]
    Off,
}
impl Copy for RuleOn {}

#[derive(PartialEq, Debug, Clone, Eq, Hash, PartialOrd, Ord, Default)]

pub enum ColumnQualifier {
    Qualified,
    Unqualified,
    Consistent,
    #[default]
    Off,
}

impl fmt::Display for ColumnQualifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColumnQualifier::Qualified => write!(f, "qualified"),
            ColumnQualifier::Unqualified => write!(f, "unqualified"),
            ColumnQualifier::Consistent => write!(f, "consistent"),
            ColumnQualifier::Off => write!(f, "off"),
        }
    }
}

impl StatefulPattern for ColumnQualifier {
    fn init(init: &Self) -> ReduceablePatterns<Self> {
        if init == &ColumnQualifier::Consistent {
            vec![ColumnQualifier::Qualified, ColumnQualifier::Unqualified]
                .into_iter()
                .collect()
        } else {
            vec![init.to_owned()].into_iter().collect()
        }
    }
}

impl ReduceablePatterns<ColumnQualifier> {
    pub fn reduce(&self, input: bool) -> Self {
        let mut current = HashSet::new();
        if input {
            current.insert(ColumnQualifier::Qualified);
        } else {
            current.insert(ColumnQualifier::Unqualified);
        }

        self.patterns.intersection(&current).cloned().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.patterns.is_empty()
    }
}

#[derive(PartialEq, Debug, Clone, Eq, Hash, PartialOrd, Ord)]
pub struct LinterConfig {
    /// The name of these lint rules, e.g. "my_rules", can be referenced in defaults
    pub name: Option<String>,
    /// The overall switch, default is off
    pub defaults: RuleOn,
    /// Inconsistent capitalization of keywords
    pub capitalization_keywords: Option<Capitalization>, // one of 'consistent', 'upper', 'lower', 'capitalize'
    /// Inconsistent capitalization of `boolean`/`null` literal
    pub capitalization_literals: Option<Capitalization>, // ... strange no further one of ['consistent', 'upper', 'lower', 'capitalize']
    /// Inconsistent capitalization of datatypes.
    pub capitalization_types: Option<Capitalization>, // ... strange no further one of ['consistent', 'upper', 'lower', 'capitalize']
    pub capitalization_functions: Option<Capitalization>, // one of  ['consistent', 'upper', 'lower', 'pascal', 'capitalize', 'snake', 'camel']    /// Block a list of configurable words from being used
    /// Specify a list of names to block from being identifiers.
    pub convention_blocked_words: Option<Vec<String>>,
    /// Keywords should not be used as identifiers
    pub references_keywords: Option<Vec<String>>,
    /// Do not use special characters in identifiers
    pub references_special_chars: Option<String>,
    /// Unnecessary quoted identifier
    pub references_quoting: Option<RuleOn>,
    /// Columns reference should specify source table or view in queries with more than one source.
    pub references_qualification: Option<RuleOn>,
    /// Inconsistent column references in GROUP BY/ORDER BY clauses of both ordinal and non-ordinal
    pub ambiguous_column_references: Option<RuleOn>,
    /// Redundant `ELSE NULL` in a case when statement
    pub structure_else_null: Option<RuleOn>,
    /// CASE statement can be simplified
    pub structure_simple_case: Option<RuleOn>,
    /// Query defines a CTE (common-table expression) but does not use it
    pub structure_unused_cte: Option<RuleOn>,
    /// Nested CASE statement in ELSE clause could be flattened.
    pub structure_nested_case: Option<RuleOn>,
    /// DISTINCT used with parentheses
    pub structure_distinct: Option<RuleOn>,
    /// Join/From clauses should not contain subqueries. Use CTEs instead
    pub structure_subquery: Option<Vec<SubQueryScope>>, // one of ['join', 'from', 'both']
    /// Join conditions column references should follow tables reference order
    pub structure_join_condition_order: Option<RuleOn>, // one of ['earlier' = true, 'later' = false].
    /// Select wildcards then simple targets before calculations and aggregates.
    pub structure_column_order: Option<RuleOn>,
    /// Statements should not end with a semi-colon. Multi-statements must be separated with
    /// a semi-colon but the final statement should NOT end with one.
    pub convention_terminator: Option<RuleOn>,
    /// Avoid trailing commas in lists
    pub convention_comma: Option<RuleOn>, // trailing commas
    /// Inconsistent capitalization of column names
    pub ctx_case_column: Option<Capitalization>, // one of  ['consistent', 'upper', 'lower', 'pascal', 'capitalize', 'snake', 'camel']
    /// Inconsistent capitalization of table names
    pub case_table: Option<Capitalization>, // one of  ['consistent', 'upper', 'lower', 'pascal', 'capitalize', 'snake', 'camel']
    ///Inconsistent capitalization of alias
    pub case_alias: Option<Capitalization>, // one of  ['consistent', 'upper', 'lower', 'pascal', 'capitalize', 'snake', 'camel']
    /// Prevent type implicit casting or coercion
    pub ctx_no_implicit_conversions_in: Option<Vec<ImplicitConversionOperators>>,
    // // Full name: references consistent
    pub references_consistent: Option<ColumnQualifier>, // These rules check for bad smells regarding optimal performance
    /// Avoid functions in where clauses over indexed, partitioned or clustered columns, to
    ///prevent filter push down and improve performance
    pub perf_only_direct_partition_column_access: Option<RuleOn>, // These are user authored code checks

    /// Configuration properties maps keys to values, i.e. string to string
    pub check_rules: Option<BTreeMap<String, Option<String>>>,
    /// Avoid duplicate column names in the same table
    pub ctx_duplicate_column_names: Option<RuleOn>,
}

impl From<&BTreeMap<String, Value>> for LinterConfig {
    #[allow(clippy::cognitive_complexity)]
    fn from(value: &BTreeMap<String, Value>) -> Self {
        LinterConfig {
            name: None,
            defaults: RuleOn::Off,
            capitalization_keywords: match value.get("__lint_capitalization_keywords") {
                Some(Value::String(value, ..)) => match value.as_str() {
                    "Consistent" => Some(Capitalization::Consistent),
                    "Upper" => Some(Capitalization::Upper),
                    "Lower" => Some(Capitalization::Lower),
                    "Capitalize" => Some(Capitalization::Pascal),
                    "Snake" => Some(Capitalization::Snake),
                    "Camel" => Some(Capitalization::Camel),
                    _ => None,
                },
                _ => None,
            },
            capitalization_literals: match value.get("__lint_capitalization_literals") {
                Some(Value::String(value, ..)) => match value.as_str() {
                    "Consistent" => Some(Capitalization::Consistent),
                    "Upper" => Some(Capitalization::Upper),
                    "Lower" => Some(Capitalization::Lower),
                    "Capitalize" => Some(Capitalization::Pascal),
                    "Snake" => Some(Capitalization::Snake),
                    "Camel" => Some(Capitalization::Camel),
                    _ => None,
                },
                _ => None,
            },
            capitalization_types: match value.get("__lint_capitalization_types") {
                Some(Value::String(value, ..)) => match value.as_str() {
                    "Consistent" => Some(Capitalization::Consistent),
                    "Upper" => Some(Capitalization::Upper),
                    "Lower" => Some(Capitalization::Lower),
                    "Capitalize" => Some(Capitalization::Pascal),
                    "Snake" => Some(Capitalization::Snake),
                    "Camel" => Some(Capitalization::Camel),
                    _ => None,
                },
                _ => None,
            },
            capitalization_functions: match value.get("__lint_capitalization_functions") {
                Some(Value::String(value, ..)) => match value.as_str() {
                    "Consistent" => Some(Capitalization::Consistent),
                    "Upper" => Some(Capitalization::Upper),
                    "Lower" => Some(Capitalization::Lower),
                    "Capitalize" => Some(Capitalization::Pascal),
                    "Snake" => Some(Capitalization::Snake),
                    "Camel" => Some(Capitalization::Camel),
                    _ => None,
                },
                _ => None,
            },
            convention_blocked_words: match value.get("__lint_convention_blocked_words") {
                Some(Value::String(value, ..)) => {
                    Some(value.split(',').map(|s| s.to_string()).collect())
                }
                _ => None,
            },
            references_keywords: match value.get("__lint_references_keywords") {
                Some(Value::String(value, ..)) => {
                    Some(value.split(',').map(|s| s.to_string()).collect())
                }
                _ => None,
            },
            references_special_chars: match value.get("__lint_references_special_chars") {
                Some(Value::String(value, ..)) => Some(value.to_string()),
                _ => None,
            },
            references_quoting: match value.get("__lint_references_quoting") {
                Some(Value::Bool(true, ..)) => Some(RuleOn::On),
                _ => None,
            },
            references_qualification: match value.get("__lint_references_qualification") {
                Some(Value::Bool(true, ..)) => Some(RuleOn::On),
                _ => None,
            },
            ambiguous_column_references: match value.get("__lint_ambiguous_column_references") {
                Some(Value::Bool(true, ..)) => Some(RuleOn::On),
                _ => None,
            },
            structure_else_null: match value.get("__lint_structure_else_null") {
                Some(Value::Bool(true, ..)) => Some(RuleOn::On),
                _ => None,
            },
            structure_simple_case: match value.get("__lint_structure_simple_case") {
                Some(Value::Bool(true, ..)) => Some(RuleOn::On),
                _ => None,
            },
            structure_unused_cte: match value.get("__lint_structure_unused_cte") {
                Some(Value::Bool(true, ..)) => Some(RuleOn::On),
                _ => None,
            },
            structure_nested_case: match value.get("__lint_structure_nested_case") {
                Some(Value::Bool(true, ..)) => Some(RuleOn::On),
                _ => None,
            },
            structure_distinct: match value.get("__lint_structure_distinct") {
                Some(Value::Bool(true, ..)) => Some(RuleOn::On),
                _ => None,
            },
            structure_subquery: match value.get("__lint_structure_subquery") {
                Some(Value::String(value, ..)) => {
                    let options = value
                        .split(',')
                        .map(|s| match s.trim().to_lowercase().as_str() {
                            "from" => Ok(SubQueryScope::From),
                            "join" => Ok(SubQueryScope::Join),
                            _ => Err(format!("Unknown subquery scope: {s}")),
                        })
                        .collect::<Result<_, _>>()
                        .unwrap();
                    Some(options)
                }
                _ => None,
            },
            structure_join_condition_order: match value.get("__lint_structure_join_condition_order")
            {
                Some(Value::Bool(true, ..)) => Some(RuleOn::On),
                _ => None,
            },
            structure_column_order: match value.get("__lint_structure_column_order") {
                Some(Value::Bool(true, ..)) => Some(RuleOn::On),
                _ => None,
            },
            convention_terminator: match value.get("__lint_convention_terminator") {
                Some(Value::Bool(true, ..)) => Some(RuleOn::On),
                _ => None,
            },
            convention_comma: match value.get("__lint_convention_comma") {
                Some(Value::Bool(true, ..)) => Some(RuleOn::On),
                _ => None,
            },
            ctx_case_column: match value.get("__lint_ctx_case_column") {
                Some(Value::String(value, ..)) => match value.as_str() {
                    "Consistent" => Some(Capitalization::Consistent),
                    "Upper" => Some(Capitalization::Upper),
                    "Lower" => Some(Capitalization::Lower),
                    "Capitalize" => Some(Capitalization::Pascal),
                    "Snake" => Some(Capitalization::Snake),
                    "Camel" => Some(Capitalization::Camel),
                    _ => None,
                },
                _ => None,
            },
            case_table: match value.get("__lint_case_table") {
                Some(Value::String(value, ..)) => match value.as_str() {
                    "Consistent" => Some(Capitalization::Consistent),
                    "Upper" => Some(Capitalization::Upper),
                    "Lower" => Some(Capitalization::Lower),
                    "Capitalize" => Some(Capitalization::Pascal),
                    "Snake" => Some(Capitalization::Snake),
                    "Camel" => Some(Capitalization::Camel),
                    _ => None,
                },
                _ => None,
            },
            case_alias: match value.get("__lint_case_alias") {
                Some(Value::String(value, ..)) => match value.as_str() {
                    "Consistent" => Some(Capitalization::Consistent),
                    "Upper" => Some(Capitalization::Upper),
                    "Lower" => Some(Capitalization::Lower),
                    "Capitalize" => Some(Capitalization::Pascal),
                    "Snake" => Some(Capitalization::Snake),
                    "Camel" => Some(Capitalization::Camel),
                    _ => None,
                },
                _ => None,
            },
            ctx_no_implicit_conversions_in: match value.get("__lint_ctx_no_implicit_conversions_in")
            {
                Some(Value::String(value, ..)) => {
                    let options = value
                        .split(',')
                        .map(|s| match s.trim().to_lowercase().as_str() {
                            "eq" => Ok(ImplicitConversionOperators::Eq),
                            "in" => Ok(ImplicitConversionOperators::In),
                            _ => Err(format!("Unknown implicit conversion operator: {s}")),
                        })
                        .collect::<Result<_, _>>()
                        .unwrap();
                    Some(options)
                }
                _ => None,
            },
            references_consistent: match value.get("__lint_references_consistent") {
                Some(Value::String(value, ..)) => match value.as_str() {
                    "qualified" => Some(ColumnQualifier::Qualified),
                    "unqualified" => Some(ColumnQualifier::Unqualified),
                    "consistent" => Some(ColumnQualifier::Consistent),
                    _ => None,
                },
                _ => None,
            },
            perf_only_direct_partition_column_access: None,
            check_rules: None,
            ctx_duplicate_column_names: match value.get("__lint_ctx_duplicate_column_names") {
                Some(Value::Bool(true, ..)) => Some(RuleOn::On),
                _ => None,
            },
        }
    }
}

impl LinterConfig {
    #[allow(clippy::cognitive_complexity)]
    pub fn to_btree_map(&self) -> BTreeMap<String, Value> {
        let mut map = BTreeMap::new();
        if let Some(cap) = &self.capitalization_keywords {
            map.insert(
                "__lint_capitalization_keywords".to_string(),
                Value::string(cap.to_string()),
            );
        }
        if let Some(cap) = &self.capitalization_literals {
            map.insert(
                "__lint_capitalization_literals".to_string(),
                Value::string(cap.to_string()),
            );
        }
        if let Some(cap) = &self.capitalization_types {
            map.insert(
                "__lint_capitalization_types".to_string(),
                Value::string(cap.to_string()),
            );
        }
        if let Some(cap) = &self.capitalization_functions {
            map.insert(
                "__lint_capitalization_functions".to_string(),
                Value::string(cap.to_string()),
            );
        }
        if let Some(words) = &self.convention_blocked_words {
            map.insert(
                "__lint_convention_blocked_words".to_string(),
                Value::string(words.join(",")),
            );
        }
        if let Some(words) = &self.references_keywords {
            map.insert(
                "__lint_references_keywords".to_string(),
                Value::string(words.join(",")),
            );
        }
        if let Some(chars) = &self.references_special_chars {
            map.insert(
                "__lint_references_special_chars".to_string(),
                Value::string(chars.to_string()),
            );
        }
        if let Some(value) = &self.references_quoting {
            map.insert(
                "__lint_references_quoting".to_string(),
                Value::bool(value == &RuleOn::On),
            );
        }
        if let Some(value) = &self.references_qualification {
            map.insert(
                "__lint_references_qualification".to_string(),
                Value::bool(value == &RuleOn::On),
            );
        }
        if let Some(value) = &self.ambiguous_column_references {
            map.insert(
                "__lint_ambiguous_column_references".to_string(),
                Value::bool(value == &RuleOn::On),
            );
        }
        if let Some(value) = &self.structure_else_null {
            map.insert(
                "__lint_structure_else_null".to_string(),
                Value::bool(value == &RuleOn::On),
            );
        }
        if let Some(value) = &self.structure_simple_case {
            map.insert(
                "__lint_structure_simple_case".to_string(),
                Value::bool(value == &RuleOn::On),
            );
        }
        if let Some(value) = &self.structure_unused_cte {
            map.insert(
                "__lint_structure_unused_cte".to_string(),
                Value::bool(value == &RuleOn::On),
            );
        }
        if let Some(value) = &self.structure_nested_case {
            map.insert(
                "__lint_structure_nested_case".to_string(),
                Value::bool(value == &RuleOn::On),
            );
        }
        if let Some(value) = &self.structure_distinct {
            map.insert(
                "__lint_structure_distinct".to_string(),
                Value::bool(value == &RuleOn::On),
            );
        }
        if let Some(value) = &self.structure_subquery {
            map.insert(
                "__lint_structure_subquery".to_string(),
                Value::string(
                    value
                        .iter()
                        .map(|s| s.to_string())
                        .collect::<Vec<String>>()
                        .join(","),
                ),
            );
        }
        if let Some(value) = &self.structure_join_condition_order {
            map.insert(
                "__lint_structure_join_condition_order".to_string(),
                Value::bool(value == &RuleOn::On),
            );
        }
        if let Some(value) = &self.structure_column_order {
            map.insert(
                "__lint_structure_column_order".to_string(),
                Value::bool(value == &RuleOn::On),
            );
        }
        if let Some(value) = &self.convention_terminator {
            map.insert(
                "__lint_convention_terminator".to_string(),
                Value::bool(value == &RuleOn::On),
            );
        }
        if let Some(value) = &self.convention_comma {
            map.insert(
                "__lint_convention_comma".to_string(),
                Value::bool(value == &RuleOn::On),
            );
        }
        if let Some(value) = &self.ctx_case_column {
            map.insert(
                "__lint_ctx_case_column".to_string(),
                Value::string(value.to_string()),
            );
        }
        if let Some(value) = &self.case_table {
            map.insert(
                "__lint_case_table".to_string(),
                Value::string(value.to_string()),
            );
        }
        if let Some(value) = &self.case_alias {
            map.insert(
                "__lint_case_alias".to_string(),
                Value::string(value.to_string()),
            );
        }
        if let Some(value) = &self.ctx_no_implicit_conversions_in {
            map.insert(
                "__lint_ctx_no_implicit_conversions_in".to_string(),
                Value::string(
                    value
                        .iter()
                        .map(|s| s.to_string())
                        .collect::<Vec<String>>()
                        .join(","),
                ),
            );
        }
        if let Some(value) = &self.references_consistent {
            map.insert(
                "__lint_references_consistent".to_string(),
                Value::string(value.to_string()),
            );
        }
        if let Some(value) = &self.perf_only_direct_partition_column_access {
            map.insert(
                "__lint_perf_only_direct_partition_column_access".to_string(),
                Value::bool(value == &RuleOn::On),
            );
        }
        if let Some(value) = &self.ctx_duplicate_column_names {
            map.insert(
                "__lint_ctx_duplicate_column_names".to_string(),
                Value::bool(value == &RuleOn::On),
            );
        }
        map
    }

    pub fn new_fixable_config() -> LinterConfig {
        // todo: add everything that is fixable here
        LinterConfig {
            name: None,
            defaults: RuleOn::Off,
            // this is the DBT default configuration
            // todo: check that this actually matches, see .sqlfluff in the new dbt project
            capitalization_keywords: Some(Capitalization::Lower),
            capitalization_literals: Some(Capitalization::Lower),
            capitalization_functions: Some(Capitalization::Lower),
            ..Default::default()
        }
    }
    pub fn none() -> Self {
        LinterConfig {
            ..Default::default()
        }
    }
    pub fn all_lint() -> Self {
        LinterConfig {
            ctx_case_column: Some(Capitalization::Consistent),
            case_table: Some(Capitalization::Consistent),
            case_alias: Some(Capitalization::Consistent),
            ctx_no_implicit_conversions_in: Some(vec![
                ImplicitConversionOperators::Eq,
                ImplicitConversionOperators::In,
            ]),
            references_consistent: Some(ColumnQualifier::Consistent),
            references_qualification: Some(RuleOn::On),
            structure_distinct: Some(RuleOn::On),
            ambiguous_column_references: Some(RuleOn::On),
            structure_simple_case: Some(RuleOn::On),
            structure_unused_cte: Some(RuleOn::On),
            structure_else_null: Some(RuleOn::On),
            structure_nested_case: Some(RuleOn::On),
            convention_terminator: Some(RuleOn::On),
            convention_comma: Some(RuleOn::On),
            structure_join_condition_order: Some(RuleOn::On),
            structure_column_order: Some(RuleOn::On),
            structure_subquery: Some(vec![SubQueryScope::Join, SubQueryScope::From]),
            perf_only_direct_partition_column_access: Some(RuleOn::On),
            ..Default::default()
        }
    }

    pub fn all_compile() -> Self {
        LinterConfig {
            ctx_case_column: Some(Capitalization::Consistent),
            case_table: Some(Capitalization::Consistent),
            case_alias: Some(Capitalization::Consistent),
            ctx_no_implicit_conversions_in: Some(vec![
                ImplicitConversionOperators::Eq,
                ImplicitConversionOperators::In,
            ]),
            references_consistent: Some(ColumnQualifier::Consistent),
            ctx_duplicate_column_names: Some(RuleOn::On),
            ..Default::default()
        }
    }

    //internal helper
    fn get_capitalization(
        &self,
        rule: Option<Capitalization>,
        default: Capitalization,
    ) -> Option<Capitalization> {
        match (rule, self.defaults) {
            (Some(Capitalization::Off), _) => None,
            (Some(cap), _) => Some(cap),
            (None, RuleOn::On) => Some(default),
            (None, RuleOn::Off) => None,
        }
    }
    //internal helper
    fn get_flag(&self, rule: Option<RuleOn>) -> bool {
        rule.unwrap_or(self.defaults) == RuleOn::On
    }

    pub fn capitalization_keywords(&self) -> Option<Capitalization> {
        self.get_capitalization(
            self.capitalization_keywords.to_owned(),
            Capitalization::Consistent,
        )
    }

    pub fn capitalization_literals(&self) -> Option<Capitalization> {
        self.get_capitalization(
            self.capitalization_literals.to_owned(),
            Capitalization::Consistent,
        )
    }

    pub fn capitalization_types(&self) -> Option<Capitalization> {
        self.get_capitalization(
            self.capitalization_types.to_owned(),
            Capitalization::Consistent,
        )
    }

    pub fn capitalization_functions(&self) -> Option<Capitalization> {
        self.get_capitalization(
            self.capitalization_functions.to_owned(),
            Capitalization::Consistent,
        )
    }

    pub fn case_alias(&self) -> Option<Capitalization> {
        self.get_capitalization(self.case_alias.to_owned(), Capitalization::Consistent)
    }

    pub fn case_column(&self) -> Option<Capitalization> {
        self.get_capitalization(self.ctx_case_column.to_owned(), Capitalization::Consistent)
    }

    pub fn case_table(&self) -> Option<Capitalization> {
        self.get_capitalization(self.case_table.to_owned(), Capitalization::Consistent)
    }

    pub fn disallow_these_identifiers(&self) -> Vec<String> {
        match (self.convention_blocked_words.to_owned(), self.defaults) {
            (Some(identifiers), _) => identifiers,
            (None, RuleOn::On) => vec![],
            (None, RuleOn::Off) => vec![],
        }
    }

    pub fn disallow_subquery_in(&self) -> Vec<SubQueryScope> {
        match (self.structure_subquery.to_owned(), self.defaults) {
            (Some(subquery_scopes), _) => subquery_scopes,
            (None, RuleOn::On) => vec![SubQueryScope::Join, SubQueryScope::From],
            (None, RuleOn::Off) => vec![],
        }
    }

    pub fn disallow_these_chars_in_quoted_identifiers(&self) -> Option<String> {
        match (self.references_special_chars.to_owned(), self.defaults) {
            (Some(chars), _) => Some(chars),
            (None, RuleOn::On) => Some(String::new()),
            (None, RuleOn::Off) => None,
        }
    }

    pub fn flag_unqualified_multi_source_column_reference(&self) -> bool {
        self.get_flag(self.references_qualification.to_owned())
    }

    pub fn flag_distinct_parenthesis(&self) -> bool {
        self.get_flag(self.structure_distinct.to_owned())
    }

    pub fn flag_inconsistent_ordinal_column_reference(&self) -> bool {
        self.get_flag(self.ambiguous_column_references.to_owned())
    }

    pub fn flag_keywords_used_as_identifiers(&self) -> Option<Vec<String>> {
        match (self.references_keywords.to_owned(), self.defaults) {
            (Some(keywords), _) => Some(keywords),
            (None, RuleOn::On) => Some(vec![]),
            (None, RuleOn::Off) => None,
        }
    }

    pub fn flag_misordered_join_condition(&self) -> bool {
        self.get_flag(self.structure_join_condition_order.to_owned())
    }

    pub fn flag_select_item_order(&self) -> bool {
        self.get_flag(self.structure_column_order.to_owned())
    }

    pub fn flag_trailing_semicolon_after_statements(&self) -> bool {
        self.get_flag(self.convention_terminator.to_owned())
    }

    pub fn flag_unnecessary_case(&self) -> bool {
        self.get_flag(self.structure_simple_case.to_owned())
    }

    pub fn flag_unused_cte(&self) -> bool {
        self.get_flag(self.structure_unused_cte.to_owned())
    }

    pub fn flag_unnecessary_nested_case(&self) -> bool {
        self.get_flag(self.structure_nested_case.to_owned())
    }

    pub fn flag_unnecessary_else(&self) -> bool {
        self.get_flag(self.structure_else_null.to_owned())
    }

    pub fn flag_unnecessary_quoted_identifiers(&self) -> bool {
        self.get_flag(self.references_quoting.to_owned())
    }

    pub fn flag_dangling_comma(&self) -> bool {
        self.get_flag(self.convention_comma.to_owned())
    }

    // internal helper
    fn get_column_qualifier(
        &self,
        rule: Option<ColumnQualifier>,
        default: ColumnQualifier,
    ) -> Option<ColumnQualifier> {
        match (rule, self.defaults) {
            (Some(ColumnQualifier::Off), _) => None,
            (Some(qualifier), _) => Some(qualifier),
            (None, RuleOn::On) => Some(default),
            (None, RuleOn::Off) => None,
        }
    }

    pub fn flag_inconsistent_qualified_column_reference(&self) -> Option<ColumnQualifier> {
        self.get_column_qualifier(
            self.references_consistent.to_owned(),
            ColumnQualifier::Consistent,
        )
    }

    pub fn disallow_implicit_conversions_in(&self) -> Vec<ImplicitConversionOperators> {
        self.ctx_no_implicit_conversions_in
            .to_owned()
            .unwrap_or_else(|| {
                if self.defaults == RuleOn::On {
                    vec![
                        ImplicitConversionOperators::Eq,
                        ImplicitConversionOperators::In,
                    ]
                } else {
                    vec![]
                }
            })
    }

    pub fn flag_function_application_in_where_on_indexed_columns(&self) -> bool {
        self.get_flag(self.perf_only_direct_partition_column_access.to_owned())
    }

    pub fn flag_duplicate_column_names(&self) -> bool {
        self.get_flag(self.ctx_duplicate_column_names.to_owned())
    }
}

impl Default for LinterConfig {
    fn default() -> Self {
        Self {
            name: None,
            defaults: RuleOn::Off,
            capitalization_keywords: Some(Capitalization::Consistent),
            capitalization_literals: Some(Capitalization::Consistent),
            capitalization_types: Some(Capitalization::Consistent),
            capitalization_functions: Some(Capitalization::Consistent),
            convention_blocked_words: None,
            references_keywords: None,
            references_special_chars: None,
            references_quoting: Some(RuleOn::On),
            references_qualification: None,
            ambiguous_column_references: None,
            structure_else_null: Some(RuleOn::On),
            structure_simple_case: None,
            structure_unused_cte: Some(RuleOn::On),
            structure_nested_case: None,
            structure_distinct: Some(RuleOn::On),
            structure_subquery: None,
            structure_join_condition_order: None,
            structure_column_order: Some(RuleOn::On),
            convention_terminator: Some(RuleOn::On),
            convention_comma: Some(RuleOn::On),
            ctx_case_column: None,
            case_table: None,
            case_alias: None,
            ctx_no_implicit_conversions_in: None,
            references_consistent: None,
            perf_only_direct_partition_column_access: None,
            check_rules: None,
            ctx_duplicate_column_names: None,
        }
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

    /// Limiting number of shown rows. Run with --limit -1 to remove limit [default: 10]
    #[arg(long)]
    pub limit: Option<usize>,

    /// Display rows in different formats
    #[arg(global = true, long, aliases = ["format"])]
    pub output: Option<DisplayFormat>,

    /// Flag to enable or disable SQL analysis, or to run SQL in unsafe mode,  enabled by default
    #[arg(global = true, long, default_value = "on")]
    pub static_analysis: StaticAnalysisKind,
}

impl TestArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let mut eval_args = self.common_args.to_eval_args(arg, in_dir, out_dir);
        eval_args.resource_types = vec![ClapResourceType::Test, ClapResourceType::UnitTest];
        if let Some(output) = &self.output {
            eval_args.format = output.to_string();
        } else {
            eval_args.format = DEFAULT_FORMAT.clone();
        }
        eval_args.limit = self.limit;
        eval_args.static_analysis = self.static_analysis;
        eval_args.format = self
            .output
            .map(|f| f.to_string())
            .unwrap_or_else(|| DEFAULT_FORMAT.clone());
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
    #[arg(long)]
    pub resource_type: Option<ClapResourceType>,

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
    #[arg(global = true, long, action = ArgAction::SetTrue, value_parser = BoolishValueParser::new(), short = 'f')]
    pub full_refresh: bool,

    /// Limiting number of shown rows. Run with --limit 0 to remove limit [default: 10]
    #[arg(global = true, long)]
    pub limit: Option<usize>,

    /// Display rows in different formats
    #[arg(global = true, long, aliases = ["format"])]
    pub output: Option<DisplayFormat>,

    /// Flag to enable or disable SQL analysis, or to run SQL in unsafe mode,  enabled by default
    #[arg(global = true, long, default_value = "on")]
    pub static_analysis: StaticAnalysisKind,
}

impl BuildArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let mut eval_args = self.common_args.to_eval_args(arg, in_dir, out_dir);
        eval_args.phase = Phases::All;
        // Enable task cache
        if let Some(resource_type) = self.resource_type {
            eval_args.resource_types = vec![resource_type];
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
        eval_args
    }
}

#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct ListArgs {
    // Flattened Common args
    #[clap(flatten)]
    pub common_args: CommonArgs,

    /// Display rows in different formats, only table and json supported...
    #[arg(global = true, long, aliases = ["format"])]
    pub output: Option<DisplayFormat>,

    /// Space-delimited listing of node properties to include as custom keys for JSON output
    /// (e.g. `--output json --output-keys name resource_type description`)
    #[arg(long, num_args(1..), value_delimiter = ' ')]
    pub output_keys: Vec<String>,

    /// Select nodes of a specific type;
    #[arg(long)]
    pub resource_type: Option<ClapResourceType>,
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
        if let Some(output) = &self.output {
            eval_args.format = output.to_string();
        } else {
            eval_args.format = DEFAULT_FORMAT.clone();
        }
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

    /// Display rows in different formats
    #[arg(global = true, long, aliases = ["format"])]
    pub output: Option<DisplayFormat>,

    /// Flag to enable or disable SQL analysis, or to run SQL in unsafe mode,  enabled by default
    #[arg(global = true, long, default_value = "on")]
    pub static_analysis: StaticAnalysisKind,

    /// Drop incremental models and fully recalculate incremental tables.
    #[arg(global = true, long, action = ArgAction::SetTrue, value_parser = BoolishValueParser::new(), short = 'f')]
    pub full_refresh: bool,
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

        if eval_args.run_cache_mode.write_cache() {
            eval_args.resource_types = vec![ClapResourceType::Model, ClapResourceType::Source];
        } else {
            eval_args.resource_types = vec![ClapResourceType::Model];
        }
        eval_args.static_analysis = self.static_analysis;
        eval_args.full_refresh = self.full_refresh;
        eval_args.format = self
            .output
            .map(|f| f.to_string())
            .unwrap_or_else(|| DEFAULT_FORMAT.clone());
        eval_args
    }
}

#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct RunOperationArgs {
    #[arg(id = "MACRO")]
    pub macro_name: String,

    /// Supply arguments to the macro. This dictionary will be mapped to the keyword arguments defined in the selected macro. This argument should be a yml string.
    #[arg(long,value_parser = check_var)]
    pub args: Option<BTreeMap<String, Value>>,

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
        eval_args.with_schema(self.schema.clone())
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
        eval_args.with_connection(self.connection)
    }
}
#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct LineageArgs {
    // Flattened Common args
    #[clap(flatten)]
    pub common_args: CommonArgs,

    /// Display scan dependencies in addition to copy and mod dependencies. Unset by default
    #[arg(long, default_value_t = false)]
    pub show_scans: bool,

    /// Limiting the depth of shown lineage tree. Default value of 0 shows full lineage
    #[arg(long, default_value_t = 0)]
    pub max_depth: usize,

    /// Show and write column lineage with fully qualified table names
    #[arg(long, default_value_t = false)]
    pub use_fqtn: bool,

    /// Drop into an interactive REPL after executing the command
    #[arg(long, short = 'i', hide = true)]
    pub interactive: bool,
}

impl LineageArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let mut eval_args = self.common_args.to_eval_args(arg, in_dir, out_dir);
        eval_args.phase = Phases::Lineage;
        // TODO implement exclude resource types to make this simpler
        eval_args.resource_types = vec![
            ClapResourceType::Model,
            ClapResourceType::Source,
            ClapResourceType::Snapshot,
            ClapResourceType::Seed,
        ]; // all resources but tests
        eval_args
            // .with_refined_node_selectors(Some(predicate))
            .with_show_scans(self.show_scans)
            .with_max_depth(self.max_depth)
            .with_use_fqtn(self.use_fqtn)
    }
}

// reference: https://docs.getdbt.com/reference/global-configs/about-global-configs
#[derive(Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct CommonArgs {
    /// The target to execute
    //  has no ENV_VAR euivalent
    #[arg(global = true, long)]
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

    /// The number of threads to use [Run with --threads 0 to use max_cpu [default: 1]]
    // has no ENV_VAR, but can be set in profiles.yml
    #[arg(global = true, long)]
    pub threads: Option<usize>,

    /// Overrides threads.
    #[arg(global = true, long = "single-threaded", action = ArgAction::SetTrue, env = "DBT_SINGLE_THREADED", value_parser = BoolishValueParser::new())]
    pub single_threaded: bool,

    /// Warn on error (TODO: need to wire this in)
    #[arg(global = true, long, default_value = "false", action = ArgAction::SetTrue, env = "DBT_WARN_ERROR",hide = true, value_parser = BoolishValueParser::new())]
    pub warn_error: bool,
    #[arg(global = true, long, default_value = "false", action = ArgAction::SetTrue,  env = "DBT_WARN_ERROR",hide = true, value_parser = BoolishValueParser::new())]
    pub no_warn_error: bool,

    /// Warning error options
    #[arg(global = true, long,value_parser = check_var,
        env = "DBT_WARN_ERROR_OPTIONS",
        hide = true )]
    pub warn_error_options: Option<BTreeMap<String, Value>>,

    /// Debug flag
    #[arg(global = true, long, short = 'd', default_value = "false", action = ArgAction::SetTrue,  env = "DBT_DEBUG", value_parser = BoolishValueParser::new(),hide = true)]
    pub debug: bool,
    #[arg(global = true, long, default_value = "false", action = ArgAction::SetTrue,  env = "DBT_DEBUG", value_parser = BoolishValueParser::new(),hide = true)]
    pub no_debug: bool,

    /// Introspect flag
    #[arg(global = true, long,  default_value = "false", action = ArgAction::SetTrue,  env = "DBT_INTROSPECT", value_parser = BoolishValueParser::new(),hide = true)]
    pub introspect: bool,
    #[arg(global = true, long, default_value = "false", action = ArgAction::SetTrue,  env = "DBT_INTROSPECT", value_parser = BoolishValueParser::new(),hide = true)]
    pub no_introspect: bool,

    /// Write JSON artifacts to disk [env: DBT_WRITE_JSON=]. Use --no-write-json to suppress writing JSON artifacts.
    #[arg(global = true, long,  default_value_t=true,  action = ArgAction::SetTrue, env = "DBT_WRITE_JSON", value_parser = BoolishValueParser::new())]
    pub write_json: bool,
    #[arg(global = true,long,action = ArgAction::SetTrue,  default_value_t=false, env = "DBT_WRITE_JSON",value_parser = BoolishValueParser::new(),hide = true,conflicts_with = "write_json")]
    pub no_write_json: bool,

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
    #[arg(global = true, long= "no-defer", default_value_t=false, action = ArgAction::SetTrue, value_parser = BoolishValueParser::new(), hide = true, conflicts_with = "defer")]
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

    /// Configure the max file size in bytes for a single dbt.log file, before rolling over. 0 means no limit.
    #[arg(global = true, long, default_value = "false", env = "DBT_LOG_FILE_MAX_BYTES", value_parser = BoolishValueParser::new(),hide = true)]
    pub log_file_max_bytes: bool,
    #[arg(global = true, long, default_value = "false", env = "DBT_LOG_FILE_MAX_BYTES", value_parser = BoolishValueParser::new(),hide = true)]
    pub no_log_file_max_bytes: bool,

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
    #[arg(global = true, long, default_value_t=false, action = ArgAction::SetTrue, env = "DBT_SEND_ANONYMOUS_USAGE_STATS", value_parser = BoolishValueParser::new(), hide = true)]
    pub send_anonymous_usage_stats: bool,
    #[arg(global = true, long, default_value_t=false, action = ArgAction::SetTrue, env = "DBT_SEND_ANONYMOUS_USAGE_STATS", value_parser = BoolishValueParser::new(), hide = true)]
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
    #[arg(global = true, long , default_value_t=false,  action = ArgAction::SetTrue, env = "DBT_VERSION_CHECK", value_parser = BoolishValueParser::new(), hide=true)]
    pub version_check: bool,
    #[arg(global = true, long , default_value_t=false,  action = ArgAction::SetTrue, env = "DBT_VERSION_CHECK", value_parser = BoolishValueParser::new(), hide=true)]
    pub no_version_check: bool,

    //
    #[arg(global = true, long , default_value_t=false,  action = ArgAction::SetTrue,value_parser = BoolishValueParser::new(), hide=true)]
    pub empty: bool,
    #[arg(global = true, long , default_value_t=false,  action = ArgAction::SetTrue,value_parser = BoolishValueParser::new(), hide=true)]
    pub no_empty: bool,

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
    #[clap(long, env = "DBT_TASK_CACHE_URL", default_value = "noop", hide = true)]
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

    #[arg(global = true, long, hide = true)]
    pub trace_path: Option<PathBuf>,

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

    /// TEMPORARY same as sql-analysis=none.
    #[arg(global = true, long, default_value = "false", action = ArgAction::SetTrue, hide = true)]
    pub legacy_compile: bool,

    /// Flag to enable or disable SQL analysis, or to run SQL in unsafe mode,  enabled by default
    #[arg(global = true, long)]
    pub static_analysis: Option<StaticAnalysisKind>,

    /// Flag for compile conformance
    #[arg(global = true, long, default_value = "false", action = ArgAction::SetTrue, hide= true)]
    pub check_conformance: bool,

    /// If set, compile will greedily download the sources schemas cached from adapter.get_relations_by_pattern
    // This is to optimize for the usage pattern of having get_relations_by_pattern macro followed by dbt_utils.union_relations(relations)
    // that attempts to fetch a schema for each relation
    #[arg(global = true, long, default_value = "false", hide = true)]
    pub patterned_dangling_sources: bool,

    #[arg(global = true, long, default_value = "false", action = ArgAction::SetTrue, hide = true)]
    pub skip_unreferenced_table_check: bool,
}

impl CommonArgs {
    pub fn to_eval_args(&self, arg: SystemArgs, in_dir: &Path, out_dir: &Path) -> EvalArgs {
        let select_option = self
            .select
            .clone()
            .map(|selectors| parse_model_specifiers(&selectors).unwrap());

        let mut show = if self.show.contains(&ShowOptions::All) {
            ShowOptions::iter().collect()
        } else if self.show.contains(&ShowOptions::None) {
            HashSet::new()
        } else if self.show.is_empty() {
            HashSet::from([
                ShowOptions::ProgressRender,
                ShowOptions::ProgressRun,
                ShowOptions::Progress,
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

        // Determine replay mode based on provided flags
        let replay = match (&self.dbt_replay, &self.fs_record, &self.fs_replay) {
            (Some(dbt_replay), None, None) => Some(ReplayMode::DbtReplay(dbt_replay.to_path_buf())),
            (None, Some(fs_record), None) => Some(ReplayMode::FsRecord(fs_record.clone())),
            (None, None, Some(fs_replay)) => Some(ReplayMode::FsReplay(fs_replay.clone())),
            (None, None, None) => None,
            _ => None,
        };

        EvalArgs {
            command: arg.command.clone(),
            io: IoArgs {
                show,
                invocation_id: arg.io.invocation_id,
                in_dir: in_dir.to_path_buf(),
                out_dir: out_dir.to_path_buf(),
                send_anonymous_usage_stats: self.send_anonymous_usage_stats,
                status_reporter: arg.io.status_reporter.clone(),
                should_cancel_compilation: arg.io.should_cancel_compilation.clone(),
                log_format: self.log_format,
                log_level: self.log_level,
                log_level_file: self.log_level_file,
                log_path: self.log_path.clone(),
                trace_path: self.trace_path.clone(),
            },
            profiles_dir: self.profiles_dir.clone(),
            packages_install_path: self.packages_install_path.clone(),
            internal_packages_install_path: self.internal_packages_install_path.clone(),
            profile: self.profile.clone(),
            target: self.target.clone(),
            update_deps: false,
            vars: self.vars.clone().unwrap_or_default(),
            phase: self.phase.clone().unwrap_or(Phases::All),
            format: DEFAULT_FORMAT.clone(),
            limit: Some(DEFAULT_LIMIT),
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
            exclude: self
                .exclude
                .clone()
                .map(|selectors| parse_model_specifiers(&selectors).unwrap()),
            indirect_selection: self.indirect_selection,
            replay,
            interactive: false,
            check_conformance: self.check_conformance,
            max_depth: 0,
            show_scans: false,
            use_fqtn: false,
            schema: vec![],
            output_keys: vec![],
            skip_unreferenced_table_check: self.skip_unreferenced_table_check,
            state: self.state.clone(),
            defer_state: self.defer_state.clone(),
            patterned_dangling_sources: self.patterned_dangling_sources,
            connection: false,
            macro_name: "".to_string(),
            macro_args: BTreeMap::new(),
            selector: self.selector.clone(),
            resource_types: vec![],

            //flags
            warn_error: self.warn_error,
            warn_error_options: self.warn_error_options.clone().unwrap_or_default(),
            version_check: self.version_check,
            defer: if self.no_defer {
                Some(false)
            } else if self.defer {
                Some(true)
            } else {
                None
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
            send_anonymous_usage_stats: self.send_anonymous_usage_stats,
            write_json: !self.no_write_json,
            fail_fast: self.fail_fast,
            target_path: self.target_path.clone(),
            empty: self.empty,
            favor_state: self.favor_state,
            run_cache_mode: RunCacheMode::Noop,
            task_cache_url: self.task_cache_url.clone(),
            static_analysis: StaticAnalysisKind::default(),
            full_refresh: false,
            check_all: false,
        }
    }
}

/// Display rows in different formats
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
pub enum DisplayFormat {
    #[default]
    Table,
    Csv,
    Tsv,
    Json,
    NdJson,
    Yml,
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
}

#[derive(Parser, Debug, Clone, Serialize, Deserialize)]
pub struct SystemUpdateArgs {
    /// Update dbt to this version (e.g. 1.2.3) [default: latest version]
    #[arg(global = true, long)]
    pub version: Option<String>,
}

#[derive(Parser, Debug, Clone, Serialize, Deserialize)]
pub struct SystemUninstallArgs {}

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
            patterned_dangling_sources: self.common_args.patterned_dangling_sources,
            command: arg.command.clone(),
            from_main: arg.from_main,
            io: IoArgs {
                in_dir: in_dir.to_path_buf(),
                out_dir: out_dir.to_path_buf(),
                show,
                invocation_id: arg.io.invocation_id,
                send_anonymous_usage_stats: self.common_args.send_anonymous_usage_stats,
                status_reporter: arg.io.status_reporter.clone(),
                should_cancel_compilation: arg.io.should_cancel_compilation.clone(),
                log_format: self.common_args.log_format,
                log_level: self.common_args.log_level,
                log_level_file: self.common_args.log_level_file,
                log_path: self.common_args.log_path.clone(),
                trace_path: self.common_args.trace_path.clone(),
            },
            task_cache_url: "noop".to_string(),
            favor_state: self.common_args.favor_state,
            phase: Phases::Debug,
            ..Default::default()
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
            send_anonymous_usage_stats: cli.common_args().send_anonymous_usage_stats,
            status_reporter: None,
            should_cancel_compilation: None,
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
            trace_path: cli.common_args().trace_path,
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
            send_anonymous_usage_stats: cli.common_args().send_anonymous_usage_stats,
            status_reporter: None,
            should_cancel_compilation: None,
            log_format: cli.common_args().log_format,
            log_level: cli.common_args().log_level,
            log_level_file: cli.common_args().log_level_file,
            log_path: cli.common_args().log_path,
            trace_path: cli.common_args().trace_path,
        },
        from_main: false,
        target: cli.common_args().target,
        num_threads: cli.common_args().threads,
    }
}
