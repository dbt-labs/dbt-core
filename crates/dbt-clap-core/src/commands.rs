use std::any::Any;
use std::fmt;

use clap::error::ErrorKind;
use dbt_common::FsResult;
use dbt_common::io_args::{EvalArgs, FsCommand, Phases, SystemArgs};
use strum_macros::Display;

use crate::{
    BuildArgs, CleanArgs, CloneArgs, CommonArgs, CompileArgs, DebugArgs, DepsArgs, InitArgs,
    ListArgs, ManArgs, ParseArgs, RetryArgs, RunArgs, RunOperationArgs, SeedArgs, ShowArgs,
    SnapshotArgs, SourceArgs, SystemMgmtArgs, TestArgs,
};

#[derive(clap::Subcommand, Debug, Clone, Display)]
pub enum CoreCommand {
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
    /// Create clones of selected nodes
    Clone(CloneArgs),
    /// dbt installation configuration
    System(SystemMgmtArgs),
    /// Create reference documentation
    Man(ManArgs),
    /// Profile connection debugging
    Debug(DebugArgs),
    /// Retry failed nodes from the previous run
    Retry(RetryArgs),
}

impl CoreCommand {
    pub const fn as_command(&self) -> FsCommand {
        use CoreCommand::*;
        match self {
            Init(..) => FsCommand::Init,
            Deps(..) => FsCommand::Deps,
            Parse(..) => FsCommand::Parse,
            List(..) => FsCommand::List,
            Ls(..) => FsCommand::List,
            Compile(..) => FsCommand::Compile,
            Run(..) => FsCommand::Run,
            RunOperation(..) => FsCommand::RunOperation,
            Seed(..) => FsCommand::Seed,
            Snapshot(..) => FsCommand::Snapshot,
            Test(..) => FsCommand::Test,
            Build(..) => FsCommand::Build,
            Clone(..) => FsCommand::Clone,
            Clean(..) => FsCommand::Clean,
            Source(..) => FsCommand::Source,
            System(..) => FsCommand::System,
            Show(..) => FsCommand::Show,
            Man(..) => FsCommand::Man,
            Debug(..) => FsCommand::Debug,
            Retry(..) => FsCommand::Retry,
        }
    }

    pub const fn name(&self) -> &'static str {
        self.as_command().as_str()
    }
}

pub trait AbstractExtensionCommand: Send + Sync + fmt::Debug + Any {
    /// The canonical name of the command as used on the command line.
    fn name(&self) -> &'static str;

    // virtualized support for Clone, and Display
    fn clone_box(&self) -> Box<dyn AbstractExtensionCommand>;
    fn display_fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result;

    // fn as_any<'a>(&'a self) -> &'a dyn Any;
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn into_any(self: Box<Self>) -> Box<dyn Any>;

    // TODO: this list of required methods should eventually shrink as we improve the design
    fn to_eval_args(&self, common_args: &CommonArgs, system_arg: SystemArgs) -> FsResult<EvalArgs>;
    fn is_interactive(&self) -> bool;
    fn common_args(&self) -> CommonArgs;
    fn stage(&self) -> Phases;
    fn as_command(&self) -> FsCommand;
    fn extend_cli_options(&self, options: &mut Vec<String>);
    fn with_sample(&self) -> Option<String>;
    fn sampled(&self) -> Vec<String>;
}

#[allow(clippy::large_enum_variant)] // the CoreCommand is expected to be much larger than a [Box].
pub enum Command {
    Core(CoreCommand),
    Extension(Box<dyn AbstractExtensionCommand>),
}

impl Command {
    pub fn name(&self) -> &'static str {
        match self {
            Command::Core(cmd) => cmd.name(),
            Command::Extension(cmd) => cmd.name(),
        }
    }
}

impl Clone for Command {
    fn clone(&self) -> Self {
        use Command::*;
        match self {
            Core(cmd) => Core(cmd.clone()),
            Extension(cmd) => Extension(cmd.clone_box()),
        }
    }
}

impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Command::Core(cmd) => write!(f, "Core({cmd:?})"),
            Command::Extension(cmd) => {
                let cmd = &cmd as &dyn fmt::Debug;
                write!(f, "Extension({cmd:?})")
            }
        }
    }
}

impl fmt::Display for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Command::Core(cmd) => <CoreCommand as fmt::Display>::fmt(cmd, f),
            Command::Extension(cmd) => cmd.display_fmt(f),
        }
    }
}

pub trait ExtensionCommandParser: Send + Sync {
    /// Convert the `ArgMatches` that `clap` generated into a [Box<dyn ExtensionCommand>].
    #[allow(clippy::wrong_self_convention)]
    fn from_arg_matches_mut(
        &self,
        _arg_matches: &mut clap::ArgMatches,
    ) -> Result<Box<dyn AbstractExtensionCommand>, clap::Error> {
        Err(clap::Error::raw(
            ErrorKind::MissingSubcommand,
            "a subcommand is required but one was not provided",
        ))
    }

    /// Append to the [`clap::Command`] used to parse a command line invocation
    /// that supports extension commands.
    fn augment_subcommands(&self, app: clap::Command) -> clap::Command {
        app
    }

    /// Test whether this parser can parse a specific extension subcommand
    fn has_subcommand(&self, name: &str) -> bool;
}

pub struct CommandParser {
    extension_command_parser: Box<dyn ExtensionCommandParser>,
}

impl CommandParser {
    pub fn new(extension_command_parser: Box<dyn ExtensionCommandParser>) -> Self {
        Self {
            extension_command_parser,
        }
    }

    /// Convert the `ArgMatches` that `clap` generated into a [Command].
    pub fn from_arg_matches_mut(
        &self,
        arg_matches: &mut clap::ArgMatches,
    ) -> Result<Command, clap::Error> {
        let res = <CoreCommand as clap::FromArgMatches>::from_arg_matches(arg_matches);
        match res {
            Ok(_) => {
                // Ignore the successfully parsed result and repeat the parsing,
                // but mutating arg_matches this time to ensure matches are consumed.
                let core_command =
                    <CoreCommand as clap::FromArgMatches>::from_arg_matches_mut(arg_matches)?;
                Ok(Command::Core(core_command))
            }
            Err(err) => {
                if err.kind() == ErrorKind::InvalidSubcommand {
                    let extension_command = self
                        .extension_command_parser
                        .from_arg_matches_mut(arg_matches)?;
                    Ok(Command::Extension(extension_command))
                } else {
                    Err(err)
                }
            }
        }
    }

    /// Append to [`clap::Command`] so it can instantiate `Command` via
    /// [`CommandParser::from_arg_matches_mut`]
    pub fn augment_subcommands(&self, app: clap::Command) -> clap::Command {
        let app = <CoreCommand as clap::Subcommand>::augment_subcommands(app);
        self.extension_command_parser.augment_subcommands(app)
    }

    /// Test whether we can parse a specific subcommand
    #[allow(dead_code)]
    pub fn has_subcommand(&self, name: &str) -> bool {
        <CoreCommand as clap::Subcommand>::has_subcommand(name)
        // TODO: check extension commands
    }
}
