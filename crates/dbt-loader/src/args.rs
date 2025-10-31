use std::sync::Arc;
use std::{collections::BTreeMap, path::PathBuf};

pub use dbt_common::io_args::IoArgs;
use dbt_common::io_args::{EvalArgs, Phases};
use dbt_schemas::state::DbtState;

#[derive(Clone, Default)]
pub struct LoadArgs {
    pub command: String,
    pub io: IoArgs,
    // The profile directory to load the profiles from
    pub profiles_dir: Option<PathBuf>,
    // The directory to install packages
    pub packages_install_path: Option<PathBuf>,
    // The directory to install fs_internal packages
    pub internal_packages_install_path: Option<PathBuf>,
    // The profile to use
    pub profile: Option<String>,
    // The target within the profile to use for the dbt run
    pub target: Option<String>,
    // Whether to update dependencies
    pub update_deps: bool,
    // The directory to load the dbt project from
    pub vars: BTreeMap<String, dbt_serde_yaml::Value>,
    /// Whether this is the main command or a subcommand
    pub from_main: bool,
    /// threads
    pub threads: Option<usize>,
    /// Install dependencies
    pub install_deps: bool,
    /// add_package cli option
    pub add_package: Option<String>,
    /// upgrade dependencies
    pub upgrade: bool,
    /// generate lock file only
    pub lock: bool,
    // Whether to load only profiles
    pub debug_profile: bool,
    /// Inline SQL to compile (from --inline flag)
    pub inline_sql: Option<String>,
    /// This is for incremental.
    /// The [DbtState] of the previouis compile.
    /// Setting this will cause the 'load' phase to skip a lot of work
    /// and only check the file in the root package.
    pub prev_dbt_state: Option<Arc<DbtState>>,
}

impl LoadArgs {
    pub fn from_eval_args(arg: &EvalArgs) -> Self {
        Self {
            command: arg.command.clone(),
            io: arg.io.clone(),
            profile: arg.profile.clone(),
            profiles_dir: arg.profiles_dir.clone(),
            packages_install_path: arg.packages_install_path.clone(),
            internal_packages_install_path: arg.internal_packages_install_path.clone(),
            target: arg.target.clone(),
            update_deps: arg.update_deps,
            add_package: arg.add_package.clone(),
            upgrade: arg.upgrade,
            lock: arg.lock,
            vars: arg.vars.clone(),
            from_main: arg.from_main,
            threads: arg.num_threads,
            install_deps: arg.phase == Phases::Deps,
            debug_profile: arg.phase == Phases::Debug,
            inline_sql: None, // Will be set separately when needed
            prev_dbt_state: None,
        }
    }

    /// Set the inline SQL for compilation
    pub fn with_inline_sql(mut self, inline_sql: Option<String>) -> Self {
        self.inline_sql = inline_sql;
        self
    }
}
