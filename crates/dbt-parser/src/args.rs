//! Module defines the input arguments required for resolution

use dbt_common::FsResult;
use dbt_common::io_args::{IoArgs, StaticAnalysisKind};
use dbt_common::{
    io_args::{EvalArgs, FsCommand},
    node_selector::{IndirectSelection, SelectExpression},
};
use dbt_schemas::filter::RunFilter;
use std::collections::BTreeMap;

/// Args to be passed into the resolution phase
#[derive(Clone, Default, Debug)]
pub struct ResolveArgs {
    /// The command to run
    pub command: FsCommand,
    /// All io args
    pub io: IoArgs,
    /// Vars to pass to the jinja environment
    pub vars: BTreeMap<String, dbt_yaml::Value>,
    /// Whether this is the main command or a subcommand
    pub from_main: bool,
    /// selector name
    pub selector: Option<String>,
    /// select
    pub select: Option<SelectExpression>,
    /// indirect selection
    pub indirect_selection: Option<IndirectSelection>,
    /// exclude
    pub exclude: Option<SelectExpression>,
    /// Connection-pool size resolved from the profile/CLI `threads` setting.
    /// Exposed to Jinja as `NUM_THREADS` for dbt parity; do NOT consult this
    /// to size parser parallelism — use [`no_parallel`] for that.
    pub num_threads: Option<usize>,
    /// Force sequential rendering/resolution. This is the only knob that
    /// controls parser parallelism; otherwise parse saturates CPUs.
    pub no_parallel: bool,
    /// replay mode
    pub replay: Option<dbt_common::io_args::ReplayMode>,
    /// Sample config
    pub sample_config: RunFilter,
    /// For remapping unique_is to (database, schema, table) when sampling is enabled
    pub sample_renaming: BTreeMap<String, (String, String, String)>,
    /// Global static analysis settings
    pub static_analysis: Option<StaticAnalysisKind>,
    /// Store failures?
    pub store_failures: bool,
    /// Whether to skip creating generic tests
    pub skip_creating_generic_tests: bool,
    /// Maximum size (MiB) for seed files whose contents are hashed
    /// 1 MiB default); `0` means "no limit".
    pub maximum_seed_size_mib: u64,
}

impl ResolveArgs {
    /// Produce [ResolveArgs] from a set of [EvalArgs]
    pub fn try_from_eval_args(arg: &EvalArgs) -> FsResult<Self> {
        Ok(ResolveArgs {
            command: arg.command,
            io: arg.io.clone(),
            vars: arg.vars.clone(),
            from_main: arg.from_main,
            selector: arg.selector.clone(),
            select: arg.select.clone(),
            exclude: arg.exclude.clone(),
            num_threads: arg.num_threads,
            no_parallel: arg.no_parallel,
            indirect_selection: arg.indirect_selection,
            replay: arg.replay.clone(),
            sample_config: RunFilter::try_from(arg.empty, arg.sample.clone())?,
            sample_renaming: arg.sample_renaming.clone(),
            static_analysis: arg.static_analysis,
            store_failures: arg.store_failures,
            skip_creating_generic_tests: arg.skip_creating_generic_tests,
            maximum_seed_size_mib: arg.maximum_seed_size_mib,
        })
    }
}
