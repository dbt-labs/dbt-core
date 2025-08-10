//! Module defines the input arguments required for resolution

use dbt_common::FsResult;
use dbt_common::io_args::IoArgs;
use dbt_common::{
    io_args::EvalArgs,
    node_selector::{IndirectSelection, SelectExpression},
};
use dbt_schemas::filter::RunFilter;
use std::collections::BTreeMap;

/// Args to be passed into the resolution phase
#[derive(Clone, Default, Debug)]
pub struct ResolveArgs {
    /// The command to run
    pub command: String,
    /// All io args
    pub io: IoArgs,
    /// Vars to pass to the jinja environment
    pub vars: BTreeMap<String, dbt_serde_yaml::Value>,
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
    /// Number of tHreads to use
    pub num_threads: Option<usize>,
    /// replay mode
    pub replay: Option<dbt_common::io_args::ReplayMode>,
    /// Sample config
    pub sample_config: RunFilter,
}

impl ResolveArgs {
    /// Produce [ResolveArgs] from a set of [EvalArgs]
    pub fn try_from_eval_args(arg: &EvalArgs) -> FsResult<Self> {
        Ok(ResolveArgs {
            command: arg.command.clone(),
            io: arg.io.clone(),
            vars: arg.vars.clone(),
            from_main: arg.from_main,
            selector: arg.selector.clone(),
            select: arg.select.clone(),
            exclude: arg.exclude.clone(),
            num_threads: arg.num_threads,
            indirect_selection: arg.indirect_selection,
            replay: arg.replay.clone(),
            sample_config: RunFilter::try_from(arg.empty, arg.sample.clone())?,
        })
    }
}
