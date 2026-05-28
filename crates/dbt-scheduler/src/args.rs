use std::collections::HashSet;

use dbt_common::io_args::{ClapResourceType, EvalArgs, FsCommand, IoArgs};

#[derive(Clone, Default, Debug)]
pub struct SchedulerArgs {
    pub command: FsCommand,
    pub io: IoArgs,
    pub resource_types: Vec<ClapResourceType>,
    pub exclude_resource_types: Vec<ClapResourceType>,
    /// A set of unique ids to be exluded when scheduling.
    /// This is more efficient than relying on exclusions from resolved selectors.
    /// REVIEW: If we can make expanding exclude selectors fast, then this will not be needed.
    pub exclude_unique_ids: HashSet<String>,
}

impl SchedulerArgs {
    pub fn from_eval_args(arg: &EvalArgs) -> Self {
        Self {
            command: arg.command,
            io: arg.io.clone(),
            resource_types: arg.resource_types.clone(),
            exclude_resource_types: arg.exclude_resource_types.clone(),
            exclude_unique_ids: Default::default(),
        }
    }

    pub fn from_eval_args_with_exclude_unique_ids(
        arg: &EvalArgs,
        exclude_unique_ids: HashSet<String>,
    ) -> Self {
        Self {
            command: arg.command,
            io: arg.io.clone(),
            resource_types: arg.resource_types.clone(),
            exclude_resource_types: arg.exclude_resource_types.clone(),
            exclude_unique_ids,
        }
    }
}
