#![allow(clippy::cognitive_complexity)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::type_complexity)]

pub mod context;
pub mod precompile;
pub mod pretty_table;
mod run_tasks_args;
pub mod span_manager;
mod stats_to_results;
pub mod task;
pub mod test_aggregation;
pub mod visitor;

use std::path::PathBuf;
use std::{fmt, io};

pub use run_tasks_args::RunTasksArgs;
pub use stats_to_results::stats_to_results;

use dbt_common::FsResult;
use dbt_schemas::stats::Stats;

/// Per-node data returned by pre-run hooks.
pub trait PreTaskRunData: Send + Sync {
    /// Returns a value for `node_id`, or `None` if not present.
    fn get(&self, node_id: &str) -> Option<String>;
}

/// Abstract storage for task results. Implementations write serialized output
/// on demand.
pub trait StoreableResults: Send + Sync + fmt::Debug {
    /// Path relative to the output directory where results should be written.
    fn out_dir_relpath(&self) -> PathBuf;
    fn write_results(&self, writer: &mut dyn io::Write) -> FsResult<()>;
}

/// Core result type from running dbt tasks (compile + run statistics).
#[derive(Debug, Default)]
pub struct RunTasksOk {
    pub compile_stats: Stats,
    pub run_stats: Stats,
    pub storeables: Vec<Box<dyn StoreableResults>>,
}
