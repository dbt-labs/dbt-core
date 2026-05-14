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

pub use run_tasks_args::RunTasksArgs;
pub use stats_to_results::stats_to_results;

use dbt_common::FsResult;
use dbt_schemas::stats::Stats;

/// Abstract storage for task results. Implementations write serialized output
/// on demand. `None` storage on `RunTasksOk` means nothing to write.
pub trait TaskResultStorage: Send + Sync + std::fmt::Debug {
    fn write_results(&self, writer: &mut dyn std::io::Write) -> FsResult<()>;
}

/// Core result type from running dbt tasks (compile + run statistics).
#[derive(Debug, Default)]
pub struct RunTasksOk {
    pub compile_stats: Stats,
    pub run_stats: Stats,
    pub storage: Option<Box<dyn TaskResultStorage>>,
}

impl RunTasksOk {
    pub fn write_results(&self, writer: &mut dyn std::io::Write) -> FsResult<()> {
        if let Some(s) = &self.storage {
            s.write_results(writer)
        } else {
            Ok(())
        }
    }
}
