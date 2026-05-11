pub mod pretty_table;
mod run_tasks_args;
mod stats_to_results;

pub use run_tasks_args::RunTasksArgs;
pub use stats_to_results::stats_to_results;

use dbt_schemas::stats::Stats;

/// Core result type from running dbt tasks (compile + run statistics).
#[derive(Debug, Clone, Default)]
pub struct RunTasksOk {
    pub compile_stats: Stats,
    pub run_stats: Stats,
}
