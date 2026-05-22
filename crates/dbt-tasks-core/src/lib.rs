#![allow(clippy::cognitive_complexity)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::type_complexity)]

pub mod compiled_sql_cache;
pub mod compiler_env;
pub mod context;
pub mod local_schema_builder;
pub mod metricflow;
pub mod precompile;
pub mod pretty_table;
mod run_tasks_args;
pub mod schema_hydration;
pub mod span_manager;
mod stats_to_results;
pub mod task;
pub mod task_spans;
pub mod test_aggregation;
pub mod utils;
pub mod visitor;

use std::path::PathBuf;
use std::sync::Arc;
use std::{fmt, io};

use dbt_jinja_utils::jinja_environment::JinjaEnv;
pub use run_tasks_args::RunTasksArgs;
pub use stats_to_results::stats_to_results;

use dbt_common::FsResult;
use dbt_common::cancellation::CancellationToken;
use dbt_common::io_args::EvalArgs;
use dbt_dag::schedule::Schedule;
use dbt_schemas::state::ResolverState;
use dbt_schemas::stats::Stats;

use crate::compiler_env::CompilerEnv;

/// Preview/show results produced during task execution.
pub struct Preview {
    pub columns: Vec<String>,
    pub rows: Vec<serde_json::Value>,
}

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

/// Results that can show themselves to the user during the did_compile phase.
pub trait ShowableResults: Send + Sync + fmt::Debug {
    fn show(
        &self,
        arg: &EvalArgs,
        resolved_state: &ResolverState,
        schedule: &Schedule<String>,
        token: &CancellationToken,
    ) -> FsResult<()>;

    fn as_any(&self) -> &dyn std::any::Any;
}

pub struct PreRunTasksOk {
    pub compiler_env: CompilerEnv,
    pub resolved_state: Arc<ResolverState>,
    pub jinja_env: Arc<JinjaEnv>,
}

/// Core result type from running dbt tasks (compile + run statistics).
#[derive(Debug, Default)]
pub struct RunTasksOk {
    pub compile_stats: Stats,
    pub run_stats: Stats,
    pub storeables: Vec<Box<dyn StoreableResults>>,
    pub showables: Vec<Box<dyn ShowableResults>>,
}
