mod artifact_validation;
mod assertions;
mod dbt;
mod env;
mod io;
mod log_capture;
mod profiles;
mod record_and_replay;
mod task_seq;

pub mod goldie;
pub mod tasks;
pub mod utils;

pub type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

pub use env::{ProjectEnv, TestEnv};
pub use task_seq::{fs_cmd_vec, CommandFn, TaskSeq};

// Public tasks.
pub use artifact_validation::ArtifactComparisonTask;
pub use assertions::{AssertDirExistsTask, AssertFileContainsTask, AssertFileExistsTask};
pub use dbt::DbtRecordTask;
pub use io::{CpFromTargetTask, FileWriteTask, RmDirTask, RmTask, SedTask};
pub use log_capture::ExecuteAndCaptureLogs;
pub use profiles::HydrateProfilesTask;
pub use record_and_replay::RrTask;
pub use tasks::prepare_command_vec;
pub use tasks::ExecuteAndCompare;

use async_trait::async_trait;

#[async_trait]
pub trait Task {
    async fn run(
        &self,
        project_env: &ProjectEnv,
        test_env: &TestEnv,
        task_index: usize,
    ) -> TestResult<()>;

    /// Tells if the task is a main task or is a helper task (e.g.,
    /// touch).
    fn is_counted(&self) -> bool {
        false
    }
}
