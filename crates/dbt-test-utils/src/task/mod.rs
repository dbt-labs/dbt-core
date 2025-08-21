mod artifact_validation;
mod assertions;
mod dbt;
mod env;
mod io;
mod log_capture;
mod manifest_capture;
mod profiles;
mod record_and_replay;
mod task_seq;

pub mod goldie;
pub mod tasks;
pub mod utils;

pub type TestResult<T> = Result<T, TestError>;

#[derive(Debug)]
pub enum TestError {
    GoldieMismatch(Vec<String>),
    Generic(Box<dyn Error>),
}

impl Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            TestError::GoldieMismatch(lines) => {
                write!(f, "Goldie mismatch:\n{}", lines.join("\n"))
            }
            TestError::Generic(err) => write!(f, "{err}"),
        }
    }
}

impl TestError {
    pub fn new(msg: impl Into<String>) -> Self {
        let e = FsError::new(ErrorCode::Generic, msg.into());
        TestError::Generic(Box::new(e))
    }
}

impl<E> From<E> for TestError
where
    E: Into<Box<dyn Error>>,
{
    fn from(err: E) -> Self {
        TestError::Generic(err.into())
    }
}

impl From<TestError> for FsError {
    fn from(err: TestError) -> Self {
        match err {
            TestError::GoldieMismatch(lines) => {
                let msg = format!("Goldie mismatch:\n{}", lines.join("\n"));
                FsError::new(ErrorCode::Generic, msg)
            }
            TestError::Generic(err) => FsError::new(ErrorCode::Generic, err.to_string()),
        }
    }
}

impl From<TestError> for Box<FsError> {
    fn from(err: TestError) -> Self {
        Box::new(FsError::from(err))
    }
}

use std::error::Error;
use std::fmt::Display;

use dbt_common::ErrorCode;
use dbt_common::FsError;
pub use env::{ProjectEnv, TestEnv};
pub use task_seq::{CommandFn, TaskSeq, fs_cmd_vec};

// Public tasks.
pub use artifact_validation::ArtifactComparisonTask;
pub use assertions::{AssertDirExistsTask, AssertFileContainsTask, AssertFileExistsTask};
pub use dbt::DbtRecordTask;
pub use io::{CpFromTargetTask, FileWriteTask, RmDirTask, RmTask, SedTask};
pub use log_capture::ExecuteAndCaptureLogs;
pub use manifest_capture::CaptureDbtManifest;
pub use profiles::HydrateProfilesTask;
pub use record_and_replay::RrTask;
pub use tasks::ExecuteAndCompare;
pub use tasks::prepare_command_vec;

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
