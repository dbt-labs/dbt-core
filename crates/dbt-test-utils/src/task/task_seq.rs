use super::io::{RmTask, TouchTask};
use super::tasks::{NopTask, ShExecute};
use super::utils::{check_set_user_env_var, redirect_buffer_to_stdin, strip_full_test_name};
use super::{ProjectEnv, Task, TestEnv, TestResult};

use dbt_common::error::FsResult;
use dbt_common::string_utils::split_into_whitespace_and_brackets;
use std::future::Future;
use std::iter;
use std::path::{Path, PathBuf};
use std::pin::Pin;

pub type BoxedSendFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub type CommandFn = dyn Fn(Vec<String>, PathBuf, std::fs::File, std::fs::File) -> BoxedSendFuture<FsResult<i32>>
    + Send
    + Sync;

pub fn fs_cmd_vec(cmd: impl AsRef<str>) -> Vec<String> {
    let cmd_str = cmd.as_ref();
    let mut parts = split_into_whitespace_and_brackets(cmd_str);

    // Only add --show progress if --show is not already present
    if !parts.iter().any(|s| s == "--show") {
        parts.push("--show".to_string());
        parts.push("progress".to_string());
    }

    iter::once("fs".to_string())
        .chain(parts)
        .collect::<Vec<_>>()
}

/// A sequence of tasks. Created tasks are executed lazily. The
/// sequence can be executed multiple times using same or a different
/// workspace.
pub struct TaskSeq {
    name: String,
    full_name: String,
    tasks: Vec<Box<dyn Task>>,
}

impl TaskSeq {
    pub fn new(full_test_name: impl Into<String>) -> Self {
        let full_name = full_test_name.into();
        let name = strip_full_test_name(full_name.as_str());
        Self {
            name,
            full_name,
            tasks: Vec::new(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Construct a unique path for this test.
    pub fn unique_path(&self) -> PathBuf {
        PathBuf::from(self.full_name.replace("::", "/"))
    }

    /// Creates an arbitrary Task in the sequence. This can be useful
    /// to inspect target_dir or anything in the middle or at the end
    /// of the sequence.
    pub fn task(&mut self, task: Box<dyn Task>) -> &mut Self {
        self.tasks.push(task);
        self
    }

    /// Creates a task for run a shell command.  NOTE: using this
    /// command will lead to platform dependent tests and should be
    /// used as appropriate.
    pub fn sh(&mut self, cmd_vec: &[impl ToString]) -> &mut Self {
        self.task(Box::new(ShExecute::new(
            self.name().to_owned(),
            cmd_vec.iter().map(|s| s.to_string()).collect(),
        )))
    }

    /// Creates a task that does not do anything. This task can be
    /// used to increase the task count without any work in case we
    /// need to skip/mock some steps, e.g., during execution that are
    /// done during update.
    pub fn nop(&mut self) -> &mut Self {
        self.task(Box::new(NopTask))
    }

    /// Creates a touch task on the given path.
    pub fn touch(&mut self, path: impl Into<String>) -> &mut Self {
        self.task(Box::new(TouchTask::new(path)))
    }

    /// Creates a task to write the given content to the file at the specified
    /// path.
    pub fn write_file(
        &mut self,
        file_path: impl Into<String>,
        content: impl Into<String>,
    ) -> &mut Self {
        self.task(Box::new(super::io::FileWriteTask::new(file_path, content)))
    }

    /// Creates a remove task to delete the file at the given path.
    pub fn rm_file(&mut self, path: impl Into<String>) -> &mut Self {
        self.task(Box::new(RmTask::new(path)))
    }

    /// Executes this sequence in the given environment, with the given buffer
    /// as stdin.
    ///
    /// This is useful for testing commands that read from stdin, e.g. `run -i`.
    pub async fn execute_in_with_stdin(
        &self,
        workspace: &ProjectEnv,
        buffer: &str,
    ) -> TestResult<()> {
        let _temp_file = redirect_buffer_to_stdin(buffer)?;
        self.execute_in(workspace).await?;
        Ok(())
    }

    /// Executes this sequence in the given environment.
    pub async fn execute_in(&self, project_env: &ProjectEnv) -> TestResult<()> {
        let test_env = project_env.create_test_env()?;
        let _cwd_guard = CurrentWorkingDirGuard::new(&project_env.absolute_project_dir);
        run_test_tasks(&self.tasks, project_env, &test_env).await?;

        Ok(())
    }
}

struct CurrentWorkingDirGuard {
    original_dir: PathBuf,
}

impl CurrentWorkingDirGuard {
    fn new(dir: impl AsRef<Path>) -> Self {
        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir.as_ref()).unwrap();
        Self { original_dir }
    }
}

impl Drop for CurrentWorkingDirGuard {
    fn drop(&mut self) {
        std::env::set_current_dir(&self.original_dir).unwrap();
    }
}

async fn run_test_tasks(
    tasks: &[Box<dyn Task + '_>],
    project_env: &ProjectEnv,
    test_env: &TestEnv,
) -> TestResult<()> {
    check_set_user_env_var();

    let mut index = 0;
    for task in tasks {
        task.run(project_env, test_env, index).await?;
        if task.is_counted() {
            index += 1;
        }
    }

    Ok(())
}
