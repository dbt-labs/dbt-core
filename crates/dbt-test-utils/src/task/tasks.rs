//! Core tasks.

use std::{io::Write, path::PathBuf, process::Command, sync::Arc};

use async_trait::async_trait;
use dbt_common::{FsResult, constants::DBT_INTERNAL_PACKAGES_DIR_NAME};

use super::{
    ProjectEnv, Task, TestEnv, TestError, TestResult, goldie::execute_and_compare,
    task_seq::CommandFn,
};

/// Common helper function to prepare command vector with standard DBT paths and options
pub fn prepare_command_vec(
    mut cmd_vec: Vec<String>,
    project_env: &ProjectEnv,
    test_env: &TestEnv,
    filter_brackets: bool,
) -> Vec<String> {
    let project_dir = &project_env.absolute_project_dir;
    let target_dir = &test_env.temp_dir.join("target");
    let logs_dir = &test_env.temp_dir.join("logs");
    let internal_packages_install_path = &test_env.temp_dir.join(DBT_INTERNAL_PACKAGES_DIR_NAME);

    // Filter command arguments if requested (for ExecuteAndCompare)
    if filter_brackets {
        cmd_vec = cmd_vec
            .iter()
            .map(|cmd| {
                if cmd.starts_with('{') && cmd.ends_with('}') {
                    cmd[1..cmd.len() - 1].to_string()
                } else {
                    cmd.to_string()
                }
            })
            .collect();
    }

    // Redirect logs unless it is already specified
    if !cmd_vec.iter().any(|s| s.starts_with("--log-path")) {
        cmd_vec.push(format!("--log-path={}", logs_dir.display()));
    }

    // Add standard DBT flags (allow thetest to fail if caller added them manually)
    cmd_vec.push(format!("--target-path={}", target_dir.display()));
    cmd_vec.push(format!("--project-dir={}", project_dir.display()));
    cmd_vec.push(format!(
        "--internal-packages-install-path={}",
        internal_packages_install_path.display()
    ));

    cmd_vec
}

pub struct ExecuteAndCompare {
    name: String,
    cmd_vec: Vec<String>,
    threads: usize,
    use_recording: bool,
    func: Arc<CommandFn>,
}

impl ExecuteAndCompare {
    /// Construct a new sequential execute and compare task
    pub fn new(
        name: String,
        mut cmd_vec: Vec<String>,
        func: Arc<CommandFn>,
        use_recording: bool,
    ) -> Self {
        cmd_vec.push("--threads=1".to_string());
        if !cmd_vec.iter().any(|s| *s == "--log-format") {
            cmd_vec.push("--log-format=text".to_string());
        }

        Self {
            name,
            cmd_vec,
            threads: 1,
            use_recording,
            func,
        }
    }

    /// Construct a new parallel execute and compare task
    pub fn new_parallel(
        name: String,
        mut cmd_vec: Vec<String>,
        func: Arc<CommandFn>,
        threads: usize,
    ) -> Self {
        cmd_vec.push(format!("--threads={threads}"));
        if !cmd_vec.iter().any(|s| *s == "--log-format") {
            cmd_vec.push("--log-format=text".to_string());
        }

        Self {
            name,
            cmd_vec,
            // Cannot use recording in parallel mode since order of events is
            // not deterministic
            threads,
            use_recording: false,
            func,
        }
    }
    // cmd_vec: &[String],
    // project_dir: PathBuf,
    // stdout_file: File,
    // stderr_file: File,
}

#[async_trait]
impl Task for ExecuteAndCompare {
    async fn run(
        &self,
        project_env: &ProjectEnv,
        test_env: &TestEnv,
        task_index: usize,
    ) -> TestResult<()> {
        // Prepare cli command using the common helper
        let mut cmd_vec = prepare_command_vec(
            self.cmd_vec.clone(),
            project_env,
            test_env,
            true, // filter brackets for ExecuteAndCompare
        );

        // Add recording flag if needed
        if self.use_recording {
            cmd_vec.push(format!(
                "--dbt-replay={}",
                test_env
                    .golden_dir
                    .join(format!("recording_{task_index}.json"))
                    .display()
            ));
        }

        match execute_and_compare(
            &self.name,
            cmd_vec.as_slice(),
            project_env,
            test_env,
            task_index,
            self.threads != 1,
            self.func.clone(),
        )
        .await
        {
            Ok(patches) if patches.is_empty() => Ok(()),
            Ok(patches) => Err(TestError::GoldieMismatch(patches)),
            Err(e) => Err(e.into()),
        }
    }

    fn is_counted(&self) -> bool {
        true
    }
}

pub struct NopTask;

#[async_trait]
impl Task for NopTask {
    async fn run(
        &self,
        _project_env: &ProjectEnv,
        _test_env: &TestEnv,
        _task_index: usize,
    ) -> TestResult<()> {
        Ok(())
    }

    fn is_counted(&self) -> bool {
        true
    }
}

/// Task to execute any sh command.
pub struct ShExecute {
    name: String,
    cmd_vec: Vec<String>,
}

impl ShExecute {
    pub fn new(name: String, raw_cmd: Vec<String>) -> Self {
        Self {
            name,
            cmd_vec: raw_cmd,
        }
    }
}

#[async_trait]
impl Task for ShExecute {
    async fn run(
        &self,
        project_env: &ProjectEnv,
        test_env: &TestEnv,
        task_index: usize,
    ) -> TestResult<()> {
        let boxed_fn: Arc<CommandFn> = Arc::new(|cmd_vec, dir, stdout, stderr| {
            Box::pin(exec_sh(cmd_vec, dir, stdout, stderr))
        });

        match execute_and_compare(
            &self.name,
            self.cmd_vec.as_slice(),
            project_env,
            test_env,
            task_index,
            false,
            boxed_fn,
        )
        .await
        {
            Ok(patches) if patches.is_empty() => Ok(()),
            Ok(patches) => Err(TestError::GoldieMismatch(patches)),
            Err(e) => Err(e.into()),
        }
    }

    fn is_counted(&self) -> bool {
        true
    }
}

// Util function to execute sh commands
async fn exec_sh(
    cmd_vec: Vec<String>,
    project_dir: PathBuf,
    stdout_file: std::fs::File,
    stderr_file: std::fs::File,
) -> FsResult<i32> {
    let status = Command::new(&cmd_vec[0])
        .args(&cmd_vec[1..])
        .stdout(
            stdout_file
                .try_clone()
                .expect("Could not clone stdout_file"),
        )
        .stderr(
            stderr_file
                .try_clone()
                .expect("Could not clone stderr_file"),
        )
        .current_dir(project_dir)
        .spawn();

    match status {
        Ok(mut child) => {
            child.wait().expect("Could not wait on process");
            Ok(0)
        }
        Err(e) => {
            writeln!(&stderr_file, "Error spawning command: {cmd_vec:?} {e}")
                .expect("Could not write");
            Ok(1)
        }
    }
}
