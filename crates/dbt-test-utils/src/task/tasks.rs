//! Core tasks.

use std::{
    io::Write,
    path::PathBuf,
    process::Command,
    sync::{Arc, Mutex, atomic::AtomicI32},
};

use async_trait::async_trait;
use dbt_common::{FsResult, constants::DBT_INTERNAL_PACKAGES_DIR_NAME, stdfs};

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

/// A task that executes a command without comparing output to goldie files and captures stdout and stderr.
pub struct ExecuteOnly {
    name: String,
    cmd_vec: Vec<String>,
    func: Arc<CommandFn>,
    redirect_outputs: bool,
    stdout: Arc<Mutex<String>>,
    stderr: Arc<Mutex<String>>,
    exit_code: AtomicI32,
}

impl ExecuteOnly {
    /// Construct a new execute only task.
    ///
    /// If `redirect_outputs` is true, `target-path`, `project-dir`, and `log-path`
    /// will be added to the command vector automatically.
    pub fn new(
        name: String,
        cmd_vec: Vec<String>,
        func: Arc<CommandFn>,
        redirect_outputs: bool,
    ) -> Self {
        Self {
            name,
            cmd_vec,
            func,
            redirect_outputs,
            stdout: Arc::new(Mutex::new(String::default())),
            stderr: Arc::new(Mutex::new(String::default())),
            exit_code: AtomicI32::new(0),
        }
    }

    pub fn get_exit_code(&self) -> i32 {
        self.exit_code.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn get_stdout(&self) -> String {
        self.stdout.lock().expect("Lock is poisoned").clone()
    }

    pub fn get_stderr(&self) -> String {
        self.stderr.lock().expect("Lock is poisoned").clone()
    }
}

#[async_trait]
impl Task for ExecuteOnly {
    async fn run(
        &self,
        project_env: &ProjectEnv,
        test_env: &TestEnv,
        task_index: usize,
    ) -> TestResult<()> {
        let mut cmd_vec = self.cmd_vec.clone();

        // Prepare cli command using the common helper if `redirect_outputs` is true
        if self.redirect_outputs {
            cmd_vec = prepare_command_vec(
                cmd_vec,
                project_env,
                test_env,
                false, // don't filter brackets for ExecuteOnly
            );
        }

        // Create stdout and stderr files
        let task_suffix = if task_index > 0 {
            format!("_{task_index}")
        } else {
            "".to_string()
        };
        let stdout_path = test_env
            .temp_dir
            .join(format!("{}{}.stdout", self.name, task_suffix));
        let stderr_path = test_env
            .temp_dir
            .join(format!("{}{}.stderr", self.name, task_suffix));

        let stdout_file = stdfs::File::create(&stdout_path)?;
        let stderr_file = stdfs::File::create(&stderr_path)?;

        // Execute the command
        let res = (self.func)(
            cmd_vec,
            project_env.absolute_project_dir.clone(),
            stdout_file,
            stderr_file,
        )
        .await?;

        // Store stdout and stderr contents contents in the struct for later access if needed
        *self.stdout.lock().unwrap() = stdfs::read_to_string(&stdout_path)?;
        *self.stderr.lock().unwrap() = stdfs::read_to_string(&stderr_path)?;

        // Store exit code
        self.exit_code
            .store(res, std::sync::atomic::Ordering::SeqCst);

        // Don't compare with goldie files
        Ok(())
    }

    fn is_counted(&self) -> bool {
        true
    }
}

#[async_trait]
impl Task for Arc<ExecuteOnly> {
    async fn run(
        &self,
        project_env: &ProjectEnv,
        test_env: &TestEnv,
        task_index: usize,
    ) -> TestResult<()> {
        self.as_ref().run(project_env, test_env, task_index).await
    }

    fn is_counted(&self) -> bool {
        true
    }
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
