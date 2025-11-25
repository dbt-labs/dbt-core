//! Core tasks.

use std::{
    io::Write,
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Mutex, atomic::AtomicI32},
};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use dbt_common::{ErrorCode, FsResult, constants::DBT_INTERNAL_PACKAGES_DIR_NAME, err, stdfs};
use dbt_telemetry::TelemetryRecord;
use dbt_test_primitives::is_update_golden_files_mode;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::task::{
    goldie::{TextualPatch, diff_goldie},
    utils::{
        maybe_normalize_schema_name, maybe_normalize_tmp_paths, normalize_inline_sql_files,
        normalize_version,
    },
};

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

        let mut target_dir = project_env.absolute_project_dir.join("target");
        // Prepare cli command using the common helper if `redirect_outputs` is true
        if self.redirect_outputs {
            cmd_vec = prepare_command_vec(
                cmd_vec,
                project_env,
                test_env,
                false, // don't filter brackets for ExecuteOnly
            );
            target_dir = test_env.temp_dir.join("target");
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
            target_dir,
            stdout_file,
            stderr_file,
            test_env.get_tracing_handle(),
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

type TelemetryArrowDeserializer =
    fn(&RecordBatch) -> Result<Vec<TelemetryRecord>, Box<dyn std::error::Error>>;

pub struct ExecuteAndCompareTelemetry {
    name: String,
    cmd_vec: Vec<String>,
    func: Arc<CommandFn>,
    telemetry_deserializer: TelemetryArrowDeserializer,
}

impl ExecuteAndCompareTelemetry {
    const OTEL_JSONL_FILE_NAME: &str = "otel.jsonl";
    const OTEL_PARQUET_FILE_NAME: &str = "otel.parquet";
    const TELEMETRY_INVOCATION_ID: &str = "424242424242";

    pub fn new(
        name: String,
        mut cmd_vec: Vec<String>,
        func: Arc<CommandFn>,
        telemetry_deserializer: TelemetryArrowDeserializer,
    ) -> Self {
        Self::assert_flag_absent(
            &cmd_vec,
            "--otel-file-name",
            "ExecuteAndCompareTelemetry sets --otel-file-name automatically",
        );
        Self::assert_flag_absent(
            &cmd_vec,
            "--otel-parquet-file-name",
            "ExecuteAndCompareTelemetry sets --otel-parquet-file-name automatically",
        );
        Self::assert_flag_absent(
            &cmd_vec,
            "--invocation-id",
            "ExecuteAndCompareTelemetry sets --invocation-id automatically",
        );
        Self::assert_flag_absent(
            &cmd_vec,
            "--threads",
            "ExecuteAndCompareTelemetry forces --threads=1",
        );

        cmd_vec.push("--threads=1".to_string());
        cmd_vec.push(format!("--otel-file-name={}", Self::OTEL_JSONL_FILE_NAME));
        cmd_vec.push(format!(
            "--otel-parquet-file-name={}",
            Self::OTEL_PARQUET_FILE_NAME
        ));
        cmd_vec.push(format!("--invocation-id={}", Self::TELEMETRY_INVOCATION_ID));

        Self {
            name,
            cmd_vec,
            func,
            telemetry_deserializer,
        }
    }

    fn assert_flag_absent(cmd_vec: &[String], flag: &str, message: &str) {
        if cmd_vec.iter().any(|arg| arg.contains(flag)) {
            panic!("{message}");
        }
    }

    /// Compare telemetry outputs (JSONL and parquet formats) against golden snapshots.
    /// Processes both telemetry formats and returns any differences found.
    fn compare_telemetry(
        &self,
        cmd_vec: &[String],
        project_env: &ProjectEnv,
        test_env: &TestEnv,
        task_index: usize,
    ) -> FsResult<Vec<TextualPatch>> {
        // Resolve actual output directories from command flags or defaults
        let log_dir = Self::resolve_path(
            Self::extract_flag_value(cmd_vec, "--log-path"),
            &project_env.absolute_project_dir,
            test_env.temp_dir.join("logs"),
        );
        let target_dir = Self::resolve_path(
            Self::extract_flag_value(cmd_vec, "--target-path"),
            &project_env.absolute_project_dir,
            test_env.temp_dir.join("target"),
        );

        let task_suffix = Self::task_suffix(task_index);

        // Path to native JSONL telemetry output (written directly by the CLI)
        let actual_jsonl_path = log_dir.join(Self::OTEL_JSONL_FILE_NAME);
        // Path to parquet telemetry output (written to target/metadata by the CLI)
        let actual_parquet_path = target_dir
            .join("metadata")
            .join(Self::OTEL_PARQUET_FILE_NAME);

        // Golden snapshot paths: separate files for native JSONL and parquet-derived JSONL
        let golden_jsonl_path = test_env
            .golden_dir
            .join(format!("{}{}.otel.jsonl", self.name, task_suffix));
        let golden_parquet_jsonl_path = test_env
            .golden_dir
            .join(format!("{}{}.otel.parquet.jsonl", self.name, task_suffix));

        // Verify native JSONL output exists
        if !actual_jsonl_path.exists() {
            return err!(
                ErrorCode::FileNotFound,
                "expected telemetry jsonl file at {} but it was not produced",
                actual_jsonl_path.display()
            );
        }

        // Read and postprocess native JSONL telemetry
        let actual_jsonl_content =
            Self::postprocess_jsonl(stdfs::read_to_string(&actual_jsonl_path)?);

        // Verify parquet output exists
        if !actual_parquet_path.exists() {
            return err!(
                ErrorCode::FileNotFound,
                "expected telemetry parquet file at {} but it was not produced",
                actual_parquet_path.display()
            );
        }

        // Deserialize parquet to records, then serialize to JSONL with postprocessing
        let actual_parquet_bytes = stdfs::read(&actual_parquet_path)?;
        let actual_parquet_as_jsonl = self.parquet_to_jsonl(actual_parquet_bytes)?;

        if is_update_golden_files_mode() {
            // Update golden snapshots: native JSONL and parquet-derived JSONL
            stdfs::write(&golden_jsonl_path, actual_jsonl_content)?;
            stdfs::write(&golden_parquet_jsonl_path, actual_parquet_as_jsonl)?;
            return Ok(vec![]);
        }

        // Compare native JSONL telemetry against its snapshot
        let patches = diff_goldie(
            "jsonl telemetry",
            actual_jsonl_content,
            false, // single backslash is JSON is not a path separator, so avoid the default normalization
            &golden_jsonl_path,
            Self::postprocess_jsonl,
        )
        .into_iter()
        // Compare parquet-derived JSONL against its snapshot
        .chain(diff_goldie(
            "parquet telemetry",
            actual_parquet_as_jsonl,
            false, // single backslash is JSON is not a path separator, so avoid the default normalization
            &golden_parquet_jsonl_path,
            Self::postprocess_jsonl,
        ))
        .collect::<Vec<_>>();

        Ok(patches)
    }

    /// Convert parquet bytes to JSONL format with postprocessing applied.
    /// Deserializes parquet to telemetry records, serializes each to JSON, and applies normalization.
    fn parquet_to_jsonl(&self, parquet_bytes: Vec<u8>) -> FsResult<String> {
        // Deserialize parquet to telemetry records
        let records = Self::read_parquet_to_records(parquet_bytes, self.telemetry_deserializer)?;

        // Convert each record to a JSON line
        let jsonl_lines: Result<Vec<String>, _> = records
            .into_iter()
            .map(|record| {
                serde_json::to_string(&record).map_err(|err| {
                    Box::new(dbt_common::FsError::new(
                        ErrorCode::SerializationError,
                        format!("failed to serialize telemetry record to json: {err}"),
                    ))
                })
            })
            .collect();
        let jsonl_lines = jsonl_lines?;

        // Join lines and apply postprocessing to normalize volatile fields
        let jsonl = jsonl_lines.join("\n");
        Ok(Self::postprocess_jsonl(jsonl))
    }

    /// Read and deserialize parquet bytes into telemetry records using the provided deserializer.
    fn read_parquet_to_records(
        parquet_bytes: Vec<u8>,
        deserializer: TelemetryArrowDeserializer,
    ) -> FsResult<Vec<TelemetryRecord>> {
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from_owner(parquet_bytes))
                .map_err(|err| {
                    dbt_common::FsError::new(
                        ErrorCode::ParquetError,
                        format!("failed to construct parquet reader: {err}"),
                    )
                })?
                .build()
                .map_err(|err| {
                    dbt_common::FsError::new(
                        ErrorCode::ParquetError,
                        format!("failed to build parquet batch reader: {err}"),
                    )
                })?;

        let mut records = Vec::new();
        for batch in reader {
            let batch = batch.map_err(|err| {
                dbt_common::FsError::new(
                    ErrorCode::ParquetError,
                    format!("failed to read parquet batch: {err}"),
                )
            })?;
            let mut batch_records = deserializer(&batch).map_err(|err| {
                dbt_common::FsError::new(
                    ErrorCode::ParquetError,
                    format!("failed to deserialize telemetry records: {err}"),
                )
            })?;
            records.append(&mut batch_records);
        }

        Ok(records)
    }

    /// Resolve a path from command flag value, making it absolute if relative to project dir.
    fn resolve_path(maybe_path: Option<String>, project_dir: &Path, default: PathBuf) -> PathBuf {
        match maybe_path {
            Some(value) => {
                let candidate = PathBuf::from(value);
                if candidate.is_relative() {
                    project_dir.join(candidate)
                } else {
                    candidate
                }
            }
            None => default,
        }
    }

    /// Generate a suffix string for golden file names based on task index (e.g., "_1" or "").
    fn task_suffix(task_index: usize) -> String {
        if task_index > 0 {
            format!("_{task_index}")
        } else {
            String::new()
        }
    }

    /// Extract the value of a command-line flag from the command vector.
    fn extract_flag_value(cmd_vec: &[String], flag: &str) -> Option<String> {
        let prefix = format!("{flag}=");
        let iter = cmd_vec.iter().enumerate();
        for (idx, arg) in iter {
            if arg == flag {
                return cmd_vec.get(idx + 1).cloned();
            }
            if arg.starts_with(&prefix) {
                return Some(arg[prefix.len()..].to_string());
            }
        }
        None
    }

    /// Normalize environment-dependent keys in JSONL content to make tests reproducible.
    fn normalize_volatile_keys(content: String) -> String {
        const KEYS: &[&str] = &[
            // Keys that contain timestamps in nanoseconds since epoch
            "time_unix_nano",
            "start_time_unix_nano",
            "end_time_unix_nano",
            // durations
            "duration_ms",
            // raw command includes local paths
            "raw_command",
            // Keys with absolute paths
            "log_path",
            "project_dir",
            "target_path",
            // sql hash is different during recording due to random schema
            "sql_hash",
            // these are obviously environment-dependent
            "host_os",
            "host_arch",
            // dbt version changes frequently and it's embedded in process & invocation spans
            "version",
        ];

        fn find_key_and_normalize(value: &mut serde_json::Value, key: &str) {
            if let Some(obj) = value.as_object_mut() {
                if let Some(v) = obj.get_mut(key) {
                    if v.is_string() {
                        *v = serde_json::Value::String("<normalized>".to_string());
                    } else if v.is_number() {
                        *v = serde_json::Value::Number(serde_json::Number::from(0));
                    } else if v.is_array() {
                        *v = serde_json::Value::Array(vec![]);
                    } else if v.is_object() {
                        *v = serde_json::Value::Object(serde_json::Map::new());
                    } else if v.is_boolean() {
                        *v = serde_json::Value::Bool(false);
                    } else if v.is_null() {
                        // do nothing
                    }
                } else {
                    for (_k, v) in obj.iter_mut() {
                        find_key_and_normalize(v, key);
                    }
                }
            } else if let Some(arr) = value.as_array_mut() {
                for v in arr.iter_mut() {
                    find_key_and_normalize(v, key);
                }
            }
        }

        content
            .lines()
            .map(|line| {
                if line.trim().is_empty() {
                    return String::new();
                }

                let mut json: serde_json::Value = serde_json::from_str(line)
                    .unwrap_or_else(|_| panic!("Failed to parse jsonl line: {line}"));

                for key in KEYS {
                    find_key_and_normalize(&mut json, key);
                }

                serde_json::to_string(&json).expect("Failed to serialize modified jsonl line")
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Remove keys from JSONL that cannot be replayed (e.g., query_id from warehouse responses).
    fn strip_non_replayable_keys(content: String) -> String {
        // Keys whose values we strip because the replay mechanism cannot reproduce them
        const KEYS: &[&str] = &["attributes.query_id"];

        fn remove_key_by_path(value: &mut serde_json::Value, path: &[&str]) {
            if path.is_empty() {
                return;
            }
            if let Some(obj) = value.as_object_mut() {
                if path.len() == 1 {
                    obj.remove(path[0]);
                } else if let Some(next) = obj.get_mut(path[0]) {
                    remove_key_by_path(next, &path[1..]);
                }
            }
        }

        content
            .lines()
            .map(|line| {
                if line.trim().is_empty() {
                    return String::new();
                }

                let mut json: serde_json::Value = serde_json::from_str(line)
                    .unwrap_or_else(|_| panic!("Failed to parse jsonl line: {line}"));

                for key in KEYS {
                    let parts: Vec<&str> = key.split('.').collect();
                    remove_key_by_path(&mut json, &parts);
                }

                serde_json::to_string(&json).expect("Failed to serialize modified jsonl line")
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Normalize path separators in JSON strings to make Windows/Unix tests compatible.
    /// On Windows, replaces slashes with '|' to ignore separator differences; no-op on other platforms.
    fn json_safe_normalize_slashes(output: String) -> String {
        #[cfg(windows)]
        {
            output.replace("\\\\", "|").replace("/", "|")
        }
        #[cfg(not(windows))]
        {
            output
        }
    }

    /// Apply all normalization transforms to JSONL content for stable golden file comparison.
    fn postprocess_jsonl(content: String) -> String {
        [
            maybe_normalize_schema_name,
            maybe_normalize_tmp_paths,
            Self::normalize_volatile_keys,
            Self::strip_non_replayable_keys,
            Self::json_safe_normalize_slashes,
            normalize_version,
            normalize_inline_sql_files,
        ]
        .iter()
        .fold(content, |acc, transform| transform(acc))
    }
}

#[async_trait]
impl Task for ExecuteAndCompareTelemetry {
    async fn run(
        &self,
        project_env: &ProjectEnv,
        test_env: &TestEnv,
        task_index: usize,
    ) -> TestResult<()> {
        // Prepare the remaining cli command using the common helper
        let cmd_vec = prepare_command_vec(self.cmd_vec.clone(), project_env, test_env, true);

        let mut patches = match execute_and_compare(
            &self.name,
            cmd_vec.as_slice(),
            project_env,
            test_env,
            task_index,
            false,
            self.func.clone(),
        )
        .await
        {
            Ok(patches) => patches,
            Err(e) => return Err(e.into()),
        };

        let mut telemetry_patches =
            self.compare_telemetry(&cmd_vec, project_env, test_env, task_index)?;
        patches.append(&mut telemetry_patches);

        if patches.is_empty() {
            Ok(())
        } else {
            Err(TestError::GoldieMismatch(patches))
        }
    }

    fn is_counted(&self) -> bool {
        true
    }
}

#[async_trait]
impl Task for Arc<ExecuteAndCompareTelemetry> {
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
        let boxed_fn: Arc<CommandFn> = Arc::new(|cmd_vec, dir, _, stdout, stderr, _| {
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
