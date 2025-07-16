use super::{ProjectEnv, Task, TestEnv, TestResult};
use async_trait::async_trait;
use dbt_common::logging::dbt_compat_log::LogEntry;
use dbt_common::stdfs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// A task wrapper that captures JSON logs from the stdout of the inner task
pub struct ExecuteAndCaptureLogs {
    inner: Box<dyn Task + Send + Sync>,
    captured_logs: Arc<Mutex<Vec<LogEntry>>>,
    name: String,
}

impl ExecuteAndCaptureLogs {
    /// Create a new log capturing task that wraps an existing task
    pub fn new(name: String, inner: Box<dyn Task + Send + Sync>) -> Self {
        Self {
            inner,
            captured_logs: Arc::new(Mutex::new(Vec::new())),
            name,
        }
    }

    /// Get the captured logs after execution
    pub fn get_logs(&self) -> Vec<LogEntry> {
        self.captured_logs.lock().unwrap().clone()
    }

    /// Parse logs from a file containing JSON log entries
    fn parse_logs_from_file(path: &PathBuf) -> Vec<LogEntry> {
        let mut logs = Vec::new();
        if let Ok(content) = stdfs::read_to_string(path) {
            for line in content.lines() {
                // Skip empty lines
                if line.trim().is_empty() {
                    continue;
                }

                match serde_json::from_str::<LogEntry>(line) {
                    Ok(log_entry) => {
                        assert!(log_entry.data.is_some());
                        logs.push(log_entry);
                    }
                    Err(e) => {
                        eprintln!("Failed to parse log entry: {line}\nError: {e:?}\nLine: {line}");
                        panic!("Failed to parse log entry");
                    }
                };
            }
        }

        logs
    }
}

#[async_trait]
impl Task for ExecuteAndCaptureLogs {
    async fn run(
        &self,
        project_env: &ProjectEnv,
        test_env: &TestEnv,
        task_index: usize,
    ) -> TestResult<()> {
        // Run the inner task first
        self.inner.run(project_env, test_env, task_index).await?;

        // Determine the stdout file path
        let task_suffix = if task_index > 0 {
            format!("_{task_index}")
        } else {
            "".to_string()
        };

        let stdout_path = test_env
            .temp_dir
            .join(format!("{}{}.stdout", self.name, task_suffix));

        // Parse logs from stdout and stderr
        let stdout_logs = Self::parse_logs_from_file(&stdout_path);
        let stderr_path = test_env
            .temp_dir
            .join(format!("{}{}.stderr", self.name, task_suffix));
        let stderr_logs = Self::parse_logs_from_file(&stderr_path);

        // Combine logs from both stdout and stderr
        let mut logs = stdout_logs;
        logs.extend(stderr_logs);

        // Store the captured logs
        *self.captured_logs.lock().unwrap() = logs;

        Ok(())
    }

    fn is_counted(&self) -> bool {
        self.inner.is_counted()
    }
}

#[async_trait]
impl Task for Arc<ExecuteAndCaptureLogs> {
    async fn run(
        &self,
        project_env: &ProjectEnv,
        test_env: &TestEnv,
        task_index: usize,
    ) -> TestResult<()> {
        self.as_ref().run(project_env, test_env, task_index).await
    }

    fn is_counted(&self) -> bool {
        self.as_ref().is_counted()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_common::logging::dbt_compat_log::{
        log_entry::Data, CompletionLogData, ExecutingLogData, LogInfo,
    };

    #[test]
    fn test_parse_logs_from_content() {
        // Create a basic log entry directly
        let basic_log = LogEntry {
            info: Some(LogInfo {
                category: "test".to_string(),
                code: "T001".to_string(),
                extra: std::collections::HashMap::new(),
                invocation_id: "test-123".to_string(),
                level: "info".to_string(),
                msg: "Test message".to_string(),
                name: "TestLog".to_string(),
                pid: 1234,
                thread: "main".to_string(),
                ts: "2023-01-01T00:00:00Z".to_string(),
                elapsed: None,
            }),
            data: Some(Data::ExecutionData(ExecutingLogData {
                log_version: 3,
                version: "1.0.0".to_string(),
                node_info: None,
            })),
        };

        // Serialize and test parsing
        let serialized = serde_json::to_string(&basic_log).unwrap();
        println!("Serialized basic log: {serialized}");

        let temp_dir = tempfile::tempdir().unwrap();
        let log_file = temp_dir.path().join("test.log");
        stdfs::write(&log_file, &serialized).unwrap();

        let logs = ExecuteAndCaptureLogs::parse_logs_from_file(&log_file);
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].info.as_ref().unwrap().name, "TestLog");
        assert_eq!(
            logs[0].data.as_ref().unwrap(),
            &Data::ExecutionData(ExecutingLogData {
                log_version: 3,
                version: "1.0.0".to_string(),
                node_info: None,
            })
        );

        // Test completion log format
        let completion_log = LogEntry {
            info: Some(LogInfo {
                category: "".to_string(),
                code: "".to_string(),
                extra: std::collections::HashMap::new(),
                invocation_id: "test-123".to_string(),
                level: "info".to_string(),
                msg: "Completed".to_string(),
                name: "CommandCompleted".to_string(),
                pid: 1234,
                thread: "main".to_string(),
                ts: "2023-01-01T00:00:00Z".to_string(),
                elapsed: None,
            }),
            data: Some(Data::Completion(CompletionLogData {
                log_version: 3,
                version: "1.0.0".to_string(),
                completed_at: "2023-01-01T00:00:00Z".to_string(),
                elapsed: 0.5,
                success: true,
            })),
        };

        let serialized2 = serde_json::to_string(&completion_log).unwrap();
        let log_file2 = temp_dir.path().join("test2.log");
        stdfs::write(&log_file2, &serialized2).unwrap();

        let logs2 = ExecuteAndCaptureLogs::parse_logs_from_file(&log_file2);
        assert_eq!(logs2.len(), 1);
        assert_eq!(logs2[0].info.as_ref().unwrap().name, "CommandCompleted");
        assert_eq!(
            logs2[0].data.as_ref().unwrap(),
            &Data::Completion(CompletionLogData {
                log_version: 3,
                version: "1.0.0".to_string(),
                completed_at: "2023-01-01T00:00:00Z".to_string(),
                elapsed: 0.5,
                success: true,
            })
        );
    }
}
