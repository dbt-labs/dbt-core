use std::ffi::OsStr;
use std::path::Path;

use async_trait::async_trait;
use dbt_common::stdfs;
use dbt_test_primitives::is_update_golden_files_mode;

use super::{ProjectEnv, Task, TestEnv, TestError, TestResult};
use crate::task::goldie::diff_goldie;

/// Generic task that reads a file from the project directory and compares its
/// content against a golden file. Updates the golden if `GOLDIE_UPDATE=1`.
///
/// Works for any text file: `package-lock.yml`, `run_results.json`, etc.
pub struct CompareFileGolden {
    /// Name used for the golden file (typically the test name).
    name: String,
    /// Relative path under the project dir to the file to compare.
    file_path: String,
}

impl CompareFileGolden {
    pub fn new(name: impl Into<String>, file_path: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            file_path: file_path.into(),
        }
    }
}

#[async_trait]
impl Task for CompareFileGolden {
    async fn run(
        &self,
        project_env: &ProjectEnv,
        test_env: &TestEnv,
        task_index: usize,
    ) -> TestResult<()> {
        let target_path = project_env.absolute_project_dir.join(&self.file_path);
        if !target_path.exists() {
            return Err(TestError::new(format!(
                "File '{}' does not exist at '{}'",
                self.file_path,
                target_path.display()
            )));
        }

        let content = stdfs::read_to_string(&target_path)
            .map_err(|e| TestError::new(format!("Failed to read '{}': {e}", self.file_path)))?;

        let task_suffix = if task_index > 0 {
            format!("_{task_index}")
        } else {
            String::new()
        };

        // Use the file extension as the golden file extension (e.g. .yml, .json)
        let ext = Path::new(&self.file_path)
            .extension()
            .and_then(OsStr::to_str)
            .unwrap_or("txt");
        let golden_name = format!("{}{}.{}", self.name, task_suffix, ext);

        stdfs::create_dir_all(&test_env.golden_dir)
            .map_err(|e| TestError::new(format!("Failed to create golden dir: {e}")))?;

        let golden_path = test_env.golden_dir.join(&golden_name);

        if is_update_golden_files_mode() {
            stdfs::write(&golden_path, &content)
                .map_err(|e| TestError::new(format!("Failed to write golden file: {e}")))?;
            return Ok(());
        }

        if let Some(patch) = diff_goldie(ext, content, false, &golden_path, |g| g) {
            return Err(TestError::GoldieMismatch(vec![patch]));
        }

        Ok(())
    }

    fn is_counted(&self) -> bool {
        false
    }
}
