//! Tasks for io.

use std::fs;
use std::sync::Arc;
use std::{
    io::Write,
    path::{Path, PathBuf},
};

use arrow::array::StringArray;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use dbt_common::{
    stdfs::{self, File},
    FsResult,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use regex::Regex;

use super::utils::iter_files_recursively;
use super::{ProjectEnv, Task, TestEnv, TestResult};

pub struct FileWriteTask {
    file_path: String,
    content: String,
}

impl FileWriteTask {
    pub fn new(file_path: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            file_path: file_path.into(),
            content: content.into(),
        }
    }
}

#[async_trait]
impl Task for FileWriteTask {
    async fn run(
        &self,
        project_env: &ProjectEnv,
        _test_env: &TestEnv,
        _task_index: usize,
    ) -> TestResult<()> {
        stdfs::write(
            project_env.absolute_project_dir.join(&self.file_path),
            &self.content,
        )?;
        Ok(())
    }
}

/// Task to touch a file.
pub struct TouchTask {
    path: String,
}

impl TouchTask {
    pub fn new(path: impl Into<String>) -> TouchTask {
        TouchTask { path: path.into() }
    }
}

#[async_trait]
impl Task for TouchTask {
    async fn run(
        &self,
        _project_env: &ProjectEnv,
        _test_env: &TestEnv,
        _task_index: usize,
    ) -> TestResult<()> {
        touch(PathBuf::from(&self.path))?;
        Ok(())
    }
}

// Touch is here simulate by read followed by write -- the basic touch
// is only available via its nightly
fn touch(file: PathBuf) -> FsResult<()> {
    let res = stdfs::read(&file).expect("read to succeed");
    stdfs::remove_file(&file)?;
    let mut file = File::create(&file)?;
    // TODO touch should be atomic
    file.write_all(&res).unwrap();
    // Flush the content to ensure it's written to disk
    file.flush().unwrap();
    Ok(())
}

/// Task to copy a file from the test target directory to the project directory.
/// This is specifically designed for copying artifacts like manifest.json from
/// the test environment's target directory to the project directory.
pub struct CpFromTargetTask {
    /// Filename in the target directory (e.g., "manifest.json")
    target_file: String,
    /// Destination path relative to project directory (e.g., "state/manifest.json")
    dest: String,
}

impl CpFromTargetTask {
    pub fn new(target_file: impl Into<String>, dest: impl Into<String>) -> CpFromTargetTask {
        CpFromTargetTask {
            target_file: target_file.into(),
            dest: dest.into(),
        }
    }
}

#[async_trait]
impl Task for CpFromTargetTask {
    async fn run(
        &self,
        project_env: &ProjectEnv,
        test_env: &TestEnv,
        _task_index: usize,
    ) -> TestResult<()> {
        let src_path = test_env.temp_dir.join("target").join(&self.target_file);
        let dest_path = project_env.absolute_project_dir.join(&self.dest);

        // Create parent directory for destination if it doesn't exist
        if let Some(parent) = dest_path.parent() {
            stdfs::create_dir_all(parent)?;
        }

        stdfs::copy(&src_path, &dest_path)?;
        Ok(())
    }
}

/// Task to remove a file.
pub struct RmTask {
    path: String,
}

impl RmTask {
    pub fn new(path: impl Into<String>) -> RmTask {
        RmTask { path: path.into() }
    }
}

#[async_trait]
impl Task for RmTask {
    async fn run(
        &self,
        _project_env: &ProjectEnv,
        _test_env: &TestEnv,
        _task_index: usize,
    ) -> TestResult<()> {
        stdfs::remove_file(&self.path).expect("could not remove a file");
        Ok(())
    }
}

/// Task to remove (and recreate) a directory. It does nothing if the
/// directory does not exist.
pub struct RmDirTask {
    path: PathBuf,
}

impl RmDirTask {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

#[async_trait]
impl Task for RmDirTask {
    async fn run(
        &self,
        _project_env: &ProjectEnv,
        _test_env: &TestEnv,
        _task_index: usize,
    ) -> TestResult<()> {
        if self.path.exists() {
            stdfs::remove_dir_all(&self.path)?;
        }
        stdfs::create_dir_all(&self.path)?;
        Ok(())
    }
}

/// Used to clean recorded files as they contain timestamps, etc.
// TODO: this can be generalized more to accept the list of extensions
// for files to clean (or split based on extension)
pub struct SedTask {
    pub from: String,
    pub to: String,
    pub dir: Option<PathBuf>,
}

#[async_trait]
impl Task for SedTask {
    async fn run(
        &self,
        _project_env: &ProjectEnv,
        test_env: &TestEnv,
        _task_index: usize,
    ) -> TestResult<()> {
        let replace_fn = |content: &str| {
            content
                .replace(&self.from.to_lowercase(), &self.to)
                .replace(&self.from.to_uppercase(), &self.to.to_uppercase())
        };
        let mut replace_timestamps = move |path: &Path| -> TestResult<()> {
            if path
                .extension()
                .map(|ext| {
                    ext == "sql"
                        || ext == "stdout"
                        || ext == "json"
                        || ext == "err"
                        || ext == "stderr"
                })
                .unwrap_or(false)
            {
                let content = fs::read_to_string(path)?;
                // We need to take into accoun it could be upper or
                // lowercase
                let new_content = replace_fn(&content);
                // snowsql output
                let re_time_elapsed = Regex::new(r"Time Elapsed:.*").unwrap();
                let new_content = re_time_elapsed.replace_all(&new_content, "").to_string();

                fs::write(path, new_content)?;
            }

            // Perform the same replacement for parquet files
            // TODO: this only handles replacing schema name from column values of string-like type
            if path
                .extension()
                .map(|ext| ext == "parquet")
                .unwrap_or(false)
            {
                // setup the reader
                let file = File::open(path)?;
                let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
                let schema = builder.schema().clone();
                let reader = builder.build()?;

                // setup the writer (use a temp file for later to be renamed)
                let temp_path = path.with_extension("parquet.tmp");
                let temp_file = File::create(&temp_path)?;
                let props = WriterProperties::builder().build();
                let mut writer = ArrowWriter::try_new(temp_file, schema.clone(), Some(props))?;

                for batch in reader {
                    let batch = batch?;
                    let mut new_columns = Vec::with_capacity(batch.num_columns());

                    for i in 0..batch.num_columns() {
                        let array = batch.column(i);
                        let new_array = if matches!(
                            array.data_type(),
                            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
                        ) {
                            let string_array =
                                array.as_any().downcast_ref::<StringArray>().unwrap();
                            let new_values: Vec<Option<String>> = string_array
                                .iter()
                                .map(|opt_str| opt_str.map(replace_fn))
                                .collect();
                            Arc::new(StringArray::from(new_values))
                        } else {
                            // non-string-like type columns, keep as is
                            array.clone()
                        };
                        new_columns.push(new_array);
                    }

                    // write back the updated content
                    let new_batch = RecordBatch::try_new(schema.clone(), new_columns)?;
                    writer.write(&new_batch)?;
                }

                // finalize and replace the original file
                writer.close()?;
                fs::rename(temp_path, path)?;
            }
            Ok(())
        };

        iter_files_recursively(&test_env.golden_dir, &mut replace_timestamps).await?;
        if let Some(ref dir) = self.dir {
            iter_files_recursively(dir, &mut replace_timestamps).await?;
        }
        Ok(())
    }
}
