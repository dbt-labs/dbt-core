//! Tasks for io.

use std::fs;
use std::sync::Arc;
use std::{
    io::Write,
    path::{Path, PathBuf},
};

use arrow::array::{
    Array, DictionaryArray, LargeStringArray, StringArray, StringViewArray, StructArray,
};
use arrow::datatypes::{
    DataType, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type,
    UInt64Type,
};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use dbt_common::{
    FsResult,
    stdfs::{self, File},
};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
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

// Helper: recursively rebuild string-like arrays (Utf8, LargeUtf8, Utf8View, and dictionary-encoded variants)
// inside any nesting of Struct (and dictionary) arrays. Non string-like arrays are returned as-is.
pub fn rebuild_string_like_arrays(
    array: &Arc<dyn Array>,
    replace_fn: &dyn Fn(&str) -> String,
) -> Arc<dyn Array> {
    match array.data_type() {
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
            let new_vals: Vec<Option<String>> = a.iter().map(|o| o.map(replace_fn)).collect();
            Arc::new(StringArray::from(new_vals))
        }
        DataType::LargeUtf8 => {
            let a = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            let new_vals: Vec<Option<String>> = a.iter().map(|o| o.map(replace_fn)).collect();
            Arc::new(LargeStringArray::from(new_vals))
        }
        DataType::Utf8View => {
            let a = array.as_any().downcast_ref::<StringViewArray>().unwrap();
            let new_vals: Vec<Option<String>> = a.iter().map(|o| o.map(replace_fn)).collect();
            Arc::new(StringArray::from(new_vals))
        }
        DataType::Struct(fields) => {
            let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
            let mut rebuilt_children = Vec::with_capacity(struct_array.num_columns());
            let mut needs_rebuild = false;
            for child in struct_array.columns() {
                let new_child = rebuild_string_like_arrays(child, replace_fn);
                if !Arc::ptr_eq(child, &new_child) {
                    needs_rebuild = true;
                }
                rebuilt_children.push(new_child);
            }
            if needs_rebuild {
                Arc::new(StructArray::new(
                    fields.clone(),
                    rebuilt_children,
                    struct_array.logical_nulls(),
                ))
            } else {
                array.clone()
            }
        }
        DataType::Dictionary(key_type, value_type) => {
            // Handle dictionary arrays whose values are string-like (Utf8, LargeUtf8, Utf8View)
            match value_type.as_ref() {
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                    // Rebuild underlying values if needed; preserve keys
                    macro_rules! rebuild_dict {
                        ($kt:ty) => {
                            if let Some(d) = array.as_any().downcast_ref::<DictionaryArray<$kt>>() {
                                let values = d.values();
                                let new_values = rebuild_string_like_arrays(values, replace_fn);
                                if !Arc::ptr_eq(values, &new_values) {
                                    let keys = d.keys().clone();
                                    return Arc::new(
                                        DictionaryArray::<$kt>::try_new(keys, new_values).unwrap(),
                                    );
                                } else {
                                    return array.clone();
                                }
                            }
                        };
                    }
                    match key_type.as_ref() {
                        DataType::Int8 => rebuild_dict!(Int8Type),
                        DataType::Int16 => rebuild_dict!(Int16Type),
                        DataType::Int32 => rebuild_dict!(Int32Type),
                        DataType::Int64 => rebuild_dict!(Int64Type),
                        DataType::UInt8 => rebuild_dict!(UInt8Type),
                        DataType::UInt16 => rebuild_dict!(UInt16Type),
                        DataType::UInt32 => rebuild_dict!(UInt32Type),
                        DataType::UInt64 => rebuild_dict!(UInt64Type),
                        _ => {}
                    }
                    // If we didn't early-return in macro (e.g., unexpected key type), fall back
                    array.clone()
                }
                _ => array.clone(),
            }
        }
        // non-string-like type columns, keep as is
        _ => array.clone(),
    }
}

/// Used to clean recorded files as they contain timestamps, etc.
// TODO: this can be generalized more to accept the list of extensions
// for files to clean (or split based on extension)
pub struct SedTask {
    pub from: String,
    pub to: String,
    pub dir: Option<PathBuf>,
    pub strip_comments: bool,
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
                        || ext == "jsonl"
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
                        let new_array = rebuild_string_like_arrays(array, &replace_fn);
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

            // Perform the same replacement for arrow IPC files
            // TODO: this only handles replacing schema name from column values of string-like type
            if path.extension().map(|ext| ext == "arrow").unwrap_or(false) {
                use arrow::ipc::reader::FileReader as ArrowFileReader;
                use arrow::ipc::writer::FileWriter as ArrowFileWriter;

                // setup the reader
                let file = File::open(path)?;
                let reader = ArrowFileReader::try_new(file, None)?;
                let schema = reader.schema();

                // setup the writer (use a temp file for later to be renamed)
                let temp_path = path.with_extension("arrow.tmp");
                let temp_file = File::create(&temp_path)?;
                let mut writer = ArrowFileWriter::try_new(temp_file, &schema)?;

                for batch in reader {
                    let batch = batch?;
                    let mut new_columns = Vec::with_capacity(batch.num_columns());

                    for i in 0..batch.num_columns() {
                        let array = batch.column(i);
                        let new_array = rebuild_string_like_arrays(array, &replace_fn);
                        new_columns.push(new_array);
                    }

                    // write back the updated content
                    let new_batch = RecordBatch::try_new(schema.clone(), new_columns)?;
                    writer.write(&new_batch)?;
                }

                // finalize and replace the original file
                writer.finish()?;
                drop(writer);
                fs::rename(temp_path, path)?;
            }
            Ok(())
        };

        iter_files_recursively(&test_env.golden_dir, &mut replace_timestamps).await?;
        if let Some(ref dir) = self.dir {
            iter_files_recursively(dir, &mut replace_timestamps).await?;
        }

        let mut replace_query_comments = move |path: &Path| -> TestResult<()> {
            if path.extension().map(|ext| ext == "sql").unwrap_or(false) {
                let content = fs::read_to_string(path)?;
                // A query comment only appears either at the beginning or the end of a query.
                let mut new_content = content;
                if new_content.starts_with("/*") {
                    if let Some(comment_end) = new_content.find("*/") {
                        new_content = new_content[(comment_end + "*/".len())..].to_string();
                    }
                } else if new_content.ends_with("*/")
                    && let Some(comment_start) = new_content.rfind("/*")
                {
                    new_content = new_content[..comment_start].to_string();
                };

                fs::write(path, new_content)?;
            }
            Ok(())
        };

        if self.strip_comments {
            iter_files_recursively(&test_env.golden_dir, &mut replace_query_comments).await?;
            if let Some(ref dir) = self.dir {
                iter_files_recursively(dir, &mut replace_query_comments).await?;
            }
        }

        Ok(())
    }
}
