//! Tasks for io.

use std::collections::{BTreeMap, HashMap};
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
use arrow::ipc::reader::StreamReader as ArrowStreamReader;
use arrow::ipc::writer::StreamWriter as ArrowStreamWriter;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use dbt_common::constants::DBT_TARGET_DIR_NAME;
use dbt_common::{
    FsResult,
    stdfs::{self, File},
};
use regex::Regex;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::io::Cursor;

use crate::task::utils::iter_files_recursively;
use crate::task::{ProjectEnv, Task, TestEnv, TestResult};

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
        let src_path = test_env
            .temp_dir
            .join(DBT_TARGET_DIR_NAME)
            .join(&self.target_file);
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

/// A row in the SQLite recordings.db
type RecordingRow = (
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
);

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

/// Helper function to normalize SQL strings, error messages, and data in SQLite recordings database
fn update_sqlite_recordings(db_path: &Path, replace_fn: &dyn Fn(&str) -> String) -> TestResult<()> {
    let conn = Connection::open(db_path)?;

    // Get all recordings
    let mut stmt =
        conn.prepare("SELECT unique_id, record_type, sql, data_base64, error FROM recordings")?;
    let recordings: Vec<RecordingRow> = stmt
        .query_map([], |row| {
            let unique_id: String = row.get(0)?;
            let record_type: String = row.get(1)?;
            let sql: Option<String> = row.get(2)?;
            let data_base64: Option<String> = row.get(3)?;
            let error: Option<String> = row.get(4)?;
            Ok((unique_id, record_type, sql, data_base64, error))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    // Update SQL, error, and data fields
    for (unique_id, record_type, sql, data_base64, error) in recordings {
        let new_sql = sql.as_ref().map(|s| replace_fn(s));
        let new_error = error.as_ref().map(|e| replace_fn(e));

        // Process data_base64 if present
        let new_data_base64 = if let Some(ref data) = data_base64 {
            // Decode base64 -> Arrow IPC -> process string columns -> encode back
            match BASE64_STANDARD.decode(data) {
                Ok(bytes) => {
                    let cursor = Cursor::new(bytes);
                    match ArrowStreamReader::try_new(cursor, None) {
                        Ok(reader) => {
                            let schema = reader.schema();
                            let mut output = Vec::new();
                            let mut writer = ArrowStreamWriter::try_new(&mut output, &schema)?;

                            for batch_result in reader {
                                match batch_result {
                                    Ok(batch) => {
                                        let mut new_columns =
                                            Vec::with_capacity(batch.num_columns());
                                        for i in 0..batch.num_columns() {
                                            let array = batch.column(i);
                                            let new_array =
                                                rebuild_string_like_arrays(array, replace_fn);
                                            new_columns.push(new_array);
                                        }
                                        let new_batch =
                                            RecordBatch::try_new(schema.clone(), new_columns)?;
                                        writer.write(&new_batch)?;
                                    }
                                    Err(_) => {
                                        // If batch read fails, keep original data
                                        break;
                                    }
                                }
                            }

                            writer.finish()?;
                            drop(writer);
                            Some(BASE64_STANDARD.encode(&output))
                        }
                        Err(_) => data_base64.clone(), // Keep original if can't parse
                    }
                }
                Err(_) => data_base64.clone(), // Keep original if can't decode
            }
        } else {
            None
        };

        if new_sql != sql || new_error != error || new_data_base64 != data_base64 {
            conn.execute(
                "UPDATE recordings SET sql = ?1, data_base64 = ?2, error = ?3 WHERE unique_id = ?4 AND record_type = ?5",
                params![new_sql, new_data_base64, new_error, unique_id, record_type],
            )?;
        }
    }

    Ok(())
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
        static RECORDS_NAME: &str = "recordings.db";

        let replace_fn = |content: &str| {
            content
                .replace(&self.from.to_lowercase(), &self.to)
                .replace(&self.from.to_uppercase(), &self.to.to_uppercase())
        };
        let replace_timestamps = move |path: &Path| -> TestResult<()> {
            if path
                .extension()
                .map(|ext| ext == "stdout" || ext == "stderr")
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
            Ok(())
        };

        // Normalize stdout/stdout goldies
        iter_files_recursively(&test_env.golden_dir, &replace_timestamps).await?;
        if let Some(ref dir) = self.dir {
            iter_files_recursively(dir, &replace_timestamps).await?;
        }

        // Normalize SQLite recordings
        let process_sqlite_db = move |path: &Path| -> TestResult<()> {
            if path.file_name().and_then(|n| n.to_str()) == Some(RECORDS_NAME) {
                // Apply basic schema name replacement
                update_sqlite_recordings(path, &replace_fn)?;

                // Apply warehouse name replacement
                let warehouse_replace = |content: &str| -> String {
                    content
                        .replace("FUSION_ADAPTER_TESTING", "[MASKED_WH]")
                        .replace("FUSION_SLT_WAREHOUSE", "[MASKED_WH]")
                };
                update_sqlite_recordings(path, &warehouse_replace)?;

                // Apply Time Elapsed regex removal
                let re_time_elapsed = Regex::new(r"Time Elapsed:.*").unwrap();
                let time_elapsed_replace = |content: &str| -> String {
                    re_time_elapsed.replace_all(content, "").to_string()
                };
                update_sqlite_recordings(path, &time_elapsed_replace)?;

                // Apply query comment removal if enabled
                if self.strip_comments {
                    let comment_replace = |content: &str| -> String {
                        let mut new_content = content.to_string();
                        if new_content.starts_with("/*") {
                            if let Some(comment_end) = new_content.find("*/") {
                                new_content = new_content[(comment_end + "*/".len())..].to_string();
                            }
                        } else if new_content.ends_with("*/") {
                            if let Some(comment_start) = new_content.rfind("/*") {
                                new_content = new_content[..comment_start].to_string();
                            }
                        }
                        new_content
                    };
                    update_sqlite_recordings(path, &comment_replace)?;
                }
            }
            Ok(())
        };

        if let Some(ref dir) = self.dir {
            iter_files_recursively(dir, &process_sqlite_db).await?;
        }

        // Dump the SQLite recordings to a YAML
        if std::env::var("DBT_FS_RECORD_NO_YAML").unwrap_or_default() != "1" {
            if let Some(ref dir) = self.dir {
                let yaml_path = dir.join("recordings.yaml");
                let db_path = dir.join(RECORDS_NAME);
                dump_sqlite_recordings_to_yaml(&db_path, &yaml_path)?;
            }
        }

        Ok(())
    }
}

/// Helper function to dump recordings from SQLite database to YAML format
fn dump_sqlite_recordings_to_yaml(db_path: &Path, yaml_path: &Path) -> TestResult<()> {
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    enum OperationType {
        Execute {
            sql: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            error: Option<String>,
        },
        GetTableSchema {
            table_name: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            error: Option<String>,
        },
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct YamlOperation {
        sequence: usize,
        #[serde(flatten)]
        operation: OperationType,
    }

    let conn = Connection::open(db_path)?;

    // Query all recordings, ordered by unique_id for consistency
    let mut stmt = conn.prepare(
        "SELECT unique_id, record_type, sql, data_base64, error FROM recordings ORDER BY unique_id",
    )?;

    let recordings: Vec<RecordingRow> = stmt
        .query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    // Track sequence numbers per base unique_id
    let mut sequence_counters = HashMap::new();

    // Group operations by base unique_id (using BTreeMap for sorted output)
    let mut operations_by_model = BTreeMap::new();

    for (unique_id, record_type, sql, _, error) in recordings {
        // Extract base unique_id by removing sequence suffix (e.g., "model.hello_world.hello-0" -> "model.hello_world.hello")
        let base_unique_id = if let Some(pos) = unique_id.rfind('-') {
            // Check if what follows the last '-' is a digit (sequence number)
            debug_assert!(unique_id[(pos + 1)..].chars().all(|c| c.is_ascii_digit()));
            unique_id[..pos].to_string()
        } else {
            unique_id.clone()
        };

        let sequence = *sequence_counters.entry(base_unique_id.clone()).or_insert(0);
        sequence_counters.insert(base_unique_id.clone(), sequence + 1);

        let operation = match record_type.as_str() {
            "execute" => {
                let sql = sql.unwrap_or_default();

                YamlOperation {
                    sequence,
                    operation: OperationType::Execute { sql, error },
                }
            }
            "get_table_schema" => {
                // Extract table name from unique_id (format: get_table_schema.HASH-INDEX)
                let table_name = unique_id
                    .strip_prefix("get_table_schema.")
                    .and_then(|s| s.split('-').next())
                    .unwrap_or(&unique_id)
                    .to_string();

                YamlOperation {
                    sequence,
                    operation: OperationType::GetTableSchema { table_name, error },
                }
            }
            _ => continue, // Skip unknown types
        };

        // Add operation to the appropriate model group
        operations_by_model
            .entry(base_unique_id)
            .or_insert_with(Vec::new)
            .push(operation);
    }

    if operations_by_model.is_empty() {
        return Ok(());
    }

    // Serialize to YAML (as a map with model IDs as keys)
    let yaml_str = dbt_serde_yaml::to_string(&operations_by_model)
        .map_err(|e| format!("Failed to serialize YAML: {}", e))?;

    // Post-process to use block scalars for multiline SQL
    let yaml_str = format_multiline_sql(yaml_str);

    // Write to file
    stdfs::create_dir_all(yaml_path.parent().unwrap())?;
    stdfs::write(yaml_path, yaml_str)?;

    Ok(())
}

/// Convert inline multiline strings to YAML block scalar format (|-) for better readability
/// This function:
/// 1. Keeps single-line SQL inline (even with trailing comments)
/// 2. Converts multiline SQL to block scalar format (|-)
/// 3. Trims excessive leading/trailing empty lines and collapses multiple blank lines
fn format_multiline_sql(yaml: String) -> String {
    let mut result = String::with_capacity(yaml.len() * 2);
    let lines: Vec<&str> = yaml.lines().collect();
    let mut i = 0;

    while i < lines.len() {
        let line = lines[i];

        // Check for 'sql: ' followed by either inline string or block scalar
        if let Some(sql_start) = line.find("sql: ") {
            let indent = &line[..sql_start];
            let after_sql = &line[(sql_start + 5)..].trim_start();

            // Case 1: Inline string (might contain \n for multiline)
            if after_sql.starts_with('"') {
                let sql_content = after_sql
                    .trim_start_matches('"')
                    .trim_end_matches('"')
                    .replace("\\n", "\n")
                    .replace("\\\"", "\"")
                    .replace("\\\\", "\\");

                // Check if it's truly multiline or just has a comment
                let trimmed = sql_content.trim();
                let line_count = trimmed.lines().filter(|l| !l.trim().is_empty()).count();

                if line_count <= 1 {
                    // Single line (possibly with comment) - keep inline
                    result.push_str(line);
                    result.push('\n');
                } else {
                    // Multiline - use block scalar and normalize indentation
                    result.push_str(indent);
                    result.push_str("sql: |-\n");

                    let sql_lines: Vec<&str> = sql_content.lines().collect();
                    let normalized = normalize_blank_lines(&sql_lines);

                    for sql_line in normalized {
                        result.push_str(indent);
                        result.push_str("  ");
                        result.push_str(&sql_line);
                        result.push('\n');
                    }
                }

                i += 1;
                continue;
            }

            // Case 2: Block scalar (e.g., sql: |-, sql: |2-, sql: >, etc.)
            // Normalize all block scalars to |- and trim excessive whitespace
            if after_sql.starts_with('|') || after_sql.starts_with('>') {
                let mut sql_lines = Vec::new();
                i += 1; // Move past the 'sql: |...' line

                // Collect all SQL content lines
                while i < lines.len() {
                    let content_line = lines[i];
                    // Check if this line is still part of the SQL content
                    if content_line.is_empty()
                        || content_line.starts_with(&format!("{}  ", indent))
                        || content_line.trim().is_empty()
                    {
                        if let Some(stripped) = content_line.strip_prefix(&format!("{}  ", indent))
                        {
                            sql_lines.push(stripped);
                        } else if content_line.trim().is_empty() {
                            sql_lines.push("");
                        }
                        i += 1;
                    } else {
                        break;
                    }
                }

                let normalized = normalize_blank_lines(&sql_lines);

                // Write normalized block scalar
                result.push_str(indent);
                result.push_str("sql: |-\n");

                for sql_line in normalized {
                    result.push_str(indent);
                    result.push_str("  ");
                    result.push_str(&sql_line);
                    result.push('\n');
                }

                continue;
            }
        }

        // Regular line, keep as-is
        result.push_str(line);
        result.push('\n');
        i += 1;
    }

    result
}

/// Normalize SQL content indentation and blank lines:
/// - Convert tabs to spaces
/// - Strip ALL leading whitespace from all lines for consistent left-aligned output
/// - Remove leading/trailing blank lines  
/// - The SQL from the database often has inconsistent/meaningless leading whitespace
fn normalize_blank_lines(lines: &[&str]) -> Vec<String> {
    if lines.is_empty() {
        return Vec::new();
    }

    // Convert tabs to spaces and strip all leading whitespace for consistent formatting
    let mut result: Vec<String> = lines
        .iter()
        .map(|line| line.replace('\t', "    ").trim_start().to_string())
        .collect();

    // Trim leading blank lines
    while result.first().is_some_and(|l| l.is_empty()) {
        result.remove(0);
    }

    // Trim trailing blank lines
    while result.last().is_some_and(|l| l.is_empty()) {
        result.pop();
    }

    result
}
