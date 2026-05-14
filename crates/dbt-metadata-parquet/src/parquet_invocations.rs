//! Append-only parquet for invocation records.
//!
//! Files land at:
//! ```text
//! target/
//!   metadata/invocations/v1_{N}.parquet   ← append-only, one row per command
//! ```
//!
//! ## Design
//! * **Append-only** — each invocation appends a new file with exactly one row.
//!   No deduplication, no latest-wins. Every command execution is a historical
//!   record.
//! * **File consolidation** — when file count exceeds [`CONSOLIDATE_THRESHOLD`],
//!   all files are merged into a single file preserving all rows.  No rows are
//!   ever deleted (unlike epoch tables which prune dead nodes).
//! * **Schema versioning** — filename prefix `v1_` allows future schema changes
//!   without migration.
//! * **Write timing** — the row is written at end-of-command with final status
//!   and elapsed_time. A crash mid-run means no invocation row is written.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow::datatypes::{DataType, Field, Schema};
use dbt_common::{ErrorCode, FsError, FsResult, stdfs};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use serde::{Deserialize, Serialize};
use serde_arrow::to_record_batch;

// ── constants ─────────────────────────────────────────────────────────────────

const CONSOLIDATE_THRESHOLD: usize = 32;
const SCHEMA_VERSION: u32 = 1;

// ── row schema ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationRow {
    pub invocation_id: String,
    pub command: String,
    pub status: String,
    pub selector: Option<String>,
    pub cli_args: Option<String>,
    pub project_name: Option<String>,
    pub adapter_type: Option<String>,
    pub target_name: Option<String>,
    pub profile_name: Option<String>,
    pub environment_id: Option<String>,
    pub environment_name: Option<String>,
    pub account_identifier: Option<String>,
    pub defer_env_id: Option<String>,
    pub user_id: Option<String>,
    pub user_name: Option<String>,
    pub dbt_version: String,
    pub git_sha: Option<String>,
    pub git_branch: Option<String>,
    pub git_is_dirty: Option<i32>,
    pub elapsed_secs: Option<f64>,
    pub node_count: Option<i32>,
    pub ingested_at: i64,
}

fn invocation_fields() -> Vec<Field> {
    vec![
        Field::new("invocation_id", DataType::Utf8, false),
        Field::new("command", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("selector", DataType::Utf8, true),
        Field::new("cli_args", DataType::Utf8, true),
        Field::new("project_name", DataType::Utf8, true),
        Field::new("adapter_type", DataType::Utf8, true),
        Field::new("target_name", DataType::Utf8, true),
        Field::new("profile_name", DataType::Utf8, true),
        Field::new("environment_id", DataType::Utf8, true),
        Field::new("environment_name", DataType::Utf8, true),
        Field::new("account_identifier", DataType::Utf8, true),
        Field::new("defer_env_id", DataType::Utf8, true),
        Field::new("user_id", DataType::Utf8, true),
        Field::new("user_name", DataType::Utf8, true),
        Field::new("dbt_version", DataType::Utf8, false),
        Field::new("git_sha", DataType::Utf8, true),
        Field::new("git_branch", DataType::Utf8, true),
        Field::new("git_is_dirty", DataType::Int32, true),
        Field::new("elapsed_secs", DataType::Float64, true),
        Field::new("node_count", DataType::Int32, true),
        Field::new("ingested_at", DataType::Int64, false),
    ]
}

// ── epoch helpers ─────────────────────────────────────────────────────────────

fn version_prefix() -> String {
    format!("v{}_", SCHEMA_VERSION)
}

fn existing_files(dir: &Path) -> Vec<(u32, PathBuf)> {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let prefix = version_prefix();
    let mut files: Vec<(u32, PathBuf)> = rd
        .flatten()
        .filter_map(|e| {
            let p = e.path();
            let stem = p.file_stem()?.to_str()?;
            if p.extension()?.to_str()? != "parquet" {
                return None;
            }
            let rest = stem.strip_prefix(&prefix)?;
            let n: u32 = rest.parse().ok()?;
            Some((n, p))
        })
        .collect();
    files.sort_by_key(|(n, _)| *n);
    files
}

fn next_file_number(dir: &Path) -> u32 {
    existing_files(dir).last().map(|(n, _)| n + 1).unwrap_or(0)
}

// ── write ─────────────────────────────────────────────────────────────────────

fn write_rows(path: &Path, rows: &[InvocationRow]) -> FsResult<()> {
    if let Some(parent) = path.parent() {
        stdfs::create_dir_all(parent)?;
    }
    let file = stdfs::File::create(path)?;
    let fields = invocation_fields();
    let arrow_schema = Arc::new(Schema::new(fields));
    let field_refs: Vec<_> = arrow_schema.fields().iter().map(Arc::clone).collect();
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(1).unwrap()))
        .build();
    let mut writer = ArrowWriter::try_new(file, arrow_schema, Some(props))
        .map_err(|e| FsError::new(ErrorCode::IoError, format!("Invocation ArrowWriter: {e}")))?;
    let row_refs: Vec<&InvocationRow> = rows.iter().collect();
    let batch = to_record_batch(&field_refs, &row_refs)
        .map_err(|e| FsError::new(ErrorCode::IoError, format!("Invocation serde_arrow: {e}")))?;
    writer
        .write(&batch)
        .map_err(|e| FsError::new(ErrorCode::IoError, format!("Invocation write batch: {e}")))?;
    writer
        .close()
        .map_err(|e| FsError::new(ErrorCode::IoError, format!("Invocation close: {e}")))?;
    Ok(())
}

fn read_rows(path: &Path) -> Vec<InvocationRow> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let Ok(file) = std::fs::File::open(path) else {
        return Vec::new();
    };
    let Ok(builder) = ParquetRecordBatchReaderBuilder::try_new(file) else {
        return Vec::new();
    };
    let Ok(reader) = builder.build() else {
        return Vec::new();
    };
    let mut rows = Vec::new();
    for batch in reader.flatten() {
        if let Ok(mut chunk) = serde_arrow::from_record_batch::<Vec<InvocationRow>>(&batch) {
            rows.append(&mut chunk);
        }
    }
    rows
}

// ── public API ────────────────────────────────────────────────────────────────

/// Writes a single invocation row to the invocations directory.
///
/// Called at end-of-command with final status and elapsed time.
pub fn write_invocation(dir: &Path, row: InvocationRow) -> FsResult<()> {
    let n = next_file_number(dir);
    let filename = format!("{}{}.parquet", version_prefix(), n);
    let path = dir.join(&filename);
    write_rows(&path, &[row])?;

    // Consolidate if too many files
    let files = existing_files(dir);
    if files.len() > CONSOLIDATE_THRESHOLD {
        consolidate(dir, &files)?;
    }
    Ok(())
}

/// Reads all invocation rows from the directory, ordered by ingested_at.
pub fn read_invocations(dir: &Path) -> Vec<InvocationRow> {
    let files = existing_files(dir);
    let mut all_rows = Vec::new();
    for (_, path) in &files {
        all_rows.extend(read_rows(path));
    }
    all_rows.sort_by_key(|r| r.ingested_at);
    all_rows
}

// ── consolidation ─────────────────────────────────────────────────────────────

fn consolidate(dir: &Path, files: &[(u32, PathBuf)]) -> FsResult<()> {
    let mut all_rows = Vec::new();
    for (_, path) in files {
        all_rows.extend(read_rows(path));
    }
    all_rows.sort_by_key(|r| r.ingested_at);

    let consolidated_path = dir.join(format!("{}0.parquet", version_prefix()));
    let tmp_path = dir.join(format!("{}.tmp.parquet", version_prefix()));
    write_rows(&tmp_path, &all_rows)?;

    // Atomic replace: rename tmp → target, then delete old files
    stdfs::rename(&tmp_path, &consolidated_path)?;
    for (n, path) in files {
        if *n != 0 {
            let _ = std::fs::remove_file(path);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_and_read_invocation() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        let row = InvocationRow {
            invocation_id: "inv-001".to_string(),
            command: "compile".to_string(),
            status: "success".to_string(),
            selector: Some("model.my_project.my_model".to_string()),
            cli_args: Some(r#"["compile","--select","my_model"]"#.to_string()),
            project_name: Some("my_project".to_string()),
            adapter_type: Some("snowflake".to_string()),
            target_name: Some("dev".to_string()),
            profile_name: Some("default".to_string()),
            environment_id: Some("env-123".to_string()),
            environment_name: Some("Production".to_string()),
            account_identifier: Some("acct-789".to_string()),
            defer_env_id: Some("defer-env-001".to_string()),
            user_id: Some("user-456".to_string()),
            user_name: Some("wolfram".to_string()),
            dbt_version: "2.0.0".to_string(),
            git_sha: Some("abc123".to_string()),
            git_branch: Some("main".to_string()),
            git_is_dirty: Some(0),
            elapsed_secs: Some(12.5),
            node_count: Some(42),
            ingested_at: 1_700_000_000_000_000_000,
        };

        write_invocation(dir_path, row).unwrap();

        let rows = read_invocations(dir_path);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].invocation_id, "inv-001");
        assert_eq!(rows[0].command, "compile");
        assert_eq!(rows[0].status, "success");
        assert_eq!(rows[0].elapsed_secs, Some(12.5));
        assert_eq!(rows[0].node_count, Some(42));
    }

    #[test]
    fn test_multiple_invocations_append() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        for i in 0..3 {
            let row = InvocationRow {
                invocation_id: format!("inv-{i:03}"),
                command: "run".to_string(),
                status: "success".to_string(),
                selector: None,
                cli_args: None,
                project_name: Some("proj".to_string()),
                adapter_type: Some("duckdb".to_string()),
                target_name: Some("dev".to_string()),
                profile_name: None,
                environment_id: None,
                environment_name: None,
                account_identifier: None,
                defer_env_id: None,
                user_id: None,
                user_name: None,
                dbt_version: "2.0.0".to_string(),
                git_sha: None,
                git_branch: None,
                git_is_dirty: None,
                elapsed_secs: Some(i as f64),
                node_count: None,
                ingested_at: 1_700_000_000_000_000_000 + i as i64,
            };
            write_invocation(dir_path, row).unwrap();
        }

        let rows = read_invocations(dir_path);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].invocation_id, "inv-000");
        assert_eq!(rows[2].invocation_id, "inv-002");
        // Each in its own file
        assert_eq!(existing_files(dir_path).len(), 3);
    }

    #[test]
    fn test_consolidation() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        // Write more than CONSOLIDATE_THRESHOLD files
        for i in 0..CONSOLIDATE_THRESHOLD + 2 {
            let row = InvocationRow {
                invocation_id: format!("inv-{i:03}"),
                command: "compile".to_string(),
                status: "success".to_string(),
                selector: None,
                cli_args: None,
                project_name: None,
                adapter_type: None,
                target_name: None,
                profile_name: None,
                environment_id: None,
                environment_name: None,
                account_identifier: None,
                defer_env_id: None,
                user_id: None,
                user_name: None,
                dbt_version: "2.0.0".to_string(),
                git_sha: None,
                git_branch: None,
                git_is_dirty: None,
                elapsed_secs: None,
                node_count: None,
                ingested_at: i as i64,
            };
            write_invocation(dir_path, row).unwrap();
        }

        // Consolidation fires on write 33 (33 > 32), merges all into 1 file.
        // Write 34 adds a second file.
        let files = existing_files(dir_path);
        assert!(files.len() <= 2);

        let rows = read_invocations(dir_path);
        assert_eq!(rows.len(), CONSOLIDATE_THRESHOLD + 2);
    }
}
