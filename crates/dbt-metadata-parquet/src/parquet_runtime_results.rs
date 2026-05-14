//! Append-only parquet for run result records.
//!
//! Files land at:
//! ```text
//! target/
//!   metadata/runtime/results/v1_{N}.parquet   ← append-only, one row per node per invocation
//! ```
//!
//! ## Design
//! * **Append-only** — each invocation appends a new file with one row per executed node.
//! * **File consolidation** — when file count exceeds [`CONSOLIDATE_THRESHOLD`],
//!   all files are merged into a single file preserving all rows.
//! * **Schema versioning** — filename prefix `v1_` allows future schema changes.

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
pub struct RuntimeResultRow {
    pub invocation_id: String,
    pub unique_id: String,
    pub status: String,
    pub message: Option<String>,
    pub execution_time: Option<f64>,
    pub thread_id: Option<String>,
    pub failures: Option<i64>,
    pub compiled_code_hash: Option<String>,
    pub relation_name: Option<String>,
    pub adapter_response: Option<String>,
    pub timing: Option<String>,
    pub ingested_at: i64,
}

fn result_fields() -> Vec<Field> {
    vec![
        Field::new("invocation_id", DataType::Utf8, false),
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("message", DataType::Utf8, true),
        Field::new("execution_time", DataType::Float64, true),
        Field::new("thread_id", DataType::Utf8, true),
        Field::new("failures", DataType::Int64, true),
        Field::new("compiled_code_hash", DataType::Utf8, true),
        Field::new("relation_name", DataType::Utf8, true),
        Field::new("adapter_response", DataType::Utf8, true),
        Field::new("timing", DataType::Utf8, true),
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

fn write_rows(path: &Path, rows: &[RuntimeResultRow]) -> FsResult<()> {
    if let Some(parent) = path.parent() {
        stdfs::create_dir_all(parent)?;
    }
    let file = stdfs::File::create(path)?;
    let fields = result_fields();
    let arrow_schema = Arc::new(Schema::new(fields));
    let field_refs: Vec<_> = arrow_schema.fields().iter().map(Arc::clone).collect();
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(1).unwrap()))
        .build();
    let mut writer = ArrowWriter::try_new(file, arrow_schema, Some(props)).map_err(|e| {
        FsError::new(
            ErrorCode::IoError,
            format!("RuntimeResult ArrowWriter: {e}"),
        )
    })?;
    let row_refs: Vec<&RuntimeResultRow> = rows.iter().collect();
    let batch = to_record_batch(&field_refs, &row_refs).map_err(|e| {
        FsError::new(
            ErrorCode::IoError,
            format!("RuntimeResult serde_arrow: {e}"),
        )
    })?;
    writer
        .write(&batch)
        .map_err(|e| FsError::new(ErrorCode::IoError, format!("RuntimeResult write: {e}")))?;
    writer
        .close()
        .map_err(|e| FsError::new(ErrorCode::IoError, format!("RuntimeResult close: {e}")))?;
    Ok(())
}

fn read_rows(path: &Path) -> Vec<RuntimeResultRow> {
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
        if let Ok(mut chunk) = serde_arrow::from_record_batch::<Vec<RuntimeResultRow>>(&batch) {
            rows.append(&mut chunk);
        }
    }
    rows
}

// ── public API ────────────────────────────────────────────────────────────────

/// Writes runtime result rows for one invocation to the results directory.
pub fn write_runtime_results(dir: &Path, rows: &[RuntimeResultRow]) -> FsResult<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let n = next_file_number(dir);
    let filename = format!("{}{}.parquet", version_prefix(), n);
    let path = dir.join(&filename);
    write_rows(&path, rows)?;

    let files = existing_files(dir);
    if files.len() > CONSOLIDATE_THRESHOLD {
        consolidate(dir, &files)?;
    }
    Ok(())
}

/// Reads all runtime result rows from the directory, ordered by ingested_at.
pub fn read_runtime_results(dir: &Path) -> Vec<RuntimeResultRow> {
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
    fn test_write_and_read_results() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        let rows = vec![
            RuntimeResultRow {
                invocation_id: "inv-001".to_string(),
                unique_id: "model.my_project.model_a".to_string(),
                status: "success".to_string(),
                message: None,
                execution_time: Some(2.5),
                thread_id: Some("Thread-1".to_string()),
                failures: None,
                compiled_code_hash: Some("abc123".to_string()),
                relation_name: Some("db.schema.model_a".to_string()),
                adapter_response: Some(r#"{"rows_affected": 100}"#.to_string()),
                timing: Some(
                    r#"[{"name":"compile","started_at":"t1","completed_at":"t2"}]"#.to_string(),
                ),
                ingested_at: 1_700_000_000_000_000_000,
            },
            RuntimeResultRow {
                invocation_id: "inv-001".to_string(),
                unique_id: "model.my_project.model_b".to_string(),
                status: "error".to_string(),
                message: Some("Compilation Error: column 'foo' not found".to_string()),
                execution_time: Some(0.1),
                thread_id: Some("Thread-2".to_string()),
                failures: None,
                compiled_code_hash: None,
                relation_name: None,
                adapter_response: None,
                timing: None,
                ingested_at: 1_700_000_000_000_000_000,
            },
        ];

        write_runtime_results(dir_path, &rows).unwrap();

        let read_back = read_runtime_results(dir_path);
        assert_eq!(read_back.len(), 2);
        assert_eq!(read_back[0].unique_id, "model.my_project.model_a");
        assert_eq!(read_back[0].status, "success");
        assert_eq!(read_back[1].status, "error");
        assert_eq!(
            read_back[1].message.as_deref(),
            Some("Compilation Error: column 'foo' not found")
        );
    }

    #[test]
    fn test_multiple_invocations_append() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        for i in 0..3 {
            let rows = vec![RuntimeResultRow {
                invocation_id: format!("inv-{i:03}"),
                unique_id: "model.pkg.m".to_string(),
                status: "success".to_string(),
                message: None,
                execution_time: Some(i as f64),
                thread_id: None,
                failures: None,
                compiled_code_hash: None,
                relation_name: None,
                adapter_response: None,
                timing: None,
                ingested_at: 1_700_000_000_000_000_000 + i as i64,
            }];
            write_runtime_results(dir_path, &rows).unwrap();
        }

        let all = read_runtime_results(dir_path);
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].invocation_id, "inv-000");
        assert_eq!(all[2].invocation_id, "inv-002");
        assert_eq!(existing_files(dir_path).len(), 3);
    }

    #[test]
    fn test_consolidation() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        for i in 0..CONSOLIDATE_THRESHOLD + 2 {
            let rows = vec![RuntimeResultRow {
                invocation_id: format!("inv-{i:03}"),
                unique_id: "model.pkg.m".to_string(),
                status: "success".to_string(),
                message: None,
                execution_time: None,
                thread_id: None,
                failures: None,
                compiled_code_hash: None,
                relation_name: None,
                adapter_response: None,
                timing: None,
                ingested_at: i as i64,
            }];
            write_runtime_results(dir_path, &rows).unwrap();
        }

        let files = existing_files(dir_path);
        assert!(files.len() <= 2);

        let all = read_runtime_results(dir_path);
        assert_eq!(all.len(), CONSOLIDATE_THRESHOLD + 2);
    }
}
