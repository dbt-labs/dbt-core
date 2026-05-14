//! Append-only parquet for source freshness check records.
//!
//! Files land at:
//! ```text
//! target/
//!   metadata/runtime/freshness/v1_{N}.parquet   ← append-only, one row per source per check
//! ```

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
pub struct FreshnessResultRow {
    pub invocation_id: String,
    pub unique_id: String,
    pub status: String,
    pub max_loaded_at: Option<String>,
    pub snapshotted_at: Option<String>,
    pub max_loaded_at_time_ago: Option<f64>,
    pub execution_time: Option<f64>,
    pub warn_after_count: Option<i32>,
    pub warn_after_period: Option<String>,
    pub error_after_count: Option<i32>,
    pub error_after_period: Option<String>,
    pub ingested_at: i64,
}

fn freshness_fields() -> Vec<Field> {
    vec![
        Field::new("invocation_id", DataType::Utf8, false),
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("max_loaded_at", DataType::Utf8, true),
        Field::new("snapshotted_at", DataType::Utf8, true),
        Field::new("max_loaded_at_time_ago", DataType::Float64, true),
        Field::new("execution_time", DataType::Float64, true),
        Field::new("warn_after_count", DataType::Int32, true),
        Field::new("warn_after_period", DataType::Utf8, true),
        Field::new("error_after_count", DataType::Int32, true),
        Field::new("error_after_period", DataType::Utf8, true),
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

fn write_rows(path: &Path, rows: &[FreshnessResultRow]) -> FsResult<()> {
    if let Some(parent) = path.parent() {
        stdfs::create_dir_all(parent)?;
    }
    let file = stdfs::File::create(path)?;
    let fields = freshness_fields();
    let arrow_schema = Arc::new(Schema::new(fields));
    let field_refs: Vec<_> = arrow_schema.fields().iter().map(Arc::clone).collect();
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(1).unwrap()))
        .build();
    let mut writer = ArrowWriter::try_new(file, arrow_schema, Some(props))
        .map_err(|e| FsError::new(ErrorCode::IoError, format!("Freshness ArrowWriter: {e}")))?;
    let row_refs: Vec<&FreshnessResultRow> = rows.iter().collect();
    let batch = to_record_batch(&field_refs, &row_refs)
        .map_err(|e| FsError::new(ErrorCode::IoError, format!("Freshness serde_arrow: {e}")))?;
    writer
        .write(&batch)
        .map_err(|e| FsError::new(ErrorCode::IoError, format!("Freshness write: {e}")))?;
    writer
        .close()
        .map_err(|e| FsError::new(ErrorCode::IoError, format!("Freshness close: {e}")))?;
    Ok(())
}

fn read_rows(path: &Path) -> Vec<FreshnessResultRow> {
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
        if let Ok(mut chunk) = serde_arrow::from_record_batch::<Vec<FreshnessResultRow>>(&batch) {
            rows.append(&mut chunk);
        }
    }
    rows
}

// ── public API ────────────────────────────────────────────────────────────────

/// Writes freshness result rows for one invocation.
pub fn write_freshness_results(dir: &Path, rows: &[FreshnessResultRow]) -> FsResult<()> {
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

/// Reads all freshness result rows from the directory, ordered by ingested_at.
pub fn read_freshness_results(dir: &Path) -> Vec<FreshnessResultRow> {
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
    fn test_write_and_read_freshness() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        let rows = vec![FreshnessResultRow {
            invocation_id: "inv-001".to_string(),
            unique_id: "source.my_project.raw.orders".to_string(),
            status: "pass".to_string(),
            max_loaded_at: Some("2026-05-13T10:00:00Z".to_string()),
            snapshotted_at: Some("2026-05-13T12:00:00Z".to_string()),
            max_loaded_at_time_ago: Some(7200.0),
            execution_time: Some(0.5),
            warn_after_count: Some(12),
            warn_after_period: Some("hour".to_string()),
            error_after_count: Some(24),
            error_after_period: Some("hour".to_string()),
            ingested_at: 1_700_000_000_000_000_000,
        }];

        write_freshness_results(dir_path, &rows).unwrap();

        let read_back = read_freshness_results(dir_path);
        assert_eq!(read_back.len(), 1);
        assert_eq!(read_back[0].unique_id, "source.my_project.raw.orders");
        assert_eq!(read_back[0].status, "pass");
        assert_eq!(read_back[0].max_loaded_at_time_ago, Some(7200.0));
    }
}
