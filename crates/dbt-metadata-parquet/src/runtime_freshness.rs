//! Append-only parquet for source freshness check records.
//!
//! Files land at:
//! ```text
//! target/
//!   metadata/runtime/freshness/v1_{N}.parquet   ← append-only, one row per source per check
//! ```

use std::path::{Path, PathBuf};

use arrow::datatypes::{DataType, Field};
use dbt_common::{FsResult, stdfs};
use serde::{Deserialize, Serialize};

use crate::epoch_io;

// ── constants ─────────────────────────────────────────────────────────────────

const CONSOLIDATE_THRESHOLD: usize = 32;
const VERSION_PREFIX: &str = "v1_";

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

fn existing_files(dir: &Path) -> Vec<(u32, PathBuf)> {
    epoch_io::existing_epochs(dir, VERSION_PREFIX)
}

// ── public API ────────────────────────────────────────────────────────────────

/// Writes freshness result rows for one invocation.
pub fn write_freshness_results(dir: &Path, rows: &[FreshnessResultRow]) -> FsResult<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let n = epoch_io::next_epoch(dir, VERSION_PREFIX);
    let path = dir.join(format!("{VERSION_PREFIX}{n}.parquet"));
    epoch_io::write_rows(&path, &freshness_fields(), rows)?;

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
        all_rows.extend(epoch_io::read_rows::<FreshnessResultRow>(path));
    }
    all_rows.sort_by_key(|r| r.ingested_at);
    all_rows
}

// ── consolidation ─────────────────────────────────────────────────────────────

fn consolidate(dir: &Path, files: &[(u32, PathBuf)]) -> FsResult<()> {
    let mut all_rows = Vec::new();
    for (_, path) in files {
        all_rows.extend(epoch_io::read_rows::<FreshnessResultRow>(path));
    }
    all_rows.sort_by_key(|r| r.ingested_at);

    let consolidated_path = dir.join(format!("{VERSION_PREFIX}0.parquet"));
    let tmp_path = dir.join(format!("{VERSION_PREFIX}.tmp.parquet"));
    epoch_io::write_rows(&tmp_path, &freshness_fields(), &all_rows)?;

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
