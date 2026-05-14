//! Epoch-append parquet for compile-time column types.
//!
//! Files land at:
//! ```text
//! target/
//!   compiled_state/columns/v1_{N}.parquet   ← epoch-append, latest-wins by unique_id
//! ```
//!
//! ## Design
//! * **Schema versioning** — `v1_` prefix, same pattern as compile/nodes.
//! * **Delta writes** — only columns for recomputed nodes are written per run.
//! * **Latest-wins** — partition key is `unique_id`. All columns for a node share
//!   the same `ingested_at`; the set with the highest timestamp wins entirely.
//! * **Compaction** — when epoch count exceeds [`COMPACT_THRESHOLD`], epochs are
//!   merged into `v1_0.parquet`. Optional `valid_ids` prunes dead nodes.

use std::{
    collections::{HashMap, HashSet},
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

const COMPACT_THRESHOLD: usize = 8;
const SCHEMA_VERSION: u32 = 1;

// ── row schema ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompileColumnRow {
    pub unique_id: String,
    pub column_name: String,
    pub column_index: i32,
    pub column_type: Option<String>,
    pub description: Option<String>,
    pub ingested_at: i64,
}

fn column_fields() -> Vec<Field> {
    vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("column_name", DataType::Utf8, false),
        Field::new("column_index", DataType::Int32, false),
        Field::new("column_type", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("ingested_at", DataType::Int64, false),
    ]
}

// ── epoch helpers ─────────────────────────────────────────────────────────────

fn version_prefix() -> String {
    format!("v{}_", SCHEMA_VERSION)
}

fn existing_epochs(dir: &Path) -> Vec<(u32, PathBuf)> {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let prefix = version_prefix();
    let mut epochs: Vec<(u32, PathBuf)> = rd
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
    epochs.sort_by_key(|(n, _)| *n);
    epochs
}

fn next_epoch(dir: &Path) -> u32 {
    existing_epochs(dir).last().map(|(n, _)| n + 1).unwrap_or(0)
}

// ── write / read ──────────────────────────────────────────────────────────────

fn write_rows(path: &Path, rows: &[CompileColumnRow]) -> FsResult<()> {
    if let Some(parent) = path.parent() {
        stdfs::create_dir_all(parent)?;
    }
    let file = stdfs::File::create(path)?;
    let fields = column_fields();
    let arrow_schema = Arc::new(Schema::new(fields));
    let field_refs: Vec<_> = arrow_schema.fields().iter().map(Arc::clone).collect();
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(1).unwrap()))
        .build();
    let mut writer = ArrowWriter::try_new(file, arrow_schema, Some(props)).map_err(|e| {
        FsError::new(
            ErrorCode::IoError,
            format!("CompileColumn ArrowWriter: {e}"),
        )
    })?;
    let row_refs: Vec<&CompileColumnRow> = rows.iter().collect();
    let batch = to_record_batch(&field_refs, &row_refs).map_err(|e| {
        FsError::new(
            ErrorCode::IoError,
            format!("CompileColumn serde_arrow: {e}"),
        )
    })?;
    writer
        .write(&batch)
        .map_err(|e| FsError::new(ErrorCode::IoError, format!("CompileColumn write: {e}")))?;
    writer
        .close()
        .map_err(|e| FsError::new(ErrorCode::IoError, format!("CompileColumn close: {e}")))?;
    Ok(())
}

fn read_rows(path: &Path) -> Vec<CompileColumnRow> {
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
        if let Ok(mut chunk) = serde_arrow::from_record_batch::<Vec<CompileColumnRow>>(&batch) {
            rows.append(&mut chunk);
        }
    }
    rows
}

// ── compaction ────────────────────────────────────────────────────────────────

fn compact_epochs(dir: &Path, valid_ids: Option<&HashSet<String>>) -> FsResult<()> {
    let epochs = existing_epochs(dir);
    if epochs.is_empty() {
        return Ok(());
    }

    // Latest-wins per unique_id: collect all rows, keep latest set per node.
    let mut best: HashMap<String, (i64, Vec<CompileColumnRow>)> = HashMap::new();
    for (_, path) in &epochs {
        for row in read_rows(path) {
            let entry = best.entry(row.unique_id.clone()).or_insert((0, Vec::new()));
            if row.ingested_at > entry.0 {
                entry.0 = row.ingested_at;
                entry.1.clear();
                entry.1.push(row);
            } else if row.ingested_at == entry.0 {
                entry.1.push(row);
            }
        }
    }

    let mut merged: Vec<CompileColumnRow> = Vec::new();
    for (uid, (_, rows)) in best {
        if let Some(valid) = valid_ids {
            if !valid.contains(&uid) {
                continue;
            }
        }
        merged.extend(rows);
    }
    merged.sort_by(|a, b| {
        a.unique_id
            .cmp(&b.unique_id)
            .then(a.column_index.cmp(&b.column_index))
    });

    let consolidated = dir.join(format!("{}0.parquet", version_prefix()));
    let tmp = dir.join(format!("{}.tmp.parquet", version_prefix()));
    write_rows(&tmp, &merged)?;
    stdfs::rename(&tmp, &consolidated)?;

    for (n, path) in &epochs {
        if *n != 0 {
            let _ = std::fs::remove_file(path);
        }
    }
    Ok(())
}

// ── public API ────────────────────────────────────────────────────────────────

/// Write compile-time column rows for one invocation.
///
/// `recomputed_nodes`: if Some, only these nodes were recomputed (delta write).
/// If None, this is a full compile (epoch 0).
/// `valid_ids`: passed to compaction for dead-node pruning.
pub fn write_compile_columns(
    dir: &Path,
    rows: Vec<CompileColumnRow>,
    recomputed_nodes: Option<&HashSet<String>>,
    valid_ids: Option<&HashSet<String>>,
) -> FsResult<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let epoch = if recomputed_nodes.is_some() {
        next_epoch(dir)
    } else {
        0
    };
    let filename = format!("{}{}.parquet", version_prefix(), epoch);
    let path = dir.join(&filename);
    write_rows(&path, &rows)?;

    if recomputed_nodes.is_none() {
        for (n, p) in existing_epochs(dir) {
            if n != 0 {
                let _ = std::fs::remove_file(p);
            }
        }
    }

    let epochs = existing_epochs(dir);
    if epochs.len() > COMPACT_THRESHOLD {
        compact_epochs(dir, valid_ids)?;
    }
    Ok(())
}

/// Read all compile-column rows (latest-wins per unique_id).
pub fn read_compile_columns(dir: &Path) -> Vec<CompileColumnRow> {
    let epochs = existing_epochs(dir);
    let mut best: HashMap<String, (i64, Vec<CompileColumnRow>)> = HashMap::new();
    for (_, path) in &epochs {
        for row in read_rows(path) {
            let entry = best.entry(row.unique_id.clone()).or_insert((0, Vec::new()));
            if row.ingested_at > entry.0 {
                entry.0 = row.ingested_at;
                entry.1.clear();
                entry.1.push(row);
            } else if row.ingested_at == entry.0 {
                entry.1.push(row);
            }
        }
    }
    let mut result: Vec<CompileColumnRow> = best.into_values().flat_map(|(_, rows)| rows).collect();
    result.sort_by(|a, b| {
        a.unique_id
            .cmp(&b.unique_id)
            .then(a.column_index.cmp(&b.column_index))
    });
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_and_read_columns() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        let rows = vec![
            CompileColumnRow {
                unique_id: "model.pkg.users".to_string(),
                column_name: "id".to_string(),
                column_index: 0,
                column_type: Some("INTEGER".to_string()),
                description: Some("Primary key".to_string()),
                ingested_at: 100,
            },
            CompileColumnRow {
                unique_id: "model.pkg.users".to_string(),
                column_name: "email".to_string(),
                column_index: 1,
                column_type: Some("VARCHAR".to_string()),
                description: None,
                ingested_at: 100,
            },
        ];

        write_compile_columns(dir_path, rows, None, None).unwrap();

        let read_back = read_compile_columns(dir_path);
        assert_eq!(read_back.len(), 2);
        assert_eq!(read_back[0].column_name, "id");
        assert_eq!(read_back[0].column_type.as_deref(), Some("INTEGER"));
        assert_eq!(read_back[1].column_name, "email");
    }

    #[test]
    fn test_latest_wins_per_node() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        // First write: 2 columns for model_a
        let rows1 = vec![
            CompileColumnRow {
                unique_id: "model.pkg.a".to_string(),
                column_name: "x".to_string(),
                column_index: 0,
                column_type: Some("INT".to_string()),
                description: None,
                ingested_at: 1,
            },
            CompileColumnRow {
                unique_id: "model.pkg.a".to_string(),
                column_name: "y".to_string(),
                column_index: 1,
                column_type: Some("INT".to_string()),
                description: None,
                ingested_at: 1,
            },
        ];
        let mut targets = HashSet::new();
        targets.insert("model.pkg.a".to_string());
        write_compile_columns(dir_path, rows1, Some(&targets), None).unwrap();

        // Second write: model_a recompiled with 1 column (schema changed)
        let rows2 = vec![CompileColumnRow {
            unique_id: "model.pkg.a".to_string(),
            column_name: "x".to_string(),
            column_index: 0,
            column_type: Some("BIGINT".to_string()),
            description: Some("Updated".to_string()),
            ingested_at: 2,
        }];
        write_compile_columns(dir_path, rows2, Some(&targets), None).unwrap();

        let read_back = read_compile_columns(dir_path);
        assert_eq!(read_back.len(), 1);
        assert_eq!(read_back[0].column_type.as_deref(), Some("BIGINT"));
        assert_eq!(read_back[0].description.as_deref(), Some("Updated"));
    }

    #[test]
    fn test_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        let mut targets = HashSet::new();
        targets.insert("model.pkg.m".to_string());

        for i in 0..(COMPACT_THRESHOLD + 2) {
            let rows = vec![CompileColumnRow {
                unique_id: "model.pkg.m".to_string(),
                column_name: "col".to_string(),
                column_index: 0,
                column_type: Some(format!("TYPE_{i}")),
                description: None,
                ingested_at: i as i64,
            }];
            write_compile_columns(dir_path, rows, Some(&targets), None).unwrap();
        }

        let epochs = existing_epochs(dir_path);
        assert!(epochs.len() <= 2);

        let read_back = read_compile_columns(dir_path);
        assert_eq!(read_back.len(), 1);
        assert_eq!(
            read_back[0].column_type.as_deref(),
            Some(format!("TYPE_{}", COMPACT_THRESHOLD + 1).as_str())
        );
    }
}
