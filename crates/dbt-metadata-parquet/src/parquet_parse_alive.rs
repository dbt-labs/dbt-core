//! Snapshot parquet for parse/alive — the authority on node liveness.
//!
//! Files land at:
//! ```text
//! target/
//!   parse_state/alive.parquet   ← snapshot, rewritten every parse
//! ```
//!
//! ## Design
//! * **Snapshot** — full overwrite every parse. No epoch logic needed.
//! * **Authority** — a node is alive iff it appears in this file.
//!   All other tables may contain stale rows for deleted nodes;
//!   compaction uses this table as the filter.

use std::{path::Path, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema};
use dbt_common::{ErrorCode, FsError, FsResult, stdfs};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use serde::{Deserialize, Serialize};
use serde_arrow::to_record_batch;

// ── row schema ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AliveRow {
    pub unique_id: String,
    pub resource_type: String,
    pub ingested_at: i64,
}

fn alive_fields() -> Vec<Field> {
    vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("resource_type", DataType::Utf8, false),
        Field::new("ingested_at", DataType::Int64, false),
    ]
}

// ── write ─────────────────────────────────────────────────────────────────────

pub fn write_alive(path: &Path, rows: &[AliveRow]) -> FsResult<()> {
    if let Some(parent) = path.parent() {
        stdfs::create_dir_all(parent)?;
    }
    let file = stdfs::File::create(path)?;
    let fields = alive_fields();
    let arrow_schema = Arc::new(Schema::new(fields));
    let field_refs: Vec<_> = arrow_schema.fields().iter().map(Arc::clone).collect();
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(1).unwrap()))
        .build();
    let mut writer = ArrowWriter::try_new(file, arrow_schema, Some(props))
        .map_err(|e| FsError::new(ErrorCode::IoError, format!("Alive ArrowWriter: {e}")))?;
    let row_refs: Vec<&AliveRow> = rows.iter().collect();
    let batch = to_record_batch(&field_refs, &row_refs)
        .map_err(|e| FsError::new(ErrorCode::IoError, format!("Alive serde_arrow: {e}")))?;
    writer
        .write(&batch)
        .map_err(|e| FsError::new(ErrorCode::IoError, format!("Alive write: {e}")))?;
    writer
        .close()
        .map_err(|e| FsError::new(ErrorCode::IoError, format!("Alive close: {e}")))?;
    Ok(())
}

// ── read ──────────────────────────────────────────────────────────────────────

pub fn read_alive(path: &Path) -> Vec<AliveRow> {
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
        if let Ok(mut chunk) = serde_arrow::from_record_batch::<Vec<AliveRow>>(&batch) {
            rows.append(&mut chunk);
        }
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_and_read_alive() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("alive.parquet");

        let rows = vec![
            AliveRow {
                unique_id: "model.my_project.model_a".to_string(),
                resource_type: "model".to_string(),
                ingested_at: 1_700_000_000_000_000_000,
            },
            AliveRow {
                unique_id: "source.my_project.raw.users".to_string(),
                resource_type: "source".to_string(),
                ingested_at: 1_700_000_000_000_000_000,
            },
            AliveRow {
                unique_id: "test.my_project.not_null_a".to_string(),
                resource_type: "test".to_string(),
                ingested_at: 1_700_000_000_000_000_000,
            },
        ];

        write_alive(&path, &rows).unwrap();

        let read_back = read_alive(&path);
        assert_eq!(read_back.len(), 3);
        assert_eq!(read_back[0].unique_id, "model.my_project.model_a");
        assert_eq!(read_back[0].resource_type, "model");
        assert_eq!(read_back[1].resource_type, "source");
        assert_eq!(read_back[2].resource_type, "test");
    }

    #[test]
    fn test_overwrite_alive() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("alive.parquet");

        let rows1 = vec![
            AliveRow {
                unique_id: "model.pkg.a".to_string(),
                resource_type: "model".to_string(),
                ingested_at: 1,
            },
            AliveRow {
                unique_id: "model.pkg.b".to_string(),
                resource_type: "model".to_string(),
                ingested_at: 1,
            },
        ];
        write_alive(&path, &rows1).unwrap();

        let rows2 = vec![AliveRow {
            unique_id: "model.pkg.a".to_string(),
            resource_type: "model".to_string(),
            ingested_at: 2,
        }];
        write_alive(&path, &rows2).unwrap();

        let read_back = read_alive(&path);
        assert_eq!(read_back.len(), 1);
        assert_eq!(read_back[0].unique_id, "model.pkg.a");
        assert_eq!(read_back[0].ingested_at, 2);
    }
}
