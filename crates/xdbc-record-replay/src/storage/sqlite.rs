use arrow::array::{RecordBatch, RecordBatchReader};
use arrow::ipc::reader::StreamReader as ArrowStreamReader;
use arrow::ipc::writer::StreamWriter as ArrowStreamWriter;
use arrow_schema::Schema;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use rusqlite::params;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::{RECORDS_NAME, fix_decimal_precision_in_schema};

pub(crate) struct RecordingEntry {
    pub sql: Option<String>,
    pub data_base64: Option<String>,
    pub error: Option<String>,
}

pub(crate) struct SqliteHandler {
    db_path: PathBuf,
}

impl SqliteHandler {
    pub fn new(recordings_dir: &Path) -> Self {
        Self {
            db_path: recordings_dir.join(RECORDS_NAME),
        }
    }

    #[cfg(test)]
    pub fn exists(&self) -> bool {
        self.db_path.exists()
    }

    pub fn connect(&self) -> Result<rusqlite::Connection, crate::error::RecordReplayError> {
        let conn = rusqlite::Connection::open(&self.db_path)?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS recordings (
                unique_id TEXT NOT NULL,
                record_type TEXT NOT NULL,
                sql TEXT,
                data_base64 TEXT,
                error TEXT,
                PRIMARY KEY (unique_id, record_type)
            )",
            [],
        )?;
        Ok(conn)
    }

    pub fn encode_arrow_ipc(
        &self,
        batches: &[RecordBatch],
        schema: Arc<Schema>,
    ) -> Result<String, crate::error::RecordReplayError> {
        let mut buffer = Vec::new();
        {
            let mut writer = ArrowStreamWriter::try_new(&mut buffer, &schema)?;
            for batch in batches {
                writer.write(batch)?;
            }
            writer.finish()?;
        }
        Ok(BASE64_STANDARD.encode(&buffer))
    }

    pub fn decode_arrow_ipc(
        &self,
        data_base64: &str,
    ) -> Result<Box<dyn RecordBatchReader + Send>, crate::error::RecordReplayError> {
        let bytes = BASE64_STANDARD.decode(data_base64)?;
        let cursor = Cursor::new(bytes);
        let reader = ArrowStreamReader::try_new(cursor, None)?;
        Ok(Box::new(reader))
    }

    pub fn write_execute(
        &self,
        unique_id: &str,
        sql: &str,
        batches: &[RecordBatch],
        schema: Arc<Schema>,
    ) -> Result<(), crate::error::RecordReplayError> {
        let conn = self.connect()?;
        let data_base64 = self.encode_arrow_ipc(batches, schema)?;
        conn.execute(
            "INSERT OR REPLACE INTO recordings (unique_id, record_type, sql, data_base64, error)
             VALUES (?1, 'execute', ?2, ?3, NULL)",
            params![unique_id, sql, data_base64],
        )?;
        Ok(())
    }

    pub fn write_execute_error(
        &self,
        unique_id: &str,
        sql: &str,
        error_msg: &str,
    ) -> Result<(), crate::error::RecordReplayError> {
        let conn = self.connect()?;
        conn.execute(
            "INSERT OR REPLACE INTO recordings (unique_id, record_type, sql, data_base64, error)
             VALUES (?1, 'execute', ?2, NULL, ?3)",
            params![unique_id, sql, error_msg],
        )?;
        Ok(())
    }

    pub fn write_schema(
        &self,
        unique_id: &str,
        schema: &Schema,
    ) -> Result<(), crate::error::RecordReplayError> {
        let fixed_schema = fix_decimal_precision_in_schema(schema);
        let schema_ref = Arc::new(fixed_schema);
        let empty_batch = RecordBatch::new_empty(schema_ref.clone());
        let data_base64 = self.encode_arrow_ipc(&[empty_batch], schema_ref)?;

        let conn = self.connect()?;
        conn.execute(
            "INSERT OR REPLACE INTO recordings (unique_id, record_type, sql, data_base64, error)
             VALUES (?1, 'get_table_schema', NULL, ?2, NULL)",
            params![unique_id, data_base64],
        )?;
        Ok(())
    }

    pub fn write_schema_error(
        &self,
        unique_id: &str,
        error_msg: &str,
    ) -> Result<(), crate::error::RecordReplayError> {
        let conn = self.connect()?;
        conn.execute(
            "INSERT OR REPLACE INTO recordings (unique_id, record_type, sql, data_base64, error)
             VALUES (?1, 'get_table_schema', NULL, NULL, ?2)",
            params![unique_id, error_msg],
        )?;
        Ok(())
    }

    pub fn read_execute(
        &self,
        unique_id: &str,
        replay_sql: &str,
    ) -> Result<RecordingEntry, crate::error::RecordReplayError> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            "SELECT sql, data_base64, error FROM recordings
             WHERE unique_id = ?1 AND record_type = 'execute'",
        )?;
        let entry = stmt
            .query_row(params![unique_id], |row| {
                Ok(RecordingEntry {
                    sql: row.get(0)?,
                    data_base64: row.get(1)?,
                    error: row.get(2)?,
                })
            })
            .map_err(|e| {
                crate::error::RecordReplayError(format!(
                    "Failed to read execute replay sql ({replay_sql}) for unique_id '{unique_id}': {e}"
                ))
            })?;
        Ok(entry)
    }

    pub fn read_schema(
        &self,
        unique_id: &str,
    ) -> Result<RecordingEntry, crate::error::RecordReplayError> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            "SELECT sql, data_base64, error FROM recordings
             WHERE unique_id = ?1 AND record_type = 'get_table_schema'",
        )?;
        let entry = stmt.query_row(params![unique_id], |row| {
            Ok(RecordingEntry {
                sql: row.get(0)?,
                data_base64: row.get(1)?,
                error: row.get(2)?,
            })
        })?;
        Ok(entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field};
    use std::collections::HashMap;

    #[test]
    fn execute_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let handler = SqliteHandler::new(dir.path());

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])),
            ],
        )
        .unwrap();

        let unique_id = "test-node-0";
        let sql = "SELECT * FROM users";
        handler
            .write_execute(unique_id, sql, &[batch], schema)
            .unwrap();

        let entry = handler.read_execute(unique_id, sql).unwrap();
        assert_eq!(entry.sql.as_deref(), Some(sql));
        assert!(entry.error.is_none());
        assert!(entry.data_base64.is_some());

        let mut reader = handler
            .decode_arrow_ipc(entry.data_base64.as_ref().unwrap())
            .unwrap();
        let read_batch = reader.next().unwrap().unwrap();
        assert_eq!(read_batch.num_rows(), 3);
        assert_eq!(read_batch.num_columns(), 2);
    }

    #[test]
    fn execute_error_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let handler = SqliteHandler::new(dir.path());

        let unique_id = "test-error-0";
        let sql = "SELECT * FROM nonexistent";
        let error_msg = "Table not found: nonexistent";

        handler
            .write_execute_error(unique_id, sql, error_msg)
            .unwrap();

        let entry = handler.read_execute(unique_id, sql).unwrap();
        assert_eq!(entry.sql.as_deref(), Some(sql));
        assert_eq!(entry.error.as_deref(), Some(error_msg));
        assert!(entry.data_base64.is_none());
    }

    #[test]
    fn schema_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let handler = SqliteHandler::new(dir.path());

        let mut metadata = HashMap::new();
        metadata.insert("custom_key".to_string(), "custom_value".to_string());
        let schema = Schema::new(vec![
            Field::new("col1", DataType::Float64, false),
            Field::new("col2", DataType::Boolean, true),
        ])
        .with_metadata(metadata);

        let unique_id = "node-0.get_table_schema";

        handler.write_schema(unique_id, &schema).unwrap();

        let entry = handler.read_schema(unique_id).unwrap();
        assert!(entry.sql.is_none());
        assert!(entry.error.is_none());
        assert!(entry.data_base64.is_some());

        let reader = handler
            .decode_arrow_ipc(entry.data_base64.as_ref().unwrap())
            .unwrap();
        let read_schema = reader.schema();
        assert_eq!(read_schema.fields().len(), 2);
        assert_eq!(read_schema.field(0).name(), "col1");
        assert_eq!(read_schema.field(1).name(), "col2");
        assert_eq!(
            read_schema.metadata().get("custom_key"),
            Some(&"custom_value".to_string())
        );
    }

    #[test]
    fn schema_error_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let handler = SqliteHandler::new(dir.path());

        let unique_id = "node-1.get_table_schema";
        let error_msg = "Schema not found";

        handler.write_schema_error(unique_id, error_msg).unwrap();

        let entry = handler.read_schema(unique_id).unwrap();
        assert_eq!(entry.error.as_deref(), Some(error_msg));
        assert!(entry.data_base64.is_none());
    }

    #[test]
    fn exists() {
        let dir = tempfile::tempdir().unwrap();
        let handler = SqliteHandler::new(dir.path());

        assert!(!handler.exists());
        handler.connect().unwrap();
        assert!(handler.exists());
    }

    #[test]
    fn empty_batches() {
        let dir = tempfile::tempdir().unwrap();
        let handler = SqliteHandler::new(dir.path());

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let unique_id = "empty-0";
        let sql = "SELECT x FROM empty_table";

        handler.write_execute(unique_id, sql, &[], schema).unwrap();

        let entry = handler.read_execute(unique_id, sql).unwrap();
        let mut reader = handler
            .decode_arrow_ipc(entry.data_base64.as_ref().unwrap())
            .unwrap();

        assert!(reader.next().is_none());
        assert_eq!(reader.schema().fields().len(), 1);
        assert_eq!(reader.schema().field(0).name(), "x");
    }

    #[test]
    #[allow(deprecated)]
    fn detect_storage_types() {
        use crate::storage::{RECORDS_NAME, detect_storage_type};

        let dir = tempfile::tempdir().unwrap();

        assert_eq!(
            detect_storage_type(dir.path(), "test-0"),
            crate::storage::StorageType::FileParquet
        );

        let db_path = dir.path().join(RECORDS_NAME);
        std::fs::write(&db_path, "").unwrap();
        assert_eq!(
            detect_storage_type(dir.path(), "test-0"),
            crate::storage::StorageType::Sqlite
        );

        std::fs::remove_file(&db_path).unwrap();
        let arrow_path = dir.path().join("test-0.arrow");
        std::fs::write(&arrow_path, "").unwrap();
        #[allow(deprecated)]
        {
            assert_eq!(
                detect_storage_type(dir.path(), "test-0"),
                crate::storage::StorageType::FileArrowIpc
            );
        }
    }
}
