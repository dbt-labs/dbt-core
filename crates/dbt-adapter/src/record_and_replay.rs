use crate::sql::normalize::normalize_dbt_tmp_name;
use crate::statement::*;

use adbc_core::error::{Error as AdbcError, Result as AdbcResult, Status as AdbcStatus};
use adbc_core::options::{OptionStatement, OptionValue};
use arrow::array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow::ipc::reader::FileReader as ArrowFileReader;
use arrow::ipc::reader::StreamReader as ArrowStreamReader;
use arrow::ipc::writer::FileWriter as ArrowFileWriter;
use arrow::ipc::writer::StreamWriter as ArrowStreamWriter;
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaBuilder};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use dashmap::DashMap;
use dbt_common::ErrorCode;
use dbt_common::adapter::DBT_EXECUTION_PHASES;
use dbt_common::tracing::emit::emit_warn_log_message;
use dbt_xdbc::{Connection, Statement};
use once_cell::sync::Lazy;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use regex::Regex;
use rusqlite::params;

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::fs::{self, File, create_dir_all, metadata};
use std::hash::{Hash, Hasher};
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

// The reason this is global is that we might have multiple adapters
// (we do not limit the number of adapters people can instantiate) and
// we might be running multiple fs commands in a single test (which
// can create more than one adapter total).
static COUNTERS: Lazy<DashMap<String, usize>> = Lazy::new(DashMap::new);

static RECORDS_NAME: &str = "recordings.db";

// This is cleaning we need to do for our auto generated
// schemas in tests. Note ideal it is not localized but if things
// change in the ways we generate scheams things will start
// failing.
pub fn cleanup_schema_name(input: &str) -> String {
    let re = Regex::new(r"___.*?___").unwrap();
    re.replace_all(input, "").to_string()
}

fn checksum8(input: &str) -> String {
    let input = cleanup_schema_name(input);
    let mut hasher = DefaultHasher::new();
    input.hash(&mut hasher);
    let hash = hasher.finish();
    format!("{hash:x}")[..8.min(format!("{hash:x}").len())].to_string()
}

// Build a file name from the query context. In most cases this should
// be node id followed by the number of times that node id appeared in
// queries thus far. However, for pre-compile we do not have node id
// and only sql content that we checksum and then append to it a
// sequence number.
fn compute_file_name(
    node_id: Option<&String>,
    sql: Option<&str>,
    metadata: bool,
) -> AdbcResult<String> {
    let id = match node_id {
        Some(node_id) => {
            if metadata {
                debug_assert!(
                    sql.is_some(),
                    "A Statement with metadata must have a SQL query"
                );
                format!("{}-{}", node_id, checksum8(sql.unwrap()))
            } else {
                node_id.to_owned()
            }
        }
        None => match sql {
            Some(sql) => checksum8(sql),
            None => {
                return Err(AdbcError::with_message_and_status(
                    "Neither node id nor sql was set in the query context",
                    AdbcStatus::Internal,
                ));
            }
        },
    };

    let mut entry = COUNTERS.entry(id.clone()).or_insert(0);
    let file_name = format!("{}-{}", id, *entry);
    *entry += 1;

    Ok(file_name)
}

/// Compute a file name for get_table_schema based on the table identifier.
/// Uses a hash of the fully qualified table name.
fn compute_file_name_for_table_schema(
    catalog: Option<&str>,
    db_schema: Option<&str>,
    table_name: &str,
) -> String {
    // TODO(jason): This format is hard to review - we should actually migrate multi-command
    // invocations to individual invocation scoped folders
    let fqn = format!(
        "{}.{}.{}",
        catalog.unwrap_or("_"),
        db_schema.unwrap_or("_"),
        table_name
    );
    let hash = checksum8(&fqn);
    // Use counter to handle multiple calls to the same table (e.g., before/after creation)
    let counter_key = format!("get_table_schema.{hash}");
    let mut entry = COUNTERS.entry(counter_key).or_insert(0);
    let file_name = format!("get_table_schema.{hash}-{}", *entry);
    *entry += 1;
    file_name
}

/// Fixes decimal types with invalid precision (0) by setting appropriate defaults
/// This handles the case where BigQuery returns precision=0 for NUMERIC/BIGNUMERIC
/// types without explicit precision, which is invalid for Parquet
fn fix_decimal_precision_in_schema(schema: &Schema) -> Schema {
    let fixed_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| fix_decimal_precision_in_field(field))
        .collect();

    Schema::new(fixed_fields).with_metadata(schema.metadata().clone())
}

// FIXME(jason): This looks to be more of a driver problem, let's prevent 0 precision
// from coming back. Guard against it for now.
fn fix_decimal_precision_in_field(field: &Field) -> Field {
    let fixed_data_type = match field.data_type() {
        DataType::Decimal128(precision, scale) if *precision == 0 => {
            // BigQuery NUMERIC defaults: precision=38, scale=9
            emit_warn_log_message(
                ErrorCode::InvalidType,
                format!(
                    "Found DECIMAL128 with invalid precision=0, using defaults (38, 9) for field '{}'",
                    field.name()
                ),
                None,
            );
            DataType::Decimal128(38, *scale)
        }
        DataType::Decimal256(precision, scale) if *precision == 0 => {
            // BigQuery BIGNUMERIC defaults: precision=76, scale=38
            emit_warn_log_message(
                ErrorCode::InvalidType,
                format!(
                    "Found DECIMAL256 with invalid precision=0, using defaults (76, 38) for field '{}'",
                    field.name()
                ),
                None,
            );
            DataType::Decimal256(76, *scale)
        }
        DataType::List(list_field) => {
            let fixed_list_field = Arc::new(fix_decimal_precision_in_field(list_field));
            DataType::List(fixed_list_field)
        }
        DataType::LargeList(list_field) => {
            let fixed_list_field = Arc::new(fix_decimal_precision_in_field(list_field));
            DataType::LargeList(fixed_list_field)
        }
        DataType::Struct(fields) => {
            let fixed_fields: Vec<Arc<Field>> = fields
                .iter()
                .map(|f| Arc::new(fix_decimal_precision_in_field(f)))
                .collect();
            DataType::Struct(fixed_fields.into())
        }
        other => other.clone(),
    };

    Field::new(field.name(), fixed_data_type, field.is_nullable())
        .with_metadata(field.metadata().clone())
}

#[derive(Clone, Copy)]
enum FileFormat {
    Parquet,
    ArrowIPC,
}

impl FileFormat {
    fn extension(&self) -> &str {
        match self {
            FileFormat::Parquet => "parquet",
            FileFormat::ArrowIPC => "arrow",
        }
    }
}

#[derive(Debug)]
struct FileHandlerError(String);

impl fmt::Display for FileHandlerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for FileHandlerError {}

impl From<std::io::Error> for FileHandlerError {
    fn from(e: std::io::Error) -> Self {
        FileHandlerError(format!("IO error: {e}"))
    }
}

impl From<ArrowError> for FileHandlerError {
    fn from(e: ArrowError) -> Self {
        FileHandlerError(format!("Arrow error: {e}"))
    }
}

impl From<parquet::errors::ParquetError> for FileHandlerError {
    fn from(e: parquet::errors::ParquetError) -> Self {
        FileHandlerError(format!("Parquet error: {e}"))
    }
}

impl From<serde_json::Error> for FileHandlerError {
    fn from(e: serde_json::Error) -> Self {
        FileHandlerError(format!("JSON error: {e}"))
    }
}

impl From<rusqlite::Error> for FileHandlerError {
    fn from(e: rusqlite::Error) -> Self {
        FileHandlerError(format!("SQLite error: {e}"))
    }
}

impl From<base64::DecodeError> for FileHandlerError {
    fn from(e: base64::DecodeError) -> Self {
        FileHandlerError(format!("Base64 decode error: {e}"))
    }
}

type FileHandlerResult<T> = Result<T, FileHandlerError>;

// TODO: Move this to test utils for sharing
// Note: Some methods are only used in file-based replay mode (for drop-in replacement)
#[allow(dead_code)]
struct FileHandler {
    format: FileFormat,
}

#[allow(dead_code)]
impl FileHandler {
    /// Creates a new handler for recording (defaults to Arrow IPC)
    fn new_for_record() -> Self {
        Self {
            format: FileFormat::ArrowIPC,
        }
    }

    /// Creates a handler for replay with specific format
    fn new_for_replay(format: FileFormat) -> Self {
        Self { format }
    }

    fn extension(&self) -> &str {
        self.format.extension()
    }

    /// Writes a schema to file
    /// Arrow IPC preserves metadata natively, Parquet requires separate metadata file
    fn write_schema(
        &self,
        schema: &Schema,
        base_path: &Path,
        file_name: &str,
    ) -> FileHandlerResult<()> {
        let fixed_schema = fix_decimal_precision_in_schema(schema);
        let schema_ref = Arc::new(fixed_schema.clone());
        let batch = RecordBatch::new_empty(schema_ref.clone());

        let data_path = base_path.join(format!("{file_name}.{}", self.extension()));

        match self.format {
            FileFormat::ArrowIPC => {
                let file = File::create(data_path)?;
                let mut writer = ArrowFileWriter::try_new(file, &schema_ref)?;
                writer.write(&batch)?;
                writer.finish()?;
            }
            FileFormat::Parquet => {
                let file = File::create(&data_path)?;
                let props = WriterProperties::builder().build();
                let mut writer = ArrowWriter::try_new(file, schema_ref, Some(props))?;
                writer.write(&batch)?;
                writer.close()?;

                // Write separate metadata file for Parquet
                let metadata_path = base_path.join(format!("{file_name}.metadata.json"));
                let metadata = fixed_schema.metadata();
                let metadata_json = serde_json::to_string(&metadata)?;
                fs::write(metadata_path, metadata_json)?;
            }
        }

        Ok(())
    }

    /// Writes batches to file
    fn write_batches(
        &self,
        batches: &[RecordBatch],
        schema: Arc<Schema>,
        data_path: &Path,
    ) -> FileHandlerResult<()> {
        match self.format {
            FileFormat::ArrowIPC => {
                let file = File::create(data_path)?;
                let mut writer = ArrowFileWriter::try_new(file, &schema)?;
                for batch in batches {
                    writer.write(batch)?;
                }
                writer.finish()?;
            }
            FileFormat::Parquet => {
                let file = File::create(data_path)?;
                let props = WriterProperties::builder().build();
                let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
                for batch in batches {
                    writer.write(batch)?;
                }
                writer.close()?;
            }
        }

        Ok(())
    }

    /// Writes SQL to file
    fn write_sql(&self, base_path: &Path, file_name: &str, sql: &str) -> FileHandlerResult<()> {
        let sql_path = base_path.join(format!("{file_name}.sql"));
        Ok(fs::write(sql_path, sql)?)
    }

    /// Writes an error message to file
    fn write_error(
        &self,
        base_path: &Path,
        file_name: &str,
        error_msg: &str,
    ) -> FileHandlerResult<()> {
        let err_path = base_path.join(format!("{file_name}.err"));
        Ok(fs::write(err_path, error_msg)?)
    }

    /// Reads an error message from file if it exists
    fn read_error(&self, base_path: &Path, file_name: &str) -> FileHandlerResult<Option<String>> {
        let err_path = base_path.join(format!("{file_name}.err"));
        if err_path.exists() {
            let msg = fs::read_to_string(err_path)?;
            Ok(Some(msg))
        } else {
            Ok(None)
        }
    }

    /// Reads SQL from file
    fn read_sql(&self, base_path: &Path, file_name: &str) -> FileHandlerResult<String> {
        let sql_path = base_path.join(format!("{file_name}.sql"));
        Ok(fs::read_to_string(sql_path)?)
    }

    /// Reads schema from file
    /// Arrow IPC reads metadata natively, Parquet reads from separate metadata file
    fn read_schema(&self, base_path: &Path, file_name: &str) -> FileHandlerResult<Schema> {
        let data_path = base_path.join(format!("{file_name}.{}", self.extension()));

        match self.format {
            FileFormat::ArrowIPC => {
                // Arrow IPC preserves schema metadata natively
                let file = File::open(data_path)?;
                let reader = ArrowFileReader::try_new(file, None)?;
                Ok(reader.schema().as_ref().clone())
            }
            FileFormat::Parquet => {
                // Parquet needs to read schema + separate metadata file
                let file = File::open(&data_path)?;
                let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
                let reader = builder.build()?;
                let schema = reader.schema().as_ref().clone();

                // Read and merge metadata from separate file
                let metadata_path = base_path.join(format!("{file_name}.metadata.json"));
                let metadata_json = fs::read_to_string(metadata_path)?;
                let metadata: HashMap<String, String> = serde_json::from_str(&metadata_json)?;

                let mut schema_builder = SchemaBuilder::from(schema.fields());
                for (key, value) in metadata {
                    schema_builder.metadata_mut().insert(key, value);
                }

                Ok(schema_builder.finish())
            }
        }
    }

    /// Reads batches from file
    fn read_batches<'a>(
        &self,
        path: &Path,
    ) -> FileHandlerResult<Box<dyn RecordBatchReader + Send + 'a>> {
        let file_metadata = metadata(path)?;

        // Handle empty files
        if file_metadata.len() == 0 {
            let schema = Arc::new(Schema::new(Vec::<Field>::new()));
            let batch = RecordBatch::new_empty(schema.clone());
            let results = vec![batch]
                .into_iter()
                .map(|batch| -> Result<RecordBatch, ArrowError> { Ok(batch) });
            let iterator = RecordBatchIterator::new(results, schema);
            return Ok(Box::new(iterator));
        }

        match self.format {
            FileFormat::ArrowIPC => {
                let file = File::open(path)?;
                let reader = ArrowFileReader::try_new(file, None)?;
                Ok(Box::new(reader))
            }
            FileFormat::Parquet => {
                let file = File::open(path)?;
                let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
                let reader = builder.build()?;
                Ok(Box::new(reader))
            }
        }
    }
}

/// Recordings storage type, used to determine how to read/write recordings
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StorageType {
    Sqlite,
    FileArrowIpc,
    FileParquet,
}

/// Detects the storage type for a given recordings directory
fn detect_storage_type(path: &Path, file_name: &str) -> StorageType {
    let db_path = path.join(RECORDS_NAME);
    if db_path.exists() {
        return StorageType::Sqlite;
    }

    let arrow_path = path.join(format!("{file_name}.arrow"));
    if arrow_path.exists() {
        return StorageType::FileArrowIpc;
    }

    StorageType::FileParquet
}

/// Entry read from a SQLite recording
struct RecordingEntry {
    sql: Option<String>,
    data_base64: Option<String>,
    error: Option<String>,
}

/// Handler for SQLite-based recordings storage
struct SqliteHandler {
    db_path: PathBuf,
}

impl SqliteHandler {
    fn new(recordings_dir: &Path) -> Self {
        Self {
            db_path: recordings_dir.join(RECORDS_NAME),
        }
    }

    #[cfg(test)]
    fn exists(&self) -> bool {
        self.db_path.exists()
    }

    /// TODO: it is not ideal that the a [`rusqlite::Connection``] is instantiated per Statement/Connection operation
    fn connect(&self) -> FileHandlerResult<rusqlite::Connection> {
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

    /// Encodes Arrow record batches to base64-encoded IPC stream
    fn encode_arrow_ipc(
        &self,
        batches: &[RecordBatch],
        schema: Arc<Schema>,
    ) -> FileHandlerResult<String> {
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

    /// Decodes base64-encoded Arrow IPC stream to record batches
    fn decode_arrow_ipc(
        &self,
        data_base64: &str,
    ) -> FileHandlerResult<Box<dyn RecordBatchReader + Send>> {
        let bytes = BASE64_STANDARD.decode(data_base64)?;
        let cursor = Cursor::new(bytes);
        let reader = ArrowStreamReader::try_new(cursor, None)?;
        Ok(Box::new(reader))
    }

    /// Records an execute result (success case with data)
    fn write_execute(
        &self,
        unique_id: &str,
        sql: &str,
        batches: &[RecordBatch],
        schema: Arc<Schema>,
    ) -> FileHandlerResult<()> {
        let conn = self.connect()?;
        let data_base64 = self.encode_arrow_ipc(batches, schema)?;
        conn.execute(
            "INSERT OR REPLACE INTO recordings (unique_id, record_type, sql, data_base64, error)
             VALUES (?1, 'execute', ?2, ?3, NULL)",
            params![unique_id, sql, data_base64],
        )?;
        Ok(())
    }

    /// Records an execute error
    fn write_execute_error(
        &self,
        unique_id: &str,
        sql: &str,
        error_msg: &str,
    ) -> FileHandlerResult<()> {
        let conn = self.connect()?;
        conn.execute(
            "INSERT OR REPLACE INTO recordings (unique_id, record_type, sql, data_base64, error)
             VALUES (?1, 'execute', ?2, NULL, ?3)",
            params![unique_id, sql, error_msg],
        )?;
        Ok(())
    }

    /// Records a get_table_schema result (success case)
    fn write_schema(&self, unique_id: &str, schema: &Schema) -> FileHandlerResult<()> {
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

    /// Records a get_table_schema error
    fn write_schema_error(&self, unique_id: &str, error_msg: &str) -> FileHandlerResult<()> {
        let conn = self.connect()?;
        conn.execute(
            "INSERT OR REPLACE INTO recordings (unique_id, record_type, sql, data_base64, error)
             VALUES (?1, 'get_table_schema', NULL, NULL, ?2)",
            params![unique_id, error_msg],
        )?;
        Ok(())
    }

    /// Reads an execute recording
    fn read_execute(&self, unique_id: &str, replay_sql: &str) -> FileHandlerResult<RecordingEntry> {
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
                FileHandlerError(format!(
                    "Failed to read execute replay sql ({replay_sql}) for unique_id '{}': {}",
                    unique_id, e
                ))
            })?;
        Ok(entry)
    }

    /// Reads a get_table_schema recording
    fn read_schema(&self, unique_id: &str) -> FileHandlerResult<RecordingEntry> {
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

pub(crate) struct RecordEngineConnection {
    recordings_path: PathBuf,
    inner: Box<dyn Connection>,
    node_id: Option<String>,
}

impl RecordEngineConnection {
    pub(crate) fn new(
        recordings_path: PathBuf,
        inner: Box<dyn Connection>,
        node_id: Option<String>,
    ) -> Self {
        Self {
            recordings_path,
            inner,
            node_id,
        }
    }
}

impl fmt::Debug for RecordEngineConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecordEngineConnection")
    }
}

impl Connection for RecordEngineConnection {
    fn new_statement(&mut self) -> AdbcResult<Box<dyn Statement>> {
        let inner_stmt = self.inner.new_statement()?;
        let stmt = RecordEngineStatement::new(self.recordings_path.clone(), inner_stmt);
        Ok(Box::new(stmt))
    }

    fn cancel(&mut self) -> AdbcResult<()> {
        self.inner.cancel()
    }

    fn commit(&mut self) -> AdbcResult<()> {
        self.inner.commit()
    }

    fn rollback(&mut self) -> AdbcResult<()> {
        self.inner.rollback()
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> AdbcResult<Schema> {
        let result = self.inner.get_table_schema(catalog, db_schema, table_name);

        let path = self.recordings_path.clone();
        create_dir_all(&path).map_err(|e| from_fs_error(e.into(), Some(&path)))?;

        let unique_id = compute_file_name_for_table_schema(catalog, db_schema, table_name);

        let sqlite_handler = SqliteHandler::new(&path);

        match result {
            Ok(schema) => {
                sqlite_handler
                    .write_schema(&unique_id, &schema)
                    .map_err(|e| from_fs_error(e, Some(&path)))?;
                Ok(schema)
            }
            Err(err) => {
                sqlite_handler
                    .write_schema_error(&unique_id, &format!("{err}"))
                    .map_err(|e| from_fs_error(e, Some(&path)))?;
                Err(AdbcError::with_message_and_status(
                    format!("{err}"),
                    AdbcStatus::Internal,
                ))
            }
        }
    }

    fn update_node_id(&mut self, node_id: Option<String>) {
        self.node_id = node_id
    }
}

struct RecordEngineStatement {
    recordings_path: PathBuf,
    inner_stmt: Box<dyn Statement>,
    node_id: Option<String>,
    execution_phase: &'static str,
    sql: Option<String>,
    metadata: bool,
}

impl RecordEngineStatement {
    fn new(recordings_path: PathBuf, inner_stmt: Box<dyn Statement>) -> RecordEngineStatement {
        RecordEngineStatement {
            recordings_path,
            inner_stmt,
            node_id: None,
            execution_phase: "",
            sql: None,
            metadata: false,
        }
    }
}

impl Statement for RecordEngineStatement {
    fn bind(&mut self, batch: RecordBatch) -> AdbcResult<()> {
        self.inner_stmt.bind(batch)
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> AdbcResult<()> {
        self.inner_stmt.bind_stream(reader)
    }

    fn execute<'a>(&'a mut self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'a>> {
        let sql = match &self.sql {
            Some(sql) => sql,
            None => "none",
        };

        // Execute on the actual engine's Statement
        let result = self.inner_stmt.execute();

        let path = self.recordings_path.clone();
        create_dir_all(&path).map_err(|e| from_fs_error(e.into(), Some(&path)))?;

        let unique_id = compute_file_name(self.node_id.as_ref(), Some(sql), self.metadata)?;

        let sqlite_handler = SqliteHandler::new(&path);

        match result {
            Ok(mut reader) => {
                let schema = reader.schema();
                let batches: Vec<RecordBatch> = reader.by_ref().collect::<Result<_, _>>()?;

                sqlite_handler
                    .write_execute(&unique_id, sql, &batches, schema.clone())
                    .map_err(|e| from_fs_error(e, Some(&path)))?;

                // re-construct the stream from the accumulated batches
                let results = batches
                    .into_iter()
                    .map(|batch| -> Result<RecordBatch, ArrowError> { Ok(batch) });
                let iterator = RecordBatchIterator::new(results, schema);
                let reader = Box::new(iterator);
                Ok(reader)
            }
            Err(err) => {
                sqlite_handler
                    .write_execute_error(&unique_id, sql, &format!("{err}"))
                    .map_err(|e| from_fs_error(e, Some(&path)))?;
                Err(AdbcError::with_message_and_status(
                    format!("{err}"),
                    AdbcStatus::Internal,
                ))
            }
        }
    }

    fn execute_update(&mut self) -> AdbcResult<Option<i64>> {
        self.inner_stmt.execute_update()
    }

    fn execute_schema(&mut self) -> AdbcResult<Schema> {
        self.inner_stmt.execute_schema()
    }

    fn execute_partitions(&mut self) -> AdbcResult<adbc_core::PartitionedResult> {
        self.inner_stmt.execute_partitions()
    }

    fn get_parameter_schema(&self) -> AdbcResult<Schema> {
        self.inner_stmt.get_parameter_schema()
    }

    fn prepare(&mut self) -> AdbcResult<()> {
        self.inner_stmt.prepare()
    }

    fn set_sql_query(&mut self, sql: &str) -> AdbcResult<()> {
        self.inner_stmt.set_sql_query(sql)?;
        self.sql = Some(sql.to_string());
        Ok(())
    }

    fn set_substrait_plan(&mut self, plan: &[u8]) -> AdbcResult<()> {
        self.inner_stmt.set_substrait_plan(plan)
    }

    fn cancel(&mut self) -> AdbcResult<()> {
        self.inner_stmt.cancel()
    }

    fn set_option(&mut self, key: OptionStatement, value: OptionValue) -> AdbcResult<()> {
        match (key, value) {
            (OptionStatement::Other(name), OptionValue::String(node_id)) if name == DBT_NODE_ID => {
                self.node_id = Some(node_id);
                Ok(())
            }
            (OptionStatement::Other(name), OptionValue::String(execution_phase))
                if name == DBT_EXECUTION_PHASE =>
            {
                // convert the String into one of the valid &'static str or "" if invalid
                let phase_idx = DBT_EXECUTION_PHASES
                    .iter()
                    .position(|&p| p == execution_phase.as_str());
                let execution_phase: &'static str = match phase_idx {
                    Some(idx) => DBT_EXECUTION_PHASES[idx],
                    None => {
                        debug_assert!(false, "invalid execution phase: {}", execution_phase);
                        ""
                    }
                };
                self.execution_phase = execution_phase;
                Ok(())
            }
            (OptionStatement::Other(name), _)
                if [DBT_NODE_ID, DBT_EXECUTION_PHASE].contains(&name.as_str()) =>
            {
                debug_assert!(false, "expected string value for {} option", name);
                Ok(())
            }
            (OptionStatement::Other(name), OptionValue::Int(metadata)) if name == DBT_METADATA => {
                self.metadata = metadata != 0;
                Ok(())
            }
            (k, v) => self.inner_stmt.set_option(k, v),
        }
    }

    fn get_option_string(&self, key: OptionStatement) -> AdbcResult<String> {
        match key {
            OptionStatement::Other(name) if name == DBT_NODE_ID => {
                let s = self.node_id.clone().unwrap_or_default();
                Ok(s)
            }
            OptionStatement::Other(name) if name == DBT_EXECUTION_PHASE => {
                // XXX: return the execution_phase
                Ok("".to_string())
            }
            k => self.inner_stmt.get_option_string(k),
        }
    }
}

pub(crate) struct ReplayEngineConnection {
    recordings_path: PathBuf,
    node_id: Option<String>,
}

impl ReplayEngineConnection {
    pub(crate) fn new(recordings_path: PathBuf, node_id: Option<String>) -> Self {
        Self {
            recordings_path,
            node_id,
        }
    }
}

impl fmt::Debug for ReplayEngineConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ReplayEngineConnection")
    }
}

impl Connection for ReplayEngineConnection {
    fn new_statement(&mut self) -> AdbcResult<Box<dyn Statement>> {
        let stmt = ReplayEngineStatement::new(self.recordings_path.clone());
        Ok(Box::new(stmt))
    }

    fn cancel(&mut self) -> AdbcResult<()> {
        unimplemented!("ADBC connection cancellation in replay engine")
    }

    fn commit(&mut self) -> AdbcResult<()> {
        unimplemented!("ADBC connection commit in replay engine")
    }

    fn rollback(&mut self) -> AdbcResult<()> {
        unimplemented!("ADBC connection rollback in replay engine")
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> AdbcResult<Schema> {
        let path = self.recordings_path.clone();
        // Use table identifier for deterministic file naming (order-independent)
        let unique_id = compute_file_name_for_table_schema(catalog, db_schema, table_name);

        let storage_type = detect_storage_type(&path, &unique_id);

        match storage_type {
            StorageType::Sqlite => {
                let sqlite_handler = SqliteHandler::new(&path);
                let entry = sqlite_handler
                    .read_schema(&unique_id)
                    .map_err(|e| from_fs_error(e, Some(&path)))?;

                // Check for recorded error
                if let Some(msg) = entry.error {
                    return Err(AdbcError::with_message_and_status(
                        msg,
                        AdbcStatus::Internal,
                    ));
                }

                let data_base64 = entry.data_base64.ok_or_else(|| {
                    from_fs_error(
                        FileHandlerError("Missing data in recording".to_string()),
                        Some(&path),
                    )
                })?;

                let reader = sqlite_handler
                    .decode_arrow_ipc(&data_base64)
                    .map_err(|e| from_fs_error(e, Some(&path)))?;

                Ok(reader.schema().as_ref().clone())
            }
            StorageType::FileArrowIpc | StorageType::FileParquet => {
                // Fall back to file-based recordings
                let handler = if storage_type == StorageType::FileArrowIpc {
                    FileHandler::new_for_replay(FileFormat::ArrowIPC)
                } else {
                    FileHandler::new_for_replay(FileFormat::Parquet)
                };

                if let Some(msg) = handler
                    .read_error(&path, &unique_id)
                    .map_err(|e| from_fs_error(e, Some(&path)))?
                {
                    return Err(AdbcError::with_message_and_status(
                        msg,
                        AdbcStatus::Internal,
                    ));
                }

                let schema = handler
                    .read_schema(&path, &unique_id)
                    .map_err(|e| from_fs_error(e, Some(&path)))?;
                Ok(schema)
            }
        }
    }

    fn update_node_id(&mut self, node_id: Option<String>) {
        self.node_id = node_id
    }
}

struct ReplayEngineStatement {
    recordings_path: PathBuf,
    node_id: Option<String>,
    execution_phase: &'static str,
    sql: Option<String>,
    metadata: bool,
}

impl ReplayEngineStatement {
    fn new(recordings_path: PathBuf) -> ReplayEngineStatement {
        ReplayEngineStatement {
            recordings_path,
            node_id: None,
            execution_phase: "",
            sql: None,
            metadata: false,
        }
    }
}

fn from_fs_error(e: FileHandlerError, path: Option<&Path>) -> adbc_core::error::Error {
    let message = if let Some(path) = path {
        format!("{} (path: {})", e, path.display())
    } else {
        e.to_string()
    };
    adbc_core::error::Error::with_message_and_status(message, adbc_core::error::Status::IO)
}

impl Statement for ReplayEngineStatement {
    fn bind(&mut self, _batch: RecordBatch) -> AdbcResult<()> {
        todo!("ReplayEngineStatement::bind")
    }

    fn bind_stream(&mut self, _reader: Box<dyn RecordBatchReader + Send>) -> AdbcResult<()> {
        todo!("ReplayEngineStatement::bind_stream")
    }

    fn execute<'a>(&'a mut self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'a>> {
        let replay_sql = match &self.sql {
            Some(sql) => sql,
            None => "none",
        };

        let path = self.recordings_path.clone();
        let unique_id = compute_file_name(self.node_id.as_ref(), Some(replay_sql), self.metadata)?;

        // Detect storage type for backwards compatibility
        let storage_type = detect_storage_type(&path, &unique_id);

        match storage_type {
            StorageType::Sqlite => {
                let sqlite_handler = SqliteHandler::new(&path);
                let entry = sqlite_handler
                    .read_execute(&unique_id, replay_sql)
                    .map_err(|e| from_fs_error(e, Some(&path)))?;

                // Query has to match to the recorded one, otherwise we have issues with ordering
                let record_sql = entry.sql.as_deref().unwrap_or("none");
                if normalize_sql_for_comparison(record_sql)
                    != normalize_sql_for_comparison(replay_sql)
                        .replace("FUSION_ADAPTER_TESTING", "[MASKED_WH]")
                        .replace("FUSION_SLT_WAREHOUSE", "[MASKED_WH]")
                {
                    panic!(
                        "Recorded query ({record_sql}) and actual query ({replay_sql}) do not match (unique_id: {unique_id})"
                    );
                }

                // Check for recorded error
                if let Some(msg) = entry.error {
                    return Err(AdbcError::with_message_and_status(
                        msg,
                        AdbcStatus::Internal,
                    ));
                }

                // Decode the data from Arrow IPC
                let data_base64 = entry.data_base64.ok_or_else(|| {
                    from_fs_error(
                        FileHandlerError("Missing data in recording".to_string()),
                        Some(&path),
                    )
                })?;

                let reader = sqlite_handler
                    .decode_arrow_ipc(&data_base64)
                    .map_err(|e| from_fs_error(e, Some(&path)))?;
                Ok(reader)
            }
            StorageType::FileArrowIpc | StorageType::FileParquet => {
                // Fall back to file-based storage
                let arrow_path = path.join(format!("{unique_id}.arrow"));
                let parquet_path = path.join(format!("{unique_id}.parquet"));
                let (data_path, handler) = if storage_type == StorageType::FileArrowIpc {
                    (
                        arrow_path,
                        FileHandler::new_for_replay(FileFormat::ArrowIPC),
                    )
                } else {
                    (
                        parquet_path,
                        FileHandler::new_for_replay(FileFormat::Parquet),
                    )
                };

                let sql_path = path.join(format!("{unique_id}.sql"));

                // Query has to match to the recorded one, otherwise we have issues with ordering
                if !sql_path.exists() {
                    panic!(
                        "Missing query file ({:?}) during replay. Query: {}",
                        &sql_path, replay_sql,
                    );
                }

                let record_sql = handler
                    .read_sql(&path, &unique_id)
                    .map_err(|e| from_fs_error(e, Some(&path)))?;
                if normalize_sql_for_comparison(&record_sql)
                    != normalize_sql_for_comparison(replay_sql)
                        // we need to normalize
                        // in CI, FUSION_SLT_WAREHOUSE is used,
                        // locally, FUSION_ADAPTER_TESTING is used,
                        .replace("FUSION_ADAPTER_TESTING", "[MASKED_WH]")
                        .replace("FUSION_SLT_WAREHOUSE", "[MASKED_WH]")
                {
                    panic!(
                        "Recorded query ({record_sql}) and actual query ({replay_sql}) do not match ({sql_path:?})"
                    );
                }

                // Check for recorded error
                if let Some(msg) = handler
                    .read_error(&path, &unique_id)
                    .map_err(|e| from_fs_error(e, Some(&path)))?
                {
                    return Err(AdbcError::with_message_and_status(
                        msg,
                        AdbcStatus::Internal,
                    ));
                }

                let reader = handler
                    .read_batches(&data_path)
                    .map_err(|e| from_fs_error(e, Some(&data_path)))?;
                Ok(reader)
            }
        }
    }

    fn execute_update(&mut self) -> AdbcResult<Option<i64>> {
        todo!("ReplayEngineStatement::execute_update")
    }

    fn execute_schema(&mut self) -> AdbcResult<Schema> {
        todo!("ReplayEngineStatement::execute_schema")
    }

    fn execute_partitions(&mut self) -> AdbcResult<adbc_core::PartitionedResult> {
        todo!("ReplayEngineStatement::execute_partitions")
    }

    fn get_parameter_schema(&self) -> AdbcResult<Schema> {
        todo!("ReplayEngineStatement::get_parameter_schema")
    }

    fn prepare(&mut self) -> AdbcResult<()> {
        todo!("ReplayEngineStatement::prepare")
    }

    fn set_sql_query(&mut self, sql: &str) -> AdbcResult<()> {
        self.sql = Some(sql.to_string());
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: &[u8]) -> AdbcResult<()> {
        unimplemented!("ReplayEngineStatement::set_substrait_plan")
    }

    fn cancel(&mut self) -> AdbcResult<()> {
        todo!("ReplayEngineStatement::cancel")
    }

    fn set_option(&mut self, key: OptionStatement, value: OptionValue) -> AdbcResult<()> {
        match (key, value) {
            (OptionStatement::Other(name), OptionValue::String(node_id)) if name == DBT_NODE_ID => {
                self.node_id = Some(node_id);
                Ok(())
            }
            (OptionStatement::Other(name), OptionValue::String(execution_phase))
                if name == DBT_EXECUTION_PHASE =>
            {
                // convert the String into one of the valid &'static str or "" if invalid
                let phase_idx = DBT_EXECUTION_PHASES
                    .iter()
                    .position(|&p| p == execution_phase.as_str());
                let execution_phase: &'static str = match phase_idx {
                    Some(idx) => DBT_EXECUTION_PHASES[idx],
                    None => {
                        debug_assert!(false, "invalid execution phase: {}", execution_phase);
                        ""
                    }
                };
                self.execution_phase = execution_phase;
                Ok(())
            }
            (OptionStatement::Other(name), _)
                if [DBT_NODE_ID, DBT_EXECUTION_PHASE].contains(&name.as_str()) =>
            {
                debug_assert!(false, "expected string value for {} option", name);
                Ok(())
            }
            (OptionStatement::Other(name), OptionValue::Int(metadata)) if name == DBT_METADATA => {
                self.metadata = metadata != 0;
                Ok(())
            }
            (_k, _v) => {
                // TODO: Record options and then use those values when finding the file name
                Ok(())
            }
        }
    }

    fn get_option_string(&self, key: OptionStatement) -> AdbcResult<String> {
        match key {
            OptionStatement::Other(name) if name == DBT_NODE_ID => {
                let node_id = self.node_id.clone().unwrap_or_default();
                Ok(node_id)
            }
            OptionStatement::Other(name) if name == DBT_EXECUTION_PHASE => {
                debug_assert!(!self.execution_phase.is_empty());
                Ok(self.execution_phase.to_string())
            }
            k => Statement::get_option_string(self, k),
        }
    }
}

/// Normalizes SQL for replay comparison:
/// 1. Replaces dbt temporary table UUIDs
/// 2. Collapses all whitespace into single spaces
///
/// NOTE: `query_cache` has its own variant that strips SQL comments and removes
/// schema timestamp markers instead of collapsing whitespace, because the cache
/// key must be insensitive to comments and timestamps but preserves whitespace
/// structure.
fn normalize_sql_for_comparison(sql: &str) -> String {
    let normalized = normalize_dbt_tmp_name(sql);
    normalized.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_normalize_dbt_tmp_name() {
        // Test basic UUID replacement
        let input = "SELECT * FROM dbt_tmp_800c2fb4_a0ba_4708_a0b1_813316032bfb";
        let expected = "SELECT * FROM dbt_tmp_";
        assert_eq!(normalize_dbt_tmp_name(input), expected);
    }

    #[test]
    fn test_sqlite_handler_execute_roundtrip() {
        let dir = tempdir().unwrap();
        let handler = SqliteHandler::new(dir.path());

        // Create test data
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3])),
                Arc::new(arrow::array::StringArray::from(vec![
                    Some("alice"),
                    Some("bob"),
                    None,
                ])),
            ],
        )
        .unwrap();

        // Write the recording
        let unique_id = "test-node-0";
        let sql = "SELECT * FROM users";
        handler
            .write_execute(unique_id, sql, &[batch], schema)
            .unwrap();

        // Read it back
        let entry = handler.read_execute(unique_id, sql).unwrap();
        assert_eq!(entry.sql.as_deref(), Some(sql));
        assert!(entry.error.is_none());
        assert!(entry.data_base64.is_some());

        // Decode and verify
        let mut reader = handler
            .decode_arrow_ipc(entry.data_base64.as_ref().unwrap())
            .unwrap();
        let read_batch = reader.next().unwrap().unwrap();
        assert_eq!(read_batch.num_rows(), 3);
        assert_eq!(read_batch.num_columns(), 2);
    }

    #[test]
    fn test_sqlite_handler_execute_error_roundtrip() {
        let dir = tempdir().unwrap();
        let handler = SqliteHandler::new(dir.path());

        let unique_id = "test-error-0";
        let sql = "SELECT * FROM nonexistent";
        let error_msg = "Table not found: nonexistent";

        // Write error recording
        handler
            .write_execute_error(unique_id, sql, error_msg)
            .unwrap();

        // Read it back
        let entry = handler.read_execute(unique_id, sql).unwrap();
        assert_eq!(entry.sql.as_deref(), Some(sql));
        assert_eq!(entry.error.as_deref(), Some(error_msg));
        assert!(entry.data_base64.is_none());
    }

    #[test]
    fn test_sqlite_handler_schema_roundtrip() {
        let dir = tempdir().unwrap();
        let handler = SqliteHandler::new(dir.path());

        // Create test schema with metadata
        let mut metadata = HashMap::new();
        metadata.insert("custom_key".to_string(), "custom_value".to_string());
        let schema = Schema::new(vec![
            Field::new("col1", DataType::Float64, false),
            Field::new("col2", DataType::Boolean, true),
        ])
        .with_metadata(metadata);

        let unique_id = "node-0.get_table_schema";

        // Write the schema
        handler.write_schema(unique_id, &schema).unwrap();

        // Read it back
        let entry = handler.read_schema(unique_id).unwrap();
        assert!(entry.sql.is_none());
        assert!(entry.error.is_none());
        assert!(entry.data_base64.is_some());

        // Decode and verify schema
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
    fn test_sqlite_handler_schema_error_roundtrip() {
        let dir = tempdir().unwrap();
        let handler = SqliteHandler::new(dir.path());

        let unique_id = "node-1.get_table_schema";
        let error_msg = "Schema not found";

        // Write error
        handler.write_schema_error(unique_id, error_msg).unwrap();

        // Read it back
        let entry = handler.read_schema(unique_id).unwrap();
        assert_eq!(entry.error.as_deref(), Some(error_msg));
        assert!(entry.data_base64.is_none());
    }

    #[test]
    fn test_detect_storage_type() {
        let dir = tempdir().unwrap();

        // No files - defaults to Parquet
        assert_eq!(
            detect_storage_type(dir.path(), "test-0"),
            StorageType::FileParquet
        );

        // Create recordings.db - should detect SQLite
        let db_path = dir.path().join(RECORDS_NAME);
        fs::write(&db_path, "").unwrap();
        assert_eq!(
            detect_storage_type(dir.path(), "test-0"),
            StorageType::Sqlite
        );

        // Remove db, create arrow file
        fs::remove_file(&db_path).unwrap();
        let arrow_path = dir.path().join("test-0.arrow");
        fs::write(&arrow_path, "").unwrap();
        assert_eq!(
            detect_storage_type(dir.path(), "test-0"),
            StorageType::FileArrowIpc
        );
    }

    #[test]
    fn test_sqlite_handler_exists() {
        let dir = tempdir().unwrap();
        let handler = SqliteHandler::new(dir.path());

        // Before any write
        assert!(!handler.exists());

        // After connecting (which creates the db)
        handler.connect().unwrap();
        assert!(handler.exists());
    }

    #[test]
    fn test_sqlite_handler_empty_batches() {
        let dir = tempdir().unwrap();
        let handler = SqliteHandler::new(dir.path());

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let unique_id = "empty-0";
        let sql = "SELECT x FROM empty_table";

        // Write empty result
        handler.write_execute(unique_id, sql, &[], schema).unwrap();

        // Read it back
        let entry = handler.read_execute(unique_id, sql).unwrap();
        let mut reader = handler
            .decode_arrow_ipc(entry.data_base64.as_ref().unwrap())
            .unwrap();

        // Should get no batches
        assert!(reader.next().is_none());

        // Schema should still be correct
        assert_eq!(reader.schema().fields().len(), 1);
        assert_eq!(reader.schema().field(0).name(), "x");
    }
}
