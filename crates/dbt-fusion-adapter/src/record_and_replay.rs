use crate::base_adapter::backend_of;
use crate::config::AdapterConfig;
use crate::errors::AdapterResult;
use crate::query_comment::QueryCommentConfig;
use crate::sql_engine::SqlEngine;
use crate::sql_types::TypeOps;
use crate::stmt_splitter::StmtSplitter;

use adbc_core::error::{Error as AdbcError, Result as AdbcResult, Status as AdbcStatus};
use adbc_core::options::{OptionStatement, OptionValue};
use arrow::array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaBuilder};
use dashmap::DashMap;
use dbt_common::adapter::AdapterType;
use dbt_common::cancellation::CancellationToken;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_xdbc::{Backend, Connection, QueryCtx, Statement};
use minijinja::State;
use once_cell::sync::Lazy;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use regex::Regex;

use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::fs::{self, File, create_dir_all, metadata};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::warn;

// The reason this is global is that we might have multiple adapters
// (we do not limit the number of adapters people can instantiate) and
// we might be running multiple fs commands in a single test (which
// can create more than one adapter total).
static COUNTERS: Lazy<DashMap<String, usize>> = Lazy::new(DashMap::new);

// Static regex pattern for matching dbt temporary table names with UUIDs
static DBT_TMP_UUID_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"dbt_tmp_[0-9a-f]{8}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{12}").unwrap()
});

// This is cleaning we need to do for our auto generated
// schemas in tests. Note ideal it is not localized but if things
// change in the ways we generate scheams things will start
// failing.
fn cleanup_schema_name(input: &str) -> String {
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
fn compute_file_name(query_ctx: &QueryCtx) -> AdbcResult<String> {
    let id = match query_ctx.node_id() {
        Some(node_id) => node_id,
        None => match query_ctx.sql() {
            Some(sql) => checksum8(&sql),
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

fn compute_file_name_for_node_id(node_id: Option<&str>) -> String {
    let id = node_id.unwrap_or("unknown");
    let mut entry = COUNTERS.entry(id.to_string()).or_insert(0);
    let file_name = format!("{}-{}", id, *entry);
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
            warn!(
                "Found DECIMAL128 with invalid precision=0, using defaults (38, 9) for field '{}'",
                field.name()
            );
            DataType::Decimal128(38, *scale)
        }
        DataType::Decimal256(precision, scale) if *precision == 0 => {
            // BigQuery BIGNUMERIC defaults: precision=76, scale=38
            warn!(
                "Found DECIMAL256 with invalid precision=0, using defaults (76, 38) for field '{}'",
                field.name()
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

pub struct RecordEngineInner {
    /// Path to recordings
    path: PathBuf,
    /// Actual (wrapped) engine
    engine: Arc<SqlEngine>,
}

/// Engine used for recording db interaction; recording engine is
/// a wrapper around an actual engine
#[derive(Clone)]
pub struct RecordEngine(Arc<RecordEngineInner>);

impl RecordEngine {
    pub fn new(path: PathBuf, engine: Arc<SqlEngine>) -> Self {
        let inner = RecordEngineInner { path, engine };
        RecordEngine(Arc::new(inner))
    }

    pub fn backend(&self) -> Backend {
        self.0.engine.backend()
    }

    pub fn new_connection(
        &self,
        state: Option<&State>,
        node_id: Option<String>,
    ) -> AdapterResult<Box<dyn Connection>> {
        let actual_conn = self.0.engine.new_connection(state, node_id.clone())?;
        let conn = RecordEngineConnection(self.0.clone(), actual_conn, node_id);
        Ok(Box::new(conn))
    }

    pub fn get_configured_database_name(&self) -> Option<Cow<'_, str>> {
        self.0.engine.get_configured_database_name()
    }

    pub fn config(&self, key: &str) -> Option<Cow<'_, str>> {
        self.0.engine.config(key)
    }

    pub fn get_config(&self) -> &AdapterConfig {
        self.0.engine.get_config()
    }

    pub fn adapter_type(&self) -> AdapterType {
        self.0.engine.adapter_type()
    }

    pub fn quoting(&self) -> ResolvedQuoting {
        self.0.engine.quoting()
    }

    pub fn splitter(&self) -> &dyn StmtSplitter {
        self.0.engine.splitter()
    }

    pub fn query_comment(&self) -> &QueryCommentConfig {
        self.0.engine.query_comment()
    }

    pub(crate) fn type_ops(&self) -> &dyn TypeOps {
        self.0.engine.type_ops()
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.0.engine.cancellation_token()
    }
}

struct RecordEngineConnection(Arc<RecordEngineInner>, Box<dyn Connection>, Option<String>);

impl fmt::Debug for RecordEngineConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecordEngineConnection")
    }
}

impl Connection for RecordEngineConnection {
    fn new_statement(&mut self) -> AdbcResult<Box<dyn Statement>> {
        let inner_stmt = self.1.new_statement()?;
        let stmt = RecordEngineStatement::new(self.0.clone(), inner_stmt);
        Ok(Box::new(stmt))
    }

    fn cancel(&mut self) -> AdbcResult<()> {
        self.1.cancel()
    }

    fn commit(&mut self) -> AdbcResult<()> {
        self.1.commit()
    }

    fn rollback(&mut self) -> AdbcResult<()> {
        self.1.rollback()
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> AdbcResult<Schema> {
        let result = self.1.get_table_schema(catalog, db_schema, table_name);

        let path = self.0.path.clone();
        create_dir_all(&path).map_err(|e| from_io_error(e, Some(&path)))?;

        let file_name = compute_file_name_for_node_id(self.2.as_deref());
        let err_path = path.join(format!("{file_name}.get_table_schema.err"));
        let parquet_path = path.join(format!("{file_name}.get_table_schema.parquet"));
        let metadata_path = path.join(format!("{file_name}.get_table_schema.metadata.json"));

        match result {
            Ok(schema) => {
                // Fix decimal types with invalid precision=0 before writing to parquet
                let fixed_schema = fix_decimal_precision_in_schema(&schema);

                // create empty record batch with fixed schema
                let schema_ref = Arc::new(fixed_schema.clone());
                let batch = RecordBatch::new_empty(schema_ref.clone());

                let file = File::create(&parquet_path)
                    .map_err(|e| from_io_error(e, Some(&parquet_path)))?;
                let props = WriterProperties::builder().build();
                let mut writer = ArrowWriter::try_new(file, schema_ref, Some(props))
                    .map_err(from_parquet_error)?;
                writer.write(&batch).map_err(from_parquet_error)?;
                writer.close().map_err(from_parquet_error)?;

                let metadata = fixed_schema.metadata();
                let metadata_json = serde_json::to_string(&metadata)
                    .map_err(|e| from_serde_error(e, Some(&metadata_path)))?;
                fs::write(&metadata_path, metadata_json)
                    .map_err(|e| from_io_error(e, Some(&metadata_path)))?;

                Ok(schema)
            }
            Err(err) => {
                let err_msg = format!("{err}");
                fs::write(&err_path, err_msg.clone())
                    .map_err(|e| from_io_error(e, Some(&err_path)))?;
                // do not create json or parquet, relay original error
                Err(AdbcError::with_message_and_status(
                    err_msg,
                    AdbcStatus::Internal,
                ))
            }
        }
    }
}

struct RecordEngineStatement {
    record_engine: Arc<RecordEngineInner>,
    inner_stmt: Box<dyn Statement>,
    query_ctx: Option<QueryCtx>,
}

impl RecordEngineStatement {
    pub fn new(
        record_engine: Arc<RecordEngineInner>,
        inner_stmt: Box<dyn Statement>,
    ) -> RecordEngineStatement {
        RecordEngineStatement {
            record_engine,
            inner_stmt,
            query_ctx: None,
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
        let query_ctx = self
            .query_ctx
            .clone()
            .expect("query has to be set before executing a statement");

        let sql = match query_ctx.sql() {
            Some(sql) => sql,
            None => "none".to_string(),
        };

        // Execute on the actual engine's Statement
        let result = self.inner_stmt.execute();

        let path = self.record_engine.path.clone();
        create_dir_all(&path).map_err(|e| from_io_error(e, Some(&path)))?;

        let file_name = compute_file_name(&query_ctx)?;
        let sql_path = path.join(format!("{file_name}.sql"));
        let err_path = path.join(format!("{file_name}.err"));
        let parquet_path = path.join(format!("{file_name}.parquet"));

        // store the query content (i.e., sql)
        fs::write(&sql_path, sql).map_err(|e| from_io_error(e, Some(&sql_path)))?;

        match result {
            Ok(mut reader) => {
                let schema = reader.schema();
                let batches: Vec<RecordBatch> = reader.by_ref().collect::<Result<_, _>>()?;

                let file = File::create(&parquet_path)
                    .map_err(|e| from_io_error(e, Some(&parquet_path)))?;
                let props = WriterProperties::builder().build();
                let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
                    .map_err(from_parquet_error)?;
                for batch in &batches {
                    writer.write(batch).map_err(from_parquet_error)?;
                }
                writer.close().map_err(from_parquet_error)?;
                // re-construct the stream from the accumulated batches
                let results = batches
                    .into_iter()
                    .map(|batch| -> Result<RecordBatch, ArrowError> { Ok(batch) });
                let iterator = RecordBatchIterator::new(results, schema);
                let reader = Box::new(iterator);
                Ok(reader)
            }
            Err(err) => {
                let err_msg = format!("{err}");
                fs::write(&err_path, err_msg.clone())
                    .map_err(|e| from_io_error(e, Some(&err_path)))?;
                // do not create json or parquet, relay original error
                Err(AdbcError::with_message_and_status(
                    err_msg,
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

    fn set_sql_query(&mut self, query_ctx: &QueryCtx) -> AdbcResult<()> {
        self.inner_stmt.set_sql_query(query_ctx)?;
        self.query_ctx = Some(query_ctx.clone());
        Ok(())
    }

    fn set_substrait_plan(&mut self, plan: &[u8]) -> AdbcResult<()> {
        self.inner_stmt.set_substrait_plan(plan)
    }

    fn cancel(&mut self) -> AdbcResult<()> {
        self.inner_stmt.cancel()
    }

    fn set_option(&mut self, key: OptionStatement, value: OptionValue) -> AdbcResult<()> {
        // TODO: Record options and then use those values when finding the file name
        self.inner_stmt.set_option(key, value)
    }
}

struct ReplayEngineInner {
    adapter_type: AdapterType,
    backend: Backend,
    /// Path to recordings
    path: PathBuf,
    /// Adapter config
    config: AdapterConfig,
    quoting: ResolvedQuoting,
    stmt_splitter: Arc<dyn StmtSplitter>,
    query_comment: QueryCommentConfig,
    type_ops: Box<dyn TypeOps>,
    /// Global CLI cancellation token
    cancellation_token: CancellationToken,
}

impl ReplayEngineInner {
    pub fn full_path(&self) -> PathBuf {
        self.path.clone()
    }
}

#[derive(Clone)]
pub struct ReplayEngine(Arc<ReplayEngineInner>);

impl ReplayEngine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        adapter_type: AdapterType,
        path: PathBuf,
        config: AdapterConfig,
        quoting: ResolvedQuoting,
        stmt_splitter: Arc<dyn StmtSplitter>,
        query_comment: QueryCommentConfig,
        type_ops: Box<dyn TypeOps>,
        token: CancellationToken,
    ) -> Self {
        let inner = ReplayEngineInner {
            adapter_type,
            backend: backend_of(adapter_type),
            path,
            config,
            quoting,
            stmt_splitter,
            query_comment,
            type_ops,
            cancellation_token: token,
        };
        ReplayEngine(Arc::new(inner))
    }

    pub fn new_connection(
        &self,
        _state: Option<&State>,
        node_id: Option<String>,
    ) -> AdapterResult<Box<dyn Connection>> {
        let conn = ReplayEngineConnection(self.0.clone(), node_id);
        Ok(Box::new(conn))
    }

    pub fn adapter_type(&self) -> AdapterType {
        self.0.adapter_type
    }

    pub fn backend(&self) -> Backend {
        self.0.backend
    }

    pub fn config(&self, key: &str) -> Option<Cow<'_, str>> {
        self.0.config.get_string(key)
    }

    pub fn get_config(&self) -> &AdapterConfig {
        &self.0.config
    }

    pub fn quoting(&self) -> ResolvedQuoting {
        self.0.quoting
    }

    pub fn splitter(&self) -> &dyn StmtSplitter {
        self.0.stmt_splitter.as_ref()
    }

    pub fn query_comment(&self) -> &QueryCommentConfig {
        &self.0.query_comment
    }

    pub(crate) fn type_ops(&self) -> &dyn TypeOps {
        self.0.type_ops.as_ref()
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.0.cancellation_token.clone()
    }
}

#[allow(dead_code)]
struct ReplayEngineConnection(Arc<ReplayEngineInner>, Option<String>);

impl fmt::Debug for ReplayEngineConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ReplayEngineConnection")
    }
}

impl Connection for ReplayEngineConnection {
    fn new_statement(&mut self) -> AdbcResult<Box<dyn Statement>> {
        let stmt = ReplayEngineStatement::new(self.0.clone());
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
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: &str,
    ) -> AdbcResult<Schema> {
        let path = self.0.path.clone();

        let file_name = compute_file_name_for_node_id(self.1.as_deref());
        let err_path = path.join(format!("{file_name}.get_table_schema.err"));
        let parquet_path = path.join(format!("{file_name}.get_table_schema.parquet"));
        let metadata_path = path.join(format!("{file_name}.get_table_schema.metadata.json"));

        // replay the error
        if err_path.exists() {
            let msg =
                fs::read_to_string(&err_path).map_err(|e| from_io_error(e, Some(&err_path)))?;
            return Err(AdbcError::with_message_and_status(
                msg,
                AdbcStatus::Internal,
            ));
        }

        // read the schema
        let file = File::open(&parquet_path).map_err(|e| from_io_error(e, Some(&parquet_path)))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(from_parquet_error)?;
        let reader = builder.build().map_err(from_parquet_error)?;
        let schema = reader.schema();

        // read the metadata
        let metadata_json = fs::read_to_string(&metadata_path)
            .map_err(|e| from_io_error(e, Some(&metadata_path)))?;
        let metadata: HashMap<String, String> = serde_json::from_str(&metadata_json)
            .map_err(|e| from_serde_error(e, Some(&metadata_path)))?;

        let mut schema_builder = SchemaBuilder::from(schema.fields());
        for (key, value) in metadata {
            schema_builder.metadata_mut().insert(key, value);
        }

        let schema = schema_builder.finish();
        Ok(schema)
    }
}

struct ReplayEngineStatement {
    replay_engine: Arc<ReplayEngineInner>,
    query_ctx: Option<QueryCtx>,
}

impl ReplayEngineStatement {
    fn new(replay_engine: Arc<ReplayEngineInner>) -> ReplayEngineStatement {
        ReplayEngineStatement {
            replay_engine,
            query_ctx: None,
        }
    }
}

fn from_parquet_error(e: parquet::errors::ParquetError) -> adbc_core::error::Error {
    adbc_core::error::Error::with_message_and_status(
        format!("Parquet error: {e:?}"),
        adbc_core::error::Status::IO,
    )
}

fn from_io_error(e: std::io::Error, path: Option<&Path>) -> adbc_core::error::Error {
    let message = if let Some(path) = path {
        format!("IO error: {:?} ({:?})", e, path.display())
    } else {
        format!("IO error: {e:?}")
    };
    adbc_core::error::Error::with_message_and_status(message, adbc_core::error::Status::IO)
}

fn from_serde_error(e: serde_json::Error, path: Option<&Path>) -> adbc_core::error::Error {
    let message = if let Some(path) = path {
        format!("Serde error: {:?} ({:?})", e, path.display())
    } else {
        format!("Serde error: {e:?}")
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
        let query_ctx = self
            .query_ctx
            .clone()
            .expect("query has to be set before executing a statement");

        let replay_sql = match query_ctx.sql() {
            Some(sql) => sql,
            None => "none".to_string(),
        };

        let path = self.replay_engine.full_path();
        let file_name = compute_file_name(&query_ctx)?;
        let parquet_path = path.join(format!("{file_name}.parquet"));
        let sql_path = path.join(format!("{file_name}.sql"));
        let err_path = path.join(format!("{file_name}.err"));

        // Query has to match to the recorded one, otherwise we
        // have issues with ordering or recording
        if !fs::exists(&sql_path).map_err(|e| from_io_error(e, Some(&sql_path)))? {
            panic!(
                "Missing query file ({:?}) during replay. Query: {}",
                &sql_path,
                replay_sql.as_str()
            );
        }
        // dbt_tmp_800c2fb4_a0ba_4708_a0b1_813316032bfb
        let record_sql =
            fs::read_to_string(&sql_path).map_err(|e| from_io_error(e, Some(&sql_path)))?;
        if normalize_dbt_tmp_name(&record_sql) != normalize_dbt_tmp_name(&replay_sql) {
            panic!(
                "Recorded query ({record_sql}) and actual query ({replay_sql}) do not match ({sql_path:?})"
            );
        }

        if err_path.exists() {
            // There was an error during recording, so we need to
            // replay now. TODO: Note that we do not at the moment
            // replay the exact error kind.
            let msg =
                fs::read_to_string(&err_path).map_err(|e| from_io_error(e, Some(&err_path)))?;
            return Err(AdbcError::with_message_and_status(
                msg,
                AdbcStatus::Internal,
            ));
        }

        // If parquet file is empty, then there was no schema during
        // recording
        let metadata =
            metadata(&parquet_path).map_err(|e| from_io_error(e, Some(&parquet_path)))?;
        let reader: Box<dyn RecordBatchReader + Send + 'a> = if metadata.len() == 0 {
            let schema = Arc::new(Schema::new(Vec::<Field>::new()));
            let batch = RecordBatch::new_empty(schema.clone());
            let results = vec![batch]
                .into_iter()
                .map(|batch| -> Result<RecordBatch, ArrowError> { Ok(batch) });
            let iterator = RecordBatchIterator::new(results, schema);
            Box::new(iterator)
        } else {
            let file =
                File::open(&parquet_path).map_err(|e| from_io_error(e, Some(&parquet_path)))?;
            let builder =
                ParquetRecordBatchReaderBuilder::try_new(file).map_err(from_parquet_error)?;
            let reader = builder.build().map_err(from_parquet_error)?;
            Box::new(reader)
        };
        Ok(reader)
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

    fn set_sql_query(&mut self, query_ctx: &QueryCtx) -> AdbcResult<()> {
        self.query_ctx = Some(query_ctx.clone());
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: &[u8]) -> AdbcResult<()> {
        unimplemented!("ReplayEngineStatement::set_substrait_plan")
    }

    fn cancel(&mut self) -> AdbcResult<()> {
        todo!("ReplayEngineStatement::cancel")
    }

    fn set_option(&mut self, _key: OptionStatement, _value: OptionValue) -> AdbcResult<()> {
        // TODO: Record options and then use those values when finding the file name
        Ok(())
    }
}

/// Replaces the UUID in a relation name created adapter.generate_unique_temporary_table_suffix
/// Example: "dbt_tmp_800c2fb4_a0ba_4708_a0b1_813316032bfb" -> "dbt_tmp_"
pub fn normalize_dbt_tmp_name(sql: &str) -> String {
    // Replace all matches with "dbt_tmp_"
    DBT_TMP_UUID_PATTERN
        .replace_all(sql, "dbt_tmp_")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_dbt_tmp_name() {
        // Test basic UUID replacement
        let input = "SELECT * FROM dbt_tmp_800c2fb4_a0ba_4708_a0b1_813316032bfb";
        let expected = "SELECT * FROM dbt_tmp_";
        assert_eq!(normalize_dbt_tmp_name(input), expected);
    }
}
