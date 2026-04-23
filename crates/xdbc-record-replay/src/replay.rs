use adbc_core::error::{Error as AdbcError, Result as AdbcResult, Status as AdbcStatus};
use adbc_core::options::{OptionStatement, OptionValue};
use arrow::array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;
use dbt_xdbc::{Connection, Statement};
use std::fmt;
use std::path::{Path, PathBuf};

use crate::error::{RecordReplayError, to_adbc_error};
use crate::naming::{compute_file_name, compute_file_name_for_table_schema};
use crate::storage::StorageType;
use crate::storage::sqlite::SqliteHandler;
use crate::{RecordingContext, SharedConfig};

pub struct ReplayConnection {
    recordings_path: PathBuf,
    config: SharedConfig,
    ctx: RecordingContext,
    generation: u64,
}

impl ReplayConnection {
    pub fn new(recordings_path: PathBuf, config: SharedConfig, generation: u64) -> Self {
        Self {
            recordings_path,
            config,
            ctx: RecordingContext::default(),
            generation,
        }
    }

    pub fn set_recording_context(&mut self, ctx: RecordingContext) {
        self.ctx = ctx;
    }
}

impl fmt::Debug for ReplayConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ReplayConnection")
    }
}

impl Connection for ReplayConnection {
    fn new_statement(&mut self) -> AdbcResult<Box<dyn Statement>> {
        let stmt = ReplayStatement::new(
            self.recordings_path.clone(),
            self.config.clone(),
            Default::default(),
        );
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

    #[allow(deprecated)]
    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> AdbcResult<Schema> {
        let path = self.recordings_path.clone();
        let unique_id = compute_file_name_for_table_schema(&path, catalog, db_schema, table_name);

        let storage_type = crate::storage::detect_storage_type(&path, &unique_id);

        match storage_type {
            StorageType::Sqlite => {
                let sqlite_handler = SqliteHandler::new(&path);
                let entry = sqlite_handler
                    .read_schema(&unique_id)
                    .map_err(|e| to_adbc_error(e, Some(&path)))?;

                if let Some(msg) = entry.error {
                    return Err(AdbcError::with_message_and_status(
                        msg,
                        AdbcStatus::Internal,
                    ));
                }

                let data_base64 = entry.data_base64.ok_or_else(|| {
                    to_adbc_error(
                        RecordReplayError("Missing data in recording".to_string()),
                        Some(&path),
                    )
                })?;

                let reader = sqlite_handler
                    .decode_arrow_ipc(&data_base64)
                    .map_err(|e| to_adbc_error(e, Some(&path)))?;

                Ok(reader.schema().as_ref().clone())
            }
            StorageType::FileArrowIpc | StorageType::FileParquet => {
                replay_file_schema(&path, &unique_id, storage_type)
            }
        }
    }

    fn update_node_id(&mut self, node_id: Option<String>) {
        self.ctx.node_id = node_id;
    }

    fn generation(&self) -> u64 {
        self.generation
    }
}

pub(crate) struct ReplayStatement {
    recordings_path: PathBuf,
    config: SharedConfig,
    ctx: RecordingContext,
    sql: Option<String>,
}

impl ReplayStatement {
    fn new(recordings_path: PathBuf, config: SharedConfig, ctx: RecordingContext) -> Self {
        Self {
            recordings_path,
            config,
            ctx,
            sql: None,
        }
    }
}

impl Statement for ReplayStatement {
    fn bind(&mut self, _batch: RecordBatch) -> AdbcResult<()> {
        todo!("ReplayStatement::bind")
    }

    fn bind_stream(&mut self, _reader: Box<dyn RecordBatchReader + Send>) -> AdbcResult<()> {
        todo!("ReplayStatement::bind_stream")
    }

    #[allow(deprecated)]
    fn execute<'a>(&'a mut self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'a>> {
        let replay_sql = match &self.sql {
            Some(sql) => sql,
            None => "none",
        };

        let path = self.recordings_path.clone();
        let unique_id = compute_file_name(
            &path,
            self.ctx.node_id.as_ref(),
            Some(replay_sql),
            self.ctx.metadata,
        )?;

        let storage_type = crate::storage::detect_storage_type(&path, &unique_id);

        match storage_type {
            StorageType::Sqlite => {
                let sqlite_handler = SqliteHandler::new(&path);
                let entry = sqlite_handler
                    .read_execute(&unique_id, replay_sql)
                    .map_err(|e| to_adbc_error(e, Some(&path)))?;

                let record_sql = entry.sql.as_deref().unwrap_or("none");
                if self.config.normalize_sql(record_sql) != self.config.normalize_sql(replay_sql) {
                    panic!(
                        "Recorded query ({record_sql}) and actual query ({replay_sql}) do not match (unique_id: {unique_id})"
                    );
                }

                if let Some(msg) = entry.error {
                    return Err(AdbcError::with_message_and_status(
                        msg,
                        AdbcStatus::Internal,
                    ));
                }

                let data_base64 = entry.data_base64.ok_or_else(|| {
                    to_adbc_error(
                        RecordReplayError("Missing data in recording".to_string()),
                        Some(&path),
                    )
                })?;

                let reader = sqlite_handler
                    .decode_arrow_ipc(&data_base64)
                    .map_err(|e| to_adbc_error(e, Some(&path)))?;
                Ok(reader)
            }
            StorageType::FileArrowIpc | StorageType::FileParquet => {
                replay_file_execute(&path, &unique_id, replay_sql, storage_type, &self.config)
            }
        }
    }

    fn execute_update(&mut self) -> AdbcResult<Option<i64>> {
        todo!("ReplayStatement::execute_update")
    }

    fn execute_schema(&mut self) -> AdbcResult<Schema> {
        todo!("ReplayStatement::execute_schema")
    }

    fn execute_partitions(&mut self) -> AdbcResult<adbc_core::PartitionedResult> {
        todo!("ReplayStatement::execute_partitions")
    }

    fn get_parameter_schema(&self) -> AdbcResult<Schema> {
        todo!("ReplayStatement::get_parameter_schema")
    }

    fn prepare(&mut self) -> AdbcResult<()> {
        todo!("ReplayStatement::prepare")
    }

    fn set_sql_query(&mut self, sql: &str) -> AdbcResult<()> {
        self.sql = Some(sql.to_string());
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: &[u8]) -> AdbcResult<()> {
        unimplemented!("ReplayStatement::set_substrait_plan")
    }

    fn cancel(&mut self) -> AdbcResult<()> {
        todo!("ReplayStatement::cancel")
    }

    fn set_option(&mut self, key: OptionStatement, value: OptionValue) -> AdbcResult<()> {
        if let OptionStatement::Other(ref name) = key {
            self.ctx.absorb_option(name, &value);
        }
        Ok(())
    }

    fn get_option_string(&self, _key: OptionStatement) -> AdbcResult<String> {
        Ok(String::new())
    }
}

// ---------------------------------------------------------------------------
// Deprecated file-based replay helpers
// ---------------------------------------------------------------------------

#[allow(deprecated)]
fn replay_file_schema(
    path: &Path,
    unique_id: &str,
    storage_type: StorageType,
) -> AdbcResult<Schema> {
    use crate::storage::file::{FileFormat, FileHandler};

    let handler = if storage_type == StorageType::FileArrowIpc {
        FileHandler::new_for_replay(FileFormat::ArrowIPC)
    } else {
        FileHandler::new_for_replay(FileFormat::Parquet)
    };

    if let Some(msg) = handler
        .read_error(path, unique_id)
        .map_err(|e| to_adbc_error(e, Some(path)))?
    {
        return Err(AdbcError::with_message_and_status(
            msg,
            AdbcStatus::Internal,
        ));
    }

    let schema = handler
        .read_schema(path, unique_id)
        .map_err(|e| to_adbc_error(e, Some(path)))?;
    Ok(schema)
}

#[allow(deprecated)]
fn replay_file_execute<'a>(
    path: &Path,
    unique_id: &str,
    replay_sql: &str,
    storage_type: StorageType,
    config: &SharedConfig,
) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'a>> {
    use crate::storage::file::{FileFormat, FileHandler};

    let (data_path, handler) = if storage_type == StorageType::FileArrowIpc {
        (
            path.join(format!("{unique_id}.arrow")),
            FileHandler::new_for_replay(FileFormat::ArrowIPC),
        )
    } else {
        (
            path.join(format!("{unique_id}.parquet")),
            FileHandler::new_for_replay(FileFormat::Parquet),
        )
    };

    let sql_path = path.join(format!("{unique_id}.sql"));

    if !sql_path.exists() {
        panic!(
            "Missing query file ({:?}) during replay. Query: {}",
            &sql_path, replay_sql,
        );
    }

    let record_sql = handler
        .read_sql(path, unique_id)
        .map_err(|e| to_adbc_error(e, Some(path)))?;
    if config.normalize_sql(&record_sql) != config.normalize_sql(replay_sql) {
        panic!(
            "Recorded query ({record_sql}) and actual query ({replay_sql}) do not match ({sql_path:?})"
        );
    }

    if let Some(msg) = handler
        .read_error(path, unique_id)
        .map_err(|e| to_adbc_error(e, Some(path)))?
    {
        return Err(AdbcError::with_message_and_status(
            msg,
            AdbcStatus::Internal,
        ));
    }

    let reader = handler
        .read_batches(&data_path)
        .map_err(|e| to_adbc_error(e, Some(&data_path)))?;
    Ok(reader)
}
