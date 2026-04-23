use adbc_core::error::{Error as AdbcError, Result as AdbcResult, Status as AdbcStatus};
use adbc_core::options::{OptionStatement, OptionValue};
use arrow::array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, Schema};
use dbt_xdbc::{Connection, Statement};
use std::fmt;
use std::fs::create_dir_all;
use std::path::PathBuf;

use crate::RecordingContext;
use crate::error::to_adbc_error;
use crate::naming::{compute_file_name, compute_file_name_for_table_schema};
use crate::storage::sqlite::SqliteHandler;

pub struct RecordConnection {
    recordings_path: PathBuf,
    inner: Box<dyn Connection>,
    ctx: RecordingContext,
    generation: u64,
}

impl RecordConnection {
    pub fn new(recordings_path: PathBuf, inner: Box<dyn Connection>, generation: u64) -> Self {
        Self {
            recordings_path,
            inner,
            ctx: RecordingContext::default(),
            generation,
        }
    }

    pub fn set_recording_context(&mut self, ctx: RecordingContext) {
        self.ctx = ctx;
    }
}

impl fmt::Debug for RecordConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecordConnection")
    }
}

impl Connection for RecordConnection {
    fn new_statement(&mut self) -> AdbcResult<Box<dyn Statement>> {
        let inner_stmt = self.inner.new_statement()?;
        let stmt =
            RecordStatement::new(self.recordings_path.clone(), inner_stmt, Default::default());
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
        create_dir_all(&path).map_err(|e| to_adbc_error(e.into(), Some(&path)))?;

        let unique_id = compute_file_name_for_table_schema(&path, catalog, db_schema, table_name);

        let sqlite_handler = SqliteHandler::new(&path);

        match result {
            Ok(schema) => {
                sqlite_handler
                    .write_schema(&unique_id, &schema)
                    .map_err(|e| to_adbc_error(e, Some(&path)))?;
                Ok(schema)
            }
            Err(err) => {
                sqlite_handler
                    .write_schema_error(&unique_id, &format!("{err}"))
                    .map_err(|e| to_adbc_error(e, Some(&path)))?;
                Err(AdbcError::with_message_and_status(
                    format!("{err}"),
                    AdbcStatus::Internal,
                ))
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

pub(crate) struct RecordStatement {
    recordings_path: PathBuf,
    inner_stmt: Box<dyn Statement>,
    ctx: RecordingContext,
    sql: Option<String>,
}

impl RecordStatement {
    fn new(
        recordings_path: PathBuf,
        inner_stmt: Box<dyn Statement>,
        ctx: RecordingContext,
    ) -> Self {
        Self {
            recordings_path,
            inner_stmt,
            ctx,
            sql: None,
        }
    }
}

impl Statement for RecordStatement {
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

        let result = self.inner_stmt.execute();

        let path = self.recordings_path.clone();
        create_dir_all(&path).map_err(|e| to_adbc_error(e.into(), Some(&path)))?;

        let unique_id = compute_file_name(
            &path,
            self.ctx.node_id.as_ref(),
            Some(sql),
            self.ctx.metadata,
        )?;

        let sqlite_handler = SqliteHandler::new(&path);

        match result {
            Ok(mut reader) => {
                let schema = reader.schema();
                let batches: Vec<RecordBatch> = reader.by_ref().collect::<Result<_, _>>()?;

                sqlite_handler
                    .write_execute(&unique_id, sql, &batches, schema.clone())
                    .map_err(|e| to_adbc_error(e, Some(&path)))?;

                let results = batches
                    .into_iter()
                    .map(|batch| -> Result<RecordBatch, ArrowError> { Ok(batch) });
                let iterator = RecordBatchIterator::new(results, schema);
                Ok(Box::new(iterator))
            }
            Err(err) => {
                sqlite_handler
                    .write_execute_error(&unique_id, sql, &format!("{err}"))
                    .map_err(|e| to_adbc_error(e, Some(&path)))?;
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
        if let OptionStatement::Other(ref name) = key {
            self.ctx.absorb_option(name, &value);
        }
        self.inner_stmt.set_option(key, value)
    }

    fn get_option_string(&self, key: OptionStatement) -> AdbcResult<String> {
        self.inner_stmt.get_option_string(key)
    }
}
