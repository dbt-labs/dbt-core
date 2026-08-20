use adbc_core::error::{Error as AdbcError, Result as AdbcResult, Status as AdbcStatus};
use adbc_core::options::{OptionStatement, OptionValue};
use arrow::array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, Schema};
use dbt_adbc::{Connection, Statement};
use std::fmt;
use std::fs::create_dir_all;
use std::path::PathBuf;
use std::sync::Arc;

use crate::RecordingContext;
use crate::SharedConfig;
use crate::error::to_adbc_error;
use crate::naming::{
    compute_file_name, compute_file_name_for_get_objects, compute_file_name_for_table_schema,
};
use crate::storage::sqlite::SqliteHandler;

pub struct RecordConnection {
    recordings_path: PathBuf,
    inner: Box<dyn Connection>,
    config: SharedConfig,
    ctx: RecordingContext,
    generation: u64,
}

impl RecordConnection {
    pub fn new(
        recordings_path: PathBuf,
        inner: Box<dyn Connection>,
        config: SharedConfig,
        generation: u64,
    ) -> Self {
        Self {
            recordings_path,
            inner,
            config,
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

    fn get_objects<'a>(
        &'a self,
        depth: adbc_core::options::ObjectDepth,
        catalog: Option<&'a str>,
        db_schema: Option<&'a str>,
        table_name: Option<&'a str>,
        table_type: Option<Vec<&'a str>>,
        column_name: Option<&'a str>,
    ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'a>> {
        let result = self.inner.get_objects(
            depth,
            catalog,
            db_schema,
            table_name,
            table_type.clone(),
            column_name,
        );
        let path = self.recordings_path.clone();
        create_dir_all(&path).map_err(|e| to_adbc_error(e.into(), Some(&path)))?;

        // Normalize so a randomized __dbt_tmp suffix doesn't change the hash key.
        let normalized_table_name = table_name.map(|t| self.config.normalize_sql(t));
        let unique_id = compute_file_name_for_get_objects(
            &path,
            self.ctx.node_id.as_deref(),
            catalog,
            db_schema,
            normalized_table_name.as_deref(),
            table_type.as_deref(),
            column_name,
        );

        let sqlite_handler = SqliteHandler::new(&path);

        match result {
            Ok(reader) => {
                let schema = reader.schema();
                let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;
                sqlite_handler
                    .write_objects(&unique_id, &batches, schema.clone())
                    .map_err(|e| to_adbc_error(e, Some(&path)))?;
                let batches_iter = batches
                    .into_iter()
                    .map(|batch| -> Result<RecordBatch, ArrowError> { Ok(batch) });
                Ok(Box::new(RecordBatchIterator::new(batches_iter, schema)))
            }
            Err(err) => {
                sqlite_handler
                    .write_objects_error(&unique_id, &format!("{err}"))
                    .map_err(|e| to_adbc_error(e, Some(&path)))?;
                Err(AdbcError::with_message_and_status(
                    format!("{err}"),
                    AdbcStatus::Internal,
                ))
            }
        }
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

        // Same as get_objects: keep the hash key stable despite tmp-suffix churn.
        let normalized_table_name = self.config.normalize_sql(table_name);
        let unique_id = compute_file_name_for_table_schema(
            &path,
            self.ctx.node_id.as_deref(),
            catalog,
            db_schema,
            &normalized_table_name,
        );

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

    fn fingerprint(&self) -> u64 {
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
    pub(crate) fn new(
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
        let sql = match &self.sql {
            Some(sql) => sql,
            None => "none",
        };

        let result = self.inner_stmt.execute_update();

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
            Ok(rows_affected) => {
                sqlite_handler
                    .write_execute(&unique_id, sql, &[], Arc::new(Schema::empty()))
                    .map_err(|e| to_adbc_error(e, Some(&path)))?;
                Ok(rows_affected)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replay::ReplayStatement;

    struct MockStatement {
        result: Option<AdbcResult<Option<i64>>>,
    }

    impl Statement for MockStatement {
        fn bind(&mut self, _batch: RecordBatch) -> AdbcResult<()> {
            unimplemented!()
        }
        fn bind_stream(&mut self, _reader: Box<dyn RecordBatchReader + Send>) -> AdbcResult<()> {
            unimplemented!()
        }
        fn execute<'a>(&'a mut self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'a>> {
            unimplemented!()
        }
        fn execute_update(&mut self) -> AdbcResult<Option<i64>> {
            self.result.take().expect("execute_update called once")
        }
        fn execute_schema(&mut self) -> AdbcResult<Schema> {
            unimplemented!()
        }
        fn execute_partitions(&mut self) -> AdbcResult<adbc_core::PartitionedResult> {
            unimplemented!()
        }
        fn get_parameter_schema(&self) -> AdbcResult<Schema> {
            unimplemented!()
        }
        fn prepare(&mut self) -> AdbcResult<()> {
            unimplemented!()
        }
        fn set_sql_query(&mut self, _sql: &str) -> AdbcResult<()> {
            Ok(())
        }
        fn set_substrait_plan(&mut self, _plan: &[u8]) -> AdbcResult<()> {
            unimplemented!()
        }
        fn cancel(&mut self) -> AdbcResult<()> {
            unimplemented!()
        }
    }

    fn ctx_with_node_id(node_id: &str) -> RecordingContext {
        RecordingContext {
            node_id: Some(node_id.to_string()),
            metadata: false,
        }
    }

    #[test]
    fn record_execute_update_writes_a_recording() {
        let dir = tempfile::tempdir().unwrap();
        let mock = MockStatement {
            result: Some(Ok(Some(3))),
        };
        let mut recorder = RecordStatement::new(
            dir.path().to_path_buf(),
            Box::new(mock),
            ctx_with_node_id("model.test.a"),
        );
        recorder.set_sql_query("INSERT INTO t VALUES (1)").unwrap();
        assert_eq!(recorder.execute_update().unwrap(), Some(3));

        let entry = SqliteHandler::new(dir.path())
            .read_execute("model.test.a-0", "INSERT INTO t VALUES (1)")
            .unwrap();
        assert_eq!(entry.sql.as_deref(), Some("INSERT INTO t VALUES (1)"));
    }

    #[test]
    fn replay_execute_update_matches_recording_made_via_execute() {
        let dir = tempfile::tempdir().unwrap();
        SqliteHandler::new(dir.path())
            .write_execute(
                "model.test.a-0",
                "CREATE SCHEMA IF NOT EXISTS x",
                &[],
                Arc::new(Schema::empty()),
            )
            .unwrap();

        let mut replayer = ReplayStatement::new(
            dir.path().to_path_buf(),
            SharedConfig::default(),
            ctx_with_node_id("model.test.a"),
        );
        replayer
            .set_sql_query("CREATE SCHEMA IF NOT EXISTS x")
            .unwrap();
        assert_eq!(replayer.execute_update().unwrap(), None);
    }

    #[test]
    #[should_panic(expected = "do not match")]
    fn replay_execute_update_panics_on_sql_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        SqliteHandler::new(dir.path())
            .write_execute(
                "model.test.a-0",
                "CREATE SCHEMA IF NOT EXISTS x",
                &[],
                Arc::new(Schema::empty()),
            )
            .unwrap();

        let mut replayer = ReplayStatement::new(
            dir.path().to_path_buf(),
            SharedConfig::default(),
            ctx_with_node_id("model.test.a"),
        );
        replayer
            .set_sql_query("SELECT * FROM x.non_existing_dest")
            .unwrap();
        let _ = replayer.execute_update();
    }
}
