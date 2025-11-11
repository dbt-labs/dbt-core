use once_cell::sync::Lazy;
use regex::Regex;
use scc::HashMap as SccHashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use adbc_core::error::{Error as AdbcError, Result as AdbcResult, Status as AdbcStatus};
use adbc_core::options::{OptionStatement, OptionValue};
use arrow::array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, Field, Schema};
use dbt_xdbc::query_ctx::ExecutionPhase;
use dbt_xdbc::{QueryCtx, Statement};

use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;

use crate::sql::normalize::strip_sql_comments;

#[derive(Default, Clone)]
pub enum QueryCacheMode {
    Read,
    Write,
    #[default]
    ReadWrite,
}

pub trait QueryCache: Send + Sync {
    fn new_statement(
        &self,
        stmt: Box<dyn Statement>,
        ctx: QueryCtx,
        sql: String,
    ) -> Box<dyn Statement>;
}

pub struct QueryCacheStatement {
    query_cache_config: Arc<QueryCacheConfig>,
    counters: Arc<SccHashMap<String, usize>>,
    inner_stmt: Box<dyn Statement>,
    query_ctx: QueryCtx,
    sql: String,
}

impl QueryCacheStatement {
    fn construct_output_dir(&self, node_id: &str) -> PathBuf {
        self.query_cache_config.root_path.join(node_id)
    }

    fn construct_output_file_name(&self, node_id: &str, hash: &str, num: usize) -> PathBuf {
        self.construct_output_dir(node_id)
            .join(format!("{hash}_{num}.parquet"))
    }

    fn compute_sql_hash(&self) -> String {
        let sql = if self.sql.is_empty() {
            "none"
        } else {
            &self.sql
        };
        let sql = normalize_sql_for_comparison(sql);
        let mut hasher = DefaultHasher::new();
        sql.hash(&mut hasher);
        let hash = hasher.finish();
        format!("{hash:x}")[..8.min(format!("{hash:x}").len())].to_string()
    }

    fn compute_file_index(&self, node_id: &str, phase: &ExecutionPhase, cache_key: &str) -> usize {
        // If the phase is analyze, we need to find the max file index for the given cache_key
        if matches!(phase, ExecutionPhase::Analyze) {
            let output_dir = self.construct_output_dir(node_id);
            // List all files in the directory prefixed by the cache_key, find the max file_index suffix
            if let Ok(files) = std::fs::read_dir(output_dir)
                && let Some(max_index) = files
                    .filter_map(|entry| entry.ok())
                    .filter(|entry| entry.file_name().to_str().unwrap().starts_with(cache_key))
                    .map(|entry| {
                        entry
                            .file_name()
                            .to_str()
                            .unwrap()
                            .split("_")
                            .nth(1)
                            .unwrap()
                            .parse::<usize>()
                            .unwrap()
                    })
                    .max()
            {
                return max_index;
            }
        }
        // If the phase is not analyze (or no match exists), we need to increment the counter for the given node_id
        // This is safe because analyze always happens after render, so we will only ever add a cache entry, and reuse in the next analyze
        let mut entry = self.counters.entry_sync(node_id.to_string()).or_insert(0);
        *entry += 1;
        *entry
    }

    fn read_cache<'a>(
        &self,
        file_path: &Path,
    ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'a>> {
        let metadata =
            std::fs::metadata(file_path).map_err(|e| from_io_error(e, Some(file_path)))?;
        if metadata.len() == 0 {
            let schema = Arc::new(Schema::new(Vec::<Field>::new()));
            let batch = RecordBatch::new_empty(schema.clone());
            let results = vec![batch]
                .into_iter()
                .map(|batch| -> Result<RecordBatch, ArrowError> { Ok(batch) });
            let iterator = RecordBatchIterator::new(results, schema);
            Ok(Box::new(iterator))
        } else {
            let file =
                std::fs::File::open(file_path).map_err(|e| from_io_error(e, Some(file_path)))?;
            let builder =
                ParquetRecordBatchReaderBuilder::try_new(file).map_err(from_parquet_error)?;
            let reader = builder.build().map_err(from_parquet_error)?;
            Ok(Box::new(reader))
        }
    }

    fn write_cache<'a>(
        parquet_path: &Path,
        reader: &mut Box<dyn RecordBatchReader + Send + 'a>,
    ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'a>> {
        std::fs::create_dir_all(parquet_path.parent().unwrap())
            .map_err(|e| from_io_error(e, Some(parquet_path.parent().unwrap())))?;
        let schema = reader.schema();
        let batches: Vec<RecordBatch> = reader.by_ref().collect::<Result<_, _>>()?;

        let file = std::fs::File::create(parquet_path)
            .map_err(|e| from_io_error(e, Some(parquet_path)))?;
        let props = WriterProperties::builder().build();
        let mut writer =
            ArrowWriter::try_new(file, schema.clone(), Some(props)).map_err(from_parquet_error)?;
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

    fn check_ttl(&self, file_path: &Path) -> AdbcResult<bool> {
        if let Some(ttl) = self.query_cache_config.ttl {
            if let Ok(metadata) = std::fs::metadata(file_path) {
                // The TTL is a duration (i.e. 12 hours), check if the file was modified within the TTL
                if let Ok(modified) = metadata.modified() {
                    if modified < std::time::SystemTime::now() - ttl {
                        return Ok(false);
                    }
                } else {
                    return Ok(false);
                }
            } else {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

impl Statement for QueryCacheStatement {
    fn bind(&mut self, batch: RecordBatch) -> AdbcResult<()> {
        self.inner_stmt.bind(batch)
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> AdbcResult<()> {
        self.inner_stmt.bind_stream(reader)
    }

    fn execute<'a>(&'a mut self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'a>> {
        self.inner_stmt.set_sql_query(&self.query_ctx, &self.sql)?;
        let (node_id, phase) = if let Some(node_id) = self.query_ctx.node_id()
            && let Some(phase) = self.query_ctx.phase()
            && self.query_cache_config.phases.contains(&phase)
        {
            (node_id, phase)
        } else {
            return self.inner_stmt.execute();
        };

        match self.query_cache_config.mode {
            QueryCacheMode::Read | QueryCacheMode::Write => {
                unimplemented!("QueryCacheMode::Read | QueryCacheMode::Write is not implemented")
            }
            QueryCacheMode::ReadWrite => {
                // First, compute the file name by hashing the query and suffixing the index
                let sql_hash = self.compute_sql_hash();
                let index = self.compute_file_index(node_id, &phase, &sql_hash);
                let path = self.construct_output_file_name(node_id, &sql_hash, index);
                if path.exists() {
                    if self.check_ttl(&path)? {
                        let cache_read = self.read_cache(&path);
                        return cache_read;
                    } else {
                        // Try to remove the parent directory if the file is stale
                        let _ = std::fs::remove_dir_all(path.parent().unwrap());
                    }
                }
                // Execute on the actual engine's Statement
                let result = self.inner_stmt.execute();
                // TODO: Add invalidation logic to ensure when a cache hit is not found, we invalidate downstreams (in Render Phase)
                match result {
                    Ok(mut reader) => QueryCacheStatement::write_cache(&path, &mut reader),
                    Err(err) => {
                        let err_msg = format!("{err}");
                        Err(AdbcError::with_message_and_status(
                            err_msg,
                            AdbcStatus::Internal,
                        ))
                    }
                }
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

    fn set_sql_query(&mut self, ctx: &QueryCtx, sql: &str) -> AdbcResult<()> {
        self.query_ctx = ctx.clone();
        self.sql = sql.to_string();
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

pub struct QueryCacheConfig {
    mode: QueryCacheMode,
    root_path: PathBuf,
    ttl: Option<Duration>,
    phases: Vec<ExecutionPhase>,
}

impl QueryCacheConfig {
    pub fn new(
        mode: QueryCacheMode,
        root_path: PathBuf,
        ttl: Option<Duration>,
        phases: Vec<ExecutionPhase>,
    ) -> Self {
        Self {
            mode,
            root_path,
            ttl,
            phases,
        }
    }
}

pub struct QueryCacheImpl {
    config: Arc<QueryCacheConfig>,
    // We need to keep track of which index we are on per node id (NodeId, StatementCount)
    counters: Arc<SccHashMap<String, usize>>,
}

impl QueryCacheImpl {
    pub fn new(config: QueryCacheConfig) -> Self {
        Self {
            config: Arc::new(config),
            counters: Arc::new(SccHashMap::new()),
        }
    }
}

impl QueryCache for QueryCacheImpl {
    fn new_statement(
        &self,
        stmt: Box<dyn Statement>,
        ctx: QueryCtx,
        sql: String,
    ) -> Box<dyn Statement> {
        Box::new(QueryCacheStatement {
            query_cache_config: self.config.clone(),
            counters: self.counters.clone(),
            inner_stmt: stmt,
            query_ctx: ctx,
            sql,
        })
    }
}

fn from_io_error(e: std::io::Error, path: Option<&Path>) -> adbc_core::error::Error {
    let message = if let Some(path) = path {
        format!("IO error: {:?} ({:?})", e, path.display())
    } else {
        format!("IO error: {e:?}")
    };
    adbc_core::error::Error::with_message_and_status(message, adbc_core::error::Status::IO)
}

fn from_parquet_error(e: parquet::errors::ParquetError) -> adbc_core::error::Error {
    adbc_core::error::Error::with_message_and_status(
        format!("Parquet error: {e:?}"),
        adbc_core::error::Status::IO,
    )
}

// Static regex pattern for matching dbt temporary table names with UUIDs
static DBT_TMP_UUID_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"dbt_tmp_[0-9a-f]{8}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{12}").unwrap()
});

/// Replaces the UUID in a relation name created adapter.generate_unique_temporary_table_suffix
/// Example: "dbt_tmp_800c2fb4_a0ba_4708_a0b1_813316032bfb" -> "dbt_tmp_"
pub fn normalize_dbt_tmp_name(sql: &str) -> String {
    // Replace all matches with "dbt_tmp_"
    DBT_TMP_UUID_PATTERN
        .replace_all(&strip_sql_comments(sql), "dbt_tmp_")
        .to_string()
}

/// Normalizes SQL for comparison by removing both temporary table UUIDs and schema timestamps
fn normalize_sql_for_comparison(sql: &str) -> String {
    let normalized = normalize_dbt_tmp_name(sql);
    // Also clean up schema names with timestamps
    let re = Regex::new(r"___\d+___").unwrap();
    re.replace_all(&normalized, "").to_string()
}
