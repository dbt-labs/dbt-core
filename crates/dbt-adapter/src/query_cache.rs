use dbt_adapter_core::{
    DBT_EXECUTION_PHASE_ANALYZE, DBT_EXECUTION_PHASE_RENDER, DBT_EXECUTION_PHASES,
};
use regex::Regex;
use scc::HashMap as SccHashMap;
use scc::HashSet as SccHashSet;
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::Duration;

use crate::sql::normalize::normalize_dbt_tmp_name;

use adbc_core::error::{Error as AdbcError, Result as AdbcResult, Status as AdbcStatus};
use adbc_core::options::{OptionStatement, OptionValue};
use arrow::array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, Field, Schema};
use dbt_xdbc::Statement;

use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;

use crate::sql::normalize::strip_sql_comments;
use crate::statement::*;

type NodeId = String;

pub trait QueryCache: Send + Sync {
    fn new_statement(&self, inner_stmt: Box<dyn Statement>) -> Box<dyn Statement>;

    /// Provide the reverse dependency graph so the cache can invalidate
    /// downstream nodes on a render-phase miss.  Keys are node-ids; values are
    /// the set of **direct** downstream node-ids.
    fn set_reverse_deps(&self, _reverse_deps: HashMap<NodeId, HashSet<NodeId>>) {}
}

pub struct QueryCacheStatement {
    query_cache_config: Arc<QueryCacheConfig>,
    counters: Arc<SccHashMap<NodeId, usize>>,
    reverse_deps: Arc<OnceLock<HashMap<NodeId, HashSet<NodeId>>>>,
    invalidated_nodes: Arc<SccHashSet<NodeId>>,
    inner_stmt: Box<dyn Statement>,
    /// Node ID associated with this query
    node_id: Option<NodeId>,
    /// One of [DBT_EXECUTION_PHASES] or ""
    execution_phase: &'static str,
    /// Whether the caller expects results (`true` = read/SELECT, `false` = DDL/DML).
    fetch: bool,
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
        let hex = format!("{hash:x}");
        hex[..8.min(hex.len())].to_string()
    }

    /// PRECONDITION: phase is one of [DBT_EXECUTION_PHASES]
    fn compute_file_index(&self, node_id: &str, phase: &'static str, cache_key: &str) -> usize {
        debug_assert!(DBT_EXECUTION_PHASES.contains(&phase));
        // If the phase is analyze, we need to find the max file index for the given cache_key
        if phase == DBT_EXECUTION_PHASE_ANALYZE {
            let output_dir = self.construct_output_dir(node_id);
            // List all files in the directory prefixed by the cache_key, find the max file_index suffix
            if let Ok(files) = std::fs::read_dir(output_dir)
                && let Some(max_index) = files
                    .filter_map(|entry| entry.ok())
                    .filter(|entry| {
                        entry
                            .file_name()
                            .to_str()
                            .is_some_and(|s| s.starts_with(cache_key))
                    })
                    .filter_map(|entry| parse_cache_file_index(&entry.file_name()))
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
        let parent_dir = parquet_path.parent().ok_or_else(|| {
            AdbcError::with_message_and_status(
                format!("Invalid cache path (no parent): {}", parquet_path.display()),
                AdbcStatus::Internal,
            )
        })?;
        std::fs::create_dir_all(parent_dir).map_err(|e| from_io_error(e, Some(parent_dir)))?;
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

    /// Mark all transitive downstream nodes as invalidated so they
    /// clear their own cache directories when they next execute.
    fn invalidate_downstreams(&self, node_id: &str) {
        let Some(reverse_deps) = self.reverse_deps.get() else {
            return;
        };
        let mut stack = vec![node_id];
        let mut visited = HashSet::new();
        visited.insert(node_id);

        while let Some(current) = stack.pop() {
            if let Some(downstreams) = reverse_deps.get(current) {
                for ds in downstreams {
                    if visited.insert(ds.as_str()) {
                        let _ = self.invalidated_nodes.insert_sync(ds.clone());
                        stack.push(ds.as_str());
                    }
                }
            }
        }
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
        self.inner_stmt.set_sql_query(&self.sql)?;
        let (node_id, phase) = if let Some(node_id) = &self.node_id
            && self.fetch
            && self
                .query_cache_config
                .phases
                .contains(&self.execution_phase)
        {
            (node_id, self.execution_phase)
        } else {
            return self.inner_stmt.execute();
        };

        // If an upstream node had a cache miss and marked this node as invalid,
        // delete this node's cache directory now. This is safe because the task runner
        // runs nodes in order, so upstream nodes have already finished, and only this node touches its own directory.
        if self.invalidated_nodes.remove_sync(node_id).is_some() {
            let dir = self.construct_output_dir(node_id);
            let _ = std::fs::remove_dir_all(&dir);
        }

        let sql_hash = self.compute_sql_hash();
        let index = self.compute_file_index(node_id, phase, &sql_hash);
        let path = self.construct_output_file_name(node_id, &sql_hash, index);
        if path.exists() {
            if self.check_ttl(&path)? {
                if let Ok(reader) = self.read_cache(&path) {
                    return Ok(reader);
                }
            }
            if let Some(parent) = path.parent() {
                let _ = std::fs::remove_dir_all(parent);
            }
        }

        // We have a cache miss -- invalidate downstream nodes so they also
        // re-execute instead of serving stale cached results.
        if phase == DBT_EXECUTION_PHASE_RENDER {
            self.invalidate_downstreams(node_id);
        }

        let result = self.inner_stmt.execute();
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
            (OptionStatement::Other(name), OptionValue::Int(fetch_flag)) if name == DBT_FETCH => {
                self.fetch = fetch_flag != 0;
                Ok(())
            }
            (OptionStatement::Other(name), _)
                if [DBT_NODE_ID, DBT_EXECUTION_PHASE, DBT_FETCH].contains(&name.as_str()) =>
            {
                debug_assert!(false, "expected correct value type for {name} option");
                Ok(())
            }
            (k, v) => self.inner_stmt.set_option(k, v),
        }
    }

    fn get_option_string(&self, key: OptionStatement) -> AdbcResult<String> {
        match key {
            OptionStatement::Other(name) if name == DBT_NODE_ID => {
                let node_id = self.node_id.as_deref().unwrap_or("");
                Ok(node_id.to_string())
            }
            OptionStatement::Other(name) if name == DBT_EXECUTION_PHASE => {
                Ok(self.execution_phase.to_string())
            }
            k => self.inner_stmt.get_option_string(k),
        }
    }
}

pub struct QueryCacheConfig {
    root_path: PathBuf,
    ttl: Option<Duration>,
    phases: Vec<&'static str>,
}

impl QueryCacheConfig {
    pub fn new(root_path: PathBuf, ttl: Option<Duration>, phases: Vec<&'static str>) -> Self {
        Self {
            root_path,
            ttl,
            phases,
        }
    }
}

pub struct QueryCacheImpl {
    config: Arc<QueryCacheConfig>,
    counters: Arc<SccHashMap<NodeId, usize>>,
    reverse_deps: Arc<OnceLock<HashMap<NodeId, HashSet<NodeId>>>>,
    invalidated_nodes: Arc<SccHashSet<NodeId>>,
}

impl QueryCacheImpl {
    pub fn new(config: QueryCacheConfig) -> Self {
        Self {
            config: Arc::new(config),
            counters: Arc::new(SccHashMap::new()),
            reverse_deps: Arc::new(OnceLock::new()),
            invalidated_nodes: Arc::new(SccHashSet::new()),
        }
    }
}

impl QueryCache for QueryCacheImpl {
    fn new_statement(&self, inner_stmt: Box<dyn Statement>) -> Box<dyn Statement> {
        Box::new(QueryCacheStatement {
            query_cache_config: self.config.clone(),
            counters: self.counters.clone(),
            reverse_deps: self.reverse_deps.clone(),
            invalidated_nodes: self.invalidated_nodes.clone(),
            inner_stmt,
            node_id: None,
            execution_phase: "",
            fetch: true,
            sql: "".to_string(),
        })
    }

    fn set_reverse_deps(&self, reverse_deps: HashMap<NodeId, HashSet<NodeId>>) {
        let _ = self.reverse_deps.set(reverse_deps);
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

/// Normalizes SQL for cache-key comparison:
/// 1. Strips SQL comments (so comment-only changes don't bust the cache)
/// 2. Replaces dbt temporary table UUIDs (so re-runs with new UUIDs hit the cache)
/// 3. Removes schema timestamp markers (`___<digits>___`)
///
/// NOTE: `record_and_replay` has its own variant that collapses whitespace instead
/// of stripping timestamps, and does *not* strip comments (recordings must be
/// byte-for-byte reproducible).
fn normalize_sql_for_comparison(sql: &str) -> String {
    static RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"___\d+___").unwrap());
    let without_comments = strip_sql_comments(sql);
    let without_uuids = normalize_dbt_tmp_name(&without_comments);
    RE.replace_all(&without_uuids, "").to_string()
}

/// Extracts the numeric index from a cache filename like "abc12345_1.parquet".
///
/// Cache files are named `{hash}_{index}.parquet`. This function parses the index
/// portion from the filename, handling the `.parquet` extension properly.
fn parse_cache_file_index(filename: &OsStr) -> Option<usize> {
    let stem = Path::new(filename).file_stem()?.to_str()?;
    let num_str = stem.split('_').nth(1)?;
    num_str.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::PrimitiveArray;
    use arrow::datatypes::{DataType, Int32Type};

    struct NoopStatement;

    impl Statement for NoopStatement {
        fn bind(&mut self, _: RecordBatch) -> AdbcResult<()> {
            Ok(())
        }
        fn bind_stream(&mut self, _: Box<dyn RecordBatchReader + Send>) -> AdbcResult<()> {
            Ok(())
        }
        fn execute<'a>(&'a mut self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'a>> {
            let schema = Arc::new(Schema::empty());
            let batch = RecordBatch::new_empty(schema.clone());
            Ok(Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema)))
        }
        fn execute_update(&mut self) -> AdbcResult<Option<i64>> {
            Ok(None)
        }
        fn execute_schema(&mut self) -> AdbcResult<Schema> {
            Ok(Schema::empty())
        }
        fn execute_partitions(&mut self) -> AdbcResult<adbc_core::PartitionedResult> {
            unimplemented!()
        }
        fn get_parameter_schema(&self) -> AdbcResult<Schema> {
            Ok(Schema::empty())
        }
        fn prepare(&mut self) -> AdbcResult<()> {
            Ok(())
        }
        fn set_sql_query(&mut self, _: &str) -> AdbcResult<()> {
            Ok(())
        }
        fn set_substrait_plan(&mut self, _: &[u8]) -> AdbcResult<()> {
            Ok(())
        }
        fn cancel(&mut self) -> AdbcResult<()> {
            Ok(())
        }
    }

    fn make_config(ttl: Option<Duration>) -> Arc<QueryCacheConfig> {
        Arc::new(QueryCacheConfig::new(
            PathBuf::from("/tmp/test_cache"),
            ttl,
            vec![],
        ))
    }

    fn make_stmt(sql: &str) -> QueryCacheStatement {
        QueryCacheStatement {
            query_cache_config: make_config(None),
            counters: Arc::new(SccHashMap::new()),
            reverse_deps: Arc::new(OnceLock::new()),
            invalidated_nodes: Arc::new(SccHashSet::new()),
            inner_stmt: Box::new(NoopStatement),
            node_id: None,
            execution_phase: "",
            fetch: true,
            sql: sql.to_string(),
        }
    }

    #[test]
    fn test_parse_cache_file_index() {
        // Normal cases
        assert_eq!(
            parse_cache_file_index(OsStr::new("abc12345_1.parquet")),
            Some(1)
        );
        assert_eq!(
            parse_cache_file_index(OsStr::new("abc12345_123.parquet")),
            Some(123)
        );
        assert_eq!(
            parse_cache_file_index(OsStr::new("a1b2c3d4_42.parquet")),
            Some(42)
        );

        // Without extension (edge case)
        assert_eq!(parse_cache_file_index(OsStr::new("abc_1")), Some(1));

        // Missing number part
        assert_eq!(parse_cache_file_index(OsStr::new("abc.parquet")), None);

        // Non-numeric index
        assert_eq!(parse_cache_file_index(OsStr::new("abc_foo.parquet")), None);

        // Empty string
        assert_eq!(parse_cache_file_index(OsStr::new("")), None);

        // Just underscore
        assert_eq!(parse_cache_file_index(OsStr::new("_")), None);

        // Index with leading zeros
        assert_eq!(
            parse_cache_file_index(OsStr::new("abc_007.parquet")),
            Some(7)
        );
    }

    #[test]
    fn test_normalize_strips_uuid() {
        let a = "SELECT * FROM dbt_tmp_800c2fb4_a0ba_4708_a0b1_813316032bfb";
        let b = "SELECT * FROM dbt_tmp_11111111_2222_3333_4444_555555555555";
        assert_eq!(
            normalize_sql_for_comparison(a),
            normalize_sql_for_comparison(b)
        );
    }

    #[test]
    fn test_normalize_strips_schema_timestamps() {
        let a = "SELECT * FROM schema___1234567890___table";
        let b = "SELECT * FROM schema___9999999999___table";
        assert_eq!(
            normalize_sql_for_comparison(a),
            normalize_sql_for_comparison(b)
        );
        assert_eq!(normalize_sql_for_comparison(a), "SELECT * FROM schematable");
    }

    #[test]
    fn test_normalize_strips_comments() {
        let a = "SELECT 1 -- comment";
        let b = "SELECT 1 -- different comment";
        assert_eq!(
            normalize_sql_for_comparison(a),
            normalize_sql_for_comparison(b)
        );
    }

    #[test]
    fn test_normalize_combined() {
        let a = "/* v1 */ SELECT * FROM schema___111___ WHERE dbt_tmp_aaaaaaaa_bbbb_cccc_dddd_eeeeeeeeeeee";
        let b = "/* v2 */ SELECT * FROM schema___222___ WHERE dbt_tmp_11111111_2222_3333_4444_555555555555";
        assert_eq!(
            normalize_sql_for_comparison(a),
            normalize_sql_for_comparison(b)
        );
    }

    #[test]
    fn test_normalize_passthrough() {
        let sql = "SELECT id, name FROM users WHERE active = true";
        assert_eq!(normalize_sql_for_comparison(sql), sql);
    }

    // -- compute_sql_hash -------------------------------------------------------

    #[test]
    fn test_compute_sql_hash_deterministic() {
        let stmt = make_stmt("SELECT 1");
        let h1 = stmt.compute_sql_hash();
        let h2 = stmt.compute_sql_hash();
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_compute_sql_hash_length() {
        let stmt = make_stmt("SELECT 1");
        assert_eq!(stmt.compute_sql_hash().len(), 8);
        assert!(
            stmt.compute_sql_hash()
                .chars()
                .all(|c| c.is_ascii_hexdigit())
        );
    }

    #[test]
    fn test_compute_sql_hash_differs_for_different_sql() {
        let s1 = make_stmt("SELECT 1");
        let s2 = make_stmt("SELECT 2");
        assert_ne!(s1.compute_sql_hash(), s2.compute_sql_hash());
    }

    #[test]
    fn test_compute_sql_hash_empty_sql() {
        let s1 = make_stmt("");
        let s2 = make_stmt("");
        assert_eq!(s1.compute_sql_hash(), s2.compute_sql_hash());
    }

    #[test]
    fn test_compute_sql_hash_ignores_uuid_differences() {
        let s1 = make_stmt("SELECT * FROM dbt_tmp_aaaaaaaa_bbbb_cccc_dddd_eeeeeeeeeeee");
        let s2 = make_stmt("SELECT * FROM dbt_tmp_11111111_2222_3333_4444_555555555555");
        assert_eq!(s1.compute_sql_hash(), s2.compute_sql_hash());
    }

    // -- write_cache / read_cache round-trip -------------------------------------

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let col = PrimitiveArray::<Int32Type>::from(vec![1, 2, 3]);
        RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap()
    }

    #[test]
    fn test_write_read_cache_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("node1").join("abc_1.parquet");

        let batch = make_test_batch();
        let schema = batch.schema();
        let mut reader: Box<dyn RecordBatchReader + Send> =
            Box::new(RecordBatchIterator::new(vec![Ok(batch.clone())], schema));

        let returned = QueryCacheStatement::write_cache(&path, &mut reader).unwrap();
        let returned_batches: Vec<RecordBatch> = returned.collect::<Result<_, _>>().unwrap();
        assert_eq!(returned_batches.len(), 1);
        assert_eq!(returned_batches[0], batch);

        let stmt = make_stmt("SELECT 1");
        let read_back = stmt.read_cache(&path).unwrap();
        let read_batches: Vec<RecordBatch> = read_back.collect::<Result<_, _>>().unwrap();
        assert_eq!(read_batches.len(), 1);
        assert_eq!(read_batches[0], batch);
    }

    #[test]
    fn test_read_cache_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.parquet");
        std::fs::write(&path, b"").unwrap();

        let stmt = make_stmt("SELECT 1");
        let reader = stmt.read_cache(&path).unwrap();
        let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 0);
    }

    // -- check_ttl --------------------------------------------------------------

    #[test]
    fn test_check_ttl_no_ttl_always_valid() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file.parquet");
        std::fs::write(&path, b"data").unwrap();

        let stmt = make_stmt("x");
        assert!(stmt.check_ttl(&path).unwrap());
    }

    #[test]
    fn test_check_ttl_missing_file() {
        let mut stmt = make_stmt("");
        stmt.query_cache_config = make_config(Some(Duration::from_secs(3600)));
        assert!(!stmt.check_ttl(Path::new("/nonexistent/file")).unwrap());
    }

    fn make_stmt_with_cache_dir(root: &Path) -> QueryCacheStatement {
        QueryCacheStatement {
            query_cache_config: Arc::new(QueryCacheConfig::new(root.to_path_buf(), None, vec![])),
            counters: Arc::new(SccHashMap::new()),
            reverse_deps: Arc::new(OnceLock::new()),
            invalidated_nodes: Arc::new(SccHashSet::new()),
            inner_stmt: Box::new(NoopStatement),
            node_id: None,
            execution_phase: "",
            fetch: true,
            sql: String::new(),
        }
    }

    #[test]
    fn test_invalidate_downstreams_marks_direct_downstreams() {
        let stmt = make_stmt_with_cache_dir(Path::new("/tmp/unused"));
        let _ = stmt.reverse_deps.set(HashMap::from([(
            "A".to_string(),
            HashSet::from(["B".into(), "C".into()]),
        )]));

        stmt.invalidate_downstreams("A");

        assert!(
            !stmt.invalidated_nodes.contains_sync(&"A".to_string()),
            "upstream should not be marked"
        );
        assert!(stmt.invalidated_nodes.contains_sync(&"B".to_string()));
        assert!(stmt.invalidated_nodes.contains_sync(&"C".to_string()));
    }

    #[test]
    fn test_invalidate_downstreams_marks_transitive() {
        let stmt = make_stmt_with_cache_dir(Path::new("/tmp/unused"));
        let _ = stmt.reverse_deps.set(HashMap::from([
            ("A".to_string(), HashSet::from(["B".into()])),
            ("B".to_string(), HashSet::from(["C".into()])),
        ]));

        stmt.invalidate_downstreams("A");

        assert!(!stmt.invalidated_nodes.contains_sync(&"A".to_string()));
        assert!(stmt.invalidated_nodes.contains_sync(&"B".to_string()));
        assert!(stmt.invalidated_nodes.contains_sync(&"C".to_string()));
    }

    #[test]
    fn test_invalidate_downstreams_no_deps_is_noop() {
        let stmt = make_stmt_with_cache_dir(Path::new("/tmp/unused"));
        stmt.invalidate_downstreams("A");

        assert!(!stmt.invalidated_nodes.contains_sync(&"A".to_string()));
        assert!(!stmt.invalidated_nodes.contains_sync(&"B".to_string()));
    }

    #[test]
    fn test_invalidate_downstreams_does_not_mark_unrelated_nodes() {
        let stmt = make_stmt_with_cache_dir(Path::new("/tmp/unused"));
        let _ = stmt.reverse_deps.set(HashMap::from([(
            "A".to_string(),
            HashSet::from(["B".into()]),
        )]));

        stmt.invalidate_downstreams("A");

        assert!(stmt.invalidated_nodes.contains_sync(&"B".to_string()));
        assert!(
            !stmt.invalidated_nodes.contains_sync(&"X".to_string()),
            "unrelated node should not be marked"
        );
    }

    #[test]
    fn test_invalidated_node_clears_own_cache_dir() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();

        std::fs::create_dir_all(root.join("B")).unwrap();
        assert!(root.join("B").exists());

        let stmt = make_stmt_with_cache_dir(root);
        let _ = stmt.invalidated_nodes.insert_sync("B".to_string());

        assert!(stmt.invalidated_nodes.contains_sync(&"B".to_string()));

        // Simulate what execute() does when it sees the node in the set
        if stmt
            .invalidated_nodes
            .remove_sync(&"B".to_string())
            .is_some()
        {
            let cache_dir = stmt.query_cache_config.root_path.join("B");
            let _ = std::fs::remove_dir_all(&cache_dir);
        }

        assert!(!root.join("B").exists(), "cache dir should be removed");
        assert!(!stmt.invalidated_nodes.contains_sync(&"B".to_string()));
    }

    #[test]
    fn test_set_reverse_deps_via_trait() {
        let cache = QueryCacheImpl::new(QueryCacheConfig::new(
            PathBuf::from("/tmp/test"),
            None,
            vec![],
        ));
        let mut deps = HashMap::new();
        deps.insert("A".to_string(), HashSet::from(["B".into()]));
        cache.set_reverse_deps(deps.clone());

        assert_eq!(*cache.reverse_deps.get().unwrap(), deps);
    }

    #[test]
    fn test_fetch_false_bypasses_cache() {
        use dbt_adapter_core::DBT_EXECUTION_PHASE_RENDER;

        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let phases = vec![DBT_EXECUTION_PHASE_RENDER];
        let config = Arc::new(QueryCacheConfig::new(root.to_path_buf(), None, phases));

        let mut stmt = QueryCacheStatement {
            query_cache_config: config,
            counters: Arc::new(SccHashMap::new()),
            reverse_deps: Arc::new(OnceLock::new()),
            invalidated_nodes: Arc::new(SccHashSet::new()),
            inner_stmt: Box::new(NoopStatement),
            node_id: Some("model.test".to_string()),
            execution_phase: DBT_EXECUTION_PHASE_RENDER,
            fetch: false,
            sql: "CREATE TABLE t (id INT)".to_string(),
        };

        stmt.execute().unwrap();

        let cache_dir = root.join("model.test");
        assert!(
            !cache_dir.exists(),
            "fetch=false should bypass caching entirely; no cache dir should be created"
        );
    }

    #[test]
    fn test_fetch_true_uses_cache() {
        use dbt_adapter_core::DBT_EXECUTION_PHASE_RENDER;

        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let phases = vec![DBT_EXECUTION_PHASE_RENDER];
        let config = Arc::new(QueryCacheConfig::new(root.to_path_buf(), None, phases));

        let mut stmt = QueryCacheStatement {
            query_cache_config: config,
            counters: Arc::new(SccHashMap::new()),
            reverse_deps: Arc::new(OnceLock::new()),
            invalidated_nodes: Arc::new(SccHashSet::new()),
            inner_stmt: Box::new(NoopStatement),
            node_id: Some("model.test".to_string()),
            execution_phase: DBT_EXECUTION_PHASE_RENDER,
            fetch: true,
            sql: "SELECT 1".to_string(),
        };

        stmt.execute().unwrap();

        let cache_dir = root.join("model.test");
        assert!(
            cache_dir.exists(),
            "fetch=true should write to cache; cache dir should exist"
        );
    }
}
