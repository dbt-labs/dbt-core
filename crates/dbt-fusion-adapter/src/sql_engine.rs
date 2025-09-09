use crate::TrackedStatement;
use crate::auth::Auth;
use crate::base_adapter::{AdapterFactory, backend_of};
use crate::config::AdapterConfig;
use crate::errors::{AdapterError, AdapterErrorKind, AdapterResult};
use crate::stmt_splitter::StmtSplitter;

use adbc_core::options::{OptionStatement, OptionValue};
use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow_schema::Schema;
use core::result::Result;
use dbt_common::adapter::AdapterType;
use dbt_common::cancellation::{Cancellable, CancellationToken, never_cancels};
use dbt_common::constants::EXECUTING;
use dbt_frontend_common::dialect::Dialect;
use dbt_xdbc::semaphore::Semaphore;
use dbt_xdbc::{Backend, Connection, Database, QueryCtx, connection, database, driver};
use log;
use serde_json::json;
use std::borrow::Cow;
use tracy_client::span;

use std::collections::HashMap;
use std::fmt::Write;
use std::hash::{BuildHasher, Hasher};
use std::path::PathBuf;
use std::sync::RwLock;
use std::sync::{Arc, LazyLock};
use std::{thread, time::Duration};

use super::record_and_replay::{RecordEngine, ReplayEngine};

type Options = Vec<(String, OptionValue)>;

/// Naive statement splitter used in the MockAdapter
///
/// IMPORTANT: not suitable for production use.
/// TODO: remove when the full stmt splitter is available to this crate.
static NAIVE_STMT_SPLITTER: LazyLock<Arc<dyn StmtSplitter>> =
    LazyLock::new(|| Arc::new(crate::stmt_splitter::NaiveStmtSplitter));

#[derive(Default)]
struct IdentityHasher {
    hash: u64,
    #[cfg(debug_assertions)]
    unexpected_call: bool,
}
impl Hasher for IdentityHasher {
    fn write(&mut self, _bytes: &[u8]) {
        #[cfg(debug_assertions)]
        {
            self.unexpected_call = true;
        }
    }
    fn write_u64(&mut self, i: u64) {
        self.hash = i;
    }
    fn finish(&self) -> u64 {
        #[cfg(debug_assertions)]
        {
            debug_assert!(!self.unexpected_call);
        }
        self.hash
    }
}

#[derive(Default)]
struct IdentityBuildHasher;
impl BuildHasher for IdentityBuildHasher {
    type Hasher = IdentityHasher;
    fn build_hasher(&self) -> Self::Hasher {
        IdentityHasher::default()
    }
}

#[derive(Default)]
pub struct DatabaseMap {
    inner: HashMap<database::Fingerprint, Box<dyn Database>, IdentityBuildHasher>,
}

pub struct ActualEngine {
    /// Auth configurator
    auth: Arc<dyn Auth>,
    /// Configuration
    config: AdapterConfig,
    /// Lazily initialized databases
    configured_databases: RwLock<DatabaseMap>,
    /// Semaphore for limiting the number of concurrent connections
    semaphore: Arc<Semaphore>,
    /// Adapter factory for creating relations and columns
    adapter_factory: Arc<dyn AdapterFactory>,
    /// Statement splitter
    splitter: Arc<dyn StmtSplitter>,
    /// Global CLI cancellation token
    cancellation_token: CancellationToken,
}

impl ActualEngine {
    pub fn new(
        auth: Arc<dyn Auth>,
        config: AdapterConfig,
        adapter_factory: Arc<dyn AdapterFactory>,
        splitter: Arc<dyn StmtSplitter>,
        token: CancellationToken,
    ) -> Self {
        let threads = config
            .get("threads")
            .and_then(|t| {
                let u = t.as_u64();
                debug_assert!(u.is_some(), "threads must be an integer if specified");
                u
            })
            .map(|t| t as u32)
            .unwrap_or(0u32);

        let permits = if threads > 0 { threads } else { u32::MAX };
        Self {
            auth,
            config,
            configured_databases: RwLock::new(DatabaseMap::default()),
            semaphore: Arc::new(Semaphore::new(permits)),
            adapter_factory,
            splitter,
            cancellation_token: token,
        }
    }

    fn load_driver_and_configure_database(
        &self,
        config: &AdapterConfig,
    ) -> AdapterResult<Box<dyn Database>> {
        // Delegate the configuration of the database::Builder to the Auth implementation.
        let builder = self.auth.configure(config)?;

        // The driver is loaded only once even if this runs multiple times.
        let mut driver = driver::Builder::new(self.auth.backend())
            .with_semaphore(self.semaphore.clone())
            .try_load()?;

        // builder.with_named_option(
        //     snowflake::LOG_TRACING,
        //     database::LogLevel::Debug.to_string(),
        // )?;
        // ... other configuration steps can be added here...

        // The database is configured only once even if this runs multiple times,
        // unless a different configuration is provided.
        let opts = builder.into_iter().collect::<Vec<_>>();
        let fingerprint = database::Builder::fingerprint(opts.iter());
        {
            let read_guard = self.configured_databases.read().unwrap();
            if let Some(database) = read_guard.inner.get(&fingerprint) {
                return Ok(database.clone());
            }
        }
        {
            let mut write_guard = self.configured_databases.write().unwrap();
            if let Some(database) = write_guard.inner.get(&fingerprint) {
                let database: Box<dyn Database> = database.clone();
                Ok(database)
            } else {
                let database = driver.new_database_with_opts(opts)?;
                write_guard.inner.insert(fingerprint, database.clone());
                Ok(database)
            }
        }
    }

    fn new_connection_with_config(
        &self,
        config: &AdapterConfig,
    ) -> AdapterResult<Box<dyn Connection>> {
        let mut database = self.load_driver_and_configure_database(config)?;
        let connection_builder = connection::Builder::default();
        let conn = connection_builder.build(&mut database)?;
        Ok(conn)
    }

    fn new_connection(&self, _node_id: Option<String>) -> AdapterResult<Box<dyn Connection>> {
        // TODO(felipecrv): Make this codepath more efficient
        // (no need to reconfigure the default database)
        self.new_connection_with_config(&self.config)
    }

    fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }
}

/// A simple bridge between adapters and the drivers.
#[derive(Clone)]
pub enum SqlEngine {
    /// Actual engine
    Warehouse(Arc<ActualEngine>),
    /// Engine used for recording db interaction; recording engine is
    /// a wrapper around an actual engine
    Record(RecordEngine),
    /// Engine used for replaying db interaction
    Replay(ReplayEngine),
    /// Mock engine for the MockAdapter
    Mock(AdapterType),
}

impl SqlEngine {
    /// Create a new [`SqlEngine::Warehouse`] based on the given configuration.
    pub fn new(
        auth: Arc<dyn Auth>,
        config: AdapterConfig,
        adapter_factory: Arc<dyn AdapterFactory>,
        stmt_splitter: Arc<dyn StmtSplitter>,
        token: CancellationToken,
    ) -> Arc<Self> {
        let engine = ActualEngine::new(auth, config, adapter_factory, stmt_splitter, token);
        Arc::new(SqlEngine::Warehouse(Arc::new(engine)))
    }

    /// Create a new [`SqlEngine::Replay`] based on the given path and adapter type.
    pub fn new_for_replaying(
        backend: Backend,
        path: PathBuf,
        config: AdapterConfig,
        adapter_factory: Arc<dyn AdapterFactory>,
        stmt_splitter: Arc<dyn StmtSplitter>,
        token: CancellationToken,
    ) -> Arc<Self> {
        let engine =
            ReplayEngine::new(backend, path, config, adapter_factory, stmt_splitter, token);
        Arc::new(SqlEngine::Replay(engine))
    }

    /// Create a new [`SqlEngine::Record`] wrapping the given engine.
    pub fn new_for_recording(path: PathBuf, engine: Arc<SqlEngine>) -> Arc<Self> {
        let engine = RecordEngine::new(path, engine);
        Arc::new(SqlEngine::Record(engine))
    }

    /// Get the statement splitter for this engine
    pub fn splitter(&self) -> &dyn StmtSplitter {
        match self {
            SqlEngine::Warehouse(engine) => engine.splitter.as_ref(),
            SqlEngine::Record(engine) => engine.splitter(),
            SqlEngine::Replay(engine) => engine.splitter(),
            SqlEngine::Mock(_) => NAIVE_STMT_SPLITTER.as_ref(),
        }
    }

    /// Split SQL statements using the provided dialect
    ///
    /// This method handles the splitting of SQL statements based on the dialect's rules.
    /// The dialect must be provided by the caller since the mapping from Backend to
    /// AdapterType/Dialect is not always deterministic (e.g., Generic backend,
    /// shared drivers like Postgres/Redshift).
    pub fn split_statements(&self, sql: &str, dialect: Dialect) -> Vec<String> {
        self.splitter().split(sql, dialect)
    }

    /// Create a new connection to the warehouse.
    pub fn new_connection_with_config(
        &self,
        config: &AdapterConfig,
    ) -> AdapterResult<Box<dyn Connection>> {
        let _span = span!("ActualEngine::new_connection");
        let conn = match &self {
            Self::Warehouse(actual_engine) => actual_engine.new_connection_with_config(config),
            Self::Record(record_engine) => record_engine.new_connection(None),
            Self::Replay(replay_engine) => replay_engine.new_connection(None),
            Self::Mock(_) => {
                unreachable!("Mock engine does not support new_connection_with_config")
            }
        }?;
        Ok(conn)
    }

    pub fn backend(&self) -> Backend {
        match self {
            SqlEngine::Warehouse(actual_engine) => actual_engine.auth.backend(),
            SqlEngine::Record(record_engine) => record_engine.backend(),
            SqlEngine::Replay(replay_engine) => replay_engine.backend(),
            SqlEngine::Mock(adapter_type) => backend_of(*adapter_type),
        }
    }

    /// Used to create columns after the adapter that owns this engine is created.
    pub fn adapter_factory(&self) -> &dyn AdapterFactory {
        match self {
            SqlEngine::Warehouse(actual_engine) => actual_engine.adapter_factory.as_ref(),
            SqlEngine::Record(record_engine) => record_engine.adapter_factory(),
            SqlEngine::Replay(replay_engine) => replay_engine.adapter_factory(),
            SqlEngine::Mock(_) => unreachable!("Mock engine does not support adapter_factory"),
        }
    }

    /// Create a new connection to the warehouse.
    pub fn new_connection(&self, node_id: Option<String>) -> AdapterResult<Box<dyn Connection>> {
        match &self {
            Self::Warehouse(actual_engine) => actual_engine.new_connection(node_id),
            Self::Record(record_engine) => record_engine.new_connection(node_id),
            Self::Replay(replay_engine) => replay_engine.new_connection(node_id),
            Self::Mock(_) => unreachable!("Mock engine does not support new_connection"),
        }
    }

    /// Execute the given SQL query or statement.
    pub fn execute(
        &self,
        conn: &'_ mut dyn Connection,
        query_ctx: &QueryCtx,
    ) -> AdapterResult<RecordBatch> {
        self.execute_with_options(query_ctx, conn, Options::new())
    }

    /// Execute the given SQL query or statement.
    pub fn execute_with_options(
        &self,
        query_ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        options: Options,
    ) -> AdapterResult<RecordBatch> {
        assert!(query_ctx.sql().is_some() || !options.is_empty());
        Self::log_query_ctx_for_execution(query_ctx);

        let token = self.cancellation_token();
        let do_execute = |conn: &'_ mut dyn Connection| -> Result<
            (Arc<Schema>, Vec<RecordBatch>),
            Cancellable<adbc_core::error::Error>,
        > {
            use dbt_xdbc::statement::Statement as _;

            let mut stmt = conn.new_statement()?;
            stmt.set_sql_query(query_ctx)?;

            options
                .into_iter()
                .try_for_each(|(key, value)| stmt.set_option(OptionStatement::Other(key), value))?;

            // Make sure we don't create more statements after global cancellation.
            token.check_cancellation()?;

            // Track the statement so execution can be cancelled
            // when the user Ctrl-C's the process.
            let mut stmt = TrackedStatement::new(stmt);

            let reader = stmt.execute()?;
            let schema = reader.schema();
            let mut batches = Vec::with_capacity(1);
            for res in reader {
                let batch = res.map_err(adbc_core::error::Error::from)?;
                batches.push(batch);
                // Check for cancellation before processing the next batch
                // or concatenating the batches produced so far.
                token.check_cancellation()?;
            }
            Ok((schema, batches))
        };
        let _span = span!("SqlEngine::execute");
        let (schema, batches) = match do_execute(conn) {
            Ok(res) => res,
            Err(Cancellable::Cancelled) => {
                let e = AdapterError::new(
                    AdapterErrorKind::Cancelled,
                    "SQL statement execution was cancelled",
                );
                return Err(e);
            }
            Err(Cancellable::Error(e)) => return Err(e.into()),
        };
        let total_batch = concat_batches(&schema, &batches)?;
        Ok(total_batch)
    }

    /// Format query context as we want to see it in a log file and log it in query_log
    pub fn log_query_ctx_for_execution(ctx: &QueryCtx) {
        let mut buf = String::new();

        writeln!(&mut buf, "-- created_at: {}", ctx.created_at_as_str()).unwrap();
        writeln!(&mut buf, "-- dialect: {}", ctx.adapter_type()).unwrap();

        let node_id = match ctx.node_id() {
            Some(id) => id,
            None => "not available".to_string(),
        };
        writeln!(&mut buf, "-- node_id: {node_id}").unwrap();

        match ctx.desc() {
            Some(desc) => writeln!(&mut buf, "-- desc: {desc}").unwrap(),
            None => writeln!(&mut buf, "-- desc: not provided").unwrap(),
        }

        if let Some(sql) = ctx.sql() {
            write!(&mut buf, "{sql}").unwrap();
            if !sql.ends_with(";") {
                write!(&mut buf, ";").unwrap();
            }
        }

        if node_id != "not available" {
            log::debug!(target: EXECUTING, name = "SQLQuery", data:serde = json!({ "node_info": { "unique_id": node_id } }); "{buf}");
        } else {
            log::debug!(target: EXECUTING, name = "SQLQuery"; "{buf}");
        }
    }

    /// Get the configured database name. Used by
    /// adapter.verify_database to check if the database is valid.
    pub fn get_configured_database_name(&self) -> Option<Cow<'_, str>> {
        self.config("database")
    }

    /// Get a config value by key
    ///
    /// ## Returns
    /// always is Ok(None) for non Warehouse/Record variance
    pub fn config(&self, key: &str) -> Option<Cow<'_, str>> {
        match self {
            Self::Warehouse(actual_engine) => actual_engine.config.get_string(key),
            Self::Record(record_engine) => record_engine.config(key),
            Self::Replay(replay_engine) => replay_engine.config(key),
            Self::Mock(_) => None,
        }
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        match self {
            Self::Warehouse(actual_engine) => actual_engine.cancellation_token(),
            Self::Record(record_engine) => record_engine.cancellation_token(),
            Self::Replay(replay_engine) => replay_engine.cancellation_token(),
            Self::Mock(_) => never_cancels(),
        }
    }
}

/// Execute query and retry in case of an error. Retry is done (up to
/// the given limit) regardless of the error encountered.
///
/// https://github.com/dbt-labs/dbt-adapters/blob/996a302fa9107369eb30d733dadfaf307023f33d/dbt-adapters/src/dbt/adapters/sql/connections.py#L84
pub fn execute_query_with_retry(
    engine: Arc<SqlEngine>,
    conn: &'_ mut dyn Connection,
    query_ctx: &QueryCtx,
    retry_limit: u32,
    options: &HashMap<String, String>,
) -> AdapterResult<RecordBatch> {
    let mut attempt = 0;
    let mut last_error = None;

    let options = options
        .iter()
        .map(|(key, value)| (key.clone(), OptionValue::String(value.clone())))
        .collect::<Options>();
    while attempt < retry_limit {
        match engine.execute_with_options(query_ctx, conn, options.clone()) {
            Ok(result) => return Ok(result),
            Err(err) => {
                last_error = Some(err.clone());
                thread::sleep(Duration::from_secs(1));
                attempt += 1;
            }
        }
    }

    if let Some(err) = last_error {
        Err(err)
    } else {
        unreachable!("last_error should not be None if we exit the loop")
    }
}

#[cfg(test)]
mod tests {
    use dbt_xdbc::QueryCtx;

    use super::SqlEngine;

    #[test]
    fn test_log_for_execution() {
        let query_ctx = QueryCtx::new("test_adapter")
            .with_node_id("test_node_123")
            .with_sql("SELECT * FROM test_table")
            .with_desc("Test query for logging");

        // Should not panic
        SqlEngine::log_query_ctx_for_execution(&query_ctx);
    }
}
