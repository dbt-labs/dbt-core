use crate::TrackedStatement;
use crate::auth::Auth;
use crate::config::AdapterConfig;
use crate::errors::{AdapterError, AdapterErrorKind, AdapterResult};

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow_schema::Schema;
use core::result::Result;
use dbt_common::cancellation::{Cancellable, CancellationToken};
use dbt_common::constants::EXECUTING;
use dbt_xdbc::semaphore::Semaphore;
use dbt_xdbc::{Connection, Database, QueryCtx, connection, database, driver};
use log;
use serde_json::json;
use tracy_client::span;

use std::collections::HashMap;
use std::fmt::Write;
use std::hash::{BuildHasher, Hasher};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use std::{thread, time::Duration};

use super::record_and_replay::{RecordEngine, ReplayEngine};

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
    /// Global CLI cancellation token
    cancellation_token: CancellationToken,
}

impl ActualEngine {
    pub fn new(auth: Arc<dyn Auth>, config: AdapterConfig, token: CancellationToken) -> Self {
        let threads = config
            .get_str("threads")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or_default();

        let permits = if threads > 0 { threads } else { u32::MAX };
        Self {
            auth,
            config,
            configured_databases: RwLock::new(DatabaseMap::default()),
            semaphore: Arc::new(Semaphore::new(permits)),
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

    fn new_connection(&self) -> AdapterResult<Box<dyn Connection>> {
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
}

impl SqlEngine {
    /// Create a new [`SqlEngine::Warehouse`] based on the given configuration.
    pub fn new(auth: Arc<dyn Auth>, config: AdapterConfig, token: CancellationToken) -> Arc<Self> {
        let engine = ActualEngine::new(auth, config, token);
        Arc::new(SqlEngine::Warehouse(Arc::new(engine)))
    }

    /// Create a new [`SqlEngine::Replay`] based on the given path and adapter type.
    pub fn new_for_replaying(
        path: PathBuf,
        config: AdapterConfig,
        token: CancellationToken,
    ) -> Arc<Self> {
        let engine = ReplayEngine::new(path, config, token);
        Arc::new(SqlEngine::Replay(engine))
    }

    /// Create a new [`SqlEngine::Record`] wrapping the given engine.
    pub fn new_for_recording(path: PathBuf, engine: Arc<SqlEngine>) -> Arc<Self> {
        let engine = RecordEngine::new(path, engine);
        Arc::new(SqlEngine::Record(engine))
    }

    /// Create a new connection to the warehouse.
    pub fn new_connection_with_config(
        &self,
        config: &AdapterConfig,
    ) -> AdapterResult<Box<dyn Connection>> {
        let _span = span!("ActualEngine::new_connection");
        let conn = match &self {
            Self::Warehouse(actual_engine) => actual_engine.new_connection_with_config(config),
            Self::Record(record_engine) => record_engine.new_connection(),
            Self::Replay(replay_engine) => replay_engine.new_connection(),
        }?;
        Ok(conn)
    }

    /// Create a new connection to the warehouse.
    pub fn new_connection(&self) -> AdapterResult<Box<dyn Connection>> {
        match &self {
            Self::Warehouse(actual_engine) => actual_engine.new_connection(),
            Self::Record(record_engine) => record_engine.new_connection(),
            Self::Replay(replay_engine) => replay_engine.new_connection(),
        }
    }

    /// Execute the given SQL query or statement.
    pub fn execute(
        &self,
        conn: &'_ mut dyn Connection,
        query_ctx: &QueryCtx,
    ) -> AdapterResult<RecordBatch> {
        self.execute_with_options(query_ctx, conn, &HashMap::new())
    }

    /// Execute the given SQL query or statement.
    pub fn execute_with_options(
        &self,
        query_ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        options: &HashMap<String, String>,
    ) -> AdapterResult<RecordBatch> {
        assert!(query_ctx.sql().is_some() || !options.is_empty());
        log_query(query_ctx);

        let token = self.cancellation_token();
        let do_execute = |conn: &'_ mut dyn Connection| -> Result<
            (Arc<Schema>, Vec<RecordBatch>),
            Cancellable<adbc_core::error::Error>,
        > {
            use dbt_xdbc::statement::Statement as _;

            let mut stmt = conn.new_statement()?;
            stmt.set_sql_query(query_ctx)?;

            for (key, value) in options {
                stmt.set_option(
                    adbc_core::options::OptionStatement::Other(key.clone()),
                    adbc_core::options::OptionValue::String(value.clone()),
                )?;
            }

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

    /// Get the configured database name. Used by
    /// adapter.verify_database to check if the database is valid.
    pub fn get_configured_database_name(&self) -> AdapterResult<Option<String>> {
        self.config("database")
    }

    /// Get a config value by key
    ///
    /// ## Returns
    /// always is Ok(None) for non Warehouse/Record variance
    pub fn config(&self, key: &str) -> AdapterResult<Option<String>> {
        match self {
            Self::Warehouse(actual_engine) => {
                let opt = actual_engine.config.maybe_get_str(key)?;
                Ok(opt)
            }
            Self::Record(record_engine) => record_engine.config(key),
            Self::Replay(replay_engine) => replay_engine.config(key),
        }
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        match self {
            Self::Warehouse(actual_engine) => actual_engine.cancellation_token(),
            Self::Record(record_engine) => record_engine.cancellation_token(),
            Self::Replay(replay_engine) => replay_engine.cancellation_token(),
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
) -> AdapterResult<RecordBatch> {
    let mut attempt = 0;
    let mut last_error = None;
    while attempt < retry_limit {
        match engine.execute(conn, query_ctx) {
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

/// Format query context as we want to see it in a log file.
fn log_query(query_ctx: &QueryCtx) {
    let mut buf = String::new();

    writeln!(&mut buf, "-- created_at: {}", query_ctx.created_at_as_str()).unwrap();
    writeln!(&mut buf, "-- dialect: {}", query_ctx.adapter_type()).unwrap();

    let node_id = match query_ctx.node_id() {
        Some(id) => id,
        None => "not available".to_string(),
    };
    writeln!(&mut buf, "-- node_id: {node_id}").unwrap();

    match query_ctx.desc() {
        Some(desc) => writeln!(&mut buf, "-- desc: {desc}").unwrap(),
        None => writeln!(&mut buf, "-- desc: not provided").unwrap(),
    }

    if let Some(sql) = query_ctx.sql() {
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
