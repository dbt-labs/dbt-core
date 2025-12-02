use crate::AdapterResponse;
use crate::auth::Auth;
use crate::base_adapter::backend_of;
use crate::bigquery::adapter::ADBC_EXECUTE_INVOCATION_OPTION;
use crate::config::AdapterConfig;
use crate::databricks::databricks_compute_from_state;
use crate::errors::{
    AdapterError, AdapterErrorKind, AdapterResult, adbc_error_to_adapter_error,
    arrow_error_to_adapter_error,
};
use crate::query_cache::QueryCache;
use crate::query_comment::{EMPTY_CONFIG, QueryCommentConfig};
use crate::record_and_replay::{RecordEngine, ReplayEngine};
use crate::sql_types::{NaiveTypeOpsImpl, TypeOps};
use crate::statement::*;
use crate::stmt_splitter::StmtSplitter;

use adbc_core::options::{OptionStatement, OptionValue};
use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow_schema::Schema;
use core::result::Result;
use dbt_agate::hashers::IdentityBuildHasher;
use dbt_common::adapter::AdapterType;
use dbt_common::cancellation::{Cancellable, CancellationToken, never_cancels};
use dbt_common::create_debug_span;
use dbt_common::hashing::code_hash;
use dbt_common::tracing::span_info::record_current_span_status_from_attrs;
use dbt_frontend_common::dialect::Dialect;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::telemetry::{QueryExecuted, QueryOutcome};
use dbt_xdbc::bigquery::QUERY_LABELS;
use dbt_xdbc::semaphore::Semaphore;
use dbt_xdbc::{Backend, Connection, Database, QueryCtx, Statement, connection, database, driver};
use minijinja::State;
use std::borrow::Cow;
use tracy_client::span;

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::RwLock;
use std::sync::{Arc, LazyLock};
use std::{thread, time::Duration};

pub type Options = Vec<(String, OptionValue)>;

/// Naive statement splitter used in the MockAdapter
///
/// IMPORTANT: not suitable for production use.
/// TODO: remove when the full stmt splitter is available to this crate.
static NAIVE_STMT_SPLITTER: LazyLock<Arc<dyn StmtSplitter>> =
    LazyLock::new(|| Arc::new(crate::stmt_splitter::NaiveStmtSplitter));

/// Naive type parser/formatter used in the MockAdapter
///
/// IMPORTANT: not suitable for production use. DEFAULTS TO SNOWFLAKE ALSO.
/// TODO: remove when the full parser/formatter is available to this crate.
static NAIVE_TYPE_OPS: LazyLock<Box<dyn TypeOps>> =
    LazyLock::new(|| Box::new(NaiveTypeOpsImpl::new(AdapterType::Snowflake)));

#[derive(Default)]
pub struct DatabaseMap {
    inner: HashMap<database::Fingerprint, Box<dyn Database>, IdentityBuildHasher>,
}

pub struct NoopConnection;

impl Connection for NoopConnection {
    fn new_statement(&mut self) -> adbc_core::error::Result<Box<dyn Statement>> {
        unimplemented!("ADBC statement creation in mock connection")
    }

    fn cancel(&mut self) -> adbc_core::error::Result<()> {
        unimplemented!("ADBC connection cancellation in mock connection")
    }

    fn commit(&mut self) -> adbc_core::error::Result<()> {
        unimplemented!("ADBC transaction commit in mock connection")
    }

    fn rollback(&mut self) -> adbc_core::error::Result<()> {
        unimplemented!("ADBC transaction rollback in mock connection")
    }

    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: &str,
    ) -> adbc_core::error::Result<Schema> {
        unimplemented!("ADBC table schema retrieval in mock connection")
    }

    fn update_node_id(&mut self, _node_id: Option<String>) {}
}

pub struct XdbcEngine {
    adapter_type: AdapterType,
    /// Auth configurator
    auth: Arc<dyn Auth>,
    /// Configuration
    config: AdapterConfig,
    /// Lazily initialized databases
    configured_databases: RwLock<DatabaseMap>,
    /// Semaphore for limiting the number of concurrent connections
    semaphore: Arc<Semaphore>,
    /// Resolved quoting policy
    quoting: ResolvedQuoting,
    /// Statement splitter
    splitter: Arc<dyn StmtSplitter>,
    /// Query comment config
    query_comment: QueryCommentConfig,
    /// Type operations (e.g. parsing, formatting) for the dilect this engine is for
    pub type_ops: Box<dyn TypeOps>,
    /// Query cache
    query_cache: Option<Arc<dyn QueryCache>>,
    /// Global CLI cancellation token
    cancellation_token: CancellationToken,
}

impl XdbcEngine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        adapter_type: AdapterType,
        auth: Arc<dyn Auth>,
        config: AdapterConfig,
        quoting: ResolvedQuoting,
        splitter: Arc<dyn StmtSplitter>,
        query_comment: QueryCommentConfig,
        type_ops: Box<dyn TypeOps>,
        query_cache: Option<Arc<dyn QueryCache>>,
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

        let permits = if matches!(adapter_type, AdapterType::Redshift | AdapterType::Bigquery)
            && threads > 0
        {
            threads
        } else {
            u32::MAX
        };
        Self {
            adapter_type,
            auth,
            config,
            quoting,
            configured_databases: RwLock::new(DatabaseMap::default()),
            semaphore: Arc::new(Semaphore::new(permits)),
            splitter,
            type_ops,
            query_comment,
            query_cache,
            cancellation_token: token,
        }
    }

    fn adapter_type(&self) -> AdapterType {
        self.adapter_type
    }

    fn load_driver_and_configure_database(
        &self,
        config: &AdapterConfig,
    ) -> AdapterResult<Box<dyn Database>> {
        // Delegate the configuration of the database::Builder to the Auth implementation.
        let builder = self
            .auth
            .configure(config)
            .map_err(crate::errors::auth_error_to_adapter_error)?;

        // The driver is loaded only once even if this runs multiple times.
        let mut driver = driver::Builder::new(self.auth.backend())
            .with_semaphore(self.semaphore.clone())
            .try_load()
            .map_err(adbc_error_to_adapter_error)?;

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
                let database = driver
                    .new_database_with_opts(opts)
                    .map_err(adbc_error_to_adapter_error)?;
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
        let conn = match connection_builder.build(&mut database) {
            Ok(conn) => conn,
            Err(e) => return Err(adbc_error_to_adapter_error(e)),
        };
        Ok(conn)
    }

    fn new_connection(
        &self,
        state: Option<&State>,
        _node_id: Option<String>,
    ) -> AdapterResult<Box<dyn Connection>> {
        match self.adapter_type() {
            AdapterType::Databricks => {
                if let Some(databricks_compute) = state.and_then(databricks_compute_from_state) {
                    let augmented_config = {
                        let mut mapping = self.config.repr().clone();
                        mapping.insert("databricks_compute".into(), databricks_compute.into());
                        AdapterConfig::new(mapping)
                    };
                    self.new_connection_with_config(&augmented_config)
                } else {
                    self.new_connection_with_config(&self.config)
                }
            }
            _ => {
                // TODO(felipecrv): Make this codepath more efficient
                // (no need to reconfigure the default database)
                self.new_connection_with_config(&self.config)
            }
        }
    }

    fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }
}

/// A simple bridge between adapters and the drivers.
#[derive(Clone)]
pub enum AdapterEngine {
    /// Xdbc engine
    Xdbc(Arc<XdbcEngine>),
    /// Engine used for recording db interaction; recording engine is
    /// a wrapper around an actual engine
    Record(RecordEngine),
    /// Engine used for replaying db interaction
    Replay(ReplayEngine),
    /// Mock engine for the MockAdapter
    Mock(AdapterType),
}

impl AdapterEngine {
    /// Create a new [`SqlEngine::Xdbc`] based on the given configuration.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        adapter_type: AdapterType,
        auth: Arc<dyn Auth>,
        config: AdapterConfig,
        quoting: ResolvedQuoting,
        stmt_splitter: Arc<dyn StmtSplitter>,
        query_cache: Option<Arc<dyn QueryCache>>,
        query_comment: QueryCommentConfig,
        type_ops: Box<dyn TypeOps>,
        token: CancellationToken,
    ) -> Arc<Self> {
        let engine = XdbcEngine::new(
            adapter_type,
            auth,
            config,
            quoting,
            stmt_splitter,
            query_comment,
            type_ops,
            query_cache,
            token,
        );
        Arc::new(AdapterEngine::Xdbc(Arc::new(engine)))
    }

    /// Create a new [`SqlEngine::Replay`] based on the given path and adapter type.
    #[allow(clippy::too_many_arguments)]
    pub fn new_for_replaying(
        adapter_type: AdapterType,
        path: PathBuf,
        config: AdapterConfig,
        quoting: ResolvedQuoting,
        stmt_splitter: Arc<dyn StmtSplitter>,
        query_comment: QueryCommentConfig,
        type_ops: Box<dyn TypeOps>,
        token: CancellationToken,
    ) -> Arc<Self> {
        let engine = ReplayEngine::new(
            adapter_type,
            path,
            config,
            quoting,
            stmt_splitter,
            query_comment,
            type_ops,
            token,
        );
        Arc::new(AdapterEngine::Replay(engine))
    }

    /// Create a new [`SqlEngine::Record`] wrapping the given engine.
    pub fn new_for_recording(path: PathBuf, engine: Arc<AdapterEngine>) -> Arc<Self> {
        let engine = RecordEngine::new(path, engine);
        Arc::new(AdapterEngine::Record(engine))
    }

    pub fn is_mock(&self) -> bool {
        matches!(self, AdapterEngine::Mock(_))
    }

    pub fn quoting(&self) -> ResolvedQuoting {
        match self {
            AdapterEngine::Xdbc(engine) => engine.quoting,
            AdapterEngine::Record(engine) => engine.quoting(),
            AdapterEngine::Replay(engine) => engine.quoting(),
            AdapterEngine::Mock(_) => ResolvedQuoting::default(),
        }
    }

    /// Get the statement splitter for this engine
    pub fn splitter(&self) -> &dyn StmtSplitter {
        match self {
            AdapterEngine::Xdbc(engine) => engine.splitter.as_ref(),
            AdapterEngine::Record(engine) => engine.splitter(),
            AdapterEngine::Replay(engine) => engine.splitter(),
            AdapterEngine::Mock(_) => NAIVE_STMT_SPLITTER.as_ref(),
        }
    }

    pub fn type_ops(&self) -> &dyn TypeOps {
        match self {
            AdapterEngine::Xdbc(engine) => engine.type_ops.as_ref(),
            AdapterEngine::Record(engine) => engine.type_ops(),
            AdapterEngine::Replay(engine) => engine.type_ops(),
            AdapterEngine::Mock(_adapter_type) => NAIVE_TYPE_OPS.as_ref(),
        }
    }

    /// Split SQL statements using the provided dialect
    ///
    /// This method handles the splitting of SQL statements based on the dialect's rules.
    /// The dialect must be provided by the caller since the mapping from Backend to
    /// AdapterType/Dialect is not always deterministic (e.g., Generic backend,
    /// shared drivers like Postgres/Redshift).
    pub fn split_and_filter_statements(&self, sql: &str, dialect: Dialect) -> Vec<String> {
        self.splitter()
            .split(sql, dialect)
            .into_iter()
            .filter(|statement| !self.splitter().is_empty(statement, dialect))
            .collect()
    }

    /// Get the query comment config for this engine
    pub fn query_comment(&self) -> &QueryCommentConfig {
        match self {
            AdapterEngine::Xdbc(engine) => &engine.query_comment,
            AdapterEngine::Record(engine) => engine.query_comment(),
            AdapterEngine::Replay(engine) => engine.query_comment(),
            AdapterEngine::Mock(_) => &EMPTY_CONFIG,
        }
    }

    /// Create a new connection to the warehouse.
    pub fn new_connection_with_config(
        &self,
        config: &AdapterConfig,
    ) -> AdapterResult<Box<dyn Connection>> {
        let _span = span!("ActualEngine::new_connection");
        let conn = match &self {
            Self::Xdbc(actual_engine) => actual_engine.new_connection_with_config(config),
            // TODO: the record and replay engines should have a new_connection_with_config()
            // method instead of a new_connection method
            Self::Record(record_engine) => record_engine.new_connection(None, None),
            Self::Replay(replay_engine) => replay_engine.new_connection(None, None),
            Self::Mock(_) => Ok(Box::new(NoopConnection) as Box<dyn Connection>),
        }?;
        Ok(conn)
    }

    /// Get the adapter type for this engine
    pub fn adapter_type(&self) -> AdapterType {
        match self {
            AdapterEngine::Xdbc(actual_engine) => actual_engine.adapter_type(),
            AdapterEngine::Record(record_engine) => record_engine.adapter_type(),
            AdapterEngine::Replay(replay_engine) => replay_engine.adapter_type(),
            AdapterEngine::Mock(adapter_type) => *adapter_type,
        }
    }

    pub fn backend(&self) -> Backend {
        match self {
            AdapterEngine::Xdbc(actual_engine) => actual_engine.auth.backend(),
            AdapterEngine::Record(record_engine) => record_engine.backend(),
            AdapterEngine::Replay(replay_engine) => replay_engine.backend(),
            AdapterEngine::Mock(adapter_type) => backend_of(*adapter_type),
        }
    }

    /// Create a new connection to the warehouse.
    pub fn new_connection(
        &self,
        state: Option<&State>,
        node_id: Option<String>,
    ) -> AdapterResult<Box<dyn Connection>> {
        match &self {
            Self::Xdbc(actual_engine) => actual_engine.new_connection(state, node_id),
            Self::Record(record_engine) => record_engine.new_connection(state, node_id),
            Self::Replay(replay_engine) => replay_engine.new_connection(state, node_id),
            Self::Mock(_) => Ok(Box::new(NoopConnection)),
        }
    }

    /// Execute the given SQL query or statement.
    pub fn execute(
        &self,
        state: Option<&State>,
        conn: &'_ mut dyn Connection,
        ctx: &QueryCtx,
        sql: &str,
    ) -> AdapterResult<RecordBatch> {
        self.execute_with_options(state, ctx, conn, sql, Options::new(), true)
    }

    /// Execute the given SQL query or statement.
    pub fn execute_with_options(
        &self,
        state: Option<&State>,
        ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        sql: &str,
        options: Options,
        fetch: bool,
    ) -> AdapterResult<RecordBatch> {
        assert!(!sql.is_empty() || !options.is_empty());

        let maybe_query_comment = state
            .map(|s| self.query_comment().resolve_comment(s))
            .transpose()?;

        let sql = match &maybe_query_comment {
            Some(comment) => {
                let sql = self.query_comment().add_comment(sql, comment);
                Cow::Owned(sql)
            }
            None => Cow::Borrowed(sql),
        };

        let mut options = options;
        if let Some(state) = state
            && self.adapter_type() == AdapterType::Bigquery
        {
            let mut job_labels =
                maybe_query_comment
                    .as_ref()
                    .map_or_else(BTreeMap::new, |comment| {
                        self.query_comment()
                            .get_job_labels_from_query_comment(comment)
                    });
            if let Some(invocation_id_label) = state
                .lookup("invocation_id")
                .and_then(|value| value.as_str().map(|label| label.to_owned()))
            {
                job_labels.insert(
                    ADBC_EXECUTE_INVOCATION_OPTION.to_owned(),
                    invocation_id_label,
                );
            }

            let job_label_option =
                serde_json::to_string(&job_labels).expect("Should be able to serialize job labels");
            options.push((
                QUERY_LABELS.to_owned(),
                OptionValue::String(job_label_option),
            ));
        }

        let token = self.cancellation_token();
        let do_execute = |conn: &'_ mut dyn Connection| -> Result<
            (Arc<Schema>, Vec<RecordBatch>),
            Cancellable<adbc_core::error::Error>,
        > {
            use dbt_xdbc::statement::Statement as _;

            let mut stmt = match self.query_cache() {
                Some(query_cache) => {
                    let inner_stmt = conn.new_statement()?;
                    query_cache.new_statement(inner_stmt)
                }
                None => conn.new_statement()?,
            };
            if let Some(node_id) = ctx.node_id() {
                stmt.set_option(
                    OptionStatement::Other(DBT_NODE_ID.to_string()),
                    OptionValue::String(node_id.clone()),
                )?;
            }
            if let Some(p) = ctx.phase() {
                stmt.set_option(
                    OptionStatement::Other(DBT_EXECUTION_PHASE.to_string()),
                    OptionValue::String(p.to_string()),
                )?;
            }
            options
                .into_iter()
                .try_for_each(|(key, value)| stmt.set_option(OptionStatement::Other(key), value))?;
            stmt.set_sql_query(sql.as_ref())?;

            // Make sure we don't create more statements after global cancellation.
            token.check_cancellation()?;

            // Track the statement so execution can be cancelled
            // when the user Ctrl-C's the process.
            let mut stmt = TrackedStatement::new(stmt);

            let reader = stmt.execute()?;
            let schema = reader.schema();
            let mut batches = Vec::with_capacity(1);
            if !fetch {
                return Ok((schema, batches));
            }
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

        let sql_hash = code_hash(sql.as_ref());
        let adapter_type = self.adapter_type();
        let _query_span_guard = create_debug_span(QueryExecuted::start(
            sql.to_string(),
            sql_hash,
            adapter_type.as_ref().to_owned(),
            ctx.node_id().cloned(),
            ctx.desc().cloned(),
        ))
        .entered();

        let (schema, batches) = match do_execute(conn) {
            Ok(res) => res,
            Err(Cancellable::Cancelled) => {
                let e = AdapterError::new(
                    AdapterErrorKind::Cancelled,
                    "SQL statement execution was cancelled",
                );

                // TODO: wouldn't it be possible to salvage query_id if at least one batch was produced?
                record_current_span_status_from_attrs(|attrs| {
                    if let Some(attrs) = attrs.downcast_mut::<QueryExecuted>() {
                        // dbt core had different event codes for start and end of a query
                        attrs.dbt_core_event_code = "E017".to_string();
                        attrs.set_query_outcome(QueryOutcome::Canceled);
                    }
                });

                return Err(e);
            }
            Err(Cancellable::Error(e)) => {
                // TODO: wouldn't it be possible to salvage query_id if at least one batch was produced?
                record_current_span_status_from_attrs(|attrs| {
                    if let Some(attrs) = attrs.downcast_mut::<QueryExecuted>() {
                        // dbt core had different event codes for start and end of a query
                        attrs.dbt_core_event_code = "E017".to_string();
                        attrs.set_query_outcome(QueryOutcome::Error);
                        attrs.query_error_adapter_message =
                            Some(format!("{:?}: {}", e.status, e.message));
                        attrs.query_error_vendor_code = Some(e.vendor_code);
                    }
                });

                return Err(adbc_error_to_adapter_error(e));
            }
        };
        let total_batch =
            concat_batches(&schema, &batches).map_err(arrow_error_to_adapter_error)?;

        record_current_span_status_from_attrs(|attrs| {
            if let Some(attrs) = attrs.downcast_mut::<QueryExecuted>() {
                // dbt core had different event codes for start and end of a query
                attrs.dbt_core_event_code = "E017".to_string();
                attrs.set_query_outcome(QueryOutcome::Success);
                attrs.query_id = AdapterResponse::query_id(&total_batch, adapter_type)
            }
        });

        Ok(total_batch)
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
            Self::Xdbc(actual_engine) => actual_engine.config.get_string(key),
            Self::Record(record_engine) => record_engine.config(key),
            Self::Replay(replay_engine) => replay_engine.config(key),
            Self::Mock(_) => None,
        }
    }

    // Get full config object
    pub fn get_config(&self) -> &AdapterConfig {
        match self {
            Self::Xdbc(actual_engine) => &actual_engine.config,
            Self::Record(record_engine) => record_engine.get_config(),
            Self::Replay(replay_engine) => replay_engine.get_config(),
            Self::Mock(_) => unreachable!("Mock engine does not support get_config"),
        }
    }

    // Get query cache
    pub fn query_cache(&self) -> Option<&Arc<dyn QueryCache>> {
        match self {
            Self::Xdbc(actual_engine) => actual_engine.query_cache.as_ref(),
            Self::Record(_record_engine) => None,
            Self::Replay(_replay_engine) => None,
            Self::Mock(_) => None,
        }
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        match self {
            Self::Xdbc(actual_engine) => actual_engine.cancellation_token(),
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
#[allow(clippy::too_many_arguments)]
pub fn execute_query_with_retry(
    engine: Arc<AdapterEngine>,
    state: Option<&State>,
    conn: &'_ mut dyn Connection,
    ctx: &QueryCtx,
    sql: &str,
    retry_limit: u32,
    options: &Options,
    fetch: bool,
) -> AdapterResult<RecordBatch> {
    let mut attempt = 0;
    let mut last_error = None;

    while attempt < retry_limit {
        match engine.execute_with_options(state, ctx, conn, sql, options.clone(), fetch) {
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
