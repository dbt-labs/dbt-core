use crate::AdapterResponse;
use crate::auth::Auth;
use crate::base_adapter::backend_of;
use crate::cache::RelationCache;
use crate::config::AdapterConfig;
use crate::errors::{
    AdapterError, AdapterErrorKind, AdapterResult, adbc_error_to_adapter_error,
    arrow_error_to_adapter_error,
};
use crate::query_cache::QueryCache;
use crate::query_comment::{EMPTY_CONFIG, QueryCommentConfig};
use crate::record_and_replay::{RecordEngine, ReplayEngine};
use crate::sidecar_client::SidecarClient;
use crate::sql_types::TypeOps;
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
use dbt_schemas::schemas::{DbtModel, DbtSnapshot};
use dbt_xdbc::bigquery::QUERY_LABELS;
use dbt_xdbc::semaphore::Semaphore;
use dbt_xdbc::{Backend, Connection, Database, QueryCtx, Statement, connection, database, driver};
use minijinja::State;
use serde::Deserialize;
use std::borrow::Cow;
use tracy_client::span;

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use std::{thread, time::Duration};

pub type Options = Vec<(String, OptionValue)>;

#[derive(Default)]
pub struct DatabaseMap {
    inner: HashMap<database::Fingerprint, Box<dyn Database>, IdentityBuildHasher>,
}

pub struct NoopConnection;

impl Connection for NoopConnection {
    fn new_statement(&mut self) -> adbc_core::error::Result<Box<dyn Statement>> {
        // Return an error instead of panicking so callers can handle gracefully
        Err(adbc_core::error::Error::with_message_and_status(
            "NoopConnection does not support statement creation",
            adbc_core::error::Status::NotImplemented,
        ))
    }

    fn cancel(&mut self) -> adbc_core::error::Result<()> {
        // No-op for cancel - nothing to cancel
        Ok(())
    }

    fn commit(&mut self) -> adbc_core::error::Result<()> {
        // No-op for commit - no transaction state
        Ok(())
    }

    fn rollback(&mut self) -> adbc_core::error::Result<()> {
        // No-op for rollback - no transaction state
        Ok(())
    }

    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: &str,
    ) -> adbc_core::error::Result<Schema> {
        // Return an error instead of panicking
        Err(adbc_core::error::Error::with_message_and_status(
            "NoopConnection does not support table schema retrieval",
            adbc_core::error::Status::NotImplemented,
        ))
    }

    fn update_node_id(&mut self, _node_id: Option<String>) {}
}

/// A trait abstracting the layer between the adapter layer and database drivers.
///
/// Each concrete engine type (ADBC, mock, sidecar, record, replay) implements this trait
/// directly. This is the internal adapter service for other Rust modules in Fusion as
/// the adapter layer interface is forced to abide by what is expected for consumption
/// from Jinja code.
pub trait AdapterEngine: Send + Sync {
    /// Get the adapter type for this engine
    fn adapter_type(&self) -> AdapterType;

    /// Get the ADBC backend for this engine
    fn backend(&self) -> Backend;

    /// Get the resolved quoting policy
    fn quoting(&self) -> ResolvedQuoting;

    /// Get the statement splitter for this engine
    fn splitter(&self) -> &dyn StmtSplitter;

    /// Get the type operations for this engine
    fn type_ops(&self) -> &dyn TypeOps;

    /// Get the query comment config for this engine
    fn query_comment(&self) -> &QueryCommentConfig;

    /// Get a config value by key
    fn config(&self, key: &str) -> Option<Cow<'_, str>>;

    /// Get the full config object
    fn get_config(&self) -> &AdapterConfig;

    /// Get the query cache
    fn query_cache(&self) -> Option<&Arc<dyn QueryCache>>;

    /// Get a reference to the relation cache
    fn relation_cache(&self) -> &Arc<RelationCache>;

    /// Get the cancellation token
    fn cancellation_token(&self) -> CancellationToken;

    /// Create a new connection to the warehouse.
    fn new_connection(
        &self,
        state: Option<&State>,
        node_id: Option<String>,
    ) -> AdapterResult<Box<dyn Connection>>;

    /// Create a new connection to the warehouse with the given config.
    fn new_connection_with_config(
        &self,
        config: &AdapterConfig,
    ) -> AdapterResult<Box<dyn Connection>>;

    /// Execute the given SQL query or statement with options.
    ///
    /// The default implementation uses ADBC to execute queries. Engines that
    /// route execution differently (e.g. sidecar) should override this.
    fn execute_with_options(
        &self,
        state: Option<&State>,
        ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        sql: &str,
        options: Options,
        fetch: bool,
    ) -> AdapterResult<RecordBatch> {
        adbc_execute_with_options(self, state, ctx, conn, sql, options, fetch)
    }

    // -- Methods with default implementations ---------------------------------

    /// Whether this is a mock engine
    fn is_mock(&self) -> bool {
        false
    }

    /// Whether this is a sidecar engine (subprocess-based execution)
    fn is_sidecar(&self) -> bool {
        false
    }

    /// Whether this is a replay engine
    fn is_replay(&self) -> bool {
        false
    }

    /// Get the physical execution backend for sidecar engines.
    ///
    /// Returns the actual database backend (DuckDB, Snowflake, etc.) that SQL
    /// will execute against. This differs from [`adapter_type()`] which returns
    /// the logical adapter type.
    fn physical_backend(&self) -> Option<Backend> {
        None
    }

    /// Get a reference to the sidecar client, if this is a sidecar engine.
    fn sidecar_client(&self) -> Option<&dyn SidecarClient> {
        None
    }

    /// Execute the given SQL query or statement (convenience wrapper).
    fn execute(
        &self,
        state: Option<&State>,
        conn: &'_ mut dyn Connection,
        ctx: &QueryCtx,
        sql: &str,
    ) -> AdapterResult<RecordBatch> {
        self.execute_with_options(state, ctx, conn, sql, Options::new(), true)
    }

    /// Split SQL statements using the provided dialect.
    fn split_and_filter_statements(&self, sql: &str, dialect: Dialect) -> Vec<String> {
        self.splitter()
            .split(sql, dialect)
            .into_iter()
            .filter(|statement| !self.splitter().is_empty(statement, dialect))
            .collect()
    }

    /// Get the configured database name.
    fn get_configured_database_name(&self) -> Option<Cow<'_, str>> {
        self.config("database")
    }
}

/// Default ADBC-based execute_with_options implementation.
///
/// Used by engines whose connections implement the full ADBC protocol
/// (XdbcEngine, RecordEngine, ReplayEngine).
fn adbc_execute_with_options(
    engine: &(impl AdapterEngine + ?Sized),
    state: Option<&State>,
    ctx: &QueryCtx,
    conn: &'_ mut dyn Connection,
    sql: &str,
    options: Options,
    fetch: bool,
) -> AdapterResult<RecordBatch> {
    assert!(!sql.is_empty() || !options.is_empty());

    let maybe_query_comment = state
        .map(|s| engine.query_comment().resolve_comment(s))
        .transpose()?;

    let sql = match &maybe_query_comment {
        Some(comment) => {
            let sql = engine.query_comment().add_comment(sql, comment);
            Cow::Owned(sql)
        }
        None => Cow::Borrowed(sql),
    };

    let mut options = options;
    if let Some(state) = state
        && engine.adapter_type() == AdapterType::Bigquery
    {
        let mut job_labels = maybe_query_comment
            .as_ref()
            .map_or_else(BTreeMap::new, |comment| {
                engine
                    .query_comment()
                    .get_job_labels_from_query_comment(comment)
            });
        if let Some(invocation_id_label) = state
            .lookup("invocation_id")
            .and_then(|value| value.as_str().map(|label| label.to_owned()))
        {
            job_labels.insert("dbt_invocation_id".to_string(), invocation_id_label);
        }

        let job_label_option =
            serde_json::to_string(&job_labels).expect("Should be able to serialize job labels");
        options.push((
            QUERY_LABELS.to_owned(),
            OptionValue::String(job_label_option),
        ));
    }

    let token = engine.cancellation_token();
    let do_execute = |conn: &'_ mut dyn Connection| -> Result<
        (Arc<Schema>, Vec<RecordBatch>),
        Cancellable<adbc_core::error::Error>,
    > {
        use dbt_xdbc::statement::Statement as _;

        let mut stmt = match engine.query_cache() {
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
        stmt.set_option(
            OptionStatement::Other(DBT_METADATA.to_string()),
            OptionValue::Int(ctx.is_metadata() as i64),
        )?;
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

        // This loop has been discovered to inexplicably hang in some circumstances
        // See PR https://github.com/dbt-labs/fs/pull/7755
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
    let adapter_type = engine.adapter_type();
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

            record_current_span_status_from_attrs(|attrs| {
                if let Some(attrs) = attrs.downcast_mut::<QueryExecuted>() {
                    attrs.dbt_core_event_code = "E017".to_string();
                    attrs.set_query_outcome(QueryOutcome::Canceled);
                }
            });

            return Err(e);
        }
        Err(Cancellable::Error(e)) => {
            record_current_span_status_from_attrs(|attrs| {
                if let Some(attrs) = attrs.downcast_mut::<QueryExecuted>() {
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
    let total_batch = concat_batches(&schema, &batches).map_err(arrow_error_to_adapter_error)?;

    record_current_span_status_from_attrs(|attrs| {
        if let Some(attrs) = attrs.downcast_mut::<QueryExecuted>() {
            attrs.dbt_core_event_code = "E017".to_string();
            attrs.set_query_outcome(QueryOutcome::Success);
            attrs.query_id = AdapterResponse::query_id(&total_batch, adapter_type)
        }
    });

    Ok(total_batch)
}

// ---------------------------------------------------------------------------
// XdbcEngine
// ---------------------------------------------------------------------------

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
    /// Query comment config
    query_comment: QueryCommentConfig,
    /// Type operations (e.g. parsing, formatting) for the dialect this engine is for
    pub type_ops: Box<dyn TypeOps>,
    /// Statement splitter
    splitter: Arc<dyn StmtSplitter>,
    /// Query cache
    query_cache: Option<Arc<dyn QueryCache>>,
    /// Relation cache - caches warehouse relation metadata to avoid repeated queries
    relation_cache: Arc<RelationCache>,
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
        query_comment: QueryCommentConfig,
        type_ops: Box<dyn TypeOps>,
        splitter: Arc<dyn StmtSplitter>,
        query_cache: Option<Arc<dyn QueryCache>>,
        relation_cache: Arc<RelationCache>,
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
            type_ops,
            splitter,
            query_comment,
            query_cache,
            relation_cache,
            cancellation_token: token,
        }
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
}

impl AdapterEngine for XdbcEngine {
    #[inline]
    fn adapter_type(&self) -> AdapterType {
        self.adapter_type
    }

    fn backend(&self) -> Backend {
        self.auth.backend()
    }

    fn quoting(&self) -> ResolvedQuoting {
        self.quoting
    }

    fn splitter(&self) -> &dyn StmtSplitter {
        self.splitter.as_ref()
    }

    fn type_ops(&self) -> &dyn TypeOps {
        self.type_ops.as_ref()
    }

    fn query_comment(&self) -> &QueryCommentConfig {
        &self.query_comment
    }

    fn config(&self, key: &str) -> Option<Cow<'_, str>> {
        self.config.get_string(key)
    }

    fn get_config(&self) -> &AdapterConfig {
        &self.config
    }

    fn query_cache(&self) -> Option<&Arc<dyn QueryCache>> {
        self.query_cache.as_ref()
    }

    fn relation_cache(&self) -> &Arc<RelationCache> {
        &self.relation_cache
    }

    fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    fn new_connection(
        &self,
        state: Option<&State>,
        _node_id: Option<String>,
    ) -> AdapterResult<Box<dyn Connection>> {
        match self.adapter_type {
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

    // Uses the default `execute_with_options` (ADBC-based).
}

// ---------------------------------------------------------------------------
// MockEngine
// ---------------------------------------------------------------------------

/// Mock engine state for the MockAdapter
#[derive(Clone)]
pub struct MockEngine {
    adapter_type: AdapterType,
    quoting: ResolvedQuoting,
    type_ops: Arc<dyn TypeOps>,
    stmt_splitter: Arc<dyn StmtSplitter>,
    /// Relation cache - caches warehouse relation metadata
    relation_cache: Arc<RelationCache>,
}

impl MockEngine {
    pub fn new(
        adapter_type: AdapterType,
        quoting: ResolvedQuoting,
        type_ops: Box<dyn TypeOps>,
        stmt_splitter: Arc<dyn StmtSplitter>,
        relation_cache: Arc<RelationCache>,
    ) -> Self {
        Self {
            adapter_type,
            quoting,
            type_ops: Arc::from(type_ops),
            stmt_splitter,
            relation_cache,
        }
    }
}

impl AdapterEngine for MockEngine {
    fn adapter_type(&self) -> AdapterType {
        self.adapter_type
    }

    fn backend(&self) -> Backend {
        backend_of(self.adapter_type)
    }

    fn quoting(&self) -> ResolvedQuoting {
        self.quoting
    }

    fn splitter(&self) -> &dyn StmtSplitter {
        self.stmt_splitter.as_ref()
    }

    fn type_ops(&self) -> &dyn TypeOps {
        self.type_ops.as_ref()
    }

    fn query_comment(&self) -> &QueryCommentConfig {
        &EMPTY_CONFIG
    }

    fn config(&self, _key: &str) -> Option<Cow<'_, str>> {
        None
    }

    fn get_config(&self) -> &AdapterConfig {
        unreachable!("Mock engine does not support get_config")
    }

    fn query_cache(&self) -> Option<&Arc<dyn QueryCache>> {
        None
    }

    fn relation_cache(&self) -> &Arc<RelationCache> {
        &self.relation_cache
    }

    fn cancellation_token(&self) -> CancellationToken {
        never_cancels()
    }

    fn new_connection(
        &self,
        _state: Option<&State>,
        _node_id: Option<String>,
    ) -> AdapterResult<Box<dyn Connection>> {
        Ok(Box::new(NoopConnection))
    }

    fn new_connection_with_config(
        &self,
        _config: &AdapterConfig,
    ) -> AdapterResult<Box<dyn Connection>> {
        Ok(Box::new(NoopConnection) as Box<dyn Connection>)
    }

    fn execute_with_options(
        &self,
        _state: Option<&State>,
        _ctx: &QueryCtx,
        _conn: &'_ mut dyn Connection,
        _sql: &str,
        _options: Options,
        _fetch: bool,
    ) -> AdapterResult<RecordBatch> {
        Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))
    }

    fn is_mock(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// SidecarEngine
// ---------------------------------------------------------------------------

/// Sidecar engine for subprocess-based execution.
///
/// Routes execution to a sidecar backend via SidecarClient trait.
/// Implementation details (subprocess management, message protocol) remain
/// in closed-source crates.
#[derive(Clone)]
pub struct SidecarEngine {
    adapter_type: AdapterType,
    execution_backend: Backend,
    client: Arc<dyn SidecarClient>,
    quoting: ResolvedQuoting,
    config: Arc<AdapterConfig>,
    type_ops: Arc<dyn TypeOps>,
    stmt_splitter: Arc<dyn StmtSplitter>,
    query_comment: Arc<QueryCommentConfig>,
    /// Unused for sidecar adapters - required for API compatibility
    relation_cache: Arc<RelationCache>,
}

impl SidecarEngine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        adapter_type: AdapterType,
        execution_backend: Backend,
        client: Arc<dyn SidecarClient>,
        quoting: ResolvedQuoting,
        config: AdapterConfig,
        type_ops: Box<dyn TypeOps>,
        stmt_splitter: Arc<dyn StmtSplitter>,
        query_comment: QueryCommentConfig,
        relation_cache: Arc<RelationCache>,
    ) -> Self {
        Self {
            adapter_type,
            execution_backend,
            client,
            quoting,
            config: Arc::new(config),
            type_ops: Arc::from(type_ops),
            stmt_splitter,
            query_comment: Arc::new(query_comment),
            relation_cache,
        }
    }
}

impl AdapterEngine for SidecarEngine {
    fn adapter_type(&self) -> AdapterType {
        self.adapter_type
    }

    fn backend(&self) -> Backend {
        self.execution_backend
    }

    fn quoting(&self) -> ResolvedQuoting {
        self.quoting
    }

    fn splitter(&self) -> &dyn StmtSplitter {
        self.stmt_splitter.as_ref()
    }

    fn type_ops(&self) -> &dyn TypeOps {
        self.type_ops.as_ref()
    }

    fn query_comment(&self) -> &QueryCommentConfig {
        &self.query_comment
    }

    fn config(&self, key: &str) -> Option<Cow<'_, str>> {
        self.config.get_string(key)
    }

    fn get_config(&self) -> &AdapterConfig {
        &self.config
    }

    fn query_cache(&self) -> Option<&Arc<dyn QueryCache>> {
        None
    }

    fn relation_cache(&self) -> &Arc<RelationCache> {
        &self.relation_cache
    }

    fn cancellation_token(&self) -> CancellationToken {
        never_cancels()
    }

    fn new_connection(
        &self,
        state: Option<&State>,
        node_id: Option<String>,
    ) -> AdapterResult<Box<dyn Connection>> {
        self.client.new_connection(state, node_id)
    }

    fn new_connection_with_config(
        &self,
        _config: &AdapterConfig,
    ) -> AdapterResult<Box<dyn Connection>> {
        // Sidecar mode doesn't use config-based connections
        Ok(Box::new(NoopConnection) as Box<dyn Connection>)
    }

    fn execute_with_options(
        &self,
        _state: Option<&State>,
        ctx: &QueryCtx,
        _conn: &'_ mut dyn Connection,
        sql: &str,
        _options: Options,
        fetch: bool,
    ) -> AdapterResult<RecordBatch> {
        // Route through sidecar client
        let batch_opt = self.client.execute(ctx, sql, fetch)?;
        match batch_opt {
            Some(batch) => Ok(batch),
            None => Ok(RecordBatch::new_empty(Arc::new(Schema::empty()))),
        }
    }

    fn is_sidecar(&self) -> bool {
        true
    }

    fn physical_backend(&self) -> Option<Backend> {
        Some(self.execution_backend)
    }

    fn sidecar_client(&self) -> Option<&dyn SidecarClient> {
        Some(self.client.as_ref())
    }
}

// ---------------------------------------------------------------------------
// Constructor functions (replacing former enum constructors)
// ---------------------------------------------------------------------------

/// Create a new XDBC-based [`AdapterEngine`].
#[allow(clippy::too_many_arguments)]
pub fn new_engine(
    adapter_type: AdapterType,
    auth: Arc<dyn Auth>,
    config: AdapterConfig,
    quoting: ResolvedQuoting,
    query_cache: Option<Arc<dyn QueryCache>>,
    query_comment: QueryCommentConfig,
    type_ops: Box<dyn TypeOps>,
    stmt_splitter: Arc<dyn StmtSplitter>,
    relation_cache: Arc<RelationCache>,
    token: CancellationToken,
) -> Arc<dyn AdapterEngine> {
    let engine = XdbcEngine::new(
        adapter_type,
        auth,
        config,
        quoting,
        query_comment,
        type_ops,
        stmt_splitter,
        query_cache,
        relation_cache,
        token,
    );
    Arc::new(engine)
}

/// Create a new replay-based [`AdapterEngine`].
#[allow(clippy::too_many_arguments)]
pub fn new_engine_for_replaying(
    adapter_type: AdapterType,
    path: PathBuf,
    config: AdapterConfig,
    quoting: ResolvedQuoting,
    query_comment: QueryCommentConfig,
    type_ops: Box<dyn TypeOps>,
    stmt_splitter: Arc<dyn StmtSplitter>,
    relation_cache: Arc<RelationCache>,
    token: CancellationToken,
) -> Arc<dyn AdapterEngine> {
    let engine = ReplayEngine::new(
        adapter_type,
        path,
        config,
        quoting,
        query_comment,
        type_ops,
        stmt_splitter,
        relation_cache,
        token,
    );
    Arc::new(engine)
}

/// Create a new record-wrapping [`AdapterEngine`].
pub fn new_engine_for_recording(
    path: PathBuf,
    engine: Arc<dyn AdapterEngine>,
) -> Arc<dyn AdapterEngine> {
    let engine = RecordEngine::new(path, engine);
    Arc::new(engine)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Get the Databricks compute engine configured for this model/snapshot
///
/// https://docs.getdbt.com/reference/resource-configs/databricks-configs#selecting-compute-per-model
fn databricks_compute_from_state(state: &State) -> Option<String> {
    let yaml_node = dbt_yaml::to_value(state.lookup("model").as_ref()?).ok()?;

    if let Ok(model) = DbtModel::deserialize(&yaml_node) {
        if let Some(databricks_attr) = &model.__adapter_attr__.databricks_attr {
            databricks_attr.databricks_compute.clone()
        } else {
            None
        }
    } else if let Ok(snapshot) = DbtSnapshot::deserialize(&yaml_node) {
        if let Some(databricks_attr) = &snapshot.__adapter_attr__.databricks_attr {
            databricks_attr.databricks_compute.clone()
        } else {
            None
        }
    } else {
        None
    }
}

/// Execute query and retry in case of an error. Retry is done (up to
/// the given limit) regardless of the error encountered.
///
/// https://github.com/dbt-labs/dbt-adapters/blob/996a302fa9107369eb30d733dadfaf307023f33d/dbt-adapters/src/dbt/adapters/sql/connections.py#L84
#[allow(clippy::too_many_arguments)]
pub fn execute_query_with_retry(
    engine: Arc<dyn AdapterEngine>,
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
