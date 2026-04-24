use std::borrow::Cow;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use dbt_adapter_core::AdapterType;
use dbt_agate::hashers::IdentityBuildHasher;
use dbt_auth::{AdapterConfig, Auth};
use dbt_common::AdapterResult;
use dbt_common::behavior_flags::Behavior;
use dbt_common::cancellation::CancellationToken;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::{DbtModel, DbtSnapshot};
use dbt_xdbc::semaphore::Semaphore;
use dbt_xdbc::*;
use minijinja::State;
use parking_lot::RwLock;
use serde::Deserialize;

use crate::cache::RelationCache;
use crate::engine::query_comment::QueryCommentConfig;
use crate::errors::{AdapterError, AdapterErrorKind, adbc_error_to_adapter_error};
use crate::query_cache::QueryCache;
use crate::sql_types::TypeOps;
use crate::stmt_splitter::StmtSplitter;

use super::adapter_engine::*;
use super::make_behavior;
use super::noop_connection::NoopConnection;

#[derive(Default)]
pub struct DatabaseMap {
    inner: HashMap<database::Fingerprint, Box<dyn Database>, IdentityBuildHasher>,
}

/// Operational mode for [`XdbcEngine`].
///
/// Controls how the engine creates connections and executes queries.
#[derive(Debug)]
pub enum EngineMode {
    /// Normal ADBC execution against a live warehouse.
    Live,
    /// Stubbed connections and execution
    Mock,
}

impl EngineMode {
    /// Whether this mode connects to a real warehouse.
    pub fn has_real_connections(&self) -> bool {
        matches!(self, EngineMode::Live)
    }
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
    /// User overrides for behavior flags from dbt_project.yml
    behavior_flag_overrides: BTreeMap<String, bool>,
    /// Resolved behavior object with user overrides applied
    behavior: Arc<Behavior>,
    /// Controls connection/execution behaviour.
    mode: EngineMode,
    /// The `threads` configuration value from the dbt profile.
    threads: Option<usize>,
}

impl XdbcEngine {
    #[allow(clippy::too_many_arguments)]
    fn build(
        adapter_type: AdapterType,
        auth: Arc<dyn Auth>,
        config: AdapterConfig,
        quoting: ResolvedQuoting,
        query_comment: QueryCommentConfig,
        type_ops: Box<dyn TypeOps>,
        splitter: Arc<dyn StmtSplitter>,
        query_cache: Option<Arc<dyn QueryCache>>,
        relation_cache: Arc<RelationCache>,
        behavior_flag_overrides: BTreeMap<String, bool>,
        mode: EngineMode,
        threads: Option<usize>,
    ) -> Self {
        let permits = if mode.has_real_connections() {
            threads.map(|t| (t as u32).max(1)).unwrap_or(u32::MAX)
        } else {
            u32::MAX
        };
        let behavior = make_behavior(adapter_type, &behavior_flag_overrides);
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
            behavior_flag_overrides,
            behavior,
            mode,
            threads,
        }
    }

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
        behavior_flag_overrides: BTreeMap<String, bool>,
        threads: Option<usize>,
    ) -> Self {
        Self::build(
            adapter_type,
            auth,
            config,
            quoting,
            query_comment,
            type_ops,
            splitter,
            query_cache,
            relation_cache,
            behavior_flag_overrides,
            EngineMode::Live,
            threads,
        )
    }

    /// Create a mock engine that stubs out connections and execution.
    ///
    /// Used for replay modes and test adapters that must never talk to a
    /// real warehouse.
    #[allow(clippy::too_many_arguments)]
    pub fn new_mock(
        adapter_type: AdapterType,
        auth: Arc<dyn Auth>,
        config: AdapterConfig,
        quoting: ResolvedQuoting,
        type_ops: Box<dyn TypeOps>,
        splitter: Arc<dyn StmtSplitter>,
        relation_cache: Arc<RelationCache>,
        behavior_flag_overrides: BTreeMap<String, bool>,
    ) -> Self {
        Self::build(
            adapter_type,
            auth,
            config,
            quoting,
            QueryCommentConfig::from_query_comment(None, adapter_type, false, None),
            type_ops,
            splitter,
            None,
            relation_cache,
            behavior_flag_overrides,
            EngineMode::Mock,
            None,
        )
    }

    /// Get the engine mode.
    pub fn mode(&self) -> &EngineMode {
        &self.mode
    }

    fn load_driver_and_configure_database(
        &self,
        config: &AdapterConfig,
    ) -> AdapterResult<Box<dyn Database>> {
        assert!(
            self.mode.has_real_connections(),
            "load_driver_and_configure_database called in {:?} mode",
            self.mode,
        );
        let use_cloud_credentials = config.use_dbt_cloud_credentials();
        let backend = self.auth.backend();

        let (database_builder, load_strategy) = if use_cloud_credentials {
            // Cloud credentials are used to connect to a service that manages
            // drivers and warehouse credentials for us. The "flock" driver takes
            // these credentials and behaves as a proxy to the actual.
            let builder = Self::configure_cloud_database(backend)?;
            (builder, LoadStrategy::Remote)
        } else {
            // Delegate configuration to the Auth implementation configuring
            // the warehouse driver locally.
            let auth_result = self
                .auth
                .configure(config)
                .map_err(crate::errors::auth_error_to_adapter_error)?;

            for warning in &auth_result.warnings {
                dbt_common::tracing::emit::emit_warn_log_message(
                    dbt_common::ErrorCode::InvalidConfig,
                    warning,
                    None,
                );
            }

            let load_strategy = match self.adapter_type {
                AdapterType::DuckDB => LoadStrategy::SystemThenCdnCache,
                _ => LoadStrategy::CdnCache,
            };
            (auth_result.builder, load_strategy)
        };

        // This will load the "flock" driver if load_strategy is Remote.
        let mut driver = driver::Builder::new(backend, load_strategy)
            .with_semaphore(Arc::clone(&self.semaphore))
            .try_load()
            .map_err(adbc_error_to_adapter_error)?;

        // The database is configured only once even if this runs multiple times,
        // unless a different configuration is provided.
        let opts = database_builder.into_iter().collect::<Vec<_>>();
        let fingerprint = database::Builder::fingerprint(opts.iter());
        {
            let read_guard = self.configured_databases.read();
            if let Some(database) = read_guard.inner.get(&fingerprint) {
                return Ok(database.clone());
            }
        }
        {
            let mut write_guard = self.configured_databases.write();
            if let Some(database) = write_guard.inner.get(&fingerprint) {
                let database: Box<dyn Database> = database.clone();
                Ok(database)
            } else {
                let mut database = driver
                    .new_database_with_opts(opts)
                    .map_err(adbc_error_to_adapter_error)?;
                // DuckDB: apply extensions, settings, secrets, and attachments
                if self.adapter_type == AdapterType::DuckDB {
                    self.apply_duckdb_init_sql(&mut database, config)?;
                }
                write_guard.inner.insert(fingerprint, database.clone());
                Ok(database)
            }
        }
    }

    /// Build a [database::Builder] configured with dbt Cloud credentials
    /// read from `~/.dbt/dbt_cloud.yml` (with env-var overrides applied).
    fn configure_cloud_database(backend: Backend) -> AdapterResult<database::Builder> {
        let mut builder = database::Builder::new(backend);
        let cloud_config_path = dbt_cloud_config::get_cloud_project_path()
            .map_err(|e| AdapterError::new(AdapterErrorKind::Configuration, e))?;
        let cloud_yml = dbt_cloud_config::parse_cloud_config(&cloud_config_path)
            .map_err(|e| AdapterError::new(AdapterErrorKind::Configuration, e))?;
        let resolved = dbt_cloud_config::resolve_cloud_config(cloud_yml.as_ref(), None);
        if let Some(credentials) = resolved.and_then(|r| r.credentials) {
            builder
                .with_named_option("dbt_cloud.token", credentials.token)
                .map_err(adbc_error_to_adapter_error)?;
            builder
                .with_named_option("dbt_cloud.host", credentials.host)
                .map_err(adbc_error_to_adapter_error)?;
            builder
                .with_named_option("dbt_cloud.account_id", credentials.account_id)
                .map_err(adbc_error_to_adapter_error)?;
        }
        Ok(builder)
    }

    /// Apply DuckDB init SQL (extensions, settings, secrets, attachments)
    /// to a newly created database instance. Uses a temporary connection.
    fn apply_duckdb_init_sql(
        &self,
        database: &mut Box<dyn Database>,
        config: &AdapterConfig,
    ) -> AdapterResult<()> {
        let init_stmts = dbt_auth::generate_duckdb_init_sql(config);
        if init_stmts.is_empty() {
            return Ok(());
        }
        let mut conn = database
            .new_connection()
            .map_err(adbc_error_to_adapter_error)?;
        for sql in &init_stmts {
            let mut stmt = conn.new_statement().map_err(adbc_error_to_adapter_error)?;
            stmt.set_sql_query(sql)
                .map_err(adbc_error_to_adapter_error)?;
            let _ = stmt.execute_update().map_err(|e| {
                adbc_error_to_adapter_error(adbc_core::error::Error::with_message_and_status(
                    format!("DuckDB init SQL failed on '{sql}': {e}"),
                    adbc_core::error::Status::Internal,
                ))
            })?;
        }
        Ok(())
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

    fn threads(&self) -> Option<usize> {
        self.threads
    }

    fn is_mock(&self) -> bool {
        matches!(self.mode, EngineMode::Mock)
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

    fn new_connection(
        &self,
        state: Option<&State>,
        _node_id: Option<String>,
    ) -> AdapterResult<Box<dyn Connection>> {
        let do_create_connection =
            |adapter_type: AdapterType| -> AdapterResult<Box<dyn Connection>> {
                let config = match adapter_type {
                    AdapterType::Databricks => {
                        if let Some(databricks_compute) =
                            state.and_then(databricks_compute_from_state)
                        {
                            let augmented_config = {
                                let mut mapping = self.config.repr().clone();
                                mapping
                                    .insert("databricks_compute".into(), databricks_compute.into());
                                AdapterConfig::new(mapping)
                            };
                            Cow::Owned(augmented_config)
                        } else {
                            Cow::Borrowed(&self.config)
                        }
                    }
                    _ => Cow::Borrowed(&self.config),
                };
                self.new_connection_with_config(config.as_ref())
            };

        match &self.mode {
            EngineMode::Mock => Ok(Box::new(NoopConnection)),
            EngineMode::Live => do_create_connection(self.adapter_type),
        }
    }

    fn new_connection_with_config(
        &self,
        config: &AdapterConfig,
    ) -> AdapterResult<Box<dyn Connection>> {
        if !self.mode.has_real_connections() {
            return Ok(Box::new(NoopConnection));
        }
        let mut database = self.load_driver_and_configure_database(config)?;
        let connection_builder = connection::Builder::default();
        let conn = connection_builder
            .build(&mut database)
            .map_err(|e| enrich_connection_error(self.adapter_type(), e, config))?;
        Ok(conn)
    }

    fn execute_with_options(
        &self,
        state: Option<&State>,
        ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        sql: &str,
        options: Options,
        fetch: bool,
        token: CancellationToken,
    ) -> AdapterResult<RecordBatch> {
        if matches!(self.mode, EngineMode::Mock) {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }
        adbc_execute_with_options(self, state, ctx, conn, sql, options, fetch, token)
    }

    fn behavior(&self) -> &Arc<Behavior> {
        &self.behavior
    }

    fn behavior_flag_overrides(&self) -> &BTreeMap<String, bool> {
        &self.behavior_flag_overrides
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Get the Databricks compute engine configured for this model/snapshot
///
/// https://docs.getdbt.com/reference/resource-configs/databricks-configs#selecting-compute-per-model
fn databricks_compute_from_state(state: &State) -> Option<String> {
    let yaml_node = dbt_yaml::to_value(state.lookup("model", &[]).as_ref()?).ok()?;

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

/// Enrich connection errors with adapter-specific hints where possible.
fn enrich_connection_error(
    adapter_type: AdapterType,
    err: adbc_core::error::Error,
    config: &AdapterConfig,
) -> AdapterError {
    use AdapterType::*;
    match adapter_type {
        // If `err` looks like a Snowflake HTTP 403 connection failure, replace
        // its message with one that hints at a misconfigured account identifier.
        // Other errors are returned unchanged.
        //
        // We key off HTTP 403 in the error message because that is the specific
        // status Snowflake returns when the account subdomain is not recognized.
        // The Go ADBC driver does not expose a dedicated vendor code for this
        // case (the error arrives as a raw HTTP failure, not a typed
        // SnowflakeError), so substring matching on the status code is the most
        // reliable signal available.
        Snowflake if err.message.contains(": 403") => {
            let account_display = config
                .get_string("account")
                .map(|a| format!("'{a}'"))
                .unwrap_or_else(|| "<unknown>".to_string());
            let message = format!(
                "Could not connect to Snowflake. One possible cause is an incorrect \
account identifier ({account_display}).\n\n\
If the 'account' field in your profile is wrong, the value should be \
in the format '<orgname>-<account_name>' (e.g. 'myorg-myaccount') and \
must not include '.snowflakecomputing.com'.\n\n\
You can find your account identifier in Snowsight under \
Admin > Accounts, or by running:\n  \
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME()\n\n\
See: https://docs.snowflake.com/en/user-guide/admin-account-identifier#requirements-for-account-identifiers\n\n\
Original error: {}",
                err.message
            );
            AdapterError::new(adbc_error_to_adapter_error(err).kind(), message)
        }
        _ => adbc_error_to_adapter_error(err),
    }
}
