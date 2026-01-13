//! Snowflake Sidecar Adapter
//!
//! This adapter provides a Snowflake-compatible interface for sidecar execution mode.
//! It uses the Snowflake SQL dialect, quoting rules, and type system, but delegates
//! query execution to a SidecarClient (dbt-db-runner) instead of ADBC/warehouse.
//!
//! # Architecture
//!
//! - **Parse phase**: Jinja's `execute=false` prevents adapter method calls
//! - **Compile/Render phase**: Jinja's `execute=true` allows introspection queries via sidecar
//! - **Run phase**: Uses DbRunnerBackend directly (not this adapter)
//!
//! The adapter returns results in Snowflake-compatible format (AgateTable) by converting
//! Arrow IPC results from the DuckDB worker.

use crate::AdapterTyping;
use crate::adapter_engine::{AdapterEngine, MockEngine};
use crate::base_adapter::AdapterType;
use crate::cache::RelationCache;
use crate::errors::{AdapterError, AdapterErrorKind, AdapterResult};
use crate::metadata::{MetadataAdapter, snowflake::SnowflakeMetadataAdapter};
use crate::response::AdapterResponse;
use crate::sidecar_client::SidecarClient;
use crate::sql_types::TypeOps;
use crate::typed_adapter::TypedBaseAdapter;

use dbt_agate::AgateTable;
use dbt_common::cancellation::CancellationToken;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_xdbc::{Connection, QueryCtx};
use minijinja::{State, Value};

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;

/// Snowflake adapter with sidecar execution backend
///
/// This adapter:
/// - Uses Snowflake SQL dialect and type system
/// - Delegates execute() calls to SidecarClient (DuckDB worker)
/// - Returns Snowflake-compatible results (AgateTable)
/// - Implements full TypedBaseAdapter interface
#[derive(Clone)]
pub struct SnowflakeSidecarAdapter {
    /// Adapter type (always Snowflake)
    adapter_type: AdapterType,
    /// Mock engine for type operations and quoting
    engine: Arc<AdapterEngine>,
    /// Sidecar client for query execution
    sidecar_client: Arc<dyn SidecarClient>,
    /// Flags from dbt_project.yml
    #[allow(dead_code)]
    flags: BTreeMap<String, Value>,
    /// Quoting policy
    quoting: ResolvedQuoting,
    /// Cancellation token
    cancellation_token: CancellationToken,
}

impl fmt::Debug for SnowflakeSidecarAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnowflakeSidecarAdapter")
            .field("adapter_type", &self.adapter_type)
            .field("quoting", &self.quoting)
            .finish()
    }
}

impl SnowflakeSidecarAdapter {
    /// Create a new Snowflake sidecar adapter
    pub fn new(
        flags: BTreeMap<String, Value>,
        quoting: ResolvedQuoting,
        type_ops: Box<dyn TypeOps>,
        sidecar_client: Arc<dyn SidecarClient>,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            adapter_type: AdapterType::Snowflake,
            engine: Arc::new(AdapterEngine::Mock(MockEngine::new(
                AdapterType::Snowflake,
                type_ops,
                quoting,
                Arc::new(RelationCache::default()),
            ))),
            sidecar_client,
            flags,
            quoting,
            cancellation_token,
        }
    }
}

impl AdapterTyping for SnowflakeSidecarAdapter {
    fn adapter_type(&self) -> AdapterType {
        self.adapter_type
    }

    fn metadata_adapter(&self) -> Option<Box<dyn MetadataAdapter>> {
        // Use Snowflake metadata adapter for catalog introspection
        Some(Box::new(SnowflakeMetadataAdapter::new(Arc::clone(
            &self.engine,
        ))))
    }

    fn as_typed_base_adapter(&self) -> &dyn TypedBaseAdapter {
        self
    }

    fn engine(&self) -> &Arc<AdapterEngine> {
        &self.engine
    }

    fn quoting(&self) -> ResolvedQuoting {
        self.quoting
    }

    fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }
}

impl TypedBaseAdapter for SnowflakeSidecarAdapter {
    /// Sidecar mode doesn't use connections - error if code tries to create one
    fn new_connection(
        &self,
        _state: Option<&State>,
        _node_id: Option<String>,
    ) -> AdapterResult<Box<dyn Connection>> {
        Err(AdapterError::new(
            AdapterErrorKind::NotSupported,
            "Sidecar mode does not use connections - all execution goes through SidecarClient",
        ))
    }

    /// Execute a query via sidecar (overrides default implementation)
    fn execute(
        &self,
        _state: Option<&State>,
        _conn: &'_ mut dyn Connection,
        ctx: &QueryCtx,
        sql: &str,
        _auto_begin: bool,
        fetch: bool,
        _limit: Option<i64>,
        _options: Option<HashMap<String, String>>,
    ) -> AdapterResult<(AdapterResponse, AgateTable)> {
        // Delegate to sidecar client for execution
        let batch_opt = self.sidecar_client.execute(ctx, sql, fetch)?;

        let response = AdapterResponse {
            message: "execute".to_string(),
            code: sql.to_string(),
            rows_affected: batch_opt.as_ref().map(|b| b.num_rows() as i64).unwrap_or(0),
            query_id: None,
        };

        let table = if let Some(batch) = batch_opt {
            AgateTable::from_record_batch(Arc::new(batch))
        } else {
            AgateTable::default()
        };

        Ok((response, table))
    }

    /// Add a query without fetching results (overrides default implementation)
    fn add_query(
        &self,
        ctx: &QueryCtx,
        _conn: &'_ mut dyn Connection,
        sql: &str,
        _auto_begin: bool,
        _bindings: Option<&Value>,
        _abridge_sql_log: bool,
    ) -> AdapterResult<()> {
        // Execute without fetching results
        let _ = self.sidecar_client.execute(ctx, sql, false)?;
        Ok(())
    }

    // All other TypedBaseAdapter methods use default implementations
    // which delegate to the engine or provide Snowflake-specific behavior
}
