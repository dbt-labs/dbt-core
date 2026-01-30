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
//! The adapter returns results in Snowflake-compatible format (AgateTable)

use crate::AdapterTyping;
use crate::adapter_engine::{AdapterEngine, SidecarEngine};
use crate::base_adapter::AdapterType;
use crate::cache::RelationCache;
use crate::column::Column;
use crate::config::AdapterConfig;
use crate::errors::AdapterResult;
use crate::metadata::{CatalogAndSchema, MetadataAdapter};
use crate::query_comment::QueryCommentConfig;
use crate::relation::do_create_relation;
use crate::response::AdapterResponse;
use crate::sidecar_client::SidecarClient;
use crate::sql_types::TypeOps;
use crate::typed_adapter::TypedBaseAdapter;
use dbt_schemas::schemas::relations::base::BaseRelation;

use dbt_agate::AgateTable;
use dbt_common::cancellation::CancellationToken;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_xdbc::{Backend, Connection, QueryCtx};
use minijinja::{State, Value};

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;

/// Snowflake adapter with sidecar execution backend
///
/// This adapter:
/// - Uses Snowflake SQL dialect and type system
/// - Delegates execute() calls to SidecarClient
/// - Returns Snowflake-compatible results (AgateTable)
/// - Implements full TypedBaseAdapter interface
/// - Supports introspection (get_columns_in_relation) via sidecar DESCRIBE queries
#[derive(Clone)]
pub struct SnowflakeSidecarAdapter {
    /// Adapter type (always Snowflake)
    adapter_type: AdapterType,
    /// Sidecar engine for type operations, quoting, and execution delegation
    engine: Arc<AdapterEngine>,
    /// Sidecar client for query execution (also stored in engine, kept for direct access)
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
    ///
    /// Uses `SidecarEngine` to enable introspection queries (e.g., `get_columns_in_relation`)
    /// to execute via the sidecar client instead of returning mock data.
    pub fn new(
        flags: BTreeMap<String, Value>,
        quoting: ResolvedQuoting,
        type_ops: Box<dyn TypeOps>,
        sidecar_client: Arc<dyn SidecarClient>,
        cancellation_token: CancellationToken,
    ) -> Self {
        let sidecar_engine = SidecarEngine::new(
            AdapterType::Snowflake,
            Backend::DuckDB,
            sidecar_client.clone(),
            quoting,
            AdapterConfig::default(),
            type_ops,
            QueryCommentConfig::from_query_comment(None, AdapterType::Snowflake, false),
            Arc::new(RelationCache::default()),
        );

        Self {
            adapter_type: AdapterType::Snowflake,
            engine: Arc::new(AdapterEngine::Sidecar(Arc::new(sidecar_engine))),
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
        // In sidecar mode, we don't use the Snowflake metadata adapter.
        // Schema hydration is handled via db_runner (DESCRIBE queries).
        // Returning None causes hydrate_missing_tables_from_remote to use the db_runner path.
        None
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
    /// Create a connection that delegates to the sidecar client
    fn new_connection(
        &self,
        state: Option<&State>,
        node_id: Option<String>,
    ) -> AdapterResult<Box<dyn Connection>> {
        self.sidecar_client.new_connection(state, node_id)
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

    /// Get columns in relation via sidecar (overrides default Jinja macro execution)
    fn get_columns_in_relation(
        &self,
        _state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<Vec<Column>> {
        // Build fully qualified relation name: database.schema.identifier
        let database = relation.database_as_str()?;
        let schema = relation.schema_as_str()?;
        let identifier = relation.identifier_as_str()?;
        let relation_name = format!("{}.{}.{}", database, schema, identifier);

        // Get columns via sidecar client
        let column_infos = self.sidecar_client.get_columns(&relation_name)?;

        // Convert ColumnInfo to Column objects
        let columns = column_infos
            .into_iter()
            .map(|info| {
                Column::new(
                    self.adapter_type,
                    info.name,
                    info.data_type,
                    None, // char_size
                    None, // numeric_precision
                    None, // numeric_scale
                )
            })
            .collect();

        Ok(columns)
    }

    /// List relations in schema via sidecar (overrides default Snowflake SQL)
    ///
    /// Delegates to SidecarClient::list_relations() and converts to BaseRelation objects.
    fn list_relations(
        &self,
        _query_ctx: &QueryCtx,
        _conn: &'_ mut dyn Connection,
        db_schema: &CatalogAndSchema,
    ) -> AdapterResult<Vec<Arc<dyn BaseRelation>>> {
        // Normalize schema name based on quoting policy
        let query_schema = if self.quoting.schema {
            db_schema.resolved_schema.clone()
        } else {
            db_schema.resolved_schema.to_lowercase()
        };

        // Delegate to sidecar client
        let relation_infos = self.sidecar_client.list_relations(&query_schema)?;

        // Convert to BaseRelation objects
        let mut relations = Vec::with_capacity(relation_infos.len());
        for (database, schema, name, rel_type) in relation_infos {
            let relation = do_create_relation(
                self.adapter_type,
                database,
                schema,
                Some(name),
                Some(rel_type),
                self.quoting,
            )?;
            relations.push(relation);
        }
        Ok(relations)
    }

    /// Get relation via sidecar (overrides default Snowflake SQL)
    ///
    /// Delegates to SidecarClient::get_relation_type() and converts to BaseRelation.
    fn get_relation(
        &self,
        _state: &State,
        _ctx: &QueryCtx,
        _conn: &'_ mut dyn Connection,
        database: &str,
        schema: &str,
        identifier: &str,
    ) -> AdapterResult<Option<Arc<dyn BaseRelation>>> {
        // Normalize names based on quoting policy (DuckDB is case-preserving for quoted identifiers)
        let query_schema = if self.quoting.schema {
            schema.to_string()
        } else {
            schema.to_lowercase()
        };
        let query_identifier = if self.quoting.identifier {
            identifier.to_string()
        } else {
            identifier.to_lowercase()
        };

        // Delegate to sidecar client
        let relation_type = self
            .sidecar_client
            .get_relation_type(&query_schema, &query_identifier)?;

        match relation_type {
            Some(rel_type) => {
                // Create relation with the logical adapter type (Snowflake)
                let relation = do_create_relation(
                    self.adapter_type,
                    database.to_string(),
                    schema.to_string(),
                    Some(identifier.to_string()),
                    Some(rel_type),
                    self.quoting,
                )?;
                Ok(Some(relation))
            }
            None => Ok(None),
        }
    }
}
