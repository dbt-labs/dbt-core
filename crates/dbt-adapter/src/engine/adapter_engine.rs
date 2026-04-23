use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;

use adbc_core::options::{OptionStatement, OptionValue};
use arrow::compute::concat_batches;
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use dbt_auth::AdapterConfig;
use dbt_common::behavior_flags::Behavior;
use dbt_common::cancellation::CancellationToken;
use dbt_common::hashing::code_hash;
use dbt_common::tracing::span_info::{
    read_current_span_start_info, record_current_span_status_from_attrs,
};
use dbt_common::{AdapterError, AdapterErrorKind, AdapterResult, Cancellable, create_debug_span};
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_telemetry::{QueryExecuted, QueryOutcome};
use dbt_xdbc::bigquery::QUERY_LABELS;
use dbt_xdbc::{Backend, Connection, QueryCtx};
use indexmap::IndexMap;
use minijinja::State;
use tracy_client::span;

use crate::cache::RelationCache;
use crate::engine::sidecar_client::SidecarClient;
use crate::errors::{adbc_error_to_adapter_error, arrow_error_to_adapter_error};
use crate::query_cache::QueryCache;
use crate::query_comment::QueryCommentConfig;
use crate::sql_types::TypeOps;
use crate::statement::*;
use crate::stmt_splitter::StmtSplitter;
use crate::{AdapterResponse, AdapterType};

pub type Options = Vec<(String, OptionValue)>;

/// A trait abstracting the layer between the adapter layer and database drivers.
///
/// Each concrete engine type (XDBC with live/mock/record/replay modes, sidecar)
/// implements this trait directly. This is the internal adapter service for other
/// Rust modules in Fusion as the adapter layer interface is forced to abide by
/// what is expected for consumption from Jinja code.
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

    /// Get the resolved behavior object with user overrides applied
    fn behavior(&self) -> &Arc<Behavior>;

    /// Get the user overrides for behavior flags
    fn behavior_flag_overrides(&self) -> &BTreeMap<String, bool>;

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
    #[allow(clippy::too_many_arguments)]
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
        adbc_execute_with_options(self, state, ctx, conn, sql, options, fetch, token)
    }

    // -- Methods with default implementations ---------------------------------

    /// The `threads` configuration value from the dbt profile.
    ///
    /// Used to derive connection concurrency limits in metadata adapters.
    /// Returns `None` when the setting is not available (mock, sidecar, etc.).
    fn threads(&self) -> Option<usize> {
        None
    }

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

    /// Returns a generation counter identifying this engine instance.
    ///
    /// The connection pool uses this to detect stale connections when the
    /// engine changes between sequential runs (e.g. different recording
    /// directories). Override this in engines that create stateful
    /// connections that become invalid across configuration changes.
    fn generation(&self) -> u64 {
        0
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
        token: CancellationToken,
    ) -> AdapterResult<RecordBatch> {
        self.execute_with_options(state, ctx, conn, sql, Options::new(), true, token)
    }

    /// Split SQL statements using the provided dialect.
    fn split_and_filter_statements(&self, sql: &str) -> Vec<String> {
        let adapter_type = self.adapter_type();
        self.splitter()
            .split(sql, adapter_type)
            .into_iter()
            .filter(|statement| !self.splitter().is_empty(statement, adapter_type))
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
/// (XdbcEngine in Live, Record, and Replay modes).
#[allow(clippy::too_many_arguments)]
pub(crate) fn adbc_execute_with_options(
    engine: &(impl AdapterEngine + ?Sized),
    state: Option<&State>,
    ctx: &QueryCtx,
    conn: &'_ mut dyn Connection,
    sql: &str,
    options: Options,
    fetch: bool,
    token: CancellationToken,
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
            .map_or_else(IndexMap::new, |comment| {
                engine
                    .query_comment()
                    .get_job_labels_from_query_comment(comment)
            });
        if let Some(invocation_id_label) = state
            .lookup("invocation_id", &[])
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
        stmt.set_option(
            OptionStatement::Other(DBT_FETCH.to_string()),
            OptionValue::Int(fetch as i64),
        )?;
        if engine.adapter_type() == AdapterType::Snowflake
            && let Some(traceparent) = read_current_span_start_info(|info| {
                format!("00-{:032x}-{:016x}-01", info.trace_id, info.span_id)
            })
        {
            stmt.set_option(
                OptionStatement::Other("adbc.telemetry.trace_parent".to_string()),
                OptionValue::String(traceparent),
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
        Err(err @ (Cancellable::Cancelled | Cancellable::Error(_))) => {
            let cancelled = || {
                AdapterError::new(
                    AdapterErrorKind::Cancelled,
                    "SQL statement execution was cancelled",
                )
            };
            let (adapter_error, error_message, vendor_code) = match err {
                Cancellable::Cancelled => (cancelled(), None, None),
                // Statements that were running while cancellation was triggered
                // fail with an error here. But that error is a consequence of a
                // forced cancellation, so we check the `CancellationToken` and
                // treat the error as a cancellation and that makes the terminal
                // output much better for users. Nothing went wrong with the SQL
                // execution, it was just killed because the user asked for
                // cancellation.
                Cancellable::Error(_) if token.is_cancelled() => (cancelled(), None, None),
                Cancellable::Error(e) => {
                    let error_message = Some(format!("{:?}: {}", e.status, e.message));
                    let vendor_code = Some(e.vendor_code);
                    let adapter_error = adbc_error_to_adapter_error(e);
                    (adapter_error, error_message, vendor_code)
                }
            };
            let outcome = if adapter_error.kind() == AdapterErrorKind::Cancelled {
                QueryOutcome::Canceled
            } else {
                QueryOutcome::Error
            };
            record_current_span_status_from_attrs(move |attrs| {
                if let Some(attrs) = attrs.downcast_mut::<QueryExecuted>() {
                    attrs.dbt_core_event_code = "E017".to_string();
                    attrs.set_query_outcome(outcome);
                    attrs.query_error_adapter_message = error_message.clone();
                    attrs.query_error_vendor_code = vendor_code;
                }
            });
            return Err(adapter_error);
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
