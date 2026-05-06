use crate::catalog_relation::CatalogRelation;
use crate::column::{BigqueryColumnMode, Column, ColumnBuilder};
use crate::connection::{ConnectionGuard, borrow_tlocal_connection};
use crate::engine::{AdapterEngine, Options as ExecuteOptions, execute_query_with_retry};
use crate::errors::{
    AdapterError, AdapterErrorKind, adbc_error_to_adapter_error, arrow_error_to_adapter_error,
};
use crate::formatter::format_sql_with_bindings;
use crate::information_schema::InformationSchema;
use crate::macro_exec::{
    convert_macro_result_to_record_batch, execute_macro, execute_macro_with_package,
    execute_macro_wrapper_with_package,
};
use crate::metadata::bigquery::BigqueryMetadataAdapter;
use crate::metadata::bigquery::nest_column_data_types;
use crate::metadata::databricks::DatabricksMetadataAdapter;
use crate::metadata::databricks::dbr_capabilities;
use crate::metadata::databricks::version::EngineVersion;
use crate::metadata::duckdb::DuckDBMetadataAdapter;
use crate::metadata::fabric::FabricMetadataAdapter;
use crate::metadata::postgres::PostgresMetadataAdapter;
use crate::metadata::redshift::RedshiftMetadataAdapter;
use crate::metadata::salesforce::SalesforceMetadataAdapter;
use crate::metadata::snowflake::SnowflakeMetadataAdapter;
use crate::metadata::{self, CatalogAndSchema, MetadataAdapter};
use crate::query_ctx::{node_id_from_state, query_ctx_from_state};
use crate::record_batch_utils::{
    RenamedColumn, disambiguate_column_names, extract_first_value_as_i64, get_column_values,
};
use crate::relation::RelationObject;
use crate::relation::config_v2::{ComponentConfigLoader, RelationConfig};
use crate::relation::databricks::config::DatabricksRelationMetadata;
use crate::relation::snowflake::SnowflakeRelation;
use crate::render_constraint::render_column_constraint;
use crate::response::{AdapterResponse, ResultObject};
use crate::snapshots::SnapshotStrategy;
use crate::value::*;
use crate::{AdapterResult, load_catalogs, python};

use adbc_core::options::OptionValue;
use arrow::array::{BooleanArray, RecordBatch, StringArray, TimestampMillisecondArray};
use arrow_array::{Array as _, ArrayRef, Decimal128Array};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use dashmap::DashMap;
use dbt_adapter_core::AdapterType;
use dbt_agate::AgateTable;
use dbt_common::behavior_flags::{Behavior, BehaviorFlag};
use dbt_common::cancellation::CancellationToken;
use dbt_common::tracing::emit::emit_warn_log_message;
use dbt_common::{ErrorCode, FsResult, unexpected_fs_err};
use dbt_schema_store::SchemaStoreTrait;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::common::ConstraintType;
use dbt_schemas::schemas::common::DbtIncrementalStrategy;
use dbt_schemas::schemas::common::DbtMaterialization;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::common::{ClusterConfig, Constraint, ConstraintSupport, PartitionConfig};
use dbt_schemas::schemas::dbt_column::{DbtColumn, DbtColumnRef};
use dbt_schemas::schemas::manifest::BigqueryPartitionConfig;
use dbt_schemas::schemas::profiles::DuckDBPathInfo;
use dbt_schemas::schemas::project::ModelConfig;
use dbt_schemas::schemas::properties::ModelConstraint;
use dbt_schemas::schemas::relations::base::{BaseRelation, ComponentName, TableFormat};
use dbt_schemas::schemas::serde::minijinja_value_to_typed_struct;
use dbt_schemas::schemas::{CommonAttributes, InternalDbtNodeAttributes, InternalDbtNodeWrapper};
use dbt_xdbc::bigquery::*;
use dbt_xdbc::salesforce::DATA_TRANSFORM_RUN_TIMEOUT;
use dbt_xdbc::{Connection, QueryCtx};
use dbt_yaml::Value as YmlValue;
use indexmap::IndexMap;
use minijinja::dispatch_object::DispatchObject;
use minijinja::value::{Object, ValueMap};
use minijinja::{self, invalid_argument, invalid_argument_inner};
use minijinja::{State, Value, args};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

use AdapterType::*;
use InnerAdapter::*;

static CREDENTIAL_IN_COPY_INTO_REGEX: Lazy<Regex> = Lazy::new(|| {
    // This is NOT the same as the Python regex used in dbt-databricks. Rust lacks lookaround.
    // This achieves the same result for the proper structure.  See original at time of port:
    // https://github.com/databricks/dbt-databricks/blob/66f513b960c62ee21c4c399264a41a56853f3d82/dbt/adapters/databricks/utils.py#L19
    Regex::new(r"credential\s*(\(\s*'[\w\-]+'\s*=\s*'.*?'\s*(?:,\s*'[\w\-]+'\s*=\s*'.*?'\s*)*\))")
        .expect("CREDENTIALS_IN_COPY_INTO_REGEX invalid")
});

/// Returns true if all non-null values in a Float64 column have zero fractional parts.
/// Equivalent to Python adapter's `convert_number_type` implementation.
fn try_to_int_col(col: &arrow_array::Float64Array) -> bool {
    col.iter().all(|v| match v {
        None => true,
        Some(f) if f.is_nan() || f.is_infinite() => true,
        Some(f) => f.fract() == 0.0,
    })
}

/// Returns a callback that emits a warning when duplicate column names are renamed.
fn warn_duplicate_columns(node_id: Option<String>) -> impl FnOnce(&[RenamedColumn<'_>]) {
    use std::fmt::Write;

    move |renamed: &[RenamedColumn<'_>]| {
        let mut msg = match &node_id {
            Some(id) => format!(
                "Query for node '{}' returned duplicate column names. \
                 Columns were renamed to ensure uniqueness: ",
                id
            ),
            None => "Query returned duplicate column names. \
                     Columns were renamed to ensure uniqueness: "
                .to_string(),
        };

        for (i, r) in renamed.iter().enumerate() {
            if i > 0 {
                msg.push_str(", ");
            }
            write!(msg, "'{}' -> '{}'", r.original, r.renamed).unwrap();
        }

        emit_warn_log_message(ErrorCode::DuplicateColumns, msg, None);
    }
}

/// Read a boolean adapter config, tolerating the casing variants
/// dbt-core users may write in `profiles.yml`. Missing keys default to
/// `false`; unparseable values return a `Configuration` error.
pub(crate) fn get_bool_config(engine: &dyn AdapterEngine, key: &str) -> AdapterResult<bool> {
    dbt_common::string_utils::try_parse_bool_str(engine.config(key).as_deref(), key)
        .map(|o| o.unwrap_or(false))
        .map_err(|e| AdapterError::new(AdapterErrorKind::Configuration, e.to_string()))
}

pub fn quote_ident(adapter_type: AdapterType, identifier: &str) -> String {
    let q = dbt_adapter_core::quote_char(adapter_type);
    format!("{q}{identifier}{q}")
}

pub fn quote_component(
    adapter_type: AdapterType,
    quoting: &ResolvedQuoting,
    identifier: &str,
    component: ComponentName,
) -> String {
    if quoting.must_quote(component) {
        quote_ident(adapter_type, identifier)
    } else {
        identifier.to_string()
    }
}

/// Returns the relation name for current node from the state.
pub fn database_schema_alias_from_state(state: &State) -> Option<(String, String, String)> {
    let model = state.lookup("model", &[])?;
    let database = model.get_attr("database").ok()?.as_str()?.to_string();
    let schema = model.get_attr("schema").ok()?.as_str()?.to_string();
    let alias = model.get_attr("alias").ok()?.as_str()?.to_string();
    Some((database, schema, alias))
}

/// Checks if the given [BaseRelation] matches the node currently being rendered
pub(crate) fn matches_current_relation(state: &State, relation: &dyn BaseRelation) -> bool {
    if let Some((database, schema, alias)) = database_schema_alias_from_state(state) {
        // Lowercase name comparison because relation names from the local project
        // are user specified, whereas the input relation may have been a normalized name
        // from the warehouse
        relation
            .database_as_str()
            .is_ok_and(|s| s.eq_ignore_ascii_case(&database))
            && relation
                .schema_as_str()
                .is_ok_and(|s| s.eq_ignore_ascii_case(&schema))
            && relation
                .identifier_as_str()
                .is_ok_and(|s| s.eq_ignore_ascii_case(&alias))
    } else {
        false
    }
}

/// Discriminator for the adapter implementation path.
///
/// Used by [AdapterImpl] methods to dispatch between the
/// live-database path and the recorded-trace replay path.
pub enum InnerAdapter<'a> {
    /// The standard implementation for running against live databases.
    Impl(AdapterType, &'a Arc<dyn AdapterEngine>),
    /// Delegates to a replay adapter for recorded trace playback.
    Replay(AdapterType, &'a dyn Replayer),
}

impl AdapterImpl {
    pub fn metadata_adapter(&self) -> Option<Box<dyn MetadataAdapter>> {
        match self.inner_adapter() {
            Replay(_, replay) => replay.metadata_adapter(),
            Impl(_, engine) => {
                // In sidecar mode, schema hydration is handled via db_runner.
                if engine.is_sidecar() {
                    return None;
                }
                // The explicit mock adapter variant has no metadata adapter.
                if self.is_explicit_mock() {
                    return None;
                }
                let engine = Arc::clone(engine);
                let metadata_adapter =
                    match self.adapter_type() {
                        Snowflake => Box::new(SnowflakeMetadataAdapter::new(engine))
                            as Box<dyn MetadataAdapter>,
                        Bigquery => Box::new(BigqueryMetadataAdapter::new(engine))
                            as Box<dyn MetadataAdapter>,
                        Databricks | Spark => Box::new(DatabricksMetadataAdapter::new(engine))
                            as Box<dyn MetadataAdapter>,
                        Redshift => Box::new(RedshiftMetadataAdapter::new(engine))
                            as Box<dyn MetadataAdapter>,
                        Salesforce => {
                            Box::new(SalesforceMetadataAdapter::new()) as Box<dyn MetadataAdapter>
                        }
                        Postgres => Box::new(PostgresMetadataAdapter::new(engine))
                            as Box<dyn MetadataAdapter>,
                        DuckDB => {
                            Box::new(DuckDBMetadataAdapter::new(engine)) as Box<dyn MetadataAdapter>
                        }
                        Fabric => {
                            Box::new(FabricMetadataAdapter::new(engine)) as Box<dyn MetadataAdapter>
                        }
                        ClickHouse => todo!("ClickHouse"),
                        Exasol => return None,
                        Starburst => todo!("Starburst"),
                        Athena => todo!("Athena"),
                        Trino => todo!("Trino"),
                        Datafusion => todo!("Datafusion"),
                        Dremio => todo!("Dremio"),
                        Oracle => todo!("Oracle"),
                    };
                Some(metadata_adapter)
            }
        }
    }

    /// Execute `use warehouse [name]` statement for Snowflake.
    /// For other warehouses, this is noop.
    pub fn use_warehouse(
        &self,
        conn: &'_ mut dyn Connection,
        warehouse: String,
        node_id: &str,
        token: CancellationToken,
    ) -> FsResult<()> {
        match self.inner_adapter() {
            Replay(_, replay) => replay.replay_use_warehouse(conn, warehouse, node_id),
            Impl(Snowflake, _) => {
                let ctx = QueryCtx::default().with_node_id(node_id);
                let sql = format!("use warehouse {warehouse}");
                self.exec_stmt(&ctx, conn, &sql, false, token)?;
                Ok(())
            }
            Impl(..) => {
                debug_assert!(false, "use_warehouse is Snowflake-specific");
                Ok(())
            }
        }
    }

    /// Execute `use warehouse [name]` statement for Snowflake.
    /// For other warehouses, this is noop.
    pub fn restore_warehouse(
        &self,
        conn: &'_ mut dyn Connection,
        node_id: &str,
        token: CancellationToken,
    ) -> FsResult<()> {
        match self.adapter_type() {
            Snowflake => {
                let warehouse = self.get_db_config("warehouse").ok_or_else(|| {
                    unexpected_fs_err!("'warehouse' not found in Snowflake DB config")
                })?;
                let ctx = QueryCtx::default().with_node_id(node_id);
                let sql = format!("use warehouse {warehouse}");
                self.exec_stmt(&ctx, conn, &sql, false, token)?;
            }
            _ => debug_assert!(
                false,
                "only Snowflake adapter should call restore_warehouse"
            ),
        }
        Ok(())
    }

    /// BaseAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/base/impl.py#L655
    pub fn cache_added(
        &self,
        _state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        let _ = self
            .engine()
            .relation_cache()
            .insert_relation(relation, None);
        Ok(none_value())
    }

    /// BaseAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/base/impl.py#L666
    pub fn cache_dropped(
        &self,
        _state: &State,
        relation: &Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        let _ = self
            .engine()
            .relation_cache()
            .evict_relation(relation.as_ref());
        Ok(none_value())
    }

    /// BaseAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/base/impl.py#L678
    pub fn cache_renamed(
        &self,
        _state: &State,
        from_relation: &Arc<dyn BaseRelation>,
        to_relation: &Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        let _ = self
            .engine()
            .relation_cache()
            .rename_relation(from_relation.as_ref(), Arc::clone(to_relation));
        Ok(none_value())
    }

    /// Get DB config by key
    pub fn get_db_config(&self, key: &str) -> Option<Cow<'_, str>> {
        self.engine().config(key)
    }

    pub fn get_db_config_value(&self, key: &str) -> Option<&YmlValue> {
        let engine = self.engine();
        if engine.get_config().contains_key(key) {
            return engine.get_config().get(key);
        }
        None
    }

    /// Returns the table format for `database` (e.g. `DuckLake` for DuckLake-backed databases).
    ///
    /// Mirrors the Python reference implementation in dbt-duckdb:
    /// https://github.com/duckdb/dbt-duckdb/blob/main/dbt/adapters/duckdb/credentials.py
    pub fn table_format_for_database(&self, database: &str) -> TableFormat {
        if self.adapter_type() != DuckDB {
            return TableFormat::Default;
        }

        let path_config = self.get_db_config("path");
        let path_info = DuckDBPathInfo::parse_path(path_config.as_deref());
        let primary_database = self
            .get_db_config("database")
            .map(|value| value.into_owned())
            .unwrap_or_else(|| path_info.database.to_owned());
        let primary_is_ducklake = self
            .get_db_config_value("is_ducklake")
            .and_then(|value| value.as_bool())
            .unwrap_or(false)
            || path_info.is_ducklake;
        if primary_is_ducklake && primary_database.eq_ignore_ascii_case(database) {
            return TableFormat::DuckLake;
        }

        let Some(attach_val) = self.get_db_config_value("attach") else {
            return TableFormat::Default;
        };
        let YmlValue::Sequence(seq, _) = attach_val else {
            return TableFormat::Default;
        };
        for item in seq {
            let YmlValue::Mapping(map, _) = item else {
                continue;
            };
            let Some(path) = map.get("path").and_then(|v| v.as_str()) else {
                continue;
            };
            let attach_info = DuckDBPathInfo::parse_path(Some(path));
            let explicit_ducklake = map
                .get("is_ducklake")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if !explicit_ducklake && !attach_info.is_ducklake {
                continue;
            }

            let attachment_db = map
                .get("alias")
                .and_then(|v| v.as_str())
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| attach_info.database.to_owned());
            if attachment_db.eq_ignore_ascii_case(database) {
                return TableFormat::DuckLake;
            }
        }
        TableFormat::Default
    }

    /// BaseAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/base/impl.py#L1749
    /// PostgresAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-postgres/src/dbt/adapters/postgres/impl.py#L175
    /// RedshiftAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-redshift/src/dbt/adapters/redshift/impl.py#L490
    /// SnowflakeAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-snowflake/src/dbt/adapters/snowflake/impl.py#L502
    pub fn valid_incremental_strategies(&self) -> &[DbtIncrementalStrategy] {
        use DbtIncrementalStrategy::*;

        match self.adapter_type() {
            Postgres | DuckDB => &[Append, DeleteInsert, Merge, Microbatch],
            Snowflake => &[Append, DeleteInsert, InsertOverwrite, Merge, Microbatch],
            Bigquery => &[Append],
            Databricks => &[Append, Merge, InsertOverwrite, ReplaceWhere],
            Redshift => &[Append, DeleteInsert, Merge, Microbatch],
            Fabric => &[Append, DeleteInsert, Merge, Microbatch],
            Salesforce => &[Append, Merge],
            Spark | ClickHouse | Exasol | Athena | Starburst | Trino | Datafusion | Dremio
            | Oracle => {
                unimplemented!("valid_incremental_strategies not implemented")
            }
        }
    }

    /// Redact credentials expressions from DDL statements
    ///
    /// DatabricksAdapter https://github.com/databricks/dbt-databricks/blob/2f11abb306a400cde32b27891b766bf41a11fb1f/dbt/adapters/databricks/impl.py#L833
    pub fn redact_credentials(&self, sql: &str) -> AdapterResult<String> {
        if self.adapter_type() != Databricks {
            return Err(AdapterError::new(
                AdapterErrorKind::NotSupported,
                "redact_credentials is a Databricks-specific function",
            ));
        }
        let Some(caps) = CREDENTIAL_IN_COPY_INTO_REGEX.captures(sql) else {
            // WARN: Malformed input by user means credentials may leak.
            // However, this _is_ the fallback strategy implemented in Python.
            return Ok(sql.to_string());
        };

        // Capture the full matched credential(...) string, including the surrounding parentheses.
        // Then extract only the inner key-value content
        let full_parens = caps.get(1).unwrap().as_str();
        let inner = &full_parens[1..full_parens.len() - 1];

        let redacted_pairs = inner
            .split(',')
            .map(|pair| {
                let key = pair.split('=').next().unwrap_or("").trim();
                format!("{key} = '[REDACTED]'")
            })
            .collect::<Vec<_>>()
            .join(", ");

        let redacted_sql = sql.replacen(full_parens, &format!("({redacted_pairs})"), 1);

        Ok(redacted_sql)
    }

    /// BaseAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/base/impl.py#L505
    pub fn get_partitions_metadata(
        &self,
        _state: &State,
        _relation: &dyn BaseRelation,
    ) -> Result<Value, minijinja::Error> {
        unimplemented!("get_partitions_metadata")
    }

    /// Borrow the current thread-local connection or create one if it's not set yet.
    ///
    /// A guard is returned. When destroyed, the guard returns the connection to
    /// the thread-local variable. If another connection became the thread-local
    /// in the mean time, that connection is dropped and the return proceeds as
    /// normal.
    pub fn borrow_tlocal_connection(
        &self,
        state: Option<&State>,
        node_id: Option<String>,
    ) -> Result<ConnectionGuard<'_>, AdapterError> {
        borrow_tlocal_connection(self.engine().as_ref(), state, node_id)
    }

    /// Helper method for execute
    #[allow(clippy::too_many_arguments)]
    #[inline(always)]
    pub fn execute_inner(
        &self,
        engine: Arc<dyn AdapterEngine>,
        state: Option<&State>,
        conn: &'_ mut dyn Connection,
        ctx: &QueryCtx,
        sql: &str,
        _auto_begin: bool,
        fetch: bool,
        _limit: Option<i64>,
        options: Option<HashMap<String, String>>,
        token: CancellationToken,
    ) -> AdapterResult<(AdapterResponse, AgateTable)> {
        let adapter_type = self.adapter_type();
        // BigQuery and DuckDB support multi-statement execution.
        // BigQuery: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language
        // DuckDB: temp tables are connection-scoped; batching CREATE TEMP + DML in one
        // execute() call avoids the need for cross-call connection caching.
        let statements = if adapter_type == Bigquery || adapter_type == DuckDB {
            if engine.splitter().is_empty(sql, adapter_type) {
                vec![]
            } else {
                vec![sql.to_owned()]
            }
        } else {
            engine.split_and_filter_statements(sql)
        };
        if statements.is_empty() {
            return Ok((AdapterResponse::default(), AgateTable::default()));
        }

        let mut options = options
            .unwrap_or_default()
            .into_iter()
            .map(|(key, value)| (key, OptionValue::String(value)))
            .collect::<Vec<_>>();
        if let Some(state) = state {
            options.extend(self.get_adbc_execute_options(state));
        }

        // Configure warehouse specific options
        #[allow(clippy::single_match)]
        match self.adapter_type() {
            Salesforce => {
                if let Some(timeout) = engine.config("data_transform_run_timeout") {
                    let timeout = timeout.parse::<i64>().map_err(|e| {
                        AdapterError::new(
                            AdapterErrorKind::Configuration,
                            format!("data_transform_run_timeout must be an integer string: {e}",),
                        )
                    })?;
                    options.push((
                        DATA_TRANSFORM_RUN_TIMEOUT.to_string(),
                        OptionValue::Int(timeout),
                    ));
                }
            }
            _ => {}
        }

        let mut last_batch = None;
        for sql in statements {
            last_batch = Some(execute_query_with_retry(
                engine.clone(),
                state,
                conn,
                ctx,
                &sql,
                1,
                &options,
                fetch,
                token.clone(),
            )?);
        }

        let last_batch = last_batch.expect("last_batch should never be None");

        let response = AdapterResponse::new(&last_batch, self.adapter_type());

        // Deduplicate column names to match dbt-core's behavior, which renames
        // duplicate columns to `col_2`, `col_3`, etc.
        // BigQuery is the exception to this deduping
        let last_batch = if self.adapter_type() != Bigquery {
            let node_id = state.and_then(node_id_from_state);
            disambiguate_column_names(last_batch, Some(warn_duplicate_columns(node_id)))
        } else {
            last_batch
        };

        let table = AgateTable::from_record_batch(Arc::new(last_batch));

        Ok((response, table))
    }

    /// BaseAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/base/impl.py#L453
    #[allow(clippy::too_many_arguments)]
    pub fn execute(
        &self,
        state: Option<&State>,
        conn: &'_ mut dyn Connection,
        ctx: Option<&QueryCtx>,
        sql: &str,
        auto_begin: bool,
        fetch: bool,
        limit: Option<i64>,
        options: Option<HashMap<String, String>>,
        token: CancellationToken,
    ) -> AdapterResult<(AdapterResponse, AgateTable)> {
        if self.mock_state().is_some() {
            if !self.introspect_enabled() {
                return Err(AdapterError::new(
                    AdapterErrorKind::NotSupported,
                    "Introspective queries are disabled (--no-introspect).",
                ));
            }
            let response = AdapterResponse {
                message: "execute".to_string(),
                code: sql.to_string(),
                rows_affected: 1,
                query_id: None,
            };

            let schema = Arc::new(Schema::new(vec![Field::new(
                "names",
                DataType::Decimal128(38, 10),
                true,
            )]));
            let decimal_array: ArrayRef = Arc::new(Decimal128Array::from(vec![Some(42)]));
            let batch = RecordBatch::try_new(schema, vec![decimal_array]).unwrap();

            let table = AgateTable::from_record_batch(Arc::new(batch));

            return Ok((response, table));
        }
        let ctx = match ctx.map(Cow::Borrowed) {
            Some(ctx) => ctx,
            None => {
                let ctx = match state {
                    Some(s) => query_ctx_from_state(s)?,
                    None => QueryCtx::default(),
                }
                .with_desc("execute adapter call");
                Cow::Owned(ctx)
            }
        };
        match self.inner_adapter() {
            Replay(_, replay) => replay.replay_execute(
                state,
                conn,
                ctx.as_ref(),
                sql,
                auto_begin,
                fetch,
                limit,
                options,
            ),
            Impl(_, engine) => self.execute_inner(
                Arc::clone(engine),
                state,
                conn,
                ctx.as_ref(),
                sql,
                auto_begin,
                fetch,
                limit,
                options,
                token,
            ),
        }
    }

    /// Execute a statement, expect no results.
    pub fn exec_stmt(
        &self,
        ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        sql: &str,
        auto_begin: bool,
        token: CancellationToken,
    ) -> AdapterResult<AdapterResponse> {
        // default values are the same as in dispatch_adapter_calls()
        let (response, _) = self.execute(
            None,       // empty state
            conn,       // connection
            Some(ctx),  // context around the SQL string
            sql,        // the SQL string
            auto_begin, // auto_begin
            false,      // fetch
            None,       // limit
            None,       // options
            token,
        )?;
        Ok(response)
    }

    /// Execute a query and get results in an [AgateTable].
    pub fn query(
        &self,
        ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        sql: &str,
        limit: Option<i64>,
        token: CancellationToken,
    ) -> AdapterResult<(AdapterResponse, AgateTable)> {
        self.execute(
            None,      // state
            conn,      // connection
            Some(ctx), // context around the SQL string
            sql,       // the SQL string
            false,     // auto_begin
            true,      // fetch
            limit,     // limit
            None,      // options
            token,
        )
    }

    /// SQLAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/sql/impl.py#L55
    /// BigQueryAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L573
    #[allow(clippy::too_many_arguments)]
    pub fn add_query(
        &self,
        state: &State,
        conn: &'_ mut dyn Connection,
        sql: &str,
        auto_begin: bool,
        bindings: Option<&Value>,
        abridge_sql_log: bool,
        token: CancellationToken,
    ) -> AdapterResult<()> {
        if self.mock_state().is_some() {
            unimplemented!("query addition to connection in MockAdapter")
        }
        let sql = if let Some(bindings) = bindings {
            Cow::Owned(format_sql_with_bindings(
                self.adapter_type(),
                sql,
                bindings,
            )?)
        } else {
            Cow::Borrowed(sql)
        };
        let ctx = query_ctx_from_state(state)?.with_desc("add_query adapter call");
        match self.inner_adapter() {
            Replay(_, replay) => replay.replay_add_query(
                &ctx,
                conn,
                sql.as_ref(),
                auto_begin,
                bindings,
                abridge_sql_log,
            ),
            Impl(Bigquery, _) => {
                // Bigquery does not support add_query
                Err(AdapterError::new(
                    AdapterErrorKind::NotSupported,
                    "bigquery.add_query",
                ))
            }
            Impl(_, engine) => {
                self.execute_inner(
                    Arc::clone(engine),
                    None,
                    conn,
                    &ctx,
                    sql.as_ref(),
                    auto_begin,
                    false,
                    None,
                    None,
                    token,
                )?;
                Ok(())
            }
        }
    }

    /// Submit Python job
    ///
    /// Executes Python code in the warehouse's Python runtime.
    /// Default implementation raises Internal error.
    ///
    /// BaseAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/base/impl.py#L1727
    /// SnowflakeAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-snowflake/src/dbt/adapters/snowflake/impl.py#L417
    pub fn submit_python_job(
        &self,
        ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        state: &State,
        model: &Value,
        compiled_code: &str,
        token: CancellationToken,
    ) -> AdapterResult<AdapterResponse> {
        match self.inner_adapter() {
            Impl(Snowflake, engine) => {
                let code = python::snowflake::finalize_python_code(state, model, compiled_code)?;
                let (response, _) = self.execute_inner(
                    Arc::clone(engine),
                    Some(state),
                    conn,
                    ctx,
                    &code,
                    false,
                    false,
                    None,
                    None,
                    token,
                )?;
                Ok(response)
            }
            Replay(Snowflake, replay) => {
                let code = python::snowflake::finalize_python_code(state, model, compiled_code)?;
                // In DBT Replay mode, route through the replay adapter to consume recorded execute calls.
                let (response, _) = replay.replay_execute(
                    Some(state),
                    conn,
                    ctx,
                    &code,
                    false,
                    false,
                    None,
                    None,
                )?;
                Ok(response)
            }
            // https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup#running-python-models-on-bigquery-dataframes
            // https://docs.getdbt.com/reference/resource-configs/bigquery-configs#python-model-configuration
            Impl(Bigquery, _) => python::bigquery::submit_python_job(
                self,
                ctx,
                conn,
                state,
                model,
                compiled_code,
                token,
            ),
            // https://docs.getdbt.com/reference/resource-configs/databricks-configs
            Impl(Databricks, _) => {
                python::databricks::submit_python_job(self, ctx, conn, state, model, compiled_code)
            }
            Replay(Bigquery | Databricks, replay) => {
                replay.replay_submit_python_job(ctx, conn, state, model, compiled_code)
            }
            Replay(
                adapter_type @ (Postgres | Redshift | Salesforce | DuckDB | Spark | Fabric
                | ClickHouse | Exasol | Starburst | Athena | Trino | Datafusion
                | Dremio | Oracle),
                _,
            )
            | Impl(
                adapter_type @ (Postgres | Redshift | Salesforce | DuckDB | Spark | Fabric
                | ClickHouse | Exasol | Starburst | Athena | Trino | Datafusion
                | Dremio | Oracle),
                _,
            ) => Err(AdapterError::new(
                AdapterErrorKind::Internal,
                format!("Python models are not supported for {adapter_type} adapter",),
            )),
        }
    }

    /// Wrap the identifier in the appropriate quoting character for the adapter.
    ///
    /// Assumes the identifier is not quoted.
    ///
    /// SQLAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/sql/impl.py#L214
    pub fn quote(&self, identifier: &str) -> String {
        quote_ident(self.adapter_type(), identifier)
    }

    /// SQLAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/sql/impl.py#L217
    /// AthenaAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-athena/src/dbt/adapters/athena/impl.py#L1154
    /// BigQueryAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L299
    /// SnowflakeAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-snowflake/src/dbt/adapters/snowflake/impl.py#L205
    pub fn list_schemas(&self, result_set: Arc<RecordBatch>) -> AdapterResult<Vec<String>> {
        if self.mock_state().is_some() {
            return Ok(vec![]);
        }
        let schema_column_values = {
            let col_name = match self.adapter_type() {
                Snowflake | Salesforce => "name",
                Databricks | Spark => "databaseName",
                Bigquery => "schema_name",
                Postgres | Redshift => "nspname",
                DuckDB => "schema_name",
                Fabric => "schema",
                ClickHouse => todo!("ClickHouse"),
                Exasol => "name",
                Starburst => todo!("Starburst"),
                Athena => todo!("Athena"),
                Trino => todo!("Trino"),
                Datafusion => todo!("Datafusion"),
                Dremio => todo!("Dremio"),
                Oracle => todo!("Oracle"),
            };
            get_column_values::<StringArray>(&result_set, col_name)?
        };

        let n = result_set.num_rows();
        let mut schemas = Vec::<String>::with_capacity(n);
        for i in 0..n {
            let name: &str = schema_column_values.value(i);
            schemas.push(name.to_string());
        }
        Ok(schemas)
    }

    /// SQLAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/sql/impl.py#L166
    pub fn create_schema(
        &self,
        state: &State,
        relation: &Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        let args = [RelationObject::new(Arc::clone(relation)).into_value()];
        execute_macro(state, &args, "create_schema")?;
        Ok(none_value())
    }

    /// SQLAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/sql/impl.py#L177
    pub fn drop_schema(
        &self,
        state: &State,
        relation: &Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        self.engine()
            .relation_cache()
            .evict_schema_for_relation(relation.as_ref());
        let args = [RelationObject::new(Arc::clone(relation)).into_value()];
        execute_macro(state, &args, "drop_schema")?;
        Ok(none_value())
    }

    /// BaseAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/base/impl.py#L894
    pub fn valid_snapshot_target(
        &self,
        _state: &State,
        _relation: &Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        unimplemented!("valid_snapshot_target")
    }

    /// BaseAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/base/impl.py#L1769
    pub fn get_incremental_strategy_macro(
        &self,
        state: &State,
        strategy: &str,
    ) -> Result<Value, minijinja::Error> {
        if strategy != "default" {
            let strategy_ = DbtIncrementalStrategy::from_str(strategy)
                .map_err(|e| invalid_argument_inner!("Invalid strategy value {}", e))?;
            if !self.valid_incremental_strategies().contains(&strategy_)
                && builtin_incremental_strategies().contains(&strategy_)
            {
                return invalid_argument!(
                    "The incremental strategy '{}' is not valid for this adapter",
                    strategy
                );
            }
        }

        let strategy = strategy.replace("+", "_");
        let macro_name = format!("get_incremental_{strategy}_sql");

        // Return the macro
        Ok(Value::from_object(DispatchObject {
            macro_name,
            package_name: None,
            strict: false,
            auto_execute: false,
            context: Some(state.get_base_context()),
        }))
    }

    /// BaseAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/base/impl.py#L1047
    #[allow(clippy::too_many_arguments)]
    pub fn get_relation(
        &self,
        state: &State,
        ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        database: &str,
        schema: &str,
        identifier: &str,
        token: CancellationToken,
    ) -> AdapterResult<Option<Arc<dyn BaseRelation>>> {
        if self.mock_state().is_some() {
            if !self.introspect_enabled() {
                return Err(AdapterError::new(
                    AdapterErrorKind::NotSupported,
                    "Introspective queries are disabled (--no-introspect).",
                ));
            }
            return Ok(Some(Arc::new(SnowflakeRelation::new(
                Some(database.to_string()),
                Some(schema.to_string()),
                Some(identifier.to_string()),
                None,
                TableFormat::Default,
                self.quoting(),
            ))));
        }
        match self.inner_adapter() {
            Replay(_, replay) => {
                replay.replay_get_relation(state, ctx, conn, database, schema, identifier)
            }
            Impl(adapter_type, engine) if engine.is_sidecar() => {
                let client = engine.sidecar_client().unwrap();
                let query_schema = schema.to_string();
                let query_identifier = identifier.to_string();
                let relation_type = client.get_relation_type(&query_schema, &query_identifier)?;
                match relation_type {
                    Some(rel_type) => {
                        let relation = crate::relation::do_create_relation(
                            adapter_type,
                            database.to_string(),
                            schema.to_string(),
                            Some(identifier.to_string()),
                            Some(rel_type),
                            self.quoting(),
                        )?;
                        Ok(Some(relation.into()))
                    }
                    None => Ok(None),
                }
            }
            Impl(_, _engine) => {
                let relation_opt = metadata::get_relation::get_relation(
                    self, state, ctx, conn, database, schema, identifier, token,
                )?;
                let relation =
                    relation_opt.map(|relation| -> Arc<dyn BaseRelation> { relation.into() });
                Ok(relation)
            }
        }
    }

    /// Get a catalog relation, which in Core is a serialized type.
    /// In Fusion, we treat it as a Jinja accessible flat container of values
    /// needed for Iceberg ddl generation.
    ///
    /// BaseAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/base/impl.py#L350
    /// BigQueryAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L1384
    /// SnowflakeAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-snowflake/src/dbt/adapters/snowflake/impl.py#L539
    pub fn build_catalog_relation(&self, model: &Value) -> AdapterResult<CatalogRelation> {
        CatalogRelation::from_model_config_and_catalogs(
            &self.adapter_type(),
            model,
            load_catalogs::fetch_catalogs(),
        )
    }

    /// Get all relevant metadata about a dynamic table
    ///
    /// SnowflakeAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-snowflake/src/dbt/adapters/snowflake/impl.py#L559
    pub fn describe_dynamic_table(
        &self,
        state: &State,
        conn: &'_ mut dyn Connection,
        relation: &Arc<dyn BaseRelation>,
        include_transient: bool,
        token: CancellationToken,
    ) -> Result<Value, minijinja::Error> {
        let adapter_type = self.adapter_type();
        match adapter_type {
            Snowflake => {
                let ctx = query_ctx_from_state(state)?.with_desc("describe_dynamic_table");

                let quoting = relation.quote_policy();

                let schema = if quoting.schema {
                    relation.schema_as_quoted_str()?
                } else {
                    relation.schema_as_str()?
                };

                let database = if quoting.database {
                    relation.database_as_quoted_str()?
                } else {
                    relation.database_as_str()?
                };

                let show_sql = format!(
                    "show dynamic tables like '{}' in schema {database}.{schema}",
                    relation.identifier_as_str()?
                );

                let (_, table) = self.query(&ctx, conn, &show_sql, None, token.clone())?;

                let table = table
                    .rename(Some(table.column_names()), None, false, false)?
                    .select(&[
                        "name".to_string(),
                        "schema_name".to_string(),
                        "database_name".to_string(),
                        "text".to_string(),
                        "target_lag".to_string(),
                        "scheduler".to_string(),
                        "warehouse".to_string(),
                        "refresh_mode".to_string(),
                        "initialization_warehouse".to_string(),
                        "immutable_where".to_string(),
                        "cluster_by".to_string(),
                    ]);

                // SHOW DYNAMIC TABLES does not expose transient status, so we need to run SHOW
                // TABLES if we need to check transient
                let table = if include_transient {
                    let show_tables_sql = format!(
                        "show tables like '{}' in schema {database}.{schema}",
                        relation.identifier_as_str()?
                    );
                    let (_, tables) = self.query(&ctx, conn, &show_tables_sql, None, token)?;
                    let tables = tables.rename(Some(tables.column_names()), None, false, false)?;
                    let tables_batch = tables.to_record_batch();
                    let is_transient = if tables_batch.num_rows() > 0 {
                        get_column_values::<StringArray>(&tables_batch, "kind")
                            .ok()
                            .map(|col| col.value(0).eq_ignore_ascii_case("TRANSIENT"))
                            .unwrap_or(false)
                    } else {
                        false
                    };

                    // Fold the transient column into the SHOW DYNAMIC TABLES result
                    let record_batch = table.to_record_batch();
                    let num_rows = record_batch.num_rows();
                    let transient_col: ArrayRef =
                        Arc::new(BooleanArray::from(vec![Some(is_transient); num_rows]));
                    let mut fields: Vec<Arc<Field>> =
                        record_batch.schema().fields().iter().cloned().collect();
                    fields.push(Arc::new(Field::new("transient", DataType::Boolean, true)));
                    let new_schema = Arc::new(Schema::new(fields));
                    let mut columns = record_batch.columns().to_vec();
                    columns.push(transient_col);
                    let new_batch = RecordBatch::try_new(new_schema, columns).map_err(|e| {
                        minijinja::Error::new(minijinja::ErrorKind::InvalidOperation, e.to_string())
                    })?;
                    AgateTable::from_record_batch(Arc::new(new_batch))
                } else {
                    table
                };

                Ok(Value::from(ValueMap::from([(
                    Value::from("dynamic_table"),
                    Value::from_object(table),
                )])))
            }
            Postgres | Bigquery | Databricks | Redshift | Salesforce | Spark | DuckDB | Fabric
            | ClickHouse | Exasol | Starburst | Athena | Trino | Datafusion | Dremio | Oracle => {
                let err = format!(
                    "describe_dynamic_table is not supported by the {} adapter",
                    adapter_type
                );
                Err(minijinja::Error::new(
                    minijinja::ErrorKind::InvalidOperation,
                    err,
                ))
            }
        }
    }

    /// SQLAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/sql/impl.py#L145
    pub fn drop_relation(
        &self,
        state: &State,
        relation: &Arc<dyn BaseRelation>,
    ) -> AdapterResult<Value> {
        if self.mock_state().is_some() {
            return Ok(none_value());
        }
        match self.inner_adapter() {
            Replay(_, replay) => replay.replay_drop_relation(state, relation),
            Impl(_, _engine) => {
                if relation.relation_type().is_none() {
                    return Err(AdapterError::new(
                        AdapterErrorKind::Configuration,
                        "relation has no type",
                    ));
                }
                let args = vec![RelationObject::new(Arc::clone(relation)).into_value()];
                execute_macro(state, &args, "drop_relation")?;
                Ok(none_value())
            }
        }
    }

    /// SQLAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/sql/impl.py#L222
    pub fn check_schema_exists(
        &self,
        state: &State,
        database: &str,
        schema: &str,
    ) -> Result<Value, minijinja::Error> {
        // Replay fast-path: consult trace-derived cache if available
        if let Replay(..) = self.inner_adapter() {
            // TODO: move this logic to the [ReplayAdapter]
            if let Some(exists) = self.schema_exists_from_trace(database, schema) {
                return Ok(Value::from(exists));
            }
        }

        let information_schema = InformationSchema {
            adapter_type: self.adapter_type(),
            database: Some(database.to_string()),
            schema: "INFORMATION_SCHEMA".to_string(),
            identifier: None,
            location: None,
            is_delta: None,
        };

        let (package_name, macro_name) = self.check_schema_exists_macro(state, &[])?;
        let batch = execute_macro_wrapper_with_package(
            state,
            &[
                RelationObject::new(Arc::new(information_schema)).into_value(),
                Value::from(schema),
            ],
            &macro_name,
            &package_name,
        )?;

        match extract_first_value_as_i64(&batch) {
            Some(0) => Ok(Value::from(false)),
            Some(1) => Ok(Value::from(true)),
            _ => Err(minijinja::Error::new(
                minijinja::ErrorKind::ReturnValue,
                "invalid return value",
            )),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn get_relations_by_pattern(
        &self,
        state: &State,
        schema_pattern: &str,
        table_pattern: &str,
        exclude: Option<&str>,
        database: Option<&str>,
        quote_table: Option<bool>,
        excluded_schemas: Option<Value>,
    ) -> Result<Value, minijinja::Error> {
        // Validate excluded_schemas if provided
        if let Some(ref schemas) = excluded_schemas {
            let _ =
                minijinja_value_to_typed_struct::<Vec<String>>(schemas.clone()).map_err(|e| {
                    minijinja::Error::new(
                        minijinja::ErrorKind::SerdeDeserializeError,
                        e.to_string(),
                    )
                })?;
        }

        // Get default database from state if not provided
        let database_str = if let Some(db) = database {
            db.to_string()
        } else {
            let target = state.lookup("target", &[]).ok_or_else(|| {
                minijinja::Error::new(
                    minijinja::ErrorKind::InvalidOperation,
                    "target is not set in state",
                )
            })?;
            let db_value = target.get_attr("database").unwrap_or_default();
            db_value.as_str().unwrap_or_default().to_string()
        };

        // Build args array for macro call
        // Note: For optional string parameters like 'exclude', we pass empty string instead of None
        // because the macro expects a string and None gets converted to "none" string
        let args = vec![
            Value::from(schema_pattern),
            Value::from(table_pattern),
            exclude.map(Value::from).unwrap_or_else(|| Value::from("")),
            Value::from(database_str.as_str()),
            quote_table
                .map(Value::from)
                .unwrap_or_else(|| Value::from(false)),
            excluded_schemas.unwrap_or_else(|| Value::from_iter::<Vec<String>>(vec![])),
        ];

        let result = execute_macro(state, &args, "get_relations_by_pattern_internal")?;
        Ok(result)
    }

    /// Get the full macro name for check_schema_exists
    ///
    /// # Returns
    ///
    /// Returns (package_name, macro_name)
    pub fn check_schema_exists_macro(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> AdapterResult<(String, String)> {
        if matches!(self.adapter_type(), Databricks | Spark) {
            Ok((
                "dbt_spark".to_string(),
                "spark__check_schema_exists".to_string(),
            ))
        } else {
            Ok(("dbt".to_string(), "check_schema_exists".to_string()))
        }
    }

    /// Determine if the current Databricks connection points to a classic
    /// cluster (as opposed to a SQL warehouse).
    ///
    /// DatabricksAdapter https://github.com/databricks/dbt-databricks/blob/2f11abb306a400cde32b27891b766bf41a11fb1f/dbt/adapters/databricks/impl.py#L961
    pub fn is_cluster(&self) -> AdapterResult<bool> {
        if self.adapter_type() != Databricks {
            return Err(AdapterError::new(
                AdapterErrorKind::NotSupported,
                "is_cluster is only available for the Databricks adapter",
            ));
        }

        let http_path = self
            .engine()
            .get_config()
            .get_string("http_path")
            .ok_or_else(|| {
                AdapterError::new(
                    AdapterErrorKind::Configuration,
                    "http_path is required to determine Databricks compute type",
                )
            })?;

        let normalized = http_path.trim().to_ascii_lowercase();
        if normalized.contains("/warehouses/") {
            return Ok(false);
        }
        if normalized.contains("/protocolv1/") {
            return Ok(true);
        }
        Ok(false)
    }

    /// Returns true if the adapter supports the given feature.
    pub fn has_feature(
        &self,
        state: &State,
        name: &str,
        token: CancellationToken,
    ) -> AdapterResult<Option<bool>> {
        // PRE-CONDITION: adapter_type is DuckDB
        let is_motherduck = |engine: &dyn AdapterEngine| {
            engine
                .config("path")
                .map(|p| dbt_auth::is_motherduck_path(&p))
                .unwrap_or(false)
        };

        match (self.adapter_type(), name) {
            (DuckDB, "motherduck") => Ok(Some(is_motherduck(self.engine().as_ref()))),
            (DuckDB, "transactions") => {
                // MotherDuck does not support explicit transactions
                Ok(Some(!is_motherduck(self.engine().as_ref())))
            }
            // Assume that all other adapters support transactions for now.
            (_, "transactions") => Ok(Some(true)),
            (Redshift, "datasharing") => Ok(Some(get_bool_config(
                self.engine().as_ref(),
                "datasharing",
            )?)),
            (Databricks, _) => {
                let mut conn =
                    self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
                let has_capability = self.has_dbr_capability(state, conn.as_mut(), name, token)?;
                Ok(Some(has_capability))
            }
            _ => {
                emit_warn_log_message(
                    ErrorCode::InvalidArgument,
                    format!(
                        "Unrecognized feature: {} for {} adapter",
                        name,
                        self.adapter_type()
                    ),
                    None,
                );
                // None is falsy, so features should be named in such a way that
                // `false` is the most reasonable assumption.
                Ok(None)
            }
        }
    }

    /// Returns a dict with database/schema/identifier for temp tables on MotherDuck.
    pub fn get_temp_relation_path(
        &self,
        database: &str,
        identifier: &str,
        batch_id: &str,
    ) -> AdapterResult<BTreeMap<String, Value>> {
        let mut path = BTreeMap::new();
        path.insert("database".to_owned(), Value::from(database));
        path.insert("schema".to_owned(), Value::from("dbt_temp"));
        path.insert(
            "identifier".to_owned(),
            Value::from(format!("{identifier}__{batch_id}")),
        );
        Ok(path)
    }

    /// Rename relation
    ///
    /// SQLAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/sql/impl.py#L155
    pub fn rename_relation(
        &self,
        state: &State,
        from_relation: &Arc<dyn BaseRelation>,
        to_relation: &Arc<dyn BaseRelation>,
    ) -> AdapterResult<Value> {
        match self.inner_adapter() {
            Replay(_, replay) => replay.replay_rename_relation(state, from_relation, to_relation),
            Impl(_, _engine) => {
                // Execute the macro with the relation objects
                let args = vec![
                    RelationObject::new(Arc::clone(from_relation)).into_value(),
                    RelationObject::new(Arc::clone(to_relation)).into_value(),
                ];

                let _empty_retval = execute_macro(state, &args, "rename_relation")?;
                Ok(none_value())
            }
        }
    }

    /// Returns the columns that exist in the source_relations but not in the target_relations
    ///
    /// BaseAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/base/impl.py#L862
    pub fn get_missing_columns(
        &self,
        state: &State,
        source_relation: &Arc<dyn BaseRelation>,
        target_relation: &Arc<dyn BaseRelation>,
    ) -> AdapterResult<Vec<Column>> {
        match self.inner_adapter() {
            Replay(_, replay) => {
                replay.replay_get_missing_columns(state, source_relation, target_relation)
            }
            Impl(_, _engine) => {
                // Get columns for both relations
                let source_cols = self.get_columns_in_relation(state, source_relation.as_ref())?;
                let target_cols = self.get_columns_in_relation(state, target_relation.as_ref())?;

                let source_cols_map: BTreeMap<_, _> = source_cols
                    .into_iter()
                    .map(|col| (col.name().to_string(), col))
                    .collect();
                let target_cols_set: std::collections::HashSet<_> =
                    target_cols.into_iter().map(|col| col.into_name()).collect();

                Ok(source_cols_map
                    .into_iter()
                    .filter_map(|(name, col)| {
                        if target_cols_set.contains(&name) {
                            None
                        } else {
                            Some(col)
                        }
                    })
                    .collect())
            }
        }
    }

    fn get_columns_in_relation_via_adbc(
        &self,
        state: &State,
        relation: &dyn BaseRelation,
    ) -> AdapterResult<Vec<Column>> {
        let conn = self.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
        let schema = conn
            .get_table_schema(
                relation.database(),
                relation.schema(),
                relation.identifier().ok_or_else(|| {
                    AdapterError::new(
                        AdapterErrorKind::UnexpectedResult,
                        "relation does not have identifier",
                    )
                })?,
            )
            .map_err(|e| AdapterError::new(AdapterErrorKind::Driver, e.to_string()))?;
        // NOTE: it's okay to skip conversion to an SDF-frontend since
        // `schema_to_columns()` will first try to parse the type from the
        // `PLATFORM:type` metadata key returned by the driver.
        self.schema_to_columns(None, &Arc::new(schema))
    }

    /// Get columns in relation
    ///
    /// SQLAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/sql/impl.py#L161
    /// AthenaAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-athena/src/dbt/adapters/athena/impl.py#L1217
    /// BigQueryAdapter https://github.com/dbt-labs/dbt-adapters/blob/fe308ee83cfc200b6ff196f8662b9882d7cec505/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L330
    /// SnowflakeAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-snowflake/src/dbt/adapters/snowflake/impl.py#L216
    /// SparkAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-spark/src/dbt/adapters/spark/impl.py#L318
    /// DatabricksAdapter https://github.com/databricks/dbt-databricks/blob/822b105b15e644676d9e1f47cbfd765cd4c1541f/dbt/adapters/databricks/impl.py#L454
    pub fn get_columns_in_relation(
        &self,
        state: &State,
        relation: &dyn BaseRelation,
    ) -> AdapterResult<Vec<Column>> {
        // Mock adapter: return fake column without executing jinja macro
        if self.engine().is_mock() {
            return Ok(vec![Column::new(
                self.adapter_type(),
                "one".to_string(),
                "text".to_string(),
                Some(256),
                None,
                None,
            )]);
        }

        // Sidecar adapter: delegate to sidecar client
        if let Some(client) = self.engine().sidecar_client() {
            let database = relation.database_as_str()?;
            let schema = relation.schema_as_str()?;
            let identifier = relation.identifier_as_str()?;
            let relation_name = format!("{}.{}.{}", database, schema, identifier);
            let column_infos = client.get_columns(&relation_name)?;
            let columns = column_infos
                .into_iter()
                .map(|info| {
                    Column::new(
                        self.adapter_type(),
                        info.name,
                        info.data_type,
                        None,
                        None,
                        None,
                    )
                })
                .collect();
            return Ok(columns);
        }

        if self.adapter_type() == Bigquery {
            return self.get_columns_in_relation_via_adbc(state, relation);
        }

        let macro_execution_result: AdapterResult<Value> = match self.adapter_type() {
            // Bigquery early returns from `get_columns_in_relation_via_adbc()`
            Bigquery => unreachable!(),
            Databricks => {
                // use DESCRIBE TABLE EXTENDED ... AS JSON for full type strings
                // Plain DESCRIBE TABLE truncates long data types server-side
                //
                // https://github.com/databricks/dbt-databricks/blob/822b105b15e644676d9e1f47cbfd765cd4c1541f/dbt/adapters/databricks/impl.py#L439-L452
                //
                // Note: is_hive_metastore() returns false for Unity Catalog temporary tables (matching Python semantics).
                // The `temporary` field only tracks UC temporary tables, not HMS temporary views.
                let use_legacy = relation.is_hive_metastore()
                    || relation.is_materialized_view()
                    || relation.is_streaming_table();

                if !use_legacy {
                    let json_result = execute_macro_with_package(
                        state,
                        &[RelationObject::new(relation.to_owned()).into_value()],
                        "get_columns_comments_as_json",
                        "dbt_databricks",
                    );
                    match json_result {
                        Ok(ref val) => {
                            if let Some(columns) = self.try_columns_from_json_describe(val) {
                                return columns;
                            }
                        }
                        Err(ref e) => {
                            if e.message().contains("[TABLE_OR_VIEW_NOT_FOUND]") {
                                return Ok(Vec::new());
                            }
                            // PARSE_SYNTAX_ERROR / UNSUPPORTED_FEATURE -> DBR < 16.2;
                            // fall through to legacy DESCRIBE TABLE
                        }
                    }
                }

                execute_macro_with_package(
                    state,
                    &[RelationObject::new(relation.to_owned()).into_value()],
                    "get_columns_comments",
                    "dbt_databricks",
                )
            }
            Postgres | Snowflake | Redshift | DuckDB | Fabric | Spark => execute_macro(
                state,
                &[RelationObject::new(relation.to_owned()).into_value()],
                "get_columns_in_relation",
            ),
            Salesforce | ClickHouse | Exasol | Starburst | Athena | Trino | Datafusion | Dremio
            | Oracle => {
                unimplemented!("get_columns_in_relation not implemented")
            }
        };

        match self.adapter_type() {
            // Bigquery early returns from `get_columns_in_relation_via_adbc()`
            Bigquery => unreachable!(),
            adapter_type @ (Postgres | Redshift | Fabric | DuckDB | Spark) => {
                let result = macro_execution_result?;
                Ok(Column::vec_from_jinja_value(adapter_type, result)?)
            }
            Snowflake => {
                let result = match macro_execution_result {
                    Ok(result) => result,
                    Err(err) => {
                        // TODO: switch to checking the vendor error code when available.
                        // See https://github.com/dbt-labs/fs/pull/4267#discussion_r2182835729
                        if err.message().contains("does not exist or not authorized") {
                            return Ok(Vec::new());
                        }
                        return Err(err);
                    }
                };

                Ok(Column::vec_from_jinja_value(Snowflake, result)?)
            }
            Databricks => {
                // Databricks inherits the implementation from the Spark adapter.
                //
                // The DESCRIBE TABLE output includes metadata sections (e.g. "# Partition Information",
                // "# Clustering Information") that must be filtered out. This matches the Python
                // Spark adapter behavior which filters rows where col_name starts with '#'.
                //
                // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-spark/src/dbt/adapters/spark/impl.py#L317-L336
                // https://github.com/dbt-labs/dbt-fusion/issues/1230
                let result = match macro_execution_result
                    .and_then(|r| convert_macro_result_to_record_batch(&r))
                {
                    Ok(result) => result,
                    Err(err) => {
                        // TODO: switch to checking the vendor error code when available.
                        // See https://github.com/dbt-labs/fs/pull/4267#discussion_r2182835729
                        // Only checks for the observed Databricks error message, avoiding
                        // all messages in the reference python Spark adapter.
                        if err.message().contains("[TABLE_OR_VIEW_NOT_FOUND]") {
                            return Ok(Vec::new());
                        }
                        return Err(err);
                    }
                };

                let name_string_array = get_column_values::<StringArray>(&result, "col_name")?;
                let dtype_string_array = get_column_values::<StringArray>(&result, "data_type")?;
                let comment_string_array =
                    get_column_values::<StringArray>(&result, "comment").ok();

                // Filter out metadata rows (like "# Partition Information", "# Clustering Information")
                // These are section headers in DESCRIBE TABLE output, not actual columns.
                let columns = (0..name_string_array.len())
                    .filter(|&i| !name_string_array.value(i).starts_with('#'))
                    .map(|i| {
                        let comment = comment_string_array.as_ref().and_then(|arr| {
                            if arr.is_null(i) {
                                None
                            } else {
                                let s = arr.value(i);
                                if s.is_empty() {
                                    None
                                } else {
                                    Some(s.to_string())
                                }
                            }
                        });

                        Column::new(
                            Databricks,
                            name_string_array.value(i).to_string(),
                            dtype_string_array.value(i).to_string(),
                            None, // char_size
                            None, // numeric_precision
                            None, // numeric_scale
                        )
                        .with_comment(comment)
                    })
                    .collect::<Vec<_>>();
                Ok(columns)
            }
            Salesforce | ClickHouse | Exasol | Starburst | Athena | Trino | Datafusion | Dremio
            | Oracle => {
                unimplemented!("get_columns_in_relation not implemented")
            }
        }
    }

    /// Try to parse columns from a `DESCRIBE TABLE EXTENDED ... AS JSON` result.
    ///
    /// Returns `Some(Ok(columns))` on success, `Some(Err(...))` on hard failure,
    /// or `None` if the result couldn't be parsed as JSON metadata (caller should
    /// fall back to plain DESCRIBE TABLE).
    fn try_columns_from_json_describe(&self, result: &Value) -> Option<AdapterResult<Vec<Column>>> {
        use crate::metadata::MetadataProcessor as _;
        use crate::metadata::databricks::describe_table::DatabricksTableMetadata;

        let batch = match convert_macro_result_to_record_batch(result) {
            Ok(b) => b,
            Err(_) => return None,
        };
        let metadata = match DatabricksTableMetadata::from_record_batch(batch) {
            Ok(m) => m,
            Err(_) => return None,
        };

        let columns = metadata
            .columns
            .iter()
            .map(|col| {
                let comment = col.comment.clone().filter(|s| !s.is_empty());
                Column::new(
                    Databricks,
                    col.name.clone(),
                    col.type_.sql_type(),
                    None,
                    None,
                    None,
                )
                .with_comment(comment)
            })
            .collect();
        Some(Ok(columns))
    }

    /// Truncate relation
    ///
    /// BaseAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/base/impl.py#L745
    /// BigQueryAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L257
    pub fn truncate_relation(
        &self,
        state: &State,
        relation: &Arc<dyn BaseRelation>,
    ) -> AdapterResult<Value> {
        match self.inner_adapter() {
            Replay(_, replay) => replay.replay_truncate_relation(state, relation),
            Impl(Bigquery, _) => {
                // BigQuery does not support truncate_relation
                Err(AdapterError::new(
                    AdapterErrorKind::NotSupported,
                    "bigquery.truncate_relation",
                ))
            }
            Impl(
                Snowflake | Databricks | Redshift | Salesforce | Postgres | Spark | DuckDB | Fabric
                | ClickHouse | Exasol | Starburst | Athena | Trino | Datafusion | Dremio | Oracle,
                _,
            ) => {
                // downcast relation
                let relation = RelationObject::new(Arc::clone(relation)).into_value();
                execute_macro(state, &[relation], "truncate_relation")?;
                Ok(none_value())
            }
        }
    }

    /// Quote as configured
    ///
    /// BaseAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/base/impl.py#L1103
    pub fn quote_as_configured(
        &self,
        _state: &State,
        identifier: &str,
        quote_key: &ComponentName,
    ) -> AdapterResult<String> {
        if self.quoting().get_part(quote_key) {
            Ok(self.quote(identifier))
        } else {
            Ok(identifier.to_string())
        }
    }

    /// Quote seed column, default to true if not provided
    ///
    /// BaseAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/base/impl.py#L1124
    /// AthenaAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-athena/src/dbt/adapters/athena/impl.py#L452
    /// SnowflakeAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-snowflake/src/dbt/adapters/snowflake/impl.py#L381
    pub fn quote_seed_column(
        &self,
        state: &State,
        column: &str,
        quote_config: Option<bool>,
    ) -> AdapterResult<String> {
        match self.inner_adapter() {
            Replay(_, replay) => replay.replay_quote_seed_column(state, column, quote_config),
            Impl(Snowflake | Salesforce, _) => {
                // Snowflake is special and defaults quoting to false if config is not provided
                if quote_config.unwrap_or(false) {
                    Ok(self.quote(column))
                } else {
                    Ok(column.to_string())
                }
            }
            Impl(
                Postgres | Bigquery | Databricks | Redshift | Spark | DuckDB | Fabric | ClickHouse
                | Exasol | Starburst | Athena | Trino | Datafusion | Dremio | Oracle,
                _,
            ) => {
                if quote_config.unwrap_or(true) {
                    Ok(self.quote(column))
                } else {
                    Ok(column.to_string())
                }
            }
        }
    }

    /// BaseAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/base/impl.py#L1231
    pub fn convert_type(
        &self,
        state: &State,
        table: Arc<AgateTable>,
        col_idx: i64,
    ) -> AdapterResult<String> {
        if self.mock_state().is_some() {
            unimplemented!("type conversion from table column in MockAdapter")
        }
        let batch = table.original_record_batch();
        let schema = batch.schema();
        let data_type = schema.field(col_idx as usize).data_type();

        let data_type = match data_type {
            dt if dt.is_null() => &DataType::Int32,
            DataType::Float64 => {
                let is_int = batch
                    .column(col_idx as usize)
                    .as_any()
                    .downcast_ref::<arrow_array::Float64Array>()
                    .is_some_and(try_to_int_col);
                if is_int { &DataType::Int64 } else { data_type }
            }
            dt => dt,
        };

        match self.inner_adapter() {
            Replay(_, replay) => replay.replay_convert_type(state, data_type),
            Impl(_, engine) => {
                let mut out = String::new();
                engine
                    .type_ops()
                    .format_arrow_type_as_sql(data_type, &mut out)?;
                Ok(out)
            }
        }
    }

    /// Expand the to_relation table's column types to match the schema of from_relation
    ///
    /// BaseAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/base/impl.py#L951
    pub fn expand_target_column_types(
        &self,
        state: &State,
        from_relation: &Arc<dyn BaseRelation>,
        to_relation: &Arc<dyn BaseRelation>,
    ) -> AdapterResult<Value> {
        match self.inner_adapter() {
            Replay(_, replay) => {
                replay.replay_expand_target_column_types(state, from_relation, to_relation)
            }
            Impl(Bigquery, _) | Impl(DuckDB, _) => {
                // This method is a noop for BigQuery and DuckDB.
                // BigQuery: https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L260-L261
                // DuckDB: type widening (e.g. INT→BIGINT) is handled implicitly;
                // real mismatches surface as SQL errors.
                Ok(none_value())
            }
            Impl(_, _) => {
                let from_columns = self.get_columns_in_relation(state, from_relation.as_ref())?;
                let to_columns = self.get_columns_in_relation(state, to_relation.as_ref())?;

                // Create HashMaps for efficient lookup
                let from_columns_map = from_columns
                    .into_iter()
                    .map(|c| (c.name().to_string(), c))
                    .collect::<BTreeMap<_, _>>();

                let to_columns_map = to_columns
                    .into_iter()
                    .map(|c| (c.name().to_string(), c))
                    .collect::<BTreeMap<_, _>>();

                for (column_name, reference_column) in from_columns_map {
                    let to_relation_cloned = to_relation.clone();
                    if let Some(target_column) = to_columns_map.get(&column_name)
                        && target_column.can_expand_to(&reference_column)?
                    {
                        let col_string_size = reference_column.string_size().map_err(|msg| {
                            AdapterError::new(AdapterErrorKind::UnexpectedResult, msg)
                        })?;
                        let mut new_type = reference_column
                            .as_static()
                            .string_type(Some(col_string_size as usize));

                        // Preserve collation from the target (existing) column
                        if let Some(collation) = target_column.collation() {
                            new_type = format!("{new_type} collate '{collation}'");
                        }

                        // Create args for macro execution
                        execute_macro(
                            state,
                            args!(
                                relation => RelationObject::new(to_relation_cloned).into_value(),
                                column_name => column_name,
                                new_column_type => Value::from(new_type),
                            ),
                            "alter_column_type",
                        )?;
                    }
                }
                Ok(none_value())
            }
        }
    }

    /// This was update_columns method from bigquery-adapter where googleapi is used to
    /// update/merge columns in general
    ///
    /// But since internally this is is only used to update columns descriptions, by
    /// bigquery__alter_column_comment macro and due to limitation of bigquery, we cannot update
    /// nested columns using SQL the implementation here only supports columns descriptions update
    pub fn update_columns_descriptions(
        &self,
        state: &State,
        conn: &'_ mut dyn Connection,
        relation: &Arc<dyn BaseRelation>,
        columns: IndexMap<String, DbtColumn>,
        token: CancellationToken,
    ) -> AdapterResult<Value> {
        match self.adapter_type() {
            Bigquery => {
                let database = relation.database_as_str()?;
                let table = relation.identifier_as_str()?;
                let schema = relation.schema_as_str()?;

                let columns = self.do_nest_column_data_types(columns, None)?;

                let column_to_description = columns
                    .iter()
                    .filter_map(|(name, col)| {
                        col.description
                            .as_ref()
                            .map(|desc| (name.to_string(), desc.to_string()))
                    })
                    .collect::<BTreeMap<String, String>>();

                // The heavy lift is delegated to the driver via googleapi Table.update
                // since ALTER TABLE ... ALTER COLUMNS doesn't support updating a view
                let mut options = self.get_adbc_execute_options(state);
                options.extend(vec![
                    (
                        QUERY_DESTINATION_TABLE.to_string(),
                        OptionValue::String(format!("{database}.{schema}.{table}")),
                    ),
                    (
                        UPDATE_TABLE_COLUMNS_DESCRIPTION.to_string(),
                        OptionValue::String(
                            serde_json::to_string(&column_to_description)
                                .expect("Failed to serialize column_to_description"),
                        ),
                    ),
                ]);

                let ctx = query_ctx_from_state(state)?;
                let sql = "none";
                self.engine().execute_with_options(
                    Some(state),
                    &ctx,
                    conn,
                    sql,
                    options,
                    false,
                    token,
                )?;
                Ok(none_value())
            }
            Postgres | Snowflake | Databricks | Redshift | Salesforce | Spark | Fabric | DuckDB
            | ClickHouse | Exasol | Starburst | Athena | Trino | Datafusion | Dremio | Oracle => {
                unimplemented!("only available with BigQuery adapter")
            }
        }
    }

    /// render_raw_columns_constraints
    ///
    /// BaseAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/base/impl.py#L1848
    pub fn render_raw_columns_constraints(
        &self,
        columns_map: IndexMap<String, DbtColumn>,
    ) -> AdapterResult<Vec<String>> {
        match self.adapter_type() {
            Postgres | Snowflake | Databricks | Redshift | Salesforce | Spark | DuckDB | Fabric
            | ClickHouse | Exasol | Starburst | Athena | Trino | Datafusion | Dremio | Oracle => {
                let mut result = vec![];
                for (_, column) in columns_map {
                    let col_name = if column.quote.unwrap_or(false) {
                        self.quote(&column.name)
                    } else {
                        column.name.clone()
                    };
                    let mut rendered_column_constraint = vec![format!(
                        "{} {}",
                        col_name,
                        column.data_type.as_deref().unwrap_or_default()
                    )];
                    for constraint in column.constraints {
                        let rendered = self.render_column_constraint(constraint);
                        if let Some(rendered) = rendered {
                            rendered_column_constraint.push(rendered);
                        }
                    }
                    result.push(rendered_column_constraint.join(" ").to_string())
                }
                Ok(result)
            }
            adapter_type @ Bigquery => {
                let mut rendered_constraints: BTreeMap<String, String> = BTreeMap::new();
                for (_, column) in columns_map.iter() {
                    for constraint in &column.constraints {
                        if let Some(rendered) =
                            render_column_constraint(adapter_type, constraint.clone())
                        {
                            if let Some(s) = rendered_constraints.get_mut(&rendered) {
                                s.push_str(&format!(" {rendered}"));
                            } else {
                                rendered_constraints.insert(column.name.clone(), rendered);
                            }
                        }
                    }
                }
                let nested_columns =
                    self.do_nest_column_data_types(columns_map, Some(rendered_constraints))?;
                let result = nested_columns
                    .into_values()
                    .map(|column| {
                        format!(
                            "{} {}",
                            if column.quote.unwrap_or(false) {
                                self.quote(&column.name)
                            } else {
                                column.name.clone()
                            },
                            column.data_type.unwrap_or_default()
                        )
                    })
                    .collect();
                Ok(result)
            }
        }
    }

    /// BaseAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/base/impl.py#L1816
    pub fn render_column_constraint(&self, constraint: Constraint) -> Option<String> {
        // TODO: revisit to support warn_supported, warn_unenforced
        let constraint_support = self.get_constraint_support(constraint.type_);
        if constraint_support == ConstraintSupport::NotSupported {
            return None;
        }

        let constraint_expression = constraint.expression.unwrap_or_default();

        let rendered = match constraint.type_ {
            ConstraintType::Check if !constraint_expression.is_empty() => {
                Some(format!("check ({constraint_expression})"))
            }
            ConstraintType::NotNull => Some(format!("not null {constraint_expression}")),
            ConstraintType::Unique => Some(format!("unique {constraint_expression}")),
            ConstraintType::PrimaryKey => Some(format!("primary key {constraint_expression}")),
            ConstraintType::ForeignKey => {
                if let (Some(to), Some(to_columns)) = (constraint.to, constraint.to_columns) {
                    Some(format!("references {} ({})", to, to_columns.join(", ")))
                } else if !constraint_expression.is_empty() {
                    Some(format!("references {constraint_expression}"))
                } else {
                    None
                }
            }
            ConstraintType::Custom if !constraint_expression.is_empty() => {
                Some(constraint_expression)
            }
            _ => None,
        };
        rendered.and_then(|r| match (self.adapter_type(), constraint.type_) {
            (Bigquery, ConstraintType::PrimaryKey | ConstraintType::ForeignKey) => {
                Some(format!("{r} not enforced"))
            }
            (Bigquery, _) => None,
            _ => Some(r.trim().to_string()),
        })
    }

    /// Given a constraint, return the support status of the constraint on this adapter.
    /// https://github.com/dbt-labs/dbt-adapters/blob/5379513bad9c75661b990a5ed5f32ac9c62a0758/dbt-adapters/src/dbt/adapters/base/impl.py#L293
    pub fn get_constraint_support(&self, ct: ConstraintType) -> ConstraintSupport {
        use ConstraintSupport::*;
        use ConstraintType::*;

        match (self.adapter_type(), ct) {
            // Postgres
            (Postgres, NotNull) => Enforced,
            (Postgres, ForeignKey) => Enforced,
            (Postgres, Unique) => NotEnforced,
            (Postgres, PrimaryKey) => NotEnforced,
            (Postgres, Check) => NotSupported,
            (Postgres, Custom) => NotSupported,

            // Snowflake
            // https://github.com/dbt-labs/dbt-adapters/blob/aa1de3d16267a456326a36045701fb48a61a6b6c/dbt-snowflake/src/dbt/adapters/snowflake/impl.py#L74
            (Snowflake, NotNull) => Enforced,
            (Snowflake, ForeignKey) => Enforced,
            (Snowflake, Unique) => NotEnforced,
            (Snowflake, PrimaryKey) => NotEnforced,
            (Snowflake, Check) => NotSupported,
            (Snowflake, Custom) => NotSupported,

            // BigQuery
            // https://github.com/dbt-labs/dbt-adapters/blob/4a00354a497214d9043bf4122810fe2d04de17bb/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L132
            (Bigquery, NotNull) => Enforced,
            (Bigquery, Unique) => NotSupported,
            (Bigquery, PrimaryKey) => NotEnforced,
            (Bigquery, ForeignKey) => NotEnforced,
            (Bigquery, Check) => NotSupported,
            (Bigquery, Custom) => NotSupported,

            // Databricks
            // https://github.com/databricks/dbt-databricks/blob/822b105b15e644676d9e1f47cbfd765cd4c1541f/dbt/adapters/databricks/constraints.py#L17
            (Databricks, NotNull) => Enforced,
            (Databricks, Unique) => NotSupported,
            (Databricks, PrimaryKey) => NotEnforced,
            (Databricks, ForeignKey) => NotEnforced,
            (Databricks, Check) => Enforced,
            (Databricks, Custom) => NotSupported,

            // Redshift
            // https://github.com/dbt-labs/dbt-adapters/blob/2a94cc75dba1f98fa5caff1f396f5af7ee444598/dbt-redshift/src/dbt/adapters/redshift/impl.py#L53
            (Redshift, NotNull) => Enforced,
            (Redshift, Unique) => NotEnforced,
            (Redshift, PrimaryKey) => NotEnforced,
            (Redshift, ForeignKey) => NotEnforced,
            (Redshift, Check) => NotSupported,
            (Redshift, Custom) => NotSupported,

            // DuckDB - follows Postgres
            (DuckDB, NotNull) => Enforced,
            (DuckDB, ForeignKey) => Enforced,
            (DuckDB, Unique) => NotEnforced,
            (DuckDB, PrimaryKey) => NotEnforced,
            (DuckDB, Check) => NotSupported,
            (DuckDB, Custom) => NotSupported,

            // Fabric
            (Fabric, Check) => NotSupported,
            (Fabric, NotNull) => Enforced,
            (Fabric, Unique) => Enforced,
            (Fabric, PrimaryKey) => Enforced,
            (Fabric, ForeignKey) => Enforced,
            (Fabric, Custom) => NotSupported,

            // Salesforce
            (
                Salesforce | Spark | ClickHouse | Exasol | Starburst | Athena | Trino | Datafusion
                | Dremio | Oracle,
                _,
            ) => {
                unimplemented!("constraint support not implemented")
            }
        }
    }

    /// Given existing columns and columns from our model
    /// we determine which columns to update and persist docs for
    pub fn do_get_persist_doc_columns(
        &self,
        existing_columns: Vec<Column>,
        model_columns: IndexMap<String, DbtColumnRef>,
    ) -> AdapterResult<IndexMap<String, DbtColumnRef>> {
        if self.adapter_type() != Databricks {
            return Err(AdapterError::new(
                AdapterErrorKind::NotSupported,
                "get_persist_doc_columns is a Databricks adapter operation",
            ));
        }
        // Upstream semantics (dbt-databricks): persist a column doc update if and only if the
        // desired comment (model.description, defaulting to "") differs from the existing warehouse
        // comment (defaulting to "").
        //
        // This intentionally supports "clearing" comments: desired="" + existing="foo" => update.
        let mut result = IndexMap::new();

        // Case-insensitive lookup for model columns (matches upstream behavior).
        let mut model_columns_lower: HashMap<String, &DbtColumnRef> = HashMap::new();
        for (name, col) in &model_columns {
            model_columns_lower.insert(name.to_lowercase(), col);
        }

        for existing_col in existing_columns {
            let Some(model_col) = model_columns_lower.get(&existing_col.name().to_lowercase())
            else {
                continue;
            };

            let desired = model_col.description.as_deref().unwrap_or("");
            let existing = existing_col.comment().unwrap_or("");

            if desired != existing {
                result.insert(existing_col.name().to_string(), (*model_col).clone());
            }
        }

        Ok(result)
    }

    /// DatabricksAdapter https://github.com/databricks/dbt-databricks/blob/2f11abb306a400cde32b27891b766bf41a11fb1f/dbt/adapters/databricks/impl.py#L859
    pub fn get_persist_doc_columns(
        &self,
        _state: &State,
        existing_columns: &Value,
        model_columns: &Value,
    ) -> Result<Value, minijinja::Error> {
        let existing_columns = Column::vec_from_jinja_value(Databricks, existing_columns.clone())
            .map_err(|e| {
            minijinja::Error::new(minijinja::ErrorKind::SerdeDeserializeError, e.to_string())
        })?;
        let model_columns = minijinja_value_to_typed_struct::<IndexMap<String, DbtColumnRef>>(
            model_columns.clone(),
        )
        .map_err(|e| {
            minijinja::Error::new(minijinja::ErrorKind::SerdeDeserializeError, e.to_string())
        })?;

        let persist_doc_columns =
            self.do_get_persist_doc_columns(existing_columns, model_columns)?;

        let result = IndexMap::from_iter(
            persist_doc_columns
                .into_iter()
                .map(|(col_name, col)| (col_name, Value::from_serialize(col))),
        );

        Ok(Value::from_object(result))
    }

    /// Translate the result of `show grants` (or equivalent) to match the
    /// grants which a user would configure in their project.
    /// Ideally, the SQL to show grants should also be filtering:
    /// filter OUT any grants TO the current user/role (e.g. OWNERSHIP).
    /// If that's not possible in SQL, it can be done in this method instead.
    ///
    /// BaseAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/base/impl.py#L833
    /// SnowflakeAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-snowflake/src/dbt/adapters/snowflake/impl.py#L400
    /// DatabricksAdapter https://github.com/dbt-labs/dbt-adapters/blob/c16cc7047e8678f8bb88ae294f43da2c68e9f5cc/dbt-spark/src/dbt/adapters/spark/impl.py#L500
    pub fn standardize_grants_dict(
        &self,
        grants_table: Arc<AgateTable>,
    ) -> AdapterResult<IndexMap<String, Vec<String>>> {
        let record_batch = grants_table.original_record_batch();

        match self.adapter_type() {
            Postgres | Bigquery | Redshift | DuckDB => {
                let grantee_cols = get_column_values::<StringArray>(&record_batch, "grantee")?;
                let privilege_cols =
                    get_column_values::<StringArray>(&record_batch, "privilege_type")?;

                let mut result = IndexMap::new();
                for i in 0..record_batch.num_rows() {
                    let privilege = privilege_cols.value(i);
                    let grantee = grantee_cols.value(i);

                    let list = result.entry(privilege.to_string()).or_insert_with(Vec::new);
                    list.push(grantee.to_string());
                }

                Ok(result)
            }
            Snowflake => {
                let grantee_cols = get_column_values::<StringArray>(&record_batch, "grantee_name")?;
                let granted_to_cols =
                    get_column_values::<StringArray>(&record_batch, "granted_to")?;
                let privilege_cols = get_column_values::<StringArray>(&record_batch, "privilege")?;

                let mut result = IndexMap::new();
                for i in 0..record_batch.num_rows() {
                    let privilege = privilege_cols.value(i);
                    let grantee = grantee_cols.value(i);
                    let granted_to = granted_to_cols.value(i);

                    if privilege != "OWNERSHIP"
                        && granted_to != "SHARE"
                        && granted_to != "DATABASE_ROLE"
                    {
                        let list = result.entry(privilege.to_string()).or_insert_with(Vec::new);
                        list.push(grantee.to_string());
                    }
                }

                Ok(result)
            }
            Databricks => {
                let grantee_cols = get_column_values::<StringArray>(&record_batch, "Principal")?;
                let privilege_cols = get_column_values::<StringArray>(&record_batch, "ActionType")?;
                let object_type_cols =
                    get_column_values::<StringArray>(&record_batch, "ObjectType")?;

                let mut result = IndexMap::new();
                for i in 0..record_batch.num_rows() {
                    let privilege = privilege_cols.value(i);
                    let grantee = grantee_cols.value(i);
                    let object_type = object_type_cols.value(i);

                    if object_type == "TABLE" && privilege != "OWN" {
                        let list = result.entry(privilege.to_string()).or_insert_with(Vec::new);
                        list.push(grantee.to_string());
                    }
                }

                Ok(result)
            }
            Salesforce | Spark | Fabric | ClickHouse | Exasol | Starburst | Athena | Trino
            | Datafusion | Dremio | Oracle => {
                unimplemented!("grants not implemented")
            }
        }
    }

    pub fn do_nest_column_data_types(
        &self,
        columns: IndexMap<String, DbtColumn>,
        constraints: Option<BTreeMap<String, String>>,
    ) -> AdapterResult<IndexMap<String, DbtColumn>> {
        match self.adapter_type() {
            Bigquery => nest_column_data_types(columns, constraints),
            Postgres | Snowflake | Databricks | Redshift | Salesforce | Spark | DuckDB | Fabric
            | ClickHouse | Exasol | Starburst | Athena | Trino | Datafusion | Dremio | Oracle => {
                unimplemented!("only available with BigQuery adapter")
            }
        }
    }

    /// BigQueryAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L323
    pub fn nest_column_data_types(
        &self,
        _state: &State,
        columns: &Value,
    ) -> Result<Value, minijinja::Error> {
        // TODO: 'constraints' arg are ignored; didn't find an usage example, implement later
        let columns =
            minijinja_value_to_typed_struct::<IndexMap<String, DbtColumn>>(columns.clone())
                .map_err(|e| {
                    minijinja::Error::new(
                        minijinja::ErrorKind::SerdeDeserializeError,
                        e.to_string(),
                    )
                })?;

        let nested_columns = self.do_nest_column_data_types(columns, None)?;
        let result = IndexMap::<String, Value>::from_iter(
            nested_columns
                .into_iter()
                .map(|(col_name, col)| (col_name, Value::from_serialize(col))),
        );

        Ok(Value::from_object(result))
    }

    /// BigQueryAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L1187
    pub fn get_bq_table(
        &self,
        _state: &State,
        _relation: &Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        unimplemented!("get_bq_table")
    }

    /// BigQueryAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L1219
    #[allow(clippy::too_many_arguments)]
    pub fn grant_access_to(
        &self,
        state: &State,
        conn: &'_ mut dyn Connection,
        entity: &Arc<dyn BaseRelation>,
        entity_type: &str,
        // _role is not used since this method only supports view
        // and googleapi doesn't require role if the entity is view, it'll be default to READ always
        _role: Option<&str>,
        database: &str,
        schema: &str,
        token: CancellationToken,
    ) -> AdapterResult<Value> {
        match self.adapter_type() {
            Bigquery => {
                // https://github.com/dbt-labs/dbt-adapters/blob/4a00354a497214d9043bf4122810fe2d04de17bb/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L834
                /// but instead of locking the thread, put the lock on the dataset
                static DATASET_LOCK: LazyLock<DashMap<String, bool>> = LazyLock::new(DashMap::new);

                // adapter.grant_access_to when seen in Jinja macros, `entity_type` is always set to view
                // https://github.com/dbt-labs/dbt-adapters/blob/4a00354a497214d9043bf4122810fe2d04de17bb/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L842
                // Besides, there is a deserialization bug in the existing py impl when entity_type is not `view`
                if entity_type != "view" {
                    return Err(AdapterError::new(
                        AdapterErrorKind::Configuration,
                        "Only views are supported for grant_access_to".to_string(),
                    ));
                }

                #[derive(Serialize, Deserialize)]
                struct Dataset {
                    project: String,
                    dataset: String,
                }
                let mut payload = BTreeMap::new();
                payload.insert(
                    format!(
                        "{}.{}.{}",
                        entity.database_as_str()?,
                        entity.schema_as_str()?,
                        entity.identifier_as_str()?
                    ),
                    vec![Dataset {
                        project: database.to_string(),
                        dataset: schema.to_string(),
                    }],
                );

                let _lock = DATASET_LOCK
                    .entry(format!("{database}.{schema}"))
                    .or_insert_with(|| true);

                let ctx = query_ctx_from_state(state)?;
                let sql = "none"; // empty sql that won't really be executed
                let mut options = self.get_adbc_execute_options(state);
                options.push((
                    UPDATE_DATASET_AUTHORIZE_VIEW_TO_DATASETS.to_string(),
                    OptionValue::String(serde_json::to_string(&payload)?),
                ));
                self.engine().execute_with_options(
                    Some(state),
                    &ctx,
                    conn,
                    sql,
                    options,
                    false,
                    token,
                )?;
                Ok(none_value())
            }
            Postgres | Snowflake | Databricks | Redshift | Salesforce | Spark | DuckDB | Fabric
            | ClickHouse | Exasol | Starburst | Athena | Trino | Datafusion | Dremio | Oracle => {
                unimplemented!("only available with BigQuery adapter")
            }
        }
    }

    /// BigQueryAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L1241
    pub fn get_dataset_location(
        &self,
        state: &State,
        conn: &'_ mut dyn Connection,
        relation: &dyn BaseRelation,
        token: CancellationToken,
    ) -> AdapterResult<Option<String>> {
        match self.adapter_type() {
            Bigquery => {
                // https://cloud.google.com/bigquery/docs/information-schema-datasets-schemata
                // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L853-L854
                let sql = format!(
                    "SELECT
                location
            FROM `{}.INFORMATION_SCHEMA.SCHEMATA` WHERE schema_name = '{}'",
                    relation.database_as_str()?,
                    relation.schema_as_str()?
                );

                let ctx =
                    query_ctx_from_state(state)?.with_desc("get_dataset_location adapter call");
                let batch = self
                    .engine()
                    .execute(Some(state), conn, &ctx, &sql, token)?;

                let location = get_column_values::<StringArray>(&batch, "location")?;
                debug_assert!(batch.num_rows() <= 1);
                if batch.num_rows() == 1 {
                    let loc = location.value(0).to_owned();
                    Ok(Some(loc))
                } else {
                    Ok(None)
                }
            }
            Postgres | Snowflake | Databricks | Redshift | Salesforce | Spark | DuckDB | Fabric
            | ClickHouse | Exasol | Starburst | Athena | Trino | Datafusion | Dremio | Oracle => {
                unimplemented!("only available with BigQuery adapter")
            }
        }
    }

    /// BigQueryAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L730
    #[allow(clippy::too_many_arguments)]
    pub fn update_table_description(
        &self,
        state: &State,
        conn: &'_ mut dyn Connection,
        database: &str,
        schema: &str,
        identifier: &str,
        description: &str,
        token: CancellationToken,
    ) -> AdapterResult<Value> {
        match self.adapter_type() {
            Bigquery => {
                // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L686-L696
                // Use BigQuery API via driver option instead of SQL
                // Reuse QUERY_DESTINATION_TABLE for the table reference
                let table_ref = format!("{database}.{schema}.{identifier}");

                let ctx =
                    query_ctx_from_state(state)?.with_desc("update_table_description adapter call");
                self.engine().execute_with_options(
                    Some(state),
                    &ctx,
                    conn,
                    "", // Empty SQL - the driver will handle this via the option
                    vec![
                        (
                            QUERY_DESTINATION_TABLE.to_string(),
                            OptionValue::String(table_ref),
                        ),
                        (
                            UPDATE_TABLE_DESCRIPTION.to_string(),
                            OptionValue::String(description.to_string()),
                        ),
                    ],
                    false,
                    token,
                )?;
                Ok(none_value())
            }
            Postgres | Snowflake | Databricks | Redshift | Salesforce | Spark | DuckDB | Fabric
            | ClickHouse | Exasol | Starburst | Athena | Trino | Datafusion | Dremio | Oracle => {
                unimplemented!("only available with BigQuery adapter")
            }
        }
    }

    /// BigQueryAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L930
    #[allow(clippy::too_many_arguments)]
    pub fn load_dataframe(
        &self,
        ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        sql: &str,
        database: &str,
        schema: &str,
        table_name: &str,
        agate_table: Arc<AgateTable>,
        file_path: &str,
        column_overrides: IndexMap<String, String>,
        field_delimiter: &str,
        token: CancellationToken,
    ) -> AdapterResult<Value> {
        match self.adapter_type() {
            Bigquery => {
                // https://github.com/dbt-labs/dbt-adapters/blob/4b3966efc50b1d013907a88bee4ab8ebd022d17a/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L668
                //
                // TODO: Because we don't support custom materialization yet, we're breaking this
                // one. Later we can document to end users that their old way of using this macro
                // is bugged. The fix will be trivial for any power user relying on this adapter
                // method and we can provide clear guidance for migration.
                let ingest_schema = crate::seed::ingest_schema_with_column_overrides(
                    agate_table.original_record_batch().schema().as_ref(),
                    &column_overrides,
                    self.adapter_type(),
                )?;

                let serialized_ingest_schema: Vec<u8> = {
                    // serialize the Arrow schema as an Arrow IPC byte blob
                    let mut buf = Vec::<u8>::new();
                    let () = StreamWriter::try_new(&mut buf, &ingest_schema)
                        .and_then(|mut w| w.finish())
                        .map_err(arrow_error_to_adapter_error)?;
                    Ok(buf) as AdapterResult<Vec<u8>>
                }?;

                self.engine().execute_with_options(
                    None,
                    ctx,
                    conn,
                    sql,
                    vec![
                        (
                            QUERY_DESTINATION_TABLE.to_string(),
                            OptionValue::String(format!("{database}.{schema}.{table_name}")),
                        ),
                        (
                            INGEST_FILE_DELIMITER.to_string(),
                            OptionValue::String(field_delimiter.to_string()),
                        ),
                        (
                            INGEST_PATH.to_string(),
                            OptionValue::String(file_path.to_string()),
                        ),
                        (
                            INGEST_SCHEMA.to_string(),
                            OptionValue::Bytes(serialized_ingest_schema),
                        ),
                    ],
                    false,
                    token,
                )?;

                Ok(none_value())
            }
            Salesforce => todo!("load_dataframe() for the Salesforce adapter"),
            Postgres | Snowflake | Databricks | Redshift | Spark | DuckDB | Fabric | ClickHouse
            | Exasol | Starburst | Athena | Trino | Datafusion | Dremio | Oracle => {
                unimplemented!("only available with BigQuery or Salesforce adapter")
            }
        }
    }

    /// This only supports non-nested columns additions
    ///
    /// Since internally this is only used by snapshot materialization macro where newly added
    /// columns all have non-nested data types, Read from
    /// [here](https://github.com/sdf-labs/fs/blob/9b87be839f6aa54cab1ab91cde2c77855758c396/crates/dbt-loader/src/dbt_macro_assets/dbt-adapters/macros/materializations/snapshots/snapshot.sql#L32-L33).
    /// This builds sql that creates the snapshot relation, and this relation only adds non-nested
    /// columns to the source relation it is supposed to work well for this use case due to
    /// limitation:
    /// https://cloud.google.com/bigquery/docs/managing-table-schemas#add_a_nested_column_to_a_record_column
    ///
    /// BigQueryAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L742
    pub fn alter_table_add_columns(
        &self,
        state: &State,
        conn: &'_ mut dyn Connection,
        relation: &Arc<dyn BaseRelation>,
        columns: Value,
        token: CancellationToken,
    ) -> AdapterResult<Value> {
        match self.adapter_type() {
            Bigquery => {
                let table = relation.identifier_as_str()?;
                let schema = relation.schema_as_str()?;

                let columns = Column::vec_from_jinja_value(Bigquery, columns)?;
                if columns.is_empty() {
                    return Ok(none_value());
                }

                let add_columns: Vec<String> = columns
                    .iter()
                    .map(|col| format!("ADD COLUMN {} {}", col.name(), &col.dtype()))
                    .collect();

                let sql = format!(
                    "ALTER TABLE {schema}.{table}
            {}",
                    add_columns.join("\n,")
                );
                let ctx =
                    query_ctx_from_state(state)?.with_desc("alter_table_add_columns adapter call");
                self.engine().execute_with_options(
                    Some(state),
                    &ctx,
                    conn,
                    &sql,
                    self.get_adbc_execute_options(state),
                    false,
                    token,
                )?;

                Ok(none_value())
            }
            Postgres | Snowflake | Databricks | Redshift | Salesforce | Spark | DuckDB | Fabric
            | ClickHouse | Exasol | Starburst | Athena | Trino | Datafusion | Dremio | Oracle => {
                unimplemented!("only available with BigQuery adapter")
            }
        }
    }

    /// Given a list of sources (BaseRelations), calculate the metadata-based freshness in batch.
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L1390
    pub fn calculate_freshness_from_metadata_batch(
        &self,
        state: &State,
        sources: Vec<Value>,
    ) -> AdapterResult<Value> {
        let kwargs = args!(
            information_schema => Value::from("INFORMATION_SCHEMA"),
            relations => Value::from_object(sources),
        );

        let result: Value = execute_macro(state, kwargs, "get_relation_last_modified")?;
        let result = result.downcast_object::<ResultObject>().unwrap();

        let table = result.table.as_ref().expect("AgateTable exists");
        let record_batch = table.original_record_batch();

        let identifier_column_values =
            get_column_values::<StringArray>(&record_batch, "IDENTIFIER")?;
        let schema_column_values = get_column_values::<StringArray>(&record_batch, "SCHEMA")?;
        let last_modified_column_values =
            get_column_values::<TimestampMillisecondArray>(&record_batch, "LAST_MODIFIED")?;

        let mut result = BTreeMap::new();
        for i in 0..record_batch.num_rows() {
            let identifier = identifier_column_values.value(i).to_lowercase();
            let schema = schema_column_values.value(i).to_lowercase();
            let last_modified = last_modified_column_values.value(i);
            result.insert((identifier, schema), last_modified);
        }
        let result = Value::from_serialize(result);

        Ok(result)
    }

    /// Convert an Arrow [Schema] to a [Vec] of [Column]s.
    ///
    /// This is not part of the Jinja adapter API.
    ///
    /// NOTE(jason): This schema might come directly out of the driver and is not
    /// a sdf frontend schema - this function might not format types perfectly yet
    ///
    /// NOTE(felipecrv): we are working on making it easy to not confuse
    /// driver-generated schemas versus canonicalized sdf frontend schemas
    pub fn schema_to_columns(
        &self,
        _original: Option<&Arc<Schema>>,
        schema: &Arc<Schema>,
    ) -> AdapterResult<Vec<Column>> {
        let type_formatter = self.engine().type_ops();
        let builder = ColumnBuilder::new(self.adapter_type());

        let fields = schema.fields();
        let mut columns = Vec::<Column>::with_capacity(fields.len());
        for field in fields {
            let column = builder.build(field, type_formatter)?;
            columns.push(column);
        }
        Ok(columns)
    }

    /// BaseAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/base/impl.py#L486
    pub fn get_column_schema_from_query(
        &self,
        state: &State,
        conn: &mut dyn Connection,
        ctx: &QueryCtx,
        sql: &str,
        token: CancellationToken,
    ) -> AdapterResult<Vec<Column>> {
        match self.inner_adapter() {
            Replay(_, replay) => replay.replay_get_column_schema_from_query(state, conn, ctx),
            Impl(Bigquery, engine) => {
                // https://github.com/dbt-labs/dbt-adapters/blob/f4dfd350942cce11ff25e3d22f2bee9e60b12b6d/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L444
                let batch = engine.execute(Some(state), conn, ctx, sql, token)?;
                let schema = batch.schema();

                let type_ops = engine.type_ops();
                let builder = ColumnBuilder::new(self.adapter_type());

                let fields = schema.fields();

                let mut columns = Vec::<Column>::with_capacity(fields.len());
                for field in fields {
                    let column = builder.build(field, type_ops)?;
                    columns.push(column);
                }

                let flattened_columns =
                    columns.iter().flat_map(|column| column.flatten()).collect();
                Ok(flattened_columns)
            }
            Impl(_, engine) => {
                let (_, table) = self.execute_inner(
                    Arc::clone(engine),
                    Some(state),
                    conn,
                    ctx,
                    sql,
                    false,
                    true,
                    None,
                    None,
                    token,
                )?;
                let schema = table.original_record_batch().schema();
                self.schema_to_columns(None, &schema)
            }
        }
    }

    /// Get columns in select sql
    ///
    /// BigQueryAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L541
    pub fn get_columns_in_select_sql(
        &self,
        _conn: &'_ mut dyn Connection,
        _sql: &str,
    ) -> AdapterResult<Vec<Column>> {
        unimplemented!("only available with BigQuery adapter")
    }

    /// Used by redshift and postgres to check if the database string is consistent with what's in the project `config`
    ///
    /// PostgresAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-postgres/src/dbt/adapters/postgres/impl.py#L118
    pub fn verify_database(&self, database: String) -> AdapterResult<Value> {
        match self.inner_adapter() {
            Replay(_, replay) => replay.replay_verify_database(&database),
            Impl(adapter_type @ (Postgres | DuckDB), engine) => {
                if let Some(configured_database) = engine.get_configured_database_name() {
                    if database == configured_database {
                        Ok(Value::from(()))
                    } else {
                        Err(AdapterError::new(
                            AdapterErrorKind::UnexpectedDbReference,
                            format!(
                                "Cross-db references not allowed in the {} adapter ({} vs {})",
                                adapter_type, database, configured_database
                            ),
                        ))
                    }
                } else {
                    Ok(Value::from(()))
                }
            }
            Impl(Redshift, engine) => {
                let ra3_node = get_bool_config(engine.as_ref(), "ra3_node")?;
                let datasharing = get_bool_config(engine.as_ref(), "datasharing")?;

                // We have no guarantees that `database` is unquoted, but we do know that `configured_database` will be unquoted.
                // For the Redshift adapter, we can just trim the `"` character per `self.quote`.
                let database = database.trim_matches('\"');
                let configured_database = engine.config("database");

                if let Some(configured_database) = configured_database {
                    if !database.eq_ignore_ascii_case(&configured_database)
                        && !ra3_node
                        && !datasharing
                    {
                        return Err(AdapterError::new(
                            AdapterErrorKind::UnexpectedDbReference,
                            format!(
                                "Cross-db references allowed only in RA3.* node or with datasharing enabled ({database} vs {configured_database})"
                            ),
                        ));
                    }
                }

                Ok(Value::from(()))
            }
            Impl(
                adapter_type @ (Snowflake | Bigquery | Databricks | Salesforce | Spark | Fabric
                | ClickHouse | Exasol | Starburst | Athena | Trino | Datafusion
                | Dremio | Oracle),
                _,
            ) => {
                unimplemented!(
                    "verify_database is not implemented for the {} adapter",
                    adapter_type
                )
            }
        }
    }

    /// Check if a given partition and clustering column spec for a table
    /// can replace an existing relation in the database. BigQuery does not
    /// allow tables to be replaced with another table that has a different
    /// partitioning spec. This method returns True if the given config spec is
    /// identical to that of the existing table.
    ///
    /// BigQueryAdapter https://github.com/dbt-labs/dbt-adapters/blob/4a00354a497214d9043bf4122810fe2d04de17bb/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L541
    pub fn is_replaceable(
        &self,
        conn: &'_ mut dyn Connection,
        relation: &Arc<dyn BaseRelation>,
        local_partition_by: Option<BigqueryPartitionConfig>,
        local_cluster_by: Option<ClusterConfig>,
        state: Option<&State>,
    ) -> AdapterResult<bool> {
        use crate::relation::bigquery::config::components::{ClusterByLoader, PartitionByLoader};
        match self.adapter_type() {
            Bigquery => {
                if let (Replay(_, replay), Some(state)) = (self.inner_adapter(), state) {
                    return replay.replay_is_replaceable(state);
                }

                let schema_result = conn
                    .get_table_schema(
                        Some(&relation.database_as_str()?),
                        Some(&relation.schema_as_str()?),
                        &relation.identifier_as_str()?,
                    )
                    .map_err(adbc_error_to_adapter_error);

                match schema_result {
                    Ok(schema) => {
                        let remote_partition_by = PartitionByLoader.from_remote_state(&schema)?;
                        let local_partition_by =
                            PartitionByLoader::new_component_type_erased(local_partition_by);
                        let is_partition_match = local_partition_by
                            .diff_from(Some(remote_partition_by.as_ref()))
                            .is_none();

                        let remote_cluster_by = ClusterByLoader.from_remote_state(&schema)?;
                        let local_cluster_by = ClusterByLoader::new_component_type_erased(
                            local_cluster_by
                                .map(|cb| cb.into_fields())
                                .unwrap_or_default(),
                        );
                        let is_cluster_match = local_cluster_by
                            .diff_from(Some(remote_cluster_by.as_ref()))
                            .is_none();

                        Ok(is_partition_match && is_cluster_match)
                    }
                    Err(e) => {
                        if e.kind() == AdapterErrorKind::NotFound {
                            Ok(true)
                        } else {
                            Err(e)
                        }
                    }
                }
            }
            adapter_type @ (Postgres | Snowflake | Databricks | Redshift | Salesforce | Spark
            | DuckDB | Fabric | ClickHouse | Exasol | Starburst | Athena
            | Trino | Datafusion | Dremio | Oracle) => {
                unimplemented!(
                    "is_replaceable is only available with BigQuery adapter, not {}",
                    adapter_type
                )
            }
        }
    }

    /// BigQueryAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L956
    pub fn upload_file(&self, _state: &State, _args: &[Value]) -> Result<Value, minijinja::Error> {
        unimplemented!("upload_file")
    }

    /// BigQueryAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L670
    pub fn parse_partition_by(&self, partition_by: Value) -> AdapterResult<Value> {
        match self.adapter_type() {
            Bigquery => {
                // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L579-L586
                // Pure config parse; safe for both BigQuery and Replay (when adapter type is BigQuery)
                let raw_partition_by = partition_by;
                if raw_partition_by.is_none() {
                    return Ok(none_value());
                }

                // Lowercase all string values to match dbt-core behavior
                let normalized = if let Ok(partition_by_map) =
                    minijinja_value_to_typed_struct::<IndexMap<String, Value>>(
                        raw_partition_by.clone(),
                    ) {
                    let new_map: IndexMap<String, Value> = partition_by_map
                        .into_iter()
                        .map(|(key, value)| {
                            let normalized_value = if let Some(s) = value.as_str() {
                                Value::from(s.to_lowercase())
                            } else {
                                value
                            };
                            (key, normalized_value)
                        })
                        .collect();
                    Value::from_serialize(&new_map)
                } else {
                    raw_partition_by.clone()
                };

                let partition_by = minijinja_value_to_typed_struct::<PartitionConfig>(normalized)
                    .map_err(|e| {
                    minijinja::Error::new(
                        minijinja::ErrorKind::SerdeDeserializeError,
                        format!("adapter.parse_partition_by failed on {raw_partition_by:?}: {e}"),
                    )
                })?;

                let validated_config = partition_by.into_bigquery().ok_or_else(|| {
                    minijinja::Error::new(
                        minijinja::ErrorKind::InvalidArgument,
                        "Expect a BigqueryPartitionConfigStruct",
                    )
                })?;

                Ok(Value::from_object(validated_config))
            }
            Postgres | Snowflake | Databricks | Redshift | Salesforce | Spark | DuckDB | Fabric
            | ClickHouse | Exasol | Starburst | Athena | Trino | Datafusion | Dremio | Oracle => {
                unimplemented!("only available with BigQuery adapter")
            }
        }
    }

    /// BigQueryAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L1139
    pub fn get_table_options(
        &self,
        state: &State,
        config: ModelConfig,
        node: &InternalDbtNodeWrapper,
        temporary: bool,
    ) -> AdapterResult<BTreeMap<String, Value>> {
        match self.adapter_type() {
            adapter_type @ Bigquery => metadata::bigquery::object_options::get_table_options_value(
                state,
                config,
                node,
                temporary,
                adapter_type,
            ),
            Postgres | Snowflake | Databricks | Redshift | Salesforce | Spark | DuckDB | Fabric
            | ClickHouse | Exasol | Starburst | Athena | Trino | Datafusion | Dremio | Oracle => {
                unimplemented!("only available with BigQuery adapter")
            }
        }
    }

    /// BigQueryAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L1178
    pub fn get_view_options(
        &self,
        state: &State,
        config: ModelConfig,
        common_attr: &CommonAttributes,
    ) -> AdapterResult<BTreeMap<String, Value>> {
        match self.adapter_type() {
            Bigquery => Ok(
                metadata::bigquery::object_options::get_common_table_options_value(
                    state,
                    config,
                    common_attr,
                    false,
                ),
            ),
            Postgres | Snowflake | Databricks | Redshift | Salesforce | Spark | DuckDB | Fabric
            | ClickHouse | Exasol | Starburst | Athena | Trino | Datafusion | Dremio | Oracle => {
                unimplemented!("only available with BigQuery adapter")
            }
        }
    }

    /// BigQueryAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L1111
    pub fn get_common_options(
        &self,
        state: &State,
        config: ModelConfig,
        node: &InternalDbtNodeWrapper,
        temporary: bool,
    ) -> Result<Value, minijinja::Error> {
        match self.adapter_type() {
            Bigquery => {
                let node = node.as_internal_node();
                let options = metadata::bigquery::object_options::get_common_table_options_value(
                    state,
                    config,
                    node.common(),
                    temporary,
                );
                Ok(Value::from_serialize(options))
            }
            Postgres | Snowflake | Databricks | Redshift | Salesforce | Spark | DuckDB | Fabric
            | ClickHouse | Exasol | Starburst | Athena | Trino | Datafusion | Dremio | Oracle => {
                Err(minijinja::Error::new(
                    minijinja::ErrorKind::InvalidOperation,
                    "get_common_options is only available with BigQuery adapter",
                ))
            }
        }
    }

    /// Add time ingestion partition column to columns list
    ///
    /// BigQueryAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L342
    pub fn add_time_ingestion_partition_column(
        &self,
        columns: Value,
        partition_config: BigqueryPartitionConfig,
    ) -> AdapterResult<Value> {
        match self.adapter_type() {
            Bigquery => {
                let mut result = Column::vec_from_jinja_value(Bigquery, columns.clone())?;

                if result
                    .iter()
                    .any(|c| c.name() == BigqueryPartitionConfig::PARTITION_TIME)
                {
                    return Ok(columns);
                }

                result.push(Column::new_bigquery(
                    partition_config
                        .insertable_time_partitioning_field()?
                        .as_str()
                        .expect("must be a str")
                        .to_owned(),
                    partition_config.data_type,
                    &[],
                    // TODO(serramatutu): proper mode
                    BigqueryColumnMode::Nullable,
                ));

                Ok(Value::from(result))
            }
            Postgres | Snowflake | Databricks | Redshift | Salesforce | Spark | DuckDB | Fabric
            | ClickHouse | Exasol | Starburst | Athena | Trino | Datafusion | Dremio | Oracle => {
                unimplemented!("only available with BigQuery adapter")
            }
        }
    }

    /// BaseAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/base/impl.py#L973
    pub fn list_relations(
        &self,
        query_ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        db_schema: &CatalogAndSchema,
        token: CancellationToken,
    ) -> AdapterResult<Vec<Arc<dyn BaseRelation>>> {
        if self.mock_state().is_some() {
            if !self.introspect_enabled() {
                return Err(AdapterError::new(
                    AdapterErrorKind::NotSupported,
                    "Introspective queries are disabled (--no-introspect).",
                ));
            }
            return Err(AdapterError::new(
                AdapterErrorKind::Internal,
                format!(
                    "list_relations_without_caching is not implemented for this adapter: {}",
                    self.adapter_type()
                ),
            ));
        }
        use crate::metadata::*;

        match self.inner_adapter() {
            Replay(_, replay) => replay.replay_list_relations(query_ctx, conn, db_schema),
            Impl(adapter_type, engine) if engine.is_sidecar() => {
                let client = engine.sidecar_client().unwrap();
                let query_schema = db_schema.resolved_schema.clone();
                let relation_infos = client.list_relations(&query_schema)?;
                let mut relations: Vec<Arc<dyn BaseRelation>> =
                    Vec::with_capacity(relation_infos.len());
                for (database, schema, name, rel_type) in relation_infos {
                    let relation = crate::relation::do_create_relation(
                        adapter_type,
                        database,
                        schema,
                        Some(name),
                        Some(rel_type),
                        self.quoting(),
                    )?;
                    relations.push(relation.into());
                }
                Ok(relations)
            }
            Impl(Snowflake, engine) => {
                snowflake::list_relations(engine.as_ref(), query_ctx, conn, db_schema, token)
            }
            Impl(Bigquery, engine) => {
                bigquery::list_relations(engine.as_ref(), query_ctx, conn, db_schema, token)
            }
            Impl(Databricks | Spark, engine) => {
                databricks::list_relations(engine.as_ref(), query_ctx, conn, db_schema, token)
            }
            Impl(Redshift, engine) => {
                redshift::list_relations(engine.as_ref(), query_ctx, conn, db_schema, token)
            }
            Impl(DuckDB, engine) => {
                duckdb::list_relations(engine.as_ref(), query_ctx, conn, db_schema, token)
            }
            Impl(Fabric, engine) => {
                fabric::list_relations(engine.as_ref(), query_ctx, conn, db_schema, token)
            }
            Impl(
                adapter_type @ (Postgres | Salesforce | ClickHouse | Exasol | Starburst | Athena
                | Trino | Datafusion | Dremio | Oracle),
                _,
            ) => {
                let err = AdapterError::new(
                    AdapterErrorKind::Internal,
                    format!(
                        "list_relations_without_caching is not implemented for this adapter: {adapter_type}",
                    ),
                );
                Err(err)
            }
        }
    }

    pub fn behavior_object(&self) -> &Arc<Behavior> {
        if let Some(mock) = self.mock_state() {
            return &mock.behavior;
        }
        self.engine().behavior()
    }

    /// Check if a DBR capability is available for current compute.
    ///
    /// https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/impl.py#L336-L354
    ///
    /// PRE-CONDITION: adapter_type must be Databricks
    fn has_dbr_capability(
        &self,
        state: &State,
        conn: &mut dyn Connection,
        capability_name: &str,
        token: CancellationToken,
    ) -> AdapterResult<bool> {
        debug_assert!(self.adapter_type() == Databricks);
        let capability = dbr_capabilities::DbrCapability::from_str(capability_name)
            .map_err(|e| AdapterError::new(AdapterErrorKind::Configuration, e))?;

        let is_cluster = self.is_cluster()?;
        let is_sql_warehouse = !is_cluster;

        let query_ctx = query_ctx_from_state(state)?.with_desc("has_dbr_capability adapter call");
        let dbr_version =
            DatabricksMetadataAdapter::get_engine_version(self, &query_ctx, conn, token)?;

        Ok(dbr_capabilities::has_capability(
            capability,
            dbr_version,
            is_sql_warehouse,
        ))
    }

    /// DatabricksAdapter https://github.com/databricks/dbt-databricks/blob/2f11abb306a400cde32b27891b766bf41a11fb1f/dbt/adapters/databricks/impl.py#L349
    pub fn compare_dbr_version(
        &self,
        state: &State,
        conn: &mut dyn Connection,
        major: i64,
        minor: i64,
        token: CancellationToken,
    ) -> AdapterResult<Value> {
        match self.adapter_type() {
            Databricks => {
                let query_ctx =
                    query_ctx_from_state(state)?.with_desc("compare_dbr_version adapter call");

                let current_version =
                    DatabricksMetadataAdapter::get_engine_version(self, &query_ctx, conn, token)?;
                let expected_version = EngineVersion::Full(major, minor);

                let result = match current_version.cmp(&expected_version) {
                    std::cmp::Ordering::Greater => 1,
                    std::cmp::Ordering::Equal => 0,
                    std::cmp::Ordering::Less => -1,
                };

                Ok(Value::from(result))
            }
            Postgres | Snowflake | Bigquery | Redshift | Salesforce | Spark | DuckDB | Fabric
            | ClickHouse | Exasol | Starburst | Athena | Trino | Datafusion | Dremio | Oracle => {
                unimplemented!("only available with Databricksadapter")
            }
        }
    }

    /// Get the external root directory from engine config, defaulting to `"."`.
    pub fn external_root(&self) -> String {
        self.engine()
            .config("external_root")
            .unwrap_or(Cow::Borrowed("."))
            .into_owned()
    }

    /// Build the write-options string for DuckDB external materializations.
    pub fn external_write_options(&self, write_location: &str, rendered_options: &Value) -> String {
        let mut opts: IndexMap<String, String> = IndexMap::new();
        if let Ok(keys) = rendered_options.try_iter() {
            for key in keys {
                let key_str = key.to_string();
                if let Ok(val) = rendered_options.get_item(&key) {
                    opts.insert(key_str, val.to_string());
                }
            }
        }

        // Infer format from file extension if not provided
        if !opts.contains_key("format") {
            let ext = write_location
                .rsplit('.')
                .next()
                .filter(|e| *e != write_location)
                .unwrap_or("");
            if !ext.is_empty() {
                opts.insert("format".to_string(), ext.to_lowercase());
            } else if opts.contains_key("delimiter") {
                opts.insert("format".to_string(), "csv".to_string());
            } else {
                opts.insert("format".to_string(), "parquet".to_string());
            }
        }

        // Default CSV header
        if opts.get("format").map(|f| f.as_str()) == Some("csv") && !opts.contains_key("header") {
            opts.insert("header".to_string(), "1".to_string());
        }

        // Normalize partition_by parens
        if let Some(v) = opts.get("partition_by").cloned() {
            if v.contains(',') && !v.starts_with('(') {
                opts.insert("partition_by".to_string(), format!("({v})"));
            }
        }

        // Build result: quote special keys
        let ret: Vec<String> = opts
            .iter()
            .map(|(k, v)| {
                let lower = k.to_lowercase();
                if matches!(lower.as_str(), "delimiter" | "quote" | "escape" | "null")
                    && !v.starts_with('\'')
                {
                    format!("{k} '{v}'")
                } else {
                    format!("{k} {v}")
                }
            })
            .collect();
        ret.join(", ")
    }

    /// Build the read location (possibly a glob path) for DuckDB external materializations.
    pub fn external_read_location(&self, write_location: &str, rendered_options: &Value) -> String {
        let partition_by = rendered_options
            .get_item(&Value::from("partition_by"))
            .ok()
            .filter(|v| !v.is_undefined() && !v.is_none());
        let per_thread = rendered_options
            .get_item(&Value::from("per_thread_output"))
            .ok()
            .filter(|v| !v.is_undefined() && !v.is_none());

        if partition_by.is_some() || per_thread.is_some() {
            let mut globs = vec![write_location.to_string(), "*".to_string()];
            if let Some(pb) = &partition_by {
                let pb_str = pb.to_string();
                let count = pb_str.split(',').count();
                for _ in 0..count {
                    globs.push("*".to_string());
                }
            }
            let format = rendered_options
                .get_item(&Value::from("format"))
                .ok()
                .filter(|v| !v.is_undefined() && !v.is_none())
                .map(|v| v.to_string())
                .unwrap_or_else(|| "parquet".to_string());
            format!("{}.{}", globs.join("/"), format)
        } else {
            write_location.to_string()
        }
    }

    /// DatabricksAdapter https://github.com/databricks/dbt-databricks/blob/2f11abb306a400cde32b27891b766bf41a11fb1f/dbt/adapters/databricks/impl.py#L307
    pub fn compute_external_path(
        &self,
        config: ModelConfig,
        node: &dyn InternalDbtNodeAttributes,
        is_incremental: bool,
    ) -> AdapterResult<String> {
        match self.adapter_type() {
            Databricks => {
                // TODO: dbt seems to allow optional database and schema
                // https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/impl.py#L212-L213
                let location_root = config
                    .__warehouse_specific_config__
                    .location_root
                    .ok_or_else(|| {
                        AdapterError::new(
                            AdapterErrorKind::Configuration,
                            "location_root is required for external tables.",
                        )
                    })?;

                let include_full_name_in_path = config
                    .__warehouse_specific_config__
                    .include_full_name_in_path
                    .unwrap_or_default();

                // Build path using the same logic as posixpath.join
                let path = if include_full_name_in_path {
                    format!(
                        "{}/{}/{}/{}",
                        location_root.trim_end_matches('/'),
                        node.database().trim_end_matches('/'),
                        node.schema().trim_end_matches('/'),
                        node.name()
                    )
                } else {
                    format!(
                        "{}/{}/{}",
                        location_root.trim_end_matches('/'),
                        node.database().trim_end_matches('/'),
                        node.name()
                    )
                };

                let path = if is_incremental {
                    format!("{path}_tmp")
                } else {
                    path
                };
                Ok(path)
            }

            Postgres | Snowflake | Bigquery | Redshift | Salesforce | Spark | DuckDB | Fabric
            | ClickHouse | Exasol | Starburst | Athena | Trino | Datafusion | Dremio | Oracle => {
                unimplemented!("only available with Databricks adapter")
            }
        }
    }

    /// DatabricksAdapter https://github.com/databricks/dbt-databricks/blob/2f11abb306a400cde32b27891b766bf41a11fb1f/dbt/adapters/databricks/impl.py#L298
    pub fn update_tblproperties_for_uniform_iceberg(
        &self,
        state: &State,
        conn: &mut dyn Connection,
        config: ModelConfig,
        node: &InternalDbtNodeWrapper,
        tblproperties: &mut BTreeMap<String, Value>,
        token: CancellationToken,
    ) -> AdapterResult<()> {
        match self.adapter_type() {
            adapter_type @ Databricks => {
                // TODO(anna): Ideally from_model_config_and_catalogs would just take in an InternalDbtNodeWrapper instead of a Value. This is blocked by a Snowflake hack in `snowflake__drop_table`.
                let node_yml = node.as_internal_node().serialize();
                let catalog_relation = CatalogRelation::from_model_config_and_catalogs(
                    &adapter_type,
                    &Value::from_object(dbt_common::serde_utils::convert_yml_to_value_map(
                        node_yml,
                    )),
                    load_catalogs::fetch_catalogs(),
                )?;
                // We only have to update tblproperties if using a UniForm Iceberg table
                if catalog_relation.table_format == "iceberg" {
                    if self
                        .compare_dbr_version(state, conn, 14, 3, token)?
                        .as_i64()
                        .expect("dbr_version is a number")
                        < 0
                    {
                        return Err(AdapterError::new(
                            AdapterErrorKind::Configuration,
                            "Iceberg support requires Databricks Runtime 14.3 or later.",
                        ));
                    }

                    if catalog_relation.file_format != Some("delta".to_string()) {
                        return Err(AdapterError::new(
                            AdapterErrorKind::Configuration,
                            "When table_format is 'iceberg', file_format must be 'delta'.",
                        ));
                    }

                    let materialized = config.materialized.ok_or_else(|| {
                        AdapterError::new(
                            AdapterErrorKind::Configuration,
                            "materialized is required for iceberg tables.",
                        )
                    })?;

                    // TODO(versusfacit): support snapshot
                    if materialized != DbtMaterialization::Incremental
                        && materialized != DbtMaterialization::Table
                        && materialized != DbtMaterialization::Seed
                    {
                        return Err(AdapterError::new(
                            AdapterErrorKind::Configuration,
                            "When table_format is 'iceberg', materialized must be 'incremental', 'table', or 'seed'.",
                        ));
                    }

                    tblproperties
                        .entry("delta.enableIcebergCompatV2".to_string())
                        .or_insert_with(|| Value::from(true));

                    tblproperties
                        .entry("delta.universalFormat.enabledFormats".to_string())
                        .or_insert_with(|| Value::from("iceberg"));
                }
                Ok(())
            }
            Postgres | Snowflake | Bigquery | Redshift | Salesforce | Spark | DuckDB | Fabric
            | ClickHouse | Exasol | Starburst | Athena | Trino | Datafusion | Dremio | Oracle => {
                unimplemented!("only available with Databricks adapter")
            }
        }
    }

    /// DatabricksAdapter https://github.com/databricks/dbt-databricks/blob/2f11abb306a400cde32b27891b766bf41a11fb1f/dbt/adapters/databricks/impl.py#L274
    pub fn is_uniform(
        &self,
        state: &State,
        conn: &mut dyn Connection,
        config: ModelConfig,
        node: &InternalDbtNodeWrapper,
        token: CancellationToken,
    ) -> AdapterResult<bool> {
        match self.adapter_type() {
            adapter_type @ Databricks => {
                // TODO(anna): Ideally from_model_config_and_catalogs would just take in an InternalDbtNodeWrapper instead of a Value. This is blocked by a Snowflake hack in `snowflake__drop_table`.
                let node_yml = node.as_internal_node().serialize();
                let catalog_relation = CatalogRelation::from_model_config_and_catalogs(
                    &adapter_type,
                    &Value::from_object(dbt_common::serde_utils::convert_yml_to_value_map(
                        node_yml,
                    )),
                    load_catalogs::fetch_catalogs(),
                )?;

                if catalog_relation.table_format != "iceberg" {
                    return Ok(false);
                }

                if self
                    .compare_dbr_version(state, conn, 14, 3, token)?
                    .as_i64()
                    .expect("dbr_version is a number")
                    < 0
                {
                    return Err(AdapterError::new(
                        AdapterErrorKind::Configuration,
                        "Iceberg support requires Databricks Runtime 14.3 or later.",
                    ));
                }

                let materialized = config.materialized.ok_or_else(|| {
                    AdapterError::new(
                        AdapterErrorKind::Configuration,
                        "materialized is required for iceberg tables.",
                    )
                })?;

                // TODO(versusfacit): support snapshot
                if materialized != DbtMaterialization::Incremental
                    && materialized != DbtMaterialization::Table
                    && materialized != DbtMaterialization::Seed
                {
                    return Err(AdapterError::new(
                        AdapterErrorKind::Configuration,
                        "When table_format is 'iceberg', materialized must be 'incremental', 'table', or 'seed'.",
                    ));
                }

                // v2: use_uniform from catalog spec is authoritative
                if load_catalogs::fetch_use_catalogs_v2() {
                    return Ok(catalog_relation
                        .adapter_properties
                        .get("use_uniform")
                        .and_then(|v| v.parse::<bool>().ok())
                        .unwrap_or(false));
                }

                // v1: use_managed_iceberg behavior flag drives the decision
                let use_managed_iceberg = self
                    .behavior_object()
                    .get_value(&Value::from("use_managed_iceberg"))
                    .is_some_and(|flag| flag.is_true());

                if use_managed_iceberg && catalog_relation.catalog_type != "unity" {
                    return Err(AdapterError::new(
                        AdapterErrorKind::Configuration,
                        "Managed Iceberg tables are only supported in Unity Catalog. Set 'use_uniform' adapter property to true for Hive Metastore.",
                    ));
                }

                Ok(!use_managed_iceberg)
            }
            Postgres | Snowflake | Bigquery | Redshift | Salesforce | Spark | DuckDB | Fabric
            | ClickHouse | Exasol | Starburst | Athena | Trino | Datafusion | Dremio | Oracle => {
                unimplemented!("only available with Databricks adapter")
            }
        }
    }

    /// Resolve file format from model config.
    ///
    /// Returns the file_format from config, or adapter-specific default.
    /// Databricks default: "delta". Used by clone materialization.
    ///
    /// DatabricksConfig has file_format: str = "delta"
    ///
    /// DatabricksAdapter https://github.com/databricks/dbt-databricks/blob/2f11abb306a400cde32b27891b766bf41a11fb1f/dbt/adapters/databricks/impl.py#L994
    pub fn resolve_file_format(&self, config: ModelConfig) -> AdapterResult<String> {
        match self.adapter_type() {
            Databricks => {
                let file_format = config
                    .__warehouse_specific_config__
                    .file_format
                    .as_deref()
                    .unwrap_or("delta")
                    .to_string();
                Ok(file_format)
            }
            _ => unimplemented!("resolve_file_format is only supported in Databricks"),
        }
    }

    /// Given a relation, fetch its configurations from the remote data warehouse
    ///
    /// DatabricksAdapter https://github.com/databricks/dbt-databricks/blob/2f11abb306a400cde32b27891b766bf41a11fb1f/dbt/adapters/databricks/impl.py#L931
    pub fn get_relation_config(
        &self,
        state: &State,
        conn: &mut dyn Connection,
        relation: &Arc<dyn BaseRelation>,
        token: CancellationToken,
    ) -> AdapterResult<RelationConfig> {
        use crate::relation::databricks::config::relation_types;

        let (relation_type, remote_state) = {
            // IMPORTANT: do not bypass replay by constructing an AdapterImpl from the engine.
            // In replay mode, adapter calls must go through the replay adapter so they consume
            // the recording stream.
            let metadata_adapter = DatabricksMetadataAdapter::new_from_adapter(self.clone());
            metadata_adapter.fetch_relation_config_from_remote(state, conn, relation, token)?
        };

        let config_loader = match relation_type {
            RelationType::Table => relation_types::incremental_table::new_loader(),
            RelationType::MaterializedView => relation_types::materialized_view::new_loader(),
            RelationType::StreamingTable => relation_types::streaming_table::new_loader(),
            RelationType::View => relation_types::view::new_loader(),
            _ => {
                return Err(AdapterError::new(
                    AdapterErrorKind::Configuration,
                    format!("Unsupported materialization type: {:?}", relation_type),
                ));
            }
        };

        let config = config_loader.from_remote_state(&remote_state)?;

        Ok(config)
    }

    /// Given a model, parse and build its configurations
    ///
    /// DatabricksAdapter https://github.com/databricks/dbt-databricks/blob/2f11abb306a400cde32b27891b766bf41a11fb1f/dbt/adapters/databricks/impl.py#L944
    pub fn get_config_from_model(&self, model: &InternalDbtNodeWrapper) -> AdapterResult<Value> {
        use crate::relation::databricks::config::relation_types;

        let model = model.as_internal_node();

        let config_loader = match model.materialized() {
            DbtMaterialization::Incremental => relation_types::incremental_table::new_loader(),
            DbtMaterialization::MaterializedView => relation_types::materialized_view::new_loader(),
            DbtMaterialization::StreamingTable => relation_types::streaming_table::new_loader(),
            DbtMaterialization::View => relation_types::view::new_loader(),
            _ => {
                return Err(AdapterError::new(
                    AdapterErrorKind::Configuration,
                    format!(
                        "Unsupported materialization type: {:?}",
                        model.materialized()
                    ),
                ));
            }
        };
        let config = config_loader.from_local_config(model)?;
        Ok(Value::from_object(config))
    }

    /// Parse columns and constraints for table creation (Databricks).
    ///
    /// Returns [enriched_columns, typed_constraints] for use with get_column_and_constraints_sql
    /// and relation.enrich().
    ///
    /// DatabricksAdapter https://github.com/databricks/dbt-databricks/blob/2f11abb306a400cde32b27891b766bf41a11fb1f/dbt/adapters/databricks/impl.py#L899
    pub fn parse_columns_and_constraints(
        &self,
        _state: &State,
        existing_columns: &Value,
        model_columns: &Value,
        model_constraints: &Value,
    ) -> Result<Value, minijinja::Error> {
        use crate::relation::databricks::typed_constraint;
        use std::collections::BTreeMap;

        if self.adapter_type() != Databricks && self.adapter_type() != Spark {
            return Err(minijinja::Error::new(
                minijinja::ErrorKind::InvalidOperation,
                "parse_columns_and_constraints is only available for Databricks/Spark adapter",
            ));
        }

        let columns: Vec<Column> = existing_columns
            .try_iter()
            .map_err(|e| {
                minijinja::Error::new(
                    minijinja::ErrorKind::InvalidOperation,
                    format!("existing_columns must be iterable: {e}"),
                )
            })?
            .map(|v| {
                v.downcast_object_ref::<Column>().cloned().ok_or_else(|| {
                    minijinja::Error::new(
                        minijinja::ErrorKind::InvalidOperation,
                        "existing_columns must contain Column objects",
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let model_columns_map: BTreeMap<String, DbtColumn> =
            minijinja_value_to_typed_struct(model_columns.clone()).map_err(|e| {
                minijinja::Error::new(
                    minijinja::ErrorKind::SerdeDeserializeError,
                    format!("model_columns: {e}"),
                )
            })?;

        let model_constraints_vec: Vec<ModelConstraint> =
            minijinja_value_to_typed_struct(model_constraints.clone()).map_err(|e| {
                minijinja::Error::new(
                    minijinja::ErrorKind::SerdeDeserializeError,
                    format!("model_constraints: {e}"),
                )
            })?;

        let column_refs: Vec<DbtColumnRef> = model_columns_map
            .values()
            .map(|c| Arc::new(c.clone()))
            .collect();

        let (not_nulls, typed_constraints) =
            typed_constraint::parse_constraints(&column_refs, &model_constraints_vec).map_err(
                |e| {
                    minijinja::Error::new(
                        minijinja::ErrorKind::InvalidOperation,
                        format!("parse_constraints: {e}"),
                    )
                },
            )?;

        let model_columns_lower: BTreeMap<String, &DbtColumn> = model_columns_map
            .iter()
            .map(|(k, v)| (k.to_lowercase(), v))
            .collect();

        let enriched_columns: Vec<Column> = columns
            .iter()
            .map(|col| {
                let model_col = model_columns_lower.get(&col.name().to_lowercase()).copied();
                let not_null = not_nulls.contains(col.name());
                col.enrich_for_create(model_col, not_null)
            })
            .collect();

        let columns_value: Vec<Value> = enriched_columns
            .into_iter()
            .map(Value::from_object)
            .collect();

        let constraints_value: Vec<Value> = typed_constraints
            .into_iter()
            .map(|c| Value::from_serialize(&c))
            .collect();

        Ok(Value::from(vec![
            Value::from(columns_value),
            Value::from(constraints_value),
        ]))
    }

    /// DatabricksAdapter https://github.com/databricks/dbt-databricks/blob/2f11abb306a400cde32b27891b766bf41a11fb1f/dbt/adapters/databricks/impl.py#L463
    pub fn get_relations_without_caching(
        &self,
        _state: &State,
        _relation: &Arc<dyn BaseRelation>,
    ) -> Result<Value, minijinja::Error> {
        unimplemented!("get_relations_without_caching")
    }

    /// PostgresAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-postgres/src/dbt/adapters/postgres/impl.py#L128
    pub fn parse_index(
        &self,
        _state: &State,
        _raw_index: &Value,
    ) -> Result<Value, minijinja::Error> {
        unimplemented!("parse_index")
    }

    /// DatabricksAdapter https://github.com/databricks/dbt-databricks/blob/2f11abb306a400cde32b27891b766bf41a11fb1f/dbt/adapters/databricks/impl.py#L990
    pub fn get_column_tags_from_model(
        &self,
        model: &dyn InternalDbtNodeAttributes,
    ) -> AdapterResult<Value> {
        use crate::relation::databricks::config::components::ColumnTagsLoader;

        if self.adapter_type() != Databricks {
            return Err(AdapterError::new(
                AdapterErrorKind::Internal,
                "get_column_tags_from_model is a Databricks adapter operation".to_string(),
            ));
        }

        let tags = (&ColumnTagsLoader as &dyn ComponentConfigLoader<DatabricksRelationMetadata>)
            .from_local_config(model)?;
        Ok(tags.to_jinja())
    }
    /// TODO: implement if necessary, currently its noop
    ///
    /// DatabricksAdapter https://github.com/databricks/dbt-databricks/blob/2f11abb306a400cde32b27891b766bf41a11fb1f/dbt/adapters/databricks/impl.py#L966
    pub fn clean_sql(&self, sql: &str) -> AdapterResult<String> {
        debug_assert!(
            self.adapter_type() == Databricks,
            "clean_sql is a Databricks-specific adapter operation"
        );
        Ok(sql.to_string())
    }

    /// relation_max_name_length
    pub fn relation_max_name_length(&self) -> AdapterResult<u32> {
        unimplemented!("only available with Postgres and Redshift adapters")
    }

    /// This uses the BigQuery SDK's copy_table API instead of SQL to properly handle partitioned
    /// tables.
    /// Reference: https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.client.Client.html#google_cloud_bigquery_client_Client_copy_table
    ///
    /// BigQueryAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L510
    pub fn copy_table(
        &self,
        state: &State,
        conn: &'_ mut dyn Connection,
        source: &Arc<dyn BaseRelation>,
        dest: &Arc<dyn BaseRelation>,
        materialization: String,
        token: CancellationToken,
    ) -> AdapterResult<()> {
        match self.adapter_type() {
            Bigquery => {
                let append = materialization == "incremental";
                let truncate = materialization == "table";
                if !append && !truncate {
                    return Err(AdapterError::new(
                        AdapterErrorKind::Configuration,
                        "copy_table 'materialization' must be either 'table' or 'incremental'"
                            .to_string(),
                    ));
                }

                let source_fqn = format!(
                    "{}.{}.{}",
                    source.database_as_str()?,
                    source.schema_as_str()?,
                    source.identifier_as_str()?
                );
                let dest_fqn = format!(
                    "{}.{}.{}",
                    dest.database_as_str()?,
                    dest.schema_as_str()?,
                    dest.identifier_as_str()?
                );

                // Determine write disposition based on materialization
                // WRITE_TRUNCATE for table materialization, WRITE_APPEND for incremental
                let write_disposition = if truncate {
                    "WRITE_TRUNCATE"
                } else {
                    "WRITE_APPEND"
                };

                let mut options = self.get_adbc_execute_options(state);
                options.extend(vec![
                    (
                        COPY_TABLE_SOURCE.to_string(),
                        OptionValue::String(source_fqn),
                    ),
                    (
                        COPY_TABLE_DESTINATION.to_string(),
                        OptionValue::String(dest_fqn),
                    ),
                    (
                        COPY_TABLE_WRITE_DISPOSITION.to_string(),
                        OptionValue::String(write_disposition.to_string()),
                    ),
                ]);

                let ctx = query_ctx_from_state(state)?.with_desc("copy_table adapter call");
                self.engine().execute_with_options(
                    Some(state),
                    &ctx,
                    conn,
                    "",
                    options,
                    false,
                    token,
                )?;

                Ok(())
            }
            Postgres | Snowflake | Databricks | Redshift | Salesforce | Spark | DuckDB | Fabric
            | ClickHouse | Exasol | Starburst | Athena | Trino | Datafusion | Dremio | Oracle => {
                unimplemented!("only available with BigQuery adapter")
            }
        }
    }

    /// BigQueryAdapter https://github.com/dbt-labs/dbt-adapters/blob/4a00354a497214d9043bf4122810fe2d04de17bb/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L818
    pub fn describe_relation(
        &self,
        conn: &'_ mut dyn Connection,
        relation: &Arc<dyn BaseRelation>,
        _state: Option<&State>,
    ) -> AdapterResult<Option<RelationConfig>> {
        if self.adapter_type() != Bigquery {
            unimplemented!("only available with BigQuery adapter");
        }

        if let Replay(_, _) = self.inner_adapter() {
            return Ok(None);
        }

        let adbc_schema = conn
            .get_table_schema(
                Some(&relation.database_as_str()?),
                Some(&relation.schema_as_str()?),
                &relation.identifier_as_str()?,
            )
            .map_err(adbc_error_to_adapter_error)?;

        let Some(relation_type) = relation.relation_type() else {
            return Ok(None);
        };

        if relation_type != RelationType::MaterializedView {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                format!(
                    "The method `BigQueryAdapter.describe_relation` is not implemented for this relation type: {relation_type}"
                ),
            ));
        }

        crate::relation::bigquery::config::relation_types::materialized_view::new_loader()
            .from_remote_state(&adbc_schema)
            .map(Some)
    }

    /// Ensure that the target relation is valid, by making sure it
    /// has the expected columns.
    ///
    /// Merged (it was not clear if we need to keep the legacy code in
    /// a separate method so we decided not to)
    ///
    /// BaseAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/base/impl.py#L927
    pub fn assert_valid_snapshot_target_given_strategy(
        &self,
        state: &State,
        relation: &Arc<dyn BaseRelation>,
        column_names: Option<BTreeMap<String, String>>,
        strategy: Arc<SnapshotStrategy>,
    ) -> AdapterResult<()> {
        match self.inner_adapter() {
            Replay(_, replay) => replay.replay_assert_valid_snapshot_target_given_strategy(
                state,
                relation,
                column_names,
                strategy,
            ),
            Impl(_, _engine) => {
                let columns = self.get_columns_in_relation(state, relation.as_ref())?;
                let names_in_relation: Vec<String> =
                    columns.iter().map(|c| c.name().to_lowercase()).collect();

                // missing columns
                let mut missing: Vec<String> = Vec::new();

                // Note: we're not checking dbt_updated_at or dbt_is_deleted
                // here because they aren't always present.
                let mut hardcoded_columns = vec!["dbt_scd_id", "dbt_valid_from", "dbt_valid_to"];

                if let Some(ref s) = strategy.hard_deletes
                    && s == "new_record"
                {
                    hardcoded_columns.push("dbt_is_deleted");
                }

                for column in hardcoded_columns {
                    let desired = match column_names {
                        Some(ref tree) => match tree.get(column) {
                            Some(v) => v.to_string(),
                            None => {
                                return Err(AdapterError::new(
                                    AdapterErrorKind::Configuration,
                                    format!("Could not find key {column}"),
                                ));
                            }
                        },
                        None => column.to_string(),
                    };

                    if !names_in_relation.contains(&desired.to_lowercase()) {
                        missing.push(desired);
                    }
                }

                if !missing.is_empty() {
                    return Err(AdapterError::new(
                        AdapterErrorKind::Configuration,
                        format!("There are missing columns: {missing:?}"),
                    ));
                }

                Ok(())
            }
        }
    }

    /// AthenaAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-athena/src/dbt/adapters/athena/impl.py#L445
    pub fn generate_unique_temporary_table_suffix(
        &self,
        suffix_initial: Option<String>,
    ) -> AdapterResult<String> {
        let suffix_initial = suffix_initial.as_deref().unwrap_or("__dbt_tmp");
        let uuid_str = Uuid::new_v4().to_string().replace('-', "_");
        Ok(format!("{suffix_initial}_{uuid_str}"))
    }

    /// Check the hard_deletes config enum, and the legacy
    /// invalidate_hard_deletes config flag in order to determine
    /// which behavior should be used for deleted records in a
    /// snapshot. The default is to ignore them.
    ///
    /// BaseAdapter https://github.com/dbt-labs/dbt-adapters/blob/0efd8d3d1081e1ab43e38797d5104f7b424a6284/dbt-adapters/src/dbt/adapters/base/impl.py#L1977
    pub fn get_hard_deletes_behavior(
        &self,
        config: BTreeMap<String, Value>,
    ) -> AdapterResult<String> {
        let invalidate_hard_deletes = config.get("invalidate_hard_deletes");
        let hard_deletes = config.get("hard_deletes");

        let invalidate_hard_deletes_is_true = invalidate_hard_deletes.is_some_and(|v| v.is_true());

        if invalidate_hard_deletes_is_true && hard_deletes.is_some() {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                "You cannot set both the invalidate_hard_deletes and hard_deletes config properties on the same snapshot.",
            ));
        }

        if invalidate_hard_deletes_is_true {
            return Ok("invalidate".to_string());
        }

        match hard_deletes {
            None => Ok("ignore".to_string()),
            Some(val) => {
                // Treat null values same as missing (None)
                if val.is_none() {
                    return Ok("ignore".to_string());
                }
                match val.as_str() {
                    Some("invalidate") => Ok("invalidate".to_string()),
                    Some("new_record") => Ok("new_record".to_string()),
                    Some("ignore") => Ok("ignore".to_string()),
                    Some(_) => Err(AdapterError::new(
                        AdapterErrorKind::Configuration,
                        "Invalid string value for property hard_deletes.",
                    )),
                    None => Err(AdapterError::new(
                        AdapterErrorKind::Configuration,
                        "Invalid type for property hard_deletes (expected string).",
                    )),
                }
            }
        }
    }

    /// Optional fast-path for replay adapters: return schema existence from the trace
    /// when available.
    ///
    /// Default is None for non-replay adapters.
    pub fn schema_exists_from_trace(&self, database: &str, schema: &str) -> Option<bool> {
        match self.inner_adapter() {
            Replay(_, replay) => replay.replay_schema_exists_from_trace(database, schema),
            Impl(_, _engine) => None,
        }
    }

    /// Get the default ADBC statement options
    pub fn get_adbc_execute_options(&self, _state: &State) -> ExecuteOptions {
        match self.adapter_type() {
            Bigquery => vec![(
                QUERY_LINK_FAILED_JOB.to_string(),
                OptionValue::String("true".to_string()),
            )],
            _ => Vec::new(),
        }
    }
}

/// List of possible builtin strategies for adapters.
/// Microbatch is always included — `require_batched_execution_for_custom_microbatch_strategy`
/// is always True in Fusion (new behavior only, no legacy path).
/// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L1690-L1691
fn builtin_incremental_strategies() -> Vec<DbtIncrementalStrategy> {
    vec![
        DbtIncrementalStrategy::Append,
        DbtIncrementalStrategy::DeleteInsert,
        DbtIncrementalStrategy::Merge,
        DbtIncrementalStrategy::InsertOverwrite,
        DbtIncrementalStrategy::Microbatch,
    ]
}

// https://github.com/dbt-labs/dbt-adapters/blob/3ed165d452a0045887a5032c621e605fd5c57447/dbt-adapters/src/dbt/adapters/base/impl.py#L117
pub(crate) static DEFAULT_BASE_BEHAVIOR_FLAGS: LazyLock<[BehaviorFlag; 3]> = LazyLock::new(|| {
    [
        BehaviorFlag::new(
            "require_batched_execution_for_custom_microbatch_strategy",
            true,
            Some("https://docs.getdbt.com/docs/build/incremental-microbatch"),
            None,
            None,
        ),
        BehaviorFlag::new("enable_truthy_nulls_equals_macro", false, None, None, None),
        BehaviorFlag::new(
            "use_catalogs_v2",
            false,
            None,
            Some(
                "Enable experimental catalogs.yml v2 schema validation. This syntax is under development and may change.",
            ),
            Some("https://github.com/dbt-labs/dbt-core/discussions/12723"),
        ),
    ]
});

/// Get adapter-specific behavior flags for a given adapter type
/// This is a standalone function to avoid needing to create adapter instances
/// just to get the flags
pub(crate) fn adapter_specific_behavior_flags(adapter_type: AdapterType) -> Vec<BehaviorFlag> {
    match adapter_type {
        Snowflake => {
            // https://github.com/dbt-labs/dbt-adapters/blob/c4c04de76d5a6c56c95965041a93156fdeaf4641/dbt-snowflake/src/dbt/adapters/snowflake/impl.py#L46
            let flag = BehaviorFlag::new(
                "snowflake_default_transient_dynamic_tables",
                false,
                Some(
                    "When enabled, dynamic tables default to transient (matching regular table behavior). This is a breaking change from previous behavior where dynamic tables were non-transient.",
                ),
                None,
                None,
            );
            vec![flag]
        }
        Databricks => {
            let use_user_folder_for_python = BehaviorFlag::new(
                "use_user_folder_for_python",
                true,
                Some(
                    "Use the user's home folder for uploading python notebooks. Shared folder use is deprecated due to governance concerns.",
                ),
                None,
                None,
            );

            let use_materialization_v2 = BehaviorFlag::new(
                "use_materialization_v2",
                false,
                Some(
                    "Use revamped materializations based on separating create and insert. This allows more performant column comments, as well as new column features.",
                ),
                None,
                None,
            );

            let use_replace_on_for_insert_overwrite = BehaviorFlag::new(
                "use_replace_on_for_insert_overwrite",
                true,
                Some(
                    "Use INSERT INTO ... REPLACE ON syntax for insert_overwrite on SQL Warehouses. When enabled, only matching partitions are overwritten; historical partitions are preserved.",
                ),
                None,
                None,
            );

            let use_managed_iceberg = BehaviorFlag::new(
                "use_managed_iceberg",
                false,
                Some(
                    "Use managed Iceberg tables when table_format is iceberg. When this flag is disabled, UniForm is used instead.",
                ),
                None,
                None,
            );

            vec![
                use_user_folder_for_python,
                use_materialization_v2,
                use_replace_on_for_insert_overwrite,
                use_managed_iceberg,
            ]
        }
        Bigquery => {
            // https://github.com/dbt-labs/dbt-adapters/blob/b9ebd240e39882a8c43ed659de423c7504d4642a/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L109-L110
            let flag = BehaviorFlag::new(
                "bigquery_noop_alter_relation_comment",
                false,
                Some(
                    "Make bigquery__alter_relation_comment a no-op. This is useful when relation descriptions are already set in DDL (e.g. via OPTIONS(description=...)) to avoid an unnecessary update.",
                ),
                None,
                None,
            );
            vec![flag]
        }
        Fabric => {
            let flag = BehaviorFlag::new(
                "empty",
                false,
                Some(
                    "When enabled, table and view materializations will be created as empty structures (no data).",
                ),
                None,
                None,
            );
            vec![flag]
        }
        Postgres | Redshift | Salesforce | Spark | DuckDB | ClickHouse | Exasol | Starburst
        | Athena | Trino | Datafusion | Dremio | Oracle => vec![],
    }
}

/// The adapter implementation. All adapter methods live here.
#[derive(Clone)]
pub struct AdapterImpl {
    inner: AdapterImplInner,
    schema_store: Option<Arc<dyn SchemaStoreTrait>>,
}

#[derive(Clone)]
struct MockState {
    engine: Arc<dyn AdapterEngine>,
    flags: BTreeMap<String, Value>,
    behavior: Arc<Behavior>,
}

#[derive(Clone)]
enum AdapterImplInner {
    Impl(Arc<dyn AdapterEngine>),
    Replay(Arc<dyn Replayer>),
    Mock(MockState),
}

impl AdapterImpl {
    pub fn new(
        engine: Arc<dyn AdapterEngine>,
        schema_store: Option<Arc<dyn SchemaStoreTrait>>,
    ) -> Self {
        Self {
            inner: AdapterImplInner::Impl(engine),
            schema_store,
        }
    }

    pub fn new_replay(
        replay: Arc<dyn Replayer>,
        schema_store: Option<Arc<dyn SchemaStoreTrait>>,
    ) -> Self {
        Self {
            inner: AdapterImplInner::Replay(replay),
            schema_store,
        }
    }

    pub fn new_mock(
        adapter_type: AdapterType,
        flags: BTreeMap<String, Value>,
        quoting: ResolvedQuoting,
        type_ops: Box<dyn crate::sql_types::TypeOps>,
        stmt_splitter: Arc<dyn crate::stmt_splitter::StmtSplitter>,
    ) -> Self {
        let backend = crate::adapter::adapter_factory::backend_of(adapter_type);
        let auth: Arc<dyn dbt_auth::Auth> = dbt_auth::auth_for_backend(backend).into();
        let engine: Arc<dyn AdapterEngine> = Arc::new(crate::engine::XdbcEngine::new_mock(
            adapter_type,
            auth,
            crate::config::AdapterConfig::default(),
            quoting,
            type_ops,
            stmt_splitter,
            Arc::new(crate::cache::RelationCache::default()),
            BTreeMap::new(),
        ));
        let is_true = flags.get("is_true").is_none_or(|v| v.is_true());
        let is_false = flags.get("is_false").is_some_and(|v| v.is_true());
        let is_unknown = flags.get("is_unknown").is_none_or(|v| v.is_true());
        let behavior = Arc::new(Behavior::new(
            vec![
                BehaviorFlag::new("is_true", is_true, None, None, None),
                BehaviorFlag::new("is_false", is_false, None, None, None),
                BehaviorFlag::new("is_unknown", is_unknown, None, None, None),
            ],
            &BTreeMap::new(),
        ));
        Self {
            inner: AdapterImplInner::Mock(MockState {
                engine,
                flags,
                behavior,
            }),
            schema_store: None,
        }
    }

    pub fn get_schema_from_cache(
        &self,
        relation: &dyn BaseRelation,
    ) -> Option<dbt_schema_store::SchemaEntry> {
        self.schema_store
            .as_ref()
            .and_then(|ss| ss.get_schema(&relation.get_canonical_fqn().unwrap_or_default()))
    }

    fn mock_state(&self) -> Option<&MockState> {
        match &self.inner {
            AdapterImplInner::Mock(state) => Some(state),
            _ => None,
        }
    }

    fn is_explicit_mock(&self) -> bool {
        matches!(&self.inner, AdapterImplInner::Mock(_))
    }

    fn introspect_enabled(&self) -> bool {
        match self.mock_state() {
            Some(mock) => mock
                .flags
                .get("introspect")
                .map(|value| value.is_true())
                .unwrap_or(true),
            None => true,
        }
    }
}

impl AdapterImpl {
    pub fn inner_adapter(&self) -> InnerAdapter<'_> {
        match &self.inner {
            AdapterImplInner::Impl(engine) => Impl(engine.adapter_type(), engine),
            AdapterImplInner::Replay(replay) => {
                Replay(replay.engine().adapter_type(), replay.as_ref())
            }
            AdapterImplInner::Mock(mock) => Impl(mock.engine.adapter_type(), &mock.engine),
        }
    }

    #[inline]
    pub fn adapter_type(&self) -> AdapterType {
        match self.inner_adapter() {
            Impl(adapter_type, _) | Replay(adapter_type, _) => adapter_type,
        }
    }

    pub fn as_replay(&self) -> Option<&dyn Replayer> {
        match self.inner_adapter() {
            Replay(_, replay) => Some(replay),
            Impl(..) => None,
        }
    }

    pub fn engine(&self) -> &Arc<dyn AdapterEngine> {
        match self.inner_adapter() {
            Impl(_, engine) => engine,
            Replay(_, replay) => replay.engine(),
        }
    }

    pub fn quoting(&self) -> ResolvedQuoting {
        match self.inner_adapter() {
            Impl(_, engine) => engine.quoting(),
            Replay(_, replay) => replay.engine().quoting(),
        }
    }
}

impl fmt::Debug for AdapterImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.adapter_type())
    }
}

/// Abstract interface for the functions that the adapter can call to perform replays
/// consuming recorded runs instead of making real calls to the data warehouse.
pub trait Replayer: fmt::Debug + Send + Sync {
    fn engine(&self) -> &Arc<dyn AdapterEngine>;

    fn adapter_type(&self) -> AdapterType {
        self.engine().adapter_type()
    }

    fn metadata_adapter(&self) -> Option<Box<dyn MetadataAdapter>>;

    /// Seed a mapping from a truncated/hashed generic test name back to the original pre-hash
    /// full test name. Default implementation is a no-op.
    fn record_test_name_truncation(&self, _truncated_name: &str, _full_name: &str) {}

    fn replay_use_warehouse(
        &self,
        conn: &'_ mut dyn Connection,
        warehouse: String,
        node_id: &str,
    ) -> FsResult<()>;

    fn replay_verify_database(&self, database: &str) -> AdapterResult<Value>;

    /// Non-consuming peek: return true if the next per-node replay record is a BigQuery
    /// `is_replaceable` record.
    ///
    /// This exists for cross-implementation replay compatibility: Mantle recorder may emit an
    /// `is_replaceable(relation=None, ...)` record even when the adapter implementation would
    /// trivially return `true` without consulting the warehouse.
    ///
    /// Default is `false` to preserve behavior for replay adapters that don't support peeking.
    fn replay_peek_is_replaceable_next(&self, _state: &State) -> AdapterResult<bool> {
        Ok(false)
    }

    #[allow(clippy::too_many_arguments)]
    fn replay_execute(
        &self,
        state: Option<&State>,
        conn: &'_ mut dyn Connection,
        ctx: &QueryCtx,
        sql: &str,
        auto_begin: bool,
        fetch: bool,
        limit: Option<i64>,
        options: Option<HashMap<String, String>>,
    ) -> AdapterResult<(AdapterResponse, AgateTable)>;

    fn replay_add_query(
        &self,
        ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        sql: &str,
        auto_begin: bool,
        bindings: Option<&Value>,
        abridge_sql_log: bool,
    ) -> AdapterResult<()>;

    fn replay_get_relation(
        &self,
        state: &State,
        query_ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        database: &str,
        schema: &str,
        identifier: &str,
    ) -> AdapterResult<Option<Arc<dyn BaseRelation>>>;

    fn replay_truncate_relation(
        &self,
        state: &State,
        relation: &Arc<dyn BaseRelation>,
    ) -> AdapterResult<Value>;

    fn replay_quote(&self, state: &State, identifier: &str) -> AdapterResult<String>;

    fn replay_quote_seed_column(
        &self,
        state: &State,
        column: &str,
        quote_config: Option<bool>,
    ) -> AdapterResult<String>;

    fn replay_convert_type(&self, state: &State, data_type: &DataType) -> AdapterResult<String>;

    fn replay_list_relations(
        &self,
        query_ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        db_schema: &CatalogAndSchema,
    ) -> AdapterResult<Vec<Arc<dyn BaseRelation>>>;

    fn replay_rename_relation(
        &self,
        state: &State,
        from_relation: &Arc<dyn BaseRelation>,
        to_relation: &Arc<dyn BaseRelation>,
    ) -> AdapterResult<Value>;

    fn replay_get_column_schema_from_query(
        &self,
        state: &State,
        _conn: &mut dyn Connection,
        _query_ctx: &QueryCtx,
    ) -> AdapterResult<Vec<Column>>;

    fn replay_get_columns_in_relation(
        &self,
        state: &State,
        relation: &Arc<dyn BaseRelation>,
        cache_result: Option<Vec<Column>>,
    ) -> Result<Value, minijinja::Error>;

    fn replay_submit_python_job(
        &self,
        ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        state: &State,
        model: &Value,
        compiled_code: &str,
    ) -> AdapterResult<AdapterResponse>;

    fn replay_render_raw_columns_constraints(
        &self,
        _state: &State,
        _columns_map: IndexMap<String, DbtColumn>,
    ) -> AdapterResult<Vec<String>>;

    fn replay_render_raw_model_constraints(
        &self,
        _state: &State,
        _raw_constraints: &[ModelConstraint],
    ) -> Result<Value, minijinja::Error>;

    fn replay_expand_target_column_types(
        &self,
        state: &State,
        _from_relation: &Arc<dyn BaseRelation>,
        _to_relation: &Arc<dyn BaseRelation>,
    ) -> AdapterResult<Value>;

    fn replay_is_replaceable(&self, state: &State) -> AdapterResult<bool>;

    fn replay_describe_relation(&self, state: &State) -> AdapterResult<Option<Value>>;

    fn replay_schema_exists_from_trace(&self, database: &str, schema: &str) -> Option<bool>;

    fn replay_get_missing_columns(
        &self,
        state: &State,
        _source_relation: &Arc<dyn BaseRelation>,
        _target_relation: &Arc<dyn BaseRelation>,
    ) -> AdapterResult<Vec<Column>>;

    fn replay_drop_relation(
        &self,
        state: &State,
        _relation: &Arc<dyn BaseRelation>,
    ) -> AdapterResult<Value>;

    fn replay_assert_valid_snapshot_target_given_strategy(
        &self,
        state: &State,
        _relation: &Arc<dyn BaseRelation>,
        _column_names: Option<BTreeMap<String, String>>,
        _strategy: Arc<SnapshotStrategy>,
    ) -> AdapterResult<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::adapter_factory::backend_of;
    use crate::cache::RelationCache;
    use crate::column::Column;
    use crate::config::AdapterConfig;
    use crate::engine::XdbcEngine;
    use crate::engine::query_comment::QueryCommentConfig;
    use crate::sql_types::SATypeOpsImpl;
    use crate::stmt_splitter::NaiveStmtSplitter;

    use dbt_adapter_core::AdapterType;
    use dbt_auth::auth_for_backend;
    use dbt_common::AdapterResult;
    use dbt_schemas::schemas::dbt_column::{DbtColumn, DbtColumnRef};
    use dbt_schemas::schemas::relations::base::ComponentName;
    use dbt_schemas::schemas::relations::{DEFAULT_RESOLVED_QUOTING, SNOWFLAKE_RESOLVED_QUOTING};
    use dbt_yaml::Mapping;

    use minijinja::{Environment, State, Value};

    fn engine(adapter_type: AdapterType) -> Arc<dyn AdapterEngine> {
        let config = match adapter_type {
            Snowflake => Mapping::from_iter([
                ("user".into(), "U".into()),
                ("password".into(), "P".into()),
                ("account".into(), "A".into()),
                ("database".into(), "D".into()),
                ("schema".into(), "S".into()),
                ("role".into(), "role".into()),
                ("warehouse".into(), "warehouse".into()),
            ]),
            DuckDB => {
                let attach = YmlValue::Sequence(
                    vec![YmlValue::Mapping(
                        Mapping::from_iter([
                            ("path".into(), "md:some_db".into()),
                            ("is_ducklake".into(), true.into()),
                        ]),
                        Default::default(),
                    )],
                    Default::default(),
                );
                Mapping::from_iter([
                    ("path".into(), "md:my_db".into()),
                    ("is_ducklake".into(), true.into()),
                    ("attach".into(), attach),
                ])
            }
            Bigquery | Redshift => Mapping::new(),
            _ => unimplemented!("mock config for adapter type {:?}", adapter_type),
        };
        build_engine(adapter_type, config)
    }

    fn build_engine(adapter_type: AdapterType, config: Mapping) -> Arc<dyn AdapterEngine> {
        let auth = auth_for_backend(backend_of(adapter_type));
        let resolved_quoting = match adapter_type {
            Snowflake => SNOWFLAKE_RESOLVED_QUOTING,
            Bigquery => DEFAULT_RESOLVED_QUOTING,
            _ => DEFAULT_RESOLVED_QUOTING,
        };
        Arc::new(XdbcEngine::new(
            adapter_type,
            auth.into(),
            AdapterConfig::new(config),
            resolved_quoting,
            QueryCommentConfig::from_query_comment(None, adapter_type, false, None),
            Box::new(SATypeOpsImpl::new(adapter_type)), // XXX: NaiveTypeOpsImpl
            Arc::new(NaiveStmtSplitter), // XXX: may cause bugs if these tests run SQL
            None,
            Arc::new(RelationCache::default()),
            BTreeMap::new(),
            None,
        ))
    }

    #[test]
    fn test_adapter_type() {
        let adapter = AdapterImpl::new(engine(Snowflake), None);
        assert_eq!(adapter.adapter_type(), Snowflake);
    }

    #[test]
    fn test_quote_for_snowflake() {
        let adapter = AdapterImpl::new(engine(Snowflake), None);
        assert_eq!(adapter.quote("abc"), "\"abc\"");
    }

    #[test]
    fn test_quote_for_bigquery() {
        let adapter = AdapterImpl::new(engine(Bigquery), None);
        assert_eq!(adapter.quote("abc"), "`abc`");
    }

    #[test]
    fn test_quote_seed_column_for_snowflake() -> AdapterResult<()> {
        let adapter = AdapterImpl::new(engine(Snowflake), None);
        let env = Environment::new();
        let state = State::new_for_env(&env);
        let quoted = adapter
            .quote_seed_column(&state, "my_column", None)
            .unwrap();
        assert_eq!(quoted, "my_column");
        let quoted = adapter
            .quote_seed_column(&state, "my_column", Some(false))
            .unwrap();
        assert_eq!(quoted, "my_column");
        let quoted = adapter
            .quote_seed_column(&state, "my_column", Some(true))
            .unwrap();
        assert_eq!(quoted, "\"my_column\"");
        Ok(())
    }

    #[test]
    fn test_quote_as_configured_for_snowflake() -> AdapterResult<()> {
        let adapter = AdapterImpl::new(engine(Snowflake), None);

        let env = Environment::new();
        let state = State::new_for_env(&env);
        let quoted = adapter
            .quote_as_configured(&state, "my_schema", &ComponentName::Schema)
            .unwrap();
        assert_eq!(quoted, "my_schema");

        let quoted = adapter
            .quote_as_configured(&state, "my_database", &ComponentName::Database)
            .unwrap();
        assert_eq!(quoted, "my_database");

        let quoted = adapter
            .quote_as_configured(&state, "my_table", &ComponentName::Identifier)
            .unwrap();
        assert_eq!(quoted, "my_table");
        Ok(())
    }

    #[test]
    fn test_redshift_quote() {
        let adapter = AdapterImpl::new(engine(Redshift), None);
        assert_eq!(adapter.quote("abc"), "\"abc\"");
    }

    #[test]
    fn test_table_format_primary_motherduck_ducklake() {
        let adapter = AdapterImpl::new(engine(DuckDB), None);

        assert_eq!(
            adapter.table_format_for_database("my_db"),
            TableFormat::DuckLake
        );
        assert_eq!(
            adapter.table_format_for_database("other"),
            TableFormat::Default
        );
    }

    #[test]
    fn test_table_format_unaliased_motherduck_attachment() {
        let adapter = AdapterImpl::new(engine(DuckDB), None);

        assert_eq!(
            adapter.table_format_for_database("some_db"),
            TableFormat::DuckLake
        );
        assert_eq!(
            adapter.table_format_for_database("main"),
            TableFormat::Default
        );
    }

    // Checks that get_persist_doc_columns generates an explicit empty comment update only when the existing
    // warehouse comment is non-empty.
    #[test]
    fn test_get_persist_doc_columns_clear_comment_only_when_needed() {
        let adapter = AdapterImpl::new_mock(
            Databricks,
            BTreeMap::new(),
            DEFAULT_RESOLVED_QUOTING,
            Box::new(SATypeOpsImpl::new(Databricks)),
            Arc::new(NaiveStmtSplitter),
        );

        let env = Environment::new();
        let state = State::new_for_env(&env);

        // Model column has *no* description, which round-trips through Jinja as `""` (empty string).
        let model_col = Arc::new(DbtColumn {
            name: "sales_channel_name".to_string(),
            description: None,
            ..Default::default()
        });
        let mut model_columns_map: IndexMap<String, DbtColumnRef> = IndexMap::new();
        model_columns_map.insert("sales_channel_name".to_string(), model_col);
        let model_columns = Value::from_serialize(model_columns_map);

        let existing_non_empty = Value::from(vec![Value::from_object(
            Column::new(
                Databricks,
                "sales_channel_name".to_string(),
                "string".to_string(),
                None,
                None,
                None,
            )
            .with_comment(Some("Name of the sales channel".to_string())),
        )]);
        let selected = adapter
            .get_persist_doc_columns(&state, &existing_non_empty, &model_columns)
            .expect("get_persist_doc_columns should succeed");
        let v = selected
            .get_item(&Value::from("sales_channel_name"))
            .expect("get_item should succeed");
        assert!(
            !v.is_undefined(),
            "Expected column to be selected to clear existing non-empty comment, got: {selected:?}"
        );

        let existing_empty = Value::from(vec![Value::from_object(
            Column::new(
                Databricks,
                "sales_channel_name".to_string(),
                "string".to_string(),
                None,
                None,
                None,
            )
            .with_comment(Some("".to_string())),
        )]);
        let selected = adapter
            .get_persist_doc_columns(&state, &existing_empty, &model_columns)
            .expect("get_persist_doc_columns should succeed");
        let v = selected
            .get_item(&Value::from("sales_channel_name"))
            .expect("get_item should succeed");
        assert!(
            v.is_undefined(),
            "Expected column NOT to be selected when existing comment is already empty, got: {selected:?}"
        );
    }

    /// Test that verifies the logic for determining when to use legacy DESCRIBE TABLE
    /// vs. DESCRIBE EXTENDED ... AS JSON for Databricks relations.
    ///
    /// This test documents the expected behavior, matching Python dbt-databricks semantics:
    /// - The `temporary` field only tracks Unity Catalog temporary tables, not HMS temporary views
    /// - is_hive_metastore() returns false for UC temporary tables (even if database is "hive_metastore")
    /// - Hive Metastore tables (both regular and temporary views): use legacy
    /// - Unity Catalog regular tables: use JSON (NOT legacy)
    /// - Unity Catalog temporary tables: use JSON (NOT legacy)
    /// - Materialized views: use legacy
    /// - Streaming tables: use legacy
    #[test]
    fn test_databricks_get_columns_use_legacy_logic() {
        use crate::relation::Relation;
        use crate::relation::databricks::DEFAULT_DATABRICKS_DATABASE;
        use dbt_schemas::schemas::relations::DEFAULT_RESOLVED_QUOTING;

        // Test 1: Non-temporary Hive Metastore table -> should use legacy
        let hive_table = Relation::new(
            Databricks,
            Some(DEFAULT_DATABRICKS_DATABASE.to_string()),
            Some("schema1".to_string()),
            Some("table1".to_string()),
            Some(RelationType::Table),
            None,
            DEFAULT_RESOLVED_QUOTING,
            None,
            false,
            false, // NOT temporary
        );
        assert!(
            hive_table.is_hive_metastore(),
            "Expected is_hive_metastore() to return true for non-temporary table in hive_metastore"
        );
        assert!(!hive_table.is_temporary(), "Expected non-temporary table");
        // use_legacy = is_hive_metastore || is_materialized_view || is_streaming_table
        // use_legacy = true || false || false = true
        let use_legacy_hive_table = hive_table.is_hive_metastore()
            || hive_table.is_materialized_view()
            || hive_table.is_streaming_table();
        assert!(
            use_legacy_hive_table,
            "Expected non-temporary Hive Metastore table to use legacy DESCRIBE"
        );

        // Test 2: Unity Catalog temporary table (with hive_metastore database name) -> should NOT use legacy
        // Key test: is_hive_metastore() should return FALSE for UC temporary tables
        // Note: In practice, UC temp tables wouldn't have database="hive_metastore", but this tests the logic
        let uc_temp_table = Relation::new(
            Databricks,
            Some(DEFAULT_DATABRICKS_DATABASE.to_string()),
            Some("schema1".to_string()),
            Some("temp_table".to_string()),
            Some(RelationType::Table),
            None,
            DEFAULT_RESOLVED_QUOTING,
            None,
            false,
            true, // IS temporary (UC temporary table)
        );
        assert!(
            !uc_temp_table.is_hive_metastore(),
            "Expected is_hive_metastore() to return FALSE for UC temporary table (matching Python semantics)"
        );
        assert!(uc_temp_table.is_temporary(), "Expected temporary table");
        // use_legacy = is_hive_metastore || is_materialized_view || is_streaming_table
        // use_legacy = false || false || false = false
        let use_legacy_uc_temp = uc_temp_table.is_hive_metastore()
            || uc_temp_table.is_materialized_view()
            || uc_temp_table.is_streaming_table();
        assert!(
            !use_legacy_uc_temp,
            "Expected UC temporary table to use JSON DESCRIBE (not legacy)"
        );

        // Test 3: Unity Catalog table (non-temporary) -> should NOT use legacy
        let unity_table = Relation::new(
            Databricks,
            Some("unity_catalog".to_string()),
            Some("schema1".to_string()),
            Some("table1".to_string()),
            Some(RelationType::Table),
            None,
            DEFAULT_RESOLVED_QUOTING,
            None,
            false,
            false,
        );
        assert!(
            !unity_table.is_hive_metastore(),
            "Expected Unity Catalog table (not Hive Metastore)"
        );
        // use_legacy = is_hive_metastore || is_materialized_view || is_streaming_table
        // use_legacy = false || false || false = false
        let use_legacy_unity = unity_table.is_hive_metastore()
            || unity_table.is_materialized_view()
            || unity_table.is_streaming_table();
        assert!(
            !use_legacy_unity,
            "Expected Unity Catalog table to use JSON DESCRIBE (not legacy)"
        );

        // Test 4: Materialized view -> should use legacy
        let mv = Relation::new(
            Databricks,
            Some("unity_catalog".to_string()),
            Some("schema1".to_string()),
            Some("mv1".to_string()),
            Some(RelationType::MaterializedView),
            None,
            DEFAULT_RESOLVED_QUOTING,
            None,
            false,
            false,
        );
        assert!(mv.is_materialized_view(), "Expected materialized view");
        // use_legacy = is_hive_metastore || is_materialized_view || is_streaming_table
        // use_legacy = false || true || false = true
        let use_legacy_mv =
            mv.is_hive_metastore() || mv.is_materialized_view() || mv.is_streaming_table();
        assert!(
            use_legacy_mv,
            "Expected materialized view to use legacy DESCRIBE"
        );
    }

    #[test]
    fn test_try_to_int_col() {
        use arrow_array::Float64Array;

        // whole numbers → true
        assert!(try_to_int_col(&Float64Array::from(vec![1.0, 2.0, 100.0])));
        // fractional values → false
        assert!(!try_to_int_col(&Float64Array::from(vec![1.0, 2.5, 3.0])));
        // nulls are ignored → true
        assert!(try_to_int_col(&Float64Array::from(vec![
            Some(1.0),
            None,
            Some(3.0)
        ])));
        // NaN/Inf are ignored → true
        assert!(try_to_int_col(&Float64Array::from(vec![
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            1.0
        ])));
        // empty column → true
        assert!(try_to_int_col(&Float64Array::from(Vec::<f64>::new())));
    }

    #[test]
    fn test_verify_database_redshift_cross_db_blocked_without_flags() {
        let config = Mapping::from_iter([("database".into(), "mydb".into())]);
        let adapter = AdapterImpl::new(build_engine(Redshift, config), None);
        let result = adapter.verify_database("otherdb".to_string());
        assert!(
            result.is_err(),
            "cross-db ref should be blocked without ra3_node or datasharing"
        );
    }

    #[test]
    fn test_verify_database_redshift_same_db_always_allowed() {
        let config = Mapping::from_iter([("database".into(), "mydb".into())]);
        let adapter = AdapterImpl::new(build_engine(Redshift, config), None);
        assert!(adapter.verify_database("mydb".to_string()).is_ok());
    }

    #[test]
    fn test_verify_database_redshift_cross_db_allowed_with_ra3_node() {
        let config = Mapping::from_iter([
            ("database".into(), "mydb".into()),
            ("ra3_node".into(), true.into()),
        ]);
        let adapter = AdapterImpl::new(build_engine(Redshift, config), None);
        assert!(adapter.verify_database("otherdb".to_string()).is_ok());
    }

    #[test]
    fn test_verify_database_redshift_cross_db_allowed_with_datasharing() {
        let config = Mapping::from_iter([
            ("database".into(), "mydb".into()),
            ("datasharing".into(), true.into()),
        ]);
        let adapter = AdapterImpl::new(build_engine(Redshift, config), None);
        assert!(adapter.verify_database("otherdb".to_string()).is_ok());
    }

    #[test]
    fn test_verify_database_redshift_accepts_mixed_case_string_flags() {
        // dbt-core accepts booleans as strings in any casing (e.g. "True" from YAML).
        let config = Mapping::from_iter([
            ("database".into(), "mydb".into()),
            ("datasharing".into(), "True".into()),
        ]);
        let adapter = AdapterImpl::new(build_engine(Redshift, config), None);
        assert!(adapter.verify_database("otherdb".to_string()).is_ok());
    }

    #[test]
    fn test_has_feature_datasharing_false_by_default() {
        let env = Environment::new();
        let state = State::new_for_env(&env);
        let adapter = AdapterImpl::new(engine(Redshift), None);
        let result = adapter
            .has_feature(&state, "datasharing", CancellationToken::never_cancels())
            .unwrap();
        assert_eq!(result, Some(false));
    }

    #[test]
    fn test_has_feature_datasharing_true_when_set() {
        let env = Environment::new();
        let state = State::new_for_env(&env);
        let config = Mapping::from_iter([
            ("database".into(), "mydb".into()),
            ("datasharing".into(), true.into()),
        ]);
        let adapter = AdapterImpl::new(build_engine(Redshift, config), None);
        let result = adapter
            .has_feature(&state, "datasharing", CancellationToken::never_cancels())
            .unwrap();
        assert_eq!(result, Some(true));
    }
}
