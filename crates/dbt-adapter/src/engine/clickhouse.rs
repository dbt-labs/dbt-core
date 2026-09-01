//! ClickHouse-specific connection bootstrap for [`super::AdbcEngine`].

use std::borrow::Cow;

use dbt_adbc::{Connection, Database, Statement};
use dbt_auth::AdapterConfig;
use dbt_common::AdapterResult;

use crate::errors::{AdapterError, AdapterErrorKind, adbc_error_to_adapter_error};
use crate::metadata::clickhouse;

/// The profile `schema` (a ClickHouse database; the driver maps it to the
/// ADBC schema).
pub(crate) fn target_schema(config: &AdapterConfig) -> Option<Cow<'_, str>> {
    config.get_string("schema").filter(|s| !s.is_empty())
}

fn prepared_stmt(conn: &mut Box<dyn Connection>, sql: &str) -> AdapterResult<Box<dyn Statement>> {
    let mut stmt = conn.new_statement().map_err(adbc_error_to_adapter_error)?;
    stmt.set_sql_query(sql)
        .map_err(adbc_error_to_adapter_error)?;
    Ok(stmt)
}

/// httpclient.py `database_dropped` parity: after dropping the connection's
/// own default database, clear it so follow-up statements don't fail with
/// UNKNOWN_DATABASE. Clearing falls back to the user's default database.
pub(crate) fn database_dropped(
    conn: &mut dyn Connection,
    config: &AdapterConfig,
    dropped_schema: &str,
) -> AdapterResult<()> {
    if target_schema(config).as_deref() == Some(dropped_schema) {
        conn.set_option(
            adbc_core::options::OptionConnection::CurrentSchema,
            adbc_core::options::OptionValue::String(String::new()),
        )
        .map_err(adbc_error_to_adapter_error)?;
    }
    Ok(())
}

/// Mirrors dbclient.py `_ensure_database`, over a temporary connection with
/// no current schema set: the server rejects every request whose default
/// database does not exist, so a fresh target could never bootstrap itself.
pub(crate) fn ensure_database(database: &dyn Database, config: &AdapterConfig) -> AdapterResult<()> {
    let Some(db_name) = target_schema(config) else {
        return Ok(());
    };
    let mut conn = database
        .new_connection()
        .map_err(adbc_error_to_adapter_error)?;

    let db_exists = |conn: &mut Box<dyn Connection>| -> AdapterResult<bool> {
        let mut stmt = prepared_stmt(conn, &clickhouse::exists_database_sql(&db_name))?;
        let mut reader = stmt.execute().map_err(adbc_error_to_adapter_error)?;
        let batch = reader
            .next()
            .transpose()
            .map_err(|e| AdapterError::new(AdapterErrorKind::Internal, e.to_string()))?;
        // EXISTS DATABASE returns a single UInt8 row.
        Ok(batch.is_some_and(|batch| {
            use arrow_array::cast::AsArray;
            batch.num_rows() > 0
                && batch
                    .column(0)
                    .as_primitive::<arrow_array::types::UInt8Type>()
                    .value(0)
                    != 0
        }))
    };

    if db_exists(&mut conn)? {
        return Ok(());
    }
    let create_sql = clickhouse::create_database_sql(
        &db_name,
        config.get_str("database_engine"),
        config.get_str("cluster"),
    );
    prepared_stmt(&mut conn, &create_sql)?
        .execute_update()
        .map_err(|e| {
            AdapterError::new(
                AdapterErrorKind::Configuration,
                format!("Failed to create {db_name} database due to ClickHouse exception: {e}"),
            )
        })?;
    if !db_exists(&mut conn)? {
        return Err(AdapterError::new(
            AdapterErrorKind::Configuration,
            format!("Failed to create database {db_name} for unknown reason"),
        ));
    }
    Ok(())
}
