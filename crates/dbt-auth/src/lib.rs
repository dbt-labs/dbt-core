#![allow(clippy::let_and_return)]
#![allow(clippy::collapsible_else_if)]

use std::io;

use dbt_xdbc::{Backend, database};

mod config;

// Database-specific auth implementations
mod athena;
mod bigquery;
mod clickhouse;
mod databricks;
mod duckdb;
mod exasol;
#[cfg(test)]
mod flock;
mod postgres;
mod redshift;
mod salesforce;
mod snowflake;
mod spark;
mod sqlserver;
#[cfg(test)]
mod test_options;

pub use config::AdapterConfig;
pub use duckdb::init::{generate_duckdb_init_sql, is_motherduck_path, motherduck_database_name};

/// The result of configuring an auth backend.
///
/// Contains the configured database builder and any warnings emitted during
/// configuration (e.g. ignored profile fields).
#[derive(Debug)]
pub struct AuthOutcome {
    pub builder: database::Builder,
    pub warnings: Vec<String>,
}

/// Authorization trait.
pub trait Auth: Send + Sync {
    /// Return the XDBC backend this authenticator is for.
    fn backend(&self) -> Backend;

    /// Configure the XDBC database builder.
    fn configure(&self, config: &AdapterConfig) -> Result<AuthOutcome, AuthError>;
}

/// Macro used to structure the AdapterConfig -> database::Builder pipeline
#[macro_export]
macro_rules! auth_configure_pipeline {
    ($backend:expr, $cfg:expr, $parse_auth:path, $apply_connection_args:path) => {{
        let authentication_args = $parse_auth($cfg)?;

        let builder = database::Builder::new($backend);
        let builder = authentication_args.apply(builder)?;
        let builder = $apply_connection_args($cfg, builder)?;

        Ok($crate::AuthOutcome {
            builder,
            warnings: vec![],
        })
    }};
}

/// Factory function to create an Auth instance based on the backend type.
pub fn auth_for_backend(backend: Backend) -> Box<dyn Auth> {
    match backend {
        Backend::Snowflake => Box::new(snowflake::SnowflakeAuth {}),
        Backend::Postgres => Box::new(postgres::PostgresAuth {}),
        Backend::BigQuery => Box::new(bigquery::BigqueryAuth {}),
        Backend::Databricks | Backend::DatabricksODBC => Box::new(databricks::DatabricksAuth {}),
        Backend::Redshift | Backend::RedshiftODBC => Box::new(redshift::RedshiftAuth {}),
        Backend::Salesforce => Box::new(salesforce::SalesforceAuth {}),
        Backend::Spark => Box::new(spark::SparkAuth {}),
        Backend::DuckDBExtended => Box::new(duckdb::DuckDbAuth {}),
        Backend::SQLServer => Box::new(sqlserver::SQLServerAuth {}),
        Backend::ClickHouse => Box::new(clickhouse::ClickHouseAuth {}),
        Backend::Athena => Box::new(athena::AthenaAuth {}),
        Backend::Exasol => Box::new(exasol::ExasolAuth {}),
        Backend::Generic { .. } => unimplemented!("generic backend authentication"),
    }
}

/// Error type for [dbt_auth].
///
/// For display purposes, it must be converted into an [AdapterError] first, outside of this crate.
#[derive(Debug)]
pub enum AuthError {
    /// Error from the [adbc_core] crate
    Adbc(adbc_core::error::Error),
    /// A generic configuration error
    Config(String),
    /// An error from the [serde_json] crate
    JSON(serde_json::Error),
    /// An error from the [dbt_yaml] crate
    YAML(dbt_yaml::Error),
    /// I/O error
    Io(io::Error),
}

impl AuthError {
    /// Creates a new [AuthError] from a custom message describing a configuration error.
    pub fn config(message: impl Into<String>) -> Self {
        AuthError::Config(message.into())
    }

    /// Returns a non-owned string with an error message.
    ///
    /// Used for test assertions. For display purposes, it must be converted into an
    /// [AdapterError] first outside of this crate.
    pub fn msg(&self) -> &str {
        match self {
            AuthError::Adbc(_) => "ADBC Error",
            AuthError::Config(msg) => msg,
            AuthError::JSON(_) => "JSON Error",
            AuthError::YAML(_) => "YAML Error",
            AuthError::Io(_) => "I/O Error",
        }
    }
}

impl From<adbc_core::error::Error> for AuthError {
    fn from(err: adbc_core::error::Error) -> Self {
        AuthError::Adbc(err)
    }
}

impl From<io::Error> for AuthError {
    fn from(err: io::Error) -> Self {
        AuthError::Io(err)
    }
}

impl From<serde_json::Error> for AuthError {
    fn from(err: serde_json::Error) -> Self {
        AuthError::JSON(err)
    }
}

impl From<dbt_yaml::Error> for AuthError {
    fn from(err: dbt_yaml::Error) -> Self {
        AuthError::YAML(err)
    }
}

// Enum for private key providers
//
// Cross-adapter spec for how users may provide private keys,
// either via paths to the keys or the extract key values themselves.
// Prefer strictness about including PEM headers where possible.
// For Snowflake, we are forced to support a plethora of legacy
// compliant PEM encodings. See snowflake/key_format.rs for more
#[derive(Debug)]
pub(crate) enum PrivateKeySource<'a> {
    FilePath(&'a str),
    Raw(&'a str),
}
