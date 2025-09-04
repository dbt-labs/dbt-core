#![allow(clippy::let_and_return)]
#![allow(clippy::collapsible_else_if)]

use std::io;

use dbt_xdbc::{Backend, database};

mod config;

// Database-specific auth implementations
mod bigquery;
mod databricks;
mod postgres;
mod redshift;
mod salesforce;
mod snowflake;

pub use config::AdapterConfig;

/// Authorization trait.
pub trait Auth: Send + Sync {
    /// Return the XDBC backend this authenticator is for.
    fn backend(&self) -> Backend;

    /// Configure the XDBC database builder.
    fn configure(&self, config: &AdapterConfig) -> Result<database::Builder, AuthError>;
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
    /// An error from the [dbt_serde_yaml] crate
    YAML(dbt_serde_yaml::Error),
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

impl From<dbt_serde_yaml::Error> for AuthError {
    fn from(err: dbt_serde_yaml::Error) -> Self {
        AuthError::YAML(err)
    }
}
