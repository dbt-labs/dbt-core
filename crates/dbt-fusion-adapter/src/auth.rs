use super::config::AdapterConfig;
use super::errors::AdapterResult;

use dbt_xdbc::{Backend, database};

/// Authorization trait.
pub trait Auth: Send + Sync {
    /// Return the XDBC backend this authenticator is for.
    fn backend(&self) -> Backend;

    /// Configure the XDBC database builder.
    fn configure(&self, config: &AdapterConfig) -> AdapterResult<database::Builder>;
}
