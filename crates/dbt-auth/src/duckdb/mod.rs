pub mod init;

use crate::{AdapterConfig, Auth, AuthError};

use dbt_xdbc::{Backend, database};

pub struct DuckDbAuth;

impl Auth for DuckDbAuth {
    fn backend(&self) -> Backend {
        Backend::DuckDB
    }

    fn configure(&self, config: &AdapterConfig) -> Result<database::Builder, AuthError> {
        let mut builder = database::Builder::new(self.backend());

        // DuckDB requires the database path to be specified
        // The path option from profiles.yml specifies where to store the database file
        if let Some(path) = config.get_string("path") {
            builder
                .with_named_option("path", path.as_ref())
                .map_err(|e| AuthError::Config(e.to_string()))?;
        }

        // motherduck_token must be set at database init time (DuckDB rejects
        // SET motherduck_token after initialization)
        if let Some(token) = init::resolve_motherduck_token(config) {
            builder
                .with_named_option("motherduck_token", token)
                .map_err(|e| AuthError::Config(e.to_string()))?;
        }

        Ok(builder)
    }
}
