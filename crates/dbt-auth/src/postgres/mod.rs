use crate::{AdapterConfig, Auth, AuthError};

use dbt_xdbc::{Backend, database};

pub struct PostgresAuth;

impl Auth for PostgresAuth {
    fn backend(&self) -> Backend {
        Backend::Postgres
    }

    fn configure(&self, config: &AdapterConfig) -> Result<database::Builder, AuthError> {
        let mut builder = database::Builder::new(self.backend());

        let user = config.require_string("user")?;
        let password = config.require_string("password")?;
        let host = config.require_string("host")?;
        let port = config.require_string("port")?;
        let dbname = config.require_string("database")?;

        // TODO: other options
        // let schema = config.require_to_string("schema")?;
        // let threads = config.require_to_string("threads")?;

        builder.with_parse_uri(format!(
            "postgresql://{user}:{password}@{host}:{port}/{dbname}",
        ))?;

        Ok(builder)
    }
}
