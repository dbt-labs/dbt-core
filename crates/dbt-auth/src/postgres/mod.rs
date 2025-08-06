use crate::{AdapterConfig, Auth, AuthError};

use dbt_xdbc::{Backend, database};

#[derive(Debug, Default)]
pub struct PostgresAuth;

impl Auth for PostgresAuth {
    fn backend(&self) -> Backend {
        Backend::Postgres
    }

    fn configure(&self, config: &AdapterConfig) -> Result<database::Builder, AuthError> {
        let mut builder = database::Builder::new(self.backend());

        let user = config.get_str("user")?;
        let password = config.get_str("password")?;
        let host = config.get_str("host")?;
        let port = config.get_str("port")?;
        let dbname = config.get_str("database")?;

        // TODO: other options
        // let schema = config.get_str("schema")?;
        // let threads = config.get_str("threads")?;

        builder.with_parse_uri(format!(
            "postgresql://{user}:{password}@{host}:{port}/{dbname}",
        ))?;

        Ok(builder)
    }
}
