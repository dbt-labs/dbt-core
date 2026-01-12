use crate::{AdapterConfig, Auth, AuthError};

#[expect(unused_imports)]
use dbt_xdbc::{Backend, database, spark};

/// User agent name provided to Spark by Fusion.
#[expect(dead_code)]
const USER_AGENT_NAME: &str = "dbt";

pub struct SparkAuth;

impl Auth for SparkAuth {
    fn backend(&self) -> Backend {
        Backend::Spark
    }

    fn configure(&self, _config: &AdapterConfig) -> Result<database::Builder, AuthError> {
        let builder = database::Builder::new(self.backend());
        // TODO: add named options
        // TODO: spark authentication
        // builder.with_named_option(databricks::USER_AGENT, USER_AGENT_NAME)?;
        // all of the following options are required for any Databricks connection
        debug_assert!(false, "Spark is still WIP");
        Ok(builder)
    }
}

// TODO: tests
