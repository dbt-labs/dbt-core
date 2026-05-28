#[cfg(test)]
mod tests {
    use adbc_core::{
        error::{Error, Result},
        options::AdbcVersion,
    };
    use arrow_array::{cast::AsArray, types::Decimal128Type};
    use dbt_xdbc::{Backend, connection, database, driver};

    /// Execute a statement through the "flock" driver against Snowflake.
    ///
    /// Requires:
    /// - `FLOCK_DRIVER_TESTS` env var set
    /// - `~/.dbt/dbt_cloud.yml` with valid cloud credentials
    #[test_with::env(FLOCK_DRIVER_TESTS)]
    #[test]
    fn statement_execute_flock_snowflake() -> Result<()> {
        let backend = Backend::Snowflake;
        // Load the flock driver via the Remote strategy.
        let mut driver = driver::Builder::new(backend, driver::LoadStrategy::Remote)
            .with_adbc_version(AdbcVersion::V110)
            .try_load()?;

        let mut builder = database::Builder::new(backend);

        // dbt Cloud credentials from ~/.dbt/dbt_cloud.yml (with env-var overrides).
        let cloud_config_path = dbt_cloud_config::get_cloud_project_path()
            .map_err(|e| Error::with_message_and_status(e, adbc_core::error::Status::Internal))?;
        let cloud_yml = dbt_cloud_config::parse_cloud_config(&cloud_config_path)
            .map_err(|e| Error::with_message_and_status(e, adbc_core::error::Status::Internal))?;
        let resolved = dbt_cloud_config::resolve_cloud_config(cloud_yml.as_ref(), None);
        if let Some(credentials) = resolved.and_then(|r| r.credentials) {
            builder.with_named_option("dbt_cloud.token", credentials.token)?;
            builder.with_named_option("dbt_cloud.host", credentials.host)?;
            builder.with_named_option("dbt_cloud.account_id", credentials.account_id)?;
        }

        let mut database = builder.build(&mut driver)?;
        let conn_builder = connection::Builder::default();
        let mut conn = conn_builder.build(&mut database)?;
        let mut stmt = conn.new_statement()?;
        stmt.set_sql_query("SELECT 21 + 21")?;
        let batch = stmt
            .execute()?
            .next()
            .expect("a record batch")
            .map_err(Error::from)?;
        assert_eq!(
            batch.column(0).as_primitive::<Decimal128Type>().value(0),
            42
        );
        Ok(())
    }
}
