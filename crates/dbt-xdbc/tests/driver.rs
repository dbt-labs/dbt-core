//! ADBC driver tests
//!
//! These tests are disabled by default because they require real database
//! accounts.
//!
//! To enable these tests set the `ADBC_DRIVER_TESTS` environment variable
//! when building these tests.
//!

mod tests {
    use std::collections::HashSet;
    use std::env;

    use adbc_core::{
        error::{Error, Result},
        options::{AdbcVersion, OptionConnection},
    };
    #[cfg(feature = "odbc")]
    use arrow_array::Array as _;
    use arrow_array::{cast::AsArray, types::*};
    use dbt_xdbc::{
        Backend, Connection, Database, Driver, QueryCtx, Statement, bigquery, connection,
        database::{self, LogLevel},
        databricks, driver, redshift, snowflake,
    };

    const ADBC_VERSION: AdbcVersion = AdbcVersion::V110;

    fn driver_for(backend: Backend) -> Result<Box<dyn Driver>> {
        driver::Builder::new(backend)
            .with_adbc_version(ADBC_VERSION)
            .try_load()
    }

    fn database_builder_for(backend: Backend) -> Result<database::Builder> {
        let mut database_builder = match backend {
            Backend::Snowflake => database::Builder::from_snowsql_config(),
            Backend::BigQuery => {
                let mut builder = database::Builder::new(backend);
                let project_id = env::var("ADBC_BIGQUERY_PROJECT").unwrap_or_default();
                let dataset_id = env::var("ADBC_BIGQUERY_DATASET").unwrap_or_default();
                let auth_credentials =
                    env::var("ADBC_BIGQUERY_CREDENTIAL_FILE").unwrap_or_default();

                builder
                    .with_named_option(
                        bigquery::AUTH_TYPE,
                        bigquery::auth_type::JSON_CREDENTIAL_FILE,
                    )?
                    .with_named_option(bigquery::PROJECT_ID, project_id)?
                    .with_named_option(bigquery::DATASET_ID, dataset_id)?
                    .with_named_option(bigquery::AUTH_CREDENTIALS, auth_credentials)?;
                Ok(builder)
            }
            Backend::Postgres | Backend::Redshift => {
                // Configuration for Postgres:
                //     CREATE ROLE username WITH LOGIN PASSWORD 'an_secure_password';
                //     CREATE DATABASE adbc_test;
                //     GRANT CONNECT ON DATABASE adbc_test TO username;
                //     GRANT ALL PRIVILEGES ON DATABASE adbc_test TO username;
                // Shell:
                //     export ADBC_POSTGRES_URI="postgres://username:an_secure_password@localhost/adbc_test"
                let uri = env::var("ADBC_POSTGRES_URI")
                    .unwrap_or("postgres://username:rocks_password@localhost/adbc_test".to_owned());
                let mut builder = database::Builder::new(backend);
                builder.with_parse_uri(uri)?;
                Ok(builder)
            }
            Backend::RedshiftODBC => {
                use redshift::odbc::*;
                // Redshift ODBC configuration (username/password authentication)
                // Docs: https://docs.aws.amazon.com/redshift/latest/mgmt/configure-odbc-connection.html

                let mut builder = database::Builder::new(backend);

                let host = env::var("REDSHIFT_HOST").unwrap();
                let port = env::var("REDSHIFT_PORT").unwrap_or("5439".to_string());
                let database = env::var("REDSHIFT_DATABASE").unwrap();
                let user = env::var("REDSHIFT_USER").unwrap();
                let password = env::var("REDSHIFT_PASSWORD").unwrap();
                // let schema = env::var("REDSHIFT_SCHEMA").unwrap();
                // schemata are configured on connection with SQL statements, not on driver object

                builder
                    .with_named_option(DRIVER, odbc_driver_path())?
                    .with_named_option(SERVER, host)?
                    .with_named_option(PORT_NUMBER, port)?
                    .with_named_option(DATABASE, database)?
                    .with_username(user)
                    .with_password(password);

                Ok(builder)
            }
            Backend::Databricks => {
                const HOST: &str = "adbc.databricks.host";
                const CATALOG: &str = "adbc.databricks.catalog";
                const SCHEMA: &str = "adbc.databricks.schema";
                const WAREHOUSE: &str = "adbc.databricks.warehouse";
                const TOKEN: &str = "adbc.databricks.token";

                let host = env::var("DATABRICKS_HOST").unwrap();
                let warehouse = env::var("DATABRICKS_WAREHOUSE").unwrap();
                let token = env::var("DATABRICKS_TOKEN").unwrap();

                let mut builder = database::Builder::new(backend);
                // optional
                if let Ok(catalog) = env::var("DATABRICKS_CATALOG") {
                    builder.with_named_option(CATALOG, catalog)?;
                }
                if let Ok(schema) = env::var("DATABRICKS_SCHEMA") {
                    builder.with_named_option(SCHEMA, schema)?;
                }

                builder
                    .with_named_option(HOST, host)?
                    .with_named_option(WAREHOUSE, warehouse)?
                    .with_named_option(TOKEN, token)?;
                Ok(builder)
            }
            Backend::DatabricksODBC => {
                use databricks::odbc::*;
                // more on Databricks ODBC configuration and authentication methods:
                // https://learn.microsoft.com/en-us/azure/databricks/integrations/odbc/authentication
                // There are more auth methods possible, but only PAT token is implemented for now.
                let mut builder = database::Builder::new(backend);

                let token = env::var("DATABRICKS_TOKEN").unwrap();
                let host = env::var("DATABRICKS_HOST").unwrap();
                let http_path = env::var("DATABRICKS_HTTP_PATH").unwrap();
                let port = env::var("DATABRICKS_PORT").unwrap_or(DEFAULT_PORT.to_string());

                // optional
                if let Ok(catalog) = env::var("DATABRICKS_CATALOG") {
                    builder.with_named_option(CATALOG, catalog)?;
                }
                if let Ok(schema) = env::var("DATABRICKS_SCHEMA") {
                    builder.with_named_option(SCHEMA, schema)?;
                }

                builder
                    .with_named_option(DRIVER, odbc_driver_path())?
                    .with_named_option(HOST, host)?
                    .with_named_option(PORT, port)?
                    .with_named_option(HTTP_PATH, http_path)?
                    .with_named_option(SSL, "1")?
                    .with_named_option(THRIFT_TRANSPORT, "2")?
                    .with_named_option(AUTH_MECHANISM, auth_mechanism_options::TOKEN)?
                    .with_username(DEFAULT_TOKEN_UID)
                    .with_password(token);
                Ok(builder)
            }
            Backend::Generic { .. } => unimplemented!("generic backend database builder in tests"),
        }?;
        if backend == Backend::Snowflake {
            database_builder
                .with_named_option(snowflake::LOG_TRACING, LogLevel::Warn.to_string())?;
        }
        Ok(database_builder)
    }

    fn database_for(backend: Backend) -> Result<Box<dyn Database>> {
        let mut driver = driver_for(backend)?;
        let database_builder = database_builder_for(backend)?;
        database_builder.build(&mut driver)
    }

    fn connection_for(backend: Backend) -> Result<Box<dyn Connection>> {
        let mut database = database_for(backend)?;
        let builder = connection::Builder::default();
        builder.build(&mut database)
    }

    fn with_database(
        backend: Backend,
        func: impl FnOnce(Box<dyn Database>) -> Result<()>,
    ) -> Result<()> {
        database_for(backend).and_then(func)
    }

    fn with_connection(
        backend: Backend,
        func: impl FnOnce(&mut dyn Connection) -> Result<()>,
    ) -> Result<()> {
        // This always clones the connection because connection methods require
        // exclusive access (&mut Connection). The alternative would be an
        // `Arc<Mutex<Connection>>` however any test failure is a panic and
        // would trigger mutex poisoning.
        //
        // TODO(mbrobbel): maybe force interior mutability via the core traits?
        connection_for(backend).and_then(|mut conn| func(&mut *conn))
    }

    fn with_empty_statement(
        backend: Backend,
        func: impl FnOnce(Box<dyn Statement>) -> Result<()>,
    ) -> Result<()> {
        with_connection(backend, |connection| {
            connection.new_statement().and_then(func)
        })
    }

    /// Check the returned info by the driver using the database methods.
    #[test_with::env(ADBC_DRIVER_TESTS)]
    #[test]
    fn database_get_info() -> Result<()> {
        with_database(Backend::Snowflake, |mut database| {
            assert_eq!(database.vendor_name(), Ok("Snowflake".to_owned()));
            assert!(
                database
                    .vendor_version()
                    .is_ok_and(|version| version.starts_with("v"))
            );
            assert!(database.vendor_arrow_version().is_ok());
            assert_eq!(database.vendor_sql(), Ok(true));
            assert_eq!(database.vendor_substrait(), Ok(false));
            assert_eq!(
                database.driver_name(),
                Ok("ADBC Snowflake Driver - Go".to_owned())
            );
            assert!(database.driver_version().is_ok());
            // XXX: re-enable when we fix driver builds to embed the version
            // assert!(database
            //     .driver_arrow_version()
            //     .is_ok_and(|version| version.starts_with("v")));
            assert_eq!(database.adbc_version(), Ok(ADBC_VERSION));
            Ok(())
        })
    }

    /// Check execute of statement with `SELECT 21 + 21` query.
    fn execute_statement(backend: Backend) -> Result<()> {
        with_empty_statement(backend, |mut statement| {
            statement.set_sql_query(&QueryCtx::new("unknown").with_sql("SELECT 21 + 21"))?;
            let batch = statement
                .execute()?
                .next()
                .expect("a record batch")
                .map_err(Error::from)?;
            match backend {
                Backend::Snowflake => {
                    assert_eq!(
                        batch.column(0).as_primitive::<Decimal128Type>().value(0),
                        42
                    );
                }
                Backend::Postgres
                | Backend::Databricks
                | Backend::DatabricksODBC
                | Backend::RedshiftODBC => {
                    assert_eq!(batch.column(0).as_primitive::<Int32Type>().value(0), 42);
                }
                _ => {
                    // BigQuery and others use Int64. We change this function as we expand the set
                    // of database integrations in XDBC.
                    assert_eq!(batch.column(0).as_primitive::<Int64Type>().value(0), 42);
                }
            }
            Ok(())
        })
    }

    #[test_with::env(ADBC_DRIVER_TESTS)]
    #[test]
    fn statement_execute_snowflake() -> Result<()> {
        execute_statement(Backend::Snowflake)
    }

    #[test_with::env(ADBC_DRIVER_TESTS)]
    #[test]
    fn statement_execute_bigquery() -> Result<()> {
        execute_statement(Backend::BigQuery)
    }

    #[test_with::env(ADBC_POSTGRES_URI)]
    #[test]
    fn statement_execute_postgres() -> Result<()> {
        execute_statement(Backend::Postgres)
    }

    #[test_with::env(DATABRICKS_TOKEN)]
    #[test]
    fn statement_execute_databricks() -> Result<()> {
        execute_statement(Backend::Databricks)
    }

    #[cfg(feature = "odbc")]
    #[test_with::env(DATABRICKS_TOKEN)]
    #[test]
    fn statement_execute_databricks_odbc() -> Result<()> {
        execute_statement(Backend::DatabricksODBC)
    }

    #[cfg(feature = "odbc")]
    #[test_with::env(REDSHIFT_USER)]
    #[test]
    fn statement_execute_redshift_odbc() -> Result<()> {
        execute_statement(Backend::RedshiftODBC)
    }

    #[cfg(feature = "odbc")]
    #[test_with::env(DATABRICKS_TOKEN)]
    #[test]
    fn statement_execute_databricks_error() -> Result<()> {
        with_empty_statement(Backend::DatabricksODBC, |mut statement| {
            // SqlExecute() returns SQL_SUCCESS on this statement instead of SQL_NO_DATA,
            // so we detect that no rows were returned by treating an error from SqlFetch()
            // as an indication that no rows were returned.
            statement.set_sql_query(
                &QueryCtx::new("unknown").with_sql("CREATE TABLE IF NOT EXISTS my_table"),
            )?;
            let mut batch_reader = statement.execute()?; // succeeds
            let batch = batch_reader.next(); // returns None
            assert!(batch.is_none());
            Ok(())
        })
    }

    #[cfg(feature = "odbc")]
    #[test_with::env(DATABRICKS_TOKEN)]
    #[test]
    fn statement_execute_databricks_empty() -> Result<()> {
        with_empty_statement(Backend::DatabricksODBC, |mut statement| {
            // SqlExecute() returns SQL_NO_DATA on this query making it very easy
            // to detect that no rows were returned but the query ran successfully.
            statement
                .set_sql_query(&QueryCtx::new("unknown").with_sql("SELECT 1 AS one WHERE 1 = 0"))?;
            let mut batch_reader = statement.execute()?; // succeeds
            let batch = batch_reader.next(); // returns None
            assert!(batch.is_none());
            Ok(())
        })
    }

    #[cfg(feature = "odbc")]
    #[test_with::env(DATABRICKS_TOKEN)]
    #[test]
    fn statement_execute_databricks_bool() -> Result<()> {
        with_empty_statement(Backend::DatabricksODBC, |mut statement| {
            statement.set_sql_query(&QueryCtx::new("unknown").with_sql(
                r#"SELECT * FROM (
                    VALUES
                      (true, false, NULL),
                      (false, true, true)
                    ) AS tbl(bool_a, bool_b, bool_c)"#,
            ))?;
            let batch = statement
                .execute()?
                .next()
                .expect("a record batch")
                .map_err(Error::from)?;
            let schema = batch.schema();
            assert_eq!(schema.field(0).name(), "bool_a");
            assert_eq!(schema.field(1).name(), "bool_b");
            assert_eq!(schema.field(2).name(), "bool_c");

            let a = batch.column(0).as_boolean();
            assert!(a.value(0));
            assert!(!a.value(1));

            let b = batch.column(1).as_boolean();
            assert!(!b.value(0));
            assert!(b.value(1));

            let c = batch.column(2).as_boolean();
            assert!(c.is_null(0));
            assert!(!c.value(0)); // null is falsy
            assert!(!c.is_null(1));
            assert!(c.value(1));

            Ok(())
        })
    }

    #[cfg(feature = "odbc")]
    #[test_with::env(DATABRICKS_TOKEN)]
    #[test]
    fn statement_execute_databricks_integer() -> Result<()> {
        with_empty_statement(Backend::DatabricksODBC, |mut statement| {
            statement.set_sql_query(&QueryCtx::new("unknown").with_sql(
                r#"SELECT * FROM (
                    VALUES
                      (16::smallint,     32,                  64,                    32,                 64),
                      (NULL,             32,                  NULL,                  32,                 NULL),
                      ( 32767::smallint, 2147483647::integer, power(10, 18)::bigint, power(10, 6)::real, power(10, 18)::double)
                    ) AS tbl(i16, i32, i64, f32, f64)"#,
            ))?;
            let batch = statement
                .execute()?
                .next()
                .expect("a record batch")
                .map_err(Error::from)?;
            let schema = batch.schema();
            assert_eq!(schema.field(0).name(), "i16");
            assert_eq!(schema.field(1).name(), "i32");
            assert_eq!(schema.field(2).name(), "i64");
            assert_eq!(schema.field(3).name(), "f32");
            assert_eq!(schema.field(4).name(), "f64");

            let int16 = batch.column(0).as_primitive::<Int16Type>();
            assert_eq!(int16.value(0), 16);
            assert!(int16.is_null(1));
            assert_eq!(int16.value(2), 32767);

            let int32 = batch.column(1).as_primitive::<Int32Type>();
            assert_eq!(int32.value(0), 32);
            assert_eq!(int32.value(1), 32);
            assert_eq!(int32.value(2), 2147483647);

            let int64 = batch.column(2).as_primitive::<Int64Type>();
            assert_eq!(int64.value(0), 64);
            assert!(int64.is_null(1));
            assert_eq!(int64.value(2), 10i64.pow(18));

            let float = batch.column(3).as_primitive::<Float32Type>();
            assert_eq!(float.value(0), 32.0);
            assert_eq!(float.value(1), 32.0);
            assert_eq!(float.value(2), 10.0f32.powi(6));

            let double = batch.column(4).as_primitive::<Float64Type>();
            assert_eq!(double.value(0), 64.0);
            assert!(double.is_null(1));
            assert_eq!(double.value(2), 10.0f64.powi(18));

            Ok(())
        })
    }

    #[cfg(feature = "odbc")]
    #[test_with::env(DATABRICKS_TOKEN)]
    #[test]
    fn statement_execute_databricks_string() -> Result<()> {
        with_empty_statement(Backend::DatabricksODBC, |mut statement| {
            const REPEAT: usize = 16;
            statement.set_sql_query(&QueryCtx::new("unknown").with_sql(
                format!(r#"SELECT * FROM (
                     VALUES
                       (21 + 21, 'Snowman ☃'),
                       (43, NULL),
                       (NULL, REPEAT('A string that is longer than 64 characters because it goes on and on about nothing in particular ☃', {REPEAT}))
                   ) AS tbl(id, name)"#).as_str(),
            ))?;
            let batch = statement
                .execute()?
                .next()
                .expect("a record batch")
                .map_err(Error::from)?;
            let schema = batch.schema();
            let fields = schema.fields();
            assert_eq!(fields[0].name(), "id");
            assert_eq!(fields[1].name(), "name");

            let int_col = batch.column(0).as_primitive::<Int32Type>();
            let str_col = batch.column(1).as_string::<i32>();
            assert!(int_col.len() == 3);
            assert!(str_col.len() == 3);

            // (42, 'Snowman ☃')
            assert!(int_col.is_valid(0));
            assert_eq!(int_col.value(0), 42);
            assert!(str_col.is_valid(0));
            assert_eq!(str_col.value(0), "Snowman ☃");

            // (43, NULL)
            assert!(int_col.is_valid(1));
            assert_eq!(int_col.value(1), 43);
            assert!(str_col.is_null(1));

            // (NULL, 'A string that is...')
            assert!(int_col.is_null(2));
            assert!(str_col.is_valid(2));
            assert_eq!(
                str_col.value(2),
                "A string that is longer than 64 characters because it goes on and on about nothing in particular ☃".repeat(REPEAT)
            );
            Ok(())
        })
    }

    #[cfg(feature = "odbc")]
    #[test_with::env(DATABRICKS_TOKEN)]
    #[test]
    fn statement_execute_databricks_binary() -> Result<()> {
        with_empty_statement(Backend::DatabricksODBC, |mut statement| {
            use std::str;
            statement.set_sql_query(&QueryCtx::new("unknown").with_sql(
                r#"SELECT * FROM (
                    VALUES
                      (X'68656C6C6F', NULL),
                      (NULL, X'68656C6C6F'),
                      (X'776F726C64', X'44617461627269636B73')
                    ) AS tbl(bin_a, bin_b)"#,
            ))?;
            let batch = statement
                .execute()?
                .next()
                .expect("a record batch")
                .map_err(Error::from)?;
            let schema = batch.schema();
            assert_eq!(schema.field(0).name(), "bin_a");
            assert_eq!(schema.field(1).name(), "bin_b");

            let a = batch.column(0).as_binary::<i32>();
            assert_eq!(str::from_utf8(a.value(0)).unwrap(), "hello");
            assert!(a.is_null(1));
            assert_eq!(str::from_utf8(a.value(2)).unwrap(), "world");

            let b = batch.column(1).as_binary::<i32>();
            assert!(b.is_null(0));
            assert_eq!(str::from_utf8(b.value(1)).unwrap(), "hello");
            assert_eq!(str::from_utf8(b.value(2)).unwrap(), "Databricks");

            Ok(())
        })
    }

    #[cfg(feature = "odbc")]
    #[test_with::env(DATABRICKS_TOKEN)]
    #[test]
    fn databricks_driver_location_error() -> Result<()> {
        let mut driver = driver_for(Backend::DatabricksODBC)?;
        let mut builder = database_builder_for(Backend::DatabricksODBC)?;
        builder.with_named_option(databricks::odbc::DRIVER, "nonexistent_driver")?;
        let mut database = builder.build(&mut driver)?;
        let conn_res = connection::Builder::default().build(&mut database);
        assert!(conn_res.is_err());
        let err = conn_res.unwrap_err();
        assert!(err.message.contains("nonexistent_driver"));
        assert!(
            err.message
                .contains("The Databricks ODBC driver can be downloaded from")
        );
        Ok(())
    }

    #[cfg(feature = "odbc")]
    #[test_with::env(REDSHIFT_PASSWORD)]
    #[test]
    fn statement_execute_redshift_wchar() -> Result<()> {
        with_connection(Backend::RedshiftODBC, |conn| {
            let mut stmt = conn.new_statement()?;
            stmt.set_sql_query(&QueryCtx::new("unknown").with_sql(
                r#"CREATE TABLE IF NOT EXISTS "special_ユーザー" (id BIGINT PRIMARY KEY, name TEXT NOT NULL)"#,
            ))?;
            let _ = stmt.execute()?;
            let mut stmt = conn.new_statement()?;
            stmt.set_sql_query(&QueryCtx::new("unknown").with_sql(
                r#"CREATE TABLE IF NOT EXISTS "special_Usuário@Info" (id BIGINT PRIMARY KEY, name TEXT NOT NULL)"#,
            ))?;
            let _ = stmt.execute()?;

            let mut stmt = conn.new_statement()?;
            stmt.set_sql_query(&QueryCtx::new("unknown").with_sql(
                r#"SELECT schemaname AS schema, tablename AS object_name
FROM pg_catalog.pg_tables
WHERE tablename LIKE 'special_%'"#,
            ))?;
            let batch = stmt.execute()?.next().expect("a record batch")?;
            let names = batch.column(1).as_string::<i32>();
            assert_eq!(names.len(), 2);
            assert_eq!(names.value(0), "special_ユーザー");
            assert_eq!(names.value(1), "special_Usuário@Info");
            Ok(())
        })
    }

    #[test_with::env(ADBC_DRIVER_TESTS)]
    #[test]
    fn commit_snowflake() -> Result<()> {
        // https://github.com/apache/arrow-adbc/issues/2581
        with_connection(Backend::Snowflake, |conn| {
            conn.set_option(OptionConnection::AutoCommit, "false".into())?;
            let mut stmt = conn.new_statement()?;
            stmt.set_sql_query(
                &QueryCtx::new("unknown").with_sql("SELECT 'could be an insert statement'"),
            )?;
            let batch = stmt
                .execute()?
                .next()
                .expect("a record batch")
                .map_err(Error::from)?;
            assert_eq!(
                batch.column(0).as_string::<i32>().value(0),
                "could be an insert statement"
            );
            conn.commit()
        })
    }

    #[test_with::env(ADBC_DRIVER_TESTS)]
    #[test]
    /// Check execute schema of statement with `SHOW WAREHOUSES` query.
    fn statement_execute_schema() -> Result<()> {
        let backend = Backend::Snowflake;
        with_empty_statement(backend, |mut statement| {
            statement.set_sql_query(&QueryCtx::new("unknown").with_sql("SHOW WAREHOUSES"))?;
            let schema = statement.execute_schema()?;
            let field_names = schema
                .fields()
                .into_iter()
                .map(|field| field.name().as_ref())
                .collect::<HashSet<_>>();
            let expected_field_names = [
                "name",
                "state",
                "type",
                "size",
                "running",
                "queued",
                "is_default",
                "is_current",
                "auto_suspend",
                "auto_resume",
                "available",
                "provisioning",
                "quiescing",
                "other",
                "created_on",
                "resumed_on",
                "updated_on",
                "owner",
                "comment",
                "resource_monitor",
                "actives",
                "pendings",
                "failed",
                "suspended",
                "uuid",
                // "budget",
                "owner_role_type",
            ]
            .into_iter()
            .collect::<HashSet<_>>();
            assert_eq!(
                expected_field_names
                    .difference(&field_names)
                    .collect::<Vec<_>>(),
                Vec::<&&str>::default()
            );
            Ok(())
        })
    }
}
