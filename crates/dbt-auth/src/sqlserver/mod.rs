#![allow(dead_code, unused_mut, reason = "TODO: implement")]

use std::borrow::Cow;

use crate::{AdapterConfig, Auth, AuthError, AuthOutcome};

use dbt_xdbc::{
    Backend,
    database::{self, Builder as DatabaseBuilder},
};

const DEFAULT_AUTH: &str = "ServicePrincipal";
const DEFAULT_PORT: &str = "1433";

#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
enum SQLServerAuthIR<'a> {
    // TODO: we have a few different auth methods to support.
    // For simplicity, we can start with just ServicePrincipal, which is the default in the core adapter.
    ActiveDirectoryServicePrincipal {
        tenant_id: Option<&'a str>,
        client_id: &'a str,
        client_secret: &'a str,
    },
    ActiveDirectoryPassword {
        client_id: &'a str,
        user: &'a str,
        password: &'a str,
    },
    ActiveDirectoryEnvironment,
}

impl<'a> SQLServerAuthIR<'a> {
    pub fn apply(self, mut builder: DatabaseBuilder) -> Result<DatabaseBuilder, AuthError> {
        // nearly all auth parameters are set in the URI
        // There are quite a few parameters that can be set
        // See: https://github.com/microsoft/go-mssqldb/tree/main?tab=readme-ov-file#connection-parameters-and-dsn
        match self {
            Self::ActiveDirectoryServicePrincipal {
                tenant_id,
                client_id,
                client_secret,
            } => {
                if let Some(uri) = builder.uri.as_mut() {
                    let uid: Cow<str> = tenant_id.map_or_else(
                        || client_id.into(),
                        |tenant_id| format!("{client_id}@{tenant_id}").into(),
                    );

                    uri.query_pairs_mut()
                        .append_pair("fedauth", "ActiveDirectoryServicePrincipal")
                        .append_pair("user id", &uid)
                        .append_pair("password", client_secret)
                        .finish();
                }
            }
            Self::ActiveDirectoryPassword {
                client_id,
                user,
                password,
            } => {
                if let Some(uri) = builder.uri.as_mut() {
                    uri.query_pairs_mut()
                        .append_pair("fedauth", "ActiveDirectoryPassword")
                        .append_pair("user id", user)
                        .append_pair("password", password)
                        .append_pair("applicationclientid", client_id)
                        .finish();
                }
            }
            Self::ActiveDirectoryEnvironment => {
                if let Some(uri) = builder.uri.as_mut() {
                    uri.query_pairs_mut()
                        .append_pair("fedauth", "ActiveDirectoryEnvironment")
                        .finish();
                }
            }
        }
        Ok(builder)
    }
}

fn parse_auth<'a>(config: &'a AdapterConfig) -> Result<SQLServerAuthIR<'a>, AuthError> {
    let authentication = config.get_str("authentication").unwrap_or(DEFAULT_AUTH);
    match authentication {
        "ServicePrincipal" => Ok(SQLServerAuthIR::ActiveDirectoryServicePrincipal {
            tenant_id: config.get_str("tenant_id"),
            client_id: config.require_str("client_id")?,
            client_secret: config.require_str("client_secret")?,
        }),
        "ActiveDirectoryPassword" => Ok(SQLServerAuthIR::ActiveDirectoryPassword {
            user: config.require_str("UID")?,
            password: config.require_str("PWD")?,
            client_id: config.require_str("client_id")?,
        }),
        "environment" => Ok(SQLServerAuthIR::ActiveDirectoryEnvironment),
        "ActiveDirectoryInteractive" | "ActiveDirectoryIntegrated" | "CLI" | "auto" => {
            unimplemented!("authentication method {} not implemented", authentication)
        }
        _ => Err(AuthError::config(format!(
            "Invalid authentication method: {authentication} must be one of: [ServicePrincipal, ActiveDirectoryPassword, environment]"
        ))),
    }
}

fn apply_connection_args(
    config: &AdapterConfig,
    mut builder: DatabaseBuilder,
) -> Result<DatabaseBuilder, AuthError> {
    let host = config.require_str("host")?;
    let port = config
        .get_string("port")
        .unwrap_or_else(|| DEFAULT_PORT.into());

    // both "mssql://" and "sqlserver://" are supported by the driver,
    // but it seems like "sqlserver://" is the preferred scheme according to the underlying Go driver docs.
    //
    // See: https://github.com/microsoft/go-mssqldb?tab=readme-ov-file#deprecated
    //
    // TODO: we probably want to be a bit smarter about constructing the URI, but this is a start
    builder.with_parse_uri(format!("sqlserver://{host}:{port}"))?;

    if let Some(uri) = builder.uri.as_mut() {
        uri.query_pairs_mut()
            .append_pair("database", config.require_str("database")?)
            .finish();
    }

    // Other parameters, i.e.
    // - connection timeout
    // - dial timeout
    // - encrypt
    // - app name
    // - log
    //
    // See: https://github.com/microsoft/go-mssqldb/tree/main?tab=readme-ov-file#less-common-parameters
    Ok(builder)
}

pub struct SQLServerAuth;

impl Auth for SQLServerAuth {
    fn backend(&self) -> Backend {
        Backend::SQLServer
    }

    fn configure(&self, config: &AdapterConfig) -> Result<AuthOutcome, AuthError> {
        let authentication_args = parse_auth(config)?;
        let builder = database::Builder::new(self.backend());
        let builder = apply_connection_args(config, builder)?;
        let builder = authentication_args.apply(builder)?;
        Ok(AuthOutcome {
            builder,
            warnings: vec![],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_options::uri_value;
    use dbt_test_primitives::assert_contains;
    use dbt_yaml::Mapping;

    fn make_config(pairs: impl IntoIterator<Item = (&'static str, &'static str)>) -> AdapterConfig {
        AdapterConfig::new(Mapping::from_iter(
            pairs.into_iter().map(|(k, v)| (k.into(), v.into())),
        ))
    }

    #[test]
    fn test_service_principal_with_tenant_id() {
        let config = make_config([
            ("authentication", "ServicePrincipal"),
            ("host", "myserver.database.windows.net"),
            ("database", "mydb"),
            ("tenant_id", "my-tenant"),
            ("client_id", "my-client"),
            ("client_secret", "my-secret"),
        ]);

        let outcome = SQLServerAuth.configure(&config).expect("configure");
        let uri = uri_value(&outcome.builder);

        assert_contains!(&uri, "sqlserver://myserver.database.windows.net:1433");
        assert_contains!(&uri, "database=mydb");
        assert_contains!(&uri, "fedauth=ActiveDirectoryServicePrincipal");
        assert_contains!(&uri, "user+id=my-client%40my-tenant");
        assert_contains!(&uri, "password=my-secret");
    }

    #[test]
    fn test_service_principal_without_tenant_id() {
        let config = make_config([
            ("authentication", "ServicePrincipal"),
            ("host", "myserver.database.windows.net"),
            ("database", "mydb"),
            ("client_id", "my-client"),
            ("client_secret", "my-secret"),
        ]);

        let outcome = SQLServerAuth.configure(&config).expect("configure");
        let uri = uri_value(&outcome.builder);

        assert_contains!(&uri, "fedauth=ActiveDirectoryServicePrincipal");
        assert_contains!(&uri, "user+id=my-client");
        assert_contains!(&uri, "password=my-secret");
    }

    #[test]
    fn test_active_directory_password() {
        let config = make_config([
            ("authentication", "ActiveDirectoryPassword"),
            ("host", "myserver.database.windows.net"),
            ("database", "mydb"),
            ("client_id", "my-client"),
            ("UID", "alice@example.com"),
            ("PWD", "hunter2"),
        ]);

        let outcome = SQLServerAuth.configure(&config).expect("configure");
        let uri = uri_value(&outcome.builder);

        assert_contains!(&uri, "sqlserver://myserver.database.windows.net:1433");
        assert_contains!(&uri, "fedauth=ActiveDirectoryPassword");
        assert_contains!(&uri, "user+id=alice%40example.com");
        assert_contains!(&uri, "password=hunter2");
        assert_contains!(&uri, "applicationclientid=my-client");
    }

    #[test]
    fn test_environment_auth() {
        let config = make_config([
            ("authentication", "environment"),
            ("host", "myserver.database.windows.net"),
            ("database", "mydb"),
        ]);

        let outcome = SQLServerAuth.configure(&config).expect("configure");
        let uri = uri_value(&outcome.builder);

        assert_contains!(&uri, "sqlserver://myserver.database.windows.net:1433");
        assert_contains!(&uri, "database=mydb");
        assert_contains!(&uri, "fedauth=ActiveDirectoryEnvironment");
    }

    #[test]
    fn test_default_port_is_1433() {
        let config = make_config([
            ("authentication", "environment"),
            ("host", "myserver.database.windows.net"),
            ("database", "mydb"),
        ]);

        let outcome = SQLServerAuth.configure(&config).expect("configure");
        let uri = uri_value(&outcome.builder);

        assert_contains!(&uri, ":1433");
    }

    #[test]
    fn test_custom_port() {
        let config = make_config([
            ("authentication", "environment"),
            ("host", "myserver.database.windows.net"),
            ("port", "1434"),
            ("database", "mydb"),
        ]);

        let outcome = SQLServerAuth.configure(&config).expect("configure");
        let uri = uri_value(&outcome.builder);

        assert_contains!(&uri, ":1434");
    }

    #[test]
    fn test_service_principal_alias() {
        // "ServicePrincipal" is an alias for "ActiveDirectoryServicePrincipal"
        let config = make_config([
            ("authentication", "ServicePrincipal"),
            ("host", "myserver.database.windows.net"),
            ("database", "mydb"),
            ("client_id", "my-client"),
            ("client_secret", "my-secret"),
        ]);

        let outcome = SQLServerAuth.configure(&config).expect("configure");
        let uri = uri_value(&outcome.builder);

        assert_contains!(&uri, "fedauth=ActiveDirectoryServicePrincipal");
    }
}
