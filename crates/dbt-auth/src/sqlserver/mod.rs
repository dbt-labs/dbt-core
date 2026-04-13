#![allow(dead_code, unused_mut, reason = "TODO: implement")]

use std::borrow::Cow;

use crate::{AdapterConfig, Auth, AuthError, AuthOutcome, auth_configure_pipeline};

use dbt_xdbc::{
    Backend,
    database::{self, Builder as DatabaseBuilder},
};

const DEFAULT_AUTH: &str = "ActiveDirectoryServicePrincipal";
const DEFAULT_PORT: &str = "1433";

#[derive(Debug)]
enum SQLServerAuthIR<'a> {
    // TODO: we have a few different auth methods to support.
    // For simplicity, we can start with just ServicePrincipal, which is the default in the core adapter.
    ActiveDirectoryServicePrincipal {
        host: &'a str,
        port: Cow<'a, str>,
        database: &'a str,
        tenant_id: Option<&'a str>,
        client_id: &'a str,
        client_secret: &'a str,
    },
}

impl<'a> SQLServerAuthIR<'a> {
    pub fn apply(self, mut builder: DatabaseBuilder) -> Result<DatabaseBuilder, AuthError> {
        // nearly all auth parameters are set in the URI
        // There are quite a few parameters that can be set
        // See: https://github.com/microsoft/go-mssqldb/tree/main?tab=readme-ov-file#connection-parameters-and-dsn
        match self {
            Self::ActiveDirectoryServicePrincipal {
                host,
                port,
                database,
                tenant_id,
                client_id,
                client_secret,
            } => {
                // both "mssql://" and "sqlserver://" are supported by the driver,
                // but it seems like "sqlserver://" is the preferred scheme according to the underlying Go driver docs.
                //
                // See: https://github.com/microsoft/go-mssqldb?tab=readme-ov-file#deprecated
                //
                // TODO: we probably want to be a bit smarter about constructing the URI, but this is a start
                let b = builder.with_parse_uri(format!("sqlserver://{host}:{port}"))?;

                if let Some(uri) = b.uri.as_mut() {
                    let uid: Cow<str> = tenant_id.map_or_else(
                        || client_id.into(),
                        |tenant_id| format!("{client_id}@{tenant_id}").into(),
                    );

                    uri.query_pairs_mut()
                        .append_pair("database", database)
                        .append_pair("fedauth", DEFAULT_AUTH)
                        .append_pair("user id", &uid)
                        .append_pair("password", client_secret)
                        .finish();
                }
            }
        }
        Ok(builder)
    }
}

fn parse_auth<'a>(config: &'a AdapterConfig) -> Result<SQLServerAuthIR<'a>, AuthError> {
    Ok(SQLServerAuthIR::ActiveDirectoryServicePrincipal {
        host: config.require_str("host")?,
        port: config
            .get_string("port")
            .unwrap_or_else(|| DEFAULT_PORT.into()),
        database: config.require_str("database")?,
        tenant_id: config.get_str("tenant_id"),
        client_id: config.require_str("client_id")?,
        client_secret: config.require_str("client_secret")?,
    })
}

fn apply_connection_args(
    _config: &AdapterConfig,
    builder: DatabaseBuilder,
) -> Result<DatabaseBuilder, AuthError> {
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
        auth_configure_pipeline!(self.backend(), &config, parse_auth, apply_connection_args)
    }
}

#[cfg(test)]
mod tests {
    // TODO
}
