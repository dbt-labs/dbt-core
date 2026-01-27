use crate::{AdapterConfig, Auth, AuthError};
use std::borrow::Cow;

use dbt_xdbc::{Backend, database, spark};

/// User agent name provided to Spark by Fusion.
#[expect(dead_code)]
const USER_AGENT_NAME: &str = "dbt";

const DEFAULT_AUTH: &str = "NONE";
const DEFAULT_PORT: &str = "10000";

pub struct SparkAuth;

impl Auth for SparkAuth {
    fn backend(&self) -> Backend {
        Backend::Spark
    }

    fn configure(&self, config: &AdapterConfig) -> Result<database::Builder, AuthError> {
        let mut builder = database::Builder::new(self.backend());

        let host = config
            .get_string("host")
            .ok_or_else(|| AuthError::config("'host' is a required Spark configuration"))?;
        builder.with_named_option(spark::HOST, host)?;

        let port = config
            .get_string("port")
            .unwrap_or(Cow::Borrowed(DEFAULT_PORT));
        builder.with_named_option(spark::PORT, port)?;

        let method = config
            .get_string("method")
            .ok_or_else(|| AuthError::config("'method' is a required Spark configuration"))?;
        let transport_api = match method.as_ref() {
            "thrift" => Ok(spark::transport_api::THRIFT_BINARY),
            "http" => Ok(spark::transport_api::THRIFT_HTTP),
            "livy" => unimplemented!("livy"),
            _ => Err(AuthError::config("unsupported Spark method")),
        }?;
        builder.with_named_option(spark::TRANSPORT_API, transport_api)?;

        let auth = config
            .get_string("auth")
            .unwrap_or(Cow::Borrowed(DEFAULT_AUTH));

        let auth_type = match auth.as_ref() {
            "NOSASL" => spark::auth_type::NOSASL,
            "NONE" => spark::auth_type::PLAIN,
            "LDAP" => spark::auth_type::LDAP,
            "KERBEROS" => {
                builder.with_named_option(
                    spark::KERBEROS_SERVICE_NAME,
                    config.get_string("kerberos_service_name").ok_or_else(|| {
                        AuthError::config(
                            "'kerberos_service_name' is required when auth is 'KERBEROS'",
                        )
                    })?,
                )?;

                spark::auth_type::KERBEROS
            }
            _ => {
                return Err(AuthError::config("Invalid 'auth' method for Spark"));
            }
        };

        builder.with_named_option(spark::AUTH_TYPE, auth_type)?;

        Ok(builder)
    }
}

// TODO: tests
