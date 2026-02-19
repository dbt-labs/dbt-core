use crate::{AdapterConfig, Auth, AuthError, auth_configure_pipeline};
use database::Builder as DatabaseBuilder;

use dbt_xdbc::{Backend, database, spark};

/// User agent name provided to Spark by Fusion.
#[expect(dead_code)]
const USER_AGENT_NAME: &str = "dbt";

const DEFAULT_AUTH: &str = "NONE";
const DEFAULT_PORT: &str = "10000";

#[derive(Debug)]
enum SparkAuthIR<'a> {
    Plain,
    Nosasl,
    Ldap,
    Kerberos { service_name: &'a str },
}

impl<'a> SparkAuthIR<'a> {
    pub fn apply(self, mut builder: DatabaseBuilder) -> Result<DatabaseBuilder, AuthError> {
        let auth_type = match self {
            Self::Nosasl => spark::auth_type::NOSASL,
            Self::Plain => spark::auth_type::PLAIN,
            Self::Ldap => spark::auth_type::LDAP,
            Self::Kerberos { service_name } => {
                builder.with_named_option(spark::KERBEROS_SERVICE_NAME, service_name)?;
                spark::auth_type::KERBEROS
            }
        };

        builder.with_named_option(spark::AUTH_TYPE, auth_type)?;
        Ok(builder)
    }
}

fn parse_auth<'a>(config: &'a AdapterConfig) -> Result<SparkAuthIR<'a>, AuthError> {
    let raw_auth = config.get_string("auth");
    let auth = raw_auth.as_deref().unwrap_or(DEFAULT_AUTH);

    match auth {
        "NOSASL" => Ok(SparkAuthIR::Nosasl),
        "NONE" => Ok(SparkAuthIR::Plain),
        "LDAP" => Ok(SparkAuthIR::Ldap),
        "KERBEROS" => {
            let service_name = config.get_str("kerberos_service_name").ok_or_else(|| {
                AuthError::config("'kerberos_service_name' is required when auth is 'KERBEROS'")
            })?;
            Ok(SparkAuthIR::Kerberos { service_name })
        }
        _ => Err(AuthError::config("Invalid 'auth' method for Spark")),
    }
}

fn apply_connection_args(
    config: &AdapterConfig,
    mut builder: DatabaseBuilder,
) -> Result<DatabaseBuilder, AuthError> {
    let host = config
        .get_str("host")
        .ok_or_else(|| AuthError::config("'host' is a required Spark configuration"))?;
    builder.with_named_option(spark::HOST, host)?;

    let raw_port = config.get_string("port");
    let port = raw_port.as_deref().unwrap_or(DEFAULT_PORT);
    builder.with_named_option(spark::PORT, port)?;

    // method in Spark is not an authentication field
    let method = config
        .get_str("method")
        .ok_or_else(|| AuthError::config("'method' is a required Spark configuration"))?;
    let transport_api = match method {
        "thrift" => Ok(spark::transport_api::THRIFT_BINARY),
        "http" => Ok(spark::transport_api::THRIFT_HTTP),
        "livy" => unimplemented!("livy"),
        _ => Err(AuthError::config("unsupported Spark method")),
    }?;
    builder.with_named_option(spark::TRANSPORT_API, transport_api)?;

    Ok(builder)
}

pub struct SparkAuth;

impl Auth for SparkAuth {
    fn backend(&self) -> Backend {
        Backend::Spark
    }

    fn configure(&self, config: &AdapterConfig) -> Result<database::Builder, AuthError> {
        auth_configure_pipeline!(self.backend(), &config, parse_auth, apply_connection_args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_options::other_option_value;
    use dbt_yaml::Mapping;
    use dbt_yaml::Value as YmlValue;

    #[test]
    fn test_numeric_port_is_respected() {
        let config = Mapping::from_iter([
            ("host".into(), "H".into()),
            ("method".into(), "thrift".into()),
            ("port".into(), YmlValue::number(10001i64.into())),
        ]);

        let builder = SparkAuth {}
            .configure(&AdapterConfig::new(config))
            .expect("configure");

        assert_eq!(other_option_value(&builder, spark::PORT), Some("10001"));
    }

    #[test]
    fn test_non_string_auth_does_not_silently_default() {
        let config = Mapping::from_iter([
            ("host".into(), "H".into()),
            ("method".into(), "thrift".into()),
            ("auth".into(), YmlValue::number(1i64.into())),
        ]);

        let err = SparkAuth {}
            .configure(&AdapterConfig::new(config))
            .expect_err("configure should fail");
        assert_eq!(err.msg(), "Invalid 'auth' method for Spark");
    }
}
