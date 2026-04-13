use crate::{AdapterConfig, Auth, AuthError, AuthOutcome, auth_configure_pipeline};
use database::Builder as DatabaseBuilder;

use dbt_xdbc::{Backend, database};
use std::borrow::Cow;

const DEFAULT_HOST: &str = "localhost";
const DEFAULT_HTTP_PORT: &str = "8123";
const DEFAULT_HTTPS_PORT: &str = "8443";
const DEFAULT_USER: &str = "default";

#[derive(Debug)]
enum ClickHouseAuthIR<'a> {
    UserPass {
        user: &'a str,
        password: Cow<'a, str>,
        host: &'a str,
        port: Cow<'a, str>,
        secure: bool,
    },
}

impl<'a> ClickHouseAuthIR<'a> {
    pub fn apply(self, mut builder: DatabaseBuilder) -> Result<DatabaseBuilder, AuthError> {
        match self {
            Self::UserPass {
                user,
                password,
                host,
                port,
                secure,
            } => {
                let scheme = if secure { "https" } else { "http" };
                builder.with_parse_uri(format!("{scheme}://{host}:{port}"))?;
                builder.with_username(user);
                builder.with_password(password.as_ref());
            }
        }

        Ok(builder)
    }
}

fn parse_auth<'a>(config: &'a AdapterConfig) -> Result<ClickHouseAuthIR<'a>, AuthError> {
    let secure = config
        .get_string("secure")
        .map(|s| s == "true" || s == "1" || s == "True")
        .unwrap_or(false);

    let default_port = if secure {
        DEFAULT_HTTPS_PORT
    } else {
        DEFAULT_HTTP_PORT
    };

    Ok(ClickHouseAuthIR::UserPass {
        user: config.get_str("user").unwrap_or(DEFAULT_USER),
        password: config.get_string("password").unwrap_or(Cow::Borrowed("")),
        host: config.get_str("host").unwrap_or(DEFAULT_HOST),
        port: config
            .get_string("port")
            .unwrap_or(Cow::Borrowed(default_port)),
        secure,
    })
}

fn apply_connection_args(
    _config: &AdapterConfig,
    builder: DatabaseBuilder,
) -> Result<DatabaseBuilder, AuthError> {
    Ok(builder)
}

pub struct ClickHouseAuth;

impl Auth for ClickHouseAuth {
    fn backend(&self) -> Backend {
        Backend::ClickHouse
    }

    fn configure(&self, config: &AdapterConfig) -> Result<AuthOutcome, AuthError> {
        auth_configure_pipeline!(self.backend(), &config, parse_auth, apply_connection_args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_options::uri_value;
    use dbt_test_primitives::assert_contains;
    use dbt_yaml::Mapping;
    use dbt_yaml::Value as YmlValue;

    #[test]
    fn test_defaults_produce_valid_uri() {
        let config = Mapping::new();

        let builder = ClickHouseAuth {}
            .configure(&AdapterConfig::new(config))
            .expect("configure")
            .builder;

        let uri = uri_value(&builder);
        assert_contains!(&uri, "http://localhost:8123");
    }

    #[test]
    fn test_custom_host_and_port() {
        let config = Mapping::from_iter([
            ("host".into(), "ch.prod.internal".into()),
            ("port".into(), "9000".into()),
            ("user".into(), "alice".into()),
            ("password".into(), "secret".into()),
        ]);

        let builder = ClickHouseAuth {}
            .configure(&AdapterConfig::new(config))
            .expect("configure")
            .builder;

        let uri = uri_value(&builder);
        assert_contains!(&uri, "http://ch.prod.internal:9000");
    }

    #[test]
    fn test_secure_uses_https_and_default_port_8443() {
        let config = Mapping::from_iter([
            ("host".into(), "ch.cloud".into()),
            ("secure".into(), "true".into()),
        ]);

        let builder = ClickHouseAuth {}
            .configure(&AdapterConfig::new(config))
            .expect("configure")
            .builder;

        let uri = uri_value(&builder);
        assert_contains!(&uri, "https://ch.cloud:8443");
    }

    #[test]
    fn test_secure_as_yaml_boolean() {
        let config: Mapping = dbt_yaml::from_str(
            r#"
host: ch.cloud
secure: true
"#,
        )
        .expect("parse yaml");

        let builder = ClickHouseAuth {}
            .configure(&AdapterConfig::new(config))
            .expect("configure")
            .builder;

        let uri = uri_value(&builder);
        assert_contains!(&uri, "https://ch.cloud:8443");
    }

    #[test]
    fn test_numeric_secure_1_enables_https() {
        let config = Mapping::from_iter([
            ("host".into(), "ch.local".into()),
            ("secure".into(), YmlValue::number(1i64.into())),
        ]);

        let builder = ClickHouseAuth {}
            .configure(&AdapterConfig::new(config))
            .expect("configure")
            .builder;

        let uri = uri_value(&builder);
        assert_contains!(&uri, "https://ch.local:8443");
    }

    #[test]
    fn test_unexpected_secure_value_does_not_enable_https() {
        let config = Mapping::from_iter([
            ("host".into(), "ch.local".into()),
            ("secure".into(), YmlValue::number(42i64.into())),
        ]);

        let builder = ClickHouseAuth {}
            .configure(&AdapterConfig::new(config))
            .expect("configure")
            .builder;

        let uri = uri_value(&builder);
        assert_contains!(&uri, "http://ch.local:8123");
    }
}
