use std::borrow::Cow;

use dbt_adbc::{Backend, database};
use url::Url;

use crate::{AdapterConfig, Auth, AuthError, AuthWarningPrinter, NoopAuthWarningPrinter};

#[derive(Debug)]
struct MotherDuckAuthIR<'a> {
    host: &'a str,
    token: &'a str,
    port: u16,
    user: &'a str,
    database: &'a str,
    sslrootcert: &'a str,
    connect_timeout: Cow<'a, str>,
}

fn non_empty<'a>(config: &'a AdapterConfig, field: &str) -> Result<&'a str, AuthError> {
    let value = config.require_str(field)?;
    if value.trim().is_empty() || value.contains('\0') {
        return Err(AuthError::config(format!(
            "{field} must be a non-empty string without NUL characters"
        )));
    }
    Ok(value)
}

fn parse_auth<'a>(config: &'a AdapterConfig) -> Result<MotherDuckAuthIR<'a>, AuthError> {
    let host = non_empty(config, "host")?;
    let token = non_empty(config, "token")?;
    let user = config.get_str("user").unwrap_or("postgres");
    let database = non_empty(config, "database")?;
    let sslrootcert = config.get_str("sslrootcert").unwrap_or("system");
    let port = config
        .get_string("port")
        .unwrap_or(Cow::Borrowed("5432"))
        .parse::<u16>()
        .ok()
        .filter(|port| *port > 0)
        .ok_or_else(|| AuthError::config("port must be an integer between 1 and 65535"))?;
    let connect_timeout = config
        .get_string("connect_timeout")
        .unwrap_or(Cow::Borrowed("240"));
    if connect_timeout
        .parse::<u64>()
        .ok()
        .filter(|timeout| *timeout > 0)
        .is_none()
    {
        return Err(AuthError::config(
            "connect_timeout must be a positive integer",
        ));
    }
    if database.contains('=')
        || database.starts_with("postgres://")
        || database.starts_with("postgresql://")
    {
        return Err(AuthError::config(
            "database must be a database name, not a connection string",
        ));
    }

    Ok(MotherDuckAuthIR {
        host,
        token,
        port,
        user,
        database,
        sslrootcert,
        connect_timeout,
    })
}

pub struct MotherDuckAuth {
    _warning_printer: Box<dyn AuthWarningPrinter>,
}

impl MotherDuckAuth {
    pub fn new(warning_printer: Box<dyn AuthWarningPrinter>) -> Self {
        Self {
            _warning_printer: warning_printer,
        }
    }
}

pub fn auth() -> Box<dyn Auth> {
    Box::new(MotherDuckAuth::new(Box::new(NoopAuthWarningPrinter)))
}

impl Auth for MotherDuckAuth {
    fn backend(&self) -> Backend {
        Backend::MotherDuck
    }

    fn configure(&self, config: &AdapterConfig) -> Result<database::Builder, AuthError> {
        let auth = parse_auth(config)?;
        let mut uri = Url::parse("postgresql://localhost")
            .expect("the static MotherDuck PostgreSQL URL is valid");
        uri.set_host(Some(auth.host))
            .map_err(|_| AuthError::config("host is not a valid hostname"))?;
        uri.set_port(Some(auth.port))
            .map_err(|_| AuthError::config("port must be an integer between 1 and 65535"))?;
        uri.set_path(auth.database);
        uri.query_pairs_mut()
            .append_pair("sslmode", "verify-full")
            .append_pair("sslrootcert", auth.sslrootcert)
            .append_pair("connect_timeout", &auth.connect_timeout)
            .append_pair("options", "--attach_mode=single");

        let mut builder = database::Builder::new(self.backend());
        builder
            .with_uri(uri)
            .with_username(auth.user)
            .with_password(auth.token);
        Ok(builder)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_options::uri_value;
    use dbt_yaml::Mapping;

    fn config() -> AdapterConfig {
        AdapterConfig::new(Mapping::from_iter([
            ("host".into(), "pg.us-east-1-aws.motherduck.com".into()),
            ("token".into(), "secret/token".into()),
            ("database".into(), "analytics".into()),
            ("schema".into(), "main".into()),
        ]))
    }

    #[test]
    fn builds_secure_postgres_endpoint_uri_with_defaults() {
        let builder = MotherDuckAuth::new(Box::new(NoopAuthWarningPrinter))
            .configure(&config())
            .expect("configure");
        let uri = uri_value(&builder);

        assert!(uri.starts_with("postgresql://pg.us-east-1-aws.motherduck.com:5432/analytics?"));
        assert!(uri.contains("sslmode=verify-full"));
        assert!(uri.contains("sslrootcert=system"));
        assert!(uri.contains("connect_timeout=240"));
        assert!(uri.contains("options=--attach_mode%3Dsingle"));
        assert!(uri.contains("user=postgres"));
        assert!(uri.contains("password=secret%2Ftoken"));
        assert!(!format!("{builder:?}").contains("secret/token"));
    }

    #[test]
    fn rejects_connection_string_as_database_name() {
        let mut mapping = config().repr().clone();
        mapping.insert("database".into(), "postgresql://example.com/db".into());
        let err = MotherDuckAuth::new(Box::new(NoopAuthWarningPrinter))
            .configure(&AdapterConfig::new(mapping))
            .expect_err("database URI must be rejected");
        assert_eq!(
            err.msg(),
            "database must be a database name, not a connection string"
        );
    }

    #[test]
    fn rejects_port_zero() {
        let mut mapping = config().repr().clone();
        mapping.insert("port".into(), 0.into());
        let err = MotherDuckAuth::new(Box::new(NoopAuthWarningPrinter))
            .configure(&AdapterConfig::new(mapping))
            .expect_err("port zero must be rejected");
        assert_eq!(err.msg(), "port must be an integer between 1 and 65535");
    }
}
