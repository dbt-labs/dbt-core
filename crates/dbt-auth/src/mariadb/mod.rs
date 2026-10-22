use crate::{AdapterConfig, Auth, AuthError, AuthWarningPrinter, auth_configure_pipeline};
use database::Builder as DatabaseBuilder;
use dbt_adbc::{Backend, database, mysql};
use std::borrow::Cow;

const DEFAULT_HOST: &str = "localhost";
const DEFAULT_PORT: &str = "3306";

/// Value of the driver's `tls` DSN parameter for each profile `ssl_mode`.
///
/// `preferred` is the default: MariaDB 11.4+ enables TLS out of the box with an
/// auto-generated certificate, which `preferred` accepts without any
/// certificate setup. `verify_ca` checks the server certificate against the
/// system trust store.
fn tls_param(ssl_mode: &str) -> Result<&'static str, AuthError> {
    match ssl_mode {
        "preferred" => Ok("preferred"),
        "required" => Ok("skip-verify"),
        "verify_ca" => Ok("true"),
        "disabled" => Ok("false"),
        other => Err(AuthError::config(format!(
            "Invalid MariaDB 'ssl_mode' '{other}': expected one of \
             'preferred', 'required', 'verify_ca', 'disabled'"
        ))),
    }
}

#[derive(Debug)]
enum MariaDbAuthIR<'a> {
    UserPass {
        user: &'a str,
        password: Cow<'a, str>,
        host: &'a str,
        port: Cow<'a, str>,
        tls: &'static str,
        connect_timeout: Option<Cow<'a, str>>,
    },
}

impl<'a> MariaDbAuthIR<'a> {
    pub fn apply(
        self,
        mut builder: DatabaseBuilder,
        _warning_printer: &dyn AuthWarningPrinter,
    ) -> Result<DatabaseBuilder, AuthError> {
        match self {
            Self::UserPass {
                user,
                password,
                host,
                port,
                tls,
                connect_timeout,
            } => {
                // No default database in the URI: the target schema may not
                // exist yet, and dbt always renders `schema`.`identifier`.
                let mut uri = format!("mysql://{host}:{port}/");

                uri.push_str("?tls=");
                uri.push_str(tls);

                // Plain numbers are seconds; duration strings such as `1s`
                // (set by `dbt debug`) pass through unchanged.
                if let Some(timeout) = &connect_timeout {
                    let unit = if timeout.chars().all(|c| c.is_ascii_digit()) {
                        "s"
                    } else {
                        ""
                    };
                    uri.push_str(&format!("&timeout={timeout}{unit}"));
                }

                builder.with_parse_uri(uri)?;
                builder.with_username(user);
                builder.with_password(password.as_ref());
            }
        }

        Ok(builder)
    }
}

fn parse_auth<'a>(
    config: &'a AdapterConfig,
    _warning_printer: &dyn AuthWarningPrinter,
) -> Result<MariaDbAuthIR<'a>, AuthError> {
    let user = config
        .get_str("user")
        .ok_or_else(|| AuthError::config("MariaDB requires 'user' in profile configuration"))?;

    let password = config
        .get_string("password")
        .ok_or_else(|| AuthError::config("MariaDB requires 'password' in profile configuration"))?;

    let tls = tls_param(config.get_str("ssl_mode").unwrap_or("preferred"))?;

    Ok(MariaDbAuthIR::UserPass {
        user,
        password,
        host: config.get_str("host").unwrap_or(DEFAULT_HOST),
        port: config
            .get_string("port")
            .unwrap_or(Cow::Borrowed(DEFAULT_PORT)),
        tls,
        connect_timeout: config.get_string("connect_timeout"),
    })
}

fn apply_connection_args(
    _config: &AdapterConfig,
    mut builder: DatabaseBuilder,
    _warning_printer: &dyn AuthWarningPrinter,
) -> Result<DatabaseBuilder, AuthError> {
    builder.with_named_option(mysql::VENDOR, mysql::VENDOR_MARIADB)?;
    Ok(builder)
}

pub struct MariaDbAuth {
    pub warning_printer: Box<dyn AuthWarningPrinter>,
}

impl MariaDbAuth {
    pub fn new(warning_printer: Box<dyn AuthWarningPrinter>) -> Self {
        Self { warning_printer }
    }
}

impl Auth for MariaDbAuth {
    fn backend(&self) -> Backend {
        Backend::MySQL
    }

    fn configure(&self, config: &AdapterConfig) -> Result<database::Builder, AuthError> {
        auth_configure_pipeline!(self, &config, parse_auth, apply_connection_args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_options::{other_option_value, uri_value};
    use dbt_test_primitives::assert_contains;
    use dbt_yaml::Mapping;

    fn configure(config: Mapping) -> Result<database::Builder, AuthError> {
        MariaDbAuth::new(Box::new(crate::NoopAuthWarningPrinter))
            .configure(&AdapterConfig::new(config))
    }

    fn base_config() -> Mapping {
        Mapping::from_iter([
            ("user".into(), "dbt".into()),
            ("password".into(), "secret".into()),
            ("schema".into(), "analytics".into()),
        ])
    }

    #[test]
    fn test_defaults_use_preferred_tls() {
        let builder = configure(base_config()).expect("configure");

        let uri = uri_value(&builder);
        assert_eq!(uri, "mysql://localhost:3306/?tls=preferred");
        assert_eq!(
            other_option_value(&builder, mysql::VENDOR),
            Some(mysql::VENDOR_MARIADB)
        );
    }

    #[test]
    fn test_custom_host_port_and_timeout() {
        let mut config = base_config();
        config.insert("host".into(), "db.internal".into());
        config.insert("port".into(), 3307.into());
        config.insert("connect_timeout".into(), 10.into());

        let builder = configure(config).expect("configure");

        let uri = uri_value(&builder);
        assert_contains!(&uri, "mysql://db.internal:3307/?");
        assert_contains!(&uri, "&timeout=10s");
    }

    #[test]
    fn test_duration_timeout_passes_through() {
        let mut config = base_config();
        config.insert("connect_timeout".into(), "1s".into());

        let builder = configure(config).expect("configure");

        assert_contains!(&uri_value(&builder), "&timeout=1s");
    }

    #[test]
    fn test_ssl_modes() {
        for (ssl_mode, tls) in [
            ("preferred", "tls=preferred"),
            ("required", "tls=skip-verify"),
            ("verify_ca", "tls=true"),
            ("disabled", "tls=false"),
        ] {
            let mut config = base_config();
            config.insert("ssl_mode".into(), ssl_mode.into());

            let builder = configure(config).expect("configure");

            assert_contains!(&uri_value(&builder), tls);
        }
    }

    #[test]
    fn test_invalid_ssl_mode() {
        let mut config = base_config();
        config.insert("ssl_mode".into(), "bogus".into());

        let err = configure(config).expect_err("invalid ssl_mode");
        assert_contains!(err.msg(), "Invalid MariaDB 'ssl_mode' 'bogus'");
    }

    #[test]
    fn test_missing_password() {
        let mut config = base_config();
        config.remove("password");

        let err = configure(config).expect_err("missing password");
        assert_contains!(err.msg(), "MariaDB requires 'password'");
    }
}
