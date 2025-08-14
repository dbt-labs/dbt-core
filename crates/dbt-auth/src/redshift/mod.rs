use crate::{AdapterConfig, Auth, AuthError};

use dbt_xdbc::{Backend, database, redshift};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};

#[derive(Debug, Default)]
pub struct RedshiftAuth;

impl Auth for RedshiftAuth {
    fn backend(&self) -> Backend {
        #[cfg(feature = "odbc")]
        {
            Backend::RedshiftODBC
        }
        #[cfg(not(feature = "odbc"))]
        {
            Backend::Redshift
        }
    }

    fn configure(&self, config: &AdapterConfig) -> Result<database::Builder, AuthError> {
        let mut builder = database::Builder::new(self.backend());

        if self.backend() == Backend::RedshiftODBC {
            use redshift::odbc::*;
            for key in ["host", "port", "database", "user", "password"].iter() {
                if let Some(value) = config.maybe_get_str(key)? {
                    match *key {
                        "host" => builder.with_named_option(SERVER, value),
                        "port" => builder.with_named_option(PORT_NUMBER, value),
                        "database" => builder.with_named_option(DATABASE, value),
                        "user" => builder.with_named_option(UID, value),
                        "password" => builder.with_named_option(PASSWORD, value),
                        _ => panic!("unexpected key: {key}"),
                    }?;
                }
            }

            builder.with_named_option(DRIVER, odbc_driver_path())?;
        } else {
            // todo: update with Redshift specific configs once available
            let method = config.get_str("method").unwrap_or("database".to_string());

            let connection_str = match method.as_ref() {
                "database" => {
                    for key in ["iam_profile", "cluster_id"].iter() {
                        if config.get_str(key).is_ok() {
                            return Err(AuthError::config(format!(
                                "Cannot set '{key}' when 'method' is set to 'database'"
                            )));
                        };
                    }

                    let user = config.get_str("user")?;
                    let password = config.get_str("password")?;
                    let host = config.get_str("host")?;
                    let port = config.get_str("port")?;
                    let dbname = config.get_str("database")?;

                    let user = utf8_percent_encode(&user, NON_ALPHANUMERIC).to_string();
                    let password = utf8_percent_encode(&password, NON_ALPHANUMERIC).to_string();
                    let host = utf8_percent_encode(&host, NON_ALPHANUMERIC).to_string();
                    let port = utf8_percent_encode(&port, NON_ALPHANUMERIC).to_string();
                    let dbname = utf8_percent_encode(&dbname, NON_ALPHANUMERIC).to_string();

                    format!("postgresql://{user}:{password}@{host}:{port}/{dbname}")
                }
                "iam" => {
                    return Err(AuthError::config(
                        "IAM auth for Redshift is not supported yet. Please use username/password auth instead.",
                    ));
                }
                method => {
                    return Err(AuthError::config(format!(
                        "Unsupported auth method '{method}' for Redshift. Try 'database' instead ('iam' will be supported in later releases)."
                    )));
                }
            };

            builder.with_parse_uri(connection_str)?;
        }

        Ok(builder)
    }
}

// todo: add auth tests for Redshift ADBC driver
#[cfg(feature = "odbc")]
#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::options::{OptionDatabase, OptionValue};
    use std::collections::HashMap;
    type YmlValue = dbt_serde_yaml::Value;

    fn str_value(value: &OptionValue) -> &str {
        match value {
            OptionValue::String(s) => s.as_str(),
            _ => panic!("unexpected value"),
        }
    }

    #[test]
    fn test_basic_user_password_auth() {
        use redshift::odbc::*;
        let auth = RedshiftAuth {};

        let mut config = HashMap::new();
        config.insert(
            "server".to_string(),
            YmlValue::from("redshift-cluster.aws.com"),
        );
        config.insert("port".to_string(), YmlValue::from("5439"));
        config.insert("database".to_string(), YmlValue::from("dev"));
        config.insert("user".to_string(), YmlValue::from("admin"));
        config.insert("password".to_string(), YmlValue::from("secretpass"));

        let builder = auth
            .configure(&AdapterConfig::new(config))
            .expect("configure");

        let mut unknown_options = 0;
        builder.into_iter().for_each(|(k, v)| match k {
            OptionDatabase::Other(ref name) => match name.as_str() {
                UID => assert_eq!(str_value(&v), "admin"),
                PASSWORD => assert_eq!(str_value(&v), "secretpass"),
                DRIVER => assert_eq!(str_value(&v), odbc_driver_path()),
                SERVER => assert_eq!(str_value(&v), "redshift-cluster.aws.com"),
                PORT_NUMBER => assert_eq!(str_value(&v), "5439"),
                DATABASE => assert_eq!(str_value(&v), "dev"),
                _ => unknown_options += 1,
            },
            _ => unknown_options += 1,
        });
        assert_eq!(unknown_options, 0);
    }
}
