mod token_service;

use crate::{AdapterConfig, Auth, AuthError};
use std::borrow::Cow;
use tokio::runtime::Runtime;
use tokio::task;

use crate::redshift::token_service::{TokenEndpoint, create_token_service_client};
use adbc_core::ffi::constants::ADBC_OPTION_USERNAME;
use dbt_xdbc::redshift::{
    AUTH_IDC_CLIENT_DISPLAY_NAME, AUTH_IDC_REGION, AUTH_IDP_LISTEN_PORT, AUTH_IDP_RESPONSE_TIMEOUT,
    AUTH_ISSUER_URL, AUTH_PROVIDER, AUTH_PROVIDER_BROWSER_IDC, AUTH_PROVIDER_IDP_TOKEN, AUTH_TOKEN,
    AUTH_TOKEN_TYPE,
};
use dbt_xdbc::{
    Backend, database,
    redshift::{
        self, AWS_PROFILE, AWS_REGION, CLUSTER_IDENTIFIER, CLUSTER_TYPE,
        cluster_type::{REDSHIFT, SERVERLESS},
    },
};
use percent_encoding::utf8_percent_encode;

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
        // Reference: https://docs.aws.amazon.com/redshift/latest/dg/r_names.html
        const SET: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
            .remove(b'.')
            .remove(b'-')
            .remove(b'_')
            .add(b' ');

        let mut builder = database::Builder::new(self.backend());

        if self.backend() == Backend::RedshiftODBC {
            use redshift::odbc::*;
            for key in ["host", "port", "database", "user", "password"].iter() {
                if let Some(value) = config.get_string(key) {
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
            let method = config
                .get("method")
                .and_then(|v| v.as_str())
                .unwrap_or("database");

            // Shared required configs and encoding
            let host = config.require_string("host")?;
            let port = config.require_string("port")?;
            let dbname = config.require_string("database")?;
            let host = utf8_percent_encode(&host, SET).to_string();
            let port = utf8_percent_encode(&port, SET).to_string();
            let dbname = utf8_percent_encode(&dbname, SET).to_string();

            match method {
                "database" => {
                    let user = config.require_string("user")?;
                    for key in ["iam_profile", "cluster_id"].iter() {
                        if config.contains_key(key) {
                            return Err(AuthError::config(format!(
                                "Cannot set '{key}' when 'method' is set to 'database'"
                            )));
                        };
                    }

                    builder.with_named_option(CLUSTER_TYPE, REDSHIFT)?;

                    let password = config.require_string("password")?;

                    let user = utf8_percent_encode(&user, SET).to_string();
                    let password = utf8_percent_encode(&password, SET).to_string();

                    let connection_str =
                        format!("postgresql://{user}:{password}@{host}:{port}/{dbname}");
                    builder.with_parse_uri(connection_str)?;
                }
                "iam" => {
                    let user = config.require_string("user")?;
                    // XXX: We can only tell serverless vs cluster from the host input
                    let is_serverless = host.contains("redshift-serverless");

                    // cluster_id doesn't exist for serverless
                    if is_serverless {
                        builder.with_named_option(CLUSTER_TYPE, SERVERLESS)?;
                    } else {
                        builder.with_named_option(CLUSTER_TYPE, REDSHIFT)?;
                        let cluster_id = config.require_string("cluster_id")?;
                        builder.with_named_option(CLUSTER_IDENTIFIER, cluster_id)?;
                    }

                    let region = config.require_string("region")?;
                    let iam_profile = config.require_string("iam_profile")?;

                    builder.with_named_option(AWS_REGION, region)?;
                    builder.with_named_option(AWS_PROFILE, iam_profile)?;
                    builder.with_named_option(ADBC_OPTION_USERNAME, user)?;

                    let connection_str = format!("postgresql://{host}:{port}/{dbname}");
                    builder.with_parse_uri(connection_str)?;
                }
                "browser_identity_center" => {
                    builder.with_named_option(AUTH_PROVIDER, AUTH_PROVIDER_BROWSER_IDC)?;

                    let idc_region = config.require_string("idc_region")?;
                    let idc_issuer_url = config.require_string("issuer_url")?;

                    builder.with_named_option(AUTH_IDC_REGION, idc_region)?;
                    builder.with_named_option(AUTH_ISSUER_URL, idc_issuer_url)?;

                    builder.with_named_option(
                        AUTH_IDP_LISTEN_PORT,
                        config
                            .get_string("idp_listen_port")
                            .unwrap_or(Cow::Borrowed("7890")),
                    )?;

                    builder.with_named_option(
                        AUTH_IDC_CLIENT_DISPLAY_NAME,
                        config
                            .get_string("idc_client_display_name")
                            .unwrap_or(Cow::Borrowed("Amazon Redshift driver")),
                    )?;

                    builder.with_named_option(
                        AUTH_IDP_RESPONSE_TIMEOUT,
                        config
                            .get_string("idp_response_timeout")
                            .unwrap_or(Cow::Borrowed("60")),
                    )?;

                    let connection_str = format!("postgresql://{host}:{port}/{dbname}");
                    builder.with_parse_uri(connection_str)?;
                }
                "oauth_token_identity_center" => {
                    builder.with_named_option(AUTH_PROVIDER, AUTH_PROVIDER_IDP_TOKEN)?;

                    let token_endpoint_value = config.require("token_endpoint")?;
                    let token_endpoint: TokenEndpoint =
                        dbt_serde_yaml::from_value::<TokenEndpoint>(token_endpoint_value.clone())
                            .map_err(|e| {
                            AuthError::config(format!("Invalid token_endpoint structure: {e}"))
                        })?;

                    let access_token = task::block_in_place(|| {
                        let rt = Runtime::new().map_err(|e| {
                            AuthError::config(format!("Failed to create Tokio runtime: {e}"))
                        })?;

                        let client = create_token_service_client(token_endpoint).map_err(|e| {
                            AuthError::config(format!("Failed to create token service: {e}"))
                        })?;

                        rt.block_on(async {
                            client.handle_request().await.map_err(|_e| {
                                AuthError::config(
                                    "access_token missing from IdP token request. \
         Please confirm correct configuration of the token_endpoint \
         field in profiles.yml and that your IdP can use a refresh token \
         to obtain an OIDC-compliant access token.",
                                )
                            })
                        })
                    })?;

                    // Apply the token to Redshift builder
                    builder.with_named_option(AUTH_PROVIDER, AUTH_PROVIDER_IDP_TOKEN)?;
                    builder.with_named_option(AUTH_TOKEN, access_token)?;
                    builder.with_named_option(AUTH_TOKEN_TYPE, "EXT_JWT")?;

                    let connection_str = format!("postgresql://{host}:{port}/{dbname}");
                    builder.with_parse_uri(connection_str)?;
                }
                method => {
                    return Err(AuthError::config(format!(
                        "Unsupported auth method '{method}' for Redshift. Try 'database' or 'iam' instead."
                    )));
                }
            };
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
    use dbt_serde_yaml::Mapping;

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

        let config = Mapping::from_iter([
            ("server".into(), "redshift-cluster.aws.com".into()),
            ("port".into(), "5439".into()),
            ("database".into(), "dev".into()),
            ("user".into(), "admin".into()),
            ("password".into(), "secretpass".into()),
        ]);

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
