//! Authentication for the FDCS (dbt Compute) backend.
//!
//! FDCS is reached through the `adbc_driver_dbt` ADBC driver, which is configured
//! entirely via named database options (see [`dbt_adbc::fdcs`]). This module
//! translates a profile mapping into those options.
//!
//! Following the crate invariants, profile values are kept borrowed until the
//! final `with_named_option` boundary (which requires `Into<String>`), and the
//! authentication families are modeled as distinct top-level IR variants.

use crate::{AdapterConfig, Auth, AuthError, AuthOutcome, auth_configure_pipeline};
use database::Builder as DatabaseBuilder;

use dbt_adbc::{Backend, database, fdcs};

/// Distinct authentication contracts supported by FDCS.
#[derive(Debug)]
enum FdcsAuthIR<'a> {
    /// Send an `X-API-Key` header.
    ApiKey { api_key: &'a str },
    /// Send a static bearer token.
    Token { token: &'a str },
    /// Interactive Okta PKCE browser login. Endpoint configuration is optional
    /// here because the driver also reads it from the environment.
    OktaBrowser {
        auth_url: Option<&'a str>,
        token_url: Option<&'a str>,
        client_id: Option<&'a str>,
    },
    /// No explicit credentials in the profile; defer to the driver/SDK's own
    /// auto-discovery (environment variables / CLI token file).
    Auto,
}

impl<'a> FdcsAuthIR<'a> {
    fn apply(self, mut builder: DatabaseBuilder) -> Result<DatabaseBuilder, AuthError> {
        match self {
            Self::ApiKey { api_key } => {
                builder.with_named_option(fdcs::AUTH_TYPE, fdcs::auth_type::API_KEY)?;
                builder.with_named_option(fdcs::AUTH_API_KEY, api_key)?;
            }
            Self::Token { token } => {
                builder.with_named_option(fdcs::AUTH_TYPE, fdcs::auth_type::TOKEN)?;
                builder.with_named_option(fdcs::AUTH_TOKEN, token)?;
            }
            Self::OktaBrowser {
                auth_url,
                token_url,
                client_id,
            } => {
                builder.with_named_option(fdcs::AUTH_TYPE, fdcs::auth_type::OKTA_BROWSER)?;
                if let Some(v) = auth_url {
                    builder.with_named_option(fdcs::OKTA_AUTH_URL, v)?;
                }
                if let Some(v) = token_url {
                    builder.with_named_option(fdcs::OKTA_TOKEN_URL, v)?;
                }
                if let Some(v) = client_id {
                    builder.with_named_option(fdcs::OKTA_CLIENT_ID, v)?;
                }
            }
            Self::Auto => {}
        }
        Ok(builder)
    }
}

fn parse_auth<'a>(config: &'a AdapterConfig) -> Result<FdcsAuthIR<'a>, AuthError> {
    // The `method` field selects the authentication family. Absent `method`,
    // infer from which credential field is present, else defer to auto-discovery.
    let method = config.get_str("method");
    match method {
        Some(fdcs::auth_type::API_KEY) => Ok(FdcsAuthIR::ApiKey {
            api_key: config.require_str("api_key")?,
        }),
        Some(fdcs::auth_type::TOKEN) => Ok(FdcsAuthIR::Token {
            token: config.require_str("token")?,
        }),
        Some(fdcs::auth_type::OKTA_BROWSER) => Ok(FdcsAuthIR::OktaBrowser {
            auth_url: config.get_str("okta_auth_url"),
            token_url: config.get_str("okta_token_url"),
            client_id: config.get_str("okta_client_id"),
        }),
        Some(other) => Err(AuthError::config(format!(
            "unknown FDCS auth method: {other}"
        ))),
        None => {
            if let Some(api_key) = config.get_str("api_key") {
                Ok(FdcsAuthIR::ApiKey { api_key })
            } else if let Some(token) = config.get_str("token") {
                Ok(FdcsAuthIR::Token { token })
            } else {
                Ok(FdcsAuthIR::Auto)
            }
        }
    }
}

fn apply_connection_args(
    config: &AdapterConfig,
    mut builder: DatabaseBuilder,
) -> Result<DatabaseBuilder, AuthError> {
    // base_url is required to reach the service.
    builder.with_named_option(fdcs::BASE_URL, config.require_str("base_url")?)?;

    if let Some(org) = config.get_str("organization") {
        builder.with_named_option(fdcs::ORGANIZATION, org)?;
    }
    if let Some(warehouse) = config.get_str("warehouse") {
        builder.with_named_option(fdcs::WAREHOUSE, warehouse)?;
    }
    if let Some(affinity) = config.get_str("affinity") {
        builder.with_named_option(fdcs::AFFINITY, affinity)?;
    }
    if let Some(dialect) = config.get_str("dialect") {
        builder.with_named_option(fdcs::DIALECT, dialect)?;
    }
    // `timeout_seconds` is commonly a number in profiles; accept numeric or string.
    if let Some(timeout) = config.get_string("timeout_seconds") {
        builder.with_named_option(fdcs::TIMEOUT_SECONDS, timeout.into_owned())?;
    }

    Ok(builder)
}

pub struct FdcsAuth;

impl Auth for FdcsAuth {
    fn backend(&self) -> Backend {
        Backend::Fdcs
    }

    fn configure(&self, config: &AdapterConfig) -> Result<AuthOutcome, AuthError> {
        auth_configure_pipeline!(self.backend(), &config, parse_auth, apply_connection_args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_options::other_option_value;
    use dbt_yaml::Mapping;

    fn configure(config: Mapping) -> DatabaseBuilder {
        FdcsAuth {}
            .configure(&AdapterConfig::new(config))
            .expect("configure")
            .builder
    }

    #[test]
    fn base_url_and_api_key() {
        let builder = configure(Mapping::from_iter([
            ("base_url".into(), "https://compute.example".into()),
            ("method".into(), "api_key".into()),
            ("api_key".into(), "secret-key".into()),
        ]));
        assert_eq!(
            other_option_value(&builder, fdcs::BASE_URL),
            Some("https://compute.example")
        );
        assert_eq!(
            other_option_value(&builder, fdcs::AUTH_TYPE),
            Some("api_key")
        );
        assert_eq!(
            other_option_value(&builder, fdcs::AUTH_API_KEY),
            Some("secret-key")
        );
    }

    #[test]
    fn okta_browser_method() {
        let builder = configure(Mapping::from_iter([
            ("base_url".into(), "https://compute.example".into()),
            ("method".into(), "okta_browser".into()),
            ("okta_client_id".into(), "client-123".into()),
        ]));
        assert_eq!(
            other_option_value(&builder, fdcs::AUTH_TYPE),
            Some("okta_browser")
        );
        assert_eq!(
            other_option_value(&builder, fdcs::OKTA_CLIENT_ID),
            Some("client-123")
        );
    }

    #[test]
    fn timeout_accepts_integer() {
        let config: Mapping = dbt_yaml::from_str(
            r#"
base_url: https://compute.example
timeout_seconds: 120
"#,
        )
        .expect("parse yaml");
        let builder = configure(config);
        assert_eq!(
            other_option_value(&builder, fdcs::TIMEOUT_SECONDS),
            Some("120")
        );
    }

    #[test]
    fn missing_base_url_errors() {
        // A missing required field surfaces as a YAML deserialization error.
        let err = FdcsAuth {}
            .configure(&AdapterConfig::new(Mapping::new()))
            .expect_err("expected missing base_url error");
        assert!(matches!(err, AuthError::YAML(_)), "got {err:?}");
    }
}
