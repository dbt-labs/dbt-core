use crate::{AdapterConfig, Auth, AuthError, AuthOutcome, auth_configure_pipeline};
use database::Builder as DatabaseBuilder;

use dbt_adbc::{Backend, alt, database};

#[derive(Debug)]
enum AltAuthIR<'a> {
    Token {
        token: &'a str,
    },
    // not yet verified
    ApiKey {
        api_key: &'a str,
    },
    OktaBrowser {
        auth_url: Option<&'a str>,
        token_url: Option<&'a str>,
        client_id: Option<&'a str>,
    },
}

impl<'a> AltAuthIR<'a> {
    fn apply(self, mut builder: DatabaseBuilder) -> Result<DatabaseBuilder, AuthError> {
        match self {
            Self::ApiKey { api_key } => {
                builder.with_named_option(alt::AUTH_TYPE, alt::auth_type::API_KEY)?;
                builder.with_named_option(alt::AUTH_API_KEY, api_key)?;
            }
            Self::Token { token } => {
                builder.with_named_option(alt::AUTH_TYPE, alt::auth_type::TOKEN)?;
                builder.with_named_option(alt::AUTH_TOKEN, token)?;
            }
            Self::OktaBrowser {
                auth_url,
                token_url,
                client_id,
            } => {
                builder.with_named_option(alt::AUTH_TYPE, alt::auth_type::OKTA_BROWSER)?;
                if let Some(v) = auth_url {
                    builder.with_named_option(alt::OKTA_AUTH_URL, v)?;
                }
                if let Some(v) = token_url {
                    builder.with_named_option(alt::OKTA_TOKEN_URL, v)?;
                }
                if let Some(v) = client_id {
                    builder.with_named_option(alt::OKTA_CLIENT_ID, v)?;
                }
            }
        }
        Ok(builder)
    }
}

fn parse_auth<'a>(config: &'a AdapterConfig) -> Result<AltAuthIR<'a>, AuthError> {
    let method = config.require_str("method")?;
    match method {
        alt::auth_type::API_KEY => Ok(AltAuthIR::ApiKey {
            api_key: config.require_str("api_key")?,
        }),
        alt::auth_type::TOKEN => Ok(AltAuthIR::Token {
            token: config.require_str("token")?,
        }),
        alt::auth_type::OKTA_BROWSER => Ok(AltAuthIR::OktaBrowser {
            auth_url: config.get_str("okta_auth_url"),
            token_url: config.get_str("okta_token_url"),
            client_id: config.get_str("okta_client_id"),
        }),
        other => Err(AuthError::config(format!(
            "unknown ALT auth method '{other}'; expected one of: '{}', '{}', '{}'",
            alt::auth_type::API_KEY,
            alt::auth_type::TOKEN,
            alt::auth_type::OKTA_BROWSER
        ))),
    }
}

fn apply_connection_args(
    config: &AdapterConfig,
    mut builder: DatabaseBuilder,
) -> Result<DatabaseBuilder, AuthError> {
    builder.with_named_option(alt::BASE_URL, config.require_str("base_url")?)?;

    if let Some(org) = config.get_str("organization") {
        builder.with_named_option(alt::ORGANIZATION, org)?;
    }

    Ok(builder)
}

pub struct AltAuth;

impl Auth for AltAuth {
    fn backend(&self) -> Backend {
        Backend::Alt
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
        AltAuth {}
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
            other_option_value(&builder, alt::BASE_URL),
            Some("https://compute.example")
        );
        assert_eq!(
            other_option_value(&builder, alt::AUTH_TYPE),
            Some("api_key")
        );
        assert_eq!(
            other_option_value(&builder, alt::AUTH_API_KEY),
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
            other_option_value(&builder, alt::AUTH_TYPE),
            Some("okta_browser")
        );
        assert_eq!(
            other_option_value(&builder, alt::OKTA_CLIENT_ID),
            Some("client-123")
        );
    }

    #[test]
    fn missing_base_url_errors() {
        let err = AltAuth {}
            .configure(&AdapterConfig::new(Mapping::new()))
            .expect_err("expected missing base_url error");
        assert!(matches!(err, AuthError::YAML(_)), "got {err:?}");
    }
}
