use crate::{AdapterConfig, Auth, AuthError, AuthOutcome, auth_configure_pipeline};
use database::Builder as DatabaseBuilder;

use dbt_adbc::{Backend, database, lake_compute};

#[derive(Debug)]
enum LakeComputeAuthIR<'a> {
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

impl<'a> LakeComputeAuthIR<'a> {
    fn apply(self, mut builder: DatabaseBuilder) -> Result<DatabaseBuilder, AuthError> {
        match self {
            Self::ApiKey { api_key } => {
                builder
                    .with_named_option(lake_compute::AUTH_TYPE, lake_compute::auth_type::API_KEY)?;
                builder.with_named_option(lake_compute::AUTH_API_KEY, api_key)?;
            }
            Self::Token { token } => {
                builder
                    .with_named_option(lake_compute::AUTH_TYPE, lake_compute::auth_type::TOKEN)?;
                builder.with_named_option(lake_compute::AUTH_TOKEN, token)?;
            }
            Self::OktaBrowser {
                auth_url,
                token_url,
                client_id,
            } => {
                builder.with_named_option(
                    lake_compute::AUTH_TYPE,
                    lake_compute::auth_type::OKTA_BROWSER,
                )?;
                if let Some(v) = auth_url {
                    builder.with_named_option(lake_compute::OKTA_AUTH_URL, v)?;
                }
                if let Some(v) = token_url {
                    builder.with_named_option(lake_compute::OKTA_TOKEN_URL, v)?;
                }
                if let Some(v) = client_id {
                    builder.with_named_option(lake_compute::OKTA_CLIENT_ID, v)?;
                }
            }
        }
        Ok(builder)
    }
}

fn parse_auth<'a>(config: &'a AdapterConfig) -> Result<LakeComputeAuthIR<'a>, AuthError> {
    let method = config.require_str("method")?;
    match method {
        lake_compute::auth_type::API_KEY => Ok(LakeComputeAuthIR::ApiKey {
            api_key: config.require_str("api_key")?,
        }),
        lake_compute::auth_type::TOKEN => Ok(LakeComputeAuthIR::Token {
            token: config.require_str("token")?,
        }),
        lake_compute::auth_type::OKTA_BROWSER => Ok(LakeComputeAuthIR::OktaBrowser {
            auth_url: config.get_str("okta_auth_url"),
            token_url: config.get_str("okta_token_url"),
            client_id: config.get_str("okta_client_id"),
        }),
        other => Err(AuthError::config(format!(
            "unknown ALT auth method '{other}'; expected one of: '{}', '{}', '{}'",
            lake_compute::auth_type::API_KEY,
            lake_compute::auth_type::TOKEN,
            lake_compute::auth_type::OKTA_BROWSER
        ))),
    }
}

fn apply_connection_args(
    config: &AdapterConfig,
    mut builder: DatabaseBuilder,
) -> Result<DatabaseBuilder, AuthError> {
    builder.with_named_option(lake_compute::BASE_URL, config.require_str("base_url")?)?;

    if let Some(bundle) = config.get_str("catalog_bundle") {
        builder.with_named_option(lake_compute::CATALOG_BUNDLE, bundle)?;
    }

    Ok(builder)
}

pub struct LakeComputeAuth;

impl Auth for LakeComputeAuth {
    fn backend(&self) -> Backend {
        Backend::LakeCompute
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
        LakeComputeAuth {}
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
            other_option_value(&builder, lake_compute::BASE_URL),
            Some("https://compute.example")
        );
        assert_eq!(
            other_option_value(&builder, lake_compute::AUTH_TYPE),
            Some("api_key")
        );
        assert_eq!(
            other_option_value(&builder, lake_compute::AUTH_API_KEY),
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
            other_option_value(&builder, lake_compute::AUTH_TYPE),
            Some("okta_browser")
        );
        assert_eq!(
            other_option_value(&builder, lake_compute::OKTA_CLIENT_ID),
            Some("client-123")
        );
    }

    #[test]
    fn missing_base_url_errors() {
        let err = LakeComputeAuth {}
            .configure(&AdapterConfig::new(Mapping::new()))
            .expect_err("expected missing base_url error");
        assert!(matches!(err, AuthError::YAML(_)), "got {err:?}");
    }
}
