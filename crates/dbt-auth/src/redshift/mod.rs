mod token_service;

use crate::{AdapterConfig, Auth, AuthError, AuthOutcome};
use std::borrow::Cow;
use tokio::runtime::Runtime;
use tokio::task;

use crate::redshift::token_service::{TokenEndpoint, create_token_service_client};
use adbc_core::constants::ADBC_OPTION_USERNAME;
use dbt_adbc::redshift::{
    AUTH_IDC_CLIENT_DISPLAY_NAME, AUTH_IDC_REGION, AUTH_IDP_LISTEN_PORT, AUTH_IDP_RESPONSE_TIMEOUT,
    AUTH_ISSUER_URL, AUTH_PROVIDER, AUTH_PROVIDER_BROWSER_IDC, AUTH_PROVIDER_IDP_TOKEN, AUTH_TOKEN,
    AUTH_TOKEN_TYPE,
};
use dbt_adbc::{
    Backend, database,
    redshift::{
        AWS_ACCESS_KEY_ID, AWS_PROFILE, AWS_REGION, AWS_SECRET_ACCESS_KEY, CLUSTER_IDENTIFIER,
        CLUSTER_TYPE,
        cluster_type::{REDSHIFT, SERVERLESS},
    },
};
use percent_encoding::utf8_percent_encode;

pub struct RedshiftAuth;

impl Auth for RedshiftAuth {
    fn backend(&self) -> Backend {
        Backend::Redshift
    }

    fn configure(&self, config: &AdapterConfig) -> Result<AuthOutcome, AuthError> {
        // Reference: https://docs.aws.amazon.com/redshift/latest/dg/r_names.html
        const SET: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
            .remove(b'.')
            .remove(b'-')
            .remove(b'_')
            .add(b' ');

        let mut builder = database::Builder::new(self.backend());

        // todo: update with Redshift specific configs once available
        let method = config
            .get("method")
            .and_then(|v| v.as_str())
            .unwrap_or("database");

        // Shared required configs and encoding
        let host = config.require_string("host")?;
        let port = config.require_string("port")?;
        let dbname = config
            .get_string("database")
            .or_else(|| config.get_string("dbname"))
            .ok_or_else(|| AuthError::config("missing required field 'database' (or 'dbname')"))?;
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
                builder.with_named_option(AWS_REGION, region)?;
                builder.with_named_option(ADBC_OPTION_USERNAME, user)?;

                // Either both access_key_id + secret_access_key, or fall back to iam_profile.
                // Mirrors dbt-redshift's __iam_user_kwargs in connections.py.
                // https://github.com/dbt-labs/dbt-adapters/blob/b43adf91c22bc91e7935014a687e37aa22115689/dbt-redshift/src/dbt/adapters/redshift/connections.py#L347-L348
                let access_key_id = config.get_string("access_key_id");
                let secret_access_key = config.get_string("secret_access_key");
                match (access_key_id, secret_access_key) {
                    (Some(access_key_id), Some(secret_access_key)) => {
                        builder.with_named_option(AWS_ACCESS_KEY_ID, access_key_id)?;
                        builder.with_named_option(AWS_SECRET_ACCESS_KEY, secret_access_key)?;
                    }
                    (Some(_), None) | (None, Some(_)) => {
                        return Err(AuthError::config(
                            "'access_key_id' and 'secret_access_key' are both needed if providing explicit credentials",
                        ));
                    }
                    (None, None) => {
                        let iam_profile = config.require_string("iam_profile")?;
                        builder.with_named_option(AWS_PROFILE, iam_profile)?;
                    }
                }

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
                let token_endpoint: TokenEndpoint = dbt_yaml::from_value::<TokenEndpoint>(
                    token_endpoint_value.clone(),
                )
                .map_err(|e| AuthError::config(format!("Invalid token_endpoint structure: {e}")))?;

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
        }

        Ok(AuthOutcome {
            builder,
            warnings: vec![],
        })
    }
}

#[cfg(test)]
mod tests_adbc {
    use super::*;
    use crate::test_options::{other_option_value, uri_value};
    use dbt_adbc::redshift::cluster_type::{REDSHIFT, SERVERLESS};
    use dbt_yaml::Mapping;

    fn iam_base_config() -> Mapping {
        Mapping::from_iter([
            ("method".into(), "iam".into()),
            ("host".into(), "dbt-sandbox-host".into()),
            ("port".into(), "5439".into()),
            ("database".into(), "ci".into()),
            ("user".into(), "awsuser".into()),
            ("region".into(), "us-east-2".into()),
            ("cluster_id".into(), "dbt-sandbox".into()),
        ])
    }

    fn configure(config: Mapping) -> Result<database::Builder, AuthError> {
        RedshiftAuth
            .configure(&AdapterConfig::new(config))
            .map(|outcome| outcome.builder)
    }

    fn assert_error_contains(config: Mapping, needle: &str) {
        match configure(config) {
            Err(e) => {
                let msg = format!("{e:?}");
                assert!(
                    msg.contains(needle),
                    "expected error message to contain {needle:?}, got {msg:?}"
                );
            }
            Ok(_) => panic!("Expected error containing {needle:?}, got Ok(_)"),
        }
    }

    #[test]
    fn test_iam_with_profile() {
        let mut config = iam_base_config();
        config.insert("iam_profile".into(), "sandbox-admin".into());

        let builder = configure(config).expect("configure");

        assert_eq!(
            other_option_value(&builder, AWS_PROFILE),
            Some("sandbox-admin")
        );
        assert_eq!(other_option_value(&builder, AWS_REGION), Some("us-east-2"));
        assert_eq!(other_option_value(&builder, CLUSTER_TYPE), Some(REDSHIFT));
        assert_eq!(
            other_option_value(&builder, CLUSTER_IDENTIFIER),
            Some("dbt-sandbox")
        );
        assert_eq!(other_option_value(&builder, AWS_ACCESS_KEY_ID), None);
        assert_eq!(other_option_value(&builder, AWS_SECRET_ACCESS_KEY), None);
        let uri = uri_value(&builder);
        assert!(
            uri.starts_with("postgresql://dbt-sandbox-host:5439/ci"),
            "unexpected URI: {uri}"
        );
    }

    #[test]
    fn test_iam_with_access_keys() {
        let mut config = iam_base_config();
        config.insert("access_key_id".into(), "AKIAEXAMPLE".into());
        config.insert("secret_access_key".into(), "sekret".into());

        let builder = configure(config).expect("configure");

        assert_eq!(
            other_option_value(&builder, AWS_ACCESS_KEY_ID),
            Some("AKIAEXAMPLE")
        );
        assert_eq!(
            other_option_value(&builder, AWS_SECRET_ACCESS_KEY),
            Some("sekret")
        );
        assert_eq!(other_option_value(&builder, AWS_PROFILE), None);
    }

    #[test]
    fn test_iam_access_keys_take_precedence_over_profile() {
        let mut config = iam_base_config();
        config.insert("iam_profile".into(), "sandbox-admin".into());
        config.insert("access_key_id".into(), "AKIAEXAMPLE".into());
        config.insert("secret_access_key".into(), "sekret".into());

        let builder = configure(config).expect("configure");

        assert_eq!(
            other_option_value(&builder, AWS_ACCESS_KEY_ID),
            Some("AKIAEXAMPLE")
        );
        assert_eq!(
            other_option_value(&builder, AWS_SECRET_ACCESS_KEY),
            Some("sekret")
        );
        assert_eq!(other_option_value(&builder, AWS_PROFILE), None);
    }

    #[test]
    fn test_iam_missing_secret_access_key_errors() {
        let mut config = iam_base_config();
        config.insert("access_key_id".into(), "AKIAEXAMPLE".into());
        assert_error_contains(
            config,
            "'access_key_id' and 'secret_access_key' are both needed",
        );
    }

    #[test]
    fn test_iam_missing_access_key_id_errors() {
        let mut config = iam_base_config();
        config.insert("secret_access_key".into(), "sekret".into());
        assert_error_contains(
            config,
            "'access_key_id' and 'secret_access_key' are both needed",
        );
    }

    #[test]
    fn test_iam_without_credentials_requires_iam_profile() {
        // No access keys, no iam_profile -> error citing iam_profile.
        let config = iam_base_config();
        assert_error_contains(config, "iam_profile");
    }

    #[test]
    fn test_iam_serverless_skips_cluster_id() {
        let mut config = iam_base_config();
        // The "redshift-serverless" substring is what triggers serverless detection
        // in `configure()`.
        config.insert("host".into(), "example.redshift-serverless.host".into());
        config.remove("cluster_id");
        config.insert("iam_profile".into(), "sandbox-admin".into());

        let builder = configure(config).expect("configure");

        assert_eq!(other_option_value(&builder, CLUSTER_TYPE), Some(SERVERLESS));
        assert_eq!(other_option_value(&builder, CLUSTER_IDENTIFIER), None);
    }

    #[test]
    fn test_iam_provisioned_requires_cluster_id() {
        let mut config = iam_base_config();
        config.remove("cluster_id");
        config.insert("iam_profile".into(), "sandbox-admin".into());
        assert_error_contains(config, "cluster_id");
    }

    #[test]
    fn test_iam_requires_region() {
        let mut config = iam_base_config();
        config.remove("region");
        config.insert("iam_profile".into(), "sandbox-admin".into());
        assert_error_contains(config, "region");
    }

    #[test]
    fn test_iam_requires_user() {
        let mut config = iam_base_config();
        config.remove("user");
        config.insert("iam_profile".into(), "sandbox-admin".into());
        assert_error_contains(config, "user");
    }

    #[test]
    fn test_database_accepts_dbname_alias() {
        let config = Mapping::from_iter([
            ("method".into(), "database".into()),
            ("host".into(), "cluster-host".into()),
            ("port".into(), "5439".into()),
            ("dbname".into(), "dev".into()),
            ("user".into(), "admin".into()),
            ("password".into(), "secretpass".into()),
        ]);
        assert!(
            configure(config).is_ok(),
            "auth should succeed when profile uses 'dbname' instead of 'database'"
        );
    }

    #[test]
    fn test_database_method_rejects_iam_profile() {
        let config = Mapping::from_iter([
            ("method".into(), "database".into()),
            ("host".into(), "cluster-host".into()),
            ("port".into(), "5439".into()),
            ("database".into(), "dev".into()),
            ("user".into(), "admin".into()),
            ("password".into(), "secretpass".into()),
            ("iam_profile".into(), "default".into()),
        ]);
        assert_error_contains(config, "iam_profile");
    }
}
