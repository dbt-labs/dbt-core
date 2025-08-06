use std::path::Path;

use crate::AdapterConfig;
use crate::Auth;
use crate::AuthError;
use dbt_xdbc::{Backend, bigquery, database};
use serde_json::Map;

pub mod types;
use types::{BigQueryAuthConfig, BigQueryAuthMethod, KeyFileJson, OAuthSecretsVariants};

/// Bigquery auth
#[derive(Debug, Default)]
pub struct BigqueryAuth;

impl TryFrom<&AdapterConfig> for BigQueryAuthConfig {
    type Error = AuthError;

    fn try_from(value: &AdapterConfig) -> Result<Self, Self::Error> {
        let json = serde_json::Value::Object(Map::from_iter(value.raw_config()));

        let config: BigQueryAuthConfig = serde_json::from_value(json).map_err(|e| {
            AuthError::config(format!(
                "Error parsing BigQuery auth config: {}, seeing top level config keys:\n{}",
                e,
                value
                    .keys()
                    .iter()
                    .map(|k| format!("- {k}"))
                    .collect::<Vec<String>>()
                    .join("\n")
            ))
        })?;

        Ok(config)
    }
}

impl TryFrom<BigQueryAuthConfig> for database::Builder {
    type Error = AuthError;

    fn try_from(value: BigQueryAuthConfig) -> Result<Self, Self::Error> {
        let mut builder = database::Builder::new(Backend::BigQuery);

        if let Some(execution_project) = value.execution_project {
            builder.with_named_option(bigquery::PROJECT_ID, &execution_project)?;
        } else {
            builder.with_named_option(bigquery::PROJECT_ID, &value.database)?;
        }

        if let Some(location) = value.location {
            builder.with_named_option(bigquery::LOCATION, &location)?;
        }

        builder.with_named_option(bigquery::DATASET_ID, &value.schema)?;

        match value.method_config {
            BigQueryAuthMethod::ServiceAccount { keyfile: path } => {
                let expanded_path = shellexpand::tilde(&path).to_string();
                if Path::new(&expanded_path).exists() {
                    builder.with_named_option(
                        bigquery::AUTH_TYPE,
                        bigquery::auth_type::JSON_CREDENTIAL_FILE,
                    )?;
                    builder
                        .with_named_option(bigquery::AUTH_CREDENTIALS, expanded_path.as_str())?;
                    Ok(builder)
                } else {
                    Err(AuthError::config(format!(
                        "Keyfile '{path}' does not exist"
                    )))
                }
            }
            BigQueryAuthMethod::ServiceAccountJson {
                keyfile_json: variant,
            } => {
                let mut keyfile_json: KeyFileJson = variant.try_into()?;
                keyfile_json.private_key = keyfile_json.private_key.replace("\\\\n", "\\n");

                let value = serde_json::to_value(keyfile_json)
                    .map_err(|e| AuthError::config(e.to_string()))?
                    .to_string();

                builder.with_named_option(
                    bigquery::AUTH_TYPE,
                    bigquery::auth_type::JSON_CREDENTIAL_STRING,
                )?;
                builder.with_named_option(bigquery::AUTH_CREDENTIALS, value)?;

                Ok(builder)
            }
            BigQueryAuthMethod::Oauth {} => {
                builder.with_named_option(bigquery::AUTH_TYPE, bigquery::auth_type::DEFAULT)?;
                Ok(builder)
            }
            BigQueryAuthMethod::OauthSecrets(variant) => match variant {
                OAuthSecretsVariants::RefreshToken(oauth_secrets_refresh_token) => {
                    builder.with_named_option(
                        bigquery::AUTH_TYPE,
                        bigquery::auth_type::USER_AUTHENTICATION,
                    )?;
                    builder.with_named_option(
                        bigquery::AUTH_CLIENT_ID,
                        oauth_secrets_refresh_token.client_id,
                    )?;
                    builder.with_named_option(
                        bigquery::AUTH_CLIENT_SECRET,
                        oauth_secrets_refresh_token.client_secret,
                    )?;
                    builder.with_named_option(
                        bigquery::AUTH_REFRESH_TOKEN,
                        oauth_secrets_refresh_token.refresh_token,
                    )?;
                    builder.with_named_option(
                        bigquery::AUTH_ACCESS_TOKEN_ENDPOINT,
                        oauth_secrets_refresh_token.token_uri,
                    )?;
                    Ok(builder)
                }
                OAuthSecretsVariants::TemporaryToken(oauth_secrets_temporary_token) => {
                    builder.with_named_option(
                        bigquery::AUTH_TYPE,
                        bigquery::auth_type::TEMPORARY_ACCESS_TOKEN,
                    )?;
                    builder.with_named_option(
                        bigquery::AUTH_ACCESS_TOKEN,
                        oauth_secrets_temporary_token.token,
                    )?;
                    Ok(builder)
                }
            },
        }
    }
}

impl Auth for BigqueryAuth {
    fn backend(&self) -> Backend {
        Backend::BigQuery
    }
    fn configure(&self, config: &AdapterConfig) -> Result<database::Builder, AuthError> {
        let big_query_auth_config: BigQueryAuthConfig = config.try_into()?;
        let builder: database::Builder = big_query_auth_config.try_into()?;

        Ok(builder)
    }
}

#[cfg(test)]
mod tests {
    use super::types::*;
    use super::*;
    use adbc_core::options::OptionValue;
    use serde_json::Value;
    use std::{
        collections::HashMap,
        fs::{File, remove_file},
    };

    fn base_config_oauth() -> HashMap<String, Value> {
        HashMap::from([
            ("method".to_string(), Value::from("oauth")),
            ("database".to_string(), Value::from("my_db")),
            ("schema".to_string(), Value::from("my_schema")),
        ])
    }

    fn option_value_to_string(val: &OptionValue) -> String {
        match val {
            OptionValue::String(s) => s.clone(),
            _ => panic!("expected String option value"),
        }
    }

    fn base_config_keyfile() -> HashMap<String, Value> {
        HashMap::from([
            ("method".to_string(), Value::from("service-account")),
            ("database".to_string(), Value::from("my_db")),
            ("schema".to_string(), Value::from("my_schema")),
            ("keyfile".to_string(), Value::from("keyfile.json")),
        ])
    }

    fn base_config_keyfile_json_base64() -> HashMap<String, Value> {
        HashMap::from([
            ("method".to_string(), Value::from("service-account-json")),
            ("database".to_string(), Value::from("my_db")),
            ("schema".to_string(), Value::from("my_schema")),
            (
                "keyfile_json".to_string(),
                Value::from(
                    "ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAiYnEtcHJvamVjdCIsCiAgInByaXZhdGVfa2V5X2lkIjogInh5ejEyMyIsCiAgInByaXZhdGVfa2V5IjogIi0tLS0tQkVHSU4gUFJJVkFURSBLRVktLS0tLVxuWFlaXG4tLS0tLUVORCBQUklWQVRFIEtFWS0tLS0tIiwKICAiY2xpZW50X2VtYWlsIjogInh5ekAxMjMuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLAogICJjbGllbnRfaWQiOiAiMTExMjIyMzMzIiwKICAiYXV0aF91cmkiOiAiaHR0cHM6Ly9hY2NvdW50cy5nb29nbGUuY29tL28vb2F1dGgyL2F1dGgiLAogICJ0b2tlbl91cmkiOiAiaHR0cHM6Ly9vYXV0aDIuZ29vZ2xlYXBpcy5jb20vdG9rZW4iLAogICJhdXRoX3Byb3ZpZGVyX3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vb2F1dGgyL3YxL2NlcnRzIiwKICAiY2xpZW50X3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vcm9ib3QvdjEvbWV0YWRhdGEveDUwOS9mZGUtYmlncXVlcnklNDBmZGUtdGVzdGluZy00NTA4MTYuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iCn0=",
                ),
            ),
        ])
    }

    fn value_option_to_string(option_value: OptionValue) -> String {
        match option_value {
            OptionValue::String(s) => s,
            _ => panic!("Expected ValueOption to be String"),
        }
    }

    #[test]
    fn test_auth_config_from_adapter_config_mismatch() {
        let mut config = base_config_keyfile();
        config.insert("method".to_string(), Value::from("service-account-json"));
        let adapter_config: &AdapterConfig = &AdapterConfig::new(config);
        let result: Result<BigQueryAuthConfig, AuthError> = adapter_config.try_into();
        assert!(result.is_err(), "Expected error with mismatch");
    }

    #[test]
    fn test_auth_config_from_adapter_config_keyfile() {
        let config = base_config_keyfile();
        let adapter_config = &AdapterConfig::new(config);
        let result: Result<BigQueryAuthConfig, AuthError> = adapter_config.try_into();
        match result {
            Ok(cfg) => {
                assert!(matches!(
                    cfg.method_config,
                    BigQueryAuthMethod::ServiceAccount { .. }
                ));
            }
            Err(err) => {
                panic!("Auth config mapping failed with error: {err:?}")
            }
        }
    }

    #[test]
    fn test_auth_config_from_adapter_config_keyfile_json_base64() {
        let config = base_config_keyfile_json_base64();
        let adapter_config = &AdapterConfig::new(config);
        let result: Result<BigQueryAuthConfig, AuthError> = adapter_config.try_into();
        match result {
            Ok(cfg) => {
                assert!(matches!(
                    cfg.method_config,
                    BigQueryAuthMethod::ServiceAccountJson { .. }
                ));
            }
            Err(err) => {
                panic!("Auth config mapping failed with error: {err:?}")
            }
        }
    }

    #[test]
    fn test_builder_from_auth_config_keyfile_json() {
        let bq_auth_config = BigQueryAuthConfig{
                database: "my_db".to_string(),
                schema: "my_schema".to_string(),
                method_config: BigQueryAuthMethod::ServiceAccountJson{ keyfile_json: KeyFileJsonVariants::Object(KeyFileJson {
                    file_type: "service_account".to_string(),
                    project_id: "bq-project".to_string(),
                    private_key_id: "xyz123".to_string(),
                    private_key: "-----BEGIN PRIVATE KEY-----\nXYZ\n-----END PRIVATE KEY-----".to_string(),
                    client_email: "xyz@123.iam.gserviceaccount.com".to_string(),
                    client_id: "111222333".to_string(),
                    auth_uri: "https://accounts.google.com/o/oauth2/auth".to_string(),
                    token_uri: "https://oauth2.googleapis.com/token".to_string(),
                    auth_provider_x509_cert_url: "https://www.googleapis.com/oauth2/v1/certs".to_string(),
                    client_x509_cert_url: "https://www.googleapis.com/robot/v1/metadata/x509/fde-bigquery%40fde-testing-450816.iam.gserviceaccount.com".to_string()
                })},
                location: Some("my_location".to_string()),
                execution_project: None,
            };

        let result: Result<database::Builder, AuthError> = bq_auth_config.clone().try_into();
        match result {
            Ok(builder) => {
                for option in builder.other {
                    let value: String = value_option_to_string(option.1);
                    match option.0 {
                        adbc_core::options::OptionDatabase::Other(o) => match o.as_str() {
                            bigquery::AUTH_CREDENTIALS => {
                                let mc = bq_auth_config.clone().method_config;
                                let actual: &Value = &serde_json::from_str(value.as_str()).unwrap();
                                let expected = &serde_json::to_value(&mc).unwrap()["keyfile_json"];
                                assert_eq!(expected, actual);
                            }
                            bigquery::PROJECT_ID => {
                                assert_eq!(value, "my_db".to_string())
                            }
                            bigquery::DATASET_ID => {
                                assert_eq!(value, "my_schema".to_string())
                            }
                            bigquery::AUTH_TYPE => {
                                assert_eq!(
                                    value,
                                    "adbc.bigquery.sql.auth_type.json_credential_string"
                                        .to_string()
                                )
                            }
                            bigquery::LOCATION => {
                                assert_eq!(value, "my_location".to_string())
                            }
                            _ => panic!("Unexpected BigQuery auth option for service account json"),
                        },
                        _ => panic!("Unexpected option field: {:?}", option.0),
                    }
                }
            }
            Err(err) => {
                panic!("Error getting Builder from BigQueryAuthConfig: {err:?}")
            }
        }
    }

    #[test]
    fn test_builder_from_auth_config_keyfile_path_expansion() {
        let short_path = "~/unit_test_keyfile.json";
        let long_path = shellexpand::tilde(short_path);
        File::create(long_path.to_string()).unwrap();

        let bq_auth_config = BigQueryAuthConfig {
            database: "my_db".to_string(),
            schema: "my_schema".to_string(),
            method_config: BigQueryAuthMethod::ServiceAccount {
                keyfile: short_path.to_string(),
            },
            location: None,
            execution_project: None,
        };

        let result: Result<database::Builder, AuthError> = bq_auth_config.try_into();

        match result {
            Ok(builder) => {
                let other_options = builder.other;
                let option_value = &other_options
                    .iter()
                    .find(|(k, _)| {
                        k == &adbc_core::options::OptionDatabase::Other(
                            bigquery::AUTH_CREDENTIALS.to_string(),
                        )
                    })
                    .unwrap()
                    .1;
                let value: String = value_option_to_string(option_value.clone());
                #[allow(unused_must_use)]
                remove_file(long_path.to_string());
                assert_eq!(value, long_path);
            }
            Err(err) => {
                #[allow(unused_must_use)]
                remove_file(long_path.to_string());
                panic!("Error getting Builder from BigQueryAuthConfig: {err:?}")
            }
        }
    }

    #[test]
    fn test_builder_from_auth_config_oauth_secrets_temporary_token() {
        let access_token = "12345abcde";
        let bq_auth_config = BigQueryAuthConfig {
            database: "my_db".to_string(),
            schema: "my_schema".to_string(),
            method_config: BigQueryAuthMethod::OauthSecrets(OAuthSecretsVariants::TemporaryToken(
                OAuthSecretsTemporaryToken {
                    token: access_token.to_string(),
                },
            )),
            location: None,
            execution_project: None,
        };

        let result: Result<database::Builder, AuthError> = bq_auth_config.try_into();

        match result {
            Ok(builder) => {
                let other_options = builder.other;
                let option_value = &other_options
                    .iter()
                    .find(|(k, _)| {
                        k == &adbc_core::options::OptionDatabase::Other(
                            bigquery::AUTH_ACCESS_TOKEN.to_string(),
                        )
                    })
                    .unwrap()
                    .1;
                let value: String = value_option_to_string(option_value.clone());
                assert_eq!(value, access_token);
            }
            Err(err) => {
                panic!("Error getting Builder from BigQueryAuthConfig: {err:?}")
            }
        }
    }

    #[test]
    fn test_auth_config_invalid_base64() {
        let fake_b64 = KeyFileJsonVariants::Base64("fake_base64".to_string());
        let check: Result<KeyFileJson, AuthError> = fake_b64.try_into();
        assert!(check.is_err(), "Expected error on invalid base64");
    }

    #[test]
    fn test_auth_config_from_adapter_config_oauth() {
        let cfg_map = base_config_oauth();
        let adapter_cfg = &AdapterConfig::new(cfg_map);
        let result: Result<BigQueryAuthConfig, AuthError> = adapter_cfg.try_into();

        match result {
            Ok(cfg) => {
                // concrete config variant
                assert!(matches!(
                    cfg.method_config,
                    BigQueryAuthMethod::Oauth { .. }
                ));
            }
            Err(e) => panic!("OAuth auth‑config mapping failed: {e:?}"),
        }
    }

    #[test]
    fn test_builder_from_auth_config_oauth() {
        let bq_cfg = BigQueryAuthConfig {
            database: "my_db".to_owned(),
            schema: "my_schema".to_owned(),
            method_config: BigQueryAuthMethod::Oauth {}, // empty struct
            location: None,
            execution_project: None,
        };

        let result: Result<database::Builder, AuthError> = bq_cfg.try_into();

        match result {
            Ok(builder) => {
                // Look for AUTH_TYPE = DEFAULT
                let auth_type_opt = builder
                    .other
                    .iter()
                    .find(|(k, _)| {
                        k == &adbc_core::options::OptionDatabase::Other(
                            bigquery::AUTH_TYPE.to_string(),
                        )
                    })
                    .expect("AUTH_TYPE option missing");

                let auth_type_val = option_value_to_string(&auth_type_opt.1);
                assert_eq!(auth_type_val, bigquery::auth_type::DEFAULT);

                // No credential‑specific options should be present
                for (k, _) in &builder.other {
                    if let adbc_core::options::OptionDatabase::Other(name) = k {
                        assert_ne!(name, bigquery::AUTH_CREDENTIALS);
                        assert_ne!(name, bigquery::AUTH_REFRESH_TOKEN);
                    }
                }
            }
            Err(e) => panic!("Builder creation for OAuth failed: {e:?}"),
        }
    }

    #[test]
    fn test_auth_config_oauth_allow_redundant_fields() {
        let mut cfg_map = base_config_oauth();
        cfg_map.insert("keyfile".into(), Value::from("some.json")); // invalid extra

        let adapter_cfg = &AdapterConfig::new(cfg_map);
        let result: Result<BigQueryAuthConfig, AuthError> = adapter_cfg.try_into();

        assert!(
            result.is_ok(),
            "Expected no error when extra fields are supplied for OAuth method"
        );
    }
}
