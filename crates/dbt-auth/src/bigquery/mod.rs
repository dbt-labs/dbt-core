use crate::AdapterConfig;
use crate::Auth;
use crate::AuthError;
use dbt_xdbc::bigquery::auth_type;
use dbt_xdbc::{Backend, bigquery, database};
use serde::{Deserialize, Serialize};
use std::path::Path;

type YmlValue = dbt_serde_yaml::Value;

#[derive(Deserialize, Serialize)]
struct KeyFileJson {
    #[serde(rename = "type")]
    pub file_type: String,
    pub project_id: String,
    pub private_key_id: String,
    pub private_key: String,
    pub client_email: String,
    pub client_id: String,
    pub auth_uri: String,
    pub token_uri: String,
    pub auth_provider_x509_cert_url: String,
    pub client_x509_cert_url: String,
}

pub struct BigqueryAuth;

impl BigqueryAuth {
    /// Derive the project ID from the config.
    ///
    /// The project ID is optional as in some auth methods it is inferred from the credentials.
    fn project_id(config: &AdapterConfig) -> Result<Option<String>, AuthError> {
        let project_id = if let Some(execution_project) = config.get_string("execution_project") {
            Some(execution_project)
        } else {
            if let Some(project) = config.get_string("project") {
                if config.get_string("database").is_some() {
                    return Err(AuthError::config(
                        "Don't specify 'database' when 'project' is specified",
                    ));
                }
                Some(project)
            } else {
                config.get_string("database") // use "database" as GCP project ID
            }
        };
        Ok(project_id.map(|s| s.to_string()))
    }

    /// Derive the dataset ID from the config.
    fn dataset_id(config: &AdapterConfig) -> Result<String, AuthError> {
        let dataset = config.get_string("dataset");
        let schema = config.get_string("schema");
        let dataset_id = if let Some(d) = dataset {
            if schema.is_some() {
                return Err(AuthError::config(
                    "Don't specify both 'dataset' and 'schema' in BigQuery config, they are aliases",
                ));
            }
            d
        } else if let Some(s) = schema {
            s
        } else {
            return Err(AuthError::config(
                "Missing required field 'dataset' or 'schema'",
            ));
        };
        Ok(dataset_id.to_string())
    }

    fn config_service_account(
        config: &AdapterConfig,
        builder: &mut database::Builder,
    ) -> Result<(), AuthError> {
        let keyfile = config.require_string("keyfile").map_err(|_| {
            AuthError::config("Missing required field 'keyfile' for method 'service-account'")
        })?;
        let expanded_path = shellexpand::tilde(keyfile.as_ref()).to_string();
        if Path::new(&expanded_path).exists() {
            builder.with_named_option(bigquery::AUTH_TYPE, auth_type::JSON_CREDENTIAL_FILE)?;
            builder.with_named_option(bigquery::AUTH_CREDENTIALS, expanded_path.as_str())?;
            Ok(())
        } else {
            Err(AuthError::config(format!(
                "Keyfile '{keyfile}' does not exist"
            )))
        }
    }

    fn config_service_account_json(
        config: &AdapterConfig,
        builder: &mut database::Builder,
    ) -> Result<(), AuthError> {
        // is has "json" in the name, but it's actually a YAML mapping
        let keyfile_json = config.require("keyfile_json")?;
        let keyfile_yaml = match keyfile_json {
            YmlValue::Mapping(_, _) => keyfile_json.clone(),
            YmlValue::String(base64_json_str, _) => {
                use base64::prelude::*;
                // base64 -> bytes
                let decoded = BASE64_STANDARD.decode(base64_json_str).map_err(|err| {
                    AuthError::config(format!(
                        "Error decoding 'keyfile_json' from base64: '{err}'"
                    ))
                })?;
                // bytes -> JSON value -> YAML value
                let keyfile_yaml: YmlValue = serde_json::from_slice(&decoded)?;
                if keyfile_yaml.is_mapping() {
                    keyfile_yaml
                } else {
                    return Err(AuthError::config(
                        "'keyfile_json' must be a JSON object when provided as base64",
                    ));
                }
            }
            _ => {
                return Err(AuthError::config(
                    "'keyfile_json' must be a YAML mapping or a base64-encoded string",
                ));
            }
        };

        // YAML value -> KeyFileJson struct
        let mut keyfile_json: KeyFileJson =
            dbt_serde_yaml::from_value(keyfile_yaml).map_err(|e| {
                AuthError::config(format!(
                    "Error parsing 'keyfile_json' in BigQuery configuration: {e}"
                ))
            })?;
        // Replace escaped newlines with a single newline
        keyfile_json.private_key = keyfile_json.private_key.replace("\\\\n", "\\n");

        // Turn it into a JSON string again so we can pass it to the ADBC driver
        let keyfile_json_string: String = serde_json::to_value(keyfile_json)
            .map_err(|e| AuthError::config(e.to_string()))?
            .to_string();

        builder.with_named_option(bigquery::AUTH_TYPE, auth_type::JSON_CREDENTIAL_STRING)?;
        builder.with_named_option(bigquery::AUTH_CREDENTIALS, keyfile_json_string)?;
        Ok(())
    }

    fn config_oauth_secrets(
        config: &AdapterConfig,
        builder: &mut database::Builder,
    ) -> Result<(), AuthError> {
        if let Some(refresh_token) = config.get_string("refresh_token") {
            let client_id = config.require_string("client_id")?;
            let client_secret = config.require_string("client_secret")?;
            let token_uri = config.require_string("token_uri")?;

            builder.with_named_option(bigquery::AUTH_TYPE, auth_type::USER_AUTHENTICATION)?;
            builder.with_named_option(bigquery::AUTH_CLIENT_ID, client_id)?;
            builder.with_named_option(bigquery::AUTH_CLIENT_SECRET, client_secret)?;
            builder.with_named_option(bigquery::AUTH_REFRESH_TOKEN, refresh_token)?;
            builder.with_named_option(bigquery::AUTH_ACCESS_TOKEN_ENDPOINT, token_uri)?;
        } else if let Some(access_token) = config.get_string("token") {
            builder.with_named_option(bigquery::AUTH_TYPE, auth_type::TEMPORARY_ACCESS_TOKEN)?;
            builder.with_named_option(bigquery::AUTH_ACCESS_TOKEN, access_token)?;
        } else {
            return Err(AuthError::config(
                "For method 'oauth-secrets', either 'refresh_token', 'client_secret', ... or 'token' must be provided",
            ));
        }
        Ok(())
    }
}

impl Auth for BigqueryAuth {
    fn backend(&self) -> Backend {
        Backend::BigQuery
    }

    fn configure(&self, config: &AdapterConfig) -> Result<database::Builder, AuthError> {
        let mut builder = database::Builder::new(Backend::BigQuery);

        if let Some(project_id) = Self::project_id(config)? {
            builder.with_named_option(bigquery::PROJECT_ID, project_id)?;
        }

        let dataset_id = Self::dataset_id(config)?;
        builder.with_named_option(bigquery::DATASET_ID, dataset_id)?;

        if let Some(location) = config.get_string("location") {
            builder.with_named_option(bigquery::LOCATION, location)?;
        }

        if let Some(method) = config.get_string("method") {
            match method.as_ref() {
                "oauth" => {
                    // interactive gcloud login
                    builder.with_named_option(bigquery::AUTH_TYPE, auth_type::DEFAULT)?;
                }
                "service-account" => {
                    Self::config_service_account(config, &mut builder)?;
                }
                "service-account-json" => {
                    Self::config_service_account_json(config, &mut builder)?;
                }
                "oauth-secrets" => {
                    Self::config_oauth_secrets(config, &mut builder)?;
                }
                unknown_method => {
                    return Err(AuthError::config(format!(
                        "Unknown or unimplemented authentication method '{unknown_method}' for BigQuery"
                    )));
                }
            };
        } else {
            return Err(AuthError::config(
                "Missing required 'method' field in BigQuery config",
            ));
        }

        Ok(builder)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::options::{OptionDatabase, OptionValue};
    use dbt_serde_yaml::Mapping;

    fn base_config_oauth() -> Mapping {
        Mapping::from_iter([
            ("method".into(), "oauth".into()),
            ("database".into(), "my_db".into()),
            ("schema".into(), "my_schema".into()),
        ])
    }

    fn base_config_keyfile() -> Mapping {
        Mapping::from_iter([
            ("method".into(), "service-account".into()),
            ("database".into(), "my_db".into()),
            ("schema".into(), "my_schema".into()),
            ("keyfile".into(), "akeyfilethatdoesnotexist.json".into()),
        ])
    }

    fn base_config_keyfile_json_base64() -> Mapping {
        Mapping::from_iter([
            ("method".into(), "service-account-json".into()),
            ("database".into(), "my_db".into()),
            ("schema".into(), "my_schema".into()),
            (
                "keyfile_json".into(),
                (
                    "ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAiYnEtcHJvamVjdCIsCiAgInByaXZhdGVfa2V5X2lkIjogInh5ejEyMyIsCiAgInByaXZhdGVfa2V5IjogIi0tLS0tQkVHSU4gUFJJVkFURSBLRVktLS0tLVxuWFlaXG4tLS0tLUVORCBQUklWQVRFIEtFWS0tLS0tIiwKICAiY2xpZW50X2VtYWlsIjogInh5ekAxMjMuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLAogICJjbGllbnRfaWQiOiAiMTExMjIyMzMzIiwKICAiYXV0aF91cmkiOiAiaHR0cHM6Ly9hY2NvdW50cy5nb29nbGUuY29tL28vb2F1dGgyL2F1dGgiLAogICJ0b2tlbl91cmkiOiAiaHR0cHM6Ly9vYXV0aDIuZ29vZ2xlYXBpcy5jb20vdG9rZW4iLAogICJhdXRoX3Byb3ZpZGVyX3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vb2F1dGgyL3YxL2NlcnRzIiwKICAiY2xpZW50X3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vcm9ib3QvdjEvbWV0YWRhdGEveDUwOS9mZGUtYmlncXVlcnklNDBmZGUtdGVzdGluZy00NTA4MTYuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iCn0="
                ).into(),
            ),
        ])
    }

    fn try_configure(config: Mapping) -> Result<database::Builder, AuthError> {
        let auth = BigqueryAuth {};
        let adapter_config = AdapterConfig::new(config);
        auth.configure(&adapter_config)
    }

    fn option_string_value(option_value: OptionValue) -> String {
        match option_value {
            OptionValue::String(s) => s,
            _ => panic!("Expected ValueOption to be String"),
        }
    }

    fn other_option_value(builder: &database::Builder, key: &str) -> Option<String> {
        let option = OptionDatabase::Other(key.to_string());
        builder.other.iter().find_map(|(k, v)| {
            if *k == option {
                Some(option_string_value(v.clone()))
            } else {
                None
            }
        })
    }

    #[test]
    fn test_auth_config_from_adapter_config_mismatch() {
        let mut config = base_config_keyfile();
        config.insert("method".into(), "service-account-json".into());
        let result = try_configure(config);
        assert!(result.is_err(), "Expected error with mismatch");
    }

    #[test]
    fn test_auth_config_from_adapter_config_keyfile() {
        let config = base_config_keyfile();
        let err = try_configure(config).unwrap_err();
        assert!(
            err.msg()
                .contains("Keyfile 'akeyfilethatdoesnotexist.json' does not exist")
        );
    }

    #[test]
    fn test_auth_config_from_adapter_config_keyfile_json_base64() {
        let config = base_config_keyfile_json_base64();
        match try_configure(config) {
            Ok(builder) => {
                assert_eq!(
                    other_option_value(&builder, bigquery::AUTH_TYPE).unwrap(),
                    auth_type::JSON_CREDENTIAL_STRING
                );
                let keyfile_json =
                    other_option_value(&builder, bigquery::AUTH_CREDENTIALS).unwrap();
                assert!(keyfile_json.contains(r#""type":"service_account""#));
                assert!(keyfile_json.contains("BEGIN PRIVATE KEY"));
                assert!(keyfile_json.contains("END PRIVATE KEY"));
            }
            Err(err) => {
                panic!("Auth config mapping failed with error: {err:?}")
            }
        }
    }

    #[test]
    fn test_builder_from_auth_config_keyfile_json() {
        let yaml_doc = r#"
method: service-account-json
database: my_db
schema: my_schema
keyfile_json:
    type: service_account
    project_id: bq-project
    private_key_id: xyz123
    private_key: |
        -----BEGIN PRIVATE KEY-----
        XYZ
        -----END PRIVATE KEY-----
    client_email: xyz@123.iam.gserviceaccount.com
    client_id: "111222333"
    auth_uri: https://accounts.google.com/o/oauth2/auth
    token_uri: https://oauth2.googleapis.com/token
    auth_provider_x509_cert_url: https://www.googleapis.com/oauth2/v1/certs
    client_x509_cert_url: https://www.googleapis.com/robot/v1/metadata/x509/fde-bigquery%40fde-testing-450816.iam.gserviceaccount.com
location: my_location
"#;
        let config = dbt_serde_yaml::from_str::<Mapping>(yaml_doc).unwrap();
        let builder = try_configure(config).unwrap();
        for option in builder.other {
            let value: String = option_string_value(option.1);
            match option.0 {
                OptionDatabase::Other(o) => match o.as_str() {
                    bigquery::AUTH_CREDENTIALS => {
                        assert!(value.contains(r#""type":"service_account""#));
                        assert!(value.contains("BEGIN PRIVATE KEY"));
                        assert!(value.contains("END PRIVATE KEY"));
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
                            "adbc.bigquery.sql.auth_type.json_credential_string".to_string()
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

    #[test]
    fn test_builder_from_auth_config_oauth_secrets_temporary_token() {
        let yaml_doc = r#"
method: oauth-secrets
database: my_db
schema: my_schema
token: 12345abcde
"#;
        let config = dbt_serde_yaml::from_str::<Mapping>(yaml_doc).unwrap();

        let builder = try_configure(config).unwrap();
        let acces_token = other_option_value(&builder, bigquery::AUTH_ACCESS_TOKEN)
            .expect("Expected AUTH_ACCESS_TOKEN option to be set");
        assert_eq!(acces_token, "12345abcde".to_string());
    }

    #[test]
    fn test_auth_config_from_adapter_config_oauth() {
        let config = base_config_oauth();
        let builder = try_configure(config).unwrap();
        let auth_type = other_option_value(&builder, bigquery::AUTH_TYPE)
            .expect("Expected AUTH_TYPE option to be set");
        assert_eq!(auth_type, auth_type::DEFAULT.to_string());
    }

    #[test]
    fn test_builder_from_auth_config_oauth() {
        let yaml_doc = r#"
database: my_db
schema: my_schema
method: oauth
"#;
        let config = dbt_serde_yaml::from_str::<Mapping>(yaml_doc).unwrap();
        let builder = try_configure(config).unwrap();
        let auth_type = other_option_value(&builder, bigquery::AUTH_TYPE)
            .expect("Expected AUTH_TYPE option to be set");
        assert_eq!(auth_type, auth_type::DEFAULT.to_string());

        // No credential‑specific options should be present
        assert!(other_option_value(&builder, bigquery::AUTH_CREDENTIALS).is_none());
        assert!(other_option_value(&builder, bigquery::AUTH_REFRESH_TOKEN).is_none());
    }

    #[test]
    fn test_auth_config_oauth_allow_redundant_fields() {
        let mut config = base_config_oauth();
        config.insert("keyfile".into(), YmlValue::from("some.json")); // invalid extra

        try_configure(config)
            .expect("Expected no error when extra fields are supplied for OAuth method");
    }
}
