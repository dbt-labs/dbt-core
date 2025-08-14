use crate::{AdapterConfig, Auth, AuthError};

use dbt_xdbc::{Backend, database, databricks};

/// Databricks auth
#[derive(Debug, Default)]
pub struct DatabricksAuth;

impl Auth for DatabricksAuth {
    fn backend(&self) -> Backend {
        #[cfg(feature = "odbc")]
        {
            Backend::DatabricksODBC
        }
        #[cfg(not(feature = "odbc"))]
        {
            Backend::Databricks
        }
    }

    fn configure(&self, config: &AdapterConfig) -> Result<database::Builder, AuthError> {
        let mut builder = database::Builder::new(self.backend());

        if self.backend() == Backend::DatabricksODBC {
            use databricks::odbc;
            // Config values for DSN-less connection to Databricks:
            // https://learn.microsoft.com/en-us/azure/databricks/integrations/odbc/authentication
            for key in ["token", "http_path", "host", "schema", "database"].iter() {
                if let Some(value) = config.maybe_get_str(key)? {
                    match *key {
                        "token" => builder.with_named_option(odbc::TOKEN_FIELD, value),
                        "http_path" => builder.with_named_option(odbc::HTTP_PATH, value),
                        "host" => builder.with_named_option(odbc::HOST, value),
                        "schema" => builder.with_named_option(odbc::SCHEMA, value),
                        "database" => builder.with_named_option(odbc::CATALOG, value),
                        _ => panic!("unexpected key: {key}"),
                    }?;
                }
            }

            // configures the ODBC driver and the defaults needed for token authentication
            builder
                .with_username(odbc::DEFAULT_TOKEN_UID)
                .with_named_option(odbc::DRIVER, odbc::odbc_driver_path())?
                .with_named_option(odbc::PORT, odbc::DEFAULT_PORT)?
                .with_named_option(odbc::SSL, "1")?
                .with_named_option(odbc::THRIFT_TRANSPORT, "2")?
                .with_named_option(odbc::AUTH_MECHANISM, odbc::auth_mechanism_options::TOKEN)?;
        } else {
            validate_config(config)?;
            // all of the following options are required for any Databricks connection
            builder.with_named_option(databricks::HOST, config.get_str("host")?)?;
            builder.with_named_option(databricks::SCHEMA, config.get_str("schema")?)?;
            builder.with_named_option(databricks::CATALOG, config.get_str("database")?)?;

            // http_path is of the form:
            //  /sql/1.0/warehouses/<warehouse-id
            //  /sql/protocolv1/o/<instance>/<cluster-id>
            // we need to extract the warehouse-id or cluster-id from the http_path
            // warehouses and clusters are separate concepts and endpoints in Databricks
            let http_path = config.get_str("http_path")?;
            if http_path.contains("warehouses") {
                let warehouse_id = http_path.split("/warehouses/").nth(1).unwrap();
                builder.with_named_option(databricks::WAREHOUSE, warehouse_id)?;
            } else if http_path.contains("protocolv1") {
                let cluster_id = http_path.split("/").nth(4).unwrap();
                builder.with_named_option(databricks::CLUSTER, cluster_id)?;
            } else {
                return Err(AuthError::config(format!("Invalid http_path: {http_path}")));
            }
            // Personal Access Token
            if let Some(token) = config.maybe_get_str("token")? {
                builder.with_named_option(databricks::TOKEN, token)?;
                builder.with_named_option(databricks::AUTH_TYPE, databricks::auth_type::PAT)?;
            }
            // Azure Client Secret Oauth
            else if let Some(azure_client_id) = config.maybe_get_str("azure_client_id")? {
                builder.with_named_option(databricks::AZURE_CLIENT_ID, azure_client_id)?;
                builder.with_named_option(
                    databricks::AZURE_CLIENT_SECRET,
                    config.get_str("azure_client_secret")?,
                )?;
                builder.with_named_option(
                    databricks::AUTH_TYPE,
                    databricks::auth_type::AZURE_CLIENT_SECRET,
                )?;
            }
            // External Browser Oauth - U2M Oauth
            else if config.get_str("client_secret").is_err() {
                if let Some(client_id) = config.maybe_get_str("client_id")? {
                    builder.with_named_option(databricks::CLIENT_ID, client_id)?;
                }
                builder.with_named_option(
                    databricks::AUTH_TYPE,
                    databricks::auth_type::EXTERNAL_BROWSER,
                )?;
            }
            // M2M Oauth
            else if let Some(client_id) = config.maybe_get_str("client_id")? {
                builder.with_named_option(databricks::CLIENT_ID, client_id)?;
                builder.with_named_option(
                    databricks::CLIENT_SECRET,
                    config.get_str("client_secret")?,
                )?;
                builder
                    .with_named_option(databricks::AUTH_TYPE, databricks::auth_type::OAUTH_M2M)?;
            } else {
                return Err(AuthError::config("No valid authentication method provided"));
            }
        }
        Ok(builder)
    }
}

fn validate_config(config: &AdapterConfig) -> Result<(), AuthError> {
    if config.get_str("http_path").is_err() {
        return Err(AuthError::config("http_path is required"));
    }
    if config.get_str("host").is_err() {
        return Err(AuthError::config("host is required".to_string()));
    }
    let is_oauth = config.get_str("auth_type").is_ok_and(|s| s == "oauth");
    if config.get_str("token").is_err() && !is_oauth {
        return Err(AuthError::config(
            "The config `auth_type: oauth` is required when not using access token",
        ));
    }
    if config.get_str("client_id").is_err() && config.get_str("client_secret").is_ok() {
        return Err(AuthError::config(
            "The config 'client_id' is required to connect to Databricks when 'client_secret' is present",
        ));
    }
    let azure_client_no_secret =
        config.get_str("azure_client_id").is_err() && config.get_str("azure_client_secret").is_ok();
    let azure_secret_no_client =
        config.get_str("azure_client_id").is_ok() && config.get_str("azure_client_secret").is_err();
    if azure_client_no_secret || azure_secret_no_client {
        return Err(AuthError::config(
            "The config 'azure_client_id' and 'azure_client_secret' must be both present or both absent",
        ));
    }
    Ok(())
}

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

    fn run_config_test(
        config: HashMap<String, YmlValue>,
        expected: &[(&str, &str)],
    ) -> Result<(), AuthError> {
        let auth = DatabricksAuth {};
        let builder = auth.configure(&AdapterConfig::new(config))?;

        let mut results = HashMap::new();

        assert_eq!(builder.clone().into_iter().count(), expected.len());

        for (k, v) in builder.into_iter() {
            let key = match k {
                OptionDatabase::Username => "user".to_owned(),
                OptionDatabase::Password => "password".to_owned(),
                OptionDatabase::Other(name) => name.to_owned(),
                _ => continue,
            };
            results.insert(key, str_value(&v).to_owned());
        }

        for &(key, expected_val) in expected {
            assert_eq!(
                results
                    .get(key)
                    .unwrap_or_else(|| panic!("Missing key: {key}")),
                &expected_val,
                "Value mismatch for key: {key}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_token_warehouse() {
        let mut config = HashMap::new();
        config.insert("host".to_string(), YmlValue::from("H".to_string()));
        config.insert("schema".to_string(), YmlValue::from("S".to_string()));
        config.insert(
            "http_path".to_string(),
            YmlValue::from("/sql/1.0/warehouses/warehouse-id".to_string()),
        );
        config.insert("token".to_string(), YmlValue::from("T".to_string()));
        config.insert("database".to_string(), YmlValue::from("C".to_string()));

        let expected = vec![
            (databricks::TOKEN, "T"),
            (databricks::SCHEMA, "S"),
            (databricks::HOST, "H"),
            (databricks::WAREHOUSE, "warehouse-id"),
            (databricks::CATALOG, "C"),
            (databricks::AUTH_TYPE, databricks::auth_type::PAT),
        ];
        run_config_test(config, &expected).unwrap();
    }

    #[test]
    fn test_token_cluster_with_optional_fields() {
        let mut config = HashMap::new();
        config.insert("host".to_string(), YmlValue::from("H".to_string()));
        config.insert("schema".to_string(), YmlValue::from("S".to_string()));
        config.insert(
            "http_path".to_string(),
            YmlValue::from("sql/protocolv1/o/1030i40i30i50i3/my-cluster-id".to_string()),
        );
        config.insert("token".to_string(), YmlValue::from("T".to_string()));
        config.insert("database".to_string(), YmlValue::from("C".to_string()));

        let expected = vec![
            (databricks::TOKEN, "T"),
            (databricks::SCHEMA, "S"),
            (databricks::HOST, "H"),
            (databricks::CLUSTER, "my-cluster-id"),
            (databricks::CATALOG, "C"),
            (databricks::AUTH_TYPE, databricks::auth_type::PAT),
        ];
        run_config_test(config, &expected).unwrap();
    }

    #[test]
    fn test_azure_client_secret() {
        let mut config = HashMap::new();
        config.insert("host".to_string(), YmlValue::from("H".to_string()));
        config.insert("schema".to_string(), YmlValue::from("S".to_string()));
        config.insert(
            "http_path".to_string(),
            YmlValue::from("sql/protocolv1/o/1030i40i30i50i3/my-cluster-id".to_string()),
        );
        config.insert(
            "azure_client_id".to_string(),
            YmlValue::from("A".to_string()),
        );
        config.insert(
            "azure_client_secret".to_string(),
            YmlValue::from("A".to_string()),
        );
        config.insert("database".to_string(), YmlValue::from("C".to_string()));
        config.insert("auth_type".to_string(), YmlValue::from("oauth".to_string()));
        let expected = vec![
            (databricks::AZURE_CLIENT_ID, "A"),
            (databricks::AZURE_CLIENT_SECRET, "A"),
            (databricks::SCHEMA, "S"),
            (databricks::HOST, "H"),
            (databricks::CLUSTER, "my-cluster-id"),
            (databricks::CATALOG, "C"),
            (
                databricks::AUTH_TYPE,
                databricks::auth_type::AZURE_CLIENT_SECRET,
            ),
        ];
        run_config_test(config, &expected).unwrap();
    }

    #[test]
    fn test_m2m_oauth() {
        let mut config = HashMap::new();
        config.insert("host".to_string(), YmlValue::from("H".to_string()));
        config.insert("schema".to_string(), YmlValue::from("S".to_string()));
        config.insert(
            "http_path".to_string(),
            YmlValue::from("sql/protocolv1/o/1030i40i30i50i3/my-cluster-id".to_string()),
        );
        config.insert("client_id".to_string(), YmlValue::from("O".to_string()));
        config.insert("client_secret".to_string(), YmlValue::from("O".to_string()));
        config.insert("database".to_string(), YmlValue::from("C".to_string()));
        config.insert("auth_type".to_string(), YmlValue::from("oauth".to_string()));

        let expected = vec![
            (databricks::CLIENT_ID, "O"),
            (databricks::CLIENT_SECRET, "O"),
            (databricks::SCHEMA, "S"),
            (databricks::HOST, "H"),
            (databricks::CLUSTER, "my-cluster-id"),
            (databricks::CATALOG, "C"),
            (databricks::AUTH_TYPE, databricks::auth_type::OAUTH_M2M),
        ];
        run_config_test(config, &expected).unwrap();
    }

    #[test]
    fn test_external_browser_oauth() {
        let mut config = HashMap::new();
        config.insert("host".to_string(), YmlValue::from("H".to_string()));
        config.insert("schema".to_string(), YmlValue::from("S".to_string()));
        config.insert(
            "http_path".to_string(),
            YmlValue::from("sql/protocolv1/o/1030i40i30i50i3/my-cluster-id".to_string()),
        );
        config.insert("client_id".to_string(), YmlValue::from("O".to_string()));
        config.insert("database".to_string(), YmlValue::from("C".to_string()));
        config.insert("auth_type".to_string(), YmlValue::from("oauth".to_string()));
        let expected = vec![
            (databricks::SCHEMA, "S"),
            (databricks::HOST, "H"),
            (databricks::CLUSTER, "my-cluster-id"),
            (databricks::CATALOG, "C"),
            (databricks::CLIENT_ID, "O"),
            (
                databricks::AUTH_TYPE,
                databricks::auth_type::EXTERNAL_BROWSER,
            ),
        ];
        run_config_test(config, &expected).unwrap();
    }

    #[test]
    fn test_external_browser_oauth_without_client_id() {
        let mut config = HashMap::new();
        config.insert("host".to_string(), YmlValue::from("H".to_string()));
        config.insert("schema".to_string(), YmlValue::from("S".to_string()));
        config.insert(
            "http_path".to_string(),
            YmlValue::from("sql/protocolv1/o/1030i40i30i50i3/my-cluster-id".to_string()),
        );
        config.insert("database".to_string(), YmlValue::from("C".to_string()));
        config.insert("auth_type".to_string(), YmlValue::from("oauth".to_string()));
        let expected = vec![
            (databricks::SCHEMA, "S"),
            (databricks::HOST, "H"),
            (databricks::CLUSTER, "my-cluster-id"),
            (databricks::CATALOG, "C"),
            (
                databricks::AUTH_TYPE,
                databricks::auth_type::EXTERNAL_BROWSER,
            ),
        ];
        run_config_test(config, &expected).unwrap();
    }

    #[test]
    fn test_validate_config_errors_with_missing_token_and_not_oauth() {
        let mut config = HashMap::new();
        config.insert("host".to_string(), YmlValue::from("H".to_string()));
        config.insert(
            "http_path".to_string(),
            YmlValue::from("sql/protocolv1/o/1030i40i30i50i3/my-cluster-id".to_string()),
        );
        config.insert("schema".to_string(), YmlValue::from("S".to_string()));
        config.insert("database".to_string(), YmlValue::from("C".to_string()));
        config.insert(
            "auth_type".to_string(),
            YmlValue::from("external_browser".to_string()),
        );
        let result = validate_config(&AdapterConfig::new(config));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().msg(),
            "The config `auth_type: oauth` is required when not using access token"
        );
    }

    #[test]
    fn test_validate_config_errors_with_missing_client_id_and_present_client_secret() {
        let mut config = HashMap::new();
        config.insert("host".to_string(), YmlValue::from("H".to_string()));
        config.insert(
            "http_path".to_string(),
            YmlValue::from("sql/protocolv1/o/1030i40i30i50i3/my-cluster-id".to_string()),
        );
        config.insert("schema".to_string(), YmlValue::from("S".to_string()));
        config.insert("database".to_string(), YmlValue::from("C".to_string()));
        config.insert(
            "client_secret".to_string(),
            YmlValue::from("some_secret".to_string()),
        );
        config.insert("auth_type".to_string(), YmlValue::from("oauth".to_string()));
        let result = validate_config(&AdapterConfig::new(config));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().msg(),
            "The config 'client_id' is required to connect to Databricks when 'client_secret' is present"
        );
    }

    #[test]
    fn test_validate_config_errors_with_missing_azure_client_id_and_present_azure_client_secret() {
        let mut config = HashMap::new();
        config.insert("host".to_string(), YmlValue::from("H".to_string()));
        config.insert(
            "http_path".to_string(),
            YmlValue::from("sql/protocolv1/o/1030i40i30i50i3/my-cluster-id".to_string()),
        );
        config.insert("schema".to_string(), YmlValue::from("S".to_string()));
        config.insert("database".to_string(), YmlValue::from("C".to_string()));
        config.insert(
            "azure_client_secret".to_string(),
            YmlValue::from("some_secret".to_string()),
        );
        config.insert("auth_type".to_string(), YmlValue::from("oauth".to_string()));
        let result = validate_config(&AdapterConfig::new(config));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().msg(),
            "The config 'azure_client_id' and 'azure_client_secret' must be both present or both absent"
        );
    }

    #[test]
    fn test_validate_config_errors_with_present_azure_client_id_and_missing_azure_client_secret() {
        let mut config = HashMap::new();
        config.insert("host".to_string(), YmlValue::from("H".to_string()));
        config.insert(
            "http_path".to_string(),
            YmlValue::from("sql/protocolv1/o/1030i40i30i50i3/my-cluster-id".to_string()),
        );
        config.insert("schema".to_string(), YmlValue::from("S".to_string()));
        config.insert("database".to_string(), YmlValue::from("C".to_string()));
        config.insert(
            "azure_client_id".to_string(),
            YmlValue::from("some_id".to_string()),
        );
        config.insert("auth_type".to_string(), YmlValue::from("oauth".to_string()));
        let result = validate_config(&AdapterConfig::new(config));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().msg(),
            "The config 'azure_client_id' and 'azure_client_secret' must be both present or both absent"
        );
    }
}
