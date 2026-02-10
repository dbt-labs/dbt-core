use crate::{AdapterConfig, Auth, AuthError};
use dbt_serde_yaml::Value;
use std::borrow::Cow;

use dbt_xdbc::{Backend, database, databricks};

/// User agent name provided to dbx for Fusion.
///
/// Official guidance is <isv-name+product-name> but dbt Core provides 'dbt' only
/// and we follow suit.
///
/// Ref: https://github.com/databricks/databricks-sql-go/blob/56b8a73b09908454e3070fe513ff2563c85ba214/connector.go#L214
const USER_AGENT_NAME: &str = "dbt";

/// Supported Databricks authentication types.
/// When `auth_type` is absent, defaults to token-based (PAT) authentication.
/// When `auth_type` is present, only `oauth` is a valid value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DatabricksAuthType {
    /// OAuth authentication
    OAuth,
    /// Personal Access Token
    Token,
}

impl DatabricksAuthType {
    /// Parse auth_type from config.
    /// - Absent or "token": defaults to token-based authentication
    /// - "oauth": OAuth authentication
    /// - Any other value: error
    fn from_config(config: &AdapterConfig) -> Result<Self, AuthError> {
        match config.get_string("auth_type") {
            Some(s) if s.eq_ignore_ascii_case("oauth") => Ok(DatabricksAuthType::OAuth),
            Some(s) if s.eq_ignore_ascii_case("token") => Ok(DatabricksAuthType::Token),
            Some(invalid) => Err(AuthError::config(format!(
                "Invalid auth_type '{}'. Valid values are: 'oauth', 'token'.",
                invalid
            ))),
            None => Ok(DatabricksAuthType::Token),
        }
    }
}

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
        let http_path = resolve_http_path(config)?;

        let mut builder = database::Builder::new(self.backend());
        builder.with_named_option(databricks::USER_AGENT, USER_AGENT_NAME)?;

        if self.backend() == Backend::DatabricksODBC {
            use databricks::odbc;
            // Config values for DSN-less connection to Databricks:
            // https://learn.microsoft.com/en-us/azure/databricks/integrations/odbc/authentication
            for key in ["token", "http_path", "host", "schema", "database"].into_iter() {
                if let Some(value) = config.get_string(key) {
                    match key {
                        "token" => builder.with_named_option(odbc::TOKEN_FIELD, value),
                        "http_path" => {
                            builder.with_named_option(odbc::HTTP_PATH, http_path.clone())
                        }
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
            builder.with_named_option(databricks::HOST, config.require_string("host")?)?;
            builder.with_named_option(databricks::SCHEMA, config.require_string("schema")?)?;
            builder.with_named_option(databricks::CATALOG, config.require_string("database")?)?;
            builder.with_named_option(databricks::HTTP_PATH, http_path)?;

            // FIXME: dbt-databricks historically has allowed garbage in the auth_type field and only responds to
            // auth_type 'oauth'. Everything else means token
            match DatabricksAuthType::from_config(config) {
                Ok(DatabricksAuthType::OAuth) => {
                    // OAuth authentication: M2M if client_secret present, otherwise External Browser (U2M)
                    if let Some(client_secret) = config.get_string("client_secret") {
                        // M2M
                        builder.with_named_option(
                            databricks::CLIENT_ID,
                            config.require_string("client_id")?,
                        )?;
                        builder.with_named_option(databricks::CLIENT_SECRET, client_secret)?;
                        builder.with_named_option(
                            databricks::AUTH_TYPE,
                            databricks::auth_type::OAUTH_M2M,
                        )?;
                    } else {
                        // U2M
                        if let Some(client_id) = config.get_string("client_id") {
                            builder.with_named_option(databricks::CLIENT_ID, client_id)?;
                        }
                        builder.with_named_option(
                            databricks::AUTH_TYPE,
                            databricks::auth_type::EXTERNAL_BROWSER,
                        )?;
                    }
                }
                Ok(DatabricksAuthType::Token) | Err(_) => {
                    // Token (default for unknown auth_type values)
                    builder
                        .with_named_option(databricks::TOKEN, config.require_string("token")?)?;
                    builder.with_named_option(databricks::AUTH_TYPE, databricks::auth_type::PAT)?;
                }
            }
        }
        Ok(builder)
    }
}

fn resolve_http_path(config: &AdapterConfig) -> Result<Cow<'_, str>, AuthError> {
    let mut http_path = config.require_string("http_path")?;
    let databricks_compute_config = config.get_string("databricks_compute");

    if let Some(databricks_compute) = databricks_compute_config {
        let compute = config.require("compute")?;

        if let Value::Mapping(map, ..) = compute
            && let Some((_, Value::Mapping(compute_map, ..))) = map
                .iter()
                .find(|(k, _)| matches!(k, Value::String(s, _) if *s == databricks_compute))
            && let Some(Value::String(path, _)) = compute_map.iter().find_map(|(k, v)| {
                if let Value::String(key, _) = k
                    && key == "http_path"
                {
                    return Some(v);
                }
                None
            })
        {
            http_path = Cow::from(path);
            return Ok(http_path);
        }

        return Err(AuthError::Config(format!(
            "Compute resource '{databricks_compute}' does not exist or does not specify http_path"
        )));
    }

    Ok(http_path)
}

fn validate_config(config: &AdapterConfig) -> Result<(), AuthError> {
    if !config.contains_key("http_path") {
        return Err(AuthError::config("http_path is required"));
    }
    if !config.contains_key("host") {
        return Err(AuthError::config("host is required".to_string()));
    }
    // FIXME: auth_type validation is lenient - unknown values default to token auth
    let _ = DatabricksAuthType::from_config(config);
    if !config.contains_key("client_id") && config.contains_key("client_secret") {
        return Err(AuthError::config(
            "The config 'client_id' is required to connect to Databricks when 'client_secret' is present",
        ));
    }
    let azure_client_no_secret =
        !config.contains_key("azure_client_id") && config.contains_key("azure_client_secret");
    let azure_secret_no_client =
        config.contains_key("azure_client_id") && !config.contains_key("azure_client_secret");
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
    use crate::test_options::option_str_value;
    use adbc_core::options::OptionDatabase;
    use dbt_serde_yaml::Mapping;

    fn run_config_test(config: Mapping, expected: &[(&str, &str)]) -> Result<(), AuthError> {
        let auth = DatabricksAuth {};
        let builder = auth.configure(&AdapterConfig::new(config))?;
        assert_eq!(builder.clone().into_iter().count(), expected.len());

        let mut results = Mapping::default();
        for (k, v) in builder.into_iter() {
            let key = match k {
                OptionDatabase::Username => "user".to_owned(),
                OptionDatabase::Password => "password".to_owned(),
                OptionDatabase::Other(name) => name.to_owned(),
                _ => continue,
            };
            results.insert(key.into(), option_str_value(&v).into());
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
        let config = Mapping::from_iter([
            ("host".into(), "H".into()),
            ("schema".into(), "S".into()),
            (
                "http_path".into(),
                "/sql/1.0/warehouses/warehouse-id".into(),
            ),
            ("token".into(), "T".into()),
            ("database".into(), "C".into()),
        ]);

        let expected = vec![
            (databricks::TOKEN, "T"),
            (databricks::SCHEMA, "S"),
            (databricks::HOST, "H"),
            (databricks::HTTP_PATH, "/sql/1.0/warehouses/warehouse-id"),
            (databricks::CATALOG, "C"),
            (databricks::USER_AGENT, USER_AGENT_NAME),
            (databricks::AUTH_TYPE, databricks::auth_type::PAT),
        ];
        run_config_test(config, &expected).unwrap();
    }

    #[test]
    fn test_token_cluster_with_optional_fields() {
        let config = Mapping::from_iter([
            ("host".into(), "H".into()),
            ("schema".into(), "S".into()),
            (
                "http_path".into(),
                "sql/protocolv1/o/1030i40i30i50i3/my-cluster-id".into(),
            ),
            ("token".into(), "T".into()),
            ("database".into(), "C".into()),
        ]);

        let expected = vec![
            (databricks::TOKEN, "T"),
            (databricks::SCHEMA, "S"),
            (databricks::HOST, "H"),
            (
                databricks::HTTP_PATH,
                "sql/protocolv1/o/1030i40i30i50i3/my-cluster-id",
            ),
            (databricks::CATALOG, "C"),
            (databricks::USER_AGENT, USER_AGENT_NAME),
            (databricks::AUTH_TYPE, databricks::auth_type::PAT),
        ];
        run_config_test(config, &expected).unwrap();
    }

    #[test]
    fn test_m2m_oauth() {
        let config = Mapping::from_iter([
            ("host".into(), "H".into()),
            ("schema".into(), "S".into()),
            (
                "http_path".into(),
                "sql/protocolv1/o/1030i40i30i50i3/my-cluster-id".into(),
            ),
            ("client_id".into(), "O".into()),
            ("client_secret".into(), "O".into()),
            ("database".into(), "C".into()),
            ("auth_type".into(), "oauth".into()),
        ]);

        let expected = vec![
            (databricks::CLIENT_ID, "O"),
            (databricks::CLIENT_SECRET, "O"),
            (databricks::SCHEMA, "S"),
            (databricks::HOST, "H"),
            (
                databricks::HTTP_PATH,
                "sql/protocolv1/o/1030i40i30i50i3/my-cluster-id",
            ),
            (databricks::CATALOG, "C"),
            (databricks::USER_AGENT, USER_AGENT_NAME),
            (databricks::AUTH_TYPE, databricks::auth_type::OAUTH_M2M),
        ];
        run_config_test(config, &expected).unwrap();
    }

    /// Test that M2M OAuth is used when auth_type is "oauth" even if a token field exists.
    /// This handles the case where a user has a leftover token from a previous PAT configuration.
    #[test]
    fn test_m2m_oauth_with_leftover_token() {
        let config = Mapping::from_iter([
            ("host".into(), "H".into()),
            ("schema".into(), "S".into()),
            (
                "http_path".into(),
                "sql/protocolv1/o/1030i40i30i50i3/my-cluster-id".into(),
            ),
            ("client_id".into(), "CLIENT_ID".into()),
            ("client_secret".into(), "CLIENT_SECRET".into()),
            ("database".into(), "C".into()),
            ("auth_type".into(), "oauth".into()),
            ("token".into(), "LEFTOVER_TOKEN".into()), // Should be ignored
        ]);

        let expected = vec![
            (databricks::CLIENT_ID, "CLIENT_ID"),
            (databricks::CLIENT_SECRET, "CLIENT_SECRET"),
            (databricks::SCHEMA, "S"),
            (databricks::HOST, "H"),
            (
                databricks::HTTP_PATH,
                "sql/protocolv1/o/1030i40i30i50i3/my-cluster-id",
            ),
            (databricks::CATALOG, "C"),
            (databricks::USER_AGENT, USER_AGENT_NAME),
            (databricks::AUTH_TYPE, databricks::auth_type::OAUTH_M2M),
        ];
        run_config_test(config, &expected).unwrap();
    }

    #[test]
    fn test_external_browser_oauth() {
        let config = Mapping::from_iter([
            ("host".into(), "H".into()),
            ("schema".into(), "S".into()),
            (
                "http_path".into(),
                "sql/protocolv1/o/1030i40i30i50i3/my-cluster-id".into(),
            ),
            ("client_id".into(), "O".into()),
            ("database".into(), "C".into()),
            ("auth_type".into(), "oauth".into()),
        ]);
        let expected = vec![
            (databricks::SCHEMA, "S"),
            (databricks::HOST, "H"),
            (
                databricks::HTTP_PATH,
                "sql/protocolv1/o/1030i40i30i50i3/my-cluster-id",
            ),
            (databricks::CATALOG, "C"),
            (databricks::CLIENT_ID, "O"),
            (databricks::USER_AGENT, USER_AGENT_NAME),
            (
                databricks::AUTH_TYPE,
                databricks::auth_type::EXTERNAL_BROWSER,
            ),
        ];
        run_config_test(config, &expected).unwrap();
    }

    #[test]
    fn test_external_browser_oauth_without_client_id() {
        let config = Mapping::from_iter([
            ("host".into(), "H".into()),
            ("schema".into(), "S".into()),
            (
                "http_path".into(),
                "sql/protocolv1/o/1030i40i30i50i3/my-cluster-id".into(),
            ),
            ("database".into(), "C".into()),
            ("auth_type".into(), "oauth".into()),
        ]);
        let expected = vec![
            (databricks::SCHEMA, "S"),
            (databricks::HOST, "H"),
            (
                databricks::HTTP_PATH,
                "sql/protocolv1/o/1030i40i30i50i3/my-cluster-id",
            ),
            (databricks::CATALOG, "C"),
            (databricks::USER_AGENT, USER_AGENT_NAME),
            (
                databricks::AUTH_TYPE,
                databricks::auth_type::EXTERNAL_BROWSER,
            ),
        ];
        run_config_test(config, &expected).unwrap();
    }

    /// Test that External Browser OAuth is used when auth_type is "oauth" even if a token field exists.
    /// This handles the case where a user has a leftover token from a previous PAT configuration.
    #[test]
    fn test_external_browser_oauth_with_leftover_token() {
        let config = Mapping::from_iter([
            ("host".into(), "H".into()),
            ("schema".into(), "S".into()),
            (
                "http_path".into(),
                "sql/protocolv1/o/1030i40i30i50i3/my-cluster-id".into(),
            ),
            ("client_id".into(), "CLIENT_ID".into()),
            ("database".into(), "C".into()),
            ("auth_type".into(), "oauth".into()),
            ("token".into(), "LEFTOVER_TOKEN".into()), // Should be ignored
        ]);
        let expected = vec![
            (databricks::SCHEMA, "S"),
            (databricks::HOST, "H"),
            (
                databricks::HTTP_PATH,
                "sql/protocolv1/o/1030i40i30i50i3/my-cluster-id",
            ),
            (databricks::CATALOG, "C"),
            (databricks::CLIENT_ID, "CLIENT_ID"),
            (databricks::USER_AGENT, USER_AGENT_NAME),
            (
                databricks::AUTH_TYPE,
                databricks::auth_type::EXTERNAL_BROWSER,
            ),
        ];
        run_config_test(config, &expected).unwrap();
    }

    #[test]
    fn test_unknown_auth_type_defaults_to_token() {
        // Unknown auth_type values default to token authentication
        let config = Mapping::from_iter([
            ("host".into(), "H".into()),
            (
                "http_path".into(),
                "sql/protocolv1/o/1030i40i30i50i3/my-cluster-id".into(),
            ),
            ("schema".into(), "S".into()),
            ("database".into(), "C".into()),
            ("token".into(), "T".into()),
            ("auth_type".into(), "external_browser".into()), // Unknown value, defaults to token
        ]);
        let expected = vec![
            (databricks::TOKEN, "T"),
            (databricks::SCHEMA, "S"),
            (databricks::HOST, "H"),
            (
                databricks::HTTP_PATH,
                "sql/protocolv1/o/1030i40i30i50i3/my-cluster-id",
            ),
            (databricks::CATALOG, "C"),
            (databricks::USER_AGENT, USER_AGENT_NAME),
            (databricks::AUTH_TYPE, databricks::auth_type::PAT),
        ];
        run_config_test(config, &expected).unwrap();
    }

    #[test]
    fn test_unknown_auth_type_pat_defaults_to_token() {
        // "pat" is not a recognized auth_type value, but defaults to token auth
        let config = Mapping::from_iter([
            ("host".into(), "H".into()),
            (
                "http_path".into(),
                "sql/protocolv1/o/1030i40i30i50i3/my-cluster-id".into(),
            ),
            ("schema".into(), "S".into()),
            ("database".into(), "C".into()),
            ("token".into(), "T".into()),
            ("auth_type".into(), "pat".into()), // Unknown value, defaults to token
        ]);
        let expected = vec![
            (databricks::TOKEN, "T"),
            (databricks::SCHEMA, "S"),
            (databricks::HOST, "H"),
            (
                databricks::HTTP_PATH,
                "sql/protocolv1/o/1030i40i30i50i3/my-cluster-id",
            ),
            (databricks::CATALOG, "C"),
            (databricks::USER_AGENT, USER_AGENT_NAME),
            (databricks::AUTH_TYPE, databricks::auth_type::PAT),
        ];
        run_config_test(config, &expected).unwrap();
    }

    #[test]
    fn test_explicit_token_auth_type() {
        // Test that auth_type: "token" works the same as omitting auth_type
        let config = Mapping::from_iter([
            ("host".into(), "H".into()),
            ("schema".into(), "S".into()),
            (
                "http_path".into(),
                "/sql/1.0/warehouses/warehouse-id".into(),
            ),
            ("token".into(), "T".into()),
            ("database".into(), "C".into()),
            ("auth_type".into(), "token".into()),
        ]);

        let expected = vec![
            (databricks::TOKEN, "T"),
            (databricks::SCHEMA, "S"),
            (databricks::HOST, "H"),
            (databricks::HTTP_PATH, "/sql/1.0/warehouses/warehouse-id"),
            (databricks::CATALOG, "C"),
            (databricks::USER_AGENT, USER_AGENT_NAME),
            (databricks::AUTH_TYPE, databricks::auth_type::PAT),
        ];
        run_config_test(config, &expected).unwrap();
    }

    #[test]
    fn test_validate_config_errors_with_missing_client_id_and_present_client_secret() {
        let config = Mapping::from_iter([
            ("host".into(), "H".into()),
            (
                "http_path".into(),
                "sql/protocolv1/o/1030i40i30i50i3/my-cluster-id".into(),
            ),
            ("schema".into(), "S".into()),
            ("database".into(), "C".into()),
            ("client_secret".into(), "some_secret".into()),
            ("auth_type".into(), "oauth".into()),
        ]);
        let result = validate_config(&AdapterConfig::new(config));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().msg(),
            "The config 'client_id' is required to connect to Databricks when 'client_secret' is present"
        );
    }

    #[test]
    fn test_validate_config_errors_with_missing_azure_client_id_and_present_azure_client_secret() {
        let config = Mapping::from_iter([
            ("host".into(), "H".into()),
            (
                "http_path".into(),
                "sql/protocolv1/o/1030i40i30i50i3/my-cluster-id".into(),
            ),
            ("schema".into(), "S".into()),
            ("database".into(), "C".into()),
            ("azure_client_secret".into(), "some_secret".into()),
            ("auth_type".into(), "oauth".into()),
        ]);
        let result = validate_config(&AdapterConfig::new(config));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().msg(),
            "The config 'azure_client_id' and 'azure_client_secret' must be both present or both absent"
        );
    }

    #[test]
    fn test_validate_config_errors_with_present_azure_client_id_and_missing_azure_client_secret() {
        let config = Mapping::from_iter([
            ("host".into(), "H".into()),
            (
                "http_path".into(),
                "sql/protocolv1/o/1030i40i30i50i3/my-cluster-id".into(),
            ),
            ("schema".into(), "S".into()),
            ("database".into(), "C".into()),
            ("azure_client_id".into(), "some_id".into()),
            ("auth_type".into(), "oauth".into()),
        ]);
        let result = validate_config(&AdapterConfig::new(config));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().msg(),
            "The config 'azure_client_id' and 'azure_client_secret' must be both present or both absent"
        );
    }

    #[test]
    fn test_resolve_http_path_no_extra_params() {
        let mapping = Mapping::from_iter([("http_path".into(), "/sql/extra/warehouse".into())]);
        let config = AdapterConfig::new(mapping);

        let path = resolve_http_path(&config).expect("expected to resolve http_path from config");

        assert_eq!(path, "/sql/extra/warehouse");
    }

    #[test]
    fn test_resolve_http_path_uses_databricks_compute() {
        let compute1_config =
            Mapping::from_iter([("http_path".into(), "/sql/warehouse/specific_compute".into())]);
        let compute_config = Mapping::from_iter([("compute1".into(), compute1_config.into())]);
        let mapping = Mapping::from_iter([
            ("http_path".into(), "/sql/config/warehouse".into()),
            ("compute".into(), compute_config.into()),
            ("databricks_compute".into(), "compute1".into()),
        ]);
        let config = AdapterConfig::new(mapping);

        let path = resolve_http_path(&config)
            .expect("expected to resolve http_path from databricks compute config");

        assert_eq!(path, "/sql/warehouse/specific_compute");
    }

    #[test]
    fn test_resolve_http_config_missing_errors() {
        let compute_config =
            Mapping::from_iter([("compute1".into(), "/sql/warehouse/specific_compute".into())]);
        let mapping = Mapping::from_iter([
            ("http_path".into(), "/sql/config/warehouse".into()),
            ("compute".into(), compute_config.into()),
            ("databricks_compute".into(), "compute2".into()),
        ]);
        let config = AdapterConfig::new(mapping);

        let result = resolve_http_path(&config);

        assert!(
            result.is_err(),
            "expected an error when http_path is missing"
        );
    }
}
