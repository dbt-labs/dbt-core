use crate::{AdapterConfig, Auth, AuthError};
use database::Builder as DatabaseBuilder;
use dbt_yaml::Value;
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
#[cfg(not(feature = "odbc"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DatabricksAuthType {
    /// OAuth authentication
    OAuth,
    /// Personal Access Token
    Token,
}

#[cfg(not(feature = "odbc"))]
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

#[cfg(not(feature = "odbc"))]
#[derive(Debug)]
enum DatabricksAuthIR<'a> {
    OAuthM2M {
        client_id: &'a str,
        client_secret: &'a str,
    },
    ExternalBrowserOAuth {
        client_id: Option<&'a str>,
    },
    Token {
        token: &'a str,
    },
}

#[cfg(not(feature = "odbc"))]
impl<'a> DatabricksAuthIR<'a> {
    pub fn apply(self, mut builder: DatabaseBuilder) -> Result<DatabaseBuilder, AuthError> {
        match self {
            Self::OAuthM2M {
                client_id,
                client_secret,
            } => {
                builder.with_named_option(databricks::CLIENT_ID, client_id)?;
                builder.with_named_option(databricks::CLIENT_SECRET, client_secret)?;
                builder
                    .with_named_option(databricks::AUTH_TYPE, databricks::auth_type::OAUTH_M2M)?;
            }
            Self::ExternalBrowserOAuth { client_id } => {
                if let Some(client_id) = client_id {
                    builder.with_named_option(databricks::CLIENT_ID, client_id)?;
                }
                builder.with_named_option(
                    databricks::AUTH_TYPE,
                    databricks::auth_type::EXTERNAL_BROWSER,
                )?;
            }
            Self::Token { token } => {
                builder.with_named_option(databricks::TOKEN, token)?;
                builder.with_named_option(databricks::AUTH_TYPE, databricks::auth_type::PAT)?;
            }
        }

        Ok(builder)
    }
}

#[cfg(not(feature = "odbc"))]
fn parse_auth<'a>(config: &'a AdapterConfig) -> Result<DatabricksAuthIR<'a>, AuthError> {
    // FIXME: dbt-databricks historically has allowed garbage in the auth_type field and only responds to
    // auth_type 'oauth'. Everything else means token
    match DatabricksAuthType::from_config(config) {
        Ok(DatabricksAuthType::OAuth) => {
            // OAuth authentication: M2M if client_secret present, otherwise External Browser (U2M)
            if config.contains_key("client_secret") {
                Ok(DatabricksAuthIR::OAuthM2M {
                    client_id: config.require_str("client_id")?,
                    client_secret: config.require_str("client_secret")?,
                })
            } else {
                Ok(DatabricksAuthIR::ExternalBrowserOAuth {
                    client_id: if config.contains_key("client_id") {
                        Some(config.require_str("client_id")?)
                    } else {
                        None
                    },
                })
            }
        }
        Ok(DatabricksAuthType::Token) | Err(_) => Ok(DatabricksAuthIR::Token {
            token: config.require_str("token")?,
        }),
    }
}

#[cfg(not(feature = "odbc"))]
fn apply_connection_args(
    config: &AdapterConfig,
    mut builder: DatabaseBuilder,
) -> Result<DatabaseBuilder, AuthError> {
    let http_path = resolve_http_path(config)?;

    validate_config(config)?;

    // all of the following options are required for any Databricks connection
    builder.with_named_option(databricks::USER_AGENT, USER_AGENT_NAME)?;
    builder.with_named_option(databricks::HOST, config.require_string("host")?)?;
    builder.with_named_option(databricks::SCHEMA, config.require_string("schema")?)?;
    builder.with_named_option(databricks::CATALOG, config.require_string("database")?)?;
    builder.with_named_option(databricks::HTTP_PATH, http_path)?;

    Ok(builder)
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
        #[cfg(feature = "odbc")]
        {
            configure_odbc(self.backend(), config)
        }

        #[cfg(not(feature = "odbc"))]
        {
            crate::auth_configure_pipeline!(
                self.backend(),
                &config,
                parse_auth,
                apply_connection_args
            )
        }
    }
}

#[cfg(feature = "odbc")]
fn configure_odbc(
    backend: Backend,
    config: &AdapterConfig,
) -> Result<database::Builder, AuthError> {
    let http_path = resolve_http_path(config)?;

    let mut builder = DatabaseBuilder::new(backend);
    builder.with_named_option(databricks::USER_AGENT, USER_AGENT_NAME)?;

    use databricks::odbc;
    // Config values for DSN-less connection to Databricks:
    // https://learn.microsoft.com/en-us/azure/databricks/integrations/odbc/authentication
    for key in ["token", "http_path", "host", "schema", "database"].into_iter() {
        if let Some(value) = config.get_string(key) {
            match key {
                "token" => builder.with_named_option(odbc::TOKEN_FIELD, value),
                "http_path" => builder.with_named_option(odbc::HTTP_PATH, http_path.clone()),
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

    Ok(builder)
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

#[cfg(not(feature = "odbc"))]
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

#[cfg(all(test, not(feature = "odbc")))]
mod tests {
    use super::*;
    use crate::test_options::other_option_value;
    use dbt_yaml::Mapping;
    use dbt_yaml::Value as YmlValue;

    fn base_config() -> Mapping {
        Mapping::from_iter([
            ("host".into(), "H".into()),
            ("schema".into(), "S".into()),
            ("database".into(), "C".into()),
            (
                "http_path".into(),
                "/sql/1.0/warehouses/warehouse-id".into(),
            ),
        ])
    }

    fn run_config_test(config: Mapping, expected: &[(&str, &str)]) -> Result<(), AuthError> {
        let auth = DatabricksAuth {};
        let builder = auth.configure(&AdapterConfig::new(config))?;
        assert_eq!(builder.clone().into_iter().count(), expected.len());

        for &(key, expected_val) in expected {
            assert_eq!(
                other_option_value(&builder, key).unwrap_or_else(|| panic!("Missing key: {key}")),
                expected_val,
                "Value mismatch for key: {key}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_token_warehouse() {
        let mut config = base_config();
        config.insert("token".into(), "T".into());

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
    fn test_oauth_fields_do_not_enable_oauth_without_auth_type() {
        // Without auth_type: oauth, we should still default to token auth even if oauth fields exist.
        let mut config = base_config();
        config.insert("token".into(), "T".into());
        config.insert("client_id".into(), "O".into());
        config.insert("client_secret".into(), "O".into());

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
        let mut config = base_config();
        config.insert(
            "http_path".into(),
            "sql/protocolv1/o/1030i40i30i50i3/my-cluster-id".into(),
        );
        config.insert("client_id".into(), "O".into());
        config.insert("client_secret".into(), "O".into());
        config.insert("auth_type".into(), "oauth".into());

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
        let mut config = base_config();
        config.insert(
            "http_path".into(),
            "sql/protocolv1/o/1030i40i30i50i3/my-cluster-id".into(),
        );
        config.insert("client_id".into(), "CLIENT_ID".into());
        config.insert("client_secret".into(), "CLIENT_SECRET".into());
        config.insert("auth_type".into(), "oauth".into());
        config.insert("token".into(), "LEFTOVER_TOKEN".into()); // Should be ignored

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
        let mut config = base_config();
        config.insert(
            "http_path".into(),
            "sql/protocolv1/o/1030i40i30i50i3/my-cluster-id".into(),
        );
        config.insert("client_id".into(), "O".into());
        config.insert("auth_type".into(), "oauth".into());
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
        let mut config = base_config();
        config.insert(
            "http_path".into(),
            "sql/protocolv1/o/1030i40i30i50i3/my-cluster-id".into(),
        );
        config.insert("auth_type".into(), "oauth".into());
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
        let mut config = base_config();
        config.insert(
            "http_path".into(),
            "sql/protocolv1/o/1030i40i30i50i3/my-cluster-id".into(),
        );
        config.insert("client_id".into(), "CLIENT_ID".into());
        config.insert("auth_type".into(), "oauth".into());
        config.insert("token".into(), "LEFTOVER_TOKEN".into()); // Should be ignored
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
        for auth_type in ["external_browser", "pat"] {
            let mut config = base_config();
            config.insert(
                "http_path".into(),
                "sql/protocolv1/o/1030i40i30i50i3/my-cluster-id".into(),
            );
            config.insert("token".into(), "T".into());
            config.insert("auth_type".into(), auth_type.into());

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
    }

    #[test]
    fn test_explicit_token_auth_type() {
        // Test that auth_type: "token" works the same as omitting auth_type
        let mut config = base_config();
        config.insert("token".into(), "T".into());
        config.insert("auth_type".into(), "token".into());

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
    fn test_missing_host_is_config_error() {
        let config = Mapping::from_iter([
            ("schema".into(), "S".into()),
            ("database".into(), "C".into()),
            (
                "http_path".into(),
                "/sql/1.0/warehouses/warehouse-id".into(),
            ),
            ("token".into(), "T".into()),
        ]);

        let err = DatabricksAuth {}
            .configure(&AdapterConfig::new(config))
            .expect_err("configure should fail");
        assert_eq!(err.msg(), "host is required");
    }

    #[test]
    fn test_missing_http_path_is_yaml_error() {
        let config = Mapping::from_iter([
            ("host".into(), "H".into()),
            ("schema".into(), "S".into()),
            ("database".into(), "C".into()),
            ("token".into(), "T".into()),
        ]);

        let err = DatabricksAuth {}
            .configure(&AdapterConfig::new(config))
            .expect_err("configure should fail");

        match err {
            AuthError::YAML(e) => assert!(e.to_string().contains("missing field `http_path`")),
            other => panic!("expected YAML missing-field error, got {other:?}"),
        }
    }

    #[test]
    fn test_missing_schema_is_yaml_error() {
        for (missing_key, expected_msg) in [
            ("schema", "missing field `schema`"),
            ("database", "missing field `database`"),
            ("token", "missing field `token`"),
        ] {
            let mut config = base_config();
            config.insert("token".into(), "T".into());
            config.remove(missing_key);

            let err = DatabricksAuth {}
                .configure(&AdapterConfig::new(config))
                .expect_err("configure should fail");

            match err {
                AuthError::YAML(e) => assert!(e.to_string().contains(expected_msg)),
                other => panic!("expected YAML missing-field error, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_parse_auth_oauth_client_secret_non_string_is_yaml_error() {
        let config = Mapping::from_iter([
            ("auth_type".into(), "oauth".into()),
            ("client_id".into(), "CID".into()),
            ("client_secret".into(), YmlValue::number(1i64.into())),
        ]);

        let err = parse_auth(&AdapterConfig::new(config)).expect_err("expected parse_auth error");
        match err {
            AuthError::YAML(e) => assert!(e.to_string().contains("missing field `client_secret`")),
            other => panic!("expected YAML missing-field error, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_auth_oauth_client_id_non_string_is_yaml_error() {
        let config = Mapping::from_iter([
            ("auth_type".into(), "oauth".into()),
            ("client_id".into(), YmlValue::bool(true)),
        ]);

        let err = parse_auth(&AdapterConfig::new(config)).expect_err("expected parse_auth error");
        match err {
            AuthError::YAML(e) => assert!(e.to_string().contains("missing field `client_id`")),
            other => panic!("expected YAML missing-field error, got {other:?}"),
        }
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
    fn test_resolve_http_path_compute_missing_http_path_errors() {
        let compute1_config = Mapping::from_iter([("name".into(), "compute1".into())]);
        let compute_config = Mapping::from_iter([("compute1".into(), compute1_config.into())]);
        let mapping = Mapping::from_iter([
            ("http_path".into(), "/sql/config/warehouse".into()),
            ("compute".into(), compute_config.into()),
            ("databricks_compute".into(), "compute1".into()),
        ]);
        let config = AdapterConfig::new(mapping);

        let err = resolve_http_path(&config).expect_err("expected missing http_path error");
        assert_eq!(
            err.msg(),
            "Compute resource 'compute1' does not exist or does not specify http_path"
        );
    }

    #[test]
    fn test_resolve_http_path_compute_wrong_shape_errors() {
        let compute_config = Mapping::from_iter([("compute1".into(), "not-a-mapping".into())]);
        let mapping = Mapping::from_iter([
            ("http_path".into(), "/sql/config/warehouse".into()),
            ("compute".into(), compute_config.into()),
            ("databricks_compute".into(), "compute1".into()),
        ]);
        let config = AdapterConfig::new(mapping);

        let err = resolve_http_path(&config).expect_err("expected compute shape error");
        assert_eq!(
            err.msg(),
            "Compute resource 'compute1' does not exist or does not specify http_path"
        );
    }

    #[test]
    fn test_resolve_http_path_compute_http_path_non_string_errors() {
        let compute1_config =
            Mapping::from_iter([("http_path".into(), YmlValue::number(1i64.into()))]);
        let compute_config = Mapping::from_iter([("compute1".into(), compute1_config.into())]);
        let mapping = Mapping::from_iter([
            ("http_path".into(), "/sql/config/warehouse".into()),
            ("compute".into(), compute_config.into()),
            ("databricks_compute".into(), "compute1".into()),
        ]);
        let config = AdapterConfig::new(mapping);

        let err = resolve_http_path(&config).expect_err("expected http_path type error");
        assert_eq!(
            err.msg(),
            "Compute resource 'compute1' does not exist or does not specify http_path"
        );
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

#[cfg(all(test, feature = "odbc"))]
mod odbc_tests {
    use super::*;
    use crate::test_options::other_option_value;
    use dbt_yaml::Mapping;

    #[test]
    fn test_odbc_minimal_token_connection_options_present() {
        use databricks::odbc;

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

        let builder = DatabricksAuth {}
            .configure(&AdapterConfig::new(config))
            .expect("configure");

        assert_eq!(
            other_option_value(&builder, databricks::USER_AGENT),
            Some(USER_AGENT_NAME)
        );
        assert_eq!(other_option_value(&builder, odbc::TOKEN_FIELD), Some("T"));
        assert_eq!(other_option_value(&builder, odbc::HOST), Some("H"));
        assert_eq!(other_option_value(&builder, odbc::SCHEMA), Some("S"));
        assert_eq!(other_option_value(&builder, odbc::CATALOG), Some("C"));
        assert_eq!(
            other_option_value(&builder, odbc::HTTP_PATH),
            Some("/sql/1.0/warehouses/warehouse-id")
        );
    }
}
