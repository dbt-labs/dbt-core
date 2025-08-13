mod key_format;

use crate::{AdapterConfig, Auth, AuthError};
use database::Builder as DatabaseBuilder;
use dbt_xdbc::database::LogLevel;
use dbt_xdbc::{Backend, database, snowflake};

use std::fs;

const APP_NAME: &str = "dbt";

// WARNING: Still needs adjustment on what is considered must-have
const REQUIRED_PARAMS: [&str; 5] = ["user", "password", "account", "role", "warehouse"];

const DEFAULT_CONNECT_TIMEOUT: &str = "10s";

/// dbt Core expects durations in seconds only so this utility appends that s
/// https://pkg.go.dev/time#ParseDuration for permitted units
fn postfix_seconds_unit(value: &str) -> String {
    format!("{value}s")
}

trait ConfigureBuilder {
    fn configure(self, builder: DatabaseBuilder) -> Result<DatabaseBuilder, AuthError>;
    fn check_authenticator_field(config: &AdapterConfig) -> Result<(), AuthError> {
        if config.maybe_get_str("authenticator")?.is_some() {
            Err(AuthError::config(
                "Profile does not need an authenticator. Use method field instead.",
            ))
        } else {
            Ok(())
        }
    }
}

/// Get Snowflake private key by path or from a Base64 encoded DER bytestring
enum PrivateKeySource {
    Literal(String),
    FilePath(String),
}

#[derive(Debug)]
struct Keypair {
    private_key_path: Option<String>,
    private_key: Option<String>,
    private_key_passphrase: Option<String>,
}

impl Keypair {
    fn new(config: &AdapterConfig) -> Result<Self, AuthError> {
        Self::check_authenticator_field(config)?;
        Ok(Keypair {
            private_key_path: config.maybe_get_str("private_key_path")?,
            private_key: config.maybe_get_str("private_key")?,
            private_key_passphrase: config.maybe_get_str("private_key_passphrase")?,
        })
    }

    fn build_keypair_parameter_key_value_pairs(
        &self,
        source: PrivateKeySource,
        passphrase: Option<String>,
    ) -> Result<Vec<(&'static str, String)>, AuthError> {
        let mut pairs = vec![(snowflake::AUTH_TYPE, snowflake::auth_type::JWT.to_owned())];
        match source {
            PrivateKeySource::Literal(ref key) => {
                pairs.push((
                    snowflake::JWT_PRIVATE_KEY_PKCS8_VALUE,
                    key_format::normalize_key(key)?,
                ));
                if let Some(pass) = passphrase {
                    pairs.push((snowflake::JWT_PRIVATE_KEY_PKCS8_PASSWORD, pass));
                }
            }
            PrivateKeySource::FilePath(path) => {
                if let Some(pass) = passphrase {
                    let key = fs::read_to_string(path)?;
                    pairs.push((
                        snowflake::JWT_PRIVATE_KEY_PKCS8_VALUE,
                        key_format::normalize_key(&key)?,
                    ));
                    pairs.push((snowflake::JWT_PRIVATE_KEY_PKCS8_PASSWORD, pass));
                } else {
                    pairs.push((snowflake::JWT_PRIVATE_KEY, path));
                }
            }
        }
        Ok(pairs)
    }
}

impl ConfigureBuilder for Keypair {
    fn configure(self, builder: DatabaseBuilder) -> Result<DatabaseBuilder, AuthError> {
        let mut builder = builder;
        let source = match (self.private_key_path.as_ref(), self.private_key.as_ref()) {
            (Some(_), Some(_)) => Err(AuthError::config(
                "Cannot specify both 'private_key' and 'private_key_path'",
            )),
            (Some(path), None) => Ok(PrivateKeySource::FilePath(path.clone())),
            (None, Some(key)) => Ok(PrivateKeySource::Literal(key.clone())),
            (None, None) => Err(AuthError::config(
                "Keypair authentication requires exactly one of 'private_key' or 'private_key_path'",
            )),
        }?;

        for (key, value) in self
            .build_keypair_parameter_key_value_pairs(source, self.private_key_passphrase.clone())?
        {
            builder.with_named_option(key, value)?;
        }
        Ok(builder)
    }
}

#[derive(Debug)]
struct NativeOauth {
    client_id: String,
    client_secret: String,
    refresh_token: String,
}

impl NativeOauth {
    fn new(config: &AdapterConfig) -> Result<Self, AuthError> {
        Self::check_authenticator_field(config)?;

        if config.maybe_get_str("token")?.is_some() {
            return Err(AuthError::config(
                "Rename 'token' to 'refresh_token' in profile for 'method: snowflake_oauth'.",
            ));
        };

        match (
            config.maybe_get_str("oauth_client_id")?,
            config.maybe_get_str("oauth_client_secret")?,
            config.maybe_get_str("refresh_token")?,
        ) {
            (Some(client_id), Some(client_secret), Some(refresh_token)) => Ok(NativeOauth {
                client_id,
                client_secret,
                refresh_token,
            }),
            _ => Err(AuthError::config(
                "Profile requires 'oauth_client_id', 'oauth_client_secret', and 'refresh_token' for method: snowflake_oauth.",
            )),
        }
    }
}

impl ConfigureBuilder for NativeOauth {
    fn configure(self, builder: DatabaseBuilder) -> Result<DatabaseBuilder, AuthError> {
        let mut builder = builder;
        builder.with_named_option(snowflake::AUTH_TYPE, snowflake::auth_type::OAUTH)?;
        builder.with_named_option(snowflake::CLIENT_ID, self.client_id)?;
        builder.with_named_option(snowflake::CLIENT_SECRET, self.client_secret)?;
        builder.with_named_option(snowflake::REFRESH_TOKEN, self.refresh_token)?;
        builder.with_named_option(snowflake::CLIENT_STORE_TEMP_CREDS, "true")?;
        Ok(builder)
    }
}

#[derive(Debug)]
struct NativeOauthJWT {
    jwt_token: String,
}

impl NativeOauthJWT {
    fn new(config: &AdapterConfig) -> Result<Self, AuthError> {
        Self::check_authenticator_field(config)?;

        if let Some(jwt_token) = config.maybe_get_str("jwt_token")? {
            Ok(NativeOauthJWT { jwt_token })
        } else {
            Err(AuthError::config(
                "Profile requires 'jwt_token' for 'method: snowflake_oauth_jwt'.",
            ))
        }
    }
}

impl ConfigureBuilder for NativeOauthJWT {
    fn configure(self, builder: DatabaseBuilder) -> Result<DatabaseBuilder, AuthError> {
        let mut builder = builder;
        builder.with_named_option(snowflake::AUTH_TYPE, snowflake::auth_type::OAUTH)?;
        builder.with_named_option(snowflake::AUTH_TOKEN, self.jwt_token)?;
        builder.with_named_option(snowflake::CLIENT_STORE_TEMP_CREDS, "true")?;
        Ok(builder)
    }
}

#[derive(Debug)]
struct Sso;

impl Sso {
    fn new(config: &AdapterConfig) -> Result<Self, AuthError> {
        Self::check_authenticator_field(config)?;
        Ok(Sso)
    }
}

impl ConfigureBuilder for Sso {
    fn configure(self, builder: DatabaseBuilder) -> Result<DatabaseBuilder, AuthError> {
        let mut builder = builder;
        builder.with_named_option(snowflake::AUTH_TYPE, snowflake::auth_type::EXTERNAL_BROWSER)?;
        builder.with_named_option(snowflake::CLIENT_STORE_TEMP_CREDS, "true")?;
        Ok(builder)
    }
}

#[derive(Debug)]
struct Warehouse;

impl Warehouse {
    fn new(config: &AdapterConfig) -> Result<Self, AuthError> {
        Self::check_authenticator_field(config)?;
        Ok(Warehouse)
    }
}

impl ConfigureBuilder for Warehouse {
    fn configure(self, builder: DatabaseBuilder) -> Result<DatabaseBuilder, AuthError> {
        Ok(builder) // user and password is part of required parameters
    }
}

#[derive(Debug)]
struct WarehouseMFA;

impl WarehouseMFA {
    fn new(config: &AdapterConfig) -> Result<Self, AuthError> {
        Self::check_authenticator_field(config)?;
        Ok(WarehouseMFA)
    }
}

impl ConfigureBuilder for WarehouseMFA {
    fn configure(self, builder: DatabaseBuilder) -> Result<DatabaseBuilder, AuthError> {
        let mut builder = builder;
        builder.with_named_option(
            snowflake::AUTH_TYPE,
            snowflake::auth_type::USERNAME_PASSWORD_MFA,
        )?;
        builder.with_named_option(snowflake::CLIENT_CACHE_MFA_TOKEN, "true")?;
        Ok(builder)
    }
}

#[derive(Debug)]
enum AuthMethod {
    Keypair(Keypair),
    Sso(Sso),
    NativeOauth(NativeOauth),
    NativeOauthJWT(NativeOauthJWT),
    Warehouse(Warehouse),
    WarehouseMFA(WarehouseMFA),
}

impl AuthMethod {
    pub fn new(config: &AdapterConfig, method: &str) -> Result<Self, AuthError> {
        match method {
            "keypair" => Keypair::new(config).map(Self::Keypair),
            "sso" => Sso::new(config).map(Self::Sso),
            "snowflake_oauth" => NativeOauth::new(config).map(Self::NativeOauth),
            "snowflake_oauth_jwt" => NativeOauthJWT::new(config).map(Self::NativeOauthJWT),
            "warehouse" => Warehouse::new(config).map(Self::Warehouse),
            "warehouse_mfa" => WarehouseMFA::new(config).map(Self::WarehouseMFA),
            unsupported_method => Err(AuthError::config(format!(
                "Profile has unsupported authentication method {unsupported_method}"
            ))),
        }
    }

    pub fn configure(self, builder: DatabaseBuilder) -> Result<DatabaseBuilder, AuthError> {
        match self {
            AuthMethod::Keypair(k) => k.configure(builder),
            AuthMethod::Sso(s) => s.configure(builder),
            AuthMethod::NativeOauth(o) => o.configure(builder),
            AuthMethod::NativeOauthJWT(j) => j.configure(builder),
            AuthMethod::Warehouse(w) => w.configure(builder),
            AuthMethod::WarehouseMFA(m) => m.configure(builder),
        }
    }
}

#[derive(Debug, Default)]
pub struct SnowflakeAuth;

impl SnowflakeAuth {
    /// For users who provide an explicit auth 'method' parameter in
    /// profiles.yml. This will unify dbt-snowflake with other
    /// existing 'perfect' adapters in FS.
    fn configure_builder_using_auth_option(
        &self,
        config: &AdapterConfig,
        method: String,
    ) -> Result<DatabaseBuilder, AuthError> {
        let mut builder = DatabaseBuilder::new(self.backend());

        for key in REQUIRED_PARAMS {
            if let Some(value) = config.maybe_get_str(key)? {
                match key {
                    "user" => Ok(builder.with_username(value)),
                    "password" => Ok(builder.with_password(value)),
                    "account" => builder.with_named_option(snowflake::ACCOUNT, value),
                    "database" => builder.with_named_option(snowflake::DATABASE, value),
                    // TODO: see if setting SCHEMA is necessary, connection cannot be established if schema doesn't exist
                    // this is a common case if we need to execute statements like `CREATE SCHEMA`
                    // "schema" => builder.with_named_option(snowflake::SCHEMA, value),
                    "role" => builder.with_named_option(snowflake::ROLE, value),
                    "warehouse" => builder.with_named_option(snowflake::WAREHOUSE, value),
                    _ => panic!("unexpected key: {key}"),
                }?;
            }
        }

        builder.with_named_option(snowflake::APPLICATION_NAME, APP_NAME)?;

        if let Some(s3_stage_vpce_dns_name) =
            config.maybe_get_str(snowflake::S3_STAGE_VPCE_DNS_NAME_PARAM_KEY)?
        {
            builder.with_named_option(
                snowflake::S3_STAGE_VPCE_DNS_NAME_PARAM_KEY,
                s3_stage_vpce_dns_name,
            )?;
        }

        let connect_timeout_duration = config
            .maybe_get_str("connect_timeout")?
            .as_deref()
            .map(postfix_seconds_unit)
            .unwrap_or_else(|| DEFAULT_CONNECT_TIMEOUT.to_string());
        builder.with_named_option(snowflake::LOGIN_TIMEOUT, connect_timeout_duration)?;

        AuthMethod::new(config, &method)?.configure(builder)
    }

    /// For backwards compatibility with Python dbt-snowflake
    /// implementation, which does not have an auth 'method' parameter
    /// in profiles.yml.
    fn configure_builder_without_auth_option(
        &self,
        config: &AdapterConfig,
    ) -> Result<DatabaseBuilder, AuthError> {
        let mut builder = DatabaseBuilder::new(self.backend());

        for key in [
            "user",
            "password",
            "account",
            "role",
            "warehouse",
            "private_key_path",
            "private_key",
            "private_key_passphrase",
            "authenticator",
            "oauth_client_id",
            "oauth_client_secret",
            "client_session_keep_alive",
            snowflake::S3_STAGE_VPCE_DNS_NAME_PARAM_KEY,
        ]
        .iter()
        {
            if let Some(value) = config.maybe_get_str(key)? {
                match *key {
                    "user" => Ok(builder.with_username(value)),
                    "password" => Ok(builder.with_password(value)),
                    "account" => builder.with_named_option(snowflake::ACCOUNT, value),
                    // TODO: see if setting SCHEMA is necessary, connection cannot be established if schema doesn't exist
                    // this is a common case if we need to execute statements like `CREATE SCHEMA` or `CREATE DATABASE`
                    "database" => builder.with_named_option(snowflake::DATABASE, value),
                    // "schema" => builder.with_named_option(snowflake::SCHEMA, value),
                    "role" => builder.with_named_option(snowflake::ROLE, value),
                    "warehouse" => builder.with_named_option(snowflake::WAREHOUSE, value),
                    "private_key_path" => {
                        builder
                            .with_named_option(snowflake::AUTH_TYPE, snowflake::auth_type::JWT)?;
                        // TODO: maybe it's safe to assume from a file we always get header and footer formatted private key
                        // the same for the same logics in `fn build_keypair_parameter_key_value_pairs`
                        let key_contents = fs::read_to_string(value)?;
                        builder.with_named_option(
                            snowflake::JWT_PRIVATE_KEY_PKCS8_VALUE,
                            &key_contents,
                        )
                    }
                    "private_key" => {
                        builder
                            .with_named_option(snowflake::AUTH_TYPE, snowflake::auth_type::JWT)?;
                        builder.with_named_option(
                            snowflake::JWT_PRIVATE_KEY_PKCS8_VALUE,
                            key_format::normalize_key(&value)?,
                        )
                    }
                    "private_key_passphrase" => {
                        builder.with_named_option(snowflake::JWT_PRIVATE_KEY_PKCS8_PASSWORD, value)
                    }
                    "client_session_keep_alive" => {
                        builder.with_named_option(snowflake::KEEP_SESSION_ALIVE, value)
                    }
                    "oauth_client_id" => builder.with_named_option(snowflake::CLIENT_ID, value),
                    "oauth_client_secret" => {
                        builder.with_named_option(snowflake::CLIENT_SECRET, value)
                    }
                    snowflake::S3_STAGE_VPCE_DNS_NAME_PARAM_KEY => builder
                        .with_named_option(snowflake::S3_STAGE_VPCE_DNS_NAME_PARAM_KEY, value),
                    "authenticator" => {
                        if value == "externalbrowser" {
                            builder
                                .with_named_option(snowflake::CLIENT_STORE_TEMP_CREDS, "true")?;
                            builder.with_named_option(
                                snowflake::AUTH_TYPE,
                                snowflake::auth_type::EXTERNAL_BROWSER,
                            )
                        } else if value == "oauth" {
                            if let Some(token) = config.maybe_get_str("token")? {
                                builder.with_named_option(snowflake::REFRESH_TOKEN, token)?;
                            } else {
                                Err(AuthError::config(
                                    "Field token: not found. Required for authenticator oauth.",
                                ))?
                            }
                            builder
                                .with_named_option(snowflake::CLIENT_STORE_TEMP_CREDS, "true")?;
                            builder.with_named_option(
                                snowflake::AUTH_TYPE,
                                snowflake::auth_type::OAUTH,
                            )
                        } else if value == "jwt" {
                            if let Some(token) = config.maybe_get_str("token")? {
                                builder.with_named_option(snowflake::AUTH_TOKEN, token)?;
                            } else {
                                Err(AuthError::config(
                                    "Field token: not found. Required for authenticator jwt.",
                                ))?
                            }
                            builder
                                .with_named_option(snowflake::CLIENT_STORE_TEMP_CREDS, "true")?;
                            builder.with_named_option(
                                snowflake::AUTH_TYPE,
                                snowflake::auth_type::OAUTH,
                            )
                        } else if value == "username_password_mfa" {
                            builder.with_named_option(
                                snowflake::AUTH_TYPE,
                                snowflake::auth_type::USERNAME_PASSWORD_MFA,
                            )?;
                            builder.with_named_option(snowflake::CLIENT_CACHE_MFA_TOKEN, "true")
                        } else {
                            Err(AuthError::config(format!(
                                "'{value}' for authenticator is not supported. If using authenticator, it must be set to exactly one of {{'externalbrowser', 'oauth', 'username_password_mfa'}}."
                            )))?
                        }
                    }
                    _ => panic!("unexpected key: {key}"),
                }?;
            }
        }

        // TODO: unified serde-based try_into for all auth methods, for now adhoc post-facto checks to
        //  reach dbt compliance
        if config.maybe_get_str("private_key_path")?.is_some()
            && config.maybe_get_str("private_key")?.is_some()
        {
            return Err(AuthError::config(
                "Cannot specify both `private_key` and `private_key_path`.".to_owned(),
            ));
        }

        let connect_timeout_duration = config
            .maybe_get_str("connect_timeout")?
            .as_deref()
            .map(postfix_seconds_unit)
            .unwrap_or_else(|| DEFAULT_CONNECT_TIMEOUT.to_string());
        builder.with_named_option(snowflake::LOGIN_TIMEOUT, connect_timeout_duration)?;

        builder.with_named_option(snowflake::APPLICATION_NAME, "dbt")?;
        Ok(builder)
    }
}

impl Auth for SnowflakeAuth {
    fn backend(&self) -> Backend {
        Backend::Snowflake
    }

    fn configure(&self, config: &AdapterConfig) -> Result<DatabaseBuilder, AuthError> {
        // TODO: can we unify configure_builder_without_auth_option and configure_builder_using_auth_option?
        // otherwise, we have to update certain logics more than 1 places
        let mut builder = match config.maybe_get_str("method")? {
            Some(method) => {
                SnowflakeAuth::configure_builder_using_auth_option(self, config, method)
            } // V2
            None => {
                SnowflakeAuth::configure_builder_without_auth_option(self, config)
                // V1 compatible
            }
        }?;
        // disable any logging from Gosnowflake that's not a fatal/panic
        builder.with_named_option(snowflake::LOG_TRACING, LogLevel::Fatal.to_string())?;
        Ok(builder)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::options::{OptionDatabase, OptionValue};
    use base64::{Engine, engine::general_purpose::STANDARD};
    use key_format::{
        PEM_ENCRYPTED_END, PEM_ENCRYPTED_START, PEM_UNENCRYPTED_END, PEM_UNENCRYPTED_START,
    };
    use pkcs8::EncodePrivateKey;
    use rsa::RsaPrivateKey;
    use rsa::rand_core::OsRng;
    use serde_json::json;
    use std::collections::HashMap;

    fn str_value(value: &OptionValue) -> &str {
        match value {
            OptionValue::String(s) => s.as_str(),
            _ => panic!("unexpected value"),
        }
    }

    // Build a base configuration common to all tests.
    fn base_config() -> HashMap<String, serde_json::Value> {
        let mut config = HashMap::new();
        config.insert("user".to_string(), json!("U"));
        config.insert("password".to_string(), json!("P"));
        config.insert("account".to_string(), json!("A"));
        config.insert("role".to_string(), json!("role"));
        config.insert("warehouse".to_string(), json!("warehouse"));
        config
    }

    fn run_config_test(config: HashMap<String, serde_json::Value>, expected: &[(&str, &str)]) {
        let auth = SnowflakeAuth {};
        let builder = auth
            .configure(&AdapterConfig::new(config))
            .expect("configure");

        let mut results = HashMap::new();

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

        assert_eq!(
            results.len(),
            expected.len(),
            "Unexpected extra keys:
    left: {results:?}
    right: {expected:?}",
        );
    }

    fn wrap_pem_64(begin: &str, body_b64: &str, end: &str) -> String {
        let mut out = String::new();
        out.push_str(begin);
        out.push('\n');
        let bytes = body_b64.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            let j = (i + 64).min(bytes.len());
            // body_b64 is ASCII, so this is safe
            out.push_str(std::str::from_utf8(&bytes[i..j]).unwrap());
            out.push('\n');
            i = j;
        }
        out.push_str(end);
        out
    }

    #[test]
    fn test_simple_pass() {
        let config = base_config();
        let expected = [
            ("user", "U"),
            ("password", "P"),
            (snowflake::ACCOUNT, "A"),
            (snowflake::ROLE, "role"),
            (snowflake::WAREHOUSE, "warehouse"),
            (snowflake::APPLICATION_NAME, APP_NAME),
            (snowflake::LOG_TRACING, "fatal"),
            (snowflake::LOGIN_TIMEOUT, DEFAULT_CONNECT_TIMEOUT),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_simple_pass_with_custom_connect_timeout_a() {
        let mut config = base_config();
        config.insert("connect_timeout".to_string(), json!("100"));
        let expected = [
            ("user", "U"),
            ("password", "P"),
            (snowflake::ACCOUNT, "A"),
            (snowflake::ROLE, "role"),
            (snowflake::WAREHOUSE, "warehouse"),
            (snowflake::APPLICATION_NAME, APP_NAME),
            (snowflake::LOG_TRACING, "fatal"),
            (snowflake::LOGIN_TIMEOUT, "100s"),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_simple_pass_with_custom_connect_timeout_b() {
        let mut config = base_config();
        config.insert("connect_timeout".to_string(), json!("0"));
        let expected = [
            ("user", "U"),
            ("password", "P"),
            (snowflake::ACCOUNT, "A"),
            (snowflake::ROLE, "role"),
            (snowflake::WAREHOUSE, "warehouse"),
            (snowflake::APPLICATION_NAME, APP_NAME),
            (snowflake::LOG_TRACING, "fatal"),
            (snowflake::LOGIN_TIMEOUT, "0s"),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_pass_with_method() {
        let mut config = base_config();
        config.insert("method".to_string(), json!("warehouse"));
        let expected = [
            ("user", "U"),
            ("password", "P"),
            (snowflake::ACCOUNT, "A"),
            (snowflake::ROLE, "role"),
            (snowflake::WAREHOUSE, "warehouse"),
            (snowflake::APPLICATION_NAME, APP_NAME),
            (snowflake::LOG_TRACING, "fatal"),
            (snowflake::LOGIN_TIMEOUT, DEFAULT_CONNECT_TIMEOUT),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_keypair_value_with_method_param() {
        let mut config = base_config();
        config.insert("method".to_string(), json!("keypair"));
        let b64_der = {
            let rsa = RsaPrivateKey::new(&mut OsRng, 2048).expect("generate RSA key");
            let der = rsa.to_pkcs8_der().expect("encode PKCS#8 DER");
            STANDARD.encode(der.as_bytes())
        };

        let expected_pem = wrap_pem_64(PEM_UNENCRYPTED_START, &b64_der, PEM_UNENCRYPTED_END);
        config.insert("private_key".to_string(), json!(b64_der));
        let expected = [
            ("user", "U"),
            ("password", "P"),
            (snowflake::ACCOUNT, "A"),
            (snowflake::ROLE, "role"),
            (snowflake::WAREHOUSE, "warehouse"),
            (
                snowflake::JWT_PRIVATE_KEY_PKCS8_VALUE,
                expected_pem.as_str(),
            ),
            (snowflake::AUTH_TYPE, "auth_jwt"),
            (snowflake::APPLICATION_NAME, APP_NAME),
            (snowflake::LOG_TRACING, "fatal"),
            (snowflake::LOGIN_TIMEOUT, DEFAULT_CONNECT_TIMEOUT),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_keypair_path_with_method_param() {
        let mut config = base_config();
        config.insert("method".to_string(), json!("keypair"));
        config.insert("private_key_path".to_string(), json!("private_key_path"));
        let expected = [
            ("user", "U"),
            ("password", "P"),
            (snowflake::ACCOUNT, "A"),
            (snowflake::ROLE, "role"),
            (snowflake::WAREHOUSE, "warehouse"),
            (snowflake::JWT_PRIVATE_KEY, "private_key_path"),
            (snowflake::AUTH_TYPE, "auth_jwt"),
            (snowflake::APPLICATION_NAME, APP_NAME),
            (snowflake::LOG_TRACING, "fatal"),
            (snowflake::LOGIN_TIMEOUT, DEFAULT_CONNECT_TIMEOUT),
        ];
        run_config_test(config, &expected);
    }

    // No library function to generate an encrypted key; made manually from
    // openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 | \
    // openssl pkcs8 -topk8 -v2 aes-256-cbc -passout pass:private_key_passphrase -inform PEM -outform DER | \
    // base64 -w0
    const ENCRYPTED_PKCS8_DER_B64: &str = "MIIFNTBfBgkqhkiG9w0BBQ0wUjAxBgkqhkiG9w0BBQwwJAQQTicT7AlFo6LN0RdUzkuo4AICCAAwDAYIKoZIhvcNAgkFADAdBglghkgBZQMEASoEEOnNZh3Day9astKrOi93uxgEggTQp2Z0RUN8e9pMhU3OUt+Jjz1HVVIILogdkDKktKbY4KOB/dT7qYDBa3pHqcHbIQm8frhpzKH4wDLptEblasFPcA0kaLHaDE8wQj6YalnMGWxF5T1aGKXqIRXr9xQFDzpllXrf2b5LIHKw1SzFX/qy8jv5KtXG6910fDVRM7h02eJFWYmm0uqbS9WHcU7IeSEgdiiER2Zvx0fsEZ3oM+gDnhg4/eW9QTRqqAU3oISSEstl+BXBYWYQFUf7wl2SEiKyDdQRzBhzSO8h00EQtiGcXviJUUoksktmQkJfIjjZBz/nHHjtNpQpTKa+uev/IY6/E2adxX3qkroSvdsK1phLq8a/JUhvVDTDxAOSNzaNQndnXJnhbpNAnnq32TilhnZhRXYjMJVXNlutTkoV90yyXara9WJ9Es2zZntuGathTYSre8VR0JAIgYvpqPP7DzD1hcbDzVES6q75gtaI+KD+af3QUnlReLP/c8roXsm27BGE2z5eo1j+gjzbOLqF/6EmkKzuLrJGl9pitSXVZBDeOzXOEIlvFytmhz+HjIGMGgBiPpBcOv73Whb91KF4PuCciXVGBhAlHlXNG5nvhL2NdfXxxHHTIGgGe9dQMAP5ap7z6sfjcLv/osp+jPqaizPZtUF3V/4OdiGFtJMRcD8Rnw/CTv/wWZksIpQ+PCJYR82dRY9Bu7F4v77ts1096otHI7dwA0SetZ2xeDngNiGlMVls3mygXknp5x8Tq737uyXId6vD/6fSBrI14gtJB6yFhbc5oc77UcWJQdvi+gOu4daLNuXdj7qlLFbQvWMNR5+LeJDsoW8jiULYX1vN+TKwzlszTBpi2+788LXWUtOC6wFxSk8SM9nVhXM4i8ONH3lioFy+N5MG9q4BGbvBiTLFfvn/MEp6fpVD1xrE9qfTfDqJjaNo3WBuSvFruLSS1Ih+ikPFHt8KV3chakByLGunOZKhkJV0B+Eh7HOD/TRoo0bf6EJ+I/WruQ/FvMRnKahuHX8Lr7nGFIg+VbNz/pMHevw1Tg9bD3koyVNbG3hpe4DFBd2gk8edIauCSAVJjt+JpJyiCfsYZw7RaCdbmjgw9Q8n43H5nAaiIfAU0hjya5RWA4HPH4e5RuZYQfvVsNUxcVTCE1BeZwZy+lFQFzd/DHW0EJQmhQwCBiy72xgn72Yv6XEkQDZOqNipcc7kja3JYSujSeXRPuWgmiQHyMQlDaz0qdJjmd5vUbFjoVFWsT3xAynddEl5hn7KCyOGDEvwdMLQI0CWP9MG+ZK8dXTE24u0oULZkWo2m2Zsqey05Erl0iKppu0d24HsJz8q9ueE5rWHOLV4L01fB5wiUvLBSkm3K9TLUeMdl/pw/3qxYe709ggQgqrM3UBcBzckEQ0sO8vBhDfbTZzKSquBS1ve29u/PUAM/g78AgcMwmiJpNrRVF5LNyLbBukSNxBigJkG61Tsqe9hfY9GsjKEefi6P0FTmaAmsw1vROCJSwqceWO+ldrYbOov0ViDYM1UfDO1lS7AItii8U1JCeuZkrMjcCZdoyhET3LTHM+NOHwLqce2RwVvoQMPk4kYftRohjR+M7/4WC9vwt5GmoK4NeNCBNdwphHLM/k5Dogu9/OOe8xrNRvunYunrU8w6ZOKR+s=";

    #[test]
    fn test_encrypted_keypair_without_method_param() {
        let mut config = base_config();
        let expected_pem = wrap_pem_64(
            PEM_ENCRYPTED_START,
            ENCRYPTED_PKCS8_DER_B64,
            PEM_ENCRYPTED_END,
        );
        let passphrase = "private_key_passphrase";
        config.insert("private_key".to_string(), json!(ENCRYPTED_PKCS8_DER_B64));
        config.insert("private_key_passphrase".to_string(), json!(passphrase));
        let expected = [
            ("user", "U"),
            ("password", "P"),
            (snowflake::ACCOUNT, "A"),
            (snowflake::ROLE, "role"),
            (snowflake::WAREHOUSE, "warehouse"),
            (
                snowflake::JWT_PRIVATE_KEY_PKCS8_VALUE,
                expected_pem.as_str(),
            ),
            (snowflake::JWT_PRIVATE_KEY_PKCS8_PASSWORD, passphrase),
            (snowflake::AUTH_TYPE, "auth_jwt"),
            (snowflake::APPLICATION_NAME, APP_NAME),
            (snowflake::LOG_TRACING, "fatal"),
            (snowflake::LOGIN_TIMEOUT, DEFAULT_CONNECT_TIMEOUT),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_encrypted_keypair_with_method_param() {
        let mut config = base_config();
        config.insert("method".to_string(), json!("keypair"));

        let passphrase = "private_key_passphrase";
        let expected_pem = format!(
            "{}\n{}\n{}",
            PEM_ENCRYPTED_START, "private_key", PEM_ENCRYPTED_END
        );

        config.insert("private_key".to_string(), json!(expected_pem));
        config.insert("private_key_passphrase".to_string(), json!(passphrase));

        let expected = [
            ("user", "U"),
            ("password", "P"),
            (snowflake::ACCOUNT, "A"),
            (snowflake::ROLE, "role"),
            (snowflake::WAREHOUSE, "warehouse"),
            (snowflake::APPLICATION_NAME, APP_NAME),
            (
                snowflake::JWT_PRIVATE_KEY_PKCS8_VALUE,
                expected_pem.as_str(),
            ),
            (snowflake::JWT_PRIVATE_KEY_PKCS8_PASSWORD, passphrase),
            (snowflake::AUTH_TYPE, "auth_jwt"),
            (snowflake::LOG_TRACING, "fatal"),
            (snowflake::LOGIN_TIMEOUT, DEFAULT_CONNECT_TIMEOUT),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_external_browser_authentication() {
        let mut config = base_config();
        config.insert("authenticator".to_string(), json!("externalbrowser"));
        let expected = [
            ("user", "U"),
            ("password", "P"),
            (snowflake::ACCOUNT, "A"),
            (snowflake::ROLE, "role"),
            (snowflake::WAREHOUSE, "warehouse"),
            (snowflake::APPLICATION_NAME, APP_NAME),
            (snowflake::AUTH_TYPE, snowflake::auth_type::EXTERNAL_BROWSER),
            (snowflake::LOG_TRACING, "fatal"),
            (snowflake::LOGIN_TIMEOUT, DEFAULT_CONNECT_TIMEOUT),
            (snowflake::CLIENT_STORE_TEMP_CREDS, "true"),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_external_browser_authentication_with_method_param() {
        let mut config = base_config();
        config.insert("method".to_string(), json!("sso"));
        let expected = [
            ("user", "U"),
            ("password", "P"),
            (snowflake::ACCOUNT, "A"),
            (snowflake::ROLE, "role"),
            (snowflake::WAREHOUSE, "warehouse"),
            (snowflake::APPLICATION_NAME, APP_NAME),
            (snowflake::AUTH_TYPE, snowflake::auth_type::EXTERNAL_BROWSER),
            (snowflake::LOG_TRACING, "fatal"),
            (snowflake::LOGIN_TIMEOUT, DEFAULT_CONNECT_TIMEOUT),
            (snowflake::CLIENT_STORE_TEMP_CREDS, "true"),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_native_oauth() {
        let mut config = base_config();
        config.insert("authenticator".to_string(), json!("oauth"));
        config.insert("oauth_client_id".to_string(), json!("C"));
        config.insert("oauth_client_secret".to_string(), json!("S"));
        config.insert("token".to_string(), json!("R"));
        let expected = [
            ("user", "U"),
            ("password", "P"),
            (snowflake::ACCOUNT, "A"),
            (snowflake::ROLE, "role"),
            (snowflake::WAREHOUSE, "warehouse"),
            (snowflake::APPLICATION_NAME, APP_NAME),
            (snowflake::AUTH_TYPE, snowflake::auth_type::OAUTH),
            (snowflake::CLIENT_ID, "C"),
            (snowflake::CLIENT_SECRET, "S"),
            (snowflake::REFRESH_TOKEN, "R"),
            (snowflake::CLIENT_STORE_TEMP_CREDS, "true"),
            (snowflake::LOG_TRACING, "fatal"),
            (snowflake::LOGIN_TIMEOUT, DEFAULT_CONNECT_TIMEOUT),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_native_oauth_with_method_param() {
        let mut config = base_config();
        config.insert("method".to_string(), json!("snowflake_oauth"));
        config.insert("oauth_client_id".to_string(), json!("C"));
        config.insert("oauth_client_secret".to_string(), json!("S"));
        config.insert("refresh_token".to_string(), json!("R"));
        let expected = [
            ("user", "U"),
            ("password", "P"),
            (snowflake::ACCOUNT, "A"),
            (snowflake::ROLE, "role"),
            (snowflake::WAREHOUSE, "warehouse"),
            (snowflake::APPLICATION_NAME, APP_NAME),
            (snowflake::AUTH_TYPE, snowflake::auth_type::OAUTH),
            (snowflake::CLIENT_ID, "C"),
            (snowflake::CLIENT_SECRET, "S"),
            (snowflake::REFRESH_TOKEN, "R"),
            (snowflake::CLIENT_STORE_TEMP_CREDS, "true"),
            (snowflake::LOG_TRACING, "fatal"),
            (snowflake::LOGIN_TIMEOUT, DEFAULT_CONNECT_TIMEOUT),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_oauth_fails_with_token_instead_of_refresh_token() {
        let mut config = base_config();
        config.insert("method".to_string(), json!("snowflake_oauth"));
        config.insert("oauth_client_id".to_string(), json!("client_id"));
        config.insert("oauth_client_secret".to_string(), json!("secret"));
        config.insert("token".to_string(), json!("should_be_refresh_token"));

        let cfg = AdapterConfig::new(config);
        let result = AuthMethod::new(&cfg, "snowflake_oauth");

        assert!(
            matches!(result, Err(ref e) if matches!(e, AuthError::Config(_))),
            "Expected configuration error, got: {result:?}"
        );

        if let Err(e) = result {
            let msg = format!("{e:?}");
            assert!(
                msg.contains("Rename") && msg.contains("refresh_token"),
                "Unexpected error message: {msg}"
            );
        }
    }

    #[test]
    fn test_oauth_fails_with_missing_required_fields() {
        let mut config = base_config();
        config.insert("method".to_string(), json!("snowflake_oauth"));
        config.insert("oauth_client_id".to_string(), json!("client_id"));
        // oauth_client_secret OMITTED ON PURPOSE
        config.insert("refresh_token".to_string(), json!("refresh_token"));

        let cfg = AdapterConfig::new(config);
        let result = AuthMethod::new(&cfg, "snowflake_oauth");

        assert!(
            matches!(result, Err(ref e) if matches!(e, AuthError::Config(_))),
            "Expected configuration error due to missing OAuth fields, got: {result:?}"
        );

        if let Err(e) = result {
            let msg = format!("{e:?}");
            assert!(
                msg.contains("oauth_client_id")
                    && msg.contains("oauth_client_secret")
                    && msg.contains("token"),
                "Unexpected error message: {msg}"
            );
        }
    }

    #[test]
    fn test_userpass_mfa() {
        let mut config = base_config();
        config.insert("authenticator".to_string(), json!("username_password_mfa"));
        let expected = [
            ("user", "U"),
            ("password", "P"),
            (snowflake::ACCOUNT, "A"),
            (snowflake::ROLE, "role"),
            (snowflake::WAREHOUSE, "warehouse"),
            (snowflake::APPLICATION_NAME, APP_NAME),
            (
                snowflake::AUTH_TYPE,
                snowflake::auth_type::USERNAME_PASSWORD_MFA,
            ),
            (snowflake::CLIENT_CACHE_MFA_TOKEN, "true"),
            (snowflake::LOG_TRACING, "fatal"),
            (snowflake::LOGIN_TIMEOUT, DEFAULT_CONNECT_TIMEOUT),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_userpass_mfa_with_method_param() {
        let mut config = base_config();
        config.insert("method".to_string(), json!("warehouse_mfa"));
        let expected = [
            ("user", "U"),
            ("password", "P"),
            (snowflake::ACCOUNT, "A"),
            (snowflake::ROLE, "role"),
            (snowflake::WAREHOUSE, "warehouse"),
            (snowflake::APPLICATION_NAME, APP_NAME),
            (
                snowflake::AUTH_TYPE,
                snowflake::auth_type::USERNAME_PASSWORD_MFA,
            ),
            (snowflake::CLIENT_CACHE_MFA_TOKEN, "true"),
            (snowflake::LOG_TRACING, "fatal"),
            (snowflake::LOGIN_TIMEOUT, DEFAULT_CONNECT_TIMEOUT),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_catch_unneeded_authenticator() {
        let mut config = base_config();
        config.insert("authenticator".to_string(), json!("wrong"));

        let cfg = AdapterConfig::new(config);
        let result = AuthMethod::new(&cfg, "warehouse_mfa");

        assert!(
            matches!(result, Err(ref e) if matches!(e, AuthError::Config(_))),
            "Expected configuration error, got: {result:?}"
        );

        if let Err(e) = result {
            let msg = format!("{e:?}");
            assert!(
                msg.contains("authenticator") && msg.contains("Use method field"),
                "Unexpected error message: {msg}"
            );
        }
    }

    #[test]
    fn test_jwt_oauth() {
        let mut config = base_config();
        config.insert("authenticator".to_string(), json!("jwt"));
        config.insert(
            "token".to_string(),
            json!("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"),
        );

        let expected = [
            ("user", "U"),
            ("password", "P"),
            (snowflake::ACCOUNT, "A"),
            (snowflake::ROLE, "role"),
            (snowflake::WAREHOUSE, "warehouse"),
            (snowflake::APPLICATION_NAME, APP_NAME),
            (snowflake::AUTH_TYPE, snowflake::auth_type::OAUTH),
            (
                snowflake::AUTH_TOKEN,
                "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
            ),
            (snowflake::CLIENT_STORE_TEMP_CREDS, "true"),
            (snowflake::LOG_TRACING, "fatal"),
            (snowflake::LOGIN_TIMEOUT, DEFAULT_CONNECT_TIMEOUT),
        ];

        run_config_test(config, &expected);
    }

    #[test]
    fn test_jwt_oauth_fails_with_token_instead_of_jwt() {
        let mut config = base_config();
        config.insert("method".to_string(), json!("snowflake_oauth_jwt"));
        config.insert("token".to_string(), json!("wrong_field"));

        let cfg = AdapterConfig::new(config);
        let result = AuthMethod::new(&cfg, "snowflake_oauth_jwt");

        assert!(
            matches!(result, Err(ref e) if matches!(e, AuthError::Config(_))),
            "Expected configuration error, got: {result:?}"
        );

        if let Err(e) = result {
            let msg = format!("{e:?}");
            assert!(
                msg.contains("Profile") && msg.contains("'jwt_token'"),
                "Unexpected error message: {msg}"
            );
        }
    }

    #[test]
    fn test_jwt_oauth_fails_with_missing_jwt() {
        let mut config = base_config();
        config.insert("method".to_string(), json!("snowflake_oauth_jwt"));
        // jwt intentionally missing

        let cfg = AdapterConfig::new(config);
        let result = AuthMethod::new(&cfg, "snowflake_oauth_jwt");

        assert!(
            matches!(result, Err(ref e) if matches!(e, AuthError::Config(_))),
            "Expected configuration error for missing jwt, got: {result:?}"
        );

        if let Err(e) = result {
            let msg = format!("{e:?}");
            assert!(
                msg.contains("jwt_token") && msg.contains("snowflake_oauth_jwt"),
                "Unexpected error message: {msg}"
            );
        }
    }

    #[test]
    fn test_s3_stage_vpce_dns_name() {
        let mut config = base_config();
        config.insert(
            snowflake::S3_STAGE_VPCE_DNS_NAME_PARAM_KEY.to_string(),
            "my-vpce-endpoint.s3.region.vpce.amazonaws.com".into(),
        );
        let expected = [
            ("user", "U"),
            ("password", "P"),
            (snowflake::ACCOUNT, "A"),
            (snowflake::ROLE, "role"),
            (snowflake::WAREHOUSE, "warehouse"),
            (snowflake::APPLICATION_NAME, "dbt"),
            (
                snowflake::S3_STAGE_VPCE_DNS_NAME_PARAM_KEY,
                "my-vpce-endpoint.s3.region.vpce.amazonaws.com",
            ),
            (snowflake::LOG_TRACING, "fatal"),
            (snowflake::LOGIN_TIMEOUT, DEFAULT_CONNECT_TIMEOUT),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_s3_stage_vpce_dns_name_with_method() {
        let mut config = base_config();
        config.insert("method".to_string(), json!("warehouse"));
        config.insert(
            snowflake::S3_STAGE_VPCE_DNS_NAME_PARAM_KEY.to_string(),
            "my-vpce-endpoint.s3.region.vpce.amazonaws.com".into(),
        );
        let expected = [
            ("user", "U"),
            ("password", "P"),
            (snowflake::ACCOUNT, "A"),
            (snowflake::ROLE, "role"),
            (snowflake::WAREHOUSE, "warehouse"),
            (snowflake::APPLICATION_NAME, APP_NAME),
            (
                snowflake::S3_STAGE_VPCE_DNS_NAME_PARAM_KEY,
                "my-vpce-endpoint.s3.region.vpce.amazonaws.com",
            ),
            (snowflake::LOG_TRACING, "fatal"),
            (snowflake::LOGIN_TIMEOUT, DEFAULT_CONNECT_TIMEOUT),
        ];
        run_config_test(config, &expected);
    }
}
