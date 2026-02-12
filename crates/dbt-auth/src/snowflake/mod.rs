mod key_format;

use crate::{AdapterConfig, Auth, AuthError, PrivateKeySource, auth_configure_pipeline};
use database::Builder as DatabaseBuilder;
use dbt_xdbc::database::LogLevel;
use dbt_xdbc::{Backend, database, snowflake};

use std::fs;

const APP_NAME: &str = "dbt";

// WARNING: Still needs adjustment on what is considered must-have
const CONNECTION_PARAMS: [&str; 10] = [
    "user",
    "password",
    "account",
    "role",
    "warehouse",
    "database",
    "schema",
    "host",
    "port",
    "protocol",
];

/// Configuration values that are needed for an auth method in a dbt-snowflake profile.
///
/// dbt snowflake only formalized `method` later in it's lifetime. For profiles without
/// `method`, we must naively copy over all these fields. These are mutually exclusive fields.
/// Username and password are always required, so they do not belong to this set.
const AUTH_PARAMS_USED_FOR_LEGACY_CONFIG: [&str; 6] = [
    "private_key_path",
    "private_key",
    "private_key_passphrase",
    "oauth_client_id",
    "oauth_client_secret",
    "authenticator",
];

const DEFAULT_CONNECT_TIMEOUT: &str = "10s";
const DEFAULT_REQUEST_TIMEOUT: &str = "600s";

/// dbt Core expects durations in seconds only so this utility appends that s
/// https://pkg.go.dev/time#ParseDuration for permitted units
fn postfix_seconds_unit(value: &str) -> String {
    format!("{value}s")
}

#[derive(Debug)]
enum SnowflakeAuthIR<'a> {
    Warehouse,
    WarehouseMFA,
    Keypair {
        key_source: PrivateKeySource<'a>,
        private_key_passphrase: Option<&'a str>,
    },
    NativeOauth {
        client_id: &'a str,
        client_secret: &'a str,
        refresh_token: &'a str,
    },
    NativeOauthJWT {
        jwt_token: &'a str,
    },
    Sso,
}

impl<'a> SnowflakeAuthIR<'a> {
    pub fn apply(self, mut builder: DatabaseBuilder) -> Result<DatabaseBuilder, AuthError> {
        match self {
            Self::NativeOauth {
                client_id,
                client_secret,
                refresh_token,
            } => {
                builder.with_named_option(snowflake::AUTH_TYPE, snowflake::auth_type::OAUTH)?;
                builder.with_named_option(snowflake::CLIENT_ID, client_id)?;
                builder.with_named_option(snowflake::CLIENT_SECRET, client_secret)?;
                builder.with_named_option(snowflake::REFRESH_TOKEN, refresh_token)?;
                builder.with_named_option(snowflake::CLIENT_STORE_TEMP_CREDS, "true")?;
            }
            Self::NativeOauthJWT { jwt_token } => {
                builder.with_named_option(snowflake::AUTH_TYPE, snowflake::auth_type::OAUTH)?;
                builder.with_named_option(snowflake::AUTH_TOKEN, jwt_token)?;
                builder.with_named_option(snowflake::CLIENT_STORE_TEMP_CREDS, "true")?;
            }
            Self::Sso => {
                builder.with_named_option(
                    snowflake::AUTH_TYPE,
                    snowflake::auth_type::EXTERNAL_BROWSER,
                )?;
                builder.with_named_option(snowflake::CLIENT_STORE_TEMP_CREDS, "true")?;
            }
            Self::Keypair {
                key_source,
                private_key_passphrase,
            } => {
                builder.with_named_option(snowflake::AUTH_TYPE, snowflake::auth_type::JWT)?;
                match key_source {
                    PrivateKeySource::FilePath(path) => {
                        fs::metadata(path).map_err(|_| {
                            AuthError::config(format!("Private key file not found: '{path}'"))
                        })?;
                        // If there's a passphrase, pass to the PKCS#8_VALUE param
                        // If no passphrase, just pass the path
                        if let Some(pass) = private_key_passphrase {
                            let key_content = fs::read_to_string(path).map_err(|_| {
                                AuthError::config(format!(
                                    "Could not read from key file: '{}'",
                                    path
                                ))
                            })?;
                            builder.with_named_option(
                                snowflake::JWT_PRIVATE_KEY_PKCS8_VALUE,
                                key_format::normalize_key(&key_content)?,
                            )?;
                            builder.with_named_option(
                                snowflake::JWT_PRIVATE_KEY_PKCS8_PASSWORD,
                                pass,
                            )?;
                        } else {
                            builder.with_named_option(snowflake::JWT_PRIVATE_KEY, path)?;
                        }
                    }
                    PrivateKeySource::Raw(raw) => {
                        builder.with_named_option(
                            snowflake::JWT_PRIVATE_KEY_PKCS8_VALUE,
                            key_format::normalize_key(raw)?,
                        )?;
                        if let Some(pass) = private_key_passphrase {
                            builder.with_named_option(
                                snowflake::JWT_PRIVATE_KEY_PKCS8_PASSWORD,
                                pass,
                            )?;
                        }
                    }
                }
            }
            Self::WarehouseMFA => {
                builder.with_named_option(
                    snowflake::AUTH_TYPE,
                    snowflake::auth_type::USERNAME_PASSWORD_MFA,
                )?;
                builder.with_named_option(snowflake::CLIENT_CACHE_MFA_TOKEN, "true")?;
            }
            Self::Warehouse => {
                // No-op: username and password are standard
            }
        }

        Ok(builder)
    }
}

fn parse_auth<'a>(config: &'a AdapterConfig) -> Result<SnowflakeAuthIR<'a>, AuthError> {
    // Case 1: Profile has `method`. We can do strict evaluation of their profiles.yml
    if let Some(method) = config.get_str("method") {
        if config.get_str("authenticator").is_some() {
            return Err(AuthError::config(
                "Using 'method' in your Snowflake profile subsumes 'authenticator' field. Please remove authenticator.",
            ));
        }

        match method {
            "keypair" => {
                let pk_path = config.get_str("private_key_path");
                let pk_raw = config.get_str("private_key");
                let pk_pass = config.get_str("private_key_passphrase");

                let source = match (pk_path, pk_raw) {
                    (Some(_), Some(_)) => Err(AuthError::config(
                        "Cannot specify both 'private_key' and 'private_key_path'",
                    )),
                    (Some(path), None) => Ok(PrivateKeySource::FilePath(path)),
                    (None, Some(raw)) => Ok(PrivateKeySource::Raw(raw)),
                    (None, None) => Err(AuthError::config(
                        "Keypair authentication requires exactly one of 'private_key' or 'private_key_path'",
                    )),
                }?;

                Ok(SnowflakeAuthIR::Keypair {
                    key_source: source,
                    private_key_passphrase: pk_pass,
                })
            }
            "sso" => Ok(SnowflakeAuthIR::Sso),
            "snowflake_oauth" => {
                if config.contains_key("token") {
                    return Err(AuthError::config(
                        "Rename 'token' to 'refresh_token' in profile for 'method: snowflake_oauth'.",
                    ));
                };

                let cid = config.get_str("oauth_client_id");
                let sec = config.get_str("oauth_client_secret");
                let tok = config.get_str("refresh_token");

                match (cid, sec, tok) {
                    (Some(client_id), Some(client_secret), Some(refresh_token)) => {
                        Ok(SnowflakeAuthIR::NativeOauth {
                            client_id,
                            client_secret,
                            refresh_token,
                        })
                    }
                    _ => Err(AuthError::config(
                        "Profile requires 'oauth_client_id', 'oauth_client_secret', and 'refresh_token' for method: snowflake_oauth.",
                    )),
                }
            }
            "snowflake_oauth_jwt" => {
                if let Some(jwt_token) = config.get_str("jwt_token") {
                    Ok(SnowflakeAuthIR::NativeOauthJWT { jwt_token })
                } else {
                    Err(AuthError::config(
                        "Profile requires 'jwt_token' for 'method: snowflake_oauth_jwt'.",
                    ))
                }
            }
            "warehouse" => Ok(SnowflakeAuthIR::Warehouse),
            "warehouse_mfa" => Ok(SnowflakeAuthIR::WarehouseMFA),
            unsupported_method => Err(AuthError::config(format!(
                "Profile has unsupported authentication method {unsupported_method}"
            ))),
        }

    // Case 2: User has no `method` and we must rely on simple passthrough.
    //
    // For backwards compatibility with dbt-snowflake in core. By default,
    // there is no `method` parameter. `method` is the standard authentication
    // method option for other adapters, however.
    // FIXME: I made this better but Felipe has a long-term iterator-based solution to make this exact
    } else {
        // Reduce ambiguity in loop
        if config.contains_key("private_key_path") && config.contains_key("private_key") {
            return Err(AuthError::config(
                "Cannot specify both `private_key` and `private_key_path`.".to_owned(),
            ));
        }

        // Take first matching, even if this isn't strictly correct right, fallback to user/pass
        for key in AUTH_PARAMS_USED_FOR_LEGACY_CONFIG.iter() {
            if let Some(value) = config.get_str(key) {
                return match *key {
                    "private_key_path" => Ok(SnowflakeAuthIR::Keypair {
                        key_source: PrivateKeySource::FilePath(value),
                        private_key_passphrase: config.get_str("private_key_passphrase"),
                    }),
                    "private_key" => Ok(SnowflakeAuthIR::Keypair {
                        key_source: PrivateKeySource::Raw(value),
                        private_key_passphrase: config.get_str("private_key_passphrase"),
                    }),
                    "private_key_passphrase" => {
                        // We found a passphrase, so we MUST find a key source to go with it
                        let path = config.get_str("private_key_path");
                        let raw = config.get_str("private_key");

                        let source = match (path, raw) {
                            (Some(p), _) => PrivateKeySource::FilePath(p),
                            (None, Some(r)) => PrivateKeySource::Raw(r),
                            (None, None) => {
                                return Err(AuthError::config(
                                    "Found 'private_key_passphrase' but missing 'private_key_path' or 'private_key'",
                                ));
                            }
                        };

                        Ok(SnowflakeAuthIR::Keypair {
                            key_source: source,
                            private_key_passphrase: Some(value),
                        })
                    }
                    "oauth_client_id" | "oauth_client_secret" => {
                        let cid = config.get_str("oauth_client_id");
                        let sec = config.get_str("oauth_client_secret");
                        // backwards compatibility; we'd prefer this to be named refresh_token but this
                        let tok = config.get_str("token");
                        match (cid, sec, tok) {
                            (Some(client_id), Some(client_secret), Some(refresh_token)) => {
                                Ok(SnowflakeAuthIR::NativeOauth {
                                    client_id,
                                    client_secret,
                                    refresh_token,
                                })
                            }
                            _ => Err(AuthError::config(
                                "Legacy OAuth requires `oauth_client_id`, `oauth_client_secret`, and `token`.",
                            )),
                        }
                    }
                    "authenticator" => {
                        if value == "externalbrowser" {
                            Ok(SnowflakeAuthIR::Sso)
                        } else if value == "oauth" {
                            let cid = config.get_str("oauth_client_id");
                            let sec = config.get_str("oauth_client_secret");
                            let tok = config.get_str("refresh_token");

                            // Use the tuple match to enforce all three fields
                            match (cid, sec, tok) {
                                (Some(client_id), Some(client_secret), Some(refresh_token)) => {
                                    Ok(SnowflakeAuthIR::NativeOauth {
                                        client_id,
                                        client_secret,
                                        refresh_token,
                                    })
                                }
                                _ => Err(AuthError::config(
                                    "Legacy 'authenticator: oauth' requires oauth_client_id, oauth_client_secret, and refresh_token",
                                )),
                            }
                        } else if value == "jwt" {
                            config
                                .get_str("token")
                                .map(|jwt_token| SnowflakeAuthIR::NativeOauthJWT { jwt_token })
                                .ok_or_else(|| {
                                    AuthError::config("Legacy 'authenticator: jwt' requires token")
                                })
                        } else if value == "username_password_mfa" {
                            Ok(SnowflakeAuthIR::WarehouseMFA)
                        } else {
                            Err(AuthError::config(format!(
                                "Unsupported authenticator: {value}"
                            )))
                        }
                    }
                    _ => panic!("unexpected key: {key}"),
                };
            }
        }

        // Fallback to username and password when no other auth is possible
        Ok(SnowflakeAuthIR::Warehouse)
    }
}

fn apply_connection_args(
    config: &AdapterConfig,
    mut builder: DatabaseBuilder,
) -> Result<DatabaseBuilder, AuthError> {
    for key in CONNECTION_PARAMS {
        if let Some(value) = config.get_str(key) {
            match key {
                "user" => Ok(builder.with_username(value)),
                "password" => Ok(builder.with_password(value)),
                "account" => builder.with_named_option(snowflake::ACCOUNT, value),
                "database" => builder.with_named_option(snowflake::DATABASE, value),
                "schema" => builder.with_named_option(snowflake::SCHEMA, value),
                "role" => builder.with_named_option(snowflake::ROLE, value),
                "warehouse" => builder.with_named_option(snowflake::WAREHOUSE, value),
                "host" => builder.with_named_option(snowflake::HOST, value),
                "port" => builder.with_named_option(snowflake::PORT, value),
                "protocol" => builder.with_named_option(snowflake::PROTOCOL, value),
                _ => panic!("unexpected key: {key}"),
            }?;
        }
    }
    builder.with_named_option(snowflake::APPLICATION_NAME, APP_NAME)?;

    // S3 VPCE Logic
    if let Some(s3_dns) = config.get_str(snowflake::S3_STAGE_VPCE_DNS_NAME_PARAM_KEY) {
        builder.with_named_option(snowflake::S3_STAGE_VPCE_DNS_NAME_PARAM_KEY, s3_dns)?;
    }

    // Query Tag Logic
    if let Some(query_tag) = config.get_str(snowflake::QUERY_TAG_PARAM_KEY) {
        builder.with_named_option(snowflake::QUERY_TAG_PARAM_KEY, query_tag)?;
    }

    // Timeout Logic
    let connect_timeout = config
        .get_str("connect_timeout")
        .map(postfix_seconds_unit)
        .unwrap_or_else(|| DEFAULT_CONNECT_TIMEOUT.to_string());
    builder.with_named_option(snowflake::LOGIN_TIMEOUT, connect_timeout)?;

    let request_timeout = config
        .get_str("request_timeout")
        .map(postfix_seconds_unit)
        .unwrap_or_else(|| DEFAULT_REQUEST_TIMEOUT.to_string());
    builder.with_named_option(snowflake::REQUEST_TIMEOUT, request_timeout)?;

    // disable any logging from Gosnowflake that's not a fatal/panic
    builder.with_named_option(snowflake::LOG_TRACING, LogLevel::Fatal.to_string())?;

    Ok(builder)
}

pub struct SnowflakeAuth;

impl Auth for SnowflakeAuth {
    fn backend(&self) -> Backend {
        Backend::Snowflake
    }

    fn configure(&self, config: &AdapterConfig) -> Result<DatabaseBuilder, AuthError> {
        auth_configure_pipeline!(self.backend(), &config, parse_auth, apply_connection_args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_options::option_str_value;
    use adbc_core::options::OptionDatabase;
    use base64::{Engine, engine::general_purpose::STANDARD};
    use dbt_yaml::Mapping;
    use key_format::{
        PEM_ENCRYPTED_END, PEM_ENCRYPTED_START, PEM_UNENCRYPTED_END, PEM_UNENCRYPTED_START,
    };
    use pkcs8::EncodePrivateKey;
    use rsa::RsaPrivateKey;
    use rsa::rand_core::OsRng;

    // Build a base configuration common to all tests.
    fn base_config() -> Mapping {
        let config = Mapping::from_iter([
            ("user".into(), "U".into()),
            ("password".into(), "P".into()),
            ("account".into(), "A".into()),
            ("role".into(), "role".into()),
            ("warehouse".into(), "warehouse".into()),
        ]);
        config
    }

    fn run_config_test(config: Mapping, expected: &[(&str, &str)]) {
        let auth = SnowflakeAuth {};
        let builder = auth
            .configure(&AdapterConfig::new(config))
            .expect("configure");

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
            (snowflake::REQUEST_TIMEOUT, DEFAULT_REQUEST_TIMEOUT),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_simple_pass_with_custom_connect_timeout_a() {
        let mut config = base_config();
        config.insert("connect_timeout".into(), "100".into());
        let expected = [
            ("user", "U"),
            ("password", "P"),
            (snowflake::ACCOUNT, "A"),
            (snowflake::ROLE, "role"),
            (snowflake::WAREHOUSE, "warehouse"),
            (snowflake::APPLICATION_NAME, APP_NAME),
            (snowflake::LOG_TRACING, "fatal"),
            (snowflake::LOGIN_TIMEOUT, "100s"),
            (snowflake::REQUEST_TIMEOUT, DEFAULT_REQUEST_TIMEOUT),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_simple_pass_with_custom_connect_timeout_b() {
        let mut config = base_config();
        config.insert("connect_timeout".into(), "0".into());
        let expected = [
            ("user", "U"),
            ("password", "P"),
            (snowflake::ACCOUNT, "A"),
            (snowflake::ROLE, "role"),
            (snowflake::WAREHOUSE, "warehouse"),
            (snowflake::APPLICATION_NAME, APP_NAME),
            (snowflake::LOG_TRACING, "fatal"),
            (snowflake::LOGIN_TIMEOUT, "0s"),
            (snowflake::REQUEST_TIMEOUT, DEFAULT_REQUEST_TIMEOUT),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_simple_pass_with_custom_request_timeout_a() {
        let mut config = base_config();
        config.insert("request_timeout".into(), "100".into());
        let expected = [
            ("user", "U"),
            ("password", "P"),
            (snowflake::ACCOUNT, "A"),
            (snowflake::ROLE, "role"),
            (snowflake::WAREHOUSE, "warehouse"),
            (snowflake::APPLICATION_NAME, APP_NAME),
            (snowflake::LOG_TRACING, "fatal"),
            (snowflake::LOGIN_TIMEOUT, DEFAULT_CONNECT_TIMEOUT),
            (snowflake::REQUEST_TIMEOUT, "100s"),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_simple_pass_with_custom_request_timeout_b() {
        let mut config = base_config();
        config.insert("request_timeout".into(), "0".into());
        let expected = [
            ("user", "U"),
            ("password", "P"),
            (snowflake::ACCOUNT, "A"),
            (snowflake::ROLE, "role"),
            (snowflake::WAREHOUSE, "warehouse"),
            (snowflake::APPLICATION_NAME, APP_NAME),
            (snowflake::LOG_TRACING, "fatal"),
            (snowflake::LOGIN_TIMEOUT, DEFAULT_CONNECT_TIMEOUT),
            (snowflake::REQUEST_TIMEOUT, "0s"),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_pass_with_method() {
        let mut config = base_config();
        config.insert("method".into(), "warehouse".into());
        let expected = [
            ("user", "U"),
            ("password", "P"),
            (snowflake::ACCOUNT, "A"),
            (snowflake::ROLE, "role"),
            (snowflake::WAREHOUSE, "warehouse"),
            (snowflake::APPLICATION_NAME, APP_NAME),
            (snowflake::LOG_TRACING, "fatal"),
            (snowflake::LOGIN_TIMEOUT, DEFAULT_CONNECT_TIMEOUT),
            (snowflake::REQUEST_TIMEOUT, DEFAULT_REQUEST_TIMEOUT),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_keypair_value_with_method_param() {
        let mut config = base_config();
        config.insert("method".into(), "keypair".into());
        let b64_der = {
            let rsa = RsaPrivateKey::new(&mut OsRng, 2048).expect("generate RSA key");
            let der = rsa.to_pkcs8_der().expect("encode PKCS#8 DER");
            STANDARD.encode(der.as_bytes())
        };

        let expected_pem = wrap_pem_64(PEM_UNENCRYPTED_START, &b64_der, PEM_UNENCRYPTED_END);
        config.insert("private_key".into(), b64_der.into());
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
            (snowflake::REQUEST_TIMEOUT, DEFAULT_REQUEST_TIMEOUT),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_keypair_path_with_method_param() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut tmp_file = NamedTempFile::new().expect("Failed to create temp file");
        writeln!(tmp_file, "fake-private-key-data").unwrap();
        let temp_path = tmp_file.path().to_str().expect("Valid UTF-8 path");

        let mut config = base_config();
        config.insert("method".into(), "keypair".into());
        config.insert("private_key_path".into(), temp_path.into());
        let expected = [
            ("user", "U"),
            ("password", "P"),
            (snowflake::ACCOUNT, "A"),
            (snowflake::ROLE, "role"),
            (snowflake::WAREHOUSE, "warehouse"),
            (snowflake::JWT_PRIVATE_KEY, temp_path),
            (snowflake::AUTH_TYPE, "auth_jwt"),
            (snowflake::APPLICATION_NAME, APP_NAME),
            (snowflake::LOG_TRACING, "fatal"),
            (snowflake::LOGIN_TIMEOUT, DEFAULT_CONNECT_TIMEOUT),
            (snowflake::REQUEST_TIMEOUT, DEFAULT_REQUEST_TIMEOUT),
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
        config.insert("private_key".into(), ENCRYPTED_PKCS8_DER_B64.into());
        config.insert("private_key_passphrase".into(), passphrase.into());
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
            (snowflake::REQUEST_TIMEOUT, DEFAULT_REQUEST_TIMEOUT),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_encrypted_keypair_with_method_param() {
        let mut config = base_config();
        config.insert("method".into(), "keypair".into());

        let passphrase = "private_key_passphrase";
        let expected_pem = format!(
            "{}\n{}\n{}",
            PEM_ENCRYPTED_START, "private_key", PEM_ENCRYPTED_END
        );

        config.insert("private_key".into(), expected_pem.clone().into());
        config.insert("private_key_passphrase".into(), passphrase.into());

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
            (snowflake::REQUEST_TIMEOUT, DEFAULT_REQUEST_TIMEOUT),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_external_browser_authentication() {
        let mut config = base_config();
        config.insert("authenticator".into(), "externalbrowser".into());
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
            (snowflake::REQUEST_TIMEOUT, DEFAULT_REQUEST_TIMEOUT),
            (snowflake::CLIENT_STORE_TEMP_CREDS, "true"),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_external_browser_authentication_with_method_param() {
        let mut config = base_config();
        config.insert("method".into(), "sso".into());
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
            (snowflake::REQUEST_TIMEOUT, DEFAULT_REQUEST_TIMEOUT),
            (snowflake::CLIENT_STORE_TEMP_CREDS, "true"),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_native_oauth() {
        let mut config = base_config();
        config.insert("authenticator".into(), "oauth".into());
        config.insert("oauth_client_id".into(), "C".into());
        config.insert("oauth_client_secret".into(), "S".into());
        config.insert("token".into(), "R".into());
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
            (snowflake::REQUEST_TIMEOUT, DEFAULT_REQUEST_TIMEOUT),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_native_oauth_with_method_param() {
        let mut config = base_config();
        config.insert("method".into(), "snowflake_oauth".into());
        config.insert("oauth_client_id".into(), "C".into());
        config.insert("oauth_client_secret".into(), "S".into());
        config.insert("refresh_token".into(), "R".into());
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
            (snowflake::REQUEST_TIMEOUT, DEFAULT_REQUEST_TIMEOUT),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_oauth_fails_with_token_instead_of_refresh_token() {
        let mut config = base_config();
        config.insert("method".into(), "snowflake_oauth".into());
        config.insert("oauth_client_id".into(), "client_id".into());
        config.insert("oauth_client_secret".into(), "secret".into());
        config.insert("token".into(), "should_be_refresh_token".into());

        let cfg = AdapterConfig::new(config);
        let result = parse_auth(&cfg);

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
    fn test_invalid_private_key_path() {
        let auth = SnowflakeAuth {};
        let bad_path = "this_file_does_not_exist.p8";

        macro_rules! assert_file_not_found {
            ($config_map:expr, $label:expr) => {
                let result = auth.configure(&AdapterConfig::new($config_map));
                assert!(
                    matches!(result, Err(AuthError::Config(ref msg)) if msg.contains("Private key file not found")),
                    "{} path failed: expected 'Private key file not found' error, got {:?}",
                    $label, result
                );
            };
        }

        // 1. Test Legacy Case
        let mut legacy_map = base_config();
        legacy_map.insert("private_key_path".into(), bad_path.into());
        assert_file_not_found!(legacy_map, "Legacy");

        // 2. Test Modern Case
        let mut modern_map = base_config();
        modern_map.insert("method".into(), "keypair".into());
        modern_map.insert("private_key_path".into(), bad_path.into());
        assert_file_not_found!(modern_map, "Modern");
    }

    #[test]
    fn test_oauth_fails_with_missing_required_fields() {
        let mut config = base_config();
        config.insert("method".into(), "snowflake_oauth".into());
        config.insert("oauth_client_id".into(), "client_id".into());
        // oauth_client_secret OMITTED ON PURPOSE
        config.insert("refresh_token".into(), "refresh_token".into());

        let cfg = AdapterConfig::new(config);
        let result = parse_auth(&cfg);

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
        config.insert("authenticator".into(), "username_password_mfa".into());
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
            (snowflake::REQUEST_TIMEOUT, DEFAULT_REQUEST_TIMEOUT),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_userpass_mfa_with_method_param() {
        let mut config = base_config();
        config.insert("method".into(), "warehouse_mfa".into());
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
            (snowflake::REQUEST_TIMEOUT, DEFAULT_REQUEST_TIMEOUT),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_catch_unneeded_authenticator() {
        let mut config = base_config();
        config.insert("method".into(), "warehouse".into());
        config.insert("authenticator".into(), "wrong".into());

        let cfg = AdapterConfig::new(config);
        let result = parse_auth(&cfg);

        assert!(
            matches!(result, Err(ref e) if matches!(e, AuthError::Config(_))),
            "Expected configuration error, got: {result:?}"
        );

        if let Err(e) = result {
            let msg = format!("{e:?}");
            assert!(
                msg.contains("Using 'method' in your Snowflake profile subsumes"),
                "Unexpected error message: {msg}"
            );
        }
    }

    #[test]
    fn test_jwt_oauth() {
        let mut config = base_config();
        config.insert("authenticator".into(), "jwt".into());
        config.insert(
            "token".into(),
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9".into(),
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
            (snowflake::REQUEST_TIMEOUT, DEFAULT_REQUEST_TIMEOUT),
        ];

        run_config_test(config, &expected);
    }

    #[test]
    fn test_jwt_oauth_fails_with_token_instead_of_jwt() {
        let mut config = base_config();
        config.insert("method".into(), "snowflake_oauth_jwt".into());
        config.insert("token".into(), "wrong_field".into());

        let cfg = AdapterConfig::new(config);
        let result = parse_auth(&cfg);

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
        config.insert("method".into(), "snowflake_oauth_jwt".into());
        // jwt intentionally missing

        let cfg = AdapterConfig::new(config);
        let result = parse_auth(&cfg);

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
            snowflake::S3_STAGE_VPCE_DNS_NAME_PARAM_KEY.into(),
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
            (snowflake::REQUEST_TIMEOUT, DEFAULT_REQUEST_TIMEOUT),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_s3_stage_vpce_dns_name_with_method() {
        let mut config = base_config();
        config.insert("method".into(), "warehouse".into());
        config.insert(
            snowflake::S3_STAGE_VPCE_DNS_NAME_PARAM_KEY.into(),
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
            (snowflake::REQUEST_TIMEOUT, DEFAULT_REQUEST_TIMEOUT),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_query_tag() {
        let mut config = base_config();
        config.insert(
            snowflake::QUERY_TAG_PARAM_KEY.into(),
            "custom-query-tag".into(),
        );
        let expected = [
            ("user", "U"),
            ("password", "P"),
            (snowflake::ACCOUNT, "A"),
            (snowflake::ROLE, "role"),
            (snowflake::WAREHOUSE, "warehouse"),
            (snowflake::APPLICATION_NAME, "dbt"),
            (snowflake::QUERY_TAG_PARAM_KEY, "custom-query-tag"),
            (snowflake::LOG_TRACING, "fatal"),
            (snowflake::LOGIN_TIMEOUT, DEFAULT_CONNECT_TIMEOUT),
            (snowflake::REQUEST_TIMEOUT, DEFAULT_REQUEST_TIMEOUT),
        ];
        run_config_test(config, &expected);
    }

    #[test]
    fn test_query_tag_with_method() {
        let mut config = base_config();
        config.insert("method".into(), "warehouse".into());

        config.insert(
            snowflake::QUERY_TAG_PARAM_KEY.into(),
            "custom-query-tag".into(),
        );
        let expected = [
            ("user", "U"),
            ("password", "P"),
            (snowflake::ACCOUNT, "A"),
            (snowflake::ROLE, "role"),
            (snowflake::WAREHOUSE, "warehouse"),
            (snowflake::APPLICATION_NAME, "dbt"),
            (snowflake::QUERY_TAG_PARAM_KEY, "custom-query-tag"),
            (snowflake::LOG_TRACING, "fatal"),
            (snowflake::LOGIN_TIMEOUT, DEFAULT_CONNECT_TIMEOUT),
            (snowflake::REQUEST_TIMEOUT, DEFAULT_REQUEST_TIMEOUT),
        ];
        run_config_test(config, &expected);
    }
}
