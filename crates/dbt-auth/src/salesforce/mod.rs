use std::fs;

use crate::{AdapterConfig, Auth, AuthError};

use database::Builder as DatabaseBuilder;
use dbt_xdbc::{Backend, database, salesforce};
use strum::{AsRefStr, IntoEnumIterator};
use strum_macros::EnumIter;

/// Main Salesforce authentication implementation
#[derive(Debug, Default)]
pub struct SalesforceAuth;

/// Required options for Salesforce authentication
#[derive(EnumIter, AsRefStr)]
#[strum(serialize_all = "snake_case")]
enum RequiredConfigOptions {
    Username,
    ClientId,
    LoginUrl,
}

impl SalesforceAuth {
    /// Configure database builder using the specified authentication method
    fn configure_builder_with_method(
        &self,
        config: &AdapterConfig,
        method: String,
    ) -> Result<DatabaseBuilder, AuthError> {
        let mut builder = DatabaseBuilder::new(self.backend());

        for option in RequiredConfigOptions::iter() {
            match option {
                RequiredConfigOptions::Username => {
                    builder.with_named_option(
                        salesforce::USERNAME,
                        config.require_string(option.as_ref())?,
                    )?;
                }
                RequiredConfigOptions::ClientId => {
                    builder.with_named_option(
                        salesforce::CLIENT_ID,
                        config.require_string(option.as_ref())?,
                    )?;
                }
                RequiredConfigOptions::LoginUrl => {
                    builder.with_named_option(
                        salesforce::LOGIN_URL,
                        config.require_string("login_url")?,
                    )?;
                }
            }
        }

        AuthMethod::new(config, &method)?.configure(&mut builder)?;
        Ok(builder)
    }
}

/// JWT Bearer authentication for Salesforce
#[derive(Debug)]
struct JwtBearerAuth {
    private_key: String,
}

impl JwtBearerAuth {
    fn new(config: &AdapterConfig) -> Result<Self, AuthError> {
        let private_key = config.get_string("private_key");
        let private_key_path = config.get_string("private_key_path");

        // Validate that exactly one private key source is provided
        let private_key = match (private_key, private_key_path) {
            (Some(_), Some(_)) => Err(AuthError::config(
                "Cannot specify both 'private_key' and 'private_key_path'. Choose one.",
            )),
            (None, None) => Err(AuthError::config(
                "JWT authentication requires either 'private_key' or 'private_key_path'.",
            )),
            (Some(private_key), None) => Ok(private_key.to_string()),
            (None, Some(private_key_path)) => {
                let private_key = fs::read_to_string(private_key_path.as_ref())?;
                Ok(private_key)
            }
        }?;

        Ok(JwtBearerAuth { private_key })
    }
}

/// Username/Password authentication for Salesforce
#[derive(Debug)]
struct UsernamePasswordAuth {
    client_secret: String,
    password: String,
}

impl UsernamePasswordAuth {
    fn new(config: &AdapterConfig) -> Result<Self, AuthError> {
        let client_secret = config.require_string("client_secret")?.to_string();
        let password = config.require_string("password")?.to_string();

        Ok(UsernamePasswordAuth {
            client_secret,
            password,
        })
    }
}

/// Enum representing different Salesforce authentication methods
#[derive(Debug)]
enum AuthMethod {
    Jwt(JwtBearerAuth),
    UsernamePassword(UsernamePasswordAuth),
}

impl AuthMethod {
    pub fn new(config: &AdapterConfig, method: &str) -> Result<Self, AuthError> {
        match method {
            "jwt_bearer" => JwtBearerAuth::new(config).map(Self::Jwt),
            "username_password" => UsernamePasswordAuth::new(config).map(Self::UsernamePassword),
            unsupported_method => Err(AuthError::config(format!(
                "Unsupported authentication method '{unsupported_method}' for Salesforce. Supported methods: jwt_bearer, username_password"
            ))),
        }
    }

    pub fn configure(self, builder: &mut DatabaseBuilder) -> Result<(), AuthError> {
        match self {
            AuthMethod::Jwt(auth) => {
                builder.with_named_option(salesforce::AUTH_TYPE, salesforce::auth_type::JWT)?;

                builder.with_named_option(salesforce::JWT_PRIVATE_KEY, auth.private_key)?;
            }
            AuthMethod::UsernamePassword(auth) => {
                builder.with_named_option(
                    salesforce::AUTH_TYPE,
                    salesforce::auth_type::USERNAME_PASSWORD,
                )?;

                builder.with_named_option(salesforce::CLIENT_SECRET, auth.client_secret)?;
                builder.with_named_option(salesforce::PASSWORD, auth.password)?;
            }
        }

        Ok(())
    }
}

impl Auth for SalesforceAuth {
    fn backend(&self) -> Backend {
        Backend::Salesforce
    }

    fn configure(&self, config: &AdapterConfig) -> Result<DatabaseBuilder, AuthError> {
        // Check if an explicit method is specified
        let method = config.require_string("method")?.to_string();
        self.configure_builder_with_method(config, method)
    }
}
