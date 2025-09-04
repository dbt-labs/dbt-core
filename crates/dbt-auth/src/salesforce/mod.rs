use std::borrow::Cow;
use std::fs;

use crate::{AdapterConfig, Auth, AuthError};

use dbt_xdbc::salesforce::auth_type;
use dbt_xdbc::{Backend, database, salesforce};

pub struct SalesforceAuth;

/// Salesforce authentication methods
enum AuthMethod {
    /// JWT Bearer authentication for Salesforce
    JwtBearer { private_key: String },
    /// Username/Password authentication
    UsernamePassword {
        client_secret: String,
        password: String,
    },
}

impl SalesforceAuth {
    /// Configure the database builder with the selected authentication method.
    ///
    /// The required options have already been set at this point (e.g. client_id, username).
    fn configure_with_method(
        method: AuthMethod,
        builder: &mut database::Builder,
    ) -> Result<(), AuthError> {
        match method {
            AuthMethod::JwtBearer { private_key } => {
                builder.with_named_option(salesforce::AUTH_TYPE, auth_type::JWT)?;
                builder.with_named_option(salesforce::JWT_PRIVATE_KEY, private_key)?;
            }
            AuthMethod::UsernamePassword {
                client_secret,
                password,
            } => {
                builder.with_named_option(salesforce::AUTH_TYPE, auth_type::USERNAME_PASSWORD)?;
                builder.with_named_option(salesforce::CLIENT_SECRET, client_secret)?;
                builder.with_named_option(salesforce::PASSWORD, password)?;
            }
        }

        Ok(())
    }
}

impl Auth for SalesforceAuth {
    fn backend(&self) -> Backend {
        Backend::Salesforce
    }

    fn configure(&self, config: &AdapterConfig) -> Result<database::Builder, AuthError> {
        // Require a "method" option to guide the interpretation of other options.
        // We can enforce this because Salesforce is a new adapter without legacy users.
        let method = config.require_string("method")?;

        let mut builder = database::Builder::new(Backend::Salesforce);
        for (opt_name, adbc_opt_name) in [
            ("username", salesforce::USERNAME),
            ("client_id", salesforce::CLIENT_ID),
            ("login_url", salesforce::LOGIN_URL),
        ] {
            let opt_value = config.require_string(opt_name)?;
            builder.with_named_option(adbc_opt_name, opt_value)?;
        }

        let auth_method = match method.as_ref() {
            "jwt_bearer" => {
                let private_key = config.get_string("private_key");
                let private_key_path = config.get_string("private_key_path");

                // Validate that exactly one private key source is provided
                let private_key = match (private_key, private_key_path) {
                    (Some(_), Some(_)) => {
                        return Err(AuthError::config(
                            "Cannot specify both 'private_key' and 'private_key_path'. Choose one.",
                        ));
                    }
                    (None, None) => {
                        return Err(AuthError::config(
                            "JWT authentication requires either 'private_key' or 'private_key_path'.",
                        ));
                    }
                    (Some(private_key), None) => private_key,
                    (None, Some(private_key_path)) => {
                        let private_key = fs::read_to_string(private_key_path.as_ref())?;
                        Cow::Owned(private_key)
                    }
                };
                AuthMethod::JwtBearer {
                    private_key: private_key.to_string(),
                }
            }
            "username_password" => {
                let client_secret = config.require_string("client_secret")?;
                let password = config.require_string("password")?;

                AuthMethod::UsernamePassword {
                    client_secret: client_secret.to_string(),
                    password: password.to_string(),
                }
            }
            unsupported_method => {
                return Err(AuthError::config(format!(
                    "Unsupported authentication method '{unsupported_method}' for Salesforce.
 Supported methods: 'jwt_bearer', 'username_password'"
                )));
            }
        };

        Self::configure_with_method(auth_method, &mut builder)?;

        Ok(builder)
    }
}
