use super::common::*;
use crate::{ErrorCode, FsResult, fs_err};
use dialoguer::Select;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SnowflakeFieldId {
    Account,
    User,
    AuthMethod,
    Password,
    NeedsMfa,
    UseKeyPath,
    PrivateKeyPath,
    PrivateKey,
    PassphraseNeeded,
    PrivateKeyPassphrase,
    OauthClientId,
    OauthClientSecret,
    Token,
    Role,
    Database,
    Warehouse,
    Schema,
}

impl FieldId for SnowflakeFieldId {
    fn config_key(&self) -> &'static str {
        match self {
            SnowflakeFieldId::Account => "account",
            SnowflakeFieldId::User => "user",
            SnowflakeFieldId::AuthMethod => "auth_method", // temporary field
            SnowflakeFieldId::Password => "password",
            SnowflakeFieldId::NeedsMfa => "needs_mfa", // temporary field
            SnowflakeFieldId::UseKeyPath => "use_key_path", // temporary field
            SnowflakeFieldId::PrivateKeyPath => "private_key_path",
            SnowflakeFieldId::PrivateKey => "private_key",
            SnowflakeFieldId::PassphraseNeeded => "passphrase_needed", // temporary field
            SnowflakeFieldId::PrivateKeyPassphrase => "private_key_passphrase",
            SnowflakeFieldId::OauthClientId => "oauth_client_id",
            SnowflakeFieldId::OauthClientSecret => "oauth_client_secret",
            SnowflakeFieldId::Token => "token",
            SnowflakeFieldId::Role => "role",
            SnowflakeFieldId::Database => "database",
            SnowflakeFieldId::Warehouse => "warehouse",
            SnowflakeFieldId::Schema => "schema",
        }
    }

    fn is_temporary(&self) -> bool {
        matches!(
            self,
            SnowflakeFieldId::AuthMethod
                | SnowflakeFieldId::NeedsMfa
                | SnowflakeFieldId::UseKeyPath
                | SnowflakeFieldId::PassphraseNeeded
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMethod {
    Password = 0,
    ExternalBrowser = 1,
    Keypair = 2,
    OAuth = 3,
}

impl AuthMethod {
    pub fn from_index(index: usize) -> Option<Self> {
        match index {
            0 => Some(AuthMethod::Password),
            1 => Some(AuthMethod::ExternalBrowser),
            2 => Some(AuthMethod::Keypair),
            3 => Some(AuthMethod::OAuth),
            _ => None,
        }
    }

    pub fn options() -> Vec<&'static str> {
        vec![
            "password",
            "externalbrowser (open browser SSO)",
            "keypair",
            "oauth",
        ]
    }
}

trait SnowflakeFieldStorage {
    fn get_auth_method(&self) -> Option<AuthMethod>;
}

impl SnowflakeFieldStorage for FieldStorage<SnowflakeFieldId> {
    fn get_auth_method(&self) -> Option<AuthMethod> {
        let auth_index = self.get_number(SnowflakeFieldId::AuthMethod)?;
        AuthMethod::from_index(auth_index)
    }
}

#[derive(Debug)]
pub struct AuthMethodField {
    pub prompt: String,
}

impl FieldType<SnowflakeFieldId> for AuthMethodField {
    fn collect_input(
        &self,
        existing_config: &ConfigMap,
        _field_id: SnowflakeFieldId,
        _storage: &FieldStorage<SnowflakeFieldId>,
    ) -> FsResult<FieldValue> {
        let options = AuthMethod::options();

        // Determine default based on existing config
        let default_index = {
            if existing_config
                .get("authenticator")
                .and_then(|v| v.as_string())
                .is_some_and(|s| s.eq_ignore_ascii_case("externalbrowser"))
            {
                1
            } else if existing_config
                .get("private_key_path")
                .or_else(|| existing_config.get("private_key"))
                .is_some()
            {
                2
            } else if existing_config
                .get("authenticator")
                .and_then(|v| v.as_string())
                .is_some_and(|s| s.eq_ignore_ascii_case("oauth"))
            {
                3
            } else {
                0
            }
        };

        let result = Select::new()
            .with_prompt(&self.prompt)
            .items(&options)
            .default(default_index)
            .interact()
            .map_err(|e| {
                fs_err!(
                    ErrorCode::IoError,
                    "Failed to get authentication method: {}",
                    e
                )
            })?;

        Ok(FieldValue::Number(result))
    }
}

pub fn auth_method_field(prompt: &str) -> TypedField<SnowflakeFieldId> {
    TypedField {
        id: SnowflakeFieldId::AuthMethod,
        field_type: Box::new(AuthMethodField {
            prompt: prompt.to_string(),
        }),
        condition: FieldCondition::Always,
    }
}

trait SnowflakeConditions {
    fn if_auth_method(self, auth: AuthMethod) -> Self;
}

impl SnowflakeConditions for TypedField<SnowflakeFieldId> {
    fn if_auth_method(self, auth: AuthMethod) -> Self {
        self.if_number(SnowflakeFieldId::AuthMethod, auth as usize)
    }
}

pub fn snowflake_config_fields() -> Vec<TypedField<SnowflakeFieldId>> {
    vec![
        input_field(
            SnowflakeFieldId::Account,
            "Snowflake account (e.g. xy12345 or org-account)",
            None,
        ),
        input_field(SnowflakeFieldId::User, "Username (e.g. jane.doe)", None),
        auth_method_field("Choose authentication method"),
        // Password authentication fields
        password_field(SnowflakeFieldId::Password, "password").if_auth_method(AuthMethod::Password),
        confirm_field(
            SnowflakeFieldId::NeedsMfa,
            "Do you use MFA for username/password?",
            false,
        )
        .if_auth_method(AuthMethod::Password),
        // Keypair authentication fields
        confirm_field(
            SnowflakeFieldId::UseKeyPath,
            "Provide private_key by file path? (No = paste key inline)",
            true,
        )
        .if_auth_method(AuthMethod::Keypair),
        input_field(
            SnowflakeFieldId::PrivateKeyPath,
            "private_key_path (e.g. ~/.ssh/snowflake.p8)",
            None,
        )
        .if_auth_method(AuthMethod::Keypair)
        .if_bool(SnowflakeFieldId::UseKeyPath, true),
        password_field(
            SnowflakeFieldId::PrivateKey,
            "private_key (paste PEM contents)",
        )
        .if_auth_method(AuthMethod::Keypair)
        .if_bool(SnowflakeFieldId::UseKeyPath, false),
        confirm_field(
            SnowflakeFieldId::PassphraseNeeded,
            "Is the private key encrypted with a passphrase?",
            false,
        )
        .if_auth_method(AuthMethod::Keypair),
        password_field(
            SnowflakeFieldId::PrivateKeyPassphrase,
            "private_key_passphrase",
        )
        .if_auth_method(AuthMethod::Keypair)
        .if_bool(SnowflakeFieldId::PassphraseNeeded, true),
        // OAuth authentication fields
        input_field(SnowflakeFieldId::OauthClientId, "oauth_client_id", None)
            .if_auth_method(AuthMethod::OAuth),
        password_field(SnowflakeFieldId::OauthClientSecret, "oauth_client_secret")
            .if_auth_method(AuthMethod::OAuth),
        password_field(SnowflakeFieldId::Token, "token (OAuth refresh token)")
            .if_auth_method(AuthMethod::OAuth),
        // Common fields
        input_field(SnowflakeFieldId::Role, "Role (e.g. TRANSFORMER)", None),
        input_field(
            SnowflakeFieldId::Database,
            "Database (e.g. ANALYTICS)",
            None,
        ),
        input_field(
            SnowflakeFieldId::Warehouse,
            "Warehouse (e.g. TRANSFORMING)",
            None,
        ),
        input_field(
            SnowflakeFieldId::Schema,
            "Schema (dbt schema, e.g. analytics)",
            None,
        ),
    ]
}

pub struct SnowflakePostProcessor;

impl AdapterPostProcessor<SnowflakeFieldId> for SnowflakePostProcessor {
    fn post_process_config(
        &self,
        config: &mut ConfigMap,
        storage: &FieldStorage<SnowflakeFieldId>,
    ) -> FsResult<()> {
        // Set authenticator based on auth method and additional flags
        match storage.get_auth_method() {
            Some(AuthMethod::Password) => {
                if storage.get_bool(SnowflakeFieldId::NeedsMfa) == Some(true) {
                    config.insert(
                        "authenticator".to_string(),
                        FieldValue::String("username_password_mfa".to_string()),
                    );
                }
            }
            Some(AuthMethod::ExternalBrowser) => {
                config.insert(
                    "authenticator".to_string(),
                    FieldValue::String("externalbrowser".to_string()),
                );
            }
            Some(AuthMethod::OAuth) => {
                config.insert(
                    "authenticator".to_string(),
                    FieldValue::String("oauth".to_string()),
                );
            }
            Some(AuthMethod::Keypair) | None => {
                // Keypair doesn't need explicit authenticator, None is default password
            }
        }

        // Set default threads if not present
        if !config.contains_key("threads") {
            config.insert("threads".to_string(), FieldValue::Number(16));
        }

        Ok(())
    }
}

pub fn setup_snowflake_profile(existing_config: Option<&ConfigMap>) -> FsResult<ConfigMap> {
    let processor = ExtendedConfigProcessor::new(snowflake_config_fields(), SnowflakePostProcessor);
    processor.process_config(existing_config)
}
