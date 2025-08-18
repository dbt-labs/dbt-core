use base64::prelude::*;
use serde::{Deserialize, Serialize};

use crate::AuthError;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct BigQueryAuthConfig {
    pub(crate) database: String,
    pub(crate) schema: String,
    pub(crate) execution_project: Option<String>,
    pub(crate) location: Option<String>,
    pub(crate) __method_config__: BigQueryAuthMethod,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct KeyFileJson {
    #[serde(rename = "type")]
    pub(crate) file_type: String,
    pub(crate) project_id: String,
    pub(crate) private_key_id: String,
    pub(crate) private_key: String,
    pub(crate) client_email: String,
    pub(crate) client_id: String,
    pub(crate) auth_uri: String,
    pub(crate) token_uri: String,
    pub(crate) auth_provider_x509_cert_url: String,
    pub(crate) client_x509_cert_url: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum KeyFileJsonVariants {
    Base64(String),
    Object(KeyFileJson),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct OAuth;

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(untagged)]
pub enum OAuthSecretsVariants {
    RefreshToken(OAuthSecretsRefreshToken),
    TemporaryToken(OAuthSecretsTemporaryToken),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct OAuthSecretsRefreshToken {
    pub(crate) refresh_token: String,
    pub(crate) client_id: String,
    pub(crate) client_secret: String,
    pub(crate) token_uri: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct OAuthSecretsTemporaryToken {
    pub(crate) token: String,
}

#[allow(clippy::large_enum_variant)]
#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "method", rename_all = "kebab-case")]
// deny_unknown_fields is not used to be conformed with dbt behavior
pub enum BigQueryAuthMethod {
    ServiceAccount { keyfile: String },
    ServiceAccountJson { keyfile_json: KeyFileJsonVariants },
    OauthSecrets(OAuthSecretsVariants),
    Oauth {}, // interactive gcloud login
}

impl TryFrom<KeyFileJsonVariants> for KeyFileJson {
    type Error = AuthError;
    fn try_from(value: KeyFileJsonVariants) -> Result<Self, Self::Error> {
        match value {
            KeyFileJsonVariants::Base64(base64_str) => {
                let decoded = BASE64_STANDARD.decode(base64_str).map_err(|err| {
                    AuthError::config(format!("Error decoding keyfile json from base64: '{err}'"))
                })?;
                let keyfile_json: KeyFileJson = serde_json::from_slice(&decoded)?;
                Ok(keyfile_json)
            }
            KeyFileJsonVariants::Object(obj) => Ok(obj),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keyfile_json_invalid_base64() {
        let fake_b64 = KeyFileJsonVariants::Base64("fake_base64".to_string());
        let check: Result<KeyFileJson, AuthError> = fake_b64.try_into();
        assert!(check.is_err(), "Expected error on invalid base64");
    }
}
