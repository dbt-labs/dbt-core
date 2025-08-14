use super::common::*;
use crate::{ErrorCode, FsResult, fs_err};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DatabricksFieldId {
    Schema,
    Host,
    HttpPath,
    HasCatalog,
    Catalog,
    AuthMethod,
    Token,
    ClientId,
    ClientSecret,
}

impl FieldId for DatabricksFieldId {
    fn config_key(&self) -> &'static str {
        match self {
            DatabricksFieldId::Schema => "schema",
            DatabricksFieldId::Host => "host",
            DatabricksFieldId::HttpPath => "http_path",
            DatabricksFieldId::HasCatalog => "has_catalog", // temporary field
            DatabricksFieldId::Catalog => "catalog",
            DatabricksFieldId::AuthMethod => "auth_method", // temporary field
            DatabricksFieldId::Token => "token",
            DatabricksFieldId::ClientId => "client_id",
            DatabricksFieldId::ClientSecret => "client_secret",
        }
    }

    fn is_temporary(&self) -> bool {
        matches!(
            self,
            DatabricksFieldId::HasCatalog | DatabricksFieldId::AuthMethod
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabricksAuthMethod {
    Token = 0,
    OAuth = 1,
}

impl DatabricksAuthMethod {
    pub fn from_index(index: usize) -> Option<Self> {
        match index {
            0 => Some(DatabricksAuthMethod::Token),
            1 => Some(DatabricksAuthMethod::OAuth),
            _ => None,
        }
    }

    pub fn options() -> Vec<&'static str> {
        vec!["token", "oauth"]
    }
}

trait DatabricksFieldStorage {
    fn get_auth_method(&self) -> Option<DatabricksAuthMethod>;
}

impl DatabricksFieldStorage for FieldStorage<DatabricksFieldId> {
    fn get_auth_method(&self) -> Option<DatabricksAuthMethod> {
        let auth_index = self.get_number(DatabricksFieldId::AuthMethod)?;
        DatabricksAuthMethod::from_index(auth_index)
    }
}

#[derive(Debug)]
pub struct DatabricksAuthMethodField {
    pub prompt: String,
}

impl FieldType<DatabricksFieldId> for DatabricksAuthMethodField {
    fn collect_input(
        &self,
        existing_config: &ConfigMap,
        _field_id: DatabricksFieldId,
        _storage: &FieldStorage<DatabricksFieldId>,
    ) -> FsResult<FieldValue> {
        let options = DatabricksAuthMethod::options();

        // Determine default based on existing config
        let default_index = if existing_config.get("token").is_some() {
            0 // token
        } else if existing_config
            .get("auth_type")
            .and_then(|v| v.as_string())
            .is_some_and(|s| s.eq_ignore_ascii_case("oauth"))
        {
            1 // oauth
        } else {
            0 // default to token
        };

        let result = dialoguer::Select::new()
            .with_prompt(&self.prompt)
            .items(&options)
            .default(default_index)
            .interact()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get auth method: {}", e))?;

        Ok(FieldValue::Number(result))
    }
}

pub fn databricks_auth_method_field(prompt: &str) -> TypedField<DatabricksFieldId> {
    TypedField {
        id: DatabricksFieldId::AuthMethod,
        field_type: Box::new(DatabricksAuthMethodField {
            prompt: prompt.to_string(),
        }),
        condition: FieldCondition::Always,
    }
}

trait DatabricksConditions {
    fn if_auth_method(self, auth: DatabricksAuthMethod) -> Self;
}

impl DatabricksConditions for TypedField<DatabricksFieldId> {
    fn if_auth_method(self, auth: DatabricksAuthMethod) -> Self {
        self.if_number(DatabricksFieldId::AuthMethod, auth as usize)
    }
}

pub fn databricks_config_fields() -> Vec<TypedField<DatabricksFieldId>> {
    vec![
        input_field(DatabricksFieldId::Schema, "Schema (schema name)", None),
        input_field(
            DatabricksFieldId::Host,
            "host (e.g. yourorg.databrickshost.com)",
            None,
        ),
        input_field(
            DatabricksFieldId::HttpPath,
            "http_path (e.g. /sql/your/http/path)",
            None,
        ),
        confirm_field(
            DatabricksFieldId::HasCatalog,
            "Specify a catalog? (Unity Catalog)",
            false,
        ),
        input_field(DatabricksFieldId::Catalog, "catalog (catalog_name)", None)
            .if_bool(DatabricksFieldId::HasCatalog, true),
        databricks_auth_method_field("Choose authentication method"),
        password_field(DatabricksFieldId::Token, "token (personal access token)")
            .if_auth_method(DatabricksAuthMethod::Token),
        input_field(
            DatabricksFieldId::ClientId,
            "client_id (oauth_client_id)",
            None,
        )
        .if_auth_method(DatabricksAuthMethod::OAuth),
        password_field(DatabricksFieldId::ClientSecret, "client_secret")
            .if_auth_method(DatabricksAuthMethod::OAuth),
    ]
}

pub struct DatabricksPostProcessor;

impl AdapterPostProcessor<DatabricksFieldId> for DatabricksPostProcessor {
    fn post_process_config(
        &self,
        config: &mut ConfigMap,
        storage: &FieldStorage<DatabricksFieldId>,
    ) -> FsResult<()> {
        if let Some(auth_method) = storage.get_auth_method() {
            match auth_method {
                DatabricksAuthMethod::OAuth => {
                    config.insert(
                        "auth_type".to_string(),
                        FieldValue::String("oauth".to_string()),
                    );
                }
                DatabricksAuthMethod::Token => {
                    // Token auth doesn't need explicit auth_type
                }
            }
        }

        if !config.contains_key("threads") {
            config.insert("threads".to_string(), FieldValue::Number(16));
        }

        Ok(())
    }
}

/// Main entry point for Databricks configuration with strong typing
pub fn setup_databricks_profile(existing_config: Option<&ConfigMap>) -> FsResult<ConfigMap> {
    let processor =
        ExtendedConfigProcessor::new(databricks_config_fields(), DatabricksPostProcessor);
    processor.process_config(existing_config)
}
