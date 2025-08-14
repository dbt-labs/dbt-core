use super::common::*;
use crate::{ErrorCode, FsResult, fs_err};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BigQueryFieldId {
    Method,
    Keyfile,
    Project,
    Dataset,
}

impl FieldId for BigQueryFieldId {
    fn config_key(&self) -> &'static str {
        match self {
            BigQueryFieldId::Method => "method",
            BigQueryFieldId::Keyfile => "keyfile",
            BigQueryFieldId::Project => "project",
            BigQueryFieldId::Dataset => "dataset",
        }
    }

    fn is_temporary(&self) -> bool {
        false // No temporary fields for BigQuery
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BigQueryAuthMethod {
    OAuth = 0,
    ServiceAccount = 1,
}

impl BigQueryAuthMethod {
    pub fn from_index(index: usize) -> Option<Self> {
        match index {
            0 => Some(BigQueryAuthMethod::OAuth),
            1 => Some(BigQueryAuthMethod::ServiceAccount),
            _ => None,
        }
    }

    pub fn options() -> Vec<&'static str> {
        vec!["oauth", "service-account"]
    }

    pub fn config_value(&self) -> &'static str {
        match self {
            BigQueryAuthMethod::OAuth => "oauth",
            BigQueryAuthMethod::ServiceAccount => "service-account",
        }
    }
}

trait BigQueryFieldStorage {
    fn get_auth_method(&self) -> Option<BigQueryAuthMethod>;
}

impl BigQueryFieldStorage for FieldStorage<BigQueryFieldId> {
    fn get_auth_method(&self) -> Option<BigQueryAuthMethod> {
        let auth_index = self.get_number(BigQueryFieldId::Method)?;
        BigQueryAuthMethod::from_index(auth_index)
    }
}

#[derive(Debug)]
pub struct BigQueryAuthMethodField {
    pub prompt: String,
}

impl FieldType<BigQueryFieldId> for BigQueryAuthMethodField {
    fn collect_input(
        &self,
        existing_config: &ConfigMap,
        _field_id: BigQueryFieldId,
        _storage: &FieldStorage<BigQueryFieldId>,
    ) -> FsResult<FieldValue> {
        let options = BigQueryAuthMethod::options();

        // Determine default based on existing config
        let default_index = if existing_config.get("keyfile").is_some() {
            1 // service-account
        } else {
            0 // oauth
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

pub fn bigquery_auth_method_field(prompt: &str) -> TypedField<BigQueryFieldId> {
    TypedField {
        id: BigQueryFieldId::Method,
        field_type: Box::new(BigQueryAuthMethodField {
            prompt: prompt.to_string(),
        }),
        condition: FieldCondition::Always,
    }
}

trait BigQueryConditions {
    fn if_auth_method(self, auth: BigQueryAuthMethod) -> Self;
}

impl BigQueryConditions for TypedField<BigQueryFieldId> {
    fn if_auth_method(self, auth: BigQueryAuthMethod) -> Self {
        self.if_number(BigQueryFieldId::Method, auth as usize)
    }
}

pub fn bigquery_config_fields() -> Vec<TypedField<BigQueryFieldId>> {
    vec![
        bigquery_auth_method_field("Choose authentication method"),
        input_field(
            BigQueryFieldId::Keyfile,
            "keyfile (path to service account json)",
            None,
        )
        .if_auth_method(BigQueryAuthMethod::ServiceAccount),
        input_field(BigQueryFieldId::Project, "project (GCP project id)", None),
        input_field(
            BigQueryFieldId::Dataset,
            "dataset (BigQuery dataset, e.g. analytics)",
            None,
        ),
    ]
}

pub struct BigQueryPostProcessor;

impl AdapterPostProcessor<BigQueryFieldId> for BigQueryPostProcessor {
    fn post_process_config(
        &self,
        config: &mut ConfigMap,
        storage: &FieldStorage<BigQueryFieldId>,
    ) -> FsResult<()> {
        if let Some(auth_method) = storage.get_auth_method() {
            config.insert(
                "method".to_string(),
                FieldValue::String(auth_method.config_value().to_string()),
            );
        }

        if !config.contains_key("threads") {
            config.insert("threads".to_string(), FieldValue::Number(16));
        }

        Ok(())
    }
}

pub fn setup_bigquery_profile(existing_config: Option<&ConfigMap>) -> FsResult<ConfigMap> {
    let processor = ExtendedConfigProcessor::new(bigquery_config_fields(), BigQueryPostProcessor);
    processor.process_config(existing_config)
}
