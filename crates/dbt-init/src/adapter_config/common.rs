use crate::{ErrorCode, FsResult, fs_err};
use dialoguer::{Confirm, Input, Password, Select};
use serde::{Serialize, Serializer};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, serde::Deserialize)]
pub enum FieldValue {
    String(String),
    Number(usize),
    Bool(bool),
    Null,
}

impl Serialize for FieldValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            FieldValue::String(s) => serializer.serialize_str(s),
            FieldValue::Number(n) => serializer.serialize_u64(*n as u64),
            FieldValue::Bool(b) => serializer.serialize_bool(*b),
            FieldValue::Null => serializer.serialize_none(),
        }
    }
}

impl FieldValue {
    pub fn from_yaml(value: &dbt_serde_yaml::Value) -> Self {
        match value {
            dbt_serde_yaml::Value::String(s, _) => FieldValue::String(s.clone()),
            dbt_serde_yaml::Value::Number(n, _) => {
                if let Some(i) = n.as_u64() {
                    FieldValue::Number(i as usize)
                } else {
                    // Fallback for non-integer numbers
                    FieldValue::String(n.to_string())
                }
            }
            dbt_serde_yaml::Value::Bool(b, _) => FieldValue::Bool(*b),
            dbt_serde_yaml::Value::Null(_) => FieldValue::Null,
            _ => {
                if let Ok(s) = dbt_serde_yaml::to_string(value) {
                    FieldValue::String(s)
                } else {
                    FieldValue::Null
                }
            }
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            FieldValue::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<&str> {
        match self {
            FieldValue::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_number(&self) -> Option<usize> {
        match self {
            FieldValue::Number(n) => Some(*n),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, FieldValue::Null)
    }
}

pub trait FieldId: std::fmt::Debug + Clone + Copy + PartialEq + Eq + std::hash::Hash {
    fn config_key(&self) -> &'static str;

    /// Fields that should be removed from the final config (temporary fields)
    fn is_temporary(&self) -> bool;
}

#[derive(Debug, Default)]
pub struct FieldStorage<T: FieldId> {
    values: HashMap<T, FieldValue>,
}

impl<T: FieldId> FieldStorage<T> {
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
        }
    }

    pub fn insert(&mut self, field: T, value: FieldValue) {
        self.values.insert(field, value);
    }

    pub fn get(&self, field: T) -> Option<&FieldValue> {
        self.values.get(&field)
    }

    pub fn get_bool(&self, field: T) -> Option<bool> {
        self.get(field)?.as_bool()
    }

    pub fn get_string(&self, field: T) -> Option<&str> {
        self.get(field)?.as_string()
    }

    pub fn get_number(&self, field: T) -> Option<usize> {
        self.get(field)?.as_number()
    }

    /// Convert to ConfigMap for native typed config
    pub fn to_config(&self) -> ConfigMap {
        let mut config = ConfigMap::new();

        for (field_id, field_value) in &self.values {
            if !field_id.is_temporary() {
                config.insert(field_id.config_key().to_string(), field_value.clone());
            }
        }

        config
    }
}

#[derive(Debug, Clone)]
pub enum FieldCondition<T: FieldId> {
    Always,
    IfBool { field: T, value: bool },
    IfNumber { field: T, value: usize },
    IfNumberOneOf { field: T, values: Vec<usize> },
    And(Vec<FieldCondition<T>>),
}

impl<T: FieldId> FieldCondition<T> {
    pub fn should_show(&self, storage: &FieldStorage<T>) -> bool {
        match self {
            FieldCondition::Always => true,
            FieldCondition::IfBool { field, value } => storage.get_bool(*field) == Some(*value),
            FieldCondition::IfNumber { field, value } => storage.get_number(*field) == Some(*value),
            FieldCondition::IfNumberOneOf { field, values } => {
                if let Some(current) = storage.get_number(*field) {
                    values.contains(&current)
                } else {
                    false
                }
            }
            FieldCondition::And(conditions) => conditions
                .iter()
                .all(|condition| condition.should_show(storage)),
        }
    }
}

pub trait FieldType<T: FieldId>: std::fmt::Debug {
    fn collect_input(
        &self,
        existing_config: &ConfigMap,
        field_id: T,
        storage: &FieldStorage<T>,
    ) -> FsResult<FieldValue>;
}

/// Input field for string values
#[derive(Debug)]
pub struct InputField {
    pub prompt: String,
    pub default_value: Option<String>,
}

impl<T: FieldId> FieldType<T> for InputField {
    fn collect_input(
        &self,
        existing_config: &ConfigMap,
        field_id: T,
        _storage: &FieldStorage<T>,
    ) -> FsResult<FieldValue> {
        let default = existing_config
            .get(field_id.config_key())
            .and_then(|v| v.as_string())
            .map(|s| s.to_string())
            .or_else(|| self.default_value.clone())
            .unwrap_or_default();

        let result = Input::new()
            .with_prompt(&self.prompt)
            .default(default)
            .interact_text()
            .map_err(|e| {
                fs_err!(
                    ErrorCode::IoError,
                    "Failed to get {}: {}",
                    field_id.config_key(),
                    e
                )
            })?;

        Ok(FieldValue::String(result))
    }
}

#[derive(Debug)]
pub struct PasswordField {
    pub prompt: String,
}

impl<T: FieldId> FieldType<T> for PasswordField {
    fn collect_input(
        &self,
        _existing_config: &ConfigMap,
        field_id: T,
        _storage: &FieldStorage<T>,
    ) -> FsResult<FieldValue> {
        let result = Password::new()
            .with_prompt(&self.prompt)
            .interact()
            .map_err(|e| {
                fs_err!(
                    ErrorCode::IoError,
                    "Failed to get {}: {}",
                    field_id.config_key(),
                    e
                )
            })?;

        Ok(FieldValue::String(result))
    }
}

#[derive(Debug)]
pub struct SelectField {
    pub prompt: String,
    pub options: Vec<String>,
    pub default_index: usize,
}

impl<T: FieldId> FieldType<T> for SelectField {
    fn collect_input(
        &self,
        _existing_config: &ConfigMap,
        field_id: T,
        _storage: &FieldStorage<T>,
    ) -> FsResult<FieldValue> {
        let result = Select::new()
            .with_prompt(&self.prompt)
            .items(&self.options)
            .default(self.default_index)
            .interact()
            .map_err(|e| {
                fs_err!(
                    ErrorCode::IoError,
                    "Failed to get {}: {}",
                    field_id.config_key(),
                    e
                )
            })?;

        Ok(FieldValue::Number(result))
    }
}

#[derive(Debug)]
pub struct ConfirmField {
    pub prompt: String,
    pub default: bool,
}

impl<T: FieldId> FieldType<T> for ConfirmField {
    fn collect_input(
        &self,
        _existing_config: &ConfigMap,
        field_id: T,
        _storage: &FieldStorage<T>,
    ) -> FsResult<FieldValue> {
        let result = Confirm::new()
            .with_prompt(&self.prompt)
            .default(self.default)
            .interact()
            .map_err(|e| {
                fs_err!(
                    ErrorCode::IoError,
                    "Failed to get {}: {}",
                    field_id.config_key(),
                    e
                )
            })?;

        Ok(FieldValue::Bool(result))
    }
}

#[derive(Debug)]
pub struct TypedField<T: FieldId> {
    pub id: T,
    pub field_type: Box<dyn FieldType<T>>,
    pub condition: FieldCondition<T>,
}

pub fn input_field<T: FieldId>(id: T, prompt: &str, default_value: Option<&str>) -> TypedField<T> {
    TypedField {
        id,
        field_type: Box::new(InputField {
            prompt: prompt.to_string(),
            default_value: default_value.map(|s| s.to_string()),
        }),
        condition: FieldCondition::Always,
    }
}

pub fn password_field<T: FieldId>(id: T, prompt: &str) -> TypedField<T> {
    TypedField {
        id,
        field_type: Box::new(PasswordField {
            prompt: prompt.to_string(),
        }),
        condition: FieldCondition::Always,
    }
}

pub fn select_field<T: FieldId>(
    id: T,
    prompt: &str,
    options: Vec<&str>,
    default_index: usize,
) -> TypedField<T> {
    TypedField {
        id,
        field_type: Box::new(SelectField {
            prompt: prompt.to_string(),
            options: options.iter().map(|s| s.to_string()).collect(),
            default_index,
        }),
        condition: FieldCondition::Always,
    }
}

pub fn confirm_field<T: FieldId>(id: T, prompt: &str, default: bool) -> TypedField<T> {
    TypedField {
        id,
        field_type: Box::new(ConfirmField {
            prompt: prompt.to_string(),
            default,
        }),
        condition: FieldCondition::Always,
    }
}

pub trait WithCondition<T: FieldId> {
    fn with_condition(self, condition: FieldCondition<T>) -> Self;
    fn if_bool(self, field: T, value: bool) -> Self;
    fn if_number(self, field: T, value: usize) -> Self;
    fn if_number_one_of(self, field: T, values: Vec<usize>) -> Self;
}

impl<T: FieldId> WithCondition<T> for TypedField<T> {
    fn with_condition(mut self, condition: FieldCondition<T>) -> Self {
        self.condition = condition;
        self
    }

    fn if_bool(mut self, field: T, value: bool) -> Self {
        let new_condition = FieldCondition::IfBool { field, value };
        self.condition = match self.condition {
            FieldCondition::Always => new_condition,
            existing => FieldCondition::And(vec![existing, new_condition]),
        };
        self
    }

    fn if_number(mut self, field: T, value: usize) -> Self {
        let new_condition = FieldCondition::IfNumber { field, value };
        self.condition = match self.condition {
            FieldCondition::Always => new_condition,
            existing => FieldCondition::And(vec![existing, new_condition]),
        };
        self
    }

    fn if_number_one_of(mut self, field: T, values: Vec<usize>) -> Self {
        let new_condition = FieldCondition::IfNumberOneOf { field, values };
        self.condition = match self.condition {
            FieldCondition::Always => new_condition,
            existing => FieldCondition::And(vec![existing, new_condition]),
        };
        self
    }
}

pub struct TypedConfigProcessor<T: FieldId> {
    fields: Vec<TypedField<T>>,
}

impl<T: FieldId> TypedConfigProcessor<T> {
    pub fn new(fields: Vec<TypedField<T>>) -> Self {
        Self { fields }
    }

    pub fn process_config(&self, existing_config: Option<&ConfigMap>) -> FsResult<ConfigMap> {
        let existing = existing_config.cloned().unwrap_or_default();
        let mut storage = FieldStorage::new();

        for field in &self.fields {
            if !field.condition.should_show(&storage) {
                continue;
            }

            let value = field
                .field_type
                .collect_input(&existing, field.id, &storage)?;
            storage.insert(field.id, value);
        }

        let mut config = storage.to_config();
        self.post_process_config(&mut config, &storage)?;

        Ok(config)
    }

    /// Default post-processing that can be overridden by specific adapters
    fn post_process_config(
        &self,
        config: &mut ConfigMap,
        _storage: &FieldStorage<T>,
    ) -> FsResult<()> {
        // Set default threads if not present
        if !config.contains_key("threads") {
            config.insert("threads".to_string(), FieldValue::Number(16));
        }

        Ok(())
    }
}

pub trait AdapterPostProcessor<T: FieldId> {
    fn post_process_config(
        &self,
        config: &mut ConfigMap,
        storage: &FieldStorage<T>,
    ) -> FsResult<()>;
}

/// Extended processor that supports custom post-processing
pub struct ExtendedConfigProcessor<T: FieldId, P: AdapterPostProcessor<T>> {
    fields: Vec<TypedField<T>>,
    post_processor: P,
}

impl<T: FieldId, P: AdapterPostProcessor<T>> ExtendedConfigProcessor<T, P> {
    pub fn new(fields: Vec<TypedField<T>>, post_processor: P) -> Self {
        Self {
            fields,
            post_processor,
        }
    }

    pub fn process_config(&self, existing_config: Option<&ConfigMap>) -> FsResult<ConfigMap> {
        let existing = existing_config.cloned().unwrap_or_default();
        let mut storage = FieldStorage::new();

        for field in &self.fields {
            if !field.condition.should_show(&storage) {
                continue;
            }

            let value = field
                .field_type
                .collect_input(&existing, field.id, &storage)?;
            storage.insert(field.id, value);
        }

        let mut config = storage.to_config();
        self.post_processor
            .post_process_config(&mut config, &storage)?;

        Ok(config)
    }
}

pub type ConfigMap = HashMap<String, FieldValue>;

#[derive(Debug, Clone)]
pub enum AdapterConfig {
    Snowflake(ConfigMap),
    Postgres(ConfigMap),
    BigQuery(ConfigMap),
    Redshift(ConfigMap),
    Databricks(ConfigMap),
}

impl AdapterConfig {
    pub fn adapter_type(&self) -> &'static str {
        match self {
            AdapterConfig::Snowflake(_) => "snowflake",
            AdapterConfig::Postgres(_) => "postgres",
            AdapterConfig::BigQuery(_) => "bigquery",
            AdapterConfig::Redshift(_) => "redshift",
            AdapterConfig::Databricks(_) => "databricks",
        }
    }

    pub fn config(&self) -> &ConfigMap {
        match self {
            AdapterConfig::Snowflake(config) => config,
            AdapterConfig::Postgres(config) => config,
            AdapterConfig::BigQuery(config) => config,
            AdapterConfig::Redshift(config) => config,
            AdapterConfig::Databricks(config) => config,
        }
    }

    pub fn from_type_and_yaml_config(
        adapter_type: &str,
        yaml_config: &dbt_serde_yaml::Value,
    ) -> Result<Self, String> {
        let config = Self::extract_config_from_yaml(yaml_config)?;

        match adapter_type {
            "snowflake" => Ok(AdapterConfig::Snowflake(config)),
            "postgres" => Ok(AdapterConfig::Postgres(config)),
            "bigquery" => Ok(AdapterConfig::BigQuery(config)),
            "redshift" => Ok(AdapterConfig::Redshift(config)),
            "databricks" => Ok(AdapterConfig::Databricks(config)),
            _ => Err(format!("Unsupported adapter type: {adapter_type}")),
        }
    }

    fn extract_config_from_yaml(yaml_val: &dbt_serde_yaml::Value) -> Result<ConfigMap, String> {
        let Some(yaml_map) = yaml_val.as_mapping() else {
            return Err("Expected YAML mapping".to_string());
        };

        let mut config = ConfigMap::new();
        for (k, v) in yaml_map.iter() {
            if let Some(key_str) = k.as_str() {
                if key_str == "type" {
                    continue; // Skip the adapter type field
                }
                config.insert(key_str.to_string(), FieldValue::from_yaml(v));
            }
        }

        Ok(config)
    }
}

#[derive(Debug, Clone)]
pub struct ProfileDefaults {
    pub adapter_config: AdapterConfig,
}

impl ProfileDefaults {
    /// Get the adapter type for use in adapter selection prompts
    pub fn adapter_type(&self) -> &'static str {
        self.adapter_config.adapter_type()
    }

    /// Get the native config for pre-filling in adapter setup
    pub fn config(&self) -> &ConfigMap {
        self.adapter_config.config()
    }
}

pub trait ProfileParser {
    fn parse_profile_yaml(
        profile_yaml: &dbt_serde_yaml::Value,
        target_name: Option<&str>,
    ) -> FsResult<Option<ProfileDefaults>>;
}

pub struct DefaultProfileParser;

impl ProfileParser for DefaultProfileParser {
    fn parse_profile_yaml(
        profile_yaml: &dbt_serde_yaml::Value,
        target_name: Option<&str>,
    ) -> FsResult<Option<ProfileDefaults>> {
        let Some(profile_map) = profile_yaml.as_mapping() else {
            return Ok(None);
        };

        // Find outputs section
        let outputs = profile_map
            .iter()
            .find(|(k, _)| k.as_str() == Some("outputs"))
            .and_then(|(_, v)| v.as_mapping());

        let Some(outputs_map) = outputs else {
            return Ok(None);
        };

        // Find the target output
        let chosen_output = if let Some(target) = target_name {
            outputs_map
                .iter()
                .find(|(k, _)| k.as_str() == Some(target))
                .map(|(_, v)| v)
                .or_else(|| outputs_map.iter().next().map(|(_, v)| v))
        } else {
            outputs_map.iter().next().map(|(_, v)| v)
        };

        let Some(output_val) = chosen_output else {
            return Ok(None);
        };

        // Extract adapter type directly from YAML
        let adapter_type = output_val.as_mapping().and_then(|map| {
            map.iter()
                .find(|(k, _)| k.as_str() == Some("type"))
                .and_then(|(_, v)| v.as_str())
        });

        let Some(adapter_type_str) = adapter_type else {
            return Ok(None);
        };

        // Create typed adapter config directly from YAML
        let adapter_config = AdapterConfig::from_type_and_yaml_config(adapter_type_str, output_val)
            .map_err(|e| fs_err!(ErrorCode::InvalidArgument, "{}", e))?;

        Ok(Some(ProfileDefaults { adapter_config }))
    }
}
