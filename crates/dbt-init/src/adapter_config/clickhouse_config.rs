use super::common::*;
use crate::{ErrorCode, FsResult, fs_err};
use dbt_schemas::schemas::profiles::ClickHouseDbConfig;
use dbt_schemas::schemas::serde::StringOrInteger;

impl InteractiveSetup for ClickHouseDbConfig {
    fn get_fields() -> Vec<ConfigField> {
        vec![
            ConfigField {
                name: "host".to_string(),
                field_type: FieldType::Input {
                    default: Some("localhost".to_string()),
                },
                condition: FieldCondition::Always,
                prompt: "Host (hostname)".to_string(),
                required: true,
            },
            ConfigField {
                name: "user".to_string(),
                field_type: FieldType::Input {
                    default: Some("default".to_string()),
                },
                condition: FieldCondition::Always,
                prompt: "Username".to_string(),
                required: true,
            },
            ConfigField {
                name: "password".to_string(),
                field_type: FieldType::Password,
                condition: FieldCondition::Always,
                prompt: "Password".to_string(),
                required: false,
            },
            ConfigField {
                name: "database".to_string(),
                field_type: FieldType::Input { default: None },
                condition: FieldCondition::Always,
                prompt: "Database name".to_string(),
                required: false,
            },
            ConfigField {
                name: "schema".to_string(),
                field_type: FieldType::Input { default: None },
                condition: FieldCondition::Always,
                prompt: "Schema (dbt schema)".to_string(),
                required: true,
            },
            ConfigField {
                name: "secure".to_string(),
                field_type: FieldType::Confirm { default: false },
                condition: FieldCondition::Always,
                prompt: "Enable HTTPS (secure)?".to_string(),
                required: true,
            },
            ConfigField {
                name: "port".to_string(),
                field_type: FieldType::Input {
                    default: Some("8123".to_string()),
                },
                condition: FieldCondition::IfFieldEquals {
                    field_name: "secure".to_string(),
                    value: FieldValue::Boolean(false),
                },
                prompt: "Port".to_string(),
                required: true,
            },
            ConfigField {
                name: "port".to_string(),
                field_type: FieldType::Input {
                    default: Some("8443".to_string()),
                },
                condition: FieldCondition::IfFieldEquals {
                    field_name: "secure".to_string(),
                    value: FieldValue::Boolean(true),
                },
                prompt: "Port".to_string(),
                required: true,
            },
        ]
    }

    fn set_field(&mut self, field_name: &str, value: FieldValue) -> FsResult<()> {
        match field_name {
            "host" => {
                if let FieldValue::String(val) = value {
                    self.host = Some(val);
                }
            }
            "port" => match value {
                FieldValue::String(val) => {
                    if let Ok(port) = val.parse::<i64>() {
                        self.port = Some(StringOrInteger::Integer(port));
                    }
                }
                FieldValue::Integer(val) => {
                    self.port = Some(StringOrInteger::Integer(val));
                }
                _ => {}
            },
            "user" => {
                if let FieldValue::String(val) = value {
                    self.user = Some(val);
                }
            }
            "password" => {
                if let FieldValue::String(val) = value {
                    self.password = Some(val);
                }
            }
            "database" => {
                if let FieldValue::String(val) = value {
                    self.database = Some(val);
                }
            }
            "schema" => {
                if let FieldValue::String(val) = value {
                    self.schema = Some(val);
                }
            }
            "secure" => {
                if let FieldValue::Boolean(val) = value {
                    self.secure = Some(val);
                }
            }
            _ => {
                return Err(fs_err!(
                    ErrorCode::InvalidArgument,
                    "Unknown field: {}",
                    field_name
                ));
            }
        }
        Ok(())
    }

    fn get_field(&self, field_name: &str) -> Option<FieldValue> {
        match field_name {
            "host" => self.host.as_ref().map(|v| FieldValue::String(v.clone())),
            "port" => self.port.as_ref().map(|v| match v {
                StringOrInteger::String(s) => FieldValue::String(s.clone()),
                StringOrInteger::Integer(i) => FieldValue::Integer(*i),
            }),
            "user" => self.user.as_ref().map(|v| FieldValue::String(v.clone())),
            "password" => self
                .password
                .as_ref()
                .map(|v| FieldValue::String(v.clone())),
            "database" => self
                .database
                .as_ref()
                .map(|v| FieldValue::String(v.clone())),
            "schema" => self.schema.as_ref().map(|v| FieldValue::String(v.clone())),
            "secure" => self.secure.map(FieldValue::Boolean),
            _ => None,
        }
    }

    fn is_field_set(&self, field_name: &str) -> bool {
        match field_name {
            "host" => self.host.is_some(),
            "port" => self.port.is_some(),
            "user" => self.user.is_some(),
            "password" => self.password.is_some(),
            "database" => self.database.is_some(),
            "schema" => self.schema.is_some(),
            "secure" => self.secure.is_some(),
            _ => false,
        }
    }
}

pub fn setup_clickhouse_profile(
    existing_config: Option<&ClickHouseDbConfig>,
) -> FsResult<Box<ClickHouseDbConfig>> {
    let default_config = ClickHouseDbConfig::default();
    let mut config = ConfigProcessor::process_config(existing_config.or(Some(&default_config)))?;

    if config.threads.is_none() {
        config.threads = Some(StringOrInteger::Integer(16));
    }

    Ok(Box::new(config))
}
