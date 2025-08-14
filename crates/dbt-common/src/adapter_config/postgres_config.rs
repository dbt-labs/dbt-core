use super::common::*;
use crate::{ErrorCode, FsResult, fs_err};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PostgresFieldId {
    Host,
    User,
    Password,
    Port,
    DbName,
    Schema,
}

impl FieldId for PostgresFieldId {
    fn config_key(&self) -> &'static str {
        match self {
            PostgresFieldId::Host => "host",
            PostgresFieldId::User => "user",
            PostgresFieldId::Password => "password",
            PostgresFieldId::Port => "port",
            PostgresFieldId::DbName => "dbname",
            PostgresFieldId::Schema => "schema",
        }
    }

    fn is_temporary(&self) -> bool {
        false // No temporary fields for Postgres
    }
}

#[derive(Debug)]
pub struct PortField {
    pub prompt: String,
    pub default_port: u32,
}

impl FieldType<PostgresFieldId> for PortField {
    fn collect_input(
        &self,
        existing_config: &ConfigMap,
        field_id: PostgresFieldId,
        _storage: &FieldStorage<PostgresFieldId>,
    ) -> FsResult<FieldValue> {
        let default_port: u32 = existing_config
            .get(field_id.config_key())
            .and_then(|v| v.as_number())
            .unwrap_or(self.default_port as usize) as u32;

        let port: u32 = dialoguer::Input::new()
            .with_prompt(&self.prompt)
            .default(default_port)
            .interact()
            .map_err(|e| {
                fs_err!(
                    ErrorCode::IoError,
                    "Failed to get {}: {}",
                    field_id.config_key(),
                    e
                )
            })?;

        Ok(FieldValue::Number(port as usize))
    }
}

pub fn port_field(
    id: PostgresFieldId,
    prompt: &str,
    default_port: u32,
) -> TypedField<PostgresFieldId> {
    TypedField {
        id,
        field_type: Box::new(PortField {
            prompt: prompt.to_string(),
            default_port,
        }),
        condition: FieldCondition::Always,
    }
}

pub fn postgres_config_fields() -> Vec<TypedField<PostgresFieldId>> {
    vec![
        input_field(PostgresFieldId::Host, "Host (hostname)", None),
        input_field(PostgresFieldId::User, "Username", None),
        password_field(PostgresFieldId::Password, "password"),
        port_field(PostgresFieldId::Port, "Port", 5432),
        input_field(PostgresFieldId::DbName, "Database name", None),
        input_field(PostgresFieldId::Schema, "Schema (dbt schema)", None),
    ]
}

pub struct PostgresPostProcessor;

impl AdapterPostProcessor<PostgresFieldId> for PostgresPostProcessor {
    fn post_process_config(
        &self,
        config: &mut ConfigMap,
        _storage: &FieldStorage<PostgresFieldId>,
    ) -> FsResult<()> {
        if !config.contains_key("threads") {
            config.insert("threads".to_string(), FieldValue::Number(16));
        }

        Ok(())
    }
}

pub fn setup_postgres_profile(existing_config: Option<&ConfigMap>) -> FsResult<ConfigMap> {
    let processor = ExtendedConfigProcessor::new(postgres_config_fields(), PostgresPostProcessor);
    processor.process_config(existing_config)
}
