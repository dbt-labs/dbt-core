use super::common::*;
use crate::FsResult;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RedshiftFieldId {
    Host,
    User,
    Password,
    DbName,
    Schema,
}

impl FieldId for RedshiftFieldId {
    fn config_key(&self) -> &'static str {
        match self {
            RedshiftFieldId::Host => "host",
            RedshiftFieldId::User => "user",
            RedshiftFieldId::Password => "password",
            RedshiftFieldId::DbName => "dbname",
            RedshiftFieldId::Schema => "schema",
        }
    }

    fn is_temporary(&self) -> bool {
        false // No temporary fields for Redshift
    }
}

pub fn redshift_config_fields() -> Vec<TypedField<RedshiftFieldId>> {
    vec![
        input_field(
            RedshiftFieldId::Host,
            "Host (hostname.region.redshift.amazonaws.com)",
            None,
        ),
        input_field(RedshiftFieldId::User, "Username", None),
        password_field(RedshiftFieldId::Password, "password"),
        input_field(RedshiftFieldId::DbName, "Database name", None),
        input_field(RedshiftFieldId::Schema, "Schema (dbt schema)", None),
    ]
}

pub struct RedshiftPostProcessor;

impl AdapterPostProcessor<RedshiftFieldId> for RedshiftPostProcessor {
    fn post_process_config(
        &self,
        config: &mut ConfigMap,
        _storage: &FieldStorage<RedshiftFieldId>,
    ) -> FsResult<()> {
        // Set default threads if not present
        if !config.contains_key("threads") {
            config.insert("threads".to_string(), FieldValue::Number(16));
        }

        Ok(())
    }
}

pub fn setup_redshift_profile(existing_config: Option<&ConfigMap>) -> FsResult<ConfigMap> {
    let processor = ExtendedConfigProcessor::new(redshift_config_fields(), RedshiftPostProcessor);
    processor.process_config(existing_config)
}
