use dbt_adapter_core::AdapterType;
use dbt_adbc::Connection;
use dbt_auth::AdapterConfig;
use dbt_common::{AdapterError, AdapterErrorKind, AdapterResult};

use crate::errors::adbc_error_to_adapter_error;

fn config_error(message: impl Into<String>) -> AdapterError {
    AdapterError::new(AdapterErrorKind::Configuration, message)
}

fn init_sql(config: &AdapterConfig) -> AdapterResult<Vec<String>> {
    let mut statements = Vec::new();

    if let Some(value) = config.get("settings") {
        let dbt_yaml::Value::Mapping(settings, _) = value else {
            return Err(config_error("settings must be a mapping"));
        };
        for (name, value) in settings {
            let Some(name) = name.as_str() else {
                return Err(config_error("setting names must be strings"));
            };
            if name.trim().is_empty() || name.contains('\0') {
                return Err(config_error(
                    "setting names must be non-empty strings without NUL characters",
                ));
            }
            let value = match value {
                dbt_yaml::Value::String(value, _) => value.clone(),
                dbt_yaml::Value::Bool(value, _) => value.to_string(),
                dbt_yaml::Value::Number(value, _) => value.to_string(),
                _ => {
                    return Err(config_error(
                        "setting values must be strings, booleans, or numbers",
                    ));
                }
            };
            let name = dbt_adapter_sql::ident::quote_identifier(name, AdapterType::MotherDuck);
            let value =
                dbt_adapter_sql::ident::escape_string_literal(&value, AdapterType::MotherDuck);
            statements.push(format!("SET {name} = '{value}'"));
        }
    }

    Ok(statements)
}

fn init_sql_execution_error(
    statement_index: usize,
    error: adbc_core::error::Error,
) -> AdapterError {
    adbc_error_to_adapter_error(adbc_core::error::Error::with_message_and_status(
        format!(
            "MotherDuck init SQL statement {} failed; the driver error was omitted because the statement may contain a secret setting value",
            statement_index + 1
        ),
        error.status,
    ))
}

pub fn apply_init_sql(conn: &mut dyn Connection, config: &AdapterConfig) -> AdapterResult<()> {
    for (idx, sql) in init_sql(config)?.iter().enumerate() {
        let mut stmt = conn.new_statement().map_err(adbc_error_to_adapter_error)?;
        stmt.set_sql_query(sql)
            .map_err(|error| init_sql_execution_error(idx, error))?;
        stmt.execute_update()
            .map_err(|error| init_sql_execution_error(idx, error))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_sql_escapes_settings() {
        let value: dbt_yaml::Value = dbt_yaml::from_str(
            r#"
settings:
  custom_user_agent: "dbt's tests"
  enable_progress_bar: false
"#,
        )
        .unwrap();
        let dbt_yaml::Value::Mapping(mapping, _) = value else {
            panic!("expected mapping");
        };

        assert_eq!(
            init_sql(&AdapterConfig::new(mapping)).unwrap(),
            vec![
                "SET \"custom_user_agent\" = 'dbt''s tests'",
                "SET \"enable_progress_bar\" = 'false'",
            ]
        );
    }

    #[test]
    fn init_sql_errors_omit_secret_setting_values() {
        let secret = "super-secret-setting-value";
        let driver_error = adbc_core::error::Error::with_message_and_status(
            format!("failed SQL: SET \"api_token\" = '{secret}'"),
            adbc_core::error::Status::InvalidArguments,
        );

        let error = init_sql_execution_error(0, driver_error);
        assert!(!error.to_string().contains(secret));
        assert!(error.to_string().contains("driver error was omitted"));
    }
}
