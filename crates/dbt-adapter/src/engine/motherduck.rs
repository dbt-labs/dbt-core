use std::collections::HashMap;

use dbt_adapter_core::AdapterType;
use dbt_adbc::Connection;
use dbt_auth::AdapterConfig;
use dbt_common::{AdapterError, AdapterErrorKind, AdapterResult};
use dbt_schemas::schemas::dbt_catalogs::{CatalogType, DbtCatalogsView};

use crate::errors::adbc_error_to_adapter_error;
use crate::load_catalogs;

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

    statements.extend(active_catalog_init_sql()?);
    Ok(statements)
}

fn active_catalog_init_sql() -> AdapterResult<Vec<String>> {
    if !load_catalogs::fetch_use_catalogs_v2() {
        return Ok(Vec::new());
    }
    let Some(catalogs) = load_catalogs::fetch_catalogs() else {
        return Ok(Vec::new());
    };
    let view = catalogs
        .view()
        .map_err(|error| config_error(error.to_string()))?;
    catalog_init_sql(Some(&view))
}

fn catalog_init_sql(view: Option<&DbtCatalogsView<'_>>) -> AdapterResult<Vec<String>> {
    let Some(view) = view else {
        return Ok(Vec::new());
    };

    let mut statements = Vec::new();
    let mut seen_aliases = HashMap::<String, String>::new();
    for catalog in &view.catalogs {
        if catalog.catalog_type != CatalogType::MotherDuck {
            continue;
        }

        let config = catalog
            .config_block(AdapterType::MotherDuck.as_ref())
            .ok_or_else(|| {
                config_error(format!(
                    "Catalog '{}' is missing its motherduck config block",
                    catalog.name
                ))
            })?;
        let database = config
            .get(dbt_yaml::Value::from("database"))
            .and_then(dbt_yaml::Value::as_str)
            .unwrap_or(catalog.name);
        let alias = config
            .get(dbt_yaml::Value::from("catalog_database"))
            .and_then(dbt_yaml::Value::as_str)
            .unwrap_or(database);
        let collision_key = alias.to_ascii_lowercase();
        if let Some(prior) = seen_aliases.get(&collision_key) {
            return Err(config_error(format!(
                "Catalog '{}' MotherDuck attach alias '{alias}' collides case-insensitively with catalog '{prior}'",
                catalog.name
            )));
        }
        seen_aliases.insert(collision_key, catalog.name.to_string());

        let database =
            dbt_adapter_sql::ident::escape_string_literal(database, AdapterType::MotherDuck);
        let alias = dbt_adapter_sql::ident::quote_identifier(alias, AdapterType::MotherDuck);
        statements.push(format!("ATTACH IF NOT EXISTS 'md:{database}' AS {alias}"));
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
    use dbt_schemas::schemas::dbt_catalogs_deprecated::DbtCatalogs;

    fn with_catalogs_view(yaml: &str, test: impl FnOnce(&DbtCatalogsView<'_>)) {
        let value: dbt_yaml::Value = dbt_yaml::from_str(yaml).unwrap();
        let dbt_yaml::Value::Mapping(mapping, span) = value else {
            panic!("expected mapping");
        };
        let catalogs = DbtCatalogs::new(mapping, span);
        let view = catalogs.view().expect("valid v2 catalogs view");
        test(&view);
    }

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

    #[test]
    fn catalog_init_sql_uses_catalog_name_defaults() {
        with_catalogs_view(
            r#"
catalogs:
  - name: reporting
    type: motherduck
    table_format: default
    config:
      motherduck: {}
"#,
            |view| {
                assert_eq!(
                    catalog_init_sql(Some(view)).unwrap(),
                    vec![r#"ATTACH IF NOT EXISTS 'md:reporting' AS "reporting""#]
                );
            },
        );
    }

    #[test]
    fn catalog_init_sql_uses_database_and_alias() {
        with_catalogs_view(
            r#"
catalogs:
  - name: reporting
    type: motherduck
    table_format: default
    config:
      motherduck:
        database: analytics
        catalog_database: reporting_alias
"#,
            |view| {
                assert_eq!(
                    catalog_init_sql(Some(view)).unwrap(),
                    vec![r#"ATTACH IF NOT EXISTS 'md:analytics' AS "reporting_alias""#]
                );
            },
        );
    }

    #[test]
    fn catalog_init_sql_escapes_database_and_alias_quotes() {
        with_catalogs_view(
            r#"
catalogs:
  - name: reporting
    type: motherduck
    table_format: default
    config:
      motherduck:
        database: "sales'west"
        catalog_database: 'report"ing'
"#,
            |view| {
                assert_eq!(
                    catalog_init_sql(Some(view)).unwrap(),
                    vec![r#"ATTACH IF NOT EXISTS 'md:sales''west' AS "report""ing""#]
                );
            },
        );
    }

    #[test]
    fn catalog_init_sql_rejects_case_insensitive_alias_collisions() {
        with_catalogs_view(
            r#"
catalogs:
  - name: first
    type: motherduck
    table_format: default
    config:
      motherduck:
        catalog_database: Reporting
  - name: second
    type: motherduck
    table_format: default
    config:
      motherduck:
        catalog_database: reporting
"#,
            |view| {
                let error = catalog_init_sql(Some(view)).unwrap_err();
                assert_eq!(error.kind(), AdapterErrorKind::Configuration);
                assert!(error.to_string().contains("collides case-insensitively"));
                assert!(error.to_string().contains("first"));
                assert!(error.to_string().contains("second"));
            },
        );
    }

    #[test]
    fn catalog_init_sql_handles_absent_and_empty_catalogs() {
        assert!(catalog_init_sql(None).unwrap().is_empty());
        with_catalogs_view("catalogs: []", |view| {
            assert!(catalog_init_sql(Some(view)).unwrap().is_empty());
        });
    }
}
