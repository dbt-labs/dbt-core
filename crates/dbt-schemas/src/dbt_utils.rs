use std::path::{Path, PathBuf};

use dbt_common::adapter::AdapterType;
use dbt_common::{ErrorCode, FsResult, err};
use dbt_serde_yaml::Spanned;

use crate::{constants::DBT_BASE_SCHEMAS_URL, schemas::common::DbtQuoting};

pub fn get_prefix(x: &Path, y: &Path) -> PathBuf {
    let x_components: Vec<_> = x.components().collect();
    let y_components: Vec<_> = y.components().collect();

    if y_components.len() > x_components.len() {
        return PathBuf::from(".");
    }

    for (x_comp, y_comp) in x_components.iter().rev().zip(y_components.iter().rev()) {
        if x_comp != y_comp {
            return PathBuf::from(".");
        }
    }

    let prefix_length = x_components.len() - y_components.len();
    x_components[..prefix_length]
        .iter()
        .map(|comp| comp.as_os_str())
        .collect::<PathBuf>()
}

pub fn get_dbt_schema_version(name: &str, version: i16) -> String {
    format!("{DBT_BASE_SCHEMAS_URL}/dbt/{name}/v{version}.json")
}

/// Resolve package quoting config
pub fn resolve_package_quoting(
    quoting: Option<DbtQuoting>,
    adapter_type: AdapterType,
) -> DbtQuoting {
    let default_quoting_bool = !matches!(adapter_type, AdapterType::Snowflake);
    let default_snowflake_ignore_case = false;
    if let Some(quoting) = quoting {
        DbtQuoting {
            database: Some(quoting.database.unwrap_or(default_quoting_bool)),
            schema: Some(quoting.schema.unwrap_or(default_quoting_bool)),
            identifier: Some(quoting.identifier.unwrap_or(default_quoting_bool)),
            snowflake_ignore_case: Some(
                quoting
                    .snowflake_ignore_case
                    .unwrap_or(default_snowflake_ignore_case),
            ),
        }
    } else {
        DbtQuoting {
            database: Some(default_quoting_bool),
            schema: Some(default_quoting_bool),
            identifier: Some(default_quoting_bool),
            snowflake_ignore_case: Some(default_snowflake_ignore_case),
        }
    }
}

/// Validate a delimiter
pub fn validate_delimeter(spanned_delimiter: &Option<Spanned<String>>) -> FsResult<Option<String>> {
    if let Some(delimiter) = spanned_delimiter.as_ref() {
        if delimiter.is_empty() {
            return Ok(None);
        } else if delimiter.len() != 1 || !delimiter.chars().next().unwrap().is_ascii() {
            return err!(
                code => ErrorCode::InvalidConfig,
                loc => delimiter.span().clone(),
                "Delimeter '{}' must be exactly one ascii character",
                delimiter.as_ref()
            );
        } else {
            return Ok(Some(delimiter.clone().into_inner()));
        }
    }

    Ok(None)
}
