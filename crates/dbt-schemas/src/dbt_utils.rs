use std::path::{Path, PathBuf};

use crate::{constants::DBT_BASE_SCHEMAS_URL, schemas::common::DbtQuoting};

/// Macro that assigns values from `parent_config` to `self` for each field
/// if and only if `self.field` is `None`.
#[macro_export]
macro_rules! default_to {
    (
        // $dest is the current "child" struct,
        // $parent is the parent config,
        // followed by multiple fields (tokens).
        $dest:ident,
        $parent:ident,
        [ $( $field:ident ),* $(,)? ]
    ) => {
        $(
            if $dest.$field.is_none() {
                $dest.$field = $parent.$field.clone();
            }
        )*
    }
}

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
pub fn resolve_package_quoting(quoting: Option<DbtQuoting>, adapter_type: &str) -> DbtQuoting {
    let default_quoting_bool = adapter_type != "snowflake";
    if let Some(quoting) = quoting {
        DbtQuoting {
            database: Some(quoting.database.unwrap_or(default_quoting_bool)),
            schema: Some(quoting.schema.unwrap_or(default_quoting_bool)),
            identifier: Some(quoting.identifier.unwrap_or(default_quoting_bool)),
        }
    } else {
        DbtQuoting {
            database: Some(default_quoting_bool),
            schema: Some(default_quoting_bool),
            identifier: Some(default_quoting_bool),
        }
    }
}
