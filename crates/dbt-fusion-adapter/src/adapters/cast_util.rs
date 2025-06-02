//! A set of util functions for casting from/to Value
use crate::adapters::relation_object::RelationObject;

use dbt_schemas::schemas::columns::base::BaseColumn;
use dbt_schemas::schemas::columns::base::StdColumn;
use dbt_schemas::schemas::columns::bigquery::BigqueryColumn;
use dbt_schemas::schemas::columns::databricks::DatabricksColumn;
use dbt_schemas::schemas::columns::postgres::PostgresColumn;
use dbt_schemas::schemas::columns::redshift::RedshiftColumn;
use dbt_schemas::schemas::columns::snowflake::SnowflakeColumn;
use dbt_schemas::schemas::relations::base::BaseRelation;
use minijinja::jinja_err;
use minijinja::Error as MinijinjaError;
use minijinja::ErrorKind as MinijinjaErrorKind;
use minijinja::Value as MinijinjaValue;
use serde::de::DeserializeOwned;
use serde::Deserialize;

use std::file;
use std::sync::Arc;

macro_rules! try_downcast_columns {
    ($columns:expr, $($type:ty),+ $(,)?) => {
        $(
            if let Some(columns) = downcast_base_columns::<$type>($columns) {
                return Ok(MinijinjaValue::from_serialize(
                    columns
                        .into_iter()
                        .map(MinijinjaValue::from_object)
                        .collect::<Vec<_>>()
                ));
            }
        )+
    };
}

/// Downcast a MinijinjaValue to a dyn BaseRelation object
pub fn downcast_value_to_dyn_base_relation(
    value: MinijinjaValue,
) -> Result<Arc<dyn BaseRelation>, MinijinjaError> {
    // Check if Arc<dyn BaseRelation> is Arc<SnowflakeRelation>
    if let Some(relation_object) = value.downcast_object::<RelationObject>() {
        Ok(relation_object.inner())
    } else {
        Err(MinijinjaError::new(
            MinijinjaErrorKind::InvalidOperation,
            format!(
                "Unsupported relation type ({}) in {}:{}",
                value,
                file!(),
                line!()
            ),
        ))
    }
}

/// Attempts to downcast a Vec of BaseColumn trait objects to a specific column type
pub fn downcast_base_columns<T: BaseColumn + Clone + 'static>(
    columns: &[Box<dyn BaseColumn>],
) -> Option<Vec<T>> {
    let mut result = Vec::with_capacity(columns.len());

    for column in columns {
        // Try to downcast each column
        if let Some(concrete) = column.as_any().downcast_ref::<T>() {
            result.push(concrete.clone());
        } else {
            return None; // If any column fails to downcast, return None
        }
    }

    Some(result)
}

/// Attempts to convert a vector of dyn BaseColumn objects values to a MinijinjaValue
pub fn dyn_base_columns_to_value(
    columns: Vec<Box<dyn BaseColumn>>,
) -> Result<MinijinjaValue, MinijinjaError> {
    try_downcast_columns!(
        &columns,
        PostgresColumn,
        SnowflakeColumn,
        BigqueryColumn,
        RedshiftColumn,
        DatabricksColumn,
        StdColumn
    );

    jinja_err!(
        MinijinjaErrorKind::InvalidArgument,
        format!("Unsupported columns type in {}", file!())
    )
}

/// The revert conversion of `dyn_base_columns_to_value` function
/// Attempts to convert a MinijinjaValue to a vector of values that impl BaseColumn trait
pub fn deserialize_value_to_base_columns_vec<T: DeserializeOwned + BaseColumn + 'static>(
    value: MinijinjaValue,
) -> Result<Vec<T>, MinijinjaError> {
    let result = Vec::<T>::deserialize(value).map_err(|e| {
        MinijinjaError::new(MinijinjaErrorKind::SerdeDeserializeError, e.to_string())
    })?;
    Ok(result)
}
