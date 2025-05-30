//! A set of util functions for casting from/to Value
use crate::adapters::bigquery::relation::BigqueryRelation;
use crate::adapters::databricks::relation::DatabricksRelation;
use crate::adapters::postgres::relation::PostgresRelation;
use crate::adapters::redshift::relation::RedshiftRelation;
use crate::adapters::snowflake::relation::SnowflakeRelation;

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
use minijinja::{invalid_argument, invalid_argument_inner};
use serde::de::DeserializeOwned;
use serde::Deserialize;

use std::file;
use std::sync::Arc;

pub(crate) trait BaseRelationExt {
    /// Downcast a BaseRelation to a MinijinjaValue
    fn to_value(&self) -> Result<MinijinjaValue, MinijinjaError>;
}

impl BaseRelationExt for dyn BaseRelation {
    fn to_value(&self) -> Result<MinijinjaValue, MinijinjaError> {
        if let Some(snowflake_relation) = self.as_any().downcast_ref::<SnowflakeRelation>() {
            Ok(MinijinjaValue::from_object(snowflake_relation.clone()))
        } else if let Some(postgres_relation) = self.as_any().downcast_ref::<PostgresRelation>() {
            Ok(MinijinjaValue::from_object(postgres_relation.clone()))
        } else if let Some(bq_relation) = self.as_any().downcast_ref::<BigqueryRelation>() {
            Ok(MinijinjaValue::from_object(bq_relation.clone()))
        } else if let Some(redshift_relation) = self.as_any().downcast_ref::<RedshiftRelation>() {
            Ok(MinijinjaValue::from_object(redshift_relation.clone()))
        } else if let Some(databricks_relation) = self.as_any().downcast_ref::<DatabricksRelation>()
        {
            Ok(MinijinjaValue::from_object(databricks_relation.clone()))
        } else {
            invalid_argument!("Unsupported relation type")
        }
    }
}

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
    if let Some(snowflake_relation) = value.downcast_object::<SnowflakeRelation>() {
        Ok(snowflake_relation)
    } else if let Some(postgres_relation) = value.downcast_object::<PostgresRelation>() {
        Ok(postgres_relation)
    } else if let Some(bq_relation) = value.downcast_object::<BigqueryRelation>() {
        Ok(bq_relation)
    } else if let Some(redshift_relation) = value.downcast_object::<RedshiftRelation>() {
        Ok(redshift_relation)
    } else if let Some(databricks_relation) = value.downcast_object::<DatabricksRelation>() {
        Ok(databricks_relation)
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

/// Downcast a MinijinjaValue to a BaseRelation
pub fn downcast_value_to_relation<T: BaseRelation>(
    value: MinijinjaValue,
) -> Result<Arc<T>, MinijinjaError> {
    if let Some(relation) = value.downcast_object::<T>() {
        Ok(relation)
    } else {
        invalid_argument!("Unexpected relation, got {:?} in {}", value, file!())
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
