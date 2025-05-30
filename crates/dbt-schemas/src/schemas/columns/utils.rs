use minijinja::{Error as MinijinjaError, ErrorKind, Value as MinijinjaValue};

use std::sync::Arc;

use super::base::BaseColumn;
use super::bigquery::BigqueryColumn;
use super::databricks::DatabricksColumn;
use super::postgres::PostgresColumn;
use super::redshift::RedshiftColumn;
use super::snowflake::SnowflakeColumn;

/// Downcast a MinijinjaValue to a BaseColumn
pub fn downcast_value_to_base_column(
    value: MinijinjaValue,
) -> Result<Arc<dyn BaseColumn>, MinijinjaError> {
    if let Some(snowflake_column) = value.downcast_object::<SnowflakeColumn>() {
        Ok(snowflake_column)
    } else if let Some(postgres_column) = value.downcast_object::<PostgresColumn>() {
        Ok(postgres_column)
    } else if let Some(bq_column) = value.downcast_object::<BigqueryColumn>() {
        Ok(bq_column)
    } else if let Some(redshift_column) = value.downcast_object::<RedshiftColumn>() {
        Ok(redshift_column)
    } else if let Some(databricks_column) = value.downcast_object::<DatabricksColumn>() {
        Ok(databricks_column)
    } else {
        Err(MinijinjaError::new(
            ErrorKind::InvalidOperation,
            format!("Unsupported column type in {}", file!()),
        ))
    }
}

/// Convert a BaseColumn to a MinijinjaValue
impl dyn BaseColumn {
    /// Convert a BaseColumn to a MinijinjaValue
    pub fn to_value(&self) -> Result<MinijinjaValue, MinijinjaError> {
        if let Some(snowflake_column) = self.as_any().downcast_ref::<SnowflakeColumn>() {
            Ok(MinijinjaValue::from_object(snowflake_column.clone()))
        } else if let Some(postgres_column) = self.as_any().downcast_ref::<PostgresColumn>() {
            Ok(MinijinjaValue::from_object(postgres_column.clone()))
        } else if let Some(bq_column) = self.as_any().downcast_ref::<BigqueryColumn>() {
            Ok(MinijinjaValue::from_object(bq_column.clone()))
        } else if let Some(redshift_column) = self.as_any().downcast_ref::<RedshiftColumn>() {
            Ok(MinijinjaValue::from_object(redshift_column.clone()))
        } else if let Some(databricks_column) = self.as_any().downcast_ref::<DatabricksColumn>() {
            Ok(MinijinjaValue::from_object(databricks_column.clone()))
        } else {
            Err(MinijinjaError::new(
                ErrorKind::InvalidOperation,
                format!("Unsupported column type in {}", file!()),
            ))
        }
    }
}
