use std::any::Any;

use dbt_adapter_proc_macros::{BaseColumnObject, StaticBaseColumnObject};
use dbt_common::current_function_name;
use minijinja::arg_utils::check_num_args;
use minijinja::arg_utils::ArgParser;
use minijinja::value::Enumerator;
use minijinja::ErrorKind;
use minijinja::{Error as MinijinjaError, Value};
use serde::{Deserialize, Serialize};

use super::base::StaticBaseColumn;
use super::base::{BaseColumn, BaseColumnProperties};

/// A struct representing a column type for use with static methods
#[derive(Clone, Debug, StaticBaseColumnObject)]
pub struct SnowflakeColumnType;

impl StaticBaseColumn for SnowflakeColumnType {
    fn try_new(
        name: String,
        dtype: String,
        char_size: Option<u32>,
        numeric_precision: Option<u64>,
        numeric_scale: Option<u64>,
    ) -> Result<Value, MinijinjaError> {
        Ok(Value::from_object(SnowflakeColumn {
            name,
            dtype,
            char_size,
            numeric_precision,
            numeric_scale,
        }))
    }
}

impl BaseColumn for SnowflakeColumn {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_value(&self) -> Value {
        Value::from_object(self.clone())
    }

    fn is_numeric(&self) -> bool {
        matches!(
            self.dtype.to_lowercase().as_str(),
            "int"
                | "integer"
                | "bigint"
                | "smallint"
                | "tinyint"
                | "byteint"
                | "numeric"
                | "decimal"
                | "number"
        )
    }

    fn is_float(&self) -> bool {
        matches!(
            self.dtype.to_lowercase().as_str(),
            "float" | "float4" | "float8" | "double" | "double precision" | "real"
        )
    }

    // everything has NUMBER(38, 0)
    fn is_integer(&self) -> bool {
        false
    }

    fn string_size(&self) -> Result<u32, MinijinjaError> {
        if !self.is_string() {
            return Err(MinijinjaError::new(
                ErrorKind::InvalidArgument,
                "Called string_size() on non-string field",
            ));
        }

        if self.dtype.to_lowercase() == "text" || self.char_size.is_none() {
            Ok(16777216)
        } else {
            Ok(self.char_size.ok_or_else(|| {
                MinijinjaError::new(
                    ErrorKind::InvalidArgument,
                    format!("char_size is not set for column: {}", self.name),
                )
            })?)
        }
    }
}

/// A struct representing a column
#[derive(Clone, Debug, Default, BaseColumnObject, Serialize, Deserialize)]
pub struct SnowflakeColumn {
    pub name: String,
    pub dtype: String,
    pub char_size: Option<u32>,
    pub numeric_precision: Option<u64>,
    pub numeric_scale: Option<u64>,
}

impl BaseColumnProperties for SnowflakeColumn {
    fn name_prop(&self) -> &str {
        &self.name
    }

    fn dtype_prop(&self) -> &str {
        &self.dtype
    }

    fn char_size_prop(&self) -> Option<u32> {
        self.char_size
    }

    fn numeric_precision_prop(&self) -> Option<u64> {
        self.numeric_precision
    }

    fn numeric_scale_prop(&self) -> Option<u64> {
        self.numeric_scale
    }
}
