use std::any::Any;

use dbt_adapter_proc_macros::{BaseColumnObject, StaticBaseColumnObject};
use dbt_common::current_function_name;
use minijinja::arg_utils::check_num_args;
use minijinja::arg_utils::ArgParser;
use minijinja::value::Enumerator;
use minijinja::{Error as MinijinjaError, Value};
use serde::{Deserialize, Serialize};

use super::base::StaticBaseColumn;
use super::base::{BaseColumn, BaseColumnProperties};

/// A struct representing a column type for use with static methods
#[derive(Clone, Debug, StaticBaseColumnObject)]
pub struct PostgresColumnType;

impl StaticBaseColumn for PostgresColumnType {
    fn try_new(
        name: String,
        dtype: String,
        char_size: Option<u32>,
        numeric_precision: Option<u64>,
        numeric_scale: Option<u64>,
    ) -> Result<Value, MinijinjaError> {
        Ok(Value::from_object(PostgresColumn {
            name,
            dtype,
            char_size,
            numeric_precision,
            numeric_scale,
        }))
    }
}

/// A struct representing a column
#[derive(Clone, Debug, Default, BaseColumnObject, Serialize, Deserialize)]
pub struct PostgresColumn {
    pub name: String,
    pub dtype: String,
    pub char_size: Option<u32>,
    pub numeric_precision: Option<u64>,
    pub numeric_scale: Option<u64>,
}

impl BaseColumn for PostgresColumn {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_value(&self) -> Value {
        Value::from_object(self.clone())
    }

    // TODO: impl data_type
}

impl BaseColumnProperties for PostgresColumn {
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
