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
pub struct DatabricksColumnType;

impl StaticBaseColumn for DatabricksColumnType {
    fn try_new(
        name: String,
        dtype: String,
        char_size: Option<u32>,
        // unused currently, may need to revisit for DECIMAL types!
        _numeric_precision: Option<u64>,
        _numeric_scale: Option<u64>,
    ) -> Result<Value, MinijinjaError> {
        Ok(Value::from_object(DatabricksColumn {
            name,
            dtype,
            char_size,
        }))
    }

    /// Translate the column type to a Snowflake type
    fn translate_type(args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut args = ArgParser::new(args, None);
        let column_type: String = args.get("dtype")?;
        Ok(Value::from(column_type))
    }
}

/// A struct representing a column
#[derive(Clone, Debug, Default, BaseColumnObject, Serialize, Deserialize)]
pub struct DatabricksColumn {
    pub name: String,
    pub dtype: String,
    pub char_size: Option<u32>,
}

impl BaseColumn for DatabricksColumn {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_value(&self) -> Value {
        Value::from_object(self.clone())
    }

    fn quoted(&self) -> Value {
        Value::from(&format!("`{}`", self.name()))
    }
}

impl BaseColumnProperties for DatabricksColumn {
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
        None
    }

    fn numeric_scale_prop(&self) -> Option<u64> {
        None
    }
}
