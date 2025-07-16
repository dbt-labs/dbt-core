use crate::schemas::columns::base::{BaseColumn, BaseColumnProperties, StaticBaseColumn};
use crate::schemas::dbt_column::DbtColumn;

use dbt_adapter_proc_macros::{BaseColumnObject, StaticBaseColumnObject};
use dbt_common::current_function_name;
use minijinja::arg_utils::ArgParser;
use minijinja::arg_utils::check_num_args;
use minijinja::value::Enumerator;
use minijinja::{Error as MinijinjaError, Value};
use serde::{Deserialize, Serialize};

use std::any::Any;

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

    /// Translate the column type to a Databricks type
    // https://github.com/databricks/dbt-databricks/blob/822b105b15e644676d9e1f47cbfd765cd4c1541f/dbt/adapters/databricks/column.py#L14
    fn translate_type(args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut args = ArgParser::new(args, None);
        let column_type: String = args.get("dtype")?;
        let column_type = match column_type.to_uppercase().as_str() {
            "LONG" => "BIGINT",
            _ => &column_type,
        };
        Ok(Value::from(column_type))
    }

    /// https://github.com/databricks/dbt-databricks/blob/822b105b15e644676d9e1f47cbfd765cd4c1541f/dbt/adapters/databricks/column.py#L66
    fn format_add_column_list(args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut args: ArgParser = ArgParser::new(args, None);
        let columns = args.get::<Value>("columns")?;

        let columns = Vec::<DatabricksColumn>::deserialize(columns)?;
        Ok(Value::from(
            columns
                .iter()
                .map(|c| {
                    format!(
                        "{} {}",
                        c.quoted().as_str().expect("column.quoted returns a string"),
                        c.dtype_prop()
                    )
                })
                .collect::<Vec<String>>()
                .join(", "),
        ))
    }

    /// https://github.com/databricks/dbt-databricks/blob/822b105b15e644676d9e1f47cbfd765cd4c1541f/dbt/adapters/databricks/column.py#L62
    fn format_remove_column_list(args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut args: ArgParser = ArgParser::new(args, None);
        let columns = args.get::<Value>("columns")?;

        let columns = Vec::<DatabricksColumn>::deserialize(columns)?;
        Ok(Value::from(
            columns
                .iter()
                .map(|c| {
                    c.quoted()
                        .as_str()
                        .expect("column.quoted returns a string")
                        .to_owned()
                })
                .collect::<Vec<String>>()
                .join(", "),
        ))
    }

    /// https://github.com/databricks/dbt-databricks/blob/5e20eeaef43e671913f995d8079d4ec2b8a1da6d/dbt/adapters/databricks/column.py#L34
    fn get_name(args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut args: ArgParser = ArgParser::new(args, None);
        let column = args.get::<Value>("column")?;
        let column = DbtColumn::deserialize(column)?;

        if column.quote.unwrap_or(false) {
            Ok(Value::from(quote(&column.name)))
        } else {
            Ok(Value::from(column.name))
        }
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
        Value::from(&quote(&self.name))
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

fn quote(name: &str) -> String {
    format!("`{name}`")
}
