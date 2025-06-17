use crate::schemas::columns::utils::downcast_value_to_base_column;

use dbt_adapter_proc_macros::{BaseColumnObject, StaticBaseColumnObject};
use dbt_common::current_function_name;
use minijinja::arg_utils::check_num_args;
use minijinja::arg_utils::ArgParser;
use minijinja::value::Enumerator;
use minijinja::{Error as MinijinjaError, ErrorKind, Value};
use regex;
use serde::{Deserialize, Serialize};

use std::any::Any;

/// Trait for static methods on relations
// TODO: Make this trait generic and put Snowflake specific implementations into Snowflake
pub trait StaticBaseColumn {
    /// Create a new column from the given arguments
    fn try_new(
        name: String,
        dtype: String,
        char_size: Option<u32>,
        numeric_precision: Option<u64>,
        numeric_scale: Option<u64>,
    ) -> Result<Value, MinijinjaError>;

    /// Create a new column from the given arguments
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/column.py#L28-L29
    fn create(args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut args = ArgParser::new(args, None);
        let name: String = args.get("name")?;
        let dtype: String = args.get("label_or_dtype")?;

        let char_size = args.get_optional::<String>("char_size");
        let char_size = char_size.and_then(|s| s.parse::<u32>().ok());
        let numeric_precision = args.get_optional::<String>("numeric_precision");
        let numeric_precision = numeric_precision.and_then(|s| s.parse::<u64>().ok());
        let numeric_scale = args.get_optional::<String>("numeric_scale");
        let numeric_scale = numeric_scale.and_then(|s| s.parse::<u64>().ok());
        // TODO: add numeric_precision and numeric_scale
        Self::try_new(name, dtype, char_size, numeric_precision, numeric_scale)
    }

    /// Translate the column type to a Snowflake type
    fn translate_type(args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut args = ArgParser::new(args, None);
        let column_type: String = args.get("dtype")?;
        match column_type.to_lowercase().as_str() {
            "string" => Ok(Value::from("TEXT")),
            _ => Ok(Value::from(column_type)),
        }
    }
    /// Whether the column is a numeric type
    fn numeric_type(args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut args: ArgParser = ArgParser::new(args, None);
        let dtype: String = args.get("dtype")?;
        let precision: Option<i64> = args.get("precision").ok();
        let scale: Option<i64> = args.get("scale").ok();

        match (precision, scale) {
            (Some(p), Some(s)) => Ok(Value::from(format!("{}({},{})", dtype, p, s))),
            _ => Ok(Value::from(dtype)),
        }
    }

    fn string_type(args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut args: ArgParser = ArgParser::new(args, None);
        let size = args.get::<usize>("size")?;
        Ok(Value::from(format!("character varying({size})")))
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/column.py#L127-L128
    fn from_description(args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut args: ArgParser = ArgParser::new(args, None);
        let name = args.get::<String>("name")?;
        let raw_data_type = args.get::<String>("raw_data_type")?;

        // TODO why is this Snowflake specific in non-Snowflake specific trait?
        let SnowflakeColumnTypeParsed {
            data_type,
            char_size,
            numeric_precision,
            numeric_scale,
        } = parse_snowflake_raw_data_type(&raw_data_type)?;

        Ok(Value::from_object(StdColumn {
            name,
            dtype: data_type,
            char_size,
            numeric_precision,
            numeric_scale,
        }))
    }

    fn format_add_column_list(_args: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("Only available for Databricks")
    }

    fn format_remove_column_list(_args: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("Only available for Databricks")
    }

    fn get_name(_args: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("Only available for Databricks")
    }
}

pub trait BaseColumnProperties {
    fn name_prop(&self) -> &str;
    fn dtype_prop(&self) -> &str;
    fn char_size_prop(&self) -> Option<u32>;
    fn numeric_precision_prop(&self) -> Option<u64>;
    fn numeric_scale_prop(&self) -> Option<u64>;
}

pub trait BaseColumn: BaseColumnProperties + Any + Send + Sync {
    fn as_any(&self) -> &dyn Any;

    fn name(&self) -> Value {
        Value::from(self.name_prop())
    }

    fn numeric_precision(&self) -> Value {
        Value::from(self.numeric_precision_prop())
    }

    fn numeric_scale(&self) -> Value {
        Value::from(self.numeric_scale_prop())
    }

    /// Returns True if this column can be expanded to the size of the other column
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/column.py#L102-L103
    ///
    /// # Panics
    ///
    /// This function will panic if the column is not a string.
    fn can_expand_to(&self, other: Value) -> Result<bool, MinijinjaError> {
        let other = downcast_value_to_base_column(other)?;
        Ok(self.is_string() && other.is_string() && self.string_size()? < other.string_size()?)
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/column.py#L49-L50
    fn is_string(&self) -> bool {
        matches!(
            self.dtype_prop().to_lowercase().as_str(),
            "text" | "character varying" | "character" | "varchar"
        )
    }

    fn is_number(&self) -> bool {
        self.is_float() || self.is_integer() || self.is_numeric()
    }

    fn is_float(&self) -> bool {
        matches!(
            self.dtype_prop().to_lowercase().as_str(),
            "real" | "float4" | "float" | "double precision" | "float8" | "double"
        )
    }

    fn is_integer(&self) -> bool {
        matches!(
            self.dtype_prop().to_lowercase().as_str(),
            "smallint"
                | "integer"
                | "bigint"
                | "smallserial"
                | "serial"
                | "bigserial"
                | "int2"
                | "int4"
                | "int8"
                | "serial2"
                | "serial4"
                | "serial8"
        )
    }

    // https://github.com/dbt-labs/dbt-adapters/blob/5de867965ab7bf7609caa624f98a31203998d1d1/dbt-adapters/src/dbt/adapters/base/column.py#L89
    fn is_numeric(&self) -> bool {
        matches!(
            self.dtype_prop().to_lowercase().as_str(),
            "numeric" | "decimal"
        )
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/column.py#L92-L93
    fn string_size(&self) -> Result<u32, MinijinjaError> {
        if !self.is_string() {
            return Err(MinijinjaError::new(
                ErrorKind::InvalidArgument,
                "Called string_size() on non-string field",
            ));
        }
        if self.dtype_prop() == "text" || self.char_size_prop().is_none() {
            Ok(256)
        } else {
            // TODO: this is probably unsafe. But in `dbt-adapters`
            // char_size seems to be unset unless initialized from `from_description` class method
            Ok(self.char_size_prop().ok_or_else(|| {
                MinijinjaError::new(
                    ErrorKind::InvalidArgument,
                    format!("char_size is not set for column: {}", self.name_prop()),
                )
            })?)
        }
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/column.py#L104-L105
    fn string_type(&self, size: u32) -> String {
        format!("character varying({})", size)
    }

    fn quoted(&self) -> Value {
        Value::from(&format!("\"{}\"", self.name()))
    }

    fn dtype(&self) -> Value {
        Value::from(self.dtype_prop())
    }

    fn data_type(&self) -> Value {
        if self.is_string() {
            Value::from(self.string_type(self.string_size().expect("string should have a size")))
        } else if self.is_numeric() {
            Value::from(self.numeric_type(
                self.dtype_prop(),
                self.numeric_precision_prop(),
                self.numeric_scale_prop(),
            ))
        } else {
            // TODO for types such as Snowflake TIMESTAMP_LTZ(6), we should return ``format!("{}({})", dtype, precision)``.
            //  Note that this would not be dbt core compatible behavior, but a more correct one.
            //  Otherwise we may create/alter a table to a wrong type.
            //  See also https://github.com/dbt-labs/fs/pull/3585#discussion_r2112390711
            self.dtype()
        }
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/5de867965ab7bf7609caa624f98a31203998d1d1/dbt-adapters/src/dbt/adapters/base/column.py#L118
    fn numeric_type(&self, dtype: &str, precision: Option<u64>, scale: Option<u64>) -> String {
        if precision.is_some() && scale.is_some() {
            format!("{}({},{})", dtype, precision.unwrap(), scale.unwrap())
        } else {
            dtype.to_string()
        }
    }

    fn char_size(&self) -> Value {
        Value::from(self.char_size_prop())
    }

    fn as_value(&self) -> Value;
}

/// A struct representing a column type for use with static methods
#[derive(Clone, Debug, StaticBaseColumnObject)]
pub struct StdColumnType;

impl StaticBaseColumn for StdColumnType {
    fn try_new(
        name: String,
        dtype: String,
        char_size: Option<u32>,
        numeric_precision: Option<u64>,
        numeric_scale: Option<u64>,
    ) -> Result<Value, MinijinjaError> {
        Ok(Value::from_object(StdColumn {
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
pub struct StdColumn {
    pub name: String,
    pub dtype: String,
    /// The size of the column in characters (u32 is enough to hold) var char of max length
    /// Postgres is 65536 (2^16 - 1)
    /// Snowflake is 16777216 (2^24)
    pub char_size: Option<u32>,
    // TODO no need for u64; this should use 32 as char size (for consistency) or less; in some database scale can be negative
    pub numeric_precision: Option<u64>,
    pub numeric_scale: Option<u64>,
}

impl BaseColumn for StdColumn {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_value(&self) -> Value {
        Value::from_object(self.clone())
    }
}

impl BaseColumnProperties for StdColumn {
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

/// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/column.py#L104-L105
pub fn string_type(size: u32) -> String {
    format!("character varying({})", size)
}

pub struct SnowflakeColumnTypeParsed {
    pub data_type: String,
    pub char_size: Option<u32>,
    // TODO no need for u64; this should use 32 as char size (for consistency) or less as Snowflake precision, scale are in 0..38 range
    pub numeric_precision: Option<u64>,
    pub numeric_scale: Option<u64>,
}

impl SnowflakeColumnTypeParsed {
    pub fn basic(data_type: &str) -> Self {
        Self {
            data_type: data_type.to_string(),
            char_size: None,
            numeric_precision: None,
            numeric_scale: None,
        }
    }
}

/// Parse a Snowflake raw data type into a tuple of (data_type, char_size, numeric_precision, numeric_scale)
pub fn parse_snowflake_raw_data_type(
    raw_data_type: &str,
) -> Result<SnowflakeColumnTypeParsed, MinijinjaError> {
    // Parse data type using regex pattern ([^(]+)(\([^)]+\))?
    let re = regex::Regex::new(r"([^(]+)(\([^)]+\))?").expect("A valid regex");
    let captures = re.captures(raw_data_type).ok_or_else(|| {
        MinijinjaError::new(
            ErrorKind::InvalidArgument,
            format!("Could not interpret raw_data_type \"{raw_data_type}\""),
        )
    })?;

    let data_type = captures
        .get(1)
        .expect("First match group exists")
        .as_str()
        .to_string();
    let mut char_size = None;
    let mut numeric_precision = None;
    let mut numeric_scale = None;

    // If we have size info (the second capture group)
    let err_msg = |raw_data_type: &str, name: &str| {
        MinijinjaError::new(
            ErrorKind::InvalidArgument,
            format!(
                "Could not interpret data_type \"{}\": could not convert \"{}\" to an integer",
                raw_data_type, name
            ),
        )
    };
    if let Some(size_match) = captures.get(2) {
        let size_info = &size_match.as_str()[1..size_match.as_str().len() - 1];
        let parts: Vec<&str> = size_info.split(',').collect();

        match parts.len() {
            1 => {
                // parse as char_size
                char_size = Some(
                    parts[0]
                        .parse::<u32>()
                        .map_err(|_| err_msg(raw_data_type, parts[0]))?,
                );
            }
            2 => {
                // parse as numeric precision and scale
                numeric_precision = Some(
                    parts[0]
                        .parse::<u64>()
                        .map_err(|_| err_msg(raw_data_type, parts[0]))?,
                );
                numeric_scale = Some(
                    parts[1]
                        .parse::<u64>()
                        .map_err(|_| err_msg(raw_data_type, parts[0]))?,
                );
            }
            _ => {}
        }
    }
    Ok(SnowflakeColumnTypeParsed {
        data_type,
        char_size,
        numeric_precision,
        numeric_scale,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use minijinja::value::Kwargs;
    use std::collections::BTreeMap;

    #[test]
    fn test_numeric_type_without_precision_scale() {
        let mut map = BTreeMap::new();
        map.insert("dtype", Value::from("NUMERIC"));
        let args = vec![Value::from(Kwargs::from_iter(map))];
        let result = StdColumnType::numeric_type(&args).unwrap();
        assert_eq!(result.as_str().unwrap(), "NUMERIC");
    }

    #[test]
    fn test_numeric_type_with_precision_scale() {
        let mut map = BTreeMap::new();
        map.insert("dtype", Value::from("NUMERIC"));
        map.insert("precision", Value::from(10));
        map.insert("scale", Value::from(2));
        let args = vec![Value::from(Kwargs::from_iter(map))];
        let result = StdColumnType::numeric_type(&args).unwrap();
        assert_eq!(result.as_str().unwrap(), "NUMERIC(10,2)");
    }

    #[test]
    fn test_from_description() {
        // Test simple type without parameters
        {
            let mut map = BTreeMap::new();
            let name = "col1".to_owned();
            let dtype = "varchar".to_owned();
            map.insert("name", Value::from(&name));
            map.insert("raw_data_type", Value::from(&dtype));

            let args = vec![Value::from(Kwargs::from_iter(map))];
            let actual = StdColumnType::from_description(&args).unwrap();
            let expected = Value::from_object(StdColumn {
                name,
                dtype,
                ..Default::default()
            });
            assert_eq!(expected, actual);
        }

        // Test type with character size
        {
            let mut map = BTreeMap::new();
            let name = "col2".to_owned();
            let dtype = "varchar".to_owned();
            map.insert("name", Value::from(&name));
            map.insert("raw_data_type", Value::from("varchar(255)"));

            let args = vec![Value::from(Kwargs::from_iter(map))];
            let actual = StdColumnType::from_description(&args).unwrap();
            let expected = Value::from_object(StdColumn {
                name,
                dtype,
                char_size: Some(255),
                ..Default::default()
            });
            assert_eq!(expected, actual);
        }

        // Test numeric type with precision and scale
        {
            let mut map = BTreeMap::new();
            let name = "col3".to_owned();
            let dtype = "numeric".to_owned();
            map.insert("name", Value::from(&name));
            map.insert("raw_data_type", Value::from("numeric(10,2)"));

            let args = vec![Value::from(Kwargs::from_iter(map))];
            let actual = StdColumnType::from_description(&args).unwrap();
            let expected = Value::from_object(StdColumn {
                name,
                dtype,
                numeric_precision: Some(10),
                numeric_scale: Some(2),
                ..Default::default()
            });
            assert_eq!(expected, actual);
        }

        // Test invalid type format
        {
            let mut map = BTreeMap::new();
            let name = "col4".to_owned();
            map.insert("name", Value::from(&name));
            map.insert("raw_data_type", Value::from("varchar(invalid)"));
            let args = vec![Value::from(Kwargs::from_iter(map))];
            let result = StdColumnType::from_description(&args);
            assert!(result.is_err());
        }

        // Test Snowflake timestamp_ltz with specific precision
        {
            let args = vec![Value::from("COL_NAME"), Value::from("TIMESTAMP_LTZ(6)")];
            let actual = StdColumnType::from_description(&args).unwrap();
            let expected = Value::from_object(StdColumn {
                name: "COL_NAME".to_string(),
                dtype: "TIMESTAMP_LTZ".to_string(),
                char_size: Some(6),
                ..Default::default()
            });
            assert_eq!(expected, actual);
        }
    }
}
